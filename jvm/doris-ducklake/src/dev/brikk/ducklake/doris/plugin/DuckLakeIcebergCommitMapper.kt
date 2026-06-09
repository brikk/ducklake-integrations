package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import org.apache.doris.thrift.TIcebergCommitData
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

/**
 * Maps a BE write-result fragment (`TIcebergCommitData`, the rich per-file commit
 * struct the BE's Iceberg file-writer emits — path + size + row count + per-field-id
 * column stats + partition values) into a DuckLake [DucklakeWriteFragment] that
 * `DucklakeCatalog.commitInsert` records as a new data file in a fresh snapshot.
 *
 * DuckLake is Iceberg-compatible by design — Parquet **field-ids == DuckLake
 * `column_id`** — so the Iceberg stats key (field-id) is the DuckLake column id
 * directly. The connector emits a `TIcebergTableSink` so the BE computes these
 * stats from the Parquet footer for free (see [DuckLakeConnectorTransaction]).
 *
 * ## Deliberately conservative bound decoding
 * Min/max arrive as Iceberg single-value binary (`ByteBuffer`). We decode ONLY the
 * spec-locked, unambiguous cases — signed integers (little-endian 4/8 bytes) and
 * `varchar` (UTF-8) — and return `null` for every other type. A `null` bound is
 * **safe**: the catalog stores it as unknown and pruning simply never eliminates
 * that file on that column. A *wrong* bound would be a correctness bug (it could
 * prune a file that holds a matching row), so we never guess.
 *
 * ## Known BE-coupled gaps (validate against a live BE before un-gating writes)
 * - **footer_size**: `TIcebergCommitData` carries no Parquet footer length, so we
 *   pass `0` (the catalog stores it as NULL = "unknown"). The reader must tolerate
 *   a NULL footer size, or the BE must be extended to report it.
 * - **path**: assumed relative to the table data dir (`pathIsRelative = true`, the
 *   INSERT convention); if the BE returns an absolute path it must be relativized.
 * - **partition_values / partition_id**: positional list → `partitionKeyIndex`
 *   assumes the BE orders values by DuckLake's partition-key index; `partitionId`
 *   (DuckLake spec id) is not derivable from the Iceberg `partition_spec_id`.
 *   Unpartitioned writes (the tested path) are unaffected.
 */
internal object DuckLakeIcebergCommitMapper {

    /** Map one BE commit fragment to a DuckLake write fragment, using the table's `columnId -> type`. */
    fun toWriteFragment(data: TIcebergCommitData, typeByColumnId: Map<Long, String>): DucklakeWriteFragment {
        val recordCount = if (data.isSetRowCount) data.rowCount else 0L
        return DucklakeWriteFragment(
            path = data.filePath,
            pathIsRelative = true, // INSERT convention: files live under the table data dir
            fileFormat = "parquet",
            fileSizeBytes = if (data.isSetFileSize) data.fileSize else 0L,
            footerSize = 0L, // not in TIcebergCommitData → catalog stores NULL (see class doc)
            recordCount = recordCount,
            columnStats = toColumnStats(data, recordCount, typeByColumnId),
            partitionValues = partitionValues(data),
            partitionId = null, // see class doc — not derivable from Iceberg spec id
            nameMap = null, // default field-id projection (Parquet field_id == column_id)
        )
    }

    private fun toColumnStats(
        data: TIcebergCommitData,
        recordCount: Long,
        typeByColumnId: Map<Long, String>,
    ): List<DucklakeFileColumnStats> {
        if (!data.isSetColumnStats) {
            return emptyList()
        }
        // Thrift getters return null for unset map fields, hence `?: emptyMap()`.
        val maps = with(data.columnStats) {
            IcebergStatMaps(
                sizes = columnSizes ?: emptyMap(),
                valueCounts = valueCounts ?: emptyMap(),
                nullCounts = nullValueCounts ?: emptyMap(),
                nanCounts = nanValueCounts ?: emptyMap(),
                lowers = lowerBounds ?: emptyMap(),
                uppers = upperBounds ?: emptyMap(),
            )
        }
        // One DuckLake stat row per field-id that carries any statistic.
        return maps.fieldIds().mapNotNull { fieldId ->
            val type = typeByColumnId[fieldId.toLong()] ?: return@mapNotNull null // unknown column → drop
            buildStat(fieldId, type, recordCount, maps)
        }
    }

    private fun buildStat(
        fieldId: Int,
        type: String,
        recordCount: Long,
        maps: IcebergStatMaps,
    ): DucklakeFileColumnStats {
        val nulls = maps.nullCounts[fieldId] ?: 0L
        // Iceberg value_counts is total-incl-null; DuckLake wants the non-null count.
        val total = maps.valueCounts[fieldId] ?: recordCount
        return DucklakeFileColumnStats(
            columnId = fieldId.toLong(),
            columnSizeBytes = maps.sizes[fieldId] ?: 0L,
            valueCount = (total - nulls).coerceAtLeast(0L),
            nullCount = nulls,
            minValue = maps.lowers[fieldId]?.let { decodeBound(type, it) },
            maxValue = maps.uppers[fieldId]?.let { decodeBound(type, it) },
            containsNan = (maps.nanCounts[fieldId] ?: 0L) > 0L,
        )
    }

    /** The six per-field-id Iceberg stat maps, gathered once from `TIcebergColumnStats`. */
    private class IcebergStatMaps(
        val sizes: Map<Int, Long>,
        val valueCounts: Map<Int, Long>,
        val nullCounts: Map<Int, Long>,
        val nanCounts: Map<Int, Long>,
        val lowers: Map<Int, ByteBuffer>,
        val uppers: Map<Int, ByteBuffer>,
    ) {
        fun fieldIds(): Set<Int> =
            (sizes.keys + valueCounts.keys + nullCounts.keys + lowers.keys + uppers.keys).toSortedSet()
    }

    /** Positional partition values → `partitionKeyIndex -> value` (empty for unpartitioned files). */
    private fun partitionValues(data: TIcebergCommitData): Map<Int, String?> {
        if (!data.isSetPartitionValues || data.partitionValues.isEmpty()) {
            return emptyMap()
        }
        return data.partitionValues.withIndex().associate { (index, value) -> index to value }
    }

    /**
     * Decode an Iceberg single-value binary bound to a DuckLake stat string, or null
     * if the type isn't one we decode unambiguously (see class doc — null is safe).
     */
    private fun decodeBound(ducklakeType: String, buffer: ByteBuffer): String? {
        val bytes = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN)
        val type = ducklakeType.trim().lowercase()
        return when {
            isIntegerType(type) && bytes.remaining() == Int.SIZE_BYTES -> bytes.int.toString()
            isIntegerType(type) && bytes.remaining() == Long.SIZE_BYTES -> bytes.long.toString()
            type == "varchar" -> StandardCharsets.UTF_8.decode(bytes).toString()
            else -> null // float/decimal/date/timestamp/unsigned/etc. — not decoded (safe: no pruning)
        }
    }

    private fun isIntegerType(type: String): Boolean =
        type == "int8" || type == "int16" || type == "int32" || type == "int64"
}
