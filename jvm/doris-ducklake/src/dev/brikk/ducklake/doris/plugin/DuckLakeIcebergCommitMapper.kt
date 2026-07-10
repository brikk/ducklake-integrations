package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import org.apache.doris.thrift.TIcebergCommitData
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.time.LocalDate

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
 * Min/max arrive as Iceberg single-value binary (`ByteBuffer`, little-endian per
 * the Iceberg single-value serialization spec). We decode ONLY the spec-locked,
 * unambiguous cases and return `null` for everything else. A `null` bound is
 * **safe**: the catalog stores it as unknown and pruning simply never eliminates
 * that file on that column. A *wrong* bound would be a correctness bug (it could
 * prune a file that holds a matching row), so we never guess.
 *
 * Decoded types, and the exact DuckLake stat-string form each must produce (the
 * authoritative oracle is `DucklakeStatTypes.parseStat`, which the READ-path range
 * prune calls on the same stored strings):
 *  - `int8/int16/int32` → Iceberg int32 (4-byte LE) → decimal string.
 *  - `int64` → Iceberg int64 (8-byte LE) → decimal string.
 *  - `float32` → Iceberg float (4-byte LE IEEE-754) → `BigDecimal`-parseable string.
 *  - `float64` → Iceberg double (8-byte LE IEEE-754) → `BigDecimal`-parseable string.
 *  - `date` → Iceberg date (4-byte LE int, days-from-epoch) → ISO-8601 `yyyy-MM-dd`
 *    (`DucklakeStatTypes` orders dates lexically, and ISO dates sort chronologically).
 *  - `decimal(p,s)` → Iceberg decimal (big-endian, two's-complement unscaled int,
 *    minimum-length) → plain `BigDecimal` string using the scale `s` parsed from the
 *    DuckLake type (`DucklakeStatTypes` treats `decimal*` as numeric → `BigDecimal`).
 *  - `varchar` → UTF-8.
 *
 * Still `null` (unsafe to decode without more context): `timestamp*` — the exact
 * textual form DuckDB stores is not pinned here, and our write-side stat is
 * lexically min/max-merged against DuckDB-written files' stats, so a mismatched
 * form (e.g. fractional-second trimming) would pick a wrong table-level bound and
 * silently mis-prune; unsigned ints, `int128`, `blob`, `uuid`, `boolean`, nested.
 * A non-finite float (`NaN`/±Inf) would not round-trip through `BigDecimal`, so
 * those are dropped too. A `decimal` with no `(p,s)` (scale unknown) stays `null`.
 *
 * ## Known BE-coupled gaps (validate against a live BE before un-gating writes)
 * - **footer_size**: `TIcebergCommitData` carries no Parquet footer length, so we
 *   pass `0` (the catalog stores it as NULL = "unknown"). The reader must tolerate
 *   a NULL footer size, or the BE must be extended to report it.
 * - **path**: assumed relative to the table data dir (`pathIsRelative = true`, the
 *   INSERT convention); if the BE returns an absolute path it must be relativized.
 * - **partition_values**: positional list → `partitionKeyIndex` assumes the BE orders
 *   values by DuckLake's partition-key index (the order [DuckLakeIcebergPartitionSpec]
 *   added the fields in). `partitionId` (the DuckLake spec id) isn't derivable from
 *   the Iceberg `partition_spec_id`, so it's passed in from the FE-bound spec.
 */
internal object DuckLakeIcebergCommitMapper {

    /**
     * Map one BE commit fragment to a DuckLake write fragment, using the table's
     * `columnId -> type` and the table data dir to relativize the BE's path.
     * [partitionId] is the active DuckLake partition spec id (null for unpartitioned
     * tables), bound onto the transaction by the write plan.
     */
    fun toWriteFragment(
        data: TIcebergCommitData,
        typeByColumnId: Map<Long, String>,
        tableDataDir: String? = null,
        partitionId: Long? = null,
    ): DucklakeWriteFragment {
        val recordCount = if (data.isSetRowCount) data.rowCount else 0L
        val (path, pathIsRelative) = resolvePath(if (data.isSetFilePath) data.filePath else "", tableDataDir)
        return DucklakeWriteFragment(
            path = path,
            pathIsRelative = pathIsRelative,
            fileFormat = "parquet",
            fileSizeBytes = if (data.isSetFileSize) data.fileSize else 0L,
            footerSize = 0L, // not in TIcebergCommitData → catalog stores NULL (see class doc)
            recordCount = recordCount,
            columnStats = toColumnStats(data, recordCount, typeByColumnId),
            partitionValues = partitionValues(data),
            partitionId = partitionId, // active DuckLake spec id, bound by the write plan (null = unpartitioned)
            nameMap = null, // default field-id projection (Parquet field_id == column_id)
        )
    }

    /**
     * The BE reports an **absolute** file path; DuckLake records INSERT files
     * **relative** to the table data dir. Strip the dir prefix → relative. If the
     * path is outside the dir (or no dir is known), keep it absolute so DuckLake
     * reads it as-is rather than (wrongly) joining it under the table dir — the
     * "doubled path" bug that breaks read-back.
     */
    private fun resolvePath(filePath: String, tableDataDir: String?): Pair<String, Boolean> {
        if (tableDataDir != null) {
            val dir = tableDataDir.trimEnd('/')
            if (filePath.startsWith(dir)) {
                return filePath.removePrefix(dir).trimStart('/') to true
            }
        }
        val absolute = filePath.contains("://") || filePath.startsWith("/")
        return filePath to !absolute
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
        val width = bytes.remaining()
        return when {
            isIntegerType(type) && width == Int.SIZE_BYTES -> bytes.int.toString()
            isIntegerType(type) && width == Long.SIZE_BYTES -> bytes.long.toString()
            type == "float32" && width == Float.SIZE_BYTES -> decodeFinite(bytes.float.toDouble())
            type == "float64" && width == Double.SIZE_BYTES -> decodeFinite(bytes.double)
            type == "date" && width == Int.SIZE_BYTES -> LocalDate.ofEpochDay(bytes.int.toLong()).toString()
            type.startsWith("decimal") -> decodeDecimal(type, buffer)
            type == "varchar" -> StandardCharsets.UTF_8.decode(bytes).toString()
            else -> null // timestamp/unsigned/int128/blob/uuid/boolean — not decoded (safe: no pruning)
        }
    }

    /**
     * Decode an Iceberg decimal single-value bound. Iceberg stores the unscaled
     * value as a minimum-length **big-endian** two's-complement integer (unlike the
     * little-endian fixed-width primitives above), and the scale lives only in the
     * DuckLake type string (`decimal(p,s)`). Returns `null` when the scale is absent
     * or the payload is empty (safe: no pruning).
     */
    private fun decodeDecimal(ducklakeType: String, buffer: ByteBuffer): String? {
        val scale = decimalScale(ducklakeType) ?: return null
        val raw = ByteArray(buffer.remaining())
        buffer.duplicate().get(raw) // raw stored order is big-endian, regardless of ByteBuffer order()
        if (raw.isEmpty()) {
            return null
        }
        return BigDecimal(BigInteger(raw), scale).toPlainString()
    }

    /** Scale `s` from `decimal(p,s)`; `0` from `decimal(p)`; `null` if unparseable/absent. */
    private fun decimalScale(ducklakeType: String): Int? {
        val open = ducklakeType.indexOf('(')
        val close = ducklakeType.indexOf(')', open + 1)
        if (open < 0 || close < 0) {
            return null
        }
        val args = ducklakeType.substring(open + 1, close).split(',')
        return when (args.size) {
            1 -> 0
            2 -> args[1].trim().toIntOrNull()
            else -> null
        }
    }

    /**
     * Render a floating-point bound as a `DucklakeStatTypes`-parseable (BigDecimal)
     * string, or null for a non-finite value (`NaN`/±Infinity don't round-trip
     * through BigDecimal, and a NaN bound has no ordering — null is safe: no pruning).
     */
    private fun decodeFinite(value: Double): String? =
        if (value.isFinite()) value.toBigDecimal().toPlainString() else null

    // int8/int16/int32 all arrive as Iceberg int32 (4 bytes); int64 as 8 bytes.
    private fun isIntegerType(type: String): Boolean =
        type == "int8" || type == "int16" || type == "int32" || type == "int64"
}
