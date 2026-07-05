package dev.brikk.ducklake.doris.plugin

import java.io.Serializable
import java.util.Locale
import java.util.Objects
import java.util.Optional
import org.apache.doris.connector.api.scan.ConnectorDeleteFile
import org.apache.doris.connector.api.scan.ConnectorScanRange
import org.apache.doris.connector.api.scan.ConnectorScanRangeType
import org.apache.doris.thrift.TFileFormatType
import org.apache.doris.thrift.TFileRangeDesc
import org.apache.doris.thrift.TIcebergDeleteFileDesc
import org.apache.doris.thrift.TIcebergFileDesc
import org.apache.doris.thrift.TTableFormatFileDesc

/**
 * Per-file scan range emitted by [DuckLakeScanPlanProvider].
 *
 * Step 4 / Option A wire format (sanity-check §2.1): emits
 * `table_format_type = "iceberg"` and packs the file shape into
 * `iceberg_params` so Doris's BE dispatches the existing Iceberg
 * parquet reader. DuckLake's parquet files already carry field-ids on each
 * column, which is what the reader consumes — zero BE change needed.
 *
 * Step 7: position-delete files supplied via [DuckLakePositionDelete]
 * are emitted as `iceberg_params.delete_files` entries with
 * `content = POSITION_DELETE (1)`. DuckLake catalogues only one active
 * position-delete file per data file per snapshot, so `positionDeletes`
 * has size 0 or 1 in v1; the wire list shape leaves headroom for that to
 * relax without a shape change.
 *
 * P6 additions (both FE-plan-level; the normal-range thrift bytes pinned by
 * `DuckLakeScanRangeThriftParityTest` are unchanged):
 *  - [pushDownRowCount]: the precomputed COUNT(*)-pushdown total the single
 *    collapsed count range carries (`-1` sentinel on every normal range —
 *    load-bearing, see `PluginDrivenScanNode.resolvePushDownRowCount`).
 *  - [partitionBearing] / [partitionValues]: metadata-sourced partition info
 *    so the engine never falls back to Hive-style `key=value` path parsing
 *    for DuckLake's non-`key=value` file layout (see
 *    `PluginDrivenSplit.buildPartitionValues` and the P6 report §(d)).
 *
 * Constructed via [Builder] (mirrors `IcebergScanRange.Builder`); the
 * positional constructors below survive for the pre-P6 call sites and the
 * thrift-parity goldens.
 */
internal class DuckLakeScanRange private constructor(builder: Builder) :
    ConnectorScanRange, Serializable {

    private val path: String =
        Objects.requireNonNull(builder.path, "path") as String
    private val start: Long = builder.start
    private val length: Long = builder.length
    private val fileSize: Long = builder.fileSize
    private val fileFormat: String =
        Objects.requireNonNull(builder.fileFormat, "fileFormat") as String
    private val positionDeletes: List<DuckLakePositionDelete> =
        java.util.List.copyOf(Objects.requireNonNull(builder.positionDeletes, "positionDeletes"))
    // Metadata-sourced partition marker + identity values (see class KDoc).
    // Copied defensively — the range is handed to the engine and serialized.
    private val partitionBearing: Boolean = builder.partitionBearing
    private val partitionValues: Map<String, String> =
        java.util.Map.copyOf(Objects.requireNonNull(builder.partitionValues, "partitionValues"))
    // -1 = "no precomputed count" sentinel (the ConnectorScanRange default).
    private val pushDownRowCount: Long = builder.pushDownRowCount

    constructor(path: String, start: Long, length: Long, fileSize: Long, fileFormat: String) :
        this(path, start, length, fileSize, fileFormat, listOf())

    constructor(
        path: String,
        start: Long,
        length: Long,
        fileSize: Long,
        fileFormat: String,
        positionDeletes: List<DuckLakePositionDelete>,
    ) : this(
        Builder()
            .path(path)
            .start(start)
            .length(length)
            .fileSize(fileSize)
            .fileFormat(fileFormat)
            .positionDeletes(positionDeletes),
    )

    @JvmName("positionDeletes")
    fun positionDeletes(): List<DuckLakePositionDelete> = positionDeletes

    override fun getRangeType(): ConnectorScanRangeType = ConnectorScanRangeType.FILE_SCAN

    override fun getPath(): Optional<String> = Optional.of(path)

    override fun getStart(): Long = start

    override fun getLength(): Long = length

    override fun getFileSize(): Long = fileSize

    override fun getFileFormat(): String = fileFormat

    override fun getProperties(): Map<String, String> = emptyMap()

    override fun getTableFormatType(): String = TABLE_FORMAT_TYPE

    /**
     * The identity partition column values for this file, keyed by lowercased
     * column name in active-spec field order (mirrors iceberg's
     * `getIdentityPartitionInfoMap` keying). Consumed FE-side only:
     * `PluginDrivenSplit.buildPartitionValues` copies `values()` onto the
     * split. Never written to thrift here — DuckLake's writer stores partition
     * columns in the parquet body, so the BE decodes them from the file
     * (we deliberately emit no `path_partition_keys`, unlike iceberg).
     */
    override fun getPartitionValues(): Map<String, String> = partitionValues

    /**
     * `true` for every range of a table with an ACTIVE partition spec (even
     * when this particular file recorded no values — e.g. bucket-only specs,
     * or files written under a retired spec). DuckLake partition values come
     * from `ducklake_file_partition_value` metadata, never from a Hive-style
     * `key=value` directory layout, so the engine must map an empty values
     * map to a non-null EMPTY list instead of `null` — `null` makes
     * `FileQueryScanNode` path-parse, which breaks on our layout (see
     * `PluginDrivenSplit.buildPartitionValues`). Unpartitioned tables keep
     * `false`, preserving the pre-P6 behaviour byte-for-byte.
     */
    override fun isPartitionBearing(): Boolean = partitionBearing

    /**
     * The precomputed COUNT(*)-pushdown row count this range carries, or `-1`
     * for every normal range. The generic `PluginDrivenScanNode` reads it
     * (via `resolvePushDownRowCount`) for the EXPLAIN `pushdown agg=COUNT (n)`
     * line; the same value drives the BE thrift `table_level_row_count` in
     * [populateRangeParams]. The `-1` sentinel is load-bearing: a table whose
     * count is not exactly servable from file metadata must emit NO count
     * range so the BE counts by reading (mirrors `IcebergScanRange`).
     */
    override fun getPushDownRowCount(): Long = pushDownRowCount

    override fun getDeleteFiles(): List<ConnectorDeleteFile> = emptyList()

    override fun populateRangeParams(formatDesc: TTableFormatFileDesc, rangeDesc: TFileRangeDesc) {
        Objects.requireNonNull(formatDesc, "formatDesc")
        Objects.requireNonNull(rangeDesc, "rangeDesc")

        val fileDesc = TIcebergFileDesc()
        fileDesc.formatVersion = ICEBERG_FORMAT_VERSION
        fileDesc.originalFilePath = path
        // v2 enables BE delete-file dispatch. We always emit the list field
        // (even when empty) so the BE side reads a present-but-empty list
        // rather than an unset field — matches IcebergScanRange behaviour.
        val deletes = ArrayList<TIcebergDeleteFileDesc>(positionDeletes.size)
        for (pd in positionDeletes) {
            deletes.add(toThrift(pd))
        }
        fileDesc.deleteFiles = deletes

        formatDesc.icebergParams = fileDesc
        // table_level_row_count: only the single collapsed COUNT(*)-pushdown
        // range carries a real count (BE's count reader serves it without
        // opening the file). Normal ranges leave the field UNSET: thrift
        // declares `table_level_row_count = -1` as the default
        // (PlanNodes.thrift), so unset is behaviourally identical to
        // IcebergScanRange's explicit -1 while keeping the normal-range wire
        // bytes byte-identical to the pre-count-pushdown shape pinned by
        // DuckLakeScanRangeThriftParityTest.
        if (pushDownRowCount >= 0) {
            formatDesc.tableLevelRowCount = pushDownRowCount
        }
        // Partition values / columns_from_path stay unset — DuckLake data
        // files carry partition columns in the parquet body, so the BE reads
        // them from the file; [getPartitionValues] is FE-plan-level only.
    }

    /**
     * Builder mirroring `IcebergScanRange.Builder`, introduced when the range
     * grew past the positional-constructor sweet spot (partition bearing +
     * values + count pushdown would push the primary constructor to nine
     * parameters). Only [DuckLakeScanPlanProvider] constructs through it.
     */
    internal class Builder {
        var path: String? = null
            private set
        var start: Long = 0L
            private set
        var length: Long = -1L
            private set
        var fileSize: Long = -1L
            private set
        var fileFormat: String? = null
            private set
        var positionDeletes: List<DuckLakePositionDelete> = listOf()
            private set
        var partitionBearing: Boolean = false
            private set
        var partitionValues: Map<String, String> = mapOf()
            private set
        var pushDownRowCount: Long = -1L
            private set

        fun path(path: String): Builder = apply { this.path = path }

        fun start(start: Long): Builder = apply { this.start = start }

        fun length(length: Long): Builder = apply { this.length = length }

        fun fileSize(fileSize: Long): Builder = apply { this.fileSize = fileSize }

        fun fileFormat(fileFormat: String): Builder = apply { this.fileFormat = fileFormat }

        fun positionDeletes(positionDeletes: List<DuckLakePositionDelete>): Builder =
            apply { this.positionDeletes = positionDeletes }

        fun partitionBearing(partitionBearing: Boolean): Builder =
            apply { this.partitionBearing = partitionBearing }

        fun partitionValues(partitionValues: Map<String, String>): Builder =
            apply { this.partitionValues = partitionValues }

        fun pushDownRowCount(pushDownRowCount: Long): Builder =
            apply { this.pushDownRowCount = pushDownRowCount }

        fun build(): DuckLakeScanRange = DuckLakeScanRange(this)
    }

    companion object {
        private const val serialVersionUID: Long = 1L

        // Iceberg v2 is the minimum that enables the BE reader's delete-file
        // dispatch path. v2 also matches DuckLake's positional-delete semantics
        // (sanity-check §2.1) so populating iceberg_params.delete_files is the
        // entire Step 7 wire-side delta — no version bump required.
        const val ICEBERG_FORMAT_VERSION: Int = 2

        // TODO(option-B): once upstream lands the 5-line BE patch recognising
        // "ducklake" as a synonym of "iceberg" in file_scanner.cpp dispatch
        // (sanity-check §2.1), flip this to "ducklake". The thrift bytes for
        // iceberg_params remain identical — only this discriminator changes.
        const val TABLE_FORMAT_TYPE: String = "iceberg"

        // Iceberg's TIcebergDeleteFileDesc.content discriminator: 1 = position
        // delete, 2 = equality delete, 3 = deletion vector. DuckLake only emits
        // position deletes (sanity-check §4: no equality-delete capability).
        // Mirrors IcebergDeleteFileDescriptor.Kind.POSITION_DELETE.wireValue().
        const val CONTENT_POSITION_DELETE: Int = 1

        private fun toThrift(pd: DuckLakePositionDelete): TIcebergDeleteFileDesc {
            val t = TIcebergDeleteFileDesc()
            t.setPath(pd.path)
            // DuckLake's catalog records the delete file's format on the
            // ducklake_delete_file.format column ("parquet" in v1). The BE
            // reader's iceberg path needs TFileFormatType (an enum); map by
            // lowercased name. Anything other than parquet would be a writer
            // upgrade we'd need to coordinate with the BE side anyway.
            when (pd.fileFormat.lowercase(Locale.ROOT)) {
                "parquet" -> t.setFileFormat(TFileFormatType.FORMAT_PARQUET)
                else -> throw IllegalStateException(
                    "Unsupported DuckLake delete-file format: " + pd.fileFormat,
                )
            }
            // POSITION_DELETE discriminator. position_lower_bound /
            // position_upper_bound are left unset: DuckLake's catalog doesn't
            // record row-position bounds on delete files in v1, so the BE has
            // to scan the full delete file. Functionally correct; pruning
            // opportunity for a later pass.
            t.setContent(CONTENT_POSITION_DELETE)
            return t
        }
    }
}
