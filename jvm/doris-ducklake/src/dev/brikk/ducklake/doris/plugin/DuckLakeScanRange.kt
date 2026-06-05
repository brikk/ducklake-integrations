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
 */
internal class DuckLakeScanRange(
    private val path: String,
    private val start: Long,
    private val length: Long,
    private val fileSize: Long,
    private val fileFormat: String,
    positionDeletes: List<DuckLakePositionDelete>,
) : ConnectorScanRange, Serializable {

    private val positionDeletes: List<DuckLakePositionDelete> =
        java.util.List.copyOf(Objects.requireNonNull(positionDeletes, "positionDeletes"))

    constructor(path: String, start: Long, length: Long, fileSize: Long, fileFormat: String) :
        this(path, start, length, fileSize, fileFormat, listOf())

    init {
        Objects.requireNonNull(path, "path")
        Objects.requireNonNull(fileFormat, "fileFormat")
    }

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

    override fun getPartitionValues(): Map<String, String> = emptyMap()

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
        // Partition values / columns_from_path stay unset — partition pruning
        // is a Step-6 / v1.5 concern.
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
