package dev.brikk.ducklake.doris.plugin

import java.util.HashMap
import java.util.LinkedHashMap
import java.util.Objects
import java.util.Optional

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeDataFile

import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.handle.ConnectorColumnHandle
import org.apache.doris.connector.api.handle.ConnectorTableHandle
import org.apache.doris.connector.api.pushdown.ConnectorExpression
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider
import org.apache.doris.connector.api.scan.ConnectorScanRange
import org.apache.doris.connector.api.scan.ConnectorScanRangeType
import org.apache.doris.connector.api.scan.ConnectorScanRequest
import org.apache.doris.thrift.TFileScanRangeParams

/**
 * Splits a DuckLake table read into per-file scan ranges. v1 emits one range
 * per [DucklakeDataFile] active at the snapshot pinned on the
 * [DuckLakeTableHandle]; deletes / pushdown / time-travel resolution
 * layer on top per the roadmap.
 */
internal class DuckLakeScanPlanProvider(
    catalog: DucklakeCatalog,
    pathResolver: DuckLakePathResolver,
    catalogProperties: Map<String, String>,
) : ConnectorScanPlanProvider {

    private val catalog: DucklakeCatalog =
        Objects.requireNonNull(catalog, "catalog")
    private val pathResolver: DuckLakePathResolver =
        Objects.requireNonNull(pathResolver, "pathResolver")
    private val catalogProperties: Map<String, String> =
        java.util.Map.copyOf(Objects.requireNonNull(catalogProperties, "catalogProperties"))

    override fun getScanRangeType(): ConnectorScanRangeType = ConnectorScanRangeType.FILE_SCAN

    override fun planScan(req: ConnectorScanRequest): List<ConnectorScanRange> {
        Objects.requireNonNull(req, "req")
        val handle = req.table.asDuckLakeHandle<DuckLakeTableHandle>()

        val schema = catalog.getSchema(handle.database, handle.snapshotId)
            ?: throw IllegalStateException(
                "Schema metadata missing for '" + handle.database + "' at snapshot " +
                    handle.snapshotId,
            )
        val table = catalog.getTableById(handle.tableId, handle.snapshotId)
            ?: throw IllegalStateException(
                "Table metadata missing for tableId=" + handle.tableId +
                    " at snapshot " + handle.snapshotId,
            )

        val tableDataPath = pathResolver.resolveTableDataPath(schema, table)

        val dataFiles = catalog.getDataFiles(handle.tableId, handle.snapshotId)

        val ranges = ArrayList<ConnectorScanRange>(dataFiles.size)
        for (file in dataFiles) {
            val absolutePath = pathResolver.resolveFilePath(
                file.path, file.pathIsRelative, tableDataPath,
            )
            // Full-file extent: start=0, length=fileSize. BE splits internally
            // along parquet row groups; pre-splitting in the planner is a v2 concern.
            ranges.add(
                DuckLakeScanRange(
                    absolutePath,
                    0L,
                    file.fileSizeBytes,
                    file.fileSizeBytes,
                    normalizeFileFormat(file.fileFormat),
                    resolvePositionDeletes(file, tableDataPath),
                ),
            )
        }
        return ranges
    }

    /**
     * Surfaces the at-most-one active position-delete file the catalog already
     * inlines on the [DucklakeDataFile] (LEFT JOIN at
     * `JdbcDucklakeCatalog#getDataFiles`, snapshot-filtered). Returns
     * an empty list when the data file has no active deletes.
     *
     * DuckLake's catalog guarantees at most one active delete file per
     * data file per snapshot
     * (`JdbcDucklakeCatalog#checkDeleteFileOverlap`), so no second
     * catalog round-trip is needed.
     */
    private fun resolvePositionDeletes(
        file: DucklakeDataFile,
        tableDataPath: String,
    ): List<DuckLakePositionDelete> {
        val deletePath = file.deleteFilePath
        if (deletePath.isEmpty) {
            return listOf()
        }
        // PATH_IS_RELATIVE and FORMAT come from the same row as PATH; when
        // PATH is non-null the others are non-null too. Defensive defaults
        // mirror DuckLake's own convention (relative paths under the
        // warehouse, parquet format).
        val isRelative = file.deleteFilePathIsRelative.orElse(true)
        val absolutePath = pathResolver.resolveFilePath(
            deletePath.get(), isRelative, tableDataPath,
        )
        val format = normalizeFileFormat(file.deleteFileFormat.orElse("parquet"))
        return listOf(DuckLakePositionDelete(absolutePath, format))
    }

    /**
     * Emits the storage credentials (s3.*, hdfs.*, AWS_*, etc.) the BE needs
     * to open data files, under [PROP_LOCATION_PREFIX] so
     * [populateScanLevelParams] can strip the prefix back off on its
     * way to the thrift descriptor. Mirrors the Iceberg connector's pattern;
     * DuckLake adds no vended-credential layer for v1 (no REST catalog), so
     * we pass through the static catalog properties only.
     */
    override fun getScanNodeProperties(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        columns: List<ConnectorColumnHandle>,
        filter: Optional<ConnectorExpression>,
    ): Map<String, String> {
        val out: MutableMap<String, String> = LinkedHashMap()
        // Tell PluginDrivenScanNode which BE reader to dispatch (else it
        // defaults to FORMAT_JNI and the iceberg reader bails with
        // "Not supported create reader for table format: iceberg / file
        // format: FORMAT_JNI").
        out[PROP_FILE_FORMAT_TYPE] = PARQUET_FORMAT
        for ((key, value) in catalogProperties) {
            if (isStorageProperty(key)) {
                out[PROP_LOCATION_PREFIX + key] = value
            }
        }
        return out
    }

    override fun populateScanLevelParams(
        params: TFileScanRangeParams,
        nodeProperties: Map<String, String>?,
    ) {
        Objects.requireNonNull(params, "params")
        if (nodeProperties == null || nodeProperties.isEmpty()) {
            return
        }
        if (params.properties == null) {
            params.properties = HashMap()
        }
        val out = params.properties
        for ((key, value) in nodeProperties) {
            if (!key.startsWith(PROP_LOCATION_PREFIX)) {
                continue
            }
            val stripped = key.substring(PROP_LOCATION_PREFIX.length)
            // The BE's S3ClientFactory::convert_properties_to_s3_conf
            // (be/src/util/s3_util.cpp) reads `AWS_*` keys verbatim — the
            // FE-side S3ObjStorage normaliser does NOT run on the parquet
            // reader path. So we emit both the FE-style ("s3.*") key and the
            // canonical BE-style ("AWS_*") alias. Belt + suspenders: if the
            // user supplies one form, the BE sees the other too.
            out[stripped] = value
            val alias = canonicalAwsAlias(stripped)
            if (alias != null) {
                out.putIfAbsent(alias, value)
            }
        }
    }

    companion object {
        // Plugin-private prefix used to ferry storage credentials through the
        // scan-node-properties → populateScanLevelParams transit. Mirrors the
        // pattern in IcebergScanPlanProvider#PROP_LOCATION_PREFIX; the prefix is
        // stripped before the keys land in TFileScanRangeParams so the BE sees
        // the canonical "s3.*" form it normalises in S3ObjStorage.
        const val PROP_LOCATION_PREFIX: String = "ducklake.location."

        // PluginDrivenScanNode reads this key out of getScanNodeProperties() to
        // decide which BE reader to dispatch to (PluginDrivenScanNode.java
        // mapFileFormatType: "parquet" → FORMAT_PARQUET, default FORMAT_JNI).
        // ConnectorScanRange#getFileFormat() is NOT consumed there — the engine
        // never reads it directly for the plugin-driven path.
        const val PROP_FILE_FORMAT_TYPE: String = "file_format_type"

        // DuckLake's writer always produces parquet. The catalog records the
        // format per data file (DucklakeDataFile.fileFormat) and our scan
        // plan validates it matches, but for the scan-node-level reader
        // dispatch one shared answer is sufficient.
        const val PARQUET_FORMAT: String = "parquet"

        /**
         * Maps an FE-form storage key to its BE-canonical `AWS_*` alias,
         * or `null` when the key has no alias. Mirrors the lookups in
         * `be/src/util/s3_util.cpp` constants:
         * ```
         *   AWS_ACCESS_KEY / AWS_SECRET_KEY / AWS_ENDPOINT / AWS_REGION / AWS_TOKEN
         * ```
         */
        @JvmStatic
        fun canonicalAwsAlias(unprefixedKey: String): String? =
            when (unprefixedKey) {
                "s3.access_key" -> "AWS_ACCESS_KEY"
                "s3.secret_key" -> "AWS_SECRET_KEY"
                "s3.endpoint" -> "AWS_ENDPOINT"
                "s3.region" -> "AWS_REGION"
                "s3.session_token" -> "AWS_TOKEN"
                else -> null
            }

        /**
         * Recognises the storage-credential keys our plugin forwards to the BE.
         * Covers Doris's primary FE-form (`s3.*`), the canonical
         * `AWS_*` aliases, the legacy `aws.*` form, and the
         * path-style toggle. Engine-injected DuckLake-specific keys
         * (`metadata.*`, `storage.warehouse`, `type`,
         * `enable.mapping.varbinary`) are deliberately excluded — they're
         * either FE-only or carry no meaning on the BE.
         */
        @JvmStatic
        fun isStorageProperty(key: String): Boolean =
            key.startsWith("s3.") ||
                key.startsWith("AWS_") ||
                key.startsWith("aws.") ||
                key.startsWith("fs.") ||
                key == "use_path_style"

        @JvmStatic
        private fun normalizeFileFormat(catalogFormat: String?): String =
            // The catalog stores the format as-recorded by the writer
            // ("parquet" / "PARQUET"). Doris's BE reader dispatch is case-sensitive
            // on the file_format string in the thrift range descriptor.
            if (catalogFormat == null) {
                "parquet"
            } else {
                catalogFormat.lowercase(java.util.Locale.getDefault())
            }
    }
}
