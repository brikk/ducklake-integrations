package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalog
import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.handle.ConnectorWriteHandle
import org.apache.doris.connector.api.write.ConnectorSinkPlan
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider
import org.apache.doris.thrift.TDataSink
import org.apache.doris.thrift.TDataSinkType
import org.apache.doris.thrift.TFileCompressType
import org.apache.doris.thrift.TFileFormatType
import org.apache.doris.thrift.TFileType
import org.apache.doris.thrift.TIcebergTableSink
import org.apache.iceberg.SchemaParser

/**
 * Builds the BE write sink for a DuckLake INSERT. Mirrors Doris's native
 * `IcebergTableSink` (`fe/.../planner/IcebergTableSink.java`): we emit a
 * **`TIcebergTableSink`** so the BE's Iceberg file-writer writes Parquet *and*
 * reports per-field-id column stats in `TIcebergCommitData` (the Hive sink
 * doesn't). The connector then commits those via [DuckLakeConnectorTransaction].
 *
 * `schema_json` is built from DuckLake columns ([DuckLakeIcebergSchema], with
 * `field_id == column_id`) rather than an iceberg `Table` (we have none). The
 * target table is bound onto the current transaction here, so its `commit()`
 * knows where to write — same seam as `MaxComputeWritePlanProvider`.
 *
 * **End-to-end validation is the compose smoke** (real FE+BE), not a headless
 * test: `output_path`/`hadoop_config`/`file_type` formats are BE-defined and have
 * no headless oracle. Unpartitioned, full-row INSERT for v1 (no
 * `partition_specs_json`; partial-column INSERT uses the full table schema).
 */
internal class DuckLakeWritePlanProvider(
    private val catalog: DucklakeCatalog,
    private val pathResolver: DuckLakePathResolver,
    private val properties: Map<String, String>,
) : ConnectorWritePlanProvider {

    override fun planWrite(session: ConnectorSession, handle: ConnectorWriteHandle): ConnectorSinkPlan {
        val tableHandle = handle.tableHandle.asDuckLakeHandle<DuckLakeTableHandle>()
        val outputPath = resolveOutputPath(tableHandle)
        bindTransaction(session, tableHandle, outputPath)
        val sink = buildSink(tableHandle, outputPath, handle.isOverwrite)
        return ConnectorSinkPlan(TDataSink(TDataSinkType.ICEBERG_TABLE_SINK).setIcebergTableSink(sink))
    }

    /**
     * Bind the resolved target + its data dir onto the transaction the engine
     * opened via `beginTransaction`. The data dir relativizes the BE's absolute
     * file paths when the transaction commits.
     */
    private fun bindTransaction(session: ConnectorSession, handle: DuckLakeTableHandle, tableDataDir: String) {
        val transaction = session.currentTransaction.orElseThrow {
            DorisConnectorException("DuckLake INSERT requires an active connector transaction on the session")
        }
        val duckLakeTransaction = transaction as? DuckLakeConnectorTransaction
            ?: throw DorisConnectorException("unexpected transaction type: ${transaction.javaClass.name}")
        duckLakeTransaction.bindTarget(handle.tableId, handle.snapshotId, tableDataDir)
    }

    private fun buildSink(handle: DuckLakeTableHandle, outputPath: String, overwrite: Boolean): TIcebergTableSink {
        val columns = catalog.getTableColumns(handle.tableId, handle.snapshotId)
        val schemaJson = SchemaParser.toJson(DuckLakeIcebergSchema.of(columns))
        return TIcebergTableSink().apply {
            setDbName(handle.database)
            setTbName(handle.table)
            setSchemaJson(schemaJson)
            setFileFormat(TFileFormatType.FORMAT_PARQUET)
            // Without a compression type the BE rejects the write ("Unsupported
            // compress type UNKNOWN with parquet"). ZSTD is standard + DuckLake-readable.
            setCompressionType(TFileCompressType.ZSTD)
            setOutputPath(outputPath)
            setOriginalOutputPath(outputPath)
            setFileType(fileTypeFor(outputPath))
            setHadoopConfig(storageConfig())
            setOverwrite(overwrite)
        }
    }

    private fun resolveOutputPath(handle: DuckLakeTableHandle): String {
        val schema = catalog.getSchema(handle.database, handle.snapshotId)
            ?: throw DorisConnectorException("schema not found: ${handle.database}")
        val table = catalog.getTable(handle.database, handle.table, handle.snapshotId)
            ?: throw DorisConnectorException("table not found: ${handle.database}.${handle.table}")
        return pathResolver.resolveTableDataPath(schema, table)
    }

    /** Storage credentials the BE writer needs — raw FE keys + BE-canonical `AWS_*` aliases (read-path parity). */
    private fun storageConfig(): Map<String, String> {
        val config = LinkedHashMap<String, String>()
        for ((key, value) in properties) {
            if (DuckLakeScanPlanProvider.isStorageProperty(key)) {
                config[key] = value
                DuckLakeScanPlanProvider.canonicalAwsAlias(key)?.let { alias -> config[alias] = value }
            }
        }
        return config
    }

    private fun fileTypeFor(path: String): TFileType = when {
        path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://") -> TFileType.FILE_S3
        path.startsWith("hdfs://") -> TFileType.FILE_HDFS
        else -> TFileType.FILE_LOCAL
    }
}
