/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import com.google.inject.Provider
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.TransactionConflictException
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.spi.NodeVersion
import io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.InMemoryRecordSet
import io.trino.spi.connector.RecordPageSource
import io.trino.spi.procedure.Procedure
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType.VARCHAR
import org.apache.parquet.format.CompressionCodec
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.Optional
import java.util.UUID

/**
 * Implements `CALL <catalog>.system.flush_inlined_data(schema_name, table_name)` — materializes
 * a table's INLINED rows (rows DuckLake stores directly in `ducklake_inlined_data_<tableId>_<sv>`
 * metadata tables, written cross-engine by DuckDB under `data_inlining_row_limit`) into a real
 * Parquet data file, then end-snapshots the inlined rows — atomically. Row count and values are
 * preserved; the rows just move from catalog-resident to file-resident.
 *
 * Why it matters: Trino's merge sink can only tombstone file-resident rows, so DELETE/UPDATE/MERGE
 * are gated while a table has inlined rows ([DucklakeMetadata.beginMerge]). Flushing first makes
 * those operations work. Mirrors upstream DuckLake's `flush_inlined_data`.
 *
 * v1 scope: writes a single Parquet file (the table may hold other formats — mixed is fine) and
 * does NOT support partitioned tables (the inlined rows carry no partition assignment; gated with
 * a clear error). Schema evolution across inlined versions is handled — each version's rows are
 * read projected onto the current columns, NULL-filling columns added later (see
 * `DucklakeCatalog.readInlinedData`). The whole read-then-write is conflict-checked at commit
 * (`ConflictMatrix.checkFlushedInlinedData`), so a concurrent change to the table's inlined data
 * aborts rather than duplicating or dropping rows.
 */
class DucklakeFlushInlinedDataProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val fileSystemFactory: DucklakeFileSystemFactory,
        private val typeConverter: DucklakeTypeConverter,
        private val pathResolver: DucklakePathResolver,
        nodeVersion: NodeVersion,
) : Provider<Procedure> {
    private val trinoVersion: String = nodeVersion.toString()
    private val writerOptions: ParquetWriterOptions = ParquetWriterOptions.builder().build()

    override fun get(): Procedure =
        Procedure(
                "system",
                "flush_inlined_data",
                ImmutableList.of(
                        Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        Procedure.Argument("TABLE_NAME", VARCHAR)),
                FLUSH_INLINED_DATA.bindTo(this),
                true)

    @Suppress("unused") // invoked via MethodHandle
    fun flushInlinedData(session: ConnectorSession, schemaName: String?, tableName: String?) {
        val schemaArg = requireArg(schemaName, "schema_name")
        val tableArg = requireArg(tableName, "table_name")

        val snapshotId = catalog.currentSnapshotId
        val (schema, table) = resolveTable(schemaArg, tableArg, snapshotId)
        val tableId = table.tableId

        // v1: a partitioned table's inlined rows have no partition assignment to write into a
        // hive-style file; gate rather than produce an unpartitioned (unprunable) file.
        if (catalog.getPartitionSpecs(tableId, snapshotId).isNotEmpty()) {
            throw TrinoException(NOT_SUPPORTED,
                    "flush_inlined_data does not support partitioned tables yet: $schemaArg.$tableArg")
        }

        val liveInfos: List<DucklakeInlinedDataInfo> = catalog.getInlinedDataInfos(tableId, snapshotId)
                .filter { it.hasLiveRows }
        if (liveInfos.isEmpty()) {
            return // nothing inlined — no-op
        }

        // Top-level columns at the current snapshot; the rows we write conform to these.
        val topLevelColumns: List<DucklakeColumn> = catalog.getTableColumns(tableId, snapshotId)
                .filter { it.parentColumn == null }
        val columnHandles: List<DucklakeColumnHandle> = topLevelColumns.map { col ->
            DucklakeColumnHandle(col.columnId, col.columnName, typeConverter.toTrinoType(col.columnType), col.nullsAllowed)
        }
        val columnTypes: List<Type> = columnHandles.map { it.columnType }
        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(tableId, snapshotId)

        // Read + convert every live inlined row across schema versions. readInlinedData resolves
        // by column_id at each version's schema and NULL-fills current columns the version lacks.
        val rows: MutableList<List<Any?>> = mutableListOf()
        for (info in liveInfos) {
            val rawRows: List<List<Any?>> = catalog.readInlinedData(tableId, info.schemaVersion, snapshotId, topLevelColumns)
            for (raw in rawRows) {
                rows.add(raw.indices.map { i -> DucklakeInlinedValueConverter.convertJdbcValue(raw[i], columnTypes[i]) })
            }
        }
        if (rows.isEmpty()) {
            return // descriptor said live, but no rows resolved at this snapshot — nothing to do
        }

        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, table)
        val fragment: DucklakeWriteFragment = writeParquetFile(
                fileSystem, tableDataPath, columnHandles, allCatalogColumns, columnTypes, rows)

        try {
            catalog.flushInlinedData(tableId, ImmutableList.of(fragment))
        }
        catch (e: TransactionConflictException) {
            throw TrinoException(TRANSACTION_CONFLICT, e.message, e)
        }
    }

    private fun requireArg(value: String?, name: String): String {
        if (value.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "$name is required")
        }
        return value
    }

    private fun resolveTable(schemaName: String, tableName: String, snapshotId: Long): Pair<DucklakeSchema, DucklakeTable> {
        val schema: DucklakeSchema = catalog.getSchema(schemaName, snapshotId)
            ?: throw TrinoException(NOT_SUPPORTED, "Schema not found: $schemaName")
        val table: DucklakeTable = catalog.getTable(schemaName, tableName, snapshotId)
            ?: throw TrinoException(NOT_SUPPORTED, "Table not found: $schemaName.$tableName")
        return schema to table
    }

    /** Materialize [rows] into one Parquet data file and return its registration fragment. */
    private fun writeParquetFile(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            columnHandles: List<DucklakeColumnHandle>,
            allCatalogColumns: List<DucklakeColumn>,
            columnTypes: List<Type>,
            rows: List<List<Any?>>): DucklakeWriteFragment {
        val columnNames: List<String> = columnHandles.map { it.columnName }
        // JSON columns are physically UTF-8 VARCHAR in parquet (catalog type stays 'json').
        val schemaConverter = ParquetSchemaConverter(
                columnTypes.map { DucklakeJsonSupport.toParquetWriteType(it) }, columnNames, false, false)
        val messageType = DucklakeParquetSchemaBuilder.buildMessageType(
                columnHandles, allCatalogColumns, schemaConverter.messageType)

        val fileName = "ducklake-${UUID.randomUUID()}.parquet"
        val filePath: Location = Location.of(tableDataPath).appendPath(fileName)
        val outputStream = fileSystem.newOutputFile(filePath).create()

        val parquetWriter = ParquetWriter(
                outputStream,
                messageType,
                schemaConverter.primitiveTypes,
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                Optional.empty(),
                Optional.empty())
        // Single unpartitioned file (partitioned tables are gated above).
        val writer = ParquetFileWriter(
                parquetWriter, outputStream, fileName, emptyMap(), null, columnHandles, allCatalogColumns)

        var fragment: DucklakeWriteFragment? = null
        try {
            // Reuse the in-memory record-set → page path (the same machinery the inlined READ
            // path uses) to turn rows into Pages, then stream them through the Parquet writer.
            RecordPageSource(InMemoryRecordSet(columnTypes, rows)).use { source ->
                while (!source.isFinished) {
                    val sourcePage = source.nextSourcePage ?: continue
                    writer.write(sourcePage.page)
                }
            }
            fragment = writer.finishAndBuildFragment()
        }
        finally {
            // On any failure (fragment still null) abort to release the writer + output stream;
            // a finally avoids a generic catch and never masks the original throwable.
            if (fragment == null) {
                writer.abort()
            }
        }
        return fragment
    }

    companion object {
        private val FLUSH_INLINED_DATA: MethodHandle

        init {
            try {
                FLUSH_INLINED_DATA = MethodHandles.lookup().findVirtual(
                        DucklakeFlushInlinedDataProcedure::class.java,
                        "flushInlinedData",
                        MethodType.methodType(
                                Void.TYPE,
                                ConnectorSession::class.java,
                                String::class.java,
                                String::class.java))
            }
            catch (e: ReflectiveOperationException) {
                throw AssertionError(e)
            }
        }
    }
}
