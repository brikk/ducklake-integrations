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
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.TransactionConflictException
import io.airlift.log.Logger
import io.airlift.units.DataSize
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
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.ConnectorSplitSource
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilter
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
 * Implements `CALL <catalog>.system.rewrite_data_files(schema_name, table_name,
 * file_size_threshold => '100MB')` — the non-partial / Iceberg-style compaction WRITER
 * (dev-docs/DESIGN-maintenance.md § 7). It reads the live rows of a table's small data files
 * through the connector's **real read path** (so positional/parquet delete files, `partial_max`
 * snapshot filtering and schema evolution are all applied), writes them into one larger Parquet
 * file, and atomically registers the merged file while end-snapshotting the sources via
 * [DucklakeCatalog.rewriteDataFiles].
 *
 * v1 scope (§ 7.3):
 *   - **Unpartitioned tables only** (a partitioned table is rejected with NOT_SUPPORTED — the merged
 *     rows carry no partition assignment, same gate as flush_inlined_data).
 *   - Source candidates are the **parquet** data files at the current snapshot smaller than
 *     `file_size_threshold` (default 100MB) that are not already cross-snapshot-compacted
 *     (`partial_max` IS NULL). Files with delete files ARE compacted — the deletes are applied to
 *     the merged file's bytes, physically dropping the tombstoned rows.
 *   - If fewer than two candidates qualify, it is a no-op.
 *   - The merged file is NON-partial (no `partial_max`); the retired sources stay readable via
 *     time-travel until `expire_snapshots` → `cleanup_old_files` reclaim them.
 *
 * Concurrency: a concurrent commit that modifies a source between the read and the commit aborts
 * the rewrite non-retryably (the catalog's `rewriteDataFiles` checks the sources are still active
 * AND that no newer delete file landed on them — see its contract).
 */
class DucklakeRewriteDataFilesProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val fileSystemFactory: DucklakeFileSystemFactory,
        private val typeConverter: DucklakeTypeConverter,
        private val pathResolver: DucklakePathResolver,
        private val splitManager: DucklakeSplitManager,
        pageSourceProviderFactory: DucklakePageSourceProviderFactory,
        nodeVersion: NodeVersion,
) : Provider<Procedure> {
    private val trinoVersion: String = nodeVersion.toString()
    private val writerOptions: ParquetWriterOptions = ParquetWriterOptions.builder().build()
    private val pageSourceProvider: DucklakePageSourceProvider = pageSourceProviderFactory.createPageSourceProvider()

    override fun get(): Procedure =
        Procedure(
                "system",
                "rewrite_data_files",
                ImmutableList.of(
                        Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        Procedure.Argument("TABLE_NAME", VARCHAR),
                        Procedure.Argument("FILE_SIZE_THRESHOLD", VARCHAR, false, "100MB")),
                REWRITE_DATA_FILES.bindTo(this),
                true)

    @Suppress("unused") // invoked via MethodHandle
    fun rewriteDataFiles(session: ConnectorSession, schemaName: String?, tableName: String?, fileSizeThreshold: String?) {
        val schemaArg = requireArg(schemaName, "schema_name")
        val tableArg = requireArg(tableName, "table_name")
        val threshold: Long = parseThreshold(fileSizeThreshold)

        val snapshotId = catalog.currentSnapshotId
        val (schema, table) = resolveTable(schemaArg, tableArg, snapshotId)
        val tableId = table.tableId

        // v1: a partitioned table's merged rows have no partition assignment to write into a
        // hive-style file; gate (same constraint as flush_inlined_data).
        if (catalog.getPartitionSpecs(tableId, snapshotId).isNotEmpty()) {
            throw TrinoException(NOT_SUPPORTED,
                    "rewrite_data_files does not support partitioned tables yet: $schemaArg.$tableArg")
        }

        val dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(tableId, snapshotId)
        val candidates: List<DucklakeDataFile> = dataFiles.filter { f ->
            FORMAT_PARQUET.equals(f.fileFormat, ignoreCase = true) &&
                f.partialMax == null &&
                f.fileSizeBytes < threshold
        }
        if (candidates.size < 2) {
            log.info("rewrite_data_files: %s.%s has %d compactable file(s) below %d bytes — nothing to compact",
                    schemaArg, tableArg, candidates.size, threshold)
            return
        }
        // Filenames are UUIDs, so the basename uniquely keys a data file to its split.
        val candidatesByBasename: Map<String, DucklakeDataFile> = candidates.associateBy { basename(it.path) }

        val topLevelColumns: List<DucklakeColumn> = catalog.getTableColumns(tableId, snapshotId)
                .filter { it.parentColumn == null }
        val columnHandles: List<DucklakeColumnHandle> = topLevelColumns.map { col ->
            DucklakeColumnHandle(col.columnId, col.columnName, typeConverter.toTrinoType(col.columnType), col.nullsAllowed)
        }
        val columnTypes: List<Type> = columnHandles.map { it.columnType }
        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(tableId, snapshotId)

        val tableHandle = DucklakeTableHandle(schemaArg, tableArg, tableId, snapshotId)
        val matchedSplits: List<DucklakeSplit> = collectCandidateSplits(session, tableHandle, candidatesByBasename.keys)
        if (matchedSplits.size < 2) {
            log.info("rewrite_data_files: %s.%s resolved %d candidate split(s) — nothing to compact",
                    schemaArg, tableArg, matchedSplits.size)
            return
        }

        val sourceIds: Set<Long> = matchedSplits.mapNotNull { candidatesByBasename[basename(it.dataFilePath)]?.dataFileId }.toSet()

        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, table)
        val fragment: DucklakeWriteFragment = writeMergedParquetFile(
                session, fileSystem, tableDataPath, tableHandle, matchedSplits,
                columnHandles, allCatalogColumns, columnTypes)

        if (fragment.recordCount == 0L) {
            // Every source row was deleted — registering an empty file is pointless; just retire the
            // sources (they hold no live rows). Use an empty-source guard by skipping: a 0-row merge
            // means the live set is empty, so leave it to expire/cleanup via a normal DELETE path.
            log.info("rewrite_data_files: %s.%s merged to 0 live rows — skipping (no file written)", schemaArg, tableArg)
            return
        }

        try {
            catalog.rewriteDataFiles(tableId, sourceIds, ImmutableList.of(fragment), snapshotId)
            log.info("rewrite_data_files: %s.%s compacted %d files into 1 (%d rows)",
                    schemaArg, tableArg, sourceIds.size, fragment.recordCount)
        }
        catch (e: TransactionConflictException) {
            throw TrinoException(TRANSACTION_CONFLICT, e.message, e)
        }
    }

    /** Drain the table's splits and keep the ones whose file matches a compaction candidate. */
    private fun collectCandidateSplits(
            session: ConnectorSession,
            tableHandle: DucklakeTableHandle,
            candidateBasenames: Set<String>): List<DucklakeSplit> {
        val matched: MutableList<DucklakeSplit> = mutableListOf()
        val source: ConnectorSplitSource = splitManager.getSplits(
                null, session, tableHandle, DynamicFilter.EMPTY, Constraint.alwaysTrue())
        try {
            while (!source.isFinished) {
                val batch = source.getNextBatch(SPLIT_BATCH_SIZE).get()
                for (split: ConnectorSplit in batch.splits) {
                    if (split is DucklakeSplit && basename(split.dataFilePath) in candidateBasenames) {
                        matched.add(split)
                    }
                }
            }
        }
        finally {
            source.close()
        }
        return matched
    }

    /** Read the live rows of [splits] through the real read path and write them into one Parquet file. */
    private fun writeMergedParquetFile(
            session: ConnectorSession,
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            tableHandle: DucklakeTableHandle,
            splits: List<DucklakeSplit>,
            columnHandles: List<DucklakeColumnHandle>,
            allCatalogColumns: List<DucklakeColumn>,
            columnTypes: List<Type>): DucklakeWriteFragment {
        val columnNames: List<String> = columnHandles.map { it.columnName }
        val schemaConverter = ParquetSchemaConverter(columnTypes, columnNames, false, false)
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
        val writer = ParquetFileWriter(
                parquetWriter, outputStream, fileName, emptyMap(), null, columnHandles, allCatalogColumns)

        val readColumns: List<ColumnHandle> = columnHandles
        var fragment: DucklakeWriteFragment? = null
        try {
            for (split in splits) {
                pageSource(session, tableHandle, split, readColumns).use { source ->
                    while (!source.isFinished) {
                        val sourcePage = source.nextSourcePage ?: continue
                        writer.write(sourcePage.page)
                    }
                }
            }
            fragment = writer.finishAndBuildFragment()
        }
        finally {
            if (fragment == null) {
                writer.abort()
            }
        }
        return fragment
    }

    private fun pageSource(
            session: ConnectorSession,
            tableHandle: DucklakeTableHandle,
            split: DucklakeSplit,
            columns: List<ColumnHandle>): ConnectorPageSource =
        pageSourceProvider.createPageSource(
                null, session, split, tableHandle, Optional.empty(), columns, DynamicFilter.EMPTY)

    private fun requireArg(value: String?, name: String): String {
        if (value.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "$name is required")
        }
        return value
    }

    private fun parseThreshold(value: String?): Long {
        val raw = if (value.isNullOrBlank()) "100MB" else value
        return try {
            DataSize.valueOf(raw).toBytes()
        }
        catch (e: IllegalArgumentException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid file_size_threshold '$raw': ${e.message}", e)
        }
    }

    private fun resolveTable(schemaName: String, tableName: String, snapshotId: Long): Pair<DucklakeSchema, DucklakeTable> {
        val schema: DucklakeSchema = catalog.getSchema(schemaName, snapshotId)
            ?: throw TrinoException(NOT_SUPPORTED, "Schema not found: $schemaName")
        val table: DucklakeTable = catalog.getTable(schemaName, tableName, snapshotId)
            ?: throw TrinoException(NOT_SUPPORTED, "Table not found: $schemaName.$tableName")
        return schema to table
    }

    private fun basename(path: String): String = path.replace('\\', '/').substringAfterLast('/')

    companion object {
        private val log: Logger = Logger.get(DucklakeRewriteDataFilesProcedure::class.java)
        private const val FORMAT_PARQUET: String = "parquet"
        private const val SPLIT_BATCH_SIZE: Int = 1000

        private val REWRITE_DATA_FILES: MethodHandle = MethodHandles.lookup().findVirtual(
                DucklakeRewriteDataFilesProcedure::class.java,
                "rewriteDataFiles",
                MethodType.methodType(
                        Void.TYPE,
                        ConnectorSession::class.java,
                        String::class.java,
                        String::class.java,
                        String::class.java))
    }
}
