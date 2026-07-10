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
import dev.brikk.ducklake.catalog.DucklakeFilePartitionValue
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.PartialMergedFile
import dev.brikk.ducklake.catalog.TransactionConflictException
import io.airlift.log.Logger
import io.airlift.units.DataSize
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.spi.NodeVersion
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT
import io.trino.spi.TrinoException
import io.trino.spi.block.RunLengthEncodedBlock
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.ConnectorSplitSource
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.procedure.Procedure
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.Type
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.VarcharType.VARCHAR
import org.apache.parquet.format.CompressionCodec
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.Optional
import java.util.UUID

/**
 * Implements `CALL <catalog>.system.rewrite_data_files(schema_name, table_name,
 * file_size_threshold => '100MB', target_file_size => '512MB', reclaim_sources_immediately =>
 * false)` — the compaction WRITER (dev-docs/DESIGN-maintenance.md § 7). It reads the live rows of a
 * table's small data files through the connector's **real read path** (so positional/parquet/puffin
 * delete files, `partial_max` snapshot filtering, and schema evolution all apply) and rewrites them
 * into fewer, larger Parquet files.
 *
 * Surface:
 *   - **Partitioned tables**: sources are grouped by partition (a file's `partition_id` + its stored
 *     partition values); each group is compacted independently and the merged files inherit the
 *     group's partition values, so pruning still works. Works for any transform (the stored values
 *     are copied, not recomputed). A partition with fewer than two compactable files is left alone.
 *   - **Size-bounded output**: within a group the merged rows roll over to a new file once
 *     `target_file_size` is reached, so compaction never produces one unbounded file.
 *   - **`reclaim_sources_immediately`**: when true, emit PARTIAL (merge_adjacent) files — each row
 *     tagged with its source file's begin_snapshot via `_ducklake_internal_snapshot_id`, back-dated
 *     so the merged file serves time-travel on its own, and the sources are deleted + scheduled NOW.
 *     Partial mode requires non-partial sources (already-`partial_max` files are skipped). When
 *     false (default), the non-partial / Iceberg-style shape: sources are end-snapshotted and stay
 *     readable for time-travel until `expire_snapshots` reclaims them; already-partial sources may
 *     be folded in too.
 *
 * Source candidates are the **parquet** data files smaller than `file_size_threshold`. Concurrency:
 * a commit that modifies a source between the read and the commit aborts the rewrite non-retryably
 * (see the catalog primitives).
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
                        Procedure.Argument("FILE_SIZE_THRESHOLD", VARCHAR, false, "100MB"),
                        Procedure.Argument("TARGET_FILE_SIZE", VARCHAR, false, "512MB"),
                        Procedure.Argument("RECLAIM_SOURCES_IMMEDIATELY", BOOLEAN, false, false),
                        // Caps SOURCE FILES consumed per invocation (0 = unlimited) so a
                        // large table doesn't rewrite everything in one commit — mirrors
                        // upstream's max_compacted_files (ducklake main 8b8e0491).
                        Procedure.Argument("MAX_COMPACTED_FILES", BIGINT, false, 0L)),
                REWRITE_DATA_FILES.bindTo(this),
                true)

    @Suppress("unused") // invoked via MethodHandle
    fun rewriteDataFiles(
            session: ConnectorSession,
            schemaName: String?,
            tableName: String?,
            fileSizeThreshold: String?,
            targetFileSize: String?,
            reclaimSourcesImmediately: Boolean,
            maxCompactedFiles: Long) {
        val schemaArg = requireArg(schemaName, "schema_name")
        val tableArg = requireArg(tableName, "table_name")
        val threshold: Long = parseDataSize(fileSizeThreshold, "file_size_threshold", "100MB")
        val targetSize: Long = parseDataSize(targetFileSize, "target_file_size", "512MB")

        val snapshotId = catalog.currentSnapshotId
        val (schema, table) = resolveTable(schemaArg, tableArg, snapshotId)
        val tableId = table.tableId

        val candidates: List<DucklakeDataFile> =
                selectCandidates(catalog.getDataFiles(tableId, snapshotId), threshold, reclaimSourcesImmediately)
        if (candidates.size < 2) {
            log.info("rewrite_data_files: %s.%s has %d compactable file(s) below %d bytes — nothing to compact",
                    schemaArg, tableArg, candidates.size, threshold)
            return
        }
        val candidatesByBasename: Map<String, DucklakeDataFile> = candidates.associateBy { basename(it.path) }

        val topLevelColumns: List<DucklakeColumn> = catalog.getTableColumns(tableId, snapshotId)
                .filter { it.parentColumn == null }
        val columnHandles: List<DucklakeColumnHandle> = topLevelColumns.map { col ->
            DucklakeColumnHandle(col.columnId, col.columnName, typeConverter.toTrinoType(col.columnType), col.nullsAllowed)
        }
        val columnTypes: List<Type> = columnHandles.map { it.columnType }
        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(tableId, snapshotId)
        val partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>> =
                if (catalog.getPartitionSpecs(tableId, snapshotId).isEmpty()) emptyMap()
                else catalog.getFilePartitionValues(tableId, snapshotId)

        val tableHandle = DucklakeTableHandle(schemaArg, tableArg, tableId, snapshotId)
        val matchedSplits: List<DucklakeSplit> = collectCandidateSplits(session, tableHandle, candidatesByBasename.keys)
        // Group splits by partition; only compact groups with >= 2 files (a lone file per partition
        // is nothing to merge). Cross-partition files are never merged.
        val uncappedGroups: Map<PartitionGroup, List<DucklakeSplit>> = matchedSplits
                .groupBy { partitionGroupOf(candidatesByBasename[basename(it.dataFilePath)], partitionValuesByFile) }
                .filterValues { it.size >= 2 }
        val groups: Map<PartitionGroup, List<DucklakeSplit>> = capSourceFiles(uncappedGroups, maxCompactedFiles)
        if (groups.isEmpty()) {
            log.info("rewrite_data_files: %s.%s — no partition has >= 2 compactable files; nothing to compact",
                    schemaArg, tableArg)
            return
        }

        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, table)
        val outputs = mutableListOf<MergedOutput>()
        val sourceIds = mutableSetOf<Long>()
        for ((group, splits) in groups) {
            outputs += GroupWriter(fileSystem, tableDataPath, tableHandle, columnHandles, allCatalogColumns,
                    columnTypes, reclaimSourcesImmediately, targetSize, group).write(session, splits)
            sourceIds += splits.mapNotNull { candidatesByBasename[basename(it.dataFilePath)]?.dataFileId }
        }

        val nonEmpty = outputs.filter { it.fragment.recordCount > 0L }
        if (nonEmpty.isEmpty()) {
            log.info("rewrite_data_files: %s.%s merged to 0 live rows — skipping (no file written)", schemaArg, tableArg)
            return
        }
        commitRewrite(tableId, sourceIds, nonEmpty, snapshotId, reclaimSourcesImmediately, schemaArg, tableArg)
    }

    /** Parquet data files below [threshold]; partial mode also requires NON-partial sources. */
    /**
     * Caps the total SOURCE FILES consumed this invocation at [maxFiles] (0 or
     * negative = unlimited). Groups are taken in deterministic order (smallest
     * member path first); the last group is trimmed to the remaining budget when
     * that still leaves >= 2 files to merge, otherwise dropped. Mirrors
     * upstream's `max_compacted_files` on `ducklake_rewrite_data_files`.
     */
    private fun capSourceFiles(
            groups: Map<PartitionGroup, List<DucklakeSplit>>,
            maxFiles: Long): Map<PartitionGroup, List<DucklakeSplit>> {
        if (maxFiles <= 0L) {
            return groups
        }
        val ordered = groups.entries.sortedBy { entry -> entry.value.minOf { it.dataFilePath } }
        val capped = LinkedHashMap<PartitionGroup, List<DucklakeSplit>>()
        var budget = maxFiles
        for ((group, splits) in ordered) {
            if (budget < 2) {
                break
            }
            val take = minOf(budget, splits.size.toLong()).toInt()
            capped[group] = if (take == splits.size) splits else splits.sortedBy { it.dataFilePath }.take(take)
            budget -= take
        }
        return capped
    }

    private fun selectCandidates(dataFiles: List<DucklakeDataFile>, threshold: Long, partial: Boolean): List<DucklakeDataFile> =
        dataFiles.filter { f ->
            FORMAT_PARQUET.equals(f.fileFormat, ignoreCase = true) &&
                f.fileSizeBytes < threshold &&
                (!partial || f.partialMax == null)
        }

    /** Partition group of a candidate file: its `partition_id` + stored partition values (keyed by key index). */
    private fun partitionGroupOf(
            file: DucklakeDataFile?,
            partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>>): PartitionGroup {
        if (file == null) {
            return PartitionGroup(null, emptyMap())
        }
        val values: Map<Int, String?> = partitionValuesByFile[file.dataFileId]
                ?.associate { it.partitionKeyIndex to it.partitionValue }
                ?: emptyMap()
        return PartitionGroup(file.partitionId, values)
    }

    private fun commitRewrite(
            tableId: Long,
            sourceIds: Set<Long>,
            outputs: List<MergedOutput>,
            snapshotId: Long,
            partial: Boolean,
            schemaArg: String,
            tableArg: String) {
        try {
            if (partial) {
                catalog.rewriteDataFilesPartial(tableId, sourceIds,
                        outputs.map { PartialMergedFile(it.fragment, it.beginSnapshot, it.partialMax) }, snapshotId)
            }
            else {
                catalog.rewriteDataFiles(tableId, sourceIds, outputs.map { it.fragment }, snapshotId)
            }
            log.info("rewrite_data_files: %s.%s compacted %d files into %d (%d rows, partial=%b)",
                    schemaArg, tableArg, sourceIds.size, outputs.size, outputs.sumOf { it.fragment.recordCount }, partial)
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

    private fun pageSource(
            session: ConnectorSession,
            tableHandle: DucklakeTableHandle,
            split: DucklakeSplit,
            columns: List<ColumnHandle>): ConnectorPageSource =
        pageSourceProvider.createPageSource(
                null, session, split, tableHandle, Optional.empty(), columns, DynamicFilter.EMPTY)

    /** Identity of a partition group: a file's `partition_id` + its stored partition values. */
    private data class PartitionGroup(val partitionId: Long?, val partitionValues: Map<Int, String?>)

    /** One output file: the registration fragment + (for partial) the back-date bounds of its rows. */
    private class MergedOutput(val fragment: DucklakeWriteFragment, val beginSnapshot: Long, val partialMax: Long)

    /**
     * Streams one partition group's source rows through the real read path into one or more Parquet
     * files, rolling over to a new file at [targetSize]. Every merged file embeds the row-lineage
     * column `_ducklake_internal_row_id` (field-id 2147483540) holding each surviving row's ORIGINAL
     * rowid — sourced from the lineage-first `$row_id` read column, so recompaction of files that
     * already carry lineage preserves the original ids; upstream compaction always writes this
     * column, and rowids now survive compaction across engines. When [partial], a
     * `_ducklake_internal_snapshot_id` BIGINT column is also written (each row tagged with its
     * source file's begin_snapshot; no field_id — the read path finds it by name) and per-file
     * begin/partial_max bounds are tracked. Both internal columns are appended AFTER the data
     * columns, so catalog stats (leaf targets built over the table columns only) are unaffected.
     */
    private inner class GroupWriter(
            private val fileSystem: TrinoFileSystem,
            private val tableDataPath: String,
            private val tableHandle: DucklakeTableHandle,
            private val columnHandles: List<DucklakeColumnHandle>,
            private val allCatalogColumns: List<DucklakeColumn>,
            private val columnTypes: List<Type>,
            private val partial: Boolean,
            private val targetSize: Long,
            private val group: PartitionGroup) {
        private val outputs = mutableListOf<MergedOutput>()
        private var writer: ParquetFileWriter? = null
        private var minBegin = Long.MAX_VALUE
        private var maxBegin = Long.MIN_VALUE

        fun write(session: ConnectorSession, splits: List<DucklakeSplit>): List<MergedOutput> {
            var success = false
            try {
                // Request the queryable $row_id virtual (LAST) alongside the data columns:
                // it is lineage-first (a source's own embedded lineage wins over
                // rowIdStart+position — recompaction preserves ORIGINAL ids) and injected
                // BEFORE delete filtering, so each surviving row carries its true DuckLake
                // rowid. The merged output embeds these as _ducklake_internal_row_id, the
                // same column upstream compaction always writes — rowids survive compaction
                // across engines.
                val readColumns = columnHandles + VirtualKind.ROW_ID.columnHandle()
                for (split in splits) {
                    pageSource(session, tableHandle, split, readColumns).use { source ->
                        while (!source.isFinished) {
                            val sourcePage = source.nextSourcePage ?: continue
                            addPage(sourcePage.page, split.beginSnapshot)
                        }
                    }
                }
                if (writer != null) {
                    finishCurrent()
                }
                success = true
            }
            finally {
                // On failure abort the open file; finished files in `outputs` were never committed,
                // so they simply become orphans reclaimable by remove_orphan_files.
                if (!success) {
                    writer?.abort()
                }
            }
            return outputs
        }

        private fun addPage(page: Page, sourceBegin: Long) {
            if (writer == null) {
                openNew()
            }
            // Incoming page: data channels…, rowid (last, from the $row_id read column).
            // Output column order: data…, [_ducklake_internal_snapshot_id], _ducklake_internal_row_id.
            val rowIdBlock = page.getBlock(page.channelCount - 1)
            val dataBlocks = Array(page.channelCount - 1) { page.getBlock(it) }
            var outPage = Page(page.positionCount, *dataBlocks)
            if (partial) {
                outPage = outPage.appendColumn(RunLengthEncodedBlock.create(BIGINT, sourceBegin, page.positionCount))
                minBegin = minOf(minBegin, sourceBegin)
                maxBegin = maxOf(maxBegin, sourceBegin)
            }
            writer!!.write(outPage.appendColumn(rowIdBlock))
            if (writer!!.getApproximateWrittenBytes() >= targetSize) {
                finishCurrent()
            }
        }

        private fun openNew() {
            val names = columnHandles.map { it.columnName } +
                    (if (partial) listOf(DucklakeDeleteFileReader.INTERNAL_SNAPSHOT_ID_COLUMN) else emptyList()) +
                    listOf(DucklakePageSink.LINEAGE_COLUMN_NAME)
            // JSON columns are physically UTF-8 VARCHAR in parquet (catalog type stays 'json').
            val types = columnTypes.map { DucklakeJsonSupport.toParquetWriteType(it) } +
                    (if (partial) listOf<Type>(BIGINT) else emptyList()) +
                    listOf<Type>(BIGINT)
            val schemaConverter = ParquetSchemaConverter(types, names, false, false)
            val messageType = DucklakeParquetSchemaBuilder.buildMessageType(
                    columnHandles, allCatalogColumns, schemaConverter.messageType,
                    mapOf(DucklakePageSink.LINEAGE_COLUMN_NAME to
                            DucklakeDeleteFileReader.ROW_ID_PARQUET_FIELD_ID.toLong()))
            val fileName = "ducklake-${UUID.randomUUID()}.parquet"
            val outputStream = fileSystem.newOutputFile(Location.of(tableDataPath).appendPath(fileName)).create()
            val parquetWriter = ParquetWriter(outputStream, messageType, schemaConverter.primitiveTypes, writerOptions,
                    CompressionCodec.ZSTD, trinoVersion, Optional.empty(), Optional.empty())
            writer = ParquetFileWriter(parquetWriter, outputStream, fileName,
                    group.partitionValues, group.partitionId, columnHandles, allCatalogColumns)
            minBegin = Long.MAX_VALUE
            maxBegin = Long.MIN_VALUE
        }

        private fun finishCurrent() {
            val fragment = writer!!.finishAndBuildFragment()
            // begin/partial_max are only meaningful for partial files; for a 0-row file the bounds
            // stay at their sentinels but the caller drops empty fragments before committing.
            outputs.add(MergedOutput(fragment, if (minBegin == Long.MAX_VALUE) 0L else minBegin,
                    if (maxBegin == Long.MIN_VALUE) 0L else maxBegin))
            writer = null
        }
    }

    private fun requireArg(value: String?, name: String): String {
        if (value.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "$name is required")
        }
        return value
    }

    private fun parseDataSize(value: String?, argName: String, default: String): Long {
        val raw = if (value.isNullOrBlank()) default else value
        return try {
            DataSize.valueOf(raw).toBytes()
        }
        catch (e: IllegalArgumentException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid $argName '$raw': ${e.message}", e)
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
                        String::class.java,
                        String::class.java,
                        java.lang.Boolean.TYPE,
                        java.lang.Long.TYPE))
    }
}
