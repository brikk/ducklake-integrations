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
import com.google.common.collect.ImmutableList.toImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableMap.toImmutableMap
import com.google.inject.Inject
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeChangeFeedDeletion
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import dev.brikk.ducklake.catalog.DucklakeSnapshotChange
import dev.brikk.ducklake.catalog.DucklakeTable
import io.airlift.log.Logger
import io.airlift.slice.Slices
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoFileSystemFactory
import io.trino.filesystem.TrinoInputFile
import io.trino.memory.context.AggregatedMemoryContext
import io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext
import io.trino.parquet.Column
import io.trino.parquet.Field
import io.trino.parquet.ParquetDataSource
import io.trino.parquet.ParquetDataSourceId
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.ParquetTypeUtils.getColumnIO
import io.trino.parquet.ParquetTypeUtils.getDescriptors
import io.trino.parquet.metadata.FileMetadata
import io.trino.parquet.metadata.ParquetMetadata
import io.trino.parquet.predicate.PredicateUtils.buildPredicate
import io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups
import io.trino.parquet.predicate.TupleDomainParquetPredicate
import io.trino.parquet.reader.MetadataReader
import io.trino.parquet.reader.ParquetReader
import io.trino.parquet.reader.RowGroupInfo
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.TransformConnectorPageSource
import io.trino.plugin.hive.parquet.ParquetPageSource
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.block.RunLengthEncodedBlock
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorPageSourceProvider
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.ConnectorTableCredentials
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.connector.EmptyPageSource
import io.trino.spi.connector.InMemoryRecordSet
import io.trino.spi.connector.RecordPageSource
import io.trino.spi.connector.SourcePage
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RowType
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.Type
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.ColumnIO
import org.apache.parquet.io.MessageColumnIO
import org.apache.parquet.schema.MessageType
import org.joda.time.DateTimeZone.UTC
import java.io.IOException
import java.nio.file.Path
import java.time.Instant
import java.util.ArrayList
import java.util.Locale
import java.util.Optional
import java.util.OptionalLong
import java.util.function.Function

/**
 * PageSourceProvider for Ducklake connector.
 * Leverages Trino's ParquetPageSource for all Parquet reading logic.
 */
class DucklakePageSourceProvider @Inject constructor(
        private val fileSystemFactory: TrinoFileSystemFactory,
        private val fileFormatDataSourceStats: FileFormatDataSourceStats,
        private val parquetReaderOptions: ParquetReaderOptions,
        private val catalog: DucklakeCatalog,
        private val duckDbReadCache: DucklakeMaterializedFileCache,
        private val duckDbS3Config: DuckDbS3Config,
        ducklakeConfig: DucklakeConfig,
        private val executorFactory: DucklakeDuckDbExecutorFactory)
        : ConnectorPageSourceProvider
{
    private val autoHttpfsThresholdBytes: Long = ducklakeConfig
            .getDuckdbAutoHttpfsThreshold().toBytes()

    // Resolves relative catalog file paths (data/delete files) to full paths for the change feed,
    // which reads files the way the split manager does. Built from the same inputs the split
    // manager's injected resolver uses; the provider isn't handed one directly.
    private val pathResolver: DucklakePathResolver = DucklakePathResolver(catalog, ducklakeConfig)

    // Schema-evolution resolution cache: (tableId, file begin_snapshot) -> column_id ->
    // physical name in the file. The DuckDB-engine read path (.db / vortex / lance) reads
    // files by their physical column names, which are the names the columns had when the file
    // was WRITTEN — so a column renamed or added after the file means the current name doesn't
    // exist in the file and the read errors. We resolve the file's write-snapshot column names
    // from the catalog so renames alias and added columns project NULL (DuckDbSelectSqlBuilder).
    // Bounded + cleared wholesale on overflow; schema-evolution reads are rare so it stays tiny.
    private val fileColumnNamesCache: java.util.concurrent.ConcurrentMap<FileColumnNamesKey, Map<Long, String>> =
            java.util.concurrent.ConcurrentHashMap()

    // Full column tree (incl. nested rows, with parent_column) per (tableId, snapshot), for building
    // nested struct reshape plans. Same bounded/cleared-on-overflow policy as fileColumnNamesCache.
    private val columnTreeCache: java.util.concurrent.ConcurrentMap<FileColumnNamesKey, List<DucklakeColumn>> =
            java.util.concurrent.ConcurrentHashMap()

    private data class FileColumnNamesKey(val tableId: Long, val beginSnapshot: Long)

    /**
     * Resolve `column_id -> physical name in the file` for a DuckDB-engine data split, by
     * reading the table's column set as of the file's begin_snapshot. Empty when the table
     * handle or begin_snapshot is unavailable (test splits) — the SQL builder then projects
     * current names directly (the no-evolution fast path).
     */
    private fun resolveFileColumnNames(table: ConnectorTableHandle, split: DucklakeSplit): Map<Long, String> {
        if (table !is DucklakeTableHandle || split.beginSnapshot <= 0L) {
            return emptyMap()
        }
        val key = FileColumnNamesKey(table.tableId, split.beginSnapshot)
        fileColumnNamesCache[key]?.let { return it }
        if (fileColumnNamesCache.size >= MAX_FILE_COLUMN_NAME_CACHE_ENTRIES) {
            fileColumnNamesCache.clear()
        }
        val resolved: Map<Long, String> = catalog.getTableColumns(table.tableId, split.beginSnapshot)
                .filter { it.parentColumn == null }
                .associate { it.columnId to it.columnName }
        fileColumnNamesCache[key] = resolved
        return resolved
    }

    /**
     * Build per-file struct reshape plans for nested schema evolution: when a projected struct's
     * shape in this file differs from the current schema (a subfield was added / dropped / renamed
     * since the file was written), the SQL builder normalizes it with `struct_pack`. Empty unless a
     * struct is projected AND its file shape actually drifted. See [NestedFieldReshapePlanner].
     */
    private fun resolveStructReshapePlans(
            table: ConnectorTableHandle,
            split: DucklakeSplit,
            projectedColumns: List<DucklakeColumnHandle>): Map<Long, List<StructFieldPlan>> {
        if (table !is DucklakeTableHandle || split.beginSnapshot <= 0L || split.beginSnapshot == table.snapshotId) {
            return emptyMap()
        }
        // Only structs can drift; skip the catalog reads entirely when none are projected.
        if (projectedColumns.none { it.columnType is RowType }) {
            return emptyMap()
        }
        val currentColumns: List<DucklakeColumn> = columnTree(table.tableId, table.snapshotId)
        val fileColumns: List<DucklakeColumn> = columnTree(table.tableId, split.beginSnapshot)
        return NestedFieldReshapePlanner.buildPlans(projectedColumns, currentColumns, fileColumns)
    }

    private fun columnTree(tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        val key = FileColumnNamesKey(tableId, snapshotId)
        columnTreeCache[key]?.let { return it }
        if (columnTreeCache.size >= MAX_FILE_COLUMN_NAME_CACHE_ENTRIES) {
            columnTreeCache.clear()
        }
        val tree: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(tableId, snapshotId)
        columnTreeCache[key] = tree
        return tree
    }

    override fun createPageSource(
            transaction: ConnectorTransactionHandle?,
            session: ConnectorSession,
            split: ConnectorSplit,
            table: ConnectorTableHandle,
            tableCredentials: Optional<ConnectorTableCredentials>,
            columns: List<ColumnHandle>,
            dynamicFilter: DynamicFilter): ConnectorPageSource
    {
        specialSplitPageSource(session, split, table, columns, dynamicFilter)?.let { return it }

        val ducklakeSplit = split as DucklakeSplit

        // Combine file statistics domain with dynamic filter for effective predicate
        val dynamicFilterPredicate: TupleDomain<DucklakeColumnHandle> = dynamicFilter.currentPredicate
                .transformKeys(DucklakeColumnHandle::class.java::cast)
        val effectivePredicate: TupleDomain<DucklakeColumnHandle> = ducklakeSplit.fileStatisticsDomain
                .intersect(dynamicFilterPredicate)

        if (effectivePredicate.isNone) {
            return EmptyPageSource()
        }

        // Extract column information
        val ducklakeColumns: List<DucklakeColumnHandle> = columns.stream()
                .map(DucklakeColumnHandle::class.java::cast)
                .collect(toImmutableList())

        // Split the hidden virtuals: CONSTANT virtuals ($path, $snapshot_id) are injected by the
        // outer wrapper below; POSITIONAL virtuals ($file_row_number, $row_id) stay in the column
        // list passed to the readers so they are injected in-pipeline before delete filtering
        // (alongside the MERGE $row_id handle, which is not a VirtualKind but is positional too).
        val sourceColumns: List<DucklakeColumnHandle> = ducklakeColumns.filterNot { col ->
            val kind: VirtualKind? = col.virtualKind()
            kind != null && !kind.perRow
        }

        log.debug("Creating page source for file: %s", ducklakeSplit.dataFilePath)

        try {
            // Get file system for the session
            val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
            val delegate: ConnectorPageSource = createDataFilePageSource(
                    session, ducklakeSplit, table, sourceColumns, effectivePredicate, fileSystem)
            return injectConstantVirtuals(delegate, ducklakeColumns, { !it.perRow }) { kind -> dataFileVirtualBlock(kind, ducklakeSplit) }
        }
        catch (e: IOException) {
            throw RuntimeException("Failed to create page source for file: ${ducklakeSplit.dataFilePath}", e)
        }
    }

    /**
     * The per-format data-file read dispatch (parquet reader vs the DuckDB-engine file scan for
     * duckdb/vortex/lance), returning the raw delegate WITHOUT the constant-virtual wrapper. Shared
     * by [createPageSource] (which wraps it) and the change feed ([createChangeFeedPageSource]),
     * which drives it per data file requesting the table columns + `$row_id`. Positional virtuals
     * ($row_id / $file_row_number) requested in [sourceColumns] are still injected in-pipeline by
     * the underlying readers; the split's own delete files (if any) are applied there too.
     */
    @Throws(IOException::class)
    private fun createDataFilePageSource(
            session: ConnectorSession,
            ducklakeSplit: DucklakeSplit,
            table: ConnectorTableHandle,
            sourceColumns: List<DucklakeColumnHandle>,
            effectivePredicate: TupleDomain<DucklakeColumnHandle>,
            fileSystem: TrinoFileSystem): ConnectorPageSource
    {
        // Resolve the data file location. NOTE: do NOT open a TrinoInputFile here — only the
        // parquet branch needs one. Lance data files are *directories* (the catalog path ends
        // in a trailing slash for an existing dir), and fileSystem.newInputFile rejects a
        // directory/trailing-slash location; the DuckDB-engine branch reads via the path string
        // (`__lance_scan('<dir>')`), not a TrinoInputFile, so opening one is both unnecessary
        // and fatal for lance.
        val dataFileLocation: Location = toLocation(ducklakeSplit.dataFilePath)
        val format = ducklakeSplit.fileFormat
        if (DucklakeSessionProperties.FORMAT_PARQUET.equals(format, ignoreCase = true)) {
            val inputFile: TrinoInputFile = fileSystem.newInputFile(dataFileLocation)
            return createParquetPageSource(inputFile, sourceColumns, ducklakeSplit, effectivePredicate, fileSystem)
        }
        // The DuckDB engine handles both the .db ATTACH path and the file-scan formats
        // (vortex + lance). createDuckDbPageSource picks the source shape per
        // split.fileFormat via resolveDuckDbReadTarget.
        if (DucklakeSessionProperties.FORMAT_DUCKDB.equals(format, ignoreCase = true) ||
                DucklakeSessionProperties.FORMAT_VORTEX.equals(format, ignoreCase = true) ||
                DucklakeSessionProperties.FORMAT_LANCE.equals(format, ignoreCase = true)) {
            val pushedExpressions: List<String> = if (table is DucklakeTableHandle)
                table.pushedExpressions
            else
                emptyList()
            // Resolve physical (file-snapshot) column names so the DuckDB-engine read
            // survives schema evolution (renamed/added columns) — see DuckDbSelectSqlBuilder.
            val fileColumnNamesById: Map<Long, String> = resolveFileColumnNames(table, ducklakeSplit)
            // ...and per-file struct reshape plans for NESTED schema evolution (added/dropped/
            // renamed struct subfields), which the SQL builder normalizes with struct_pack.
            val structReshapePlans: Map<Long, List<StructFieldPlan>> =
                    resolveStructReshapePlans(table, ducklakeSplit, sourceColumns)
            return createDuckDbPageSource(
                    dataFileLocation,
                    sourceColumns,
                    ducklakeSplit,
                    effectivePredicate,
                    pushedExpressions,
                    fileSystem,
                    session,
                    fileColumnNamesById,
                    structReshapePlans)
        }
        throw TrinoException(NOT_SUPPORTED, "Unsupported file format: $format")
    }

    private fun createInlinedPageSource(
            inlinedSplit: DucklakeInlinedSplit,
            columns: List<ColumnHandle>): ConnectorPageSource
    {
        val ducklakeColumns: List<DucklakeColumnHandle> = columns.stream()
                .map(DucklakeColumnHandle::class.java::cast)
                .collect(toImmutableList())

        // Real (catalog-resident) columns are read from the inlined table; virtuals are woven in
        // per row below. Inlined data has no backing file, so $path / $file_row_number / $row_id
        // are NULL; $snapshot_id is each row's own begin_snapshot (inlined rows are versioned
        // catalog rows, each with its own begin_snapshot — so this is genuinely per-row).
        val realColumns: List<DucklakeColumnHandle> = ducklakeColumns.filterNot { it.isVirtual() }
        val needSnapshot: Boolean = ducklakeColumns.any { it.virtualKind() == VirtualKind.SNAPSHOT_ID }

        val beginSnapshots: List<Long>? = if (needSnapshot)
                catalog.readInlinedBeginSnapshots(inlinedSplit.tableId, inlinedSplit.schemaVersion, inlinedSplit.snapshotId)
            else null

        // Read the real-column values (if any) and establish the row count.
        val tableColumns: List<DucklakeColumn> = catalog.getTableColumns(
                inlinedSplit.tableId, inlinedSplit.snapshotId)
        val columnById: Map<Long, DucklakeColumn> = tableColumns.stream()
                .collect(toImmutableMap(DucklakeColumn::columnId) { col -> col })
        val realQueryColumns: List<DucklakeColumn> = realColumns.map { handle ->
                columnById[handle.columnId]
                        ?: throw IllegalStateException("Column not found in table metadata: ${handle.columnName}")
        }

        val convertedRealRows: List<List<Any?>>
        val rowCount: Int
        if (realQueryColumns.isNotEmpty()) {
            val rawRows: List<List<Any?>> = catalog.readInlinedData(
                    inlinedSplit.tableId, inlinedSplit.schemaVersion, inlinedSplit.snapshotId, realQueryColumns)
            val realTypes: List<Type> = realColumns.map { it.columnType }
            convertedRealRows = rawRows.map { row ->
                val converted: MutableList<Any?> = ArrayList(row.size)
                for (i in row.indices) {
                    converted.add(DucklakeInlinedValueConverter.convertJdbcValue(row[i], realTypes[i]))
                }
                converted
            }
            rowCount = convertedRealRows.size
        }
        else if (beginSnapshots != null) {
            // No real columns, but $snapshot_id was requested → its row count + values suffice.
            convertedRealRows = emptyList()
            rowCount = beginSnapshots.size
        }
        else {
            // Only NULL virtuals (e.g. SELECT $path) or pure COUNT(*): query one column for the count.
            val countRows: List<List<Any?>> = catalog.readInlinedData(
                    inlinedSplit.tableId, inlinedSplit.schemaVersion, inlinedSplit.snapshotId,
                    ImmutableList.of(tableColumns.first()))
            convertedRealRows = emptyList()
            rowCount = countRows.size
        }

        // Assemble output rows in the requested column order, weaving virtuals per row.
        val outputTypes: List<Type> = ducklakeColumns.map { it.columnType }
        val outputRows: List<List<Any?>> = (0 until rowCount).map { r ->
            var realIdx = 0
            ducklakeColumns.map { col ->
                when (col.virtualKind()) {
                    null -> convertedRealRows[r][realIdx++]
                    VirtualKind.SNAPSHOT_ID -> beginSnapshots!![r]
                    else -> null  // PATH, FILE_ROW_NUMBER, ROW_ID are NULL on inlined data
                }
            }
        }

        log.debug("Created inlined page source with %d rows for tableId=%d", rowCount, inlinedSplit.tableId)
        return RecordPageSource(InMemoryRecordSet(outputTypes, outputRows))
    }

    private fun createMetadataPageSource(metadataSplit: DucklakeMetadataSplit, columns: List<ColumnHandle>): ConnectorPageSource
    {
        val projectedColumns: List<DucklakeColumnHandle> = columns.stream()
                .map(DucklakeColumnHandle::class.java::cast)
                .collect(toImmutableList())
        val projectedTypes: List<Type> = projectedColumns.stream()
                .map { it.columnType }
                .collect(toImmutableList())

        val rows: List<Map<String, Any?>> = when (metadataSplit.metadataTableType) {
            DucklakeMetadataTableType.FILES -> buildFilesRows(metadataSplit)
            DucklakeMetadataTableType.SNAPSHOTS -> buildSnapshotRows(catalog.listSnapshots())
            DucklakeMetadataTableType.CURRENT_SNAPSHOT -> catalog.getSnapshot(metadataSplit.snapshotId)
                    ?.let { snapshot -> buildSnapshotRows(listOf(snapshot)) }
                    ?: emptyList()
            DucklakeMetadataTableType.SNAPSHOT_CHANGES -> buildSnapshotChangeRows(catalog.listSnapshotChanges())
        }

        val projectedRows: List<List<Any?>> = rows.stream()
                .map { row -> projectMetadataRow(row, projectedColumns, projectedTypes) }
                .collect(toImmutableList())

        return RecordPageSource(InMemoryRecordSet(projectedTypes, projectedRows))
    }

    private fun buildFilesRows(metadataSplit: DucklakeMetadataSplit): List<Map<String, Any?>>
    {
        val dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(metadataSplit.baseTableId, metadataSplit.snapshotId)
        val rows: MutableList<Map<String, Any?>> = ArrayList(dataFiles.size)
        for (file in dataFiles) {
            val row: MutableMap<String, Any?> = linkedMapOf()
            row["data_file_id"] = file.dataFileId
            row["path"] = file.path
            row["file_format"] = file.fileFormat
            row["record_count"] = file.recordCount
            row["file_size_bytes"] = file.fileSizeBytes
            row["row_id_start"] = file.rowIdStart
            row["partition_id"] = file.partitionId
            row["delete_file_path"] = file.deleteFilePath
            rows.add(row)
        }
        return rows
    }

    /**
     * Page source for a change-feed scan ([ChangeFeedTableHandle]; F9). Builds the read plan from
     * the catalog: the data files inserted in the window (insert side) and, for the delete side,
     * the newly-deleted positions per snapshot (current delete file minus its predecessor, read
     * here through the delete-file readers). Update pairing = the `rowid`s that are BOTH deleted
     * and re-inserted in the same snapshot. Each unit reads its data file through the ordinary
     * data-file pipeline ([createDataFilePageSource]) as of the END-snapshot schema, so schema
     * evolution and every file format work unchanged. See [ChangeFeedPageSource].
     */
    private fun createChangeFeedPageSource(
            session: ConnectorSession,
            table: ChangeFeedTableHandle,
            columns: List<ColumnHandle>): ConnectorPageSource
    {
        val requestedColumns: List<DucklakeColumnHandle> = columns.map { it as DucklakeColumnHandle }
        val requestedDataColumns: List<DucklakeColumnHandle> = requestedColumns.filter { it.columnId >= 0 }

        val schema: DucklakeSchema = catalog.getSchema(table.schemaName, table.endSnapshot)
                ?: throw TrinoException(NOT_SUPPORTED, "Schema not found for change feed: ${table.schemaName}")
        val tableMetadata: DucklakeTable = catalog.getTableById(table.tableId, table.endSnapshot)
                ?: throw TrinoException(NOT_SUPPORTED, "Table not found for change feed: ${table.tableName}")
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, tableMetadata)
        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)

        val insertFiles: List<DucklakeDataFile> = if (table.feedType != ChangeFeedType.DELETIONS)
            catalog.getDataFilesAddedBetween(table.tableId, table.startSnapshot, table.endSnapshot)
        else
            emptyList()
        val deletions: List<DucklakeChangeFeedDeletion> = if (table.feedType != ChangeFeedType.INSERTIONS)
            catalog.getDeletionsBetween(table.tableId, table.startSnapshot, table.endSnapshot)
        else
            emptyList()

        // Delete side: read the newly-deleted rowids per deletion event; then pair (a rowid both
        // deleted and re-inserted in the same snapshot is an update — see ChangeFeedUnit).
        val deletedRowidsBySnapshot: MutableMap<Long, MutableSet<Long>> = mutableMapOf()
        val resolvedDeletions: List<Pair<DucklakeChangeFeedDeletion, Set<Long>>> =
                resolveChangeFeedDeletions(fileSystem, tableDataPath, deletions, deletedRowidsBySnapshot)
        val updatedBySnapshot: Map<Long, Set<Long>> = computeUpdatedRowids(deletedRowidsBySnapshot, insertFiles)

        val units: MutableList<ChangeFeedUnit> = mutableListOf()
        for (file in insertFiles) {
            units.add(insertUnit(session, table, tableDataPath, requestedDataColumns, file, updatedBySnapshot))
        }
        for ((deletion, deletedRowids) in resolvedDeletions) {
            units.add(deleteUnit(session, table, tableDataPath, requestedDataColumns, deletion, deletedRowids, updatedBySnapshot))
        }

        return ChangeFeedPageSource(units, requestedColumns, requestedDataColumns, table.feedType.hasChangeType)
    }

    /** Reads the newly-deleted rowids for each deletion event, dropping no-op events, and records
     * them by snapshot (for update pairing). */
    private fun resolveChangeFeedDeletions(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            deletions: List<DucklakeChangeFeedDeletion>,
            deletedRowidsBySnapshot: MutableMap<Long, MutableSet<Long>>): List<Pair<DucklakeChangeFeedDeletion, Set<Long>>> {
        val resolved: MutableList<Pair<DucklakeChangeFeedDeletion, Set<Long>>> = mutableListOf()
        for (deletion in deletions) {
            val deletedRowids: Set<Long> = newlyDeletedRowids(fileSystem, tableDataPath, deletion)
            if (deletedRowids.isEmpty()) {
                continue
            }
            deletedRowidsBySnapshot.getOrPut(deletion.snapshotId) { mutableSetOf() }.addAll(deletedRowids)
            resolved.add(deletion to deletedRowids)
        }
        return resolved
    }

    /** rowids that were BOTH deleted and inserted in the same snapshot (update pairing). */
    private fun computeUpdatedRowids(
            deletedRowidsBySnapshot: Map<Long, Set<Long>>,
            insertFiles: List<DucklakeDataFile>): Map<Long, Set<Long>> {
        val insertRangesBySnapshot: MutableMap<Long, MutableList<LongRange>> = mutableMapOf()
        for (file in insertFiles) {
            insertRangesBySnapshot.getOrPut(file.beginSnapshot) { mutableListOf() }
                    .add(file.rowIdStart until (file.rowIdStart + file.recordCount))
        }
        return deletedRowidsBySnapshot.mapNotNull { (snapshot, deletedRowids) ->
            val ranges: List<LongRange> = insertRangesBySnapshot[snapshot] ?: return@mapNotNull null
            val updated: Set<Long> = deletedRowids.filterTo(mutableSetOf()) { rowId -> ranges.any { rowId in it } }
            if (updated.isEmpty()) null else snapshot to updated
        }.toMap()
    }

    private fun insertUnit(
            session: ConnectorSession,
            table: ChangeFeedTableHandle,
            tableDataPath: String,
            dataColumns: List<DucklakeColumnHandle>,
            file: DucklakeDataFile,
            updatedBySnapshot: Map<Long, Set<Long>>): ChangeFeedUnit {
        val resolvedPath: String = pathResolver.resolveFilePath(file.path, file.pathIsRelative, tableDataPath)
        return ChangeFeedUnit(
                baseSource = {
                    openChangeFeedFile(session, resolvedPath, file.fileFormat, file.footerSize,
                            file.fileSizeBytes, file.rowIdStart, file.recordCount, file.beginSnapshot, table, dataColumns)
                },
                snapshotId = file.beginSnapshot,
                keepRowids = null,
                updatedRowids = updatedBySnapshot[file.beginSnapshot] ?: emptySet(),
                isDelete = false)
    }

    private fun deleteUnit(
            session: ConnectorSession,
            table: ChangeFeedTableHandle,
            tableDataPath: String,
            dataColumns: List<DucklakeColumnHandle>,
            deletion: DucklakeChangeFeedDeletion,
            deletedRowids: Set<Long>,
            updatedBySnapshot: Map<Long, Set<Long>>): ChangeFeedUnit {
        val resolvedPath: String = pathResolver.resolveFilePath(deletion.dataFilePath, deletion.dataFilePathIsRelative, tableDataPath)
        return ChangeFeedUnit(
                baseSource = {
                    openChangeFeedFile(session, resolvedPath, deletion.dataFileFormat, deletion.dataFileFooterSize,
                            deletion.dataFileSizeBytes, deletion.rowIdStart, deletion.recordCount,
                            deletion.dataFileBeginSnapshot, table, dataColumns)
                },
                snapshotId = deletion.snapshotId,
                keepRowids = deletedRowids,
                updatedRowids = updatedBySnapshot[deletion.snapshotId] ?: emptySet(),
                isDelete = true)
    }

    /** Opens one change-feed data file through the ordinary read pipeline, projecting the data
     * columns followed by `$row_id`, reading as of [table]'s END-snapshot schema. */
    private fun openChangeFeedFile(
            session: ConnectorSession,
            resolvedPath: String,
            fileFormat: String,
            footerSize: Long,
            fileSizeBytes: Long,
            rowIdStart: Long,
            recordCount: Long,
            beginSnapshot: Long,
            table: ChangeFeedTableHandle,
            dataColumns: List<DucklakeColumnHandle>): ConnectorPageSource
    {
        val split = DucklakeSplit(
                resolvedPath,
                emptyList(),
                rowIdStart,
                recordCount,
                fileSizeBytes,
                fileFormat,
                TupleDomain.all(),
                footerSize,
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptySet(),
                Optional.empty(),
                beginSnapshot,
                null,
                emptyMap())
        val syntheticHandle = DucklakeTableHandle(table.schemaName, table.tableName, table.tableId, table.endSnapshot)
        val readColumns: List<DucklakeColumnHandle> = dataColumns + VirtualKind.ROW_ID.columnHandle()
        return try {
            val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
            createDataFilePageSource(session, split, syntheticHandle, readColumns, TupleDomain.all(), fileSystem)
        }
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Failed to open change-feed data file: $resolvedPath", e)
        }
    }

    /** The file-local row ids newly deleted at [deletion]'s snapshot: current delete positions
     * minus the predecessor's, mapped to `rowIdStart + position`. */
    private fun newlyDeletedRowids(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            deletion: DucklakeChangeFeedDeletion): Set<Long>
    {
        val current: Set<Long> = if (deletion.fullFileDelete) {
            (0 until deletion.recordCount).toSet()
        }
        else {
            readDeletePositions(fileSystem, tableDataPath, deletion.currentDeletePath, deletion.currentDeletePathIsRelative,
                    deletion.currentDeleteFormat, deletion.currentDeleteFooterSize, deletion.rowIdStart)
        }
        val previous: Set<Long> = readDeletePositions(fileSystem, tableDataPath, deletion.previousDeletePath,
                deletion.previousDeletePathIsRelative, deletion.previousDeleteFormat, deletion.previousDeleteFooterSize, deletion.rowIdStart)
        return (current - previous).mapTo(mutableSetOf()) { deletion.rowIdStart + it }
    }

    /** Reads a delete file's FILE-LOCAL positions (parquet or puffin); empty when [path] is null. */
    private fun readDeletePositions(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            path: String?,
            pathIsRelative: Boolean?,
            format: String?,
            footerSize: Long?,
            rowIdStart: Long): Set<Long>
    {
        if (path == null) {
            return emptySet()
        }
        val resolved: String = pathResolver.resolveFilePath(path, pathIsRelative ?: false, tableDataPath)
        if (isPuffinPath(resolved) || "puffin".equals(format, ignoreCase = true)) {
            return DucklakePuffinDeleteReader.readDeletedPositions(fileSystem.newInputFile(toLocation(resolved)))
        }
        val positions = DucklakeDeleteFileReader.readPositions(
                fileSystem, resolved, footerSize ?: 0L, parquetReaderOptions, fileFormatDataSourceStats)
        // Legacy Trino delete files store GLOBAL row ids (rowIdStart + pos); normalize to file-local.
        return if (positions.global) positions.values.mapTo(mutableSetOf()) { it - rowIdStart } else positions.values
    }

    private fun applyDeleteFile(fileSystem: TrinoFileSystem, split: DucklakeSplit, dataSource: ConnectorPageSource): ConnectorPageSource
    {
        val hasDeleteFiles: Boolean = split.deleteFilePaths.isNotEmpty()
        val hasInlinedDeletes: Boolean = split.inlinedDeletedRowPositions.isNotEmpty()
        val hasSnapshotFilter: Boolean = split.snapshotFilterMax != null
        if (!hasDeleteFiles && !hasInlinedDeletes && !hasSnapshotFilter) {
            return dataSource
        }

        // Two delete vocabularies, kept apart: GLOBAL row ids (Trino-written `row_id` delete
        // files, value = rowIdStart + file position) and FILE-LOCAL offsets (DuckLake-spec
        // `(file_path, pos)` delete files, puffin deletion vectors, and inlined deletes from
        // ducklake_inlined_delete_<tableId>). Each value must be matched under exactly ONE
        // interpretation: merging into a single both-ways-checked set phantom-deletes rows
        // whenever the file's rowIdStart < recordCount, because global ids then numerically
        // alias local offsets (caught by AbstractDucklakeRowLevelFormatTest's spread-delete).
        val localOffsets: MutableSet<Long> = split.inlinedDeletedRowPositions.toMutableSet()
        val globalRowIds: MutableSet<Long> = mutableSetOf()
        // Partial (cross-snapshot compacted) data file: drop file-local positions whose
        // _ducklake_internal_snapshot_id exceeds the read snapshot — same file-local-offset
        // vocabulary as DuckLake-spec deletes, so they merge into localOffsets.
        split.snapshotFilterMax?.let { filterMax ->
            localOffsets.addAll(DucklakeDeleteFileReader.readSnapshotDropPositions(
                    fileSystem, split.dataFilePath, split.footerSize, filterMax,
                    parquetReaderOptions, fileFormatDataSourceStats))
        }
        for (deleteFilePath in split.deleteFilePaths) {
            if (isPuffinPath(deleteFilePath)) {
                val inputFile = fileSystem.newInputFile(toLocation(deleteFilePath))
                // Consolidated ("partial") puffin file holding deletions newer than this read →
                // apply only the per-blob deletion vectors whose ducklake-snapshot-id <= the read
                // snapshot. A non-partial file (no snapshot filter) reads the full union.
                val snapshotFilter: Long? = split.deleteFileSnapshotFilters[deleteFilePath]
                localOffsets.addAll(
                        if (snapshotFilter != null) {
                            DucklakePuffinDeleteReader.readDeletedPositions(inputFile, snapshotFilter)
                        }
                        else {
                            DucklakePuffinDeleteReader.readDeletedPositions(inputFile)
                        })
            }
            else {
                val positions = readDeletedRowsFromFile(fileSystem, deleteFilePath, split)
                if (positions.global) {
                    globalRowIds.addAll(positions.values)
                }
                else {
                    localOffsets.addAll(positions.values)
                }
            }
        }

        if (globalRowIds.isEmpty() && localOffsets.isEmpty()) {
            return dataSource
        }

        log.debug("Applying deletions to data file %s: %d parquet delete file(s), %d inlined deletes, %d global + %d local deleted rows",
                split.dataFilePath,
                split.deleteFilePaths.size,
                split.inlinedDeletedRowPositions.size,
                globalRowIds.size,
                localOffsets.size)
        return TransformConnectorPageSource.create(dataSource, DeleteRowFilterTransform(globalRowIds, localOffsets, split.rowIdStart))
    }

    private fun readDeletedRowsFromFile(fileSystem: TrinoFileSystem, deleteFilePath: String, split: DucklakeSplit): DucklakeDeleteFileReader.DeletePositions
    {
        // Delete files carry their own footer_size in ducklake_delete_file.
        val deleteFooterHint: Long = split.deleteFileFooterSizes.getOrDefault(deleteFilePath, 0L)
        // Consolidated ("partial") delete file: apply only the deletions recorded at or before the
        // read snapshot (filter by _ducklake_internal_snapshot_id).
        val snapshotFilter: Long? = split.deleteFileSnapshotFilters[deleteFilePath]
        if (snapshotFilter != null) {
            return DucklakeDeleteFileReader.readPositionsWithSnapshotFilter(
                    fileSystem,
                    deleteFilePath,
                    deleteFooterHint,
                    snapshotFilter,
                    parquetReaderOptions,
                    fileFormatDataSourceStats)
        }
        return DucklakeDeleteFileReader.readPositions(
                fileSystem,
                deleteFilePath,
                deleteFooterHint,
                parquetReaderOptions,
                fileFormatDataSourceStats)
    }

    private fun createParquetPageSource(
            inputFile: TrinoInputFile,
            columns: List<DucklakeColumnHandle>,
            split: DucklakeSplit,
            effectivePredicate: TupleDomain<DucklakeColumnHandle>,
            fileSystem: TrinoFileSystem): ConnectorPageSource
    {
        // Create memory context for reading
        val memoryContext: AggregatedMemoryContext = newSimpleAggregatedMemoryContext()

        var dataSource: ParquetDataSource? = null
        try {
            // Create Parquet data source
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(split.fileSizeBytes),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats)

            // Feed DuckLake's footer_size hint to Trino's Parquet reader. For typical
            // footers (<48 KB), this trims the blind tail read down to the exact bytes;
            // for oversized footers, it replaces the fallback two-round-trip path with a
            // single-shot read. See FooterPrefetchingParquetDataSource.
            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(
                    dataSource, split.footerSize, parquetReaderOptions.maxFooterReadSize.toBytes())

            // Read Parquet metadata
            val parquetMetadata: ParquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions,
                    Optional.empty(),
                    Optional.empty())
            val fileMetadata: FileMetadata = parquetMetadata.fileMetaData
            val fileSchema: MessageType = fileMetadata.schema
            val dataSourceId: ParquetDataSourceId = dataSource.id
            val descriptorsByPath: Map<List<String>, ColumnDescriptor> = getDescriptors(fileSchema, fileSchema)
            // B3b: when the split carries active position deletes, disable in-reader predicate
            // pushdown (row-group pruning + page-level skipping). PositionalVirtualInjectingPageSource and
            // DeleteRowFilterTransform derive file positions from cumulative page sizes
            // (nextRowOffset += positionCount), which only matches true file positions when
            // pages stream contiguously from row 0. Pruning creates gaps and mis-aligns the
            // counter, masking the wrong rows. Trino's filter pipeline still applies the
            // predicate above the page source, so query semantics are preserved — we just
            // give up row-group pruning on files that carry deletes. The performant
            // alternative (project the parquet file_row_number via appendRowNumberColumn and
            // thread it through both transforms) is deferred — see PLAN.md / BEFORE-RESUME B3b.
            //
            // INVARIANT: B3b's correctness ALSO depends on splits being whole-file. The
            // cumulative-offset math seeds nextRowOffset at 0; if a split ever covers a
            // sub-range of a file (split-by-rowgroup for parallel reads of large files, as
            // Iceberg/Hive do), this fix is INSUFFICIENT — nextRowOffset would also need to
            // be seeded from the split's start offset. Today DucklakeSplitManager.createMergedSplit
            // produces one split per data_file_id (passing primary.fileSizeBytes() and
            // start=0 throughout), and getFilteredRowGroups below is called with start=0 /
            // length=split.fileSizeBytes(), so this invariant holds. Any change to split
            // granularity MUST re-derive position math (or do the performant fix above).
            // Positions for $file_row_number / $row_id come from the cumulative page offset, which
            // only matches true file positions when the reader streams contiguously from row 0.
            // Predicate pushdown can prune row groups and break that, so disable it when ANY
            // positional column is requested — the queryable virtuals (perRow) AND the MERGE
            // $row_id (-100). The rowIds captured by a DELETE/UPDATE/MERGE source scan become
            // delete-file positions, so a pruned merge scan tombstones the WRONG rows. Caught
            // row-level on the duckdb-format path (AbstractDucklakeRowLevelFormatTest); the
            // parquet variant was latent only because pruning is row-group-granular.
            val requiresContiguousPositions: Boolean =
                    splitHasActiveDeletes(split) || columns.any { positionalColumnKind(it) != null }
            val parquetTupleDomain: TupleDomain<ColumnDescriptor> =
                    if (requiresContiguousPositions) TupleDomain.all()
                    else toParquetTupleDomain(descriptorsByPath, effectivePredicate)
            val parquetPredicate: TupleDomainParquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC)
            val rowGroups: List<RowGroupInfo> = getFilteredRowGroups(
                    0,
                    split.fileSizeBytes,
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    Domain.DEFAULT_COMPACTION_THRESHOLD,
                    parquetReaderOptions)

            // Separate out positional columns — the MERGE $row_id (-100, used for
            // DELETE/UPDATE/MERGE) plus the queryable positional virtuals $row_id and
            // $file_row_number — from file-resident columns. They are injected from cumulative
            // file position after the reader returns rows, BEFORE delete filtering, so positions
            // reflect original file positions.
            val positionalInjections: MutableList<PositionalInjection> = mutableListOf()
            val fileColumns: MutableList<DucklakeColumnHandle> = mutableListOf()
            for (i in columns.indices) {
                val addRowIdStart: Boolean? = positionalColumnKind(columns[i])
                if (addRowIdStart != null) {
                    positionalInjections.add(PositionalInjection(i, addRowIdStart))
                }
                else {
                    fileColumns.add(columns[i])
                }
            }

            // Build list of columns to read, handling missing columns for schema evolution
            val parquetColumns: ImmutableList.Builder<Column> = ImmutableList.builder()
            val messageColumnIO: MessageColumnIO = getColumnIO(fileSchema, fileSchema)
            val transforms: TransformConnectorPageSource.Builder = TransformConnectorPageSource.builder()
            var parquetColumnOrdinal = 0

            // Build field_id → ColumnIO index for field_id-based column matching (schema evolution: renames)
            val fieldIdToColumnIO: MutableMap<Int, ColumnIO> = mutableMapOf()
            for (field in fileSchema.fields) {
                if (field.id != null) {
                    val childIO: ColumnIO? = messageColumnIO.getChild(field.name)
                    if (childIO != null) {
                        fieldIdToColumnIO[field.id.intValue()] = childIO
                    }
                }
            }

            for (column in fileColumns) {
                val columnName: String = column.columnName
                // Try name-based match first, then fall back to field_id match (handles column renames)
                var columnIO: ColumnIO? = messageColumnIO.getChild(columnName)
                if (columnIO == null && column.columnId > 0) {
                    columnIO = fieldIdToColumnIO[column.columnId.toInt()]
                }
                // Finally, consult the catalog's name_map for files registered via
                // add_files — covers the case where the parquet column name differs
                // from the table column name (e.g. case-difference, or a column-rename
                // where the file kept its original name). The map is empty for files
                // without a mapping_id, so this is a no-op for INSERT-written files.
                if (columnIO == null) {
                    val parquetSourceName: String? = split.fieldIdToParquetSourceName[column.columnId]
                    if (parquetSourceName != null && parquetSourceName != columnName) {
                        columnIO = messageColumnIO.getChild(parquetSourceName)
                    }
                }

                if (columnIO == null) {
                    // Missing column in file. Two cases:
                    //   (1) hive-style external file: parquet body omits the partition column,
                    //       but the catalog has a value for it via ducklake_file_partition_value.
                    //       Project that value as a constant block.
                    //   (2) schema evolution / genuinely missing: return NULL.
                    transforms.constantValue(buildMissingColumnBlock(column, split))
                    continue
                }

                val field: Optional<Field> = DucklakeParquetTypeUtils.constructField(
                        column.columnType,
                        columnIO)
                if (field.isEmpty) {
                    // Could not construct field — return nulls (or partition constant if available)
                    transforms.constantValue(buildMissingColumnBlock(column, split))
                    continue
                }

                parquetColumns.add(Column(columnName, field.get()))
                transforms.column(parquetColumnOrdinal)
                parquetColumnOrdinal++
            }

            val presentColumns: List<Column> = parquetColumns.build()

            // Create ParquetReader with only the columns present in the file
            val parquetReader = ParquetReader(
                    Optional.ofNullable(fileMetadata.createdBy),
                    presentColumns,
                    false, // appendRowNumberColumn
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    { exception -> handleParquetException(dataSourceId, exception) },
                    if (parquetTupleDomain.isAll) Optional.empty() else Optional.of(parquetPredicate),
                    Optional.empty(), // bloomFilterStore
                    Optional.empty()) // rowFilter

            // Wrap in ParquetPageSource, apply column transforms for missing columns,
            // then apply merge-on-read delete filtering if present
            var pageSource: ConnectorPageSource = ParquetPageSource(parquetReader)
            pageSource = transforms.build(pageSource)

            // Inject positional virtuals before delete filtering so they reflect original file positions
            if (positionalInjections.isNotEmpty()) {
                pageSource = PositionalVirtualInjectingPageSource(
                        pageSource, fileColumns.size + positionalInjections.size, positionalInjections, split.rowIdStart)
            }

            pageSource = applyDeleteFile(fileSystem, split, pageSource)

            log.debug("Created Parquet page source for %d columns from file: %s",
                    columns.size, split.dataFilePath)

            return pageSource
        }
        catch (e: IOException) {
            if (dataSource != null) {
                try {
                    dataSource.close()
                }
                catch (ex: IOException) {
                    if (e != ex) {
                        e.addSuppressed(ex)
                    }
                }
            }
            throw RuntimeException("Failed to create Parquet page source for file: ${split.dataFilePath}", e)
        }
        catch (e: RuntimeException) {
            if (dataSource != null) {
                try {
                    dataSource.close()
                }
                catch (ex: IOException) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex)
                    }
                }
            }
            throw RuntimeException("Failed to create Parquet page source for file: " + split.dataFilePath, e)
        }
    }

    private fun createDuckDbPageSource(
            dataFileLocation: Location,
            columns: List<DucklakeColumnHandle>,
            split: DucklakeSplit,
            effectivePredicate: TupleDomain<DucklakeColumnHandle>,
            pushedExpressions: List<String>,
            fileSystem: TrinoFileSystem,
            session: ConnectorSession,
            fileColumnNamesById: Map<Long, String>,
            structReshapePlans: Map<Long, List<StructFieldPlan>>): ConnectorPageSource
    {
        // Separate positional columns (MERGE $row_id + queryable $row_id / $file_row_number)
        // from file-resident columns. The .db file does not store row IDs / file positions;
        // they are injected after the data page source returns its rows, exactly as on the
        // parquet path.
        val positionalInjections: MutableList<PositionalInjection> = mutableListOf()
        val fileColumns: MutableList<DucklakeColumnHandle> = mutableListOf()
        for (i in columns.indices) {
            val addRowIdStart: Boolean? = positionalColumnKind(columns[i])
            if (addRowIdStart != null) {
                positionalInjections.add(PositionalInjection(i, addRowIdStart))
            }
            else {
                fileColumns.add(columns[i])
            }
        }

        // See createParquetPageSource: disable predicate/expression pushdown when any positional
        // column is requested so cumulative-offset positions stay aligned with the scan output.
        // The MERGE $row_id (-100) counts: DuckDB applies pushed predicates per-ROW, so a pushed
        // merge scan returns only matching rows, the cumulative offsets compact, and the delete
        // file tombstones the wrong rows (the wrong-survivors failure that drove this guard).
        val requiresContiguousPositions: Boolean =
                splitHasActiveDeletes(split) || columns.any { positionalColumnKind(it) != null }

        // Empty projection (e.g. COUNT(*)) is handled inside DuckDbFilePageSource by
        // issuing a synthetic SELECT 1 and emitting empty-block pages with the right
        // row count.

        val attachTarget: DuckDbAttachTarget = resolveDuckDbReadTarget(
                session, dataFileLocation, fileSystem, split)

        val fileColumnTypes: List<Type> = fileColumns.stream()
                .map { it.columnType }
                .collect(toImmutableList())

        // Restrict the pushed-down predicate to columns we actually project (filter
        // pipeline still applies any not-pushed-down or non-projected predicates above).
        // B3b: when the split carries active position deletes, drop the pushed predicate so
        // DuckDB returns rows contiguously from row 0. PositionalVirtualInjectingPageSource's cumulative
        // nextRowOffset assumes contiguous output; predicate-pushed DuckDB scans return only
        // matching rows, breaking the position math. Trino still filters above the page source.
        // Hash-set membership so the per-domain filter is O(predicateColumns) rather than
        // O(predicateColumns * fileColumns) — fileColumns is an ArrayList, so .contains is a
        // linear scan with record-based equals per probe.
        val fileColumnSet: Set<DucklakeColumnHandle> = fileColumns.toHashSet()
        val filePredicate: TupleDomain<DucklakeColumnHandle> = if (requiresContiguousPositions)
                TupleDomain.all()
            else
                effectivePredicate.filter { col, _ -> fileColumnSet.contains(col) }

        // Carry Trino's session zone through to the executor so it can run
        // `SET TimeZone` on attach. Required for Tier C correctness (TIMESTAMP
        // WITH TIME ZONE pushdown) and harmlessly deterministic for Tier A/B
        // (DuckDB's default zone is the JVM system TZ — Costa Rica on a dev box,
        // UTC in CI — so an explicit SET is the only way to make duckdb-format
        // reads reproducible across deployment environments). See
        // dev-docs/archive/REPORT-datetime-tz-handling.md.
        val duckDbTimeZone: String? = TrinoTimeZoneNormaliser.normalise(session.timeZoneKey.id)

        // B3b: drop pushed complex expressions when the split has active deletes — same
        // reasoning as the TupleDomain drop above. DuckDB-side filtering would return only
        // matching rows, breaking PositionalVirtualInjectingPageSource's cumulative-offset math.
        val effectivePushedExpressions: List<String> = if (requiresContiguousPositions) emptyList() else pushedExpressions
        var pageSource: ConnectorPageSource = DuckDbFilePageSource(
                executorFactory.create(), attachTarget, fileColumns, fileColumnTypes, filePredicate, effectivePushedExpressions,
                duckDbTimeZone, fileColumnNamesById, structReshapePlans)

        if (positionalInjections.isNotEmpty()) {
            pageSource = PositionalVirtualInjectingPageSource(
                    pageSource, fileColumns.size + positionalInjections.size, positionalInjections, split.rowIdStart)
        }

        pageSource = applyDeleteFile(fileSystem, split, pageSource)

        log.debug("Created DuckDB page source for %d columns from file: %s",
                columns.size, split.dataFilePath)
        return pageSource
    }

    /**
     * Page sources for the non-data-file splits: metadata tables, inlined data, and the lance
     * search PTF scans. Null for ordinary data-file splits ([DucklakeSplit]).
     * (createInlinedPageSource materializes rows in memory, so it weaves virtuals per row
     * directly — it needs per-row begin_snapshot for $snapshot_id anyway. No outer wrapper.)
     */
    private fun specialSplitPageSource(
            session: ConnectorSession,
            split: ConnectorSplit,
            table: ConnectorTableHandle,
            columns: List<ColumnHandle>,
            dynamicFilter: DynamicFilter): ConnectorPageSource? = when (split) {
        is DucklakeMetadataSplit -> createMetadataPageSource(split, columns)
        is DucklakeInlinedSplit -> createInlinedPageSource(split, columns)
        is LanceSearchSplit -> createLanceSearchPageSource(session, split, table as LanceSearchTableHandle, columns, dynamicFilter)
        is ChangeFeedSplit -> createChangeFeedPageSource(session, table as ChangeFeedTableHandle, columns)
        else -> null
    }

    /**
     * Page source for one lance-search PTF-scan split (`applyTableFunction` rewrite — see
     * [LanceSearchTableHandle]). Runs the matching `lance_*` DuckDB call over the split's
     * dataset directory through [DuckDbFilePageSource], with the engine's projection
     * ([columns] — only the requested output columns are SELECTed), the handle's pushed
     * predicate intersected with the dynamic filter rendered as the `WHERE`, and `applyTopN`'s
     * trimmed per-fragment `k` folded into the rendered argument tail.
     */
    private fun createLanceSearchPageSource(
            session: ConnectorSession,
            split: LanceSearchSplit,
            tableHandle: LanceSearchTableHandle,
            columns: List<ColumnHandle>,
            dynamicFilter: DynamicFilter): ConnectorPageSource
    {
        val dynamicFilterPredicate: TupleDomain<DucklakeColumnHandle> = dynamicFilter.currentPredicate
                .transformKeys(DucklakeColumnHandle::class.java::cast)
        val effectivePredicate: TupleDomain<DucklakeColumnHandle> = tableHandle.pushedPredicate
                .intersect(dynamicFilterPredicate)
        if (effectivePredicate.isNone) {
            return EmptyPageSource()
        }

        val search: LanceSearchHandle = tableHandle.effectiveSearch()
        // With prefilter => true, lance REQUIRES every WHERE conjunct over the call to be
        // pushable into the function and errors otherwise ("requires filter pushdown for
        // prefilterable columns"). DuckDB can push single-range conjuncts (=, >, BETWEEN, ...)
        // but not OR-of-ranges or IN-lists — so render only the pushable domains; the rest stay
        // engine-side (the full predicate is always in the engine's remaining filter).
        val renderedPredicate: TupleDomain<DucklakeColumnHandle> = if (search.prefilter)
            effectivePredicate.filter { _, domain -> isPrefilterPushable(domain) }
        else
            effectivePredicate
        val projectedColumns: List<DucklakeColumnHandle> = columns.stream()
                .map(DucklakeColumnHandle::class.java::cast)
                .collect(toImmutableList())
        val target = DuckDbAttachTarget.FileScan(
                split.datasetPath,
                LanceSearchSplitProcessor.scanFunctionFor(search),
                DucklakeSessionProperties.FORMAT_LANCE,
                null,
                LanceSearchSplitProcessor.renderExtraArgsSql(search))
        val duckDbTimeZone: String? = TrinoTimeZoneNormaliser.normalise(session.timeZoneKey.id)
        return DuckDbFilePageSource(
                executorFactory.create(),
                target,
                projectedColumns,
                projectedColumns.map { it.columnType },
                renderedPredicate,
                emptyList(),
                duckDbTimeZone)
    }

    /**
     * Whether a domain renders as a WHERE conjunct DuckDB can push into a lance table function
     * under `prefilter := true`: a single range over an orderable type, with no NULL allowance
     * (nullable domains render an `OR x IS NULL` arm, which is not pushable). Probed live
     * (2026-06-10): single-range and BETWEEN push; OR-of-ranges and IN-lists do not.
     */
    private fun isPrefilterPushable(domain: io.trino.spi.predicate.Domain): Boolean {
        if (domain.isNullAllowed || domain.isNone || domain.values.isAll) {
            return false
        }
        return runCatching { domain.values.ranges.rangeCount == 1 }.getOrDefault(false)
    }

    /**
     * The read target for a DuckDB-engine split. For the `duckdb` (.db) format this is the
     * ATTACH target from [resolveDuckDbAttachTarget]. For the single-file scan format
     * (`vortex`) the same materialize-vs-streaming decision is reused, then wrapped as a
     * [DuckDbAttachTarget.FileScan] carrying the scan function + extension so the executor
     * reads via `read_vortex('path')` instead of ATTACHing a database — but WITHOUT the
     * httpfs secret on the streaming s3 shape: `read_vortex` is object_store-credentialed
     * (`AWS_*` env), not secret-credentialed, so s3-streaming vortex reads need the lance-O1 env
     * channel on the executing process. `lance` is a dataset *directory* and bypasses the
     * materialize cache entirely (see below).
     */
    private fun resolveDuckDbReadTarget(
            session: ConnectorSession,
            dataFileLocation: Location,
            fileSystem: TrinoFileSystem,
            split: DucklakeSplit): DuckDbAttachTarget
    {
        // Lance is a *dataset directory*, not a single file: `__lance_scan('<dir>')` reads the
        // whole dataset (manifest + data + index files). It must NOT route through
        // resolveDuckDbAttachTarget's materialize cache, which copies a single file to local tmp
        // — that would pull one file out of the directory and hand DuckDB a broken path. Instead
        // hand the catalog path straight to __lance_scan. No DuckDbS3Config even for s3:// — the
        // lance extension's Rust object_store ignores DuckDB httpfs secrets entirely and resolves
        // credentials from process-global AWS_* env (HANDOFF O1; the Quack sidecar gets them via
        // container env). Passing the config would only run a pointless httpfs INSTALL + the
        // CREATE of a secret lance never reads (and, before the IF NOT EXISTS + retry fix in
        // DuckDbS3Config/DuckDbCatalogWriteRetry, that needless CREATE was the write-write
        // conflict trigger first observed on this very path).
        if (DucklakeSessionProperties.FORMAT_LANCE.equals(split.fileFormat, ignoreCase = true)) {
            return DuckDbAttachTarget.FileScan(
                    dataFileLocation.toString(), "__lance_scan", "lance", null)
        }
        val base: DuckDbAttachTarget = resolveDuckDbAttachTarget(session, dataFileLocation, fileSystem, split)
        if (DucklakeSessionProperties.FORMAT_DUCKDB.equals(split.fileFormat, ignoreCase = true)) {
            return base
        }
        val scanFunction: String
        val extension: String
        when {
            DucklakeSessionProperties.FORMAT_VORTEX.equals(split.fileFormat, ignoreCase = true) -> {
                scanFunction = "read_vortex"; extension = "vortex"
            }
            else -> throw TrinoException(NOT_SUPPORTED, "Unsupported DuckDB-engine file format: ${split.fileFormat}")
        }
        return when (base) {
            is DuckDbAttachTarget.LocalPath ->
                DuckDbAttachTarget.FileScan(base.path.toAbsolutePath().toString(), scanFunction, extension, null)
            // No DuckDbS3Config on the streaming shape: `read_vortex` binds through Rust
            // object_store, which NEVER consults DuckDB httpfs secrets (probed 2026-06-11 —
            // single-threaded read with only the secret present falls back to the EC2 metadata
            // service and fails; only the vortex COPY *write* honors the secret). Credentials
            // are the lance-O1 `AWS_*` env channel: the Quack sidecar's container env, or the
            // Trino JVM's own env in-process (DuckDbS3Config.toObjectStoreEnv). Shipping the
            // secret anyway would just add pointless httpfs INSTALL + secret-create chatter.
            is DuckDbAttachTarget.HttpfsS3 ->
                DuckDbAttachTarget.FileScan(base.s3Url, scanFunction, extension, null)
            is DuckDbAttachTarget.FileScan -> base
        }
    }

    /**
     * Decide whether to materialize the `.db` file to local tmp and ATTACH that
     * path, or load DuckDB's httpfs extension and ATTACH the remote `s3://` URL
     * directly. Driven by the `duckdb_read_mode` session property; `auto`
     * (the default) consults the `ducklake.duckdb.auto-httpfs-threshold` config.
     */
    private fun resolveDuckDbAttachTarget(
            session: ConnectorSession,
            dataFileLocation: Location,
            fileSystem: TrinoFileSystem,
            split: DucklakeSplit): DuckDbAttachTarget
    {
        val mode: String = DucklakeSessionProperties.getDuckDbReadMode(session)
        val useHttpfs: Boolean = when (mode.lowercase(Locale.ROOT)) {
            DucklakeSessionProperties.READ_MODE_MATERIALIZE -> false
            DucklakeSessionProperties.READ_MODE_HTTPFS -> true
            // 'auto' picks per-file. Below the threshold the materialize cache wins
            // (small files are cheap to download and warm reads are then local). At or
            // above the threshold we stream blocks via httpfs to avoid the full pull.
            DucklakeSessionProperties.READ_MODE_AUTO -> split.fileSizeBytes >= autoHttpfsThresholdBytes
            else -> throw TrinoException(NOT_SUPPORTED, "Unsupported duckdb_read_mode: $mode")
        }

        val url: String = dataFileLocation.toString()
        val isS3: Boolean = url.startsWith("s3://") || url.startsWith("s3a://") || url.startsWith("s3n://")
        if (useHttpfs && isS3) {
            return DuckDbAttachTarget.HttpfsS3(url, duckDbS3Config)
        }
        // httpfs against a non-s3 target degrades to materialize — the local path is
        // already directly attachable, no need for a remote-streaming protocol.
        val localPath: Path = duckDbReadCache.materialize(
                fileSystem, dataFileLocation, split.fileSizeBytes)
        return DuckDbAttachTarget.LocalPath(localPath)
    }

    companion object {
        private val log: Logger = Logger.get(DucklakePageSourceProvider::class.java)

        // Cap on the schema-evolution name cache before a wholesale clear. Keyed by
        // (tableId, begin_snapshot); only populated for tables that have actually evolved
        // and been read, so this ceiling is effectively never hit in practice.
        private const val MAX_FILE_COLUMN_NAME_CACHE_ENTRIES = 2048

        private fun buildSnapshotRows(snapshots: List<DucklakeSnapshot>): List<Map<String, Any?>>
        {
            val rows: MutableList<Map<String, Any?>> = ArrayList(snapshots.size)
            for (snapshot in snapshots) {
                val row: MutableMap<String, Any?> = linkedMapOf()
                row["snapshot_id"] = snapshot.snapshotId
                row["snapshot_time"] = snapshot.snapshotTime
                row["schema_version"] = snapshot.schemaVersion
                row["next_catalog_id"] = snapshot.nextCatalogId
                row["next_file_id"] = snapshot.nextFileId
                rows.add(row)
            }
            return rows
        }

        private fun buildSnapshotChangeRows(changes: List<DucklakeSnapshotChange>): List<Map<String, Any?>>
        {
            val rows: MutableList<Map<String, Any?>> = ArrayList(changes.size)
            for (change in changes) {
                val row: MutableMap<String, Any?> = linkedMapOf()
                row["snapshot_id"] = change.snapshotId
                row["changes_made"] = change.changesMade
                row["author"] = change.author
                row["commit_message"] = change.commitMessage
                row["commit_extra_info"] = change.commitExtraInfo
                rows.add(row)
            }
            return rows
        }

        private fun projectMetadataRow(row: Map<String, Any?>, columns: List<DucklakeColumnHandle>, types: List<Type>): List<Any?>
        {
            val projected: MutableList<Any?> = ArrayList(columns.size)
            for (index in columns.indices) {
                val value: Any? = row[columns[index].columnName]
                projected.add(toNativeMetadataValue(value, types[index]))
            }
            return projected
        }

        private fun toNativeMetadataValue(value: Any?, type: Type): Any?
        {
            if (value == null) {
                return null
            }
            if (type == TIMESTAMP_TZ_MILLIS) {
                val instant: Instant = value as Instant
                return io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone(instant.toEpochMilli(), UTC_KEY)
            }
            if (type == BIGINT || type == INTEGER) {
                return value
            }
            return Slices.utf8Slice(value.toString())
        }

        private fun toParquetTupleDomain(
                descriptorsByPath: Map<List<String>, ColumnDescriptor>,
                effectivePredicate: TupleDomain<DucklakeColumnHandle>): TupleDomain<ColumnDescriptor>
        {
            if (effectivePredicate.isNone) {
                return TupleDomain.none()
            }
            if (effectivePredicate.isAll) {
                return TupleDomain.all()
            }

            val predicate: ImmutableMap.Builder<ColumnDescriptor, Domain> = ImmutableMap.builder()
            val topLevelDescriptors: Map<String, ColumnDescriptor> = descriptorsByPath.entries.stream()
                    .filter { entry -> entry.key.size == 1 }
                    .collect(toImmutableMap(
                            { entry -> entry.key[0].lowercase(Locale.ENGLISH) },
                            { entry -> entry.value },
                            { first, _ -> first }))

            val domains: Optional<Map<DucklakeColumnHandle, Domain>> = effectivePredicate.getDomains()
            if (domains.isEmpty) {
                return TupleDomain.all()
            }

            for (entry in domains.get().entries) {
                val columnHandle: DucklakeColumnHandle = entry.key
                val descriptor: ColumnDescriptor? = topLevelDescriptors[columnHandle.columnName.lowercase(Locale.ENGLISH)]
                if (descriptor != null) {
                    predicate.put(descriptor, entry.value)
                }
            }

            val parquetDomains: Map<ColumnDescriptor, Domain> = predicate.buildOrThrow()
            if (parquetDomains.isEmpty()) {
                return TupleDomain.all()
            }
            return TupleDomain.withColumnDomains(parquetDomains)
        }

        /**
         * Whether the split carries any active position deletes — either external parquet/puffin
         * delete files or inlined deletes from `ducklake_inlined_delete_<tableId>`.
         *
         * When this returns `true`, the page-source pipeline must NOT push the query
         * predicate down into the parquet reader or the DuckDB scan: doing so prunes row groups
         * / skips rows inside the underlying file, breaking the cumulative-offset math that
         * [PositionalVirtualInjectingPageSource] and [DeleteRowFilterTransform] use to compute
         * file-absolute positions for delete matching (B3b — see the bug trace and option-(B)
         * fix in `.ai/kotlin-port/BEFORE-RESUME.md`).
         */
        fun splitHasActiveDeletes(split: DucklakeSplit): Boolean =
            split.deleteFilePaths.isNotEmpty() || split.inlinedDeletedRowPositions.isNotEmpty() ||
                    // A partial-file snapshot filter drops rows by file-local position too, so the
                    // reader must stream contiguously (no row-group pruning) for positions to align.
                    split.snapshotFilterMax != null

        private fun isPuffinPath(path: String): Boolean =
            // DuckLake's delete-file path always ends with .puffin when format='puffin'
            // (see vendor/ducklake/src/storage/ducklake_delete.cpp:161 — the writer hardcodes
            // "ducklake-<uuid>-delete.puffin"). Catalog format='puffin' has already been
            // permitted by DucklakeSplitManager.validateDeleteFileFormats by the time we get
            // here; dispatching on extension keeps the split schema stable and matches the
            // pattern Trino's Iceberg connector uses for puffin DV files.
            path.regionMatches(path.length - ".puffin".length, ".puffin", 0, ".puffin".length, ignoreCase = true)

        private fun toLocation(path: String): Location
        {
            val location: Location = Location.of(path)
            if (location.scheme().isPresent) {
                return location
            }
            return Location.of(Path.of(path).toUri().toString())
        }

        private fun handleParquetException(dataSourceId: ParquetDataSourceId, exception: Exception): RuntimeException =
            exception as? TrinoException
                ?: TrinoException(
                    NOT_SUPPORTED,
                    "Error reading Parquet file: $dataSourceId",
                    exception)

        /**
         * Build the constant block emitted by [io.trino.plugin.hive.TransformConnectorPageSource]
         * for a column not present in the parquet body. Defaults to a single-position NULL block;
         * when the split carries a catalog-recorded partition value for this column (hive-style
         * external imports), parses the string value and projects it as a constant instead.
         */
        private fun buildMissingColumnBlock(column: DucklakeColumnHandle, split: DucklakeSplit): Block
        {
            val partitionValue: String? = split.partitionValuesByColumnId[column.columnId]
            if (partitionValue == null) {
                return column.columnType.createNullBlock()
            }
            try {
                val nativeValue: Any = DucklakePartitionValueParser.parseIdentity(column.columnType, partitionValue)
                return io.trino.spi.type.TypeUtils.writeNativeValue(column.columnType, nativeValue)
            }
            catch (_: RuntimeException) {
                // If the catalog's stored value can't be parsed to the column's type, fall back
                // to NULL rather than failing the whole read. The pruning path already tolerates
                // parse failures the same way.
                return column.columnType.createNullBlock()
            }
        }

        /**
         * Classify a requested column as a positional injection, or not.
         * Returns true  -> $row_id semantics (value = rowIdStart + file position): the MERGE
         *                  row-id channel (-100) and the queryable $row_id virtual (-104).
         * Returns false -> $file_row_number (value = file position, 0-based): the -103 virtual.
         * Returns null  -> not positional (real column, or a constant virtual).
         */
        private fun positionalColumnKind(column: DucklakeColumnHandle): Boolean? {
            if (column.isRowIdColumn()) {
                return true
            }
            return when (column.virtualKind()) {
                VirtualKind.ROW_ID -> true
                VirtualKind.FILE_ROW_NUMBER -> false
                else -> null
            }
        }

        /**
         * Single-position constant block for a virtual column on a file-backed split
         * (parquet or duckdb): $path = the data file path, $snapshot_id = the file's
         * begin_snapshot. The injecting page source RLE-expands it to each page.
         */
        private fun dataFileVirtualBlock(kind: VirtualKind, split: DucklakeSplit): Block = when (kind) {
            VirtualKind.PATH -> io.trino.spi.type.TypeUtils.writeNativeValue(VARCHAR, Slices.utf8Slice(split.dataFilePath))
            VirtualKind.SNAPSHOT_ID -> io.trino.spi.type.TypeUtils.writeNativeValue(BIGINT, split.beginSnapshot)
            VirtualKind.FILE_SIZE_BYTES -> io.trino.spi.type.TypeUtils.writeNativeValue(BIGINT, split.fileSizeBytes)
            else -> throw TrinoException(NOT_SUPPORTED, "Virtual column not yet supported on the read path: ${kind.columnName}")
        }

        /**
         * Wrap [delegate] so the virtual columns selected by [wrapKind] are injected as constant
         * RLE blocks at their requested output positions. Only those kinds are wrapped here; any
         * other virtuals in [requestedColumns] are assumed to be produced by [delegate] (e.g. the
         * positional virtuals injected in-pipeline on file-backed splits) and consume a delegate
         * channel. The relative order of delegate-produced columns matches their order in
         * [requestedColumns]. Returns the delegate unchanged when nothing needs wrapping.
         */
        private fun injectConstantVirtuals(
                delegate: ConnectorPageSource,
                requestedColumns: List<DucklakeColumnHandle>,
                wrapKind: (VirtualKind) -> Boolean,
                blockForKind: (VirtualKind) -> Block): ConnectorPageSource
        {
            val constantBlocks: Map<Int, Block> = requestedColumns.withIndex()
                    .mapNotNull { (i, col) ->
                        val kind: VirtualKind? = col.virtualKind()
                        if (kind != null && wrapKind(kind)) i to blockForKind(kind) else null
                    }
                    .toMap()
            if (constantBlocks.isEmpty()) {
                return delegate
            }
            return VirtualColumnInjectingPageSource(delegate, requestedColumns.size, constantBlocks)
        }
    }

    /**
     * Drops tombstoned positions from each page. Two delete vocabularies apply to a split and
     * each value is matched ONLY under its own interpretation: [globalRowIds] holds global row
     * ids (`rowIdStart + file position`; Trino-written `row_id` delete files) and [localOffsets]
     * holds file-local row offsets (DuckLake-spec `pos` delete files, puffin deletion vectors,
     * inlined deletes). Cross-matching one vocabulary against the other phantom-deletes rows
     * whenever `rowIdStart < recordCount`, because the two numeric ranges overlap.
     */
    class DeleteRowFilterTransform(
            globalRowIds: Set<Long>,
            localOffsets: Set<Long>,
            private val rowIdStart: Long) : Function<SourcePage, SourcePage>
    {
        private val globalRowIds: Set<Long> = globalRowIds.toSet()
        private val localOffsets: Set<Long> = localOffsets.toSet()
        private var nextRowOffset: Long = 0

        override fun apply(page: SourcePage): SourcePage
        {
            val positionCount: Int = page.positionCount
            val retainedPositions = IntArray(positionCount)
            var retainedCount = 0

            for (position in 0 until positionCount) {
                val rowOffset: Long = nextRowOffset + position
                val rowId: Long = rowIdStart + rowOffset

                if (!globalRowIds.contains(rowId) && !localOffsets.contains(rowOffset)) {
                    retainedPositions[retainedCount] = position
                    retainedCount++
                }
            }
            nextRowOffset += positionCount

            if (retainedCount == positionCount) {
                return page
            }
            page.selectPositions(retainedPositions, 0, retainedCount)
            return page
        }
    }

    /** One positional column to inject: its output channel index and whether to add rowIdStart. */
    private class PositionalInjection(val outputPosition: Int, val addRowIdStart: Boolean)

    /**
     * Wraps a ConnectorPageSource and injects one or more synthetic positional BIGINT columns
     * derived from the cumulative file position: the MERGE $row_id and the queryable $row_id
     * (value = rowIdStart + position) and $file_row_number (value = position, 0-based). All
     * share the same per-page running offset. Must be applied BEFORE delete-file filtering so
     * the values match original file positions.
     */
    private class PositionalVirtualInjectingPageSource(
        private val delegate: ConnectorPageSource,
        private val totalChannels: Int,
        injections: List<PositionalInjection>,
        private val rowIdStart: Long) : ConnectorPageSource
    {
        private val injections: List<PositionalInjection> = injections.toList()
        private var nextRowOffset: Long = 0

        override fun getCompletedBytes(): Long = delegate.completedBytes

        override fun getCompletedPositions(): OptionalLong = delegate.completedPositions

        override fun getReadTimeNanos(): Long = delegate.readTimeNanos

        override fun isFinished(): Boolean = delegate.isFinished

        override fun getNextSourcePage(): SourcePage?
        {
            val sourcePage: SourcePage = delegate.nextSourcePage ?: return null

            val positionCount: Int = sourcePage.positionCount

            // Build each positional block from the running offset (all share the same offset).
            val injectedBlocks: HashMap<Int, Block> = HashMap(injections.size * 2)
            for (injection in injections) {
                val blockBuilder: io.trino.spi.block.BlockBuilder = BIGINT.createBlockBuilder(null, positionCount)
                if (injection.addRowIdStart) {
                    for (i in 0 until positionCount) {
                        BIGINT.writeLong(blockBuilder, rowIdStart + nextRowOffset + i)
                    }
                }
                else {
                    for (i in 0 until positionCount) {
                        BIGINT.writeLong(blockBuilder, nextRowOffset + i)
                    }
                }
                injectedBlocks[injection.outputPosition] = blockBuilder.build()
            }
            nextRowOffset += positionCount

            // Assemble the output page: injected blocks at their positions, delegate blocks elsewhere.
            val blocks = arrayOfNulls<Block>(totalChannels)
            var srcChannel = 0
            for (i in 0 until totalChannels) {
                val injected: Block? = injectedBlocks[i]
                if (injected != null) {
                    blocks[i] = injected
                }
                else {
                    blocks[i] = sourcePage.getBlock(srcChannel)
                    srcChannel++
                }
            }

            @Suppress("UNCHECKED_CAST")
            return SourcePage.create(Page(positionCount, *(blocks as Array<Block>)))
        }

        override fun getMemoryUsage(): Long = delegate.memoryUsage

        override fun getMetrics(): io.trino.spi.metrics.Metrics = delegate.metrics

        override fun isBlocked(): java.util.concurrent.CompletableFuture<*> = delegate.isBlocked()

        @Throws(IOException::class)
        override fun close() = delegate.close()
    }

    /**
     * Wraps a ConnectorPageSource and injects constant (per-split) virtual-column blocks at
     * fixed output positions, RLE-expanded to each page's position count. The delegate supplies
     * the non-virtual columns in order; [constantBlocks] maps an output channel index to its
     * single-position constant block. Used for the constant virtuals ($path, $snapshot_id), and
     * for ALL virtuals on inlined splits (where the file-bound ones are constant NULL). The
     * row-varying virtuals on file-backed splits are handled in-pipeline by
     * [PositionalVirtualInjectingPageSource] instead.
     */
    private class VirtualColumnInjectingPageSource(
        private val delegate: ConnectorPageSource,
        private val totalChannels: Int,
        private val constantBlocks: Map<Int, Block>) : ConnectorPageSource
    {
        override fun getCompletedBytes(): Long = delegate.completedBytes

        override fun getCompletedPositions(): OptionalLong = delegate.completedPositions

        override fun getReadTimeNanos(): Long = delegate.readTimeNanos

        override fun isFinished(): Boolean = delegate.isFinished

        override fun getNextSourcePage(): SourcePage?
        {
            val sourcePage: SourcePage = delegate.nextSourcePage ?: return null
            val positionCount: Int = sourcePage.positionCount

            val blocks = arrayOfNulls<Block>(totalChannels)
            var srcChannel = 0
            for (i in 0 until totalChannels) {
                val constant: Block? = constantBlocks[i]
                if (constant != null) {
                    blocks[i] = RunLengthEncodedBlock.create(constant, positionCount)
                }
                else {
                    blocks[i] = sourcePage.getBlock(srcChannel)
                    srcChannel++
                }
            }

            @Suppress("UNCHECKED_CAST")
            return SourcePage.create(Page(positionCount, *(blocks as Array<Block>)))
        }

        override fun getMemoryUsage(): Long = delegate.memoryUsage

        override fun getMetrics(): io.trino.spi.metrics.Metrics = delegate.metrics

        override fun isBlocked(): java.util.concurrent.CompletableFuture<*> = delegate.isBlocked()

        @Throws(IOException::class)
        override fun close() = delegate.close()
    }
}
