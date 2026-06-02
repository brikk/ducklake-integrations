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
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import dev.brikk.ducklake.catalog.DucklakeSnapshotChange
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
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
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
import java.util.HashMap
import java.util.HashSet
import java.util.LinkedHashMap
import java.util.Locale
import java.util.Optional
import java.util.OptionalLong
import java.util.function.Function

/**
 * PageSourceProvider for Ducklake connector.
 * Leverages Trino's ParquetPageSource for all Parquet reading logic.
 */
public class DucklakePageSourceProvider @Inject constructor(
        fileSystemFactory: TrinoFileSystemFactory,
        fileFormatDataSourceStats: FileFormatDataSourceStats,
        parquetReaderOptions: ParquetReaderOptions,
        catalog: DucklakeCatalog,
        duckDbReadCache: DucklakeMaterializedFileCache,
        duckDbS3Config: DuckDbS3Config,
        ducklakeConfig: DucklakeConfig,
        executorFactory: DucklakeDuckDbExecutorFactory)
        : ConnectorPageSourceProvider
{
    private val fileSystemFactory: TrinoFileSystemFactory = fileSystemFactory
    private val fileFormatDataSourceStats: FileFormatDataSourceStats = fileFormatDataSourceStats
    private val parquetReaderOptions: ParquetReaderOptions = parquetReaderOptions
    private val catalog: DucklakeCatalog = catalog
    private val duckDbReadCache: DucklakeMaterializedFileCache = duckDbReadCache
    private val duckDbS3Config: DuckDbS3Config = duckDbS3Config
    private val autoHttpfsThresholdBytes: Long = ducklakeConfig
            .getDuckdbAutoHttpfsThreshold().toBytes()
    private val executorFactory: DucklakeDuckDbExecutorFactory = executorFactory

    override fun createPageSource(
            transaction: ConnectorTransactionHandle?,
            session: ConnectorSession,
            split: ConnectorSplit,
            table: ConnectorTableHandle,
            tableCredentials: Optional<ConnectorTableCredentials>,
            columns: List<ColumnHandle>,
            dynamicFilter: DynamicFilter): ConnectorPageSource
    {
        if (split is DucklakeMetadataSplit) {
            return createMetadataPageSource(split, columns)
        }

        if (split is DucklakeInlinedSplit) {
            return createInlinedPageSource(split, columns)
        }

        val ducklakeSplit = split as DucklakeSplit

        // Combine file statistics domain with dynamic filter for effective predicate
        val dynamicFilterPredicate: TupleDomain<DucklakeColumnHandle> = dynamicFilter.getCurrentPredicate()
                .transformKeys(DucklakeColumnHandle::class.java::cast)
        val effectivePredicate: TupleDomain<DucklakeColumnHandle> = ducklakeSplit.fileStatisticsDomain
                .intersect(dynamicFilterPredicate)

        if (effectivePredicate.isNone()) {
            return EmptyPageSource()
        }

        // Extract column information
        val ducklakeColumns: List<DucklakeColumnHandle> = columns.stream()
                .map(DucklakeColumnHandle::class.java::cast)
                .collect(toImmutableList())

        log.debug("Creating page source for file: %s", ducklakeSplit.dataFilePath)

        try {
            // Get file system for the session
            val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)

            // Open the data file
            val dataFileLocation: Location = toLocation(ducklakeSplit.dataFilePath)
            val inputFile: TrinoInputFile = fileSystem.newInputFile(dataFileLocation)

            // Dispatch on file format
            val format = ducklakeSplit.fileFormat
            if (DucklakeSessionProperties.FORMAT_PARQUET.equals(format, ignoreCase = true)) {
                return createParquetPageSource(
                        inputFile,
                        ducklakeColumns,
                        ducklakeSplit,
                        effectivePredicate,
                        fileSystem)
            }
            if (DucklakeSessionProperties.FORMAT_DUCKDB.equals(format, ignoreCase = true)) {
                val pushedExpressions: List<String> = if (table is DucklakeTableHandle)
                    table.pushedExpressions
                else
                    emptyList()
                return createDuckDbPageSource(
                        dataFileLocation,
                        ducklakeColumns,
                        ducklakeSplit,
                        effectivePredicate,
                        pushedExpressions,
                        fileSystem,
                        session)
            }
            throw TrinoException(NOT_SUPPORTED, "Unsupported file format: " + format)
        }
        catch (e: IOException) {
            throw RuntimeException("Failed to create page source for file: " + ducklakeSplit.dataFilePath, e)
        }
    }

    private fun createInlinedPageSource(
            inlinedSplit: DucklakeInlinedSplit,
            columns: List<ColumnHandle>): ConnectorPageSource
    {
        val ducklakeColumns: List<DucklakeColumnHandle> = columns.stream()
                .map(DucklakeColumnHandle::class.java::cast)
                .collect(toImmutableList())

        // Get the full column metadata to know column names for the SQL query
        val tableColumns: List<DucklakeColumn> = catalog.getTableColumns(
                inlinedSplit.tableId, inlinedSplit.snapshotId)

        // Handle empty projection (e.g., COUNT(*)) — we still need to know the row count.
        // Query with at least one column to get the correct number of rows.
        val emptyProjection: Boolean = ducklakeColumns.isEmpty()
        val queryColumns: List<DucklakeColumn>
        if (emptyProjection) {
            // Use the first table column just to get row count
            queryColumns = ImmutableList.of(tableColumns.first())
        }
        else {
            // Build ordered list of columns matching the requested projection
            val columnById: Map<Long, DucklakeColumn> = tableColumns.stream()
                    .collect(toImmutableMap(DucklakeColumn::columnId) { col -> col })
            queryColumns = ducklakeColumns.stream()
                    .map { handle ->
                        val col = columnById.get(handle.columnId)
                        if (col == null) {
                            throw IllegalStateException("Column not found in table metadata: " + handle.columnName)
                        }
                        col
                    }
                    .collect(toImmutableList())
        }

        // Read inlined data from the metadata catalog
        val rawRows: List<List<Any?>> = catalog.readInlinedData(
                inlinedSplit.tableId,
                inlinedSplit.schemaVersion,
                inlinedSplit.snapshotId,
                queryColumns)

        if (emptyProjection) {
            // Return empty-column rows — just the count matters
            @Suppress("UNCHECKED_CAST")
            val emptyRows: List<List<Any?>> = rawRows.stream()
                    .map { _ -> ImmutableList.of<Any>() as List<Any?> }
                    .collect(toImmutableList())
            val recordSet = InMemoryRecordSet(ImmutableList.of(), emptyRows)
            log.debug("Created inlined page source with %d rows (empty projection) for tableId=%d", rawRows.size, inlinedSplit.tableId)
            return RecordPageSource(recordSet)
        }

        // Extract Trino types for each column
        val types: List<Type> = ducklakeColumns.stream()
                .map { it.columnType }
                .collect(toImmutableList())

        // Convert JDBC values to Trino-native values
        // InMemoryRecordSet expects null for null values in the row lists
        val convertedRows: List<List<Any?>> = rawRows.stream()
                .map { row ->
                    val converted: MutableList<Any?> = java.util.ArrayList(row.size)
                    for (i in 0 until row.size) {
                        converted.add(DucklakeInlinedValueConverter.convertJdbcValue(row.get(i), types.get(i)))
                    }
                    converted as List<Any?>
                }
                .collect(toImmutableList())

        log.debug("Created inlined page source with %d rows for tableId=%d", rawRows.size, inlinedSplit.tableId)

        val recordSet = InMemoryRecordSet(types, convertedRows)
        return RecordPageSource(recordSet)
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
                    .map { snapshot -> buildSnapshotRows(java.util.List.of(snapshot)) }
                    .orElse(emptyList())
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
            val row: MutableMap<String, Any?> = LinkedHashMap()
            row.put("data_file_id", file.dataFileId)
            row.put("path", file.path)
            row.put("file_format", file.fileFormat)
            row.put("record_count", file.recordCount)
            row.put("file_size_bytes", file.fileSizeBytes)
            row.put("row_id_start", file.rowIdStart)
            row.put("partition_id", file.partitionId.orElse(null))
            row.put("delete_file_path", file.deleteFilePath.orElse(null))
            rows.add(row)
        }
        return rows
    }

    private fun applyDeleteFile(fileSystem: TrinoFileSystem, split: DucklakeSplit, dataSource: ConnectorPageSource): ConnectorPageSource
    {
        val hasDeleteFiles: Boolean = !split.deleteFilePaths.isEmpty()
        val hasInlinedDeletes: Boolean = !split.inlinedDeletedRowPositions.isEmpty()
        if (!hasDeleteFiles && !hasInlinedDeletes) {
            return dataSource
        }

        // Merge parquet delete files (global row_ids) and inlined deletes (file-local row
        // offsets, from ducklake_inlined_delete_<tableId>) into a single set. The filter
        // checks both interpretations per page position, so adding both into the same set
        // is correct: a parquet delete file row_id matches the rowId branch, an inlined
        // delete row_id matches the rowOffset branch.
        val deletedRows: MutableSet<Long> = HashSet(split.inlinedDeletedRowPositions)
        for (deleteFilePath in split.deleteFilePaths) {
            if (isPuffinPath(deleteFilePath)) {
                deletedRows.addAll(DucklakePuffinDeleteReader.readDeletedPositions(
                        fileSystem.newInputFile(toLocation(deleteFilePath))))
            }
            else {
                deletedRows.addAll(readDeletedRowsFromFile(fileSystem, deleteFilePath, split))
            }
        }

        if (deletedRows.isEmpty()) {
            return dataSource
        }

        log.debug("Applying deletions to data file %s: %d parquet delete file(s), %d inlined deletes, %d total deleted rows",
                split.dataFilePath,
                split.deleteFilePaths.size,
                split.inlinedDeletedRowPositions.size,
                deletedRows.size)
        return TransformConnectorPageSource.create(dataSource, DeleteRowFilterTransform(deletedRows, split.rowIdStart))
    }

    private fun readDeletedRowsFromFile(fileSystem: TrinoFileSystem, deleteFilePath: String, split: DucklakeSplit): Set<Long>
    {
        // Delete files carry their own footer_size in ducklake_delete_file.
        val deleteFooterHint: Long = split.deleteFileFooterSizes.getOrDefault(deleteFilePath, 0L)
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
            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(dataSource, split.footerSize)

            // Read Parquet metadata
            val parquetMetadata: ParquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions,
                    Optional.empty(),
                    Optional.empty())
            val fileMetadata: FileMetadata = parquetMetadata.getFileMetaData()
            val fileSchema: MessageType = fileMetadata.getSchema()
            val dataSourceId: ParquetDataSourceId = dataSource.getId()
            val descriptorsByPath: Map<List<String>, ColumnDescriptor> = getDescriptors(fileSchema, fileSchema)
            // B3b: when the split carries active position deletes, disable in-reader predicate
            // pushdown (row-group pruning + page-level skipping). RowIdInjectingPageSource and
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
            val parquetTupleDomain: TupleDomain<ColumnDescriptor> = if (splitHasActiveDeletes(split))
                    TupleDomain.all()
                else
                    toParquetTupleDomain(descriptorsByPath, effectivePredicate)
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

            // Separate out the synthetic $row_id column (used for DELETE/UPDATE/MERGE)
            var rowIdOutputPosition = -1
            val fileColumns: MutableList<DucklakeColumnHandle> = ArrayList()
            for (i in 0 until columns.size) {
                if (columns.get(i).isRowIdColumn()) {
                    rowIdOutputPosition = i
                }
                else {
                    fileColumns.add(columns.get(i))
                }
            }

            // Build list of columns to read, handling missing columns for schema evolution
            val parquetColumns: ImmutableList.Builder<Column> = ImmutableList.builder()
            val messageColumnIO: MessageColumnIO = getColumnIO(fileSchema, fileSchema)
            val transforms: TransformConnectorPageSource.Builder = TransformConnectorPageSource.builder()
            var parquetColumnOrdinal = 0

            // Build field_id → ColumnIO index for field_id-based column matching (schema evolution: renames)
            val fieldIdToColumnIO: MutableMap<Int, ColumnIO> = HashMap()
            for (field in fileSchema.getFields()) {
                if (field.getId() != null) {
                    val childIO: ColumnIO? = messageColumnIO.getChild(field.getName())
                    if (childIO != null) {
                        fieldIdToColumnIO.put(field.getId().intValue(), childIO)
                    }
                }
            }

            for (column in fileColumns) {
                val columnName: String = column.columnName
                // Try name-based match first, then fall back to field_id match (handles column renames)
                var columnIO: ColumnIO? = messageColumnIO.getChild(columnName)
                if (columnIO == null && column.columnId > 0) {
                    columnIO = fieldIdToColumnIO.get(column.columnId.toInt())
                }
                // Finally, consult the catalog's name_map for files registered via
                // add_files — covers the case where the parquet column name differs
                // from the table column name (e.g. case-difference, or a column-rename
                // where the file kept its original name). The map is empty for files
                // without a mapping_id, so this is a no-op for INSERT-written files.
                if (columnIO == null) {
                    val parquetSourceName: String? = split.fieldIdToParquetSourceName.get(column.columnId)
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
                if (field.isEmpty()) {
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
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    presentColumns,
                    false, // appendRowNumberColumn
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    { exception -> handleParquetException(dataSourceId, exception) },
                    if (parquetTupleDomain.isAll()) Optional.empty() else Optional.of(parquetPredicate),
                    Optional.empty(), // bloomFilterStore
                    Optional.empty()) // rowFilter

            // Wrap in ParquetPageSource, apply column transforms for missing columns,
            // then apply merge-on-read delete filtering if present
            var pageSource: ConnectorPageSource = ParquetPageSource(parquetReader)
            pageSource = transforms.build(pageSource)

            // Inject $row_id column before delete filtering so row IDs reflect original file positions
            if (rowIdOutputPosition >= 0) {
                pageSource = RowIdInjectingPageSource(pageSource, fileColumns.size, rowIdOutputPosition, split.rowIdStart)
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
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex)
                    }
                }
            }
            throw RuntimeException("Failed to create Parquet page source for file: " + split.dataFilePath, e)
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
            session: ConnectorSession): ConnectorPageSource
    {
        // Separate $row_id from file-resident columns. The .db file does not store row IDs;
        // they are injected after the data page source returns its rows, exactly as on the
        // parquet path.
        var rowIdOutputPosition = -1
        val fileColumns: MutableList<DucklakeColumnHandle> = ArrayList()
        for (i in 0 until columns.size) {
            if (columns.get(i).isRowIdColumn()) {
                rowIdOutputPosition = i
            }
            else {
                fileColumns.add(columns.get(i))
            }
        }

        // Empty projection (e.g. COUNT(*)) is handled inside DuckDbFilePageSource by
        // issuing a synthetic SELECT 1 and emitting empty-block pages with the right
        // row count.

        val attachTarget: DuckDbAttachTarget = resolveDuckDbAttachTarget(
                session, dataFileLocation, fileSystem, split)

        val fileColumnTypes: List<Type> = fileColumns.stream()
                .map { it.columnType }
                .collect(toImmutableList())

        // Restrict the pushed-down predicate to columns we actually project (filter
        // pipeline still applies any not-pushed-down or non-projected predicates above).
        // B3b: when the split carries active position deletes, drop the pushed predicate so
        // DuckDB returns rows contiguously from row 0. RowIdInjectingPageSource's cumulative
        // nextRowOffset assumes contiguous output; predicate-pushed DuckDB scans return only
        // matching rows, breaking the position math. Trino still filters above the page source.
        val filePredicate: TupleDomain<DucklakeColumnHandle> = if (splitHasActiveDeletes(split))
                TupleDomain.all()
            else
                effectivePredicate.filter { col, _ -> fileColumns.contains(col) }

        // Carry Trino's session zone through to the executor so it can run
        // `SET TimeZone` on attach. Required for Tier C correctness (TIMESTAMP
        // WITH TIME ZONE pushdown) and harmlessly deterministic for Tier A/B
        // (DuckDB's default zone is the JVM system TZ — Costa Rica on a dev box,
        // UTC in CI — so an explicit SET is the only way to make duckdb-format
        // reads reproducible across deployment environments). See
        // dev-docs/archive/REPORT-datetime-tz-handling.md.
        val duckDbTimeZone: Optional<String> = Optional.ofNullable(
                TrinoTimeZoneNormaliser.normalise(session.getTimeZoneKey().getId()))

        // B3b: drop pushed complex expressions when the split has active deletes — same
        // reasoning as the TupleDomain drop above. DuckDB-side filtering would return only
        // matching rows, breaking RowIdInjectingPageSource's cumulative-offset math.
        val effectivePushedExpressions: List<String> = if (splitHasActiveDeletes(split)) emptyList() else pushedExpressions
        var pageSource: ConnectorPageSource = DuckDbFilePageSource(
                executorFactory.create(), attachTarget, fileColumns, fileColumnTypes, filePredicate, effectivePushedExpressions,
                duckDbTimeZone)

        if (rowIdOutputPosition >= 0) {
            pageSource = RowIdInjectingPageSource(pageSource, fileColumns.size, rowIdOutputPosition, split.rowIdStart)
        }

        pageSource = applyDeleteFile(fileSystem, split, pageSource)

        log.debug("Created DuckDB page source for %d columns from file: %s",
                columns.size, split.dataFilePath)
        return pageSource
    }

    /**
     * Decide whether to materialize the {@code .db} file to local tmp and ATTACH that
     * path, or load DuckDB's httpfs extension and ATTACH the remote {@code s3://} URL
     * directly. Driven by the {@code duckdb_read_mode} session property; {@code auto}
     * (the default) consults the {@code ducklake.duckdb.auto-httpfs-threshold} config.
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
            else -> throw TrinoException(NOT_SUPPORTED, "Unsupported duckdb_read_mode: " + mode)
        }

        val url: String = dataFileLocation.toString()
        val isS3: Boolean = url.startsWith("s3://") || url.startsWith("s3a://") || url.startsWith("s3n://")
        if (useHttpfs && isS3) {
            return DuckDbAttachTarget.HttpfsS3(url, duckDbS3Config)
        }
        // httpfs against a non-s3 target degrades to materialize — the local path is
        // already directly attachable, no need for a remote-streaming protocol.
        val localPath: java.nio.file.Path = duckDbReadCache.materialize(
                fileSystem, dataFileLocation, split.fileSizeBytes)
        return DuckDbAttachTarget.LocalPath(localPath)
    }

    public companion object {
        private val log: Logger = Logger.get(DucklakePageSourceProvider::class.java)

        private fun buildSnapshotRows(snapshots: List<DucklakeSnapshot>): List<Map<String, Any?>>
        {
            val rows: MutableList<Map<String, Any?>> = ArrayList(snapshots.size)
            for (snapshot in snapshots) {
                val row: MutableMap<String, Any?> = LinkedHashMap()
                row.put("snapshot_id", snapshot.snapshotId)
                row.put("snapshot_time", snapshot.snapshotTime)
                row.put("schema_version", snapshot.schemaVersion)
                row.put("next_catalog_id", snapshot.nextCatalogId)
                row.put("next_file_id", snapshot.nextFileId)
                rows.add(row)
            }
            return rows
        }

        private fun buildSnapshotChangeRows(changes: List<DucklakeSnapshotChange>): List<Map<String, Any?>>
        {
            val rows: MutableList<Map<String, Any?>> = ArrayList(changes.size)
            for (change in changes) {
                val row: MutableMap<String, Any?> = LinkedHashMap()
                row.put("snapshot_id", change.snapshotId)
                row.put("changes_made", change.changesMade.orElse(null))
                row.put("author", change.author.orElse(null))
                row.put("commit_message", change.commitMessage.orElse(null))
                row.put("commit_extra_info", change.commitExtraInfo.orElse(null))
                rows.add(row)
            }
            return rows
        }

        private fun projectMetadataRow(row: Map<String, Any?>, columns: List<DucklakeColumnHandle>, types: List<Type>): List<Any?>
        {
            val projected: MutableList<Any?> = ArrayList(columns.size)
            for (index in 0 until columns.size) {
                val value: Any? = row.get(columns.get(index).columnName)
                projected.add(toNativeMetadataValue(value, types.get(index)))
            }
            return projected
        }

        private fun toNativeMetadataValue(value: Any?, type: Type): Any?
        {
            if (value == null) {
                return null
            }
            if (type.equals(TIMESTAMP_TZ_MILLIS)) {
                val instant: Instant = value as Instant
                return io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone(instant.toEpochMilli(), UTC_KEY)
            }
            if (type.equals(BIGINT) || type.equals(INTEGER)) {
                return value
            }
            return Slices.utf8Slice(value.toString())
        }

        private fun toParquetTupleDomain(
                descriptorsByPath: Map<List<String>, ColumnDescriptor>,
                effectivePredicate: TupleDomain<DucklakeColumnHandle>): TupleDomain<ColumnDescriptor>
        {
            if (effectivePredicate.isNone()) {
                return TupleDomain.none()
            }
            if (effectivePredicate.isAll()) {
                return TupleDomain.all()
            }

            val predicate: ImmutableMap.Builder<ColumnDescriptor, Domain> = ImmutableMap.builder()
            val topLevelDescriptors: Map<String, ColumnDescriptor> = descriptorsByPath.entries.stream()
                    .filter { entry -> entry.key.size == 1 }
                    .collect(toImmutableMap(
                            { entry -> entry.key.get(0).lowercase(Locale.ENGLISH) },
                            { entry -> entry.value },
                            { first, _ -> first }))

            val domains: Optional<Map<DucklakeColumnHandle, Domain>> = effectivePredicate.getDomains()
            if (domains.isEmpty()) {
                return TupleDomain.all()
            }

            for (entry in domains.get().entries) {
                val columnHandle: DucklakeColumnHandle = entry.key
                val descriptor: ColumnDescriptor? = topLevelDescriptors.get(columnHandle.columnName.lowercase(Locale.ENGLISH))
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
         * delete files or inlined deletes from {@code ducklake_inlined_delete_<tableId>}.
         *
         * <p>When this returns {@code true}, the page-source pipeline must NOT push the query
         * predicate down into the parquet reader or the DuckDB scan: doing so prunes row groups
         * / skips rows inside the underlying file, breaking the cumulative-offset math that
         * {@link RowIdInjectingPageSource} and {@link DeleteRowFilterTransform} use to compute
         * file-absolute positions for delete matching (B3b — see the bug trace and option-(B)
         * fix in {@code .ai/kotlin-port/BEFORE-RESUME.md}).
         */
        @JvmStatic
        fun splitHasActiveDeletes(split: DucklakeSplit): Boolean
        {
            return !split.deleteFilePaths.isEmpty() || !split.inlinedDeletedRowPositions.isEmpty()
        }

        private fun isPuffinPath(path: String): Boolean
        {
            // DuckLake's delete-file path always ends with .puffin when format='puffin'
            // (see vendor/ducklake/src/storage/ducklake_delete.cpp:161 — the writer hardcodes
            // "ducklake-<uuid>-delete.puffin"). Catalog format='puffin' has already been
            // permitted by DucklakeSplitManager.validateDeleteFileFormats by the time we get
            // here; dispatching on extension keeps the split schema stable and matches the
            // pattern Trino's Iceberg connector uses for puffin DV files.
            return path.regionMatches(path.length - ".puffin".length, ".puffin", 0, ".puffin".length, ignoreCase = true)
        }

        private fun toLocation(path: String): Location
        {
            val location: Location = Location.of(path)
            if (location.scheme().isPresent()) {
                return location
            }
            return Location.of(Path.of(path).toUri().toString())
        }

        private fun handleParquetException(dataSourceId: ParquetDataSourceId, exception: Exception): RuntimeException
        {
            if (exception is TrinoException) {
                return exception
            }
            return TrinoException(
                    NOT_SUPPORTED,
                    "Error reading Parquet file: " + dataSourceId,
                    exception)
        }

        /**
         * Build the constant block emitted by {@link io.trino.plugin.hive.TransformConnectorPageSource}
         * for a column not present in the parquet body. Defaults to a single-position NULL block;
         * when the split carries a catalog-recorded partition value for this column (hive-style
         * external imports), parses the string value and projects it as a constant instead.
         */
        private fun buildMissingColumnBlock(column: DucklakeColumnHandle, split: DucklakeSplit): Block
        {
            val partitionValue: String? = split.partitionValuesByColumnId.get(column.columnId)
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
    }

    class DeleteRowFilterTransform : Function<SourcePage, SourcePage>
    {
        private val deletedRows: Set<Long>
        private val rowIdStart: Long
        private var nextRowOffset: Long = 0

        constructor(deletedRows: Set<Long>, rowIdStart: Long) {
            this.deletedRows = deletedRows.toSet()
            this.rowIdStart = rowIdStart
        }

        override fun apply(page: SourcePage): SourcePage
        {
            val positionCount: Int = page.getPositionCount()
            val retainedPositions = IntArray(positionCount)
            var retainedCount = 0

            for (position in 0 until positionCount) {
                val rowOffset: Long = nextRowOffset + position
                val rowId: Long = rowIdStart + rowOffset

                // Ducklake delete files conceptually store row ids. We also check row offsets to
                // tolerate producers that store file-local row index values.
                if (!deletedRows.contains(rowId) && !deletedRows.contains(rowOffset)) {
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

    /**
     * Wraps a ConnectorPageSource and injects a synthetic $row_id BIGINT column.
     * The row ID is computed as rowIdStart + (cumulative file position).
     * This must be applied BEFORE delete file filtering so the IDs match original file positions.
     */
    private class RowIdInjectingPageSource : ConnectorPageSource
    {
        private val delegate: ConnectorPageSource
        private val delegateChannelCount: Int
        private val rowIdOutputPosition: Int
        private val rowIdStart: Long
        private var nextRowOffset: Long = 0

        constructor(delegate: ConnectorPageSource, delegateChannelCount: Int, rowIdOutputPosition: Int, rowIdStart: Long) {
            this.delegate = delegate
            this.delegateChannelCount = delegateChannelCount
            this.rowIdOutputPosition = rowIdOutputPosition
            this.rowIdStart = rowIdStart
        }

        override fun getCompletedBytes(): Long
        {
            return delegate.getCompletedBytes()
        }

        override fun getCompletedPositions(): OptionalLong
        {
            return delegate.getCompletedPositions()
        }

        override fun getReadTimeNanos(): Long
        {
            return delegate.getReadTimeNanos()
        }

        override fun isFinished(): Boolean
        {
            return delegate.isFinished()
        }

        override fun getNextSourcePage(): SourcePage?
        {
            val sourcePage: SourcePage = delegate.getNextSourcePage() ?: return null

            val positionCount: Int = sourcePage.getPositionCount()

            // Build the row ID block
            val blockBuilder: io.trino.spi.block.BlockBuilder = BIGINT.createBlockBuilder(null, positionCount)
            for (i in 0 until positionCount) {
                BIGINT.writeLong(blockBuilder, rowIdStart + nextRowOffset + i)
            }
            nextRowOffset += positionCount
            val rowIdBlock: Block = blockBuilder.build()

            // Build a new page with the row ID block inserted at the correct position
            val totalChannels: Int = delegateChannelCount + 1
            val blocks = arrayOfNulls<Block>(totalChannels)
            var srcChannel = 0
            for (i in 0 until totalChannels) {
                if (i == rowIdOutputPosition) {
                    blocks[i] = rowIdBlock
                }
                else {
                    blocks[i] = sourcePage.getBlock(srcChannel)
                    srcChannel++
                }
            }

            @Suppress("UNCHECKED_CAST")
            return SourcePage.create(Page(positionCount, *(blocks as Array<Block>)))
        }

        override fun getMemoryUsage(): Long
        {
            return delegate.getMemoryUsage()
        }

        @Throws(IOException::class)
        override fun close()
        {
            delegate.close()
        }
    }
}
