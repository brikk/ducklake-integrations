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
package dev.brikk.ducklake.trino.plugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeColumn;
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeSnapshot;
import dev.brikk.ducklake.catalog.DucklakeSnapshotChange;
import io.trino.plugin.hive.TransformConnectorPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

/**
 * PageSourceProvider for Ducklake connector.
 * Leverages Trino's ParquetPageSource for all Parquet reading logic.
 */
public class DucklakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(DucklakePageSourceProvider.class);

    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;
    private final DucklakeCatalog catalog;
    private final DucklakeMaterializedFileCache duckDbReadCache;
    private final DuckDbS3Config duckDbS3Config;
    private final long autoHttpfsThresholdBytes;
    private final DucklakeDuckDbExecutorFactory executorFactory;

    @Inject
    public DucklakePageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderOptions parquetReaderOptions,
            DucklakeCatalog catalog,
            DucklakeMaterializedFileCache duckDbReadCache,
            DuckDbS3Config duckDbS3Config,
            DucklakeConfig ducklakeConfig,
            DucklakeDuckDbExecutorFactory executorFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderOptions, "parquetReaderOptions is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.duckDbReadCache = requireNonNull(duckDbReadCache, "duckDbReadCache is null");
        this.duckDbS3Config = requireNonNull(duckDbS3Config, "duckDbS3Config is null");
        this.autoHttpfsThresholdBytes = requireNonNull(ducklakeConfig, "ducklakeConfig is null")
                .getDuckdbAutoHttpfsThreshold().toBytes();
        this.executorFactory = requireNonNull(executorFactory, "executorFactory is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");

        if (split instanceof DucklakeMetadataSplit metadataSplit) {
            return createMetadataPageSource(metadataSplit, columns);
        }

        if (split instanceof DucklakeInlinedSplit inlinedSplit) {
            return createInlinedPageSource(inlinedSplit, columns);
        }

        DucklakeSplit ducklakeSplit = (DucklakeSplit) split;

        // Combine file statistics domain with dynamic filter for effective predicate
        TupleDomain<DucklakeColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(DucklakeColumnHandle.class::cast);
        TupleDomain<DucklakeColumnHandle> effectivePredicate = ducklakeSplit.fileStatisticsDomain()
                .intersect(dynamicFilterPredicate);

        if (effectivePredicate.isNone()) {
            return new EmptyPageSource();
        }

        // Extract column information
        List<DucklakeColumnHandle> ducklakeColumns = columns.stream()
                .map(DucklakeColumnHandle.class::cast)
                .collect(toImmutableList());

        log.debug("Creating page source for file: %s", ducklakeSplit.dataFilePath());

        try {
            // Get file system for the session
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);

            // Open the data file
            Location dataFileLocation = toLocation(ducklakeSplit.dataFilePath());
            TrinoInputFile inputFile = fileSystem.newInputFile(dataFileLocation);

            // Dispatch on file format
            String format = ducklakeSplit.fileFormat();
            if (DucklakeSessionProperties.FORMAT_PARQUET.equalsIgnoreCase(format)) {
                return createParquetPageSource(
                        inputFile,
                        ducklakeColumns,
                        ducklakeSplit,
                        effectivePredicate,
                        fileSystem);
            }
            if (DucklakeSessionProperties.FORMAT_DUCKDB.equalsIgnoreCase(format)) {
                return createDuckDbPageSource(
                        dataFileLocation,
                        ducklakeColumns,
                        ducklakeSplit,
                        effectivePredicate,
                        fileSystem,
                        session);
            }
            throw new TrinoException(NOT_SUPPORTED, "Unsupported file format: " + format);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create page source for file: " + ducklakeSplit.dataFilePath(), e);
        }
    }

    private ConnectorPageSource createInlinedPageSource(
            DucklakeInlinedSplit inlinedSplit,
            List<ColumnHandle> columns)
    {
        List<DucklakeColumnHandle> ducklakeColumns = columns.stream()
                .map(DucklakeColumnHandle.class::cast)
                .collect(toImmutableList());

        // Get the full column metadata to know column names for the SQL query
        List<DucklakeColumn> tableColumns = catalog.getTableColumns(
                inlinedSplit.tableId(), inlinedSplit.snapshotId());

        // Handle empty projection (e.g., COUNT(*)) — we still need to know the row count.
        // Query with at least one column to get the correct number of rows.
        boolean emptyProjection = ducklakeColumns.isEmpty();
        List<DucklakeColumn> queryColumns;
        if (emptyProjection) {
            // Use the first table column just to get row count
            queryColumns = ImmutableList.of(tableColumns.getFirst());
        }
        else {
            // Build ordered list of columns matching the requested projection
            Map<Long, DucklakeColumn> columnById = tableColumns.stream()
                    .collect(toImmutableMap(DucklakeColumn::columnId, col -> col));
            queryColumns = ducklakeColumns.stream()
                    .map(handle -> {
                        DucklakeColumn col = columnById.get(handle.columnId());
                        if (col == null) {
                            throw new IllegalStateException("Column not found in table metadata: " + handle.columnName());
                        }
                        return col;
                    })
                    .collect(toImmutableList());
        }

        // Read inlined data from the metadata catalog
        List<List<Object>> rawRows = catalog.readInlinedData(
                inlinedSplit.tableId(),
                inlinedSplit.schemaVersion(),
                inlinedSplit.snapshotId(),
                queryColumns);

        if (emptyProjection) {
            // Return empty-column rows — just the count matters
            List<List<Object>> emptyRows = rawRows.stream()
                    .map(_ -> (List<Object>) ImmutableList.of())
                    .collect(toImmutableList());
            InMemoryRecordSet recordSet = new InMemoryRecordSet(ImmutableList.of(), emptyRows);
            log.debug("Created inlined page source with %d rows (empty projection) for tableId=%d", rawRows.size(), inlinedSplit.tableId());
            return new RecordPageSource(recordSet);
        }

        // Extract Trino types for each column
        List<Type> types = ducklakeColumns.stream()
                .map(DucklakeColumnHandle::columnType)
                .collect(toImmutableList());

        // Convert JDBC values to Trino-native values
        // InMemoryRecordSet expects null for null values in the row lists
        List<List<Object>> convertedRows = rawRows.stream()
                .map(row -> {
                    List<Object> converted = new java.util.ArrayList<>(row.size());
                    for (int i = 0; i < row.size(); i++) {
                        converted.add(DucklakeInlinedValueConverter.convertJdbcValue(row.get(i), types.get(i)));
                    }
                    return (List<Object>) converted;
                })
                .collect(toImmutableList());

        log.debug("Created inlined page source with %d rows for tableId=%d", rawRows.size(), inlinedSplit.tableId());

        InMemoryRecordSet recordSet = new InMemoryRecordSet(types, convertedRows);
        return new RecordPageSource(recordSet);
    }

    private ConnectorPageSource createMetadataPageSource(DucklakeMetadataSplit metadataSplit, List<ColumnHandle> columns)
    {
        List<DucklakeColumnHandle> projectedColumns = columns.stream()
                .map(DucklakeColumnHandle.class::cast)
                .collect(toImmutableList());
        List<Type> projectedTypes = projectedColumns.stream()
                .map(DucklakeColumnHandle::columnType)
                .collect(toImmutableList());

        List<Map<String, Object>> rows = switch (metadataSplit.metadataTableType()) {
            case FILES -> buildFilesRows(metadataSplit);
            case SNAPSHOTS -> buildSnapshotRows(catalog.listSnapshots());
            case CURRENT_SNAPSHOT -> catalog.getSnapshot(metadataSplit.snapshotId())
                    .map(snapshot -> buildSnapshotRows(List.of(snapshot)))
                    .orElse(List.of());
            case SNAPSHOT_CHANGES -> buildSnapshotChangeRows(catalog.listSnapshotChanges());
        };

        List<List<Object>> projectedRows = rows.stream()
                .map(row -> projectMetadataRow(row, projectedColumns, projectedTypes))
                .collect(toImmutableList());

        return new RecordPageSource(new InMemoryRecordSet(projectedTypes, projectedRows));
    }

    private List<Map<String, Object>> buildFilesRows(DucklakeMetadataSplit metadataSplit)
    {
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(metadataSplit.baseTableId(), metadataSplit.snapshotId());
        List<Map<String, Object>> rows = new ArrayList<>(dataFiles.size());
        for (DucklakeDataFile file : dataFiles) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("data_file_id", file.dataFileId());
            row.put("path", file.path());
            row.put("file_format", file.fileFormat());
            row.put("record_count", file.recordCount());
            row.put("file_size_bytes", file.fileSizeBytes());
            row.put("row_id_start", file.rowIdStart());
            row.put("partition_id", file.partitionId().orElse(null));
            row.put("delete_file_path", file.deleteFilePath().orElse(null));
            rows.add(row);
        }
        return rows;
    }

    private static List<Map<String, Object>> buildSnapshotRows(List<DucklakeSnapshot> snapshots)
    {
        List<Map<String, Object>> rows = new ArrayList<>(snapshots.size());
        for (DucklakeSnapshot snapshot : snapshots) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("snapshot_id", snapshot.snapshotId());
            row.put("snapshot_time", snapshot.snapshotTime());
            row.put("schema_version", snapshot.schemaVersion());
            row.put("next_catalog_id", snapshot.nextCatalogId());
            row.put("next_file_id", snapshot.nextFileId());
            rows.add(row);
        }
        return rows;
    }

    private static List<Map<String, Object>> buildSnapshotChangeRows(List<DucklakeSnapshotChange> changes)
    {
        List<Map<String, Object>> rows = new ArrayList<>(changes.size());
        for (DucklakeSnapshotChange change : changes) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("snapshot_id", change.snapshotId());
            row.put("changes_made", change.changesMade().orElse(null));
            row.put("author", change.author().orElse(null));
            row.put("commit_message", change.commitMessage().orElse(null));
            row.put("commit_extra_info", change.commitExtraInfo().orElse(null));
            rows.add(row);
        }
        return rows;
    }

    private static List<Object> projectMetadataRow(Map<String, Object> row, List<DucklakeColumnHandle> columns, List<Type> types)
    {
        List<Object> projected = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++) {
            Object value = row.get(columns.get(index).columnName());
            projected.add(toNativeMetadataValue(value, types.get(index)));
        }
        return projected;
    }

    private static Object toNativeMetadataValue(Object value, Type type)
    {
        if (value == null) {
            return null;
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            Instant instant = (Instant) value;
            return io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone(instant.toEpochMilli(), UTC_KEY);
        }
        if (type.equals(BIGINT) || type.equals(INTEGER)) {
            return value;
        }
        return Slices.utf8Slice(value.toString());
    }

    private ConnectorPageSource createParquetPageSource(
            TrinoInputFile inputFile,
            List<DucklakeColumnHandle> columns,
            DucklakeSplit split,
            TupleDomain<DucklakeColumnHandle> effectivePredicate,
            TrinoFileSystem fileSystem)
            throws IOException
    {
        // Create memory context for reading
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            // Create Parquet data source
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(split.fileSizeBytes()),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats);

            // Feed DuckLake's footer_size hint to Trino's Parquet reader. For typical
            // footers (<48 KB), this trims the blind tail read down to the exact bytes;
            // for oversized footers, it replaces the fallback two-round-trip path with a
            // single-shot read. See FooterPrefetchingParquetDataSource.
            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(dataSource, split.footerSize());

            // Read Parquet metadata
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions.getMaxFooterReadSize(),
                    Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetadata.getSchema();
            ParquetDataSourceId dataSourceId = dataSource.getId();
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = toParquetTupleDomain(descriptorsByPath, effectivePredicate);
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC);
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    0,
                    split.fileSizeBytes(),
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    Domain.DEFAULT_COMPACTION_THRESHOLD,
                    parquetReaderOptions);

            // Separate out the synthetic $row_id column (used for DELETE/UPDATE/MERGE)
            int rowIdOutputPosition = -1;
            List<DucklakeColumnHandle> fileColumns = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).isRowIdColumn()) {
                    rowIdOutputPosition = i;
                }
                else {
                    fileColumns.add(columns.get(i));
                }
            }

            // Build list of columns to read, handling missing columns for schema evolution
            ImmutableList.Builder<Column> parquetColumns = ImmutableList.builder();
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);
            TransformConnectorPageSource.Builder transforms = TransformConnectorPageSource.builder();
            int parquetColumnOrdinal = 0;

            // Build field_id → ColumnIO index for field_id-based column matching (schema evolution: renames)
            Map<Integer, ColumnIO> fieldIdToColumnIO = new HashMap<>();
            for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
                if (field.getId() != null) {
                    ColumnIO childIO = messageColumnIO.getChild(field.getName());
                    if (childIO != null) {
                        fieldIdToColumnIO.put(field.getId().intValue(), childIO);
                    }
                }
            }

            for (DucklakeColumnHandle column : fileColumns) {
                String columnName = column.columnName();
                // Try name-based match first, then fall back to field_id match (handles column renames)
                ColumnIO columnIO = messageColumnIO.getChild(columnName);
                if (columnIO == null && column.columnId() > 0) {
                    columnIO = fieldIdToColumnIO.get((int) column.columnId());
                }
                // Finally, consult the catalog's name_map for files registered via
                // add_files — covers the case where the parquet column name differs
                // from the table column name (e.g. case-difference, or a column-rename
                // where the file kept its original name). The map is empty for files
                // without a mapping_id, so this is a no-op for INSERT-written files.
                if (columnIO == null) {
                    String parquetSourceName = split.fieldIdToParquetSourceName().get(column.columnId());
                    if (parquetSourceName != null && !parquetSourceName.equals(columnName)) {
                        columnIO = messageColumnIO.getChild(parquetSourceName);
                    }
                }

                if (columnIO == null) {
                    // Missing column in file. Two cases:
                    //   (1) hive-style external file: parquet body omits the partition column,
                    //       but the catalog has a value for it via ducklake_file_partition_value.
                    //       Project that value as a constant block.
                    //   (2) schema evolution / genuinely missing: return NULL.
                    transforms.constantValue(buildMissingColumnBlock(column, split));
                    continue;
                }

                Optional<Field> field = DucklakeParquetTypeUtils.constructField(
                        column.columnType(),
                        columnIO);
                if (field.isEmpty()) {
                    // Could not construct field — return nulls (or partition constant if available)
                    transforms.constantValue(buildMissingColumnBlock(column, split));
                    continue;
                }

                parquetColumns.add(new Column(columnName, field.get()));
                transforms.column(parquetColumnOrdinal);
                parquetColumnOrdinal++;
            }

            List<Column> presentColumns = parquetColumns.build();

            // Create ParquetReader with only the columns present in the file
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    presentColumns,
                    false, // appendRowNumberColumn
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    exception -> handleParquetException(dataSourceId, exception),
                    parquetTupleDomain.isAll() ? Optional.empty() : Optional.of(parquetPredicate),
                    Optional.empty(), // bloomFilterStore
                    Optional.empty()); // rowFilter

            // Wrap in ParquetPageSource, apply column transforms for missing columns,
            // then apply merge-on-read delete filtering if present
            ConnectorPageSource pageSource = new ParquetPageSource(parquetReader);
            pageSource = transforms.build(pageSource);

            // Inject $row_id column before delete filtering so row IDs reflect original file positions
            if (rowIdOutputPosition >= 0) {
                pageSource = new RowIdInjectingPageSource(pageSource, fileColumns.size(), rowIdOutputPosition, split.rowIdStart());
            }

            pageSource = applyDeleteFile(fileSystem, split, pageSource);

            log.debug("Created Parquet page source for %d columns from file: %s",
                    columns.size(), split.dataFilePath());

            return pageSource;
        }
        catch (IOException | RuntimeException e) {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException ex) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex);
                    }
                }
            }
            throw new RuntimeException("Failed to create Parquet page source for file: " + split.dataFilePath(), e);
        }
    }

    private ConnectorPageSource createDuckDbPageSource(
            Location dataFileLocation,
            List<DucklakeColumnHandle> columns,
            DucklakeSplit split,
            TupleDomain<DucklakeColumnHandle> effectivePredicate,
            TrinoFileSystem fileSystem,
            ConnectorSession session)
            throws IOException
    {
        // Separate $row_id from file-resident columns. The .db file does not store row IDs;
        // they are injected after the data page source returns its rows, exactly as on the
        // parquet path.
        int rowIdOutputPosition = -1;
        List<DucklakeColumnHandle> fileColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isRowIdColumn()) {
                rowIdOutputPosition = i;
            }
            else {
                fileColumns.add(columns.get(i));
            }
        }

        // Empty projection (e.g. COUNT(*)) is handled inside DuckDbFilePageSource by
        // issuing a synthetic SELECT 1 and emitting empty-block pages with the right
        // row count.

        DuckDbAttachTarget attachTarget = resolveDuckDbAttachTarget(
                session, dataFileLocation, fileSystem, split);

        List<Type> fileColumnTypes = fileColumns.stream()
                .map(DucklakeColumnHandle::columnType)
                .collect(toImmutableList());

        // Restrict the pushed-down predicate to columns we actually project (filter
        // pipeline still applies any not-pushed-down or non-projected predicates above).
        TupleDomain<DucklakeColumnHandle> filePredicate = effectivePredicate.filter(
                (col, _) -> fileColumns.contains(col));

        ConnectorPageSource pageSource = new DuckDbFilePageSource(
                executorFactory.create(), attachTarget, fileColumns, fileColumnTypes, filePredicate);

        if (rowIdOutputPosition >= 0) {
            pageSource = new RowIdInjectingPageSource(pageSource, fileColumns.size(), rowIdOutputPosition, split.rowIdStart());
        }

        pageSource = applyDeleteFile(fileSystem, split, pageSource);

        log.debug("Created DuckDB page source for %d columns from file: %s",
                columns.size(), split.dataFilePath());
        return pageSource;
    }

    /**
     * Decide whether to materialize the {@code .db} file to local tmp and ATTACH that
     * path, or load DuckDB's httpfs extension and ATTACH the remote {@code s3://} URL
     * directly. Driven by the {@code duckdb_read_mode} session property; {@code auto}
     * (the default) consults the {@code ducklake.duckdb.auto-httpfs-threshold} config.
     */
    private DuckDbAttachTarget resolveDuckDbAttachTarget(
            ConnectorSession session,
            Location dataFileLocation,
            TrinoFileSystem fileSystem,
            DucklakeSplit split)
            throws IOException
    {
        String mode = DucklakeSessionProperties.getDuckDbReadMode(session);
        boolean useHttpfs = switch (mode.toLowerCase(Locale.ROOT)) {
            case DucklakeSessionProperties.READ_MODE_MATERIALIZE -> false;
            case DucklakeSessionProperties.READ_MODE_HTTPFS -> true;
            // 'auto' picks per-file. Below the threshold the materialize cache wins
            // (small files are cheap to download and warm reads are then local). At or
            // above the threshold we stream blocks via httpfs to avoid the full pull.
            case DucklakeSessionProperties.READ_MODE_AUTO -> split.fileSizeBytes() >= autoHttpfsThresholdBytes;
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported duckdb_read_mode: " + mode);
        };

        String url = dataFileLocation.toString();
        boolean isS3 = url.startsWith("s3://") || url.startsWith("s3a://") || url.startsWith("s3n://");
        if (useHttpfs && isS3) {
            return new DuckDbAttachTarget.HttpfsS3(url, duckDbS3Config);
        }
        // httpfs against a non-s3 target degrades to materialize — the local path is
        // already directly attachable, no need for a remote-streaming protocol.
        java.nio.file.Path localPath = duckDbReadCache.materialize(
                fileSystem, dataFileLocation, split.fileSizeBytes());
        return new DuckDbAttachTarget.LocalPath(localPath);
    }

    private ConnectorPageSource applyDeleteFile(TrinoFileSystem fileSystem, DucklakeSplit split, ConnectorPageSource dataSource)
            throws IOException
    {
        boolean hasDeleteFiles = !split.deleteFilePaths().isEmpty();
        boolean hasInlinedDeletes = !split.inlinedDeletedRowPositions().isEmpty();
        if (!hasDeleteFiles && !hasInlinedDeletes) {
            return dataSource;
        }

        // Merge parquet delete files (global row_ids) and inlined deletes (file-local row
        // offsets, from ducklake_inlined_delete_<tableId>) into a single set. The filter
        // checks both interpretations per page position, so adding both into the same set
        // is correct: a parquet delete file row_id matches the rowId branch, an inlined
        // delete row_id matches the rowOffset branch.
        Set<Long> deletedRows = new HashSet<>(split.inlinedDeletedRowPositions());
        for (String deleteFilePath : split.deleteFilePaths()) {
            if (isPuffinPath(deleteFilePath)) {
                deletedRows.addAll(DucklakePuffinDeleteReader.readDeletedPositions(
                        fileSystem.newInputFile(toLocation(deleteFilePath))));
            }
            else {
                deletedRows.addAll(readDeletedRowsFromFile(fileSystem, deleteFilePath, split));
            }
        }

        if (deletedRows.isEmpty()) {
            return dataSource;
        }

        log.debug("Applying deletions to data file %s: %d parquet delete file(s), %d inlined deletes, %d total deleted rows",
                split.dataFilePath(),
                split.deleteFilePaths().size(),
                split.inlinedDeletedRowPositions().size(),
                deletedRows.size());
        return TransformConnectorPageSource.create(dataSource, new DeleteRowFilterTransform(deletedRows, split.rowIdStart()));
    }

    private Set<Long> readDeletedRowsFromFile(TrinoFileSystem fileSystem, String deleteFilePath, DucklakeSplit split)
            throws IOException
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(toLocation(deleteFilePath));

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetDataSource dataSource = null;
        try {
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.empty(),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats);

            // Delete files carry their own footer_size in ducklake_delete_file.
            long deleteFooterHint = split.deleteFileFooterSizes().getOrDefault(deleteFilePath, 0L);
            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(dataSource, deleteFooterHint);

            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions.getMaxFooterReadSize(),
                    Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();
            ParquetDataSourceId dataSourceId = dataSource.getId();
            MessageType fileSchema = fileMetadata.getSchema();
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);

            DeleteFileColumn deleteFileColumn = getDeleteFileColumn(fileSchema, messageColumnIO);
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = TupleDomain.all();
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC);
            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    0,
                    inputFile.length(),
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    Domain.DEFAULT_COMPACTION_THRESHOLD,
                    parquetReaderOptions);

            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    ImmutableList.of(new Column(deleteFileColumn.columnName(), deleteFileColumn.field())),
                    false,
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    exception -> handleParquetException(dataSourceId, exception),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            Set<Long> deletedRows = new HashSet<>();
            try (ConnectorPageSource pageSource = new ParquetPageSource(parquetReader)) {
                while (!pageSource.isFinished()) {
                    SourcePage page = pageSource.getNextSourcePage();
                    if (page == null) {
                        continue;
                    }
                    Block block = page.getBlock(0);
                    for (int position = 0; position < block.getPositionCount(); position++) {
                        if (block.isNull(position)) {
                            continue;
                        }
                        deletedRows.add(readDeleteValue(deleteFileColumn.columnType(), block, position));
                    }
                }
            }
            return deletedRows;
        }
        catch (IOException | RuntimeException e) {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException ex) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex);
                    }
                }
            }
            throw new RuntimeException("Failed to read delete file: " + deleteFilePath, e);
        }
    }

    private static DeleteFileColumn getDeleteFileColumn(MessageType fileSchema, MessageColumnIO messageColumnIO)
    {
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
            if (!field.isPrimitive()) {
                continue;
            }
            ColumnIO columnIO = messageColumnIO.getChild(field.getName());
            if (!(columnIO instanceof PrimitiveColumnIO primitiveColumnIO)) {
                continue;
            }

            PrimitiveTypeName primitiveTypeName = primitiveColumnIO.getPrimitive();
            Type columnType = switch (primitiveTypeName) {
                case INT64 -> BIGINT;
                case INT32 -> INTEGER;
                default -> null;
            };

            if (columnType != null) {
                Field fieldDefinition = DucklakeParquetTypeUtils.constructField(columnType, columnIO)
                        .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Could not construct field for delete file column: " + field.getName()));
                return new DeleteFileColumn(field.getName(), columnType, fieldDefinition);
            }
        }

        throw new TrinoException(NOT_SUPPORTED, "Delete file must contain at least one INT32/INT64 primitive column");
    }

    private static long readDeleteValue(Type type, Block block, int position)
    {
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        throw new IllegalArgumentException("Unsupported delete file value type: " + type);
    }

    private static TupleDomain<ColumnDescriptor> toParquetTupleDomain(
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<DucklakeColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }
        if (effectivePredicate.isAll()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        Map<String, ColumnDescriptor> topLevelDescriptors = descriptorsByPath.entrySet().stream()
                .filter(entry -> entry.getKey().size() == 1)
                .collect(toImmutableMap(
                        entry -> entry.getKey().get(0).toLowerCase(Locale.ENGLISH),
                        Map.Entry::getValue,
                        (first, _) -> first));

        Optional<Map<DucklakeColumnHandle, Domain>> domains = effectivePredicate.getDomains();
        if (domains.isEmpty()) {
            return TupleDomain.all();
        }

        for (Map.Entry<DucklakeColumnHandle, Domain> entry : domains.get().entrySet()) {
            DucklakeColumnHandle columnHandle = entry.getKey();
            ColumnDescriptor descriptor = topLevelDescriptors.get(columnHandle.columnName().toLowerCase(Locale.ENGLISH));
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }

        Map<ColumnDescriptor, Domain> parquetDomains = predicate.buildOrThrow();
        if (parquetDomains.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(parquetDomains);
    }

    private static boolean isPuffinPath(String path)
    {
        // DuckLake's delete-file path always ends with .puffin when format='puffin'
        // (see vendor/ducklake/src/storage/ducklake_delete.cpp:161 — the writer hardcodes
        // "ducklake-<uuid>-delete.puffin"). Catalog format='puffin' has already been
        // permitted by DucklakeSplitManager.validateDeleteFileFormats by the time we get
        // here; dispatching on extension keeps the split schema stable and matches the
        // pattern Trino's Iceberg connector uses for puffin DV files.
        return path.regionMatches(true, path.length() - ".puffin".length(), ".puffin", 0, ".puffin".length());
    }

    private static Location toLocation(String path)
    {
        Location location = Location.of(path);
        if (location.scheme().isPresent()) {
            return location;
        }
        return Location.of(Path.of(path).toUri().toString());
    }

    private static RuntimeException handleParquetException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        return new TrinoException(
                NOT_SUPPORTED,
                "Error reading Parquet file: " + dataSourceId,
                exception);
    }

    private record DeleteFileColumn(String columnName, Type columnType, Field field) {}

    static final class DeleteRowFilterTransform
            implements Function<SourcePage, SourcePage>
    {
        private final Set<Long> deletedRows;
        private final long rowIdStart;
        private long nextRowOffset;

        DeleteRowFilterTransform(Set<Long> deletedRows, long rowIdStart)
        {
            this.deletedRows = Set.copyOf(requireNonNull(deletedRows, "deletedRows is null"));
            this.rowIdStart = rowIdStart;
        }

        @Override
        public SourcePage apply(SourcePage page)
        {
            int positionCount = page.getPositionCount();
            int[] retainedPositions = new int[positionCount];
            int retainedCount = 0;

            for (int position = 0; position < positionCount; position++) {
                long rowOffset = nextRowOffset + position;
                long rowId = rowIdStart + rowOffset;

                // Ducklake delete files conceptually store row ids. We also check row offsets to
                // tolerate producers that store file-local row index values.
                if (!deletedRows.contains(rowId) && !deletedRows.contains(rowOffset)) {
                    retainedPositions[retainedCount] = position;
                    retainedCount++;
                }
            }
            nextRowOffset += positionCount;

            if (retainedCount == positionCount) {
                return page;
            }
            page.selectPositions(retainedPositions, 0, retainedCount);
            return page;
        }
    }

    /**
     * Wraps a ConnectorPageSource and injects a synthetic $row_id BIGINT column.
     * The row ID is computed as rowIdStart + (cumulative file position).
     * This must be applied BEFORE delete file filtering so the IDs match original file positions.
     */
    private static final class RowIdInjectingPageSource
            implements ConnectorPageSource
    {
        private final ConnectorPageSource delegate;
        private final int delegateChannelCount;
        private final int rowIdOutputPosition;
        private final long rowIdStart;
        private long nextRowOffset;

        RowIdInjectingPageSource(ConnectorPageSource delegate, int delegateChannelCount, int rowIdOutputPosition, long rowIdStart)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.delegateChannelCount = delegateChannelCount;
            this.rowIdOutputPosition = rowIdOutputPosition;
            this.rowIdStart = rowIdStart;
        }

        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            return delegate.getCompletedPositions();
        }

        @Override
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            SourcePage sourcePage = delegate.getNextSourcePage();
            if (sourcePage == null) {
                return null;
            }

            int positionCount = sourcePage.getPositionCount();

            // Build the row ID block
            io.trino.spi.block.BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, positionCount);
            for (int i = 0; i < positionCount; i++) {
                BIGINT.writeLong(blockBuilder, rowIdStart + nextRowOffset + i);
            }
            nextRowOffset += positionCount;
            Block rowIdBlock = blockBuilder.build();

            // Build a new page with the row ID block inserted at the correct position
            int totalChannels = delegateChannelCount + 1;
            Block[] blocks = new Block[totalChannels];
            int srcChannel = 0;
            for (int i = 0; i < totalChannels; i++) {
                if (i == rowIdOutputPosition) {
                    blocks[i] = rowIdBlock;
                }
                else {
                    blocks[i] = sourcePage.getBlock(srcChannel);
                    srcChannel++;
                }
            }

            return SourcePage.create(new Page(positionCount, blocks));
        }

        @Override
        public long getMemoryUsage()
        {
            return delegate.getMemoryUsage();
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }
    }

    /**
     * Build the constant block emitted by {@link io.trino.plugin.hive.TransformConnectorPageSource}
     * for a column not present in the parquet body. Defaults to a single-position NULL block;
     * when the split carries a catalog-recorded partition value for this column (hive-style
     * external imports), parses the string value and projects it as a constant instead.
     */
    private static Block buildMissingColumnBlock(DucklakeColumnHandle column, DucklakeSplit split)
    {
        String partitionValue = split.partitionValuesByColumnId().get(column.columnId());
        if (partitionValue == null) {
            return column.columnType().createNullBlock();
        }
        try {
            Object nativeValue = DucklakePartitionValueParser.parseIdentity(column.columnType(), partitionValue);
            return io.trino.spi.type.TypeUtils.writeNativeValue(column.columnType(), nativeValue);
        }
        catch (RuntimeException _) {
            // If the catalog's stored value can't be parsed to the column's type, fall back
            // to NULL rather than failing the whole read. The pruning path already tolerates
            // parse failures the same way.
            return column.columnType().createNullBlock();
        }
    }
}
