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
import com.google.inject.Inject;
import com.google.inject.Provider;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeColumn;
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats;
import dev.brikk.ducklake.catalog.DucklakeNameMap;
import dev.brikk.ducklake.catalog.DucklakePartitionField;
import dev.brikk.ducklake.catalog.DucklakePartitionSpec;
import dev.brikk.ducklake.catalog.DucklakePartitionTransform;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.TransactionConflictException;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
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

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Implements {@code CALL <catalog>.system.add_files(...)} — registers
 * pre-existing parquet files as DuckLake data files of a table without rewriting.
 * Mirrors upstream's {@code ducklake_add_data_files} table function.
 *
 * <p>v1 contract:
 * <ul>
 *   <li>Each entry in {@code FILES} is a concrete file path (no glob expansion yet).</li>
 *   <li>Parquet column names must match the table column names case-insensitively;
 *       reordering is permitted.</li>
 *   <li>Hive partitioning supports the IDENTITY transform only (path segments of the
 *       form {@code key=value/}). Files for tables with transform-based partition
 *       specs (year / month / etc.) are out of scope for v1.</li>
 * </ul>
 */
public class DucklakeAddFilesProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle ADD_FILES;

    static {
        try {
            ADD_FILES = MethodHandles.lookup().findVirtual(
                    DucklakeAddFilesProcedure.class,
                    "addFiles",
                    MethodType.methodType(
                            void.class,
                            ConnectorSession.class,
                            String.class,
                            String.class,
                            List.class,
                            boolean.class,
                            boolean.class,
                            boolean.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final DucklakeCatalog catalog;
    private final DucklakeFileSystemFactory fileSystemFactory;
    private final DucklakeTypeConverter typeConverter;
    private final DucklakePathResolver pathResolver;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetReaderOptions parquetReaderOptions;

    @Inject
    public DucklakeAddFilesProcedure(
            DucklakeCatalog catalog,
            DucklakeFileSystemFactory fileSystemFactory,
            DucklakeTypeConverter typeConverter,
            DucklakePathResolver pathResolver,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetReaderConfig parquetReaderConfig)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
        this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetReaderOptions = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "add_files",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("FILES", new ArrayType(VARCHAR)),
                        new Procedure.Argument("ALLOW_MISSING", BOOLEAN, false, false),
                        new Procedure.Argument("IGNORE_EXTRA_COLUMNS", BOOLEAN, false, false),
                        new Procedure.Argument("HIVE_PARTITIONING", BOOLEAN, false, false)),
                ADD_FILES.bindTo(this),
                true);
    }

    @SuppressWarnings("unused") // invoked via MethodHandle
    public void addFiles(
            ConnectorSession session,
            String schemaName,
            String tableName,
            List<?> fileList,
            boolean allowMissing,
            boolean ignoreExtraColumns,
            boolean hivePartitioning)
    {
        if (schemaName == null || schemaName.isEmpty()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "schema_name is required");
        }
        if (tableName == null || tableName.isEmpty()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "table_name is required");
        }
        if (fileList == null || fileList.isEmpty()) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "files must be a non-empty array");
        }

        List<String> filePaths = extractStringArray(fileList);
        long snapshotId = catalog.getCurrentSnapshotId();
        Optional<DucklakeSchema> schema = catalog.getSchema(schemaName, snapshotId);
        if (schema.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Schema not found: " + schemaName);
        }
        Optional<DucklakeTable> table = catalog.getTable(schemaName, tableName, snapshotId);
        if (table.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Table not found: " + schemaName + "." + tableName);
        }
        DucklakeTable tableInfo = table.get();
        long tableId = tableInfo.tableId();

        // getTableColumns returns top-level columns with type strings already resolved
        // ("struct(j integer, i integer)" rather than the catalog row's literal "struct"),
        // which the DucklakeTypeConverter expects. getAllColumnsWithParentage is the flat
        // tree the name mapper needs for descending into children of nested types.
        List<DucklakeColumn> allColumns = catalog.getAllColumnsWithParentage(tableId, snapshotId);
        List<DucklakeColumn> topLevelColumns = catalog.getTableColumns(tableId, snapshotId);

        List<DucklakePartitionSpec> partitionSpecs = catalog.getPartitionSpecs(tableId, snapshotId);
        Optional<DucklakePartitionSpec> activePartitionSpec = partitionSpecs.isEmpty()
                ? Optional.empty()
                : Optional.of(partitionSpecs.getLast());

        if (activePartitionSpec.isPresent() && hivePartitioning) {
            for (DucklakePartitionField field : activePartitionSpec.get().fields()) {
                if (field.transform() != DucklakePartitionTransform.IDENTITY) {
                    throw new TrinoException(NOT_SUPPORTED, String.format(
                            "add_files with hive_partitioning => true currently supports identity partition transforms only; "
                                    + "table \"%s.%s\" has transform %s",
                            schemaName, tableName, field.transform()));
                }
            }
        }

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);

        Set<String> processed = new HashSet<>();
        List<dev.brikk.ducklake.catalog.DucklakeWriteFragment> fragments = new ArrayList<>();

        for (String filePath : filePaths) {
            if (filePath == null || filePath.isEmpty()) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "files contains a null/empty path");
            }
            String normalized = filePath.replace('\\', '/');
            if (!processed.add(normalized)) {
                continue;
            }
            fragments.add(buildFragment(
                    fileSystem,
                    filePath,
                    schemaName,
                    tableName,
                    allColumns,
                    topLevelColumns,
                    activePartitionSpec,
                    allowMissing,
                    ignoreExtraColumns,
                    hivePartitioning));
        }

        try {
            catalog.commitAddFiles(tableId, fragments);
        }
        catch (TransactionConflictException e) {
            throw new TrinoException(TRANSACTION_CONFLICT, e.getMessage(), e);
        }
    }

    private dev.brikk.ducklake.catalog.DucklakeWriteFragment buildFragment(
            TrinoFileSystem fileSystem,
            String filePath,
            String schemaName,
            String tableName,
            List<DucklakeColumn> allColumns,
            List<DucklakeColumn> topLevelColumns,
            Optional<DucklakePartitionSpec> activePartitionSpec,
            boolean allowMissing,
            boolean ignoreExtraColumns,
            boolean hivePartitioning)
    {
        TrinoInputFile inputFile;
        long fileSize;
        try {
            inputFile = fileSystem.newInputFile(Location.of(filePath));
            if (!inputFile.exists()) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "File does not exist: " + filePath);
            }
            fileSize = inputFile.length();
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Failed to open file: " + filePath, e);
        }

        Map<String, String> hivePartitionValues = hivePartitioning
                ? parseHivePartitions(filePath)
                : Map.of();

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetDataSource dataSource = null;
        try {
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(fileSize),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions.getMaxFooterReadSize(),
                    Optional.empty());
            FileMetadata fileMetadata = parquetMetadata.getFileMetaData();

            DucklakeAddFilesNameMapper mapper = new DucklakeAddFilesNameMapper(
                    typeConverter,
                    allowMissing,
                    ignoreExtraColumns,
                    hivePartitionValues,
                    filePath,
                    schemaName + "." + tableName);
            DucklakeAddFilesNameMapper.Result result;
            try {
                result = mapper.map(fileMetadata.getSchema(), allColumns, topLevelColumns);
            }
            catch (DucklakeAddFilesException e) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, e.getMessage());
            }

            // Convert the trimmed Trino-side ParquetMetadata to the legacy
            // org.apache.parquet.format.FileMetaData thrift shape that our extractor
            // consumes. (The extractor walks RowGroup.columns positionally and decodes
            // min/max bytes against the leaf's Trino target type.)
            org.apache.parquet.format.FileMetaData thriftMetadata = toThriftFileMetaData(parquetMetadata);

            // result.leafStatsTargets() lists one entry per matched parquet leaf in file
            // order, with parquetColumnIndex tracking through ignored-extra-columns and
            // hive-partition-overrides so the index stays aligned with RowGroup.columns.
            List<DucklakeFileColumnStats> stats = DucklakeStatsExtractor.extractStats(
                    thriftMetadata, result.leafStatsTargets());

            // footer_size: read the 4-byte little-endian footer length from the parquet
            // post-script (the trailer is `<thrift FileMetaData><4-byte LE length><4-byte magic>`).
            // This is the same value DuckLake stores in ducklake_data_file.footer_size, and
            // FooterPrefetchingParquetDataSource uses it on subsequent reads to skip the
            // blind 48 KB tail read. Best-effort: any IO/parse failure falls back to 0 (the
            // read path tolerates 0 by doing the default blind read).
            long footerSize = readFooterLengthFromPostScript(dataSource);

            long recordCount = aggregateRecordCount(thriftMetadata);

            OptionalLong partitionId = activePartitionSpec.map(spec -> OptionalLong.of(spec.partitionId()))
                    .orElse(OptionalLong.empty());

            Map<Integer, String> partitionValues = remapPartitionValuesToPartitionKeyIndex(
                    result.partitionValues(),
                    activePartitionSpec);

            DucklakeNameMap nameMap = result.nameMap();
            return new dev.brikk.ducklake.catalog.DucklakeWriteFragment(
                    filePath,
                    /* pathIsRelative */ false,
                    "parquet",
                    fileSize,
                    footerSize,
                    recordCount,
                    stats,
                    partitionValues,
                    partitionId,
                    Optional.of(nameMap));
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Failed to read parquet footer: " + filePath, e);
        }
        finally {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException ignored) {
                    // best-effort
                }
            }
        }
    }

    private static long aggregateRecordCount(org.apache.parquet.format.FileMetaData thriftMetadata)
    {
        return thriftMetadata.getNum_rows();
    }

    /**
     * Read the 4-byte little-endian Thrift FileMetaData length from the parquet
     * post-script. The trailer layout per the parquet spec:
     *
     * <pre>
     *   [ Thrift FileMetaData ][ 4 bytes LE footer length ][ 4 bytes "PAR1" magic ]
     * </pre>
     *
     * <p>This value is what DuckLake stores in {@code ducklake_data_file.footer_size}
     * (matches the existing {@link FooterPrefetchingParquetDataSource} contract:
     * footer_size is the Thrift FileMetaData length only, excluding the 8-byte
     * post-script).
     *
     * <p>Best-effort: any IO error, short read, or non-magic trailer returns 0, in
     * which case the read path falls back to its default blind tail read — i.e.
     * the previous behavior. Correctness is unaffected.
     */
    private static long readFooterLengthFromPostScript(ParquetDataSource dataSource)
    {
        try {
            if (dataSource.getEstimatedSize() < 8) {
                return 0;
            }
            io.airlift.slice.Slice tail = dataSource.readTail(8);
            if (tail.length() < 8) {
                return 0;
            }
            // Magic bytes at offset 4..7 of the tail: encrypted parquet uses "PARE"
            // and the encrypted footer length sits in the same 4-byte LE slot, so we
            // accept either marker. Anything else means this isn't a valid trailer.
            byte b4 = tail.getByte(4);
            byte b5 = tail.getByte(5);
            byte b6 = tail.getByte(6);
            byte b7 = tail.getByte(7);
            boolean isParquetMagic = b4 == 'P' && b5 == 'A' && b6 == 'R' && (b7 == '1' || b7 == 'E');
            if (!isParquetMagic) {
                return 0;
            }
            // Airlift Slice is little-endian by contract, so getInt is already LE.
            int footerLength = tail.getInt(0);
            return footerLength >= 0 ? footerLength : 0;
        }
        catch (RuntimeException | IOException _) {
            return 0;
        }
    }

    private Map<Integer, String> remapPartitionValuesToPartitionKeyIndex(
            Map<Integer, String> byFieldId,
            Optional<DucklakePartitionSpec> activePartitionSpec)
    {
        if (byFieldId.isEmpty()) {
            return Map.of();
        }
        if (activePartitionSpec.isEmpty()) {
            // Path looks partitioned but table isn't — upstream silently ignores;
            // mirror that to avoid breaking already-deployed warehouses where folks
            // happen to lay parquet under key=value/ without a partition spec.
            return Map.of();
        }
        Map<Integer, String> out = new LinkedHashMap<>();
        for (DucklakePartitionField field : activePartitionSpec.get().fields()) {
            String value = byFieldId.get((int) field.columnId());
            if (value != null) {
                out.put(field.partitionKeyIndex(), value);
            }
        }
        return out;
    }

    /**
     * Parse {@code key=value} segments out of a file path (hive-style layout).
     * URL-decoded values are returned as their raw text — upstream casts to the
     * partition column's type at read time; we forward strings, matching today's
     * connector convention.
     */
    private static Map<String, String> parseHivePartitions(String path)
    {
        Map<String, String> out = new LinkedHashMap<>();
        String normalized = path.replace('\\', '/');
        // Strip filename so we don't accidentally split on a "=" in the basename.
        int lastSlash = normalized.lastIndexOf('/');
        String dirs = lastSlash < 0 ? "" : normalized.substring(0, lastSlash);
        for (String segment : dirs.split("/")) {
            int eq = segment.indexOf('=');
            if (eq > 0 && eq < segment.length() - 1) {
                String key = segment.substring(0, eq);
                String value = segment.substring(eq + 1);
                if (!key.isEmpty()) {
                    out.put(key, value);
                }
            }
        }
        return out;
    }

    private static List<String> extractStringArray(List<?> values)
    {
        List<String> out = new ArrayList<>(values.size());
        for (Object value : values) {
            if (value == null) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "files must not contain NULL entries");
            }
            // Procedure runtime delivers VARCHAR array entries as Slice in some
            // Trino versions and String in others; tolerate both.
            if (value instanceof io.airlift.slice.Slice slice) {
                out.add(slice.toStringUtf8());
            }
            else if (value instanceof String str) {
                out.add(str);
            }
            else {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT,
                        "files entry must be VARCHAR, got " + value.getClass().getName());
            }
        }
        return out;
    }

    /**
     * Adapter from Trino's {@link ParquetMetadata} to the legacy thrift
     * {@link org.apache.parquet.format.FileMetaData} consumed by
     * {@link DucklakeStatsExtractor}. Trino's view exposes the per-row-group block
     * metadata but the existing extractor needs the thrift form; this rebuilds the
     * pieces the extractor actually reads (row_groups[].columns[].meta_data with
     * statistics) and leaves unused fields default-initialized.
     */
    private static org.apache.parquet.format.FileMetaData toThriftFileMetaData(ParquetMetadata metadata)
            throws IOException
    {
        org.apache.parquet.format.FileMetaData thrift = new org.apache.parquet.format.FileMetaData();
        long numRows = 0;
        List<org.apache.parquet.format.RowGroup> rowGroups = new ArrayList<>();
        for (var block : metadata.getBlocks()) {
            org.apache.parquet.format.RowGroup rg = new org.apache.parquet.format.RowGroup();
            rg.setNum_rows(block.rowCount());
            List<org.apache.parquet.format.ColumnChunk> chunks = new ArrayList<>();
            for (var column : block.columns()) {
                org.apache.parquet.format.ColumnChunk chunk = new org.apache.parquet.format.ColumnChunk();
                org.apache.parquet.format.ColumnMetaData meta = new org.apache.parquet.format.ColumnMetaData();
                meta.setNum_values(column.getValueCount());
                meta.setTotal_compressed_size(column.getTotalSize());
                org.apache.parquet.column.statistics.Statistics<?> nativeStats = column.getStatistics();
                if (nativeStats != null) {
                    org.apache.parquet.format.Statistics statistics = new org.apache.parquet.format.Statistics();
                    if (!nativeStats.isEmpty()) {
                        byte[] minBytes = nativeStats.getMinBytes();
                        byte[] maxBytes = nativeStats.getMaxBytes();
                        if (minBytes != null) {
                            statistics.setMin_value(minBytes);
                        }
                        if (maxBytes != null) {
                            statistics.setMax_value(maxBytes);
                        }
                    }
                    if (nativeStats.getNumNulls() >= 0) {
                        statistics.setNull_count(nativeStats.getNumNulls());
                    }
                    meta.setStatistics(statistics);
                }
                chunk.setMeta_data(meta);
                chunks.add(chunk);
            }
            rg.setColumns(chunks);
            rowGroups.add(rg);
            numRows += block.rowCount();
        }
        thrift.setRow_groups(rowGroups);
        thrift.setNum_rows(numRows);
        return thrift;
    }
}
