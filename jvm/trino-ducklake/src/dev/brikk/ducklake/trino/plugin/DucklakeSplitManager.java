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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import dev.brikk.ducklake.catalog.ColumnRangePredicate;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo;
import dev.brikk.ducklake.catalog.DucklakeFilePartitionValue;
import dev.brikk.ducklake.catalog.DucklakePartitionField;
import dev.brikk.ducklake.catalog.DucklakePartitionSpec;
import dev.brikk.ducklake.catalog.DucklakePartitionTransform;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.util.Locale;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

/**
 * Split manager for Ducklake connector.
 * Discovers data files from SQL catalog and creates splits for each Parquet file.
 */
public class DucklakeSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(DucklakeSplitManager.class);

    private final DucklakeCatalog catalog;
    private final DucklakePathResolver pathResolver;
    private final DucklakeTemporalPartitionEncoding temporalPartitionEncoding;
    private final boolean temporalPartitionEncodingReadLeniency;

    @Inject
    public DucklakeSplitManager(DucklakeCatalog catalog, DucklakeConfig config, DucklakePathResolver pathResolver)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
        this.temporalPartitionEncoding = config.getTemporalPartitionEncoding();
        this.temporalPartitionEncodingReadLeniency = config.isTemporalPartitionEncodingReadLeniency();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (table instanceof DucklakeMetadataTableHandle metadataTableHandle) {
            DucklakeMetadataSplit metadataSplit = new DucklakeMetadataSplit(
                    metadataTableHandle.baseTableId(),
                    metadataTableHandle.snapshotId(),
                    metadataTableHandle.metadataTableType());
            return new FixedSplitSource(List.of(metadataSplit));
        }

        DucklakeTableHandle tableHandle = (DucklakeTableHandle) table;

        log.debug("Getting splits for table %s at snapshot %d", tableHandle.tableName(), tableHandle.snapshotId());

        // Get all data files for this table at the snapshot
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(
                tableHandle.tableId(),
                tableHandle.snapshotId());

        log.debug("Found %d data files for table %s", dataFiles.size(), tableHandle.tableName());

        validateDeleteFileFormats(dataFiles, tableHandle);

        boolean tableHasNoDataFiles = dataFiles.isEmpty();
        List<DucklakeInlinedDataInfo> inlinedDataInfos = catalog.getInlinedDataInfos(tableHandle.tableId(), tableHandle.snapshotId());
        List<DucklakeInlinedSplit> inlinedSplits = inlinedDataInfos.stream()
                .filter(info -> catalog.hasInlinedRows(info.tableId(), info.schemaVersion(), tableHandle.snapshotId()))
                .map(info -> {
                    log.debug("Found inlined data for table %s (tableId=%d, schemaVersion=%d)",
                            tableHandle.tableName(), info.tableId(), info.schemaVersion());
                    return new DucklakeInlinedSplit(info.tableId(), info.schemaVersion(), tableHandle.snapshotId());
                })
                .collect(toImmutableList());

        List<DucklakeSplit> parquetSplits = List.of();
        if (!dataFiles.isEmpty()) {
            DucklakeTable tableMetadata = catalog.getTableById(tableHandle.tableId(), tableHandle.snapshotId())
                    .orElseThrow(() -> new IllegalStateException("Table metadata missing for table ID: " + tableHandle.tableId()));
            DucklakeSchema schemaMetadata = catalog.getSchema(tableHandle.schemaName(), tableHandle.snapshotId())
                    .orElseThrow(() -> new IllegalStateException("Schema metadata missing for schema: " + tableHandle.schemaName()));
            String tableDataPath = pathResolver.resolveTableDataPath(schemaMetadata, tableMetadata);

            TupleDomain<DucklakeColumnHandle> fileStatisticsDomain = buildFileStatisticsDomain(constraint)
                    .intersect(tableHandle.unenforcedPredicate());
            dataFiles = pruneDataFiles(dataFiles, tableHandle, constraint);
            dataFiles = pruneByPartitionValues(dataFiles, tableHandle);

            // Pre-fetch partition spec + file partition values for the splits we're about
            // to build. The page source uses these to constant-fill partition columns
            // when the parquet body is missing them (hive-style external file imports).
            List<DucklakePartitionSpec> specsForProjection = catalog.getPartitionSpecs(
                    tableHandle.tableId(), tableHandle.snapshotId());
            Optional<DucklakePartitionSpec> activeSpec = specsForProjection.isEmpty()
                    ? Optional.empty()
                    : Optional.of(specsForProjection.getLast());
            Map<Long, List<DucklakeFilePartitionValue>> partitionValuesByFile = activeSpec.isPresent()
                    ? catalog.getFilePartitionValues(tableHandle.tableId(), tableHandle.snapshotId())
                    : Map.of();

            // Batch-fetch the source-name overrides recorded in ducklake_name_mapping for
            // files registered via add_files (those carry a non-null mapping_id). The page
            // source uses these when a column's table name doesn't appear in the parquet
            // schema (e.g. case-difference, or a renamed column whose old file kept the old
            // parquet name). Avoid the query when no files in this set have mapping_ids.
            java.util.Set<Long> mappingIds = dataFiles.stream()
                    .map(DucklakeDataFile::mappingId)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            Map<Long, Map<Long, String>> nameMapsByMappingId = mappingIds.isEmpty()
                    ? Map.of()
                    : catalog.getNameMaps(mappingIds);

            // Fetch inlined-delete rows for this table at this snapshot, grouped by
            // data_file_id. DuckLake stores small deletes (below DATA_INLINING_ROW_LIMIT)
            // in ducklake_inlined_delete_<tableId>(file_id, row_id, begin_snapshot) rather
            // than as a parquet delete file. The page source merges these positions into
            // the same deleted-row set as parquet delete files. Empty map when the per-table
            // metadata table doesn't exist (common case).
            Map<Long, Set<Long>> inlinedDeletesByFileId = catalog.hasInlinedDeletes(
                    tableHandle.tableId(), tableHandle.snapshotId())
                    ? catalog.getInlinedDeletes(tableHandle.tableId(), tableHandle.snapshotId())
                    : Map.of();

            // Group by dataFileId to merge multiple delete files per data file
            // (a data file can accumulate multiple delete files across snapshots)
            Map<Long, List<DucklakeDataFile>> groupedFiles = new LinkedHashMap<>();
            for (DucklakeDataFile df : dataFiles) {
                groupedFiles.computeIfAbsent(df.dataFileId(), _ -> new ArrayList<>()).add(df);
            }
            Map<Long, Set<Long>> finalInlinedDeletesByFileId = inlinedDeletesByFileId;
            parquetSplits = groupedFiles.values().stream()
                    .map(group -> createMergedSplit(group, tableDataPath, fileStatisticsDomain, activeSpec, partitionValuesByFile, nameMapsByMappingId, finalInlinedDeletesByFileId))
                    .collect(toImmutableList());
        }

        // Empty table (no data files at all) with an inlined data table: emit an inlined
        // split so the engine gets a proper empty result instead of zero splits.
        // This does NOT apply when pruning eliminated all files — that means no rows match.
        if (tableHasNoDataFiles && inlinedSplits.isEmpty() && !inlinedDataInfos.isEmpty()) {
            DucklakeInlinedDataInfo latestInfo = inlinedDataInfos.getLast();
            log.debug("Emitting empty inlined split for table %s (tableId=%d, schemaVersion=%d)",
                    tableHandle.tableName(), latestInfo.tableId(), latestInfo.schemaVersion());
            inlinedSplits = List.of(new DucklakeInlinedSplit(latestInfo.tableId(), latestInfo.schemaVersion(), tableHandle.snapshotId()));
        }

        if (parquetSplits.isEmpty() && inlinedSplits.isEmpty()) {
            log.debug("No data files or inlined data found for table %s", tableHandle.tableName());
            return new FixedSplitSource(List.of());
        }

        List<ConnectorSplit> allSplits = new ArrayList<>(parquetSplits.size() + inlinedSplits.size());
        allSplits.addAll(parquetSplits);
        allSplits.addAll(inlinedSplits);

        log.debug("Created %d splits for table %s (%d parquet, %d inlined)",
                allSplits.size(),
                tableHandle.tableName(),
                parquetSplits.size(),
                inlinedSplits.size());

        return new FixedSplitSource(allSplits);
    }

    /**
     * Reject snapshots that reference delete files in formats this connector cannot read.
     * Today {@code parquet} positional delete files and {@code puffin} deletion-vector
     * files (DuckLake's Roaring-bitmap format, written when {@code write_deletion_vectors}
     * is enabled) are both supported. Anything else fails the query rather than silently
     * skipping deletes — a missed delete returns rows that should not be visible.
     */
    private static void validateDeleteFileFormats(List<DucklakeDataFile> dataFiles, DucklakeTableHandle tableHandle)
    {
        for (DucklakeDataFile dataFile : dataFiles) {
            Optional<String> deleteFileFormat = dataFile.deleteFileFormat();
            if (deleteFileFormat.isEmpty()) {
                continue;
            }
            String normalized = deleteFileFormat.get().toLowerCase(Locale.ROOT);
            if (normalized.equals("parquet") || normalized.equals("puffin")) {
                continue;
            }
            throw new TrinoException(NOT_SUPPORTED, String.format(
                    "Table %s.%s references a delete file with format '%s', which this connector cannot read. " +
                            "Supported formats are 'parquet' (positional delete files) and 'puffin' (DuckLake " +
                            "deletion-vector files). Compact the table to materialize deletes before reading.",
                    tableHandle.schemaName(),
                    tableHandle.tableName(),
                    deleteFileFormat.get()));
        }
    }

    private List<DucklakeDataFile> pruneDataFiles(List<DucklakeDataFile> dataFiles, DucklakeTableHandle tableHandle, Constraint constraint)
    {
        if (dataFiles.isEmpty()) {
            return dataFiles;
        }

        if (constraint == null || constraint.getSummary().isAll()) {
            return dataFiles;
        }

        if (constraint.getSummary().isNone()) {
            return List.of();
        }

        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        if (domains.isEmpty() || domains.get().isEmpty()) {
            return dataFiles;
        }

        Set<Long> candidateFileIds = dataFiles.stream()
                .map(DucklakeDataFile::dataFileId)
                .collect(toCollection(LinkedHashSet::new));
        boolean pruningApplied = false;

        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (!(entry.getKey() instanceof DucklakeColumnHandle columnHandle)) {
                continue;
            }

            Domain domain = entry.getValue();
            if (domain.isNone()) {
                return List.of();
            }

            Optional<PredicateBounds> predicateBounds = extractPredicateBounds(domain);
            if (predicateBounds.isEmpty()) {
                continue;
            }

            PredicateBounds bounds = predicateBounds.get();
            List<Long> matchingFileIds = catalog.findDataFileIdsInRange(
                    tableHandle.tableId(),
                    tableHandle.snapshotId(),
                    new ColumnRangePredicate(columnHandle.columnId(), bounds.minValue(), bounds.maxValue()));

            pruningApplied = true;
            candidateFileIds.retainAll(matchingFileIds);

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files for table %s using column %s", tableHandle.tableName(), columnHandle.columnName());
                return List.of();
            }
        }

        if (!pruningApplied) {
            return dataFiles;
        }

        List<DucklakeDataFile> prunedDataFiles = dataFiles.stream()
                .filter(file -> candidateFileIds.contains(file.dataFileId()))
                .collect(toImmutableList());

        log.debug("Pruned data files from %d to %d for table %s", dataFiles.size(), prunedDataFiles.size(), tableHandle.tableName());
        return prunedDataFiles;
    }

    private TupleDomain<DucklakeColumnHandle> buildFileStatisticsDomain(Constraint constraint)
    {
        if (constraint == null) {
            return TupleDomain.all();
        }

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            return TupleDomain.all();
        }
        if (summary.isNone()) {
            return TupleDomain.none();
        }

        Optional<Map<ColumnHandle, Domain>> domains = summary.getDomains();
        if (domains.isEmpty() || domains.get().isEmpty()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> ducklakeDomains = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (entry.getKey() instanceof DucklakeColumnHandle columnHandle) {
                ducklakeDomains.put(columnHandle, entry.getValue());
            }
        }

        Map<DucklakeColumnHandle, Domain> result = ducklakeDomains.buildOrThrow();
        if (result.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(result);
    }

    private Optional<PredicateBounds> extractPredicateBounds(Domain domain)
    {
        if (domain.isOnlyNull() || domain.getValues().isAll()) {
            return Optional.empty();
        }

        return domain.getValues().getValuesProcessor().transform(
                ranges -> {
                    if (ranges.getRangeCount() == 0) {
                        return Optional.empty();
                    }

                    Range span = ranges.getSpan();
                    String minValue = span.getLowValue()
                            .map(value -> normalizePredicateValue(domain.getType(), value))
                            .orElse(null);
                    String maxValue = span.getHighValue()
                            .map(value -> normalizePredicateValue(domain.getType(), value))
                            .orElse(null);

                    if (minValue == null && maxValue == null) {
                        return Optional.empty();
                    }
                    return Optional.of(new PredicateBounds(minValue, maxValue));
                },
                discreteValues -> extractDiscreteValueBounds(domain.getType(), discreteValues),
                allOrNone -> Optional.empty());
    }

    private Optional<PredicateBounds> extractDiscreteValueBounds(Type type, io.trino.spi.predicate.DiscreteValues discreteValues)
    {
        if (discreteValues.getValuesCount() == 0) {
            return Optional.empty();
        }

        String minValue = null;
        String maxValue = null;
        for (Object value : discreteValues.getValues()) {
            String normalized = normalizePredicateValue(type, value);
            if (minValue == null || normalized.compareTo(minValue) < 0) {
                minValue = normalized;
            }
            if (maxValue == null || normalized.compareTo(maxValue) > 0) {
                maxValue = normalized;
            }
        }
        return Optional.of(new PredicateBounds(minValue, maxValue));
    }

    private String normalizePredicateValue(Type type, Object value)
    {
        if (value instanceof io.airlift.slice.Slice slice) {
            return slice.toStringUtf8();
        }
        if (type.equals(DATE) && value instanceof Long daysSinceEpoch) {
            return LocalDate.ofEpochDay(daysSinceEpoch).toString();
        }
        return value.toString();
    }

    private List<DucklakeDataFile> pruneByPartitionValues(
            List<DucklakeDataFile> dataFiles,
            DucklakeTableHandle tableHandle)
    {
        TupleDomain<DucklakeColumnHandle> enforced = tableHandle.enforcedPredicate();
        if (enforced.isAll()) {
            return dataFiles;
        }
        if (enforced.isNone()) {
            return List.of();
        }
        if (dataFiles.isEmpty()) {
            return dataFiles;
        }

        List<DucklakePartitionSpec> specs = catalog.getPartitionSpecs(
                tableHandle.tableId(), tableHandle.snapshotId());
        if (specs.isEmpty()) {
            return dataFiles;
        }

        Map<Long, List<DucklakeFilePartitionValue>> filePartValues =
                catalog.getFilePartitionValues(tableHandle.tableId(), tableHandle.snapshotId());

        // Build columnId -> list of (partitionKeyIndex, transform, arity) for all fields.
        // A single column can have multiple transforms (e.g., year + month on the same date column).
        // Arity is populated only for BUCKET transforms; other kinds carry empty.
        Map<Long, List<PartitionKeyMapping>> columnToPartKeys = new HashMap<>();
        for (DucklakePartitionSpec spec : specs) {
            for (DucklakePartitionField field : spec.fields()) {
                columnToPartKeys.computeIfAbsent(field.columnId(), _ -> new ArrayList<>())
                        .add(new PartitionKeyMapping(field.partitionKeyIndex(), field.transform(), field.arity()));
            }
        }

        Set<Long> candidateFileIds = dataFiles.stream()
                .map(DucklakeDataFile::dataFileId)
                .collect(toCollection(LinkedHashSet::new));

        for (Map.Entry<DucklakeColumnHandle, Domain> entry : enforced.getDomains().orElse(Map.of()).entrySet()) {
            DucklakeColumnHandle column = entry.getKey();
            Domain domain = entry.getValue();
            List<PartitionKeyMapping> mappings = columnToPartKeys.get(column.columnId());
            if (mappings == null) {
                continue;
            }

            candidateFileIds.removeIf(fileId -> {
                List<DucklakeFilePartitionValue> values = filePartValues.getOrDefault(fileId, List.of());
                // A file is pruned if ANY partition transform definitively excludes it
                for (PartitionKeyMapping mapping : mappings) {
                    Optional<DucklakeFilePartitionValue> partEntry = values.stream()
                            .filter(v -> v.partitionKeyIndex() == mapping.keyIndex())
                            .findFirst();
                    if (partEntry.isEmpty()) {
                        continue;
                    }
                    String partValue = partEntry.get().partitionValue();
                    if (partValue == null) {
                        // Null partition value — can only match IS NULL predicates, don't prune
                        continue;
                    }
                    if (!partitionValueMatchesDomain(column.columnType(), partValue, domain, mapping.transform(), mapping.arity())) {
                        return true; // this transform excludes the file
                    }
                }
                return false; // no transform excluded the file
            });

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files by partition values for table %s", tableHandle.tableName());
                return List.of();
            }
        }

        List<DucklakeDataFile> result = dataFiles.stream()
                .filter(f -> candidateFileIds.contains(f.dataFileId()))
                .collect(toImmutableList());
        log.debug("Partition pruning: %d -> %d files for table %s", dataFiles.size(), result.size(), tableHandle.tableName());
        return result;
    }

    private boolean partitionValueMatchesDomain(
            Type columnType,
            String partitionValue,
            Domain domain,
            DucklakePartitionTransform transform,
            java.util.OptionalInt arity)
    {
        try {
            if (transform.isIdentity()) {
                Object nativeValue = parsePartitionValue(columnType, partitionValue);
                return domain.includesNullableValue(nativeValue);
            }
            if (transform.isTemporal()) {
                return DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                        columnType,
                        partitionValue,
                        domain,
                        transform,
                        temporalPartitionEncoding,
                        temporalPartitionEncodingReadLeniency);
            }
            if (transform.isBucket()) {
                return DucklakeBucketPartitionMatcher.partitionValueMatchesDomain(
                        columnType,
                        partitionValue,
                        domain,
                        arity.orElseThrow(() -> new IllegalStateException("BUCKET partition field missing arity")));
            }
            return true; // unknown transform — don't prune
        }
        catch (RuntimeException _) {
            return true; // parse failure — don't prune to avoid false negatives
        }
    }

    private static Object parsePartitionValue(Type type, String value)
    {
        return DucklakePartitionValueParser.parseIdentity(type, value);
    }

    private record PartitionKeyMapping(int keyIndex, DucklakePartitionTransform transform, java.util.OptionalInt arity) {}

    private DucklakeSplit createMergedSplit(
            List<DucklakeDataFile> dataFileGroup,
            String tableDataPath,
            TupleDomain<DucklakeColumnHandle> fileStatisticsDomain,
            Optional<DucklakePartitionSpec> activePartitionSpec,
            Map<Long, List<DucklakeFilePartitionValue>> partitionValuesByFile,
            Map<Long, Map<Long, String>> nameMapsByMappingId,
            Map<Long, Set<Long>> inlinedDeletesByFileId)
    {
        DucklakeDataFile primary = dataFileGroup.getFirst();
        String dataFilePath = pathResolver.resolveFilePath(primary.path(), primary.pathIsRelative(), tableDataPath);

        // Collect all delete file paths from the group (multiple delete files for same
        // data file) together with each delete file's footer-size hint. Built in a single
        // pass so resolved paths line up with their catalog-recorded footer sizes; paths
        // are still deduplicated, and when duplicates carry different (or absent) hints we
        // prefer the first recorded positive hint.
        LinkedHashMap<String, Long> deleteFileFooterSizes = new LinkedHashMap<>();
        for (DucklakeDataFile df : dataFileGroup) {
            if (df.deleteFilePath().isEmpty()) {
                continue;
            }
            String resolvedDeletePath = pathResolver.resolveFilePath(
                    df.deleteFilePath().orElseThrow(),
                    df.deleteFilePathIsRelative().orElse(false),
                    tableDataPath);
            long hint = df.deleteFileFooterSize().orElse(0L);
            deleteFileFooterSizes.merge(resolvedDeletePath, hint, (existing, incoming) -> existing > 0 ? existing : incoming);
        }
        List<String> deleteFilePaths = List.copyOf(deleteFileFooterSizes.keySet());

        Map<Long, String> partitionValuesByColumnId = buildIdentityPartitionValues(
                primary.dataFileId(),
                activePartitionSpec,
                partitionValuesByFile);

        Map<Long, String> fieldIdToParquetSourceName = primary.mappingId()
                .map(mid -> nameMapsByMappingId.getOrDefault(mid, Map.<Long, String>of()))
                .orElse(Map.of());

        Set<Long> inlinedDeletedRowPositions = inlinedDeletesByFileId.getOrDefault(primary.dataFileId(), Set.of());

        return new DucklakeSplit(
                dataFilePath,
                deleteFilePaths,
                primary.rowIdStart(),
                primary.recordCount(),
                primary.fileSizeBytes(),
                primary.fileFormat(),
                fileStatisticsDomain,
                primary.footerSize(),
                deleteFileFooterSizes,
                partitionValuesByColumnId,
                fieldIdToParquetSourceName,
                inlinedDeletedRowPositions);
    }

    /**
     * Build the per-file {@code columnId -> partitionValue} map for IDENTITY-transform
     * partition fields. Skip non-identity transforms — their stored value is derived
     * (e.g. {@code year(date)} = 2024) and can't be projected back as the original
     * column. The page source provider uses this map to constant-fill partition
     * columns that don't appear in the parquet body.
     */
    private static Map<Long, String> buildIdentityPartitionValues(
            long dataFileId,
            Optional<DucklakePartitionSpec> activePartitionSpec,
            Map<Long, List<DucklakeFilePartitionValue>> partitionValuesByFile)
    {
        if (activePartitionSpec.isEmpty()) {
            return Map.of();
        }
        List<DucklakeFilePartitionValue> values = partitionValuesByFile.getOrDefault(dataFileId, List.of());
        if (values.isEmpty()) {
            return Map.of();
        }
        Map<Integer, String> byKeyIndex = new HashMap<>();
        for (DucklakeFilePartitionValue v : values) {
            byKeyIndex.put(v.partitionKeyIndex(), v.partitionValue());
        }
        Map<Long, String> out = new HashMap<>();
        for (DucklakePartitionField field : activePartitionSpec.get().fields()) {
            if (field.transform() != DucklakePartitionTransform.IDENTITY) {
                continue;
            }
            String value = byKeyIndex.get(field.partitionKeyIndex());
            if (value != null) {
                out.put(field.columnId(), value);
            }
        }
        return out;
    }

    private record PredicateBounds(String minValue, String maxValue) {}
}
