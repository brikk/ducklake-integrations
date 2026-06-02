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

import com.google.common.collect.ImmutableList.toImmutableList
import com.google.common.collect.ImmutableMap
import com.google.inject.Inject
import io.airlift.log.Logger
import dev.brikk.ducklake.catalog.ColumnRangePredicate
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo
import dev.brikk.ducklake.catalog.DucklakeFilePartitionValue
import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import io.trino.filesystem.cache.SplitAffinityProvider
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.ConnectorSplitManager
import io.trino.spi.connector.ConnectorSplitSource
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.TrinoException
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.connector.FixedSplitSource
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.Range
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.Type
import io.trino.spi.type.DateType.DATE

import java.time.LocalDate
import java.util.Locale
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.Optional
import java.util.stream.Collectors.toCollection

/**
 * Split manager for Ducklake connector.
 * Discovers data files from SQL catalog and creates splits for each Parquet file.
 */
public class DucklakeSplitManager @Inject constructor(
        catalog: DucklakeCatalog,
        config: DucklakeConfig,
        pathResolver: DucklakePathResolver,
        splitAffinityProvider: SplitAffinityProvider) : ConnectorSplitManager {
    private val catalog: DucklakeCatalog = catalog
    private val pathResolver: DucklakePathResolver = pathResolver
    private val temporalPartitionEncoding: DucklakeTemporalPartitionEncoding = config.getTemporalPartitionEncoding()
    private val temporalPartitionEncodingReadLeniency: Boolean = config.isTemporalPartitionEncodingReadLeniency()
    private val splitAffinityProvider: SplitAffinityProvider = splitAffinityProvider

    override fun getSplits(
            transaction: ConnectorTransactionHandle?,
            session: ConnectorSession?,
            table: ConnectorTableHandle,
            dynamicFilter: DynamicFilter,
            constraint: Constraint): ConnectorSplitSource {
        if (table is DucklakeMetadataTableHandle) {
            val metadataTableHandle: DucklakeMetadataTableHandle = table
            val metadataSplit = DucklakeMetadataSplit(
                    metadataTableHandle.baseTableId,
                    metadataTableHandle.snapshotId,
                    metadataTableHandle.metadataTableType)
            return FixedSplitSource(listOf(metadataSplit))
        }

        val tableHandle: DucklakeTableHandle = table as DucklakeTableHandle

        log.debug("Getting splits for table %s at snapshot %d", tableHandle.tableName(), tableHandle.snapshotId())

        // Get all data files for this table at the snapshot
        var dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(
                tableHandle.tableId(),
                tableHandle.snapshotId())

        log.debug("Found %d data files for table %s", dataFiles.size, tableHandle.tableName())

        validateDeleteFileFormats(dataFiles, tableHandle)

        val tableHasNoDataFiles: Boolean = dataFiles.isEmpty()
        val inlinedDataInfos: List<DucklakeInlinedDataInfo> = catalog.getInlinedDataInfos(tableHandle.tableId(), tableHandle.snapshotId())
        var inlinedSplits: List<DucklakeInlinedSplit> = inlinedDataInfos.stream()
                .filter { info -> catalog.hasInlinedRows(info.tableId, info.schemaVersion, tableHandle.snapshotId()) }
                .map { info ->
                    log.debug("Found inlined data for table %s (tableId=%d, schemaVersion=%d)",
                            tableHandle.tableName(), info.tableId, info.schemaVersion)
                    DucklakeInlinedSplit(info.tableId, info.schemaVersion, tableHandle.snapshotId())
                }
                .collect(toImmutableList())

        var parquetSplits: List<DucklakeSplit> = listOf()
        if (!dataFiles.isEmpty()) {
            val tableMetadata: DucklakeTable = catalog.getTableById(tableHandle.tableId(), tableHandle.snapshotId())
                    .orElseThrow { IllegalStateException("Table metadata missing for table ID: " + tableHandle.tableId()) }
            val schemaMetadata: DucklakeSchema = catalog.getSchema(tableHandle.schemaName(), tableHandle.snapshotId())
                    .orElseThrow { IllegalStateException("Schema metadata missing for schema: " + tableHandle.schemaName()) }
            val tableDataPath: String = pathResolver.resolveTableDataPath(schemaMetadata, tableMetadata)

            val fileStatisticsDomain: TupleDomain<DucklakeColumnHandle> = buildFileStatisticsDomain(constraint)
                    .intersect(tableHandle.unenforcedPredicate())
            dataFiles = pruneDataFiles(dataFiles, tableHandle, constraint)
            dataFiles = pruneByPartitionValues(dataFiles, tableHandle)

            // Pre-fetch partition spec + file partition values for the splits we're about
            // to build. The page source uses these to constant-fill partition columns
            // when the parquet body is missing them (hive-style external file imports).
            val specsForProjection: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(
                    tableHandle.tableId(), tableHandle.snapshotId())
            val activeSpec: Optional<DucklakePartitionSpec> = if (specsForProjection.isEmpty())
                Optional.empty()
            else
                Optional.of(specsForProjection.last())
            val partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>> = if (activeSpec.isPresent)
                catalog.getFilePartitionValues(tableHandle.tableId(), tableHandle.snapshotId())
            else
                mapOf()

            // Batch-fetch the source-name overrides recorded in ducklake_name_mapping for
            // files registered via add_files (those carry a non-null mapping_id). The page
            // source uses these when a column's table name doesn't appear in the parquet
            // schema (e.g. case-difference, or a renamed column whose old file kept the old
            // parquet name). Avoid the query when no files in this set have mapping_ids.
            val mappingIds: java.util.Set<Long> = dataFiles.stream()
                    .map { it.mappingId }
                    .filter { it.isPresent }
                    .map { it.get() }
                    .collect(java.util.stream.Collectors.toUnmodifiableSet()) as java.util.Set<Long>
            val nameMapsByMappingId: Map<Long, Map<Long, String>> = if (mappingIds.isEmpty())
                mapOf()
            else
                catalog.getNameMaps(mappingIds)

            // Fetch inlined-delete rows for this table at this snapshot, grouped by
            // data_file_id. DuckLake stores small deletes (below DATA_INLINING_ROW_LIMIT)
            // in ducklake_inlined_delete_<tableId>(file_id, row_id, begin_snapshot) rather
            // than as a parquet delete file. The page source merges these positions into
            // the same deleted-row set as parquet delete files. Empty map when the per-table
            // metadata table doesn't exist (common case).
            val inlinedDeletesByFileId: Map<Long, java.util.Set<Long>> = if (catalog.hasInlinedDeletes(
                            tableHandle.tableId(), tableHandle.snapshotId()))
                catalog.getInlinedDeletes(tableHandle.tableId(), tableHandle.snapshotId())
            else
                emptyMap()

            // Group by dataFileId to merge multiple delete files per data file
            // (a data file can accumulate multiple delete files across snapshots)
            val groupedFiles: MutableMap<Long, MutableList<DucklakeDataFile>> = LinkedHashMap()
            for (df in dataFiles) {
                groupedFiles.computeIfAbsent(df.dataFileId) { _ -> ArrayList() }.add(df)
            }
            val finalInlinedDeletesByFileId: Map<Long, java.util.Set<Long>> = inlinedDeletesByFileId
            parquetSplits = groupedFiles.values.stream()
                    .map { group -> createMergedSplit(group, tableDataPath, fileStatisticsDomain, activeSpec, partitionValuesByFile, nameMapsByMappingId, finalInlinedDeletesByFileId) }
                    .collect(toImmutableList())
        }

        // Empty table (no data files at all) with an inlined data table: emit an inlined
        // split so the engine gets a proper empty result instead of zero splits.
        // This does NOT apply when pruning eliminated all files — that means no rows match.
        if (tableHasNoDataFiles && inlinedSplits.isEmpty() && !inlinedDataInfos.isEmpty()) {
            val latestInfo: DucklakeInlinedDataInfo = inlinedDataInfos.last()
            log.debug("Emitting empty inlined split for table %s (tableId=%d, schemaVersion=%d)",
                    tableHandle.tableName(), latestInfo.tableId, latestInfo.schemaVersion)
            inlinedSplits = listOf(DucklakeInlinedSplit(latestInfo.tableId, latestInfo.schemaVersion, tableHandle.snapshotId()))
        }

        if (parquetSplits.isEmpty() && inlinedSplits.isEmpty()) {
            log.debug("No data files or inlined data found for table %s", tableHandle.tableName())
            return FixedSplitSource(listOf<ConnectorSplit>())
        }

        val allSplits: MutableList<ConnectorSplit> = ArrayList(parquetSplits.size + inlinedSplits.size)
        allSplits.addAll(parquetSplits)
        allSplits.addAll(inlinedSplits)

        log.debug("Created %d splits for table %s (%d parquet, %d inlined)",
                allSplits.size,
                tableHandle.tableName(),
                parquetSplits.size,
                inlinedSplits.size)

        return FixedSplitSource(allSplits)
    }

    private fun pruneDataFiles(dataFiles: List<DucklakeDataFile>, tableHandle: DucklakeTableHandle, constraint: Constraint?): List<DucklakeDataFile> {
        if (dataFiles.isEmpty()) {
            return dataFiles
        }

        if (constraint == null || constraint.getSummary().isAll()) {
            return dataFiles
        }

        if (constraint.getSummary().isNone()) {
            return listOf()
        }

        val domains: Optional<Map<ColumnHandle, Domain>> = constraint.getSummary().getDomains()
        if (domains.isEmpty || domains.get().isEmpty()) {
            return dataFiles
        }

        val candidateFileIds: MutableSet<Long> = dataFiles.stream()
                .map { it.dataFileId }
                .collect(toCollection { LinkedHashSet<Long>() })
        var pruningApplied = false

        for (entry in domains.get().entries) {
            val key = entry.key
            if (key !is DucklakeColumnHandle) {
                continue
            }
            val columnHandle: DucklakeColumnHandle = key

            val domain: Domain = entry.value
            if (domain.isNone()) {
                return listOf()
            }

            val predicateBounds: Optional<PredicateBounds> = extractPredicateBounds(domain)
            if (predicateBounds.isEmpty) {
                continue
            }

            val bounds: PredicateBounds = predicateBounds.get()
            val matchingFileIds: List<Long> = catalog.findDataFileIdsInRange(
                    tableHandle.tableId(),
                    tableHandle.snapshotId(),
                    ColumnRangePredicate(columnHandle.columnId(), bounds.minValue, bounds.maxValue))

            pruningApplied = true
            candidateFileIds.retainAll(matchingFileIds)

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files for table %s using column %s", tableHandle.tableName(), columnHandle.columnName())
                return listOf()
            }
        }

        if (!pruningApplied) {
            return dataFiles
        }

        val prunedDataFiles: List<DucklakeDataFile> = dataFiles.stream()
                .filter { file -> candidateFileIds.contains(file.dataFileId) }
                .collect(toImmutableList())

        log.debug("Pruned data files from %d to %d for table %s", dataFiles.size, prunedDataFiles.size, tableHandle.tableName())
        return prunedDataFiles
    }

    private fun buildFileStatisticsDomain(constraint: Constraint?): TupleDomain<DucklakeColumnHandle> {
        if (constraint == null) {
            return TupleDomain.all()
        }

        val summary: TupleDomain<ColumnHandle> = constraint.getSummary()
        if (summary.isAll()) {
            return TupleDomain.all()
        }
        if (summary.isNone()) {
            return TupleDomain.none()
        }

        val domains: Optional<Map<ColumnHandle, Domain>> = summary.getDomains()
        if (domains.isEmpty || domains.get().isEmpty()) {
            return TupleDomain.all()
        }

        val ducklakeDomains: ImmutableMap.Builder<DucklakeColumnHandle, Domain> = ImmutableMap.builder()
        for (entry in domains.get().entries) {
            val key = entry.key
            if (key is DucklakeColumnHandle) {
                ducklakeDomains.put(key, entry.value)
            }
        }

        val result: Map<DucklakeColumnHandle, Domain> = ducklakeDomains.buildOrThrow()
        if (result.isEmpty()) {
            return TupleDomain.all()
        }
        return TupleDomain.withColumnDomains(result)
    }

    private fun extractPredicateBounds(domain: Domain): Optional<PredicateBounds> {
        if (domain.isOnlyNull() || domain.getValues().isAll()) {
            return Optional.empty()
        }

        return domain.getValues().getValuesProcessor().transform<Optional<PredicateBounds>>(
                { ranges ->
                    if (ranges.getRangeCount() == 0) {
                        return@transform Optional.empty<PredicateBounds>()
                    }

                    val span: Range = ranges.getSpan()
                    val minValue: String? = span.getLowValue()
                            .map { value -> normalizePredicateValue(domain.getType(), value) }
                            .orElse(null)
                    val maxValue: String? = span.getHighValue()
                            .map { value -> normalizePredicateValue(domain.getType(), value) }
                            .orElse(null)

                    if (minValue == null && maxValue == null) {
                        return@transform Optional.empty<PredicateBounds>()
                    }
                    Optional.of(PredicateBounds(minValue, maxValue))
                },
                { discreteValues -> extractDiscreteValueBounds(domain.getType(), discreteValues) },
                { allOrNone -> Optional.empty<PredicateBounds>() })
    }

    private fun extractDiscreteValueBounds(type: Type, discreteValues: io.trino.spi.predicate.DiscreteValues): Optional<PredicateBounds> {
        if (discreteValues.getValuesCount() == 0) {
            return Optional.empty()
        }

        var minValue: String? = null
        var maxValue: String? = null
        for (value in discreteValues.getValues()) {
            val normalized: String = normalizePredicateValue(type, value)
            if (minValue == null || normalized.compareTo(minValue) < 0) {
                minValue = normalized
            }
            if (maxValue == null || normalized.compareTo(maxValue) > 0) {
                maxValue = normalized
            }
        }
        return Optional.of(PredicateBounds(minValue, maxValue))
    }

    private fun normalizePredicateValue(type: Type, value: Any): String {
        if (value is io.airlift.slice.Slice) {
            return value.toStringUtf8()
        }
        if (type.equals(DATE) && value is Long) {
            return LocalDate.ofEpochDay(value).toString()
        }
        return value.toString()
    }

    private fun pruneByPartitionValues(
            dataFiles: List<DucklakeDataFile>,
            tableHandle: DucklakeTableHandle): List<DucklakeDataFile> {
        val enforced: TupleDomain<DucklakeColumnHandle> = tableHandle.enforcedPredicate()
        if (enforced.isAll()) {
            return dataFiles
        }
        if (enforced.isNone()) {
            return listOf()
        }
        if (dataFiles.isEmpty()) {
            return dataFiles
        }

        val specs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(
                tableHandle.tableId(), tableHandle.snapshotId())
        if (specs.isEmpty()) {
            return dataFiles
        }

        val filePartValues: Map<Long, List<DucklakeFilePartitionValue>> =
                catalog.getFilePartitionValues(tableHandle.tableId(), tableHandle.snapshotId())

        // Build columnId -> list of (partitionKeyIndex, transform, arity) for all fields.
        // A single column can have multiple transforms (e.g., year + month on the same date column).
        // Arity is populated only for BUCKET transforms; other kinds carry empty.
        val columnToPartKeys: MutableMap<Long, MutableList<PartitionKeyMapping>> = HashMap()
        for (spec in specs) {
            for (field in spec.fields) {
                columnToPartKeys.computeIfAbsent(field.columnId) { _ -> ArrayList() }
                        .add(PartitionKeyMapping(field.partitionKeyIndex, field.transform, field.arity))
            }
        }

        val candidateFileIds: MutableSet<Long> = dataFiles.stream()
                .map { it.dataFileId }
                .collect(toCollection { LinkedHashSet<Long>() })

        for (entry in enforced.getDomains().orElse(mapOf<DucklakeColumnHandle, Domain>()).entries) {
            val column: DucklakeColumnHandle = entry.key
            val domain: Domain = entry.value
            val mappings: List<PartitionKeyMapping>? = columnToPartKeys[column.columnId()]
            if (mappings == null) {
                continue
            }

            candidateFileIds.removeIf { fileId ->
                val values: List<DucklakeFilePartitionValue> = filePartValues.getOrDefault(fileId, listOf<DucklakeFilePartitionValue>())
                // A file is pruned if ANY partition transform definitively excludes it
                for (mapping in mappings) {
                    val partEntry: Optional<DucklakeFilePartitionValue> = values.stream()
                            .filter { v -> v.partitionKeyIndex == mapping.keyIndex }
                            .findFirst()
                    if (partEntry.isEmpty) {
                        continue
                    }
                    val partValue: String? = partEntry.get().partitionValue
                    if (partValue == null) {
                        // Null partition value — can only match IS NULL predicates, don't prune
                        continue
                    }
                    if (!partitionValueMatchesDomain(column.columnType(), partValue, domain, mapping.transform, mapping.arity)) {
                        return@removeIf true // this transform excludes the file
                    }
                }
                false // no transform excluded the file
            }

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files by partition values for table %s", tableHandle.tableName())
                return listOf()
            }
        }

        val result: List<DucklakeDataFile> = dataFiles.stream()
                .filter { f -> candidateFileIds.contains(f.dataFileId) }
                .collect(toImmutableList())
        log.debug("Partition pruning: %d -> %d files for table %s", dataFiles.size, result.size, tableHandle.tableName())
        return result
    }

    private fun partitionValueMatchesDomain(
            columnType: Type,
            partitionValue: String,
            domain: Domain,
            transform: DucklakePartitionTransform,
            arity: java.util.OptionalInt): Boolean {
        try {
            if (transform.isIdentity()) {
                val nativeValue: Any = parsePartitionValue(columnType, partitionValue)
                return domain.includesNullableValue(nativeValue)
            }
            if (transform.isTemporal()) {
                return DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                        columnType,
                        partitionValue,
                        domain,
                        transform,
                        temporalPartitionEncoding,
                        temporalPartitionEncodingReadLeniency)
            }
            if (transform.isBucket()) {
                return DucklakeBucketPartitionMatcher.partitionValueMatchesDomain(
                        columnType,
                        partitionValue,
                        domain,
                        arity.orElseThrow { IllegalStateException("BUCKET partition field missing arity") })
            }
            return true // unknown transform — don't prune
        }
        catch (_: RuntimeException) {
            return true // parse failure — don't prune to avoid false negatives
        }
    }

    private data class PartitionKeyMapping(val keyIndex: Int, val transform: DucklakePartitionTransform, val arity: java.util.OptionalInt)

    private fun createMergedSplit(
            dataFileGroup: List<DucklakeDataFile>,
            tableDataPath: String,
            fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>,
            activePartitionSpec: Optional<DucklakePartitionSpec>,
            partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>>,
            nameMapsByMappingId: Map<Long, Map<Long, String>>,
            inlinedDeletesByFileId: Map<Long, java.util.Set<Long>>): DucklakeSplit {
        val primary: DucklakeDataFile = dataFileGroup.first()
        val dataFilePath: String = pathResolver.resolveFilePath(primary.path, primary.pathIsRelative, tableDataPath)

        // Collect all delete file paths from the group (multiple delete files for same
        // data file) together with each delete file's footer-size hint. Built in a single
        // pass so resolved paths line up with their catalog-recorded footer sizes; paths
        // are still deduplicated, and when duplicates carry different (or absent) hints we
        // prefer the first recorded positive hint.
        val deleteFileFooterSizes: LinkedHashMap<String, Long> = LinkedHashMap()
        for (df in dataFileGroup) {
            if (df.deleteFilePath.isEmpty) {
                continue
            }
            val resolvedDeletePath: String = pathResolver.resolveFilePath(
                    df.deleteFilePath.orElseThrow(),
                    df.deleteFilePathIsRelative.orElse(false),
                    tableDataPath)
            val hint: Long = df.deleteFileFooterSize.orElse(0L)
            deleteFileFooterSizes.merge(resolvedDeletePath, hint) { existing, incoming -> if (existing > 0) existing else incoming }
        }
        val deleteFilePaths: List<String> = deleteFileFooterSizes.keys.toList()

        val partitionValuesByColumnId: Map<Long, String> = buildIdentityPartitionValues(
                primary.dataFileId,
                activePartitionSpec,
                partitionValuesByFile)

        val fieldIdToParquetSourceName: Map<Long, String> = primary.mappingId
                .map { mid -> nameMapsByMappingId.getOrDefault(mid, mapOf<Long, String>()) }
                .orElse(mapOf())

        @Suppress("UNCHECKED_CAST")
        val inlinedDeletedRowPositions: Set<Long> = inlinedDeletesByFileId.getOrDefault(primary.dataFileId, emptySet<Long>()) as Set<Long>

        val affinityKey: Optional<String> = splitAffinityProvider.getKey(dataFilePath, 0L, primary.fileSizeBytes)

        return DucklakeSplit(
                dataFilePath,
                deleteFilePaths,
                primary.rowIdStart,
                primary.recordCount,
                primary.fileSizeBytes,
                primary.fileFormat,
                fileStatisticsDomain,
                primary.footerSize,
                deleteFileFooterSizes,
                partitionValuesByColumnId,
                fieldIdToParquetSourceName,
                inlinedDeletedRowPositions,
                affinityKey)
    }

    private data class PredicateBounds(val minValue: String?, val maxValue: String?)

    companion object {
        private val log: Logger = Logger.get(DucklakeSplitManager::class.java)

        /**
         * Reject snapshots that reference delete files in formats this connector cannot read.
         * Today {@code parquet} positional delete files and {@code puffin} deletion-vector
         * files (DuckLake's Roaring-bitmap format, written when {@code write_deletion_vectors}
         * is enabled) are both supported. Anything else fails the query rather than silently
         * skipping deletes — a missed delete returns rows that should not be visible.
         */
        private fun validateDeleteFileFormats(dataFiles: List<DucklakeDataFile>, tableHandle: DucklakeTableHandle) {
            for (dataFile in dataFiles) {
                val deleteFileFormat: Optional<String> = dataFile.deleteFileFormat
                if (deleteFileFormat.isEmpty) {
                    continue
                }
                val normalized: String = deleteFileFormat.get().lowercase(Locale.ROOT)
                if (normalized == "parquet" || normalized == "puffin") {
                    continue
                }
                throw TrinoException(NOT_SUPPORTED, String.format(
                        "Table %s.%s references a delete file with format '%s', which this connector cannot read. " +
                                "Supported formats are 'parquet' (positional delete files) and 'puffin' (DuckLake " +
                                "deletion-vector files). Compact the table to materialize deletes before reading.",
                        tableHandle.schemaName(),
                        tableHandle.tableName(),
                        deleteFileFormat.get()))
            }
        }

        private fun parsePartitionValue(type: Type, value: String): Any {
            return DucklakePartitionValueParser.parseIdentity(type, value)
        }

        /**
         * Build the per-file {@code columnId -> partitionValue} map for IDENTITY-transform
         * partition fields. Skip non-identity transforms — their stored value is derived
         * (e.g. {@code year(date)} = 2024) and can't be projected back as the original
         * column. The page source provider uses this map to constant-fill partition
         * columns that don't appear in the parquet body.
         */
        private fun buildIdentityPartitionValues(
                dataFileId: Long,
                activePartitionSpec: Optional<DucklakePartitionSpec>,
                partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>>): Map<Long, String> {
            if (activePartitionSpec.isEmpty) {
                return mapOf()
            }
            val values: List<DucklakeFilePartitionValue> = partitionValuesByFile.getOrDefault(dataFileId, listOf<DucklakeFilePartitionValue>())
            if (values.isEmpty()) {
                return mapOf()
            }
            val byKeyIndex: MutableMap<Int, String?> = HashMap()
            for (v in values) {
                byKeyIndex.put(v.partitionKeyIndex, v.partitionValue)
            }
            val out: MutableMap<Long, String> = HashMap()
            for (field in activePartitionSpec.get().fields) {
                if (field.transform != DucklakePartitionTransform.IDENTITY) {
                    continue
                }
                val value: String? = byKeyIndex[field.partitionKeyIndex]
                if (value != null) {
                    out.put(field.columnId, value)
                }
            }
            return out
        }
    }
}
