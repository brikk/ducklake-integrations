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
import io.airlift.slice.Slices
import dev.brikk.ducklake.catalog.ColumnRangePredicate
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo
import dev.brikk.ducklake.catalog.DucklakeFilePartitionValue
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
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.Optional
import java.util.OptionalInt
import java.util.stream.Collectors.toCollection

/**
 * Split manager for Ducklake connector.
 * Discovers data files from SQL catalog and creates splits for each Parquet file.
 */
class DucklakeSplitManager @Inject constructor(
        private val catalog: DucklakeCatalog,
        config: DucklakeConfig,
        private val pathResolver: DucklakePathResolver,
        private val splitAffinityProvider: SplitAffinityProvider) : ConnectorSplitManager {
    private val temporalPartitionEncoding: DucklakeTemporalPartitionEncoding = config.getTemporalPartitionEncoding()
    private val temporalPartitionEncodingReadLeniency: Boolean = config.isTemporalPartitionEncodingReadLeniency()

    override fun getSplits(
            transaction: ConnectorTransactionHandle?,
            session: ConnectorSession?,
            table: ConnectorTableHandle,
            dynamicFilterColumns: MutableSet<ColumnHandle>,
            constraint: Constraint): ConnectorSplitSource {
        specialSplitSource(table)?.let { return it }

        val tableHandle: DucklakeTableHandle = table as DucklakeTableHandle

        log.debug("Getting splits for table %s at snapshot %d", tableHandle.tableName, tableHandle.snapshotId)

        // Get all data files for this table at the snapshot
        var dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(
                tableHandle.tableId,
                tableHandle.snapshotId)

        log.debug("Found %d data files for table %s", dataFiles.size, tableHandle.tableName)

        validateDeleteFileFormats(dataFiles, tableHandle)
        validateNoUnfilterablePartialFiles(tableHandle)

        val tableHasNoDataFiles: Boolean = dataFiles.isEmpty()
        // getInlinedDataInfos already records, per schema version, whether rows are live at the
        // snapshot (computed in the same EXISTS probe that confirms the table exists), so we filter
        // on the carried flag instead of issuing a second per-element hasInlinedRows round trip.
        val inlinedDataInfos: List<DucklakeInlinedDataInfo> = catalog.getInlinedDataInfos(tableHandle.tableId, tableHandle.snapshotId)
        var inlinedSplits: List<DucklakeInlinedSplit> = inlinedDataInfos.stream()
                .filter { info -> info.hasLiveRows }
                .map { info ->
                    log.debug("Found inlined data for table %s (tableId=%d, schemaVersion=%d)",
                            tableHandle.tableName, info.tableId, info.schemaVersion)
                    DucklakeInlinedSplit(info.tableId, info.schemaVersion, tableHandle.snapshotId)
                }
                .collect(toImmutableList())

        var parquetSplits: List<DucklakeSplit> = listOf()
        if (!dataFiles.isEmpty()) {
            val tableMetadata: DucklakeTable = catalog.getTableById(tableHandle.tableId, tableHandle.snapshotId)
                    ?: throw IllegalStateException("Table metadata missing for table ID: ${tableHandle.tableId}")
            val schemaMetadata: DucklakeSchema = catalog.getSchema(tableHandle.schemaName, tableHandle.snapshotId)
                    ?: throw IllegalStateException("Schema metadata missing for schema: ${tableHandle.schemaName}")
            val tableDataPath: String = pathResolver.resolveTableDataPath(schemaMetadata, tableMetadata)

            val fileStatisticsDomain: TupleDomain<DucklakeColumnHandle> = buildFileStatisticsDomain(constraint)
                    .intersect(tableHandle.unenforcedPredicate)
            dataFiles = pruneDataFiles(dataFiles, tableHandle, constraint)
            dataFiles = pruneByPartitionValues(dataFiles, tableHandle)
            dataFiles = pruneByPath(dataFiles, tableHandle, tableDataPath)

            // Pre-fetch partition spec + file partition values for the splits we're about
            // to build. The page source uses these to constant-fill partition columns
            // when the parquet body is missing them (hive-style external file imports).
            val specsForProjection: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(
                    tableHandle.tableId, tableHandle.snapshotId)
            val activeSpec: Optional<DucklakePartitionSpec> = if (specsForProjection.isEmpty())
                Optional.empty()
            else
                Optional.of(specsForProjection.last())
            val partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>> = if (activeSpec.isPresent)
                catalog.getFilePartitionValues(tableHandle.tableId, tableHandle.snapshotId)
            else
                mapOf()

            // Batch-fetch the source-name overrides recorded in ducklake_name_mapping for
            // files registered via add_files (those carry a non-null mapping_id). The page
            // source uses these when a column's table name doesn't appear in the parquet
            // schema (e.g. case-difference, or a renamed column whose old file kept the old
            // parquet name). Avoid the query when no files in this set have mapping_ids.
            val mappingIds: Set<Long> = dataFiles.stream()
                    .map { it.mappingId }
                    .filter { it != null }
                    .map { it!! }
                    .collect(java.util.stream.Collectors.toUnmodifiableSet())
            val nameMapsByMappingId: Map<Long, Map<Long, String>> = fetchNameMaps(mappingIds)
            // Hive-partition entries of the same name maps: columns whose value lives in the
            // data-file PATH (`key=value/`) rather than the parquet body. DuckDB's
            // `ducklake_add_data_files(..., hive_partitioning => true)` records partition columns
            // this way (no partition spec, no ducklake_file_partition_value rows) — see
            // getPartitionNameMaps. createMergedSplit parses the value out of each file's path and
            // constant-fills the column via the existing missing-column machinery.
            val partitionNameMapsByMappingId: Map<Long, Map<Long, String>> = fetchPartitionNameMaps(mappingIds)

            // Fetch inlined-delete rows for this table at this snapshot, grouped by
            // data_file_id. DuckLake stores small deletes (below DATA_INLINING_ROW_LIMIT)
            // in ducklake_inlined_delete_<tableId>(file_id, row_id, begin_snapshot) rather
            // than as a parquet delete file. The page source merges these positions into
            // the same deleted-row set as parquet delete files. Empty map when the per-table
            // metadata table doesn't exist (common case).
            val inlinedDeletesByFileId: Map<Long, Set<Long>> = if (catalog.hasInlinedDeletes(
                            tableHandle.tableId, tableHandle.snapshotId))
                catalog.getInlinedDeletes(tableHandle.tableId, tableHandle.snapshotId)
            else
                emptyMap()

            // Group by dataFileId to merge multiple delete files per data file
            // (a data file can accumulate multiple delete files across snapshots)
            val groupedFiles: Map<Long, List<DucklakeDataFile>> = dataFiles.groupBy { it.dataFileId }
            parquetSplits = groupedFiles.values
                    .map { group -> createMergedSplit(group, tableDataPath, fileStatisticsDomain, activeSpec, partitionValuesByFile, nameMapsByMappingId, partitionNameMapsByMappingId, inlinedDeletesByFileId, tableHandle.snapshotId) }
        }

        // Empty table (no data files at all) with an inlined data table: emit an inlined
        // split so the engine gets a proper empty result instead of zero splits.
        // This does NOT apply when pruning eliminated all files — that means no rows match.
        if (tableHasNoDataFiles && inlinedSplits.isEmpty() && !inlinedDataInfos.isEmpty()) {
            val latestInfo: DucklakeInlinedDataInfo = inlinedDataInfos.last()
            log.debug("Emitting empty inlined split for table %s (tableId=%d, schemaVersion=%d)",
                    tableHandle.tableName, latestInfo.tableId, latestInfo.schemaVersion)
            inlinedSplits = listOf(DucklakeInlinedSplit(latestInfo.tableId, latestInfo.schemaVersion, tableHandle.snapshotId))
        }

        if (parquetSplits.isEmpty() && inlinedSplits.isEmpty()) {
            log.debug("No data files or inlined data found for table %s", tableHandle.tableName)
            return FixedSplitSource(listOf<ConnectorSplit>())
        }

        val allSplits: MutableList<ConnectorSplit> = ArrayList(parquetSplits.size + inlinedSplits.size)
        allSplits.addAll(parquetSplits)
        allSplits.addAll(inlinedSplits)

        log.debug("Created %d splits for table %s (%d parquet, %d inlined)",
                allSplits.size,
                tableHandle.tableName,
                parquetSplits.size,
                inlinedSplits.size)

        return FixedSplitSource(allSplits)
    }

    /**
     * Fixed-split sources for the non-data-file handles: metadata tables ($files etc.) and the
     * lance search PTF scans (`applyTableFunction` rewrite — the handle already carries the
     * resolved dataset directories, one [LanceSearchSplit] each; predicate/topN pushdown state
     * rides on the table handle and is consumed by the page source). Null for ordinary tables.
     */
    private fun specialSplitSource(table: ConnectorTableHandle): ConnectorSplitSource? = when (table) {
        is DucklakeMetadataTableHandle -> FixedSplitSource(listOf(DucklakeMetadataSplit(
                table.baseTableId,
                table.snapshotId,
                table.metadataTableType)))
        is LanceSearchTableHandle -> FixedSplitSource(table.search().datasetPaths.map { LanceSearchSplit(it) })
        is ChangeFeedTableHandle -> FixedSplitSource(listOf(ChangeFeedSplit(table.tableId)))
        else -> null
    }

    /**
     * Splits for table-function execution — today the lance searches (`system.lance_vector_search`
     * / `lance_fts` / `lance_hybrid_search`). The handle already carries the resolved lance
     * dataset directories (one split each); the per-split search inputs ride on the handle itself
     * (see [LanceSearchHandle]).
     */
    override fun getSplits(
            transaction: ConnectorTransactionHandle,
            session: ConnectorSession,
            function: io.trino.spi.function.table.ConnectorTableFunctionHandle): ConnectorSplitSource {
        if (function is LanceSearchHandle) {
            return FixedSplitSource(function.datasetPaths.map { LanceSearchSplit(it) })
        }
        throw IllegalArgumentException("Unknown table function handle: ${function.javaClass.name}")
    }

    private fun fetchNameMaps(mappingIds: Set<Long>): Map<Long, Map<Long, String>> =
        if (mappingIds.isEmpty()) mapOf() else catalog.getNameMaps(mappingIds)

    private fun fetchPartitionNameMaps(mappingIds: Set<Long>): Map<Long, Map<Long, String>> =
        if (mappingIds.isEmpty()) mapOf() else catalog.getPartitionNameMaps(mappingIds)

    private fun pruneDataFiles(dataFiles: List<DucklakeDataFile>, tableHandle: DucklakeTableHandle, constraint: Constraint?): List<DucklakeDataFile> {
        if (dataFiles.isEmpty()) {
            return dataFiles
        }

        if (constraint == null || constraint.summary.isAll) {
            return dataFiles
        }

        if (constraint.summary.isNone) {
            return listOf()
        }

        val domains: Optional<Map<ColumnHandle, Domain>> = constraint.summary.getDomains()
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
            if (domain.isNone) {
                return listOf()
            }

            val predicateBounds: Optional<PredicateBounds> = extractPredicateBounds(domain)
            if (predicateBounds.isEmpty) {
                continue
            }

            val bounds: PredicateBounds = predicateBounds.get()
            val matchingFileIds: List<Long> = catalog.findDataFileIdsInRange(
                    tableHandle.tableId,
                    tableHandle.snapshotId,
                    ColumnRangePredicate(columnHandle.columnId, bounds.minValue, bounds.maxValue))

            pruningApplied = true
            candidateFileIds.retainAll(matchingFileIds)

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files for table %s using column %s", tableHandle.tableName, columnHandle.columnName)
                return listOf()
            }
        }

        if (!pruningApplied) {
            return dataFiles
        }

        val prunedDataFiles: List<DucklakeDataFile> = dataFiles.stream()
                .filter { file -> candidateFileIds.contains(file.dataFileId) }
                .collect(toImmutableList())

        log.debug("Pruned data files from %d to %d for table %s", dataFiles.size, prunedDataFiles.size, tableHandle.tableName)
        return prunedDataFiles
    }

    private fun buildFileStatisticsDomain(constraint: Constraint?): TupleDomain<DucklakeColumnHandle> {
        if (constraint == null) {
            return TupleDomain.all()
        }

        val summary: TupleDomain<ColumnHandle> = constraint.summary
        if (summary.isAll) {
            return TupleDomain.all()
        }
        if (summary.isNone) {
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
        if (domain.isOnlyNull || domain.values.isAll) {
            return Optional.empty()
        }

        return domain.values.valuesProcessor.transform(
                { ranges ->
                    if (ranges.rangeCount == 0) {
                        return@transform Optional.empty<PredicateBounds>()
                    }

                    val span: Range = ranges.span
                    val minValue: String? = span.lowValue
                            .map { value -> normalizePredicateValue(domain.type, value) }
                            .orElse(null)
                    val maxValue: String? = span.highValue
                            .map { value -> normalizePredicateValue(domain.type, value) }
                            .orElse(null)

                    if (minValue == null && maxValue == null) {
                        return@transform Optional.empty<PredicateBounds>()
                    }
                    Optional.of(PredicateBounds(minValue, maxValue))
                },
                { discreteValues -> extractDiscreteValueBounds(domain.type, discreteValues) },
                { allOrNone -> Optional.empty<PredicateBounds>() })
    }

    private fun extractDiscreteValueBounds(type: Type, discreteValues: io.trino.spi.predicate.DiscreteValues): Optional<PredicateBounds> {
        if (discreteValues.valuesCount == 0) {
            return Optional.empty()
        }

        var minValue: String? = null
        var maxValue: String? = null
        for (value in discreteValues.values) {
            val normalized: String = normalizePredicateValue(type, value)
            if (minValue == null || normalized < minValue) {
                minValue = normalized
            }
            if (maxValue == null || normalized > maxValue) {
                maxValue = normalized
            }
        }
        return Optional.of(PredicateBounds(minValue, maxValue))
    }

    private fun normalizePredicateValue(type: Type, value: Any): String {
        if (value is io.airlift.slice.Slice) {
            return value.toStringUtf8()
        }
        if (type == DATE && value is Long) {
            return LocalDate.ofEpochDay(value).toString()
        }
        return value.toString()
    }

    /**
     * Prune data files by a predicate on the `$path` virtual column (e.g. `WHERE "$path" = '…'`
     * or an IN-list). `$path` is not a partition column, so its domain lands in
     * [DucklakeTableHandle.unenforcedPredicate]; we evaluate each file's RESOLVED path (the same
     * value the `$path` virtual exposes) against that domain. Purely an optimization — the engine
     * re-applies the predicate above the scan (it's unenforced), so a missed file is still correct.
     *
     * Only parquet/duckdb data files are pruned here; inlined splits (whose `$path` is NULL) are
     * left to the engine's filter — they are small and few, so pruning them isn't worth the code.
     */
    private fun pruneByPath(
            dataFiles: List<DucklakeDataFile>,
            tableHandle: DucklakeTableHandle,
            tableDataPath: String): List<DucklakeDataFile> {
        val pathDomain: Domain = tableHandle.unenforcedPredicate.domains.orElse(emptyMap()).entries
                .firstOrNull { it.key.virtualKind() == VirtualKind.PATH }
                ?.value
                ?: return dataFiles
        if (pathDomain.isAll) {
            return dataFiles
        }
        return dataFiles.filter { df ->
            val resolvedPath: String = pathResolver.resolveFilePath(df.path, df.pathIsRelative, tableDataPath)
            pathDomain.includesNullableValue(Slices.utf8Slice(resolvedPath))
        }
    }

    private fun pruneByPartitionValues(
            dataFiles: List<DucklakeDataFile>,
            tableHandle: DucklakeTableHandle): List<DucklakeDataFile> {
        val enforced: TupleDomain<DucklakeColumnHandle> = tableHandle.enforcedPredicate
        if (enforced.isAll) {
            return dataFiles
        }
        if (enforced.isNone) {
            return listOf()
        }
        if (dataFiles.isEmpty()) {
            return dataFiles
        }

        val specs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(
                tableHandle.tableId, tableHandle.snapshotId)
        if (specs.isEmpty()) {
            return dataFiles
        }

        val filePartValues: Map<Long, List<DucklakeFilePartitionValue>> =
                catalog.getFilePartitionValues(tableHandle.tableId, tableHandle.snapshotId)

        // Build columnId -> list of (partitionKeyIndex, transform, arity) for all fields.
        // A single column can have multiple transforms (e.g., year + month on the same date column).
        // Arity is populated only for BUCKET transforms; other kinds carry empty.
        val columnToPartKeys: MutableMap<Long, MutableList<PartitionKeyMapping>> = mutableMapOf()
        for (spec in specs) {
            for (field in spec.fields) {
                columnToPartKeys.computeIfAbsent(field.columnId) { _ -> mutableListOf() }
                        .add(
                            PartitionKeyMapping(
                                field.partitionKeyIndex,
                                field.transform,
                                field.arity?.let { OptionalInt.of(it) } ?: OptionalInt.empty(),
                            ),
                        )
            }
        }

        // Partition-evolution guard (see buildIdentityPartitionValues): a file's partition
        // values are keyed by the key indices of the spec it was written under, and specs
        // number keys from 0. getPartitionSpecs returns only the spec(s) active at this
        // snapshot, so a file written under a retired spec would have its values matched
        // against the active spec's key->column mapping — pruning the wrong column and
        // dropping correct rows. Skip pruning any file whose partition_id isn't one of the
        // specs we actually have the field mapping for.
        val specPartitionIds: Set<Long> = specs.map { it.partitionId }.toSet()
        val partitionIdByFileId: Map<Long, Long?> = dataFiles.associate { it.dataFileId to it.partitionId }

        val candidateFileIds: MutableSet<Long> = dataFiles.stream()
                .map { it.dataFileId }
                .collect(toCollection { LinkedHashSet<Long>() })

        for (entry in enforced.getDomains().orElse(mapOf<DucklakeColumnHandle, Domain>()).entries) {
            val column: DucklakeColumnHandle = entry.key
            val domain: Domain = entry.value
            val mappings: MutableList<PartitionKeyMapping> = columnToPartKeys[column.columnId] ?: continue

            candidateFileIds.removeIf { fileId ->
                val filePartitionId: Long? = partitionIdByFileId[fileId]
                if (filePartitionId != null && !specPartitionIds.contains(filePartitionId)) {
                    return@removeIf false // foreign/retired spec — can't map key indices, don't prune
                }
                val values: List<DucklakeFilePartitionValue> = filePartValues.getOrDefault(fileId, listOf())
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
                        // NULL partition value: every row's source value is NULL (transforms map
                        // NULL to NULL), so a domain that excludes NULL definitively excludes the
                        // file. This is the ENFORCED predicate — the engine adds no residual
                        // filter — so merely "not pruning" would leak the file's NULL rows into
                        // `col = 'x'` results (caught by TestDucklakePartitionedWriteFormats'
                        // NULL-region INSERT). A null-allowing domain keeps the file.
                        if (!domain.isNullAllowed) {
                            return@removeIf true
                        }
                        continue
                    }
                    if (!partitionValueMatchesDomain(column.columnType, partValue, domain, mapping.transform, mapping.arity)) {
                        return@removeIf true // this transform excludes the file
                    }
                }
                false // no transform excluded the file
            }

            if (candidateFileIds.isEmpty()) {
                log.debug("Pruned all data files by partition values for table %s", tableHandle.tableName)
                return listOf()
            }
        }

        val result: List<DucklakeDataFile> = dataFiles.stream()
                .filter { f -> candidateFileIds.contains(f.dataFileId) }
                .collect(toImmutableList())
        log.debug("Partition pruning: %d -> %d files for table %s", dataFiles.size, result.size, tableHandle.tableName)
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

    /**
     * True when a data file's joined DELETE file is a cross-snapshot consolidated ("partial") file
     * holding deletions NEWER than [snapshotId] in a format we snapshot-filter on read (parquet or
     * puffin) — i.e. the page source must apply only the deletions recorded at/before [snapshotId].
     */
    private fun needsPartialDeleteSnapshotFilter(df: DucklakeDataFile, snapshotId: Long): Boolean {
        val deletePartialMax = df.deleteFilePartialMax ?: return false
        if (deletePartialMax <= snapshotId) {
            return false
        }
        val deleteFormat = df.deleteFileFormat?.lowercase(Locale.ROOT)
        return deleteFormat == "parquet" || deleteFormat == "puffin"
    }

    private data class PartitionKeyMapping(val keyIndex: Int, val transform: DucklakePartitionTransform, val arity: java.util.OptionalInt)

    private fun createMergedSplit(
            dataFileGroup: List<DucklakeDataFile>,
            tableDataPath: String,
            fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>,
            activePartitionSpec: Optional<DucklakePartitionSpec>,
            partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>>,
            nameMapsByMappingId: Map<Long, Map<Long, String>>,
            partitionNameMapsByMappingId: Map<Long, Map<Long, String>>,
            inlinedDeletesByFileId: Map<Long, Set<Long>>,
            snapshotId: Long): DucklakeSplit {
        val primary: DucklakeDataFile = dataFileGroup.first()
        val dataFilePath: String = pathResolver.resolveFilePath(primary.path, primary.pathIsRelative, tableDataPath)

        // Collect all delete file paths from the group (multiple delete files for same
        // data file) together with each delete file's footer-size hint. Built in a single
        // pass so resolved paths line up with their catalog-recorded footer sizes; paths
        // are still deduplicated, and when duplicates carry different (or absent) hints we
        // prefer the first recorded positive hint.
        val deleteFileFooterSizes: LinkedHashMap<String, Long> = linkedMapOf()
        val deleteFileSnapshotFilters: LinkedHashMap<String, Long> = linkedMapOf()
        for (df in dataFileGroup) {
            val deleteFilePath = df.deleteFilePath ?: continue
            val resolvedDeletePath: String = pathResolver.resolveFilePath(
                    deleteFilePath,
                    df.deleteFilePathIsRelative ?: false,
                    tableDataPath)
            val hint: Long = df.deleteFileFooterSize ?: 0L
            deleteFileFooterSizes.merge(resolvedDeletePath, hint) { existing, incoming -> if (existing > 0) existing else incoming }
            // Consolidated ("partial") delete file holding deletions newer than this read → filter
            // its deletions to those recorded at/before snapshotId. PARQUET filters by the
            // file's _ducklake_internal_snapshot_id column; PUFFIN by each blob's embedded
            // ducklake-snapshot-id (DucklakePuffinDeleteReader). Both consumed via this map by the
            // page source.
            if (needsPartialDeleteSnapshotFilter(df, snapshotId)) {
                deleteFileSnapshotFilters[resolvedDeletePath] = snapshotId
            }
        }
        val deleteFilePaths: List<String> = deleteFileFooterSizes.keys.toList()

        val partitionValuesByColumnId: Map<Long, String> = resolvePartitionValuesByColumnId(
                primary,
                dataFilePath,
                activePartitionSpec,
                partitionValuesByFile,
                partitionNameMapsByMappingId)

        val fieldIdToParquetSourceName: Map<Long, String> = primary.mappingId
                ?.let { mid -> nameMapsByMappingId.getOrDefault(mid, mapOf()) }
                ?: mapOf()

        @Suppress("UNCHECKED_CAST")
        val inlinedDeletedRowPositions: Set<Long> = inlinedDeletesByFileId.getOrDefault(primary.dataFileId, emptySet<Long>()) as Set<Long>

        val affinityKey: Optional<String> = splitAffinityProvider.getKey(dataFilePath, 0L, primary.fileSizeBytes)

        // Partial (cross-snapshot compacted) file holding rows newer than this read → the page
        // source must drop rows whose _ducklake_internal_snapshot_id > snapshotId. partial_max <=
        // snapshotId means every row is valid → no filter.
        val partialMax: Long? = primary.partialMax
        val snapshotFilterMax: Long? = if (partialMax != null && partialMax > snapshotId) snapshotId else null

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
                affinityKey,
                primary.beginSnapshot,
                snapshotFilterMax,
                deleteFileSnapshotFilters)
    }

    /**
     * Defensive gate for a partial DELETE file in a format we cannot snapshot-filter. Partial DATA
     * files and partial PARQUET + PUFFIN delete files are all filtered on read now (via
     * [DucklakeSplit.snapshotFilterMax] / [DucklakeSplit.deleteFileSnapshotFilters]); this only
     * fires for an unknown delete-file format (which [validateDeleteFileFormats] also rejects). See
     * dev-docs/DESIGN-maintenance.md § 6.
     */
    private fun validateNoUnfilterablePartialFiles(tableHandle: DucklakeTableHandle) {
        if (catalog.hasPartialDeleteFilesRequiringSnapshotFilter(tableHandle.tableId, tableHandle.snapshotId)) {
            throw TrinoException(NOT_SUPPORTED, String.format(
                    "Table %s.%s has a cross-snapshot consolidated delete file in an unsupported format whose " +
                            "deletions extend beyond snapshot %d. Read at the latest snapshot, or expire old snapshots.",
                    tableHandle.schemaName,
                    tableHandle.tableName,
                    tableHandle.snapshotId))
        }
    }

    /**
     * The `column_id → partition value` map used to constant-fill partition columns absent from a
     * file's parquet body. Two disjoint sources, merged: IDENTITY-transform partition-spec values
     * (from `ducklake_file_partition_value`) and hive-partition columns of an add_files-registered
     * file (DuckDB `ducklake_add_data_files(..., hive_partitioning => true)`), whose value lives in
     * the file PATH via `is_partition` name-map entries. A hive-partitioned add_files table has no
     * partition spec, so the two never overlap.
     */
    private fun resolvePartitionValuesByColumnId(
            primary: DucklakeDataFile,
            dataFilePath: String,
            activePartitionSpec: Optional<DucklakePartitionSpec>,
            partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>>,
            partitionNameMapsByMappingId: Map<Long, Map<Long, String>>): Map<Long, String> {
        val identityPartitionValues: Map<Long, String> = buildIdentityPartitionValues(
                primary.dataFileId,
                primary.partitionId,
                activePartitionSpec,
                partitionValuesByFile)
        val hivePartitionValues: Map<Long, String> = primary.mappingId
                ?.let { mid -> partitionNameMapsByMappingId[mid] }
                ?.let { partitionKeysByColumnId -> parseHivePartitionColumnValues(dataFilePath, partitionKeysByColumnId) }
                ?: mapOf()
        return if (hivePartitionValues.isEmpty())
            identityPartitionValues
        else
            identityPartitionValues + hivePartitionValues
    }

    private data class PredicateBounds(val minValue: String?, val maxValue: String?)

    companion object {
        private val log: Logger = Logger.get(DucklakeSplitManager::class.java)

        /**
         * Reject snapshots that reference delete files in formats this connector cannot read.
         * Today `parquet` positional delete files and `puffin` deletion-vector
         * files (DuckLake's Roaring-bitmap format, written when `write_deletion_vectors`
         * is enabled) are both supported. Anything else fails the query rather than silently
         * skipping deletes — a missed delete returns rows that should not be visible.
         */
        private fun validateDeleteFileFormats(dataFiles: List<DucklakeDataFile>, tableHandle: DucklakeTableHandle) {
            for (dataFile in dataFiles) {
                val deleteFileFormat: String = dataFile.deleteFileFormat ?: continue
                val normalized: String = deleteFileFormat.lowercase(Locale.ROOT)
                if (normalized == "parquet" || normalized == "puffin") {
                    continue
                }
                throw TrinoException(NOT_SUPPORTED, String.format(
                        "Table %s.%s references a delete file with format '%s', which this connector cannot read. " +
                                "Supported formats are 'parquet' (positional delete files) and 'puffin' (DuckLake " +
                                "deletion-vector files). Compact the table to materialize deletes before reading.",
                        tableHandle.schemaName,
                        tableHandle.tableName,
                        deleteFileFormat))
            }
        }

        private fun parsePartitionValue(type: Type, value: String): Any =
            DucklakePartitionValueParser.parseIdentity(type, value)

        // DuckDB's sentinel for a NULL hive partition value (`col=__HIVE_DEFAULT_PARTITION__/`).
        private const val HIVE_DEFAULT_PARTITION: String = DucklakeHivePartitionCodec.HIVE_DEFAULT_PARTITION

        /**
         * Extract this file's hive-partition column values from its PATH. [partitionKeysByColumnId]
         * maps each partition column's catalog `column_id` to its path key (the name-map
         * `source_name` DuckDB recorded at add_files time — rename-safe because it is keyed by
         * column_id). Returns `column_id → decoded value`; a key whose value is the
         * `__HIVE_DEFAULT_PARTITION__` sentinel is OMITTED so the column projects NULL through the
         * missing-column path, and a key absent from the path is skipped likewise.
         */
        internal fun parseHivePartitionColumnValues(
                dataFilePath: String,
                partitionKeysByColumnId: Map<Long, String>): Map<Long, String> {
            if (partitionKeysByColumnId.isEmpty()) {
                return mapOf()
            }
            val pathValues: Map<String, String> = parseHivePartitionsFromPath(dataFilePath)
            if (pathValues.isEmpty()) {
                return mapOf()
            }
            val out: MutableMap<Long, String> = linkedMapOf()
            for ((columnId, sourceName) in partitionKeysByColumnId) {
                val raw: String = pathValues[sourceName.lowercase(Locale.ROOT)] ?: continue
                if (raw == HIVE_DEFAULT_PARTITION) {
                    // NULL partition — leave unset so the read path yields NULL.
                    continue
                }
                out[columnId] = DucklakeHivePartitionCodec.decode(raw)
            }
            return out
        }

        /**
         * Parse `key=value` segments out of a file path (hive-style `key=value/` layout). Keys are
         * URL-decoded (DuckDB/DuckLake escape the key too — see [DucklakeHivePartitionCodec]) and
         * lower-cased for case-insensitive matching; values are returned still-encoded (decoded
         * lazily by the caller). The filename is stripped so a stray `=` in the basename is not
         * split on.
         */
        private fun parseHivePartitionsFromPath(path: String): Map<String, String> {
            val normalized: String = path.replace('\\', '/')
            val lastSlash: Int = normalized.lastIndexOf('/')
            val dirs: String = if (lastSlash < 0) "" else normalized.substring(0, lastSlash)
            val out: MutableMap<String, String> = linkedMapOf()
            for (segment in dirs.split('/')) {
                val eq: Int = segment.indexOf('=')
                if (eq > 0 && eq < segment.length - 1) {
                    val key: String = segment.substring(0, eq)
                    val value: String = segment.substring(eq + 1)
                    out[DucklakeHivePartitionCodec.decode(key).lowercase(Locale.ROOT)] = value
                }
            }
            return out
        }

        /**
         * Build the per-file `columnId -> partitionValue` map for IDENTITY-transform
         * partition fields. Skip non-identity transforms — their stored value is derived
         * (e.g. `year(date)` = 2024) and can't be projected back as the original
         * column. The page source provider uses this map to constant-fill partition
         * columns that don't appear in the parquet body.
         */
        internal fun buildIdentityPartitionValues(
                dataFileId: Long,
                filePartitionId: Long?,
                activePartitionSpec: Optional<DucklakePartitionSpec>,
                partitionValuesByFile: Map<Long, List<DucklakeFilePartitionValue>>): Map<Long, String> {
            if (activePartitionSpec.isEmpty) {
                return mapOf()
            }
            val spec: DucklakePartitionSpec = activePartitionSpec.get()
            // Partition-evolution guard. A file's stored partition values are keyed by the
            // partition_key_index of the spec the file was WRITTEN under, and every spec
            // numbers its keys from 0. ducklake_file_partition_value carries no partition_id,
            // so the only safe keyIndex->columnId mapping is the file's own spec. When the
            // file was written under a different (e.g. retired) spec than the active one,
            // mapping its values through the active spec would constant-fill the wrong
            // column — so decline to fill rather than surface corrupt data. The page source
            // then reads the column from the file body (or yields NULL). A file with no
            // partition_id (unpartitioned, or fixtures that don't record it) keeps the prior
            // behavior of trusting the active spec — those carry no partition values anyway.
            if (filePartitionId != null && filePartitionId != spec.partitionId) {
                return mapOf()
            }
            val values: List<DucklakeFilePartitionValue> = partitionValuesByFile.getOrDefault(dataFileId, listOf())
            if (values.isEmpty()) {
                return mapOf()
            }
            val byKeyIndex: MutableMap<Int, String?> = mutableMapOf()
            for (v in values) {
                byKeyIndex[v.partitionKeyIndex] = v.partitionValue
            }
            val out: MutableMap<Long, String> = mutableMapOf()
            for (field in spec.fields) {
                if (field.transform != DucklakePartitionTransform.IDENTITY) {
                    continue
                }
                val value: String? = byKeyIndex[field.partitionKeyIndex]
                if (value != null) {
                    out[field.columnId] = value
                }
            }
            return out
        }
    }
}
