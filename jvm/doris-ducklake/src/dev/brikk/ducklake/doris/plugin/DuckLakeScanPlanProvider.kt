package dev.brikk.ducklake.doris.plugin

import java.util.Locale
import java.util.Objects
import java.util.Optional

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeFilePartitionValue
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform

import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.handle.ConnectorColumnHandle
import org.apache.doris.connector.api.handle.ConnectorTableHandle
import org.apache.doris.connector.api.pushdown.ConnectorExpression
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider
import org.apache.doris.connector.api.scan.ConnectorScanRange
import org.apache.doris.connector.api.scan.ConnectorScanRangeType
import org.apache.doris.thrift.TFileScanRangeParams

/**
 * Splits a DuckLake table read into per-file scan ranges. v1 emits one range
 * per [DucklakeDataFile] active at the snapshot pinned on the
 * [DuckLakeTableHandle]; deletes / pushdown / time-travel resolution
 * layer on top per the roadmap.
 *
 * P6 additions:
 *  - COUNT(*) pushdown: the 7-arg [planScan] override receives the engine's
 *    no-grouping COUNT(*) signal and — when the count is exactly servable
 *    from `ducklake_data_file.record_count` metadata — collapses the scan to
 *    a SINGLE range carrying the total (mirrors
 *    `IcebergScanPlanProvider.planCountPushdown`'s collapse shape).
 *  - partition-bearing ranges: every range of a partitioned table reports
 *    `isPartitionBearing() == true` plus the file's IDENTITY partition
 *    values, so the engine stops Hive-path-parsing DuckLake's
 *    non-`key=value` file layout (P6 report §(d)).
 */
internal class DuckLakeScanPlanProvider(
    catalog: DucklakeCatalog,
    pathResolver: DuckLakePathResolver,
    catalogProperties: Map<String, String>,
) : ConnectorScanPlanProvider {

    private val catalog: DucklakeCatalog =
        Objects.requireNonNull(catalog, "catalog")
    private val pathResolver: DuckLakePathResolver =
        Objects.requireNonNull(pathResolver, "pathResolver")
    private val catalogProperties: Map<String, String> =
        java.util.Map.copyOf(Objects.requireNonNull(catalogProperties, "catalogProperties"))

    override fun getScanRangeType(): ConnectorScanRangeType = ConnectorScanRangeType.FILE_SCAN

    override fun planScan(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        columns: List<ConnectorColumnHandle>,
        filter: Optional<ConnectorExpression>,
    ): List<ConnectorScanRange> =
        planScanInternal(handle, filter, countPushdown = false)

    /**
     * COUNT(*)-pushdown-aware scan entry, mirroring the iceberg connector's
     * 7-arg override (`IcebergScanPlanProvider.planScan` → `planScanInternal`).
     * `limit` / `requiredPartitions` are not consumed by the DuckLake read
     * path — pruning is predicate-driven via the handle's [DuckLakeTableHandle.prunedFileIds],
     * exactly like the 4-arg path (the default SPI overloads fold down to it).
     */
    override fun planScan(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        columns: List<ConnectorColumnHandle>,
        filter: Optional<ConnectorExpression>,
        limit: Long,
        requiredPartitions: List<String>?,
        countPushdown: Boolean,
    ): List<ConnectorScanRange> =
        planScanInternal(handle, filter, countPushdown)

    private fun planScanInternal(
        handle: ConnectorTableHandle,
        filter: Optional<ConnectorExpression>,
        countPushdown: Boolean,
    ): List<ConnectorScanRange> {
        // v1: full scan. Column projection and `filter` pushdown layer on top
        // later (READ todo Step 6); the snapshot is already pinned on the handle
        // at getTableHandle time, so reads stay version-consistent.
        val dlHandle = handle.asDuckLakeHandle<DuckLakeTableHandle>()

        val schema = catalog.getSchema(dlHandle.database, dlHandle.snapshotId)
            ?: throw IllegalStateException(
                "Schema metadata missing for '" + dlHandle.database + "' at snapshot " +
                    dlHandle.snapshotId,
            )
        val table = catalog.getTableById(dlHandle.tableId, dlHandle.snapshotId)
            ?: throw IllegalStateException(
                "Table metadata missing for tableId=" + dlHandle.tableId +
                    " at snapshot " + dlHandle.snapshotId,
            )

        // Correctness gate (corpus first-contact finding, 2026-07-06): DuckLake
        // inlines small INSERTs/DELETEs into metadata-catalog rows
        // (ducklake_inlined_data_* / ducklake_inlined_delete_*) by default on
        // PG backends. The Doris read path serves rows from data FILES only,
        // so a table with live inlined state would SILENTLY return wrong rows
        // (empty for inlined data, over-counted for inlined deletes). Fail
        // loudly instead — two cheap metadata probes per plan buy "never
        // silently wrong". Serving inlined rows is a TODO-read item.
        failOnLiveInlinedState(dlHandle)

        val tableDataPath = pathResolver.resolveTableDataPath(schema, table)
        val dataFiles = catalog.getDataFiles(dlHandle.tableId, dlHandle.snapshotId)
        val partitions = resolvePartitionContext(dlHandle)

        // COUNT(*) pushdown: when the count is exactly servable from file
        // metadata, collapse the scan to a single range carrying the total
        // (iceberg emission shape). Any doubt → fall through to the normal
        // scan so BE counts by reading (the -1 sentinel is load-bearing —
        // see PluginDrivenScanNode.resolvePushDownRowCount).
        if (countPushdown && isCountServableFromMetadata(dlHandle, filter, dataFiles)) {
            return planCountPushdown(dataFiles, tableDataPath, partitions)
        }

        // Honour the file-level pruning applyFilter computed from column stats
        // (null = no filter pushed, scan all files).
        val prunedIds = dlHandle.prunedFileIds
        val scanFiles = if (prunedIds != null) {
            dataFiles.filter { it.dataFileId in prunedIds }
        } else {
            dataFiles
        }

        val ranges = ArrayList<ConnectorScanRange>(scanFiles.size)
        for (file in scanFiles) {
            ranges.add(buildRange(file, tableDataPath, partitions, PUSH_DOWN_COUNT_NONE))
        }
        return ranges
    }

    /**
     * Throws [DorisConnectorException] when the table has live inlined state
     * at the pinned snapshot — the "not supported" wording is deliberate: it
     * is the connector's documented-gap convention (and what the corpus
     * mirror's error classifier recognizes as an engine-skip).
     */
    private fun failOnLiveInlinedState(handle: DuckLakeTableHandle) {
        val liveInlinedData =
            catalog.getInlinedDataInfos(handle.tableId, handle.snapshotId).any { it.hasLiveRows }
        if (liveInlinedData || catalog.hasInlinedDeletes(handle.tableId, handle.snapshotId)) {
            throw DorisConnectorException(
                "DuckLake table has live inlined " +
                    (if (liveInlinedData) "data" else "delete") +
                    " rows in the metadata catalog; reading them is not supported by the Doris " +
                    "connector yet (rows live in ducklake_inlined_* tables, not parquet). " +
                    "Flush them (DuckDB: CALL <lake>.flush_inlined_data()) or disable inlining " +
                    "(CALL <lake>.set_option('data_inlining_row_limit', '0')) and rewrite.",
            )
        }
    }

    /**
     * Whether a no-grouping COUNT(*) can be answered EXACTLY from
     * `ducklake_data_file.record_count` at the handle's pinned snapshot,
     * the DuckLake analogue of `IcebergScanPlanProvider.getCountFromSummary`'s
     * `>= 0` gate. Refuses (→ normal scan; BE counts by reading) when:
     *  - any filter is in play (a remaining filter expression, a pushed filter
     *    on the handle, or a pruned file set) — the count must be over ALL rows;
     *  - any active data file has a position-delete file attached — deletes
     *    reduce counts below the file metadata;
     *  - a partial (cross-snapshot compacted) data file extends beyond the
     *    pinned snapshot ([DucklakeDataFile.partialMax] > snapshot) — its
     *    record_count includes rows a read at this snapshot must drop;
     *  - the table has inlined deletes at the snapshot;
     *  - the table has live inlined data rows at the snapshot — those rows are
     *    in the catalog DB, not in any parquet file, so a file-metadata sum
     *    undercounts. [DucklakeCatalog.getInlinedDataInfos] probes existence
     *    AND per-snapshot liveness in one round-trip ([dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo.hasLiveRows]),
     *    the same descriptor the trino split manager keys inlined splits off.
     *
     * Cheap in-memory checks run first; the two catalog round-trips only fire
     * for a clean, unfiltered scan (the case that actually serves a count).
     */
    private fun isCountServableFromMetadata(
        dlHandle: DuckLakeTableHandle,
        filter: Optional<ConnectorExpression>,
        dataFiles: List<DucklakeDataFile>,
    ): Boolean {
        if (filter.isPresent || dlHandle.pushedFilter != null || dlHandle.prunedFileIds != null) {
            return false
        }
        if (dataFiles.any { it.deleteFilePath != null }) {
            return false
        }
        if (dataFiles.any { (it.partialMax ?: Long.MIN_VALUE) > dlHandle.snapshotId }) {
            return false
        }
        if (catalog.hasInlinedDeletes(dlHandle.tableId, dlHandle.snapshotId)) {
            return false
        }
        return catalog.getInlinedDataInfos(dlHandle.tableId, dlHandle.snapshotId)
            .none { it.hasLiveRows }
    }

    /**
     * Emit the single collapsed COUNT(*)-pushdown range: the FIRST data file's
     * whole-file range carrying the full metadata count, exactly the shape
     * `IcebergScanPlanProvider.planCountPushdown` emits (one range bearing the
     * summed total; BE's count reader serves `table_level_row_count` without
     * opening the file, so which file backs the range is irrelevant). An empty
     * table yields NO range, so BE gets 0 ranges and COUNT returns 0 —
     * iceberg parity again (its empty-snapshot count short-circuits the same
     * way via empty planFiles()).
     */
    private fun planCountPushdown(
        dataFiles: List<DucklakeDataFile>,
        tableDataPath: String,
        partitions: PartitionContext,
    ): List<ConnectorScanRange> {
        val first = dataFiles.firstOrNull() ?: return listOf()
        val totalRecords = dataFiles.sumOf { it.recordCount }
        return listOf(buildRange(first, tableDataPath, partitions, totalRecords))
    }

    /**
     * Build the BE-ready range for one active data file. Shared by the normal
     * scan loop and the count-pushdown collapse so both emit byte-identical
     * ranges apart from the count carrier (mirrors iceberg's shared
     * `buildRange`). Full-file extent: start=0, length=fileSize — BE splits
     * internally along parquet row groups; pre-splitting in the planner is a
     * v2 concern.
     */
    private fun buildRange(
        file: DucklakeDataFile,
        tableDataPath: String,
        partitions: PartitionContext,
        pushDownRowCount: Long,
    ): DuckLakeScanRange {
        val absolutePath = pathResolver.resolveFilePath(
            file.path, file.pathIsRelative, tableDataPath,
        )
        return DuckLakeScanRange.Builder()
            .path(absolutePath)
            .start(0L)
            .length(file.fileSizeBytes)
            .fileSize(file.fileSizeBytes)
            .fileFormat(normalizeFileFormat(file.fileFormat))
            .positionDeletes(resolvePositionDeletes(file, tableDataPath))
            .partitionBearing(partitions.partitionBearing)
            .partitionValues(identityPartitionValues(file, partitions))
            .pushDownRowCount(pushDownRowCount)
            .build()
    }

    /**
     * The partition inputs one `planScan` needs, resolved once per plan (not
     * per file): the ACTIVE spec — the last of
     * [DucklakeCatalog.getPartitionSpecs], matching the trino split manager's
     * `specsForProjection.last()` — plus, only when that spec has IDENTITY
     * fields worth surfacing, the per-file value rows and the
     * columnId→name mapping. Bucket/temporal-only specs skip both extra
     * round-trips: their derived values (e.g. `year(ts)` = 2024) must NOT be
     * presented as raw column values (iceberg surfaces identity values only —
     * `IcebergPartitionUtils.getIdentityPartitionInfoMap` skips non-identity
     * transforms; we mirror that exactly), so there is nothing to fetch.
     */
    private fun resolvePartitionContext(dlHandle: DuckLakeTableHandle): PartitionContext {
        val specs = catalog.getPartitionSpecs(dlHandle.tableId, dlHandle.snapshotId)
        val activeSpec = specs.lastOrNull() ?: return PartitionContext.UNPARTITIONED
        val hasIdentityField =
            activeSpec.fields.any { it.transform == DucklakePartitionTransform.IDENTITY }
        if (!hasIdentityField) {
            return PartitionContext(activeSpec, mapOf(), mapOf())
        }
        val valuesByFileId =
            catalog.getFilePartitionValues(dlHandle.tableId, dlHandle.snapshotId)
        // Lowercased to match Doris's identifier convention (and iceberg's
        // identity-map keying via getIdentityPartitionInfoMap).
        val columnNamesById = catalog.getTableColumns(dlHandle.tableId, dlHandle.snapshotId)
            .associate { it.columnId to it.columnName.lowercase(Locale.ROOT) }
        return PartitionContext(activeSpec, valuesByFileId, columnNamesById)
    }

    /**
     * The per-file `columnName -> partitionValue` map for IDENTITY-transform
     * fields of the active spec, in spec-field order — the same value model as
     * the trino side's `DucklakeSplitManager.buildIdentityPartitionValues`
     * (columnId-keyed there; name-keyed here because the SPI map is what
     * `PluginDrivenSplit` consumes) and the same identity-only rule as
     * iceberg's `getIdentityPartitionInfoMap`.
     *
     * Partition-evolution guard (trino parity): a file's stored values are
     * keyed by the `partition_key_index` of the spec it was WRITTEN under and
     * every spec numbers keys from 0, so a file from a retired spec must yield
     * an EMPTY map rather than values mapped through the wrong spec. NULL
     * partition values are skipped (nothing to constant-fill; the BE reads the
     * column from the parquet body anyway).
     */
    private fun identityPartitionValues(
        file: DucklakeDataFile,
        partitions: PartitionContext,
    ): Map<String, String> {
        val spec = partitions.activeSpec ?: return mapOf()
        val filePartitionId = file.partitionId
        if (filePartitionId != null && filePartitionId != spec.partitionId) {
            return mapOf()
        }
        val values = partitions.valuesByFileId[file.dataFileId] ?: return mapOf()
        if (values.isEmpty()) {
            return mapOf()
        }
        val byKeyIndex = HashMap<Int, String?>(values.size)
        for (v in values) {
            byKeyIndex[v.partitionKeyIndex] = v.partitionValue
        }
        val out = LinkedHashMap<String, String>()
        for (field in spec.fields) {
            if (field.transform == DucklakePartitionTransform.IDENTITY) {
                val value = byKeyIndex[field.partitionKeyIndex]
                val columnName = partitions.columnNamesById[field.columnId]
                if (value != null && columnName != null) {
                    out[columnName] = value
                }
            }
        }
        return out
    }

    /**
     * Immutable per-plan partition inputs (see [resolvePartitionContext]).
     * [partitionBearing] is a TABLE-level fact — the presence of an active
     * spec — deliberately independent of whether a given file recorded values,
     * per the `ConnectorScanRange.isPartitionBearing` contract.
     */
    private class PartitionContext(
        val activeSpec: DucklakePartitionSpec?,
        val valuesByFileId: Map<Long, List<DucklakeFilePartitionValue>>,
        val columnNamesById: Map<Long, String>,
    ) {
        val partitionBearing: Boolean
            get() = activeSpec != null

        companion object {
            val UNPARTITIONED = PartitionContext(null, mapOf(), mapOf())
        }
    }

    /**
     * Surfaces the at-most-one active position-delete file the catalog already
     * inlines on the [DucklakeDataFile] (LEFT JOIN at
     * `JdbcDucklakeCatalog#getDataFiles`, snapshot-filtered). Returns
     * an empty list when the data file has no active deletes.
     *
     * DuckLake's catalog guarantees at most one active delete file per
     * data file per snapshot
     * (`JdbcDucklakeCatalog#checkDeleteFileOverlap`), so no second
     * catalog round-trip is needed.
     */
    private fun resolvePositionDeletes(
        file: DucklakeDataFile,
        tableDataPath: String,
    ): List<DuckLakePositionDelete> {
        val deletePath = file.deleteFilePath
        if (deletePath == null) {
            return listOf()
        }
        // PATH_IS_RELATIVE and FORMAT come from the same row as PATH; when
        // PATH is non-null the others are non-null too. Defensive defaults
        // mirror DuckLake's own convention (relative paths under the
        // warehouse, parquet format).
        val isRelative = file.deleteFilePathIsRelative ?: true
        val absolutePath = pathResolver.resolveFilePath(
            deletePath, isRelative, tableDataPath,
        )
        val format = normalizeFileFormat(file.deleteFileFormat ?: "parquet")
        return listOf(DuckLakePositionDelete(absolutePath, format))
    }

    /**
     * Emits the storage credentials (s3.*, hdfs.*, AWS_*, etc.) the BE needs
     * to open data files, under [PROP_LOCATION_PREFIX] so
     * [populateScanLevelParams] can strip the prefix back off on its
     * way to the thrift descriptor. Mirrors the Iceberg connector's pattern;
     * DuckLake adds no vended-credential layer for v1 (no REST catalog), so
     * we pass through the static catalog properties only.
     */
    override fun getScanNodeProperties(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        columns: List<ConnectorColumnHandle>,
        filter: Optional<ConnectorExpression>,
    ): Map<String, String> {
        val out: MutableMap<String, String> = linkedMapOf()
        // Tell PluginDrivenScanNode which BE reader to dispatch (else it
        // defaults to FORMAT_JNI and the iceberg reader bails with
        // "Not supported create reader for table format: iceberg / file
        // format: FORMAT_JNI").
        out[PROP_FILE_FORMAT_TYPE] = PARQUET_FORMAT
        for ((key, value) in catalogProperties) {
            if (isStorageProperty(key)) {
                out[PROP_LOCATION_PREFIX + key] = value
            }
        }
        return out
    }

    override fun populateScanLevelParams(
        params: TFileScanRangeParams,
        nodeProperties: Map<String, String>?,
    ) {
        Objects.requireNonNull(params, "params")
        if (nodeProperties == null || nodeProperties.isEmpty()) {
            return
        }
        if (params.properties == null) {
            params.properties = mutableMapOf()
        }
        val out = params.properties
        for ((key, value) in nodeProperties) {
            if (!key.startsWith(PROP_LOCATION_PREFIX)) {
                continue
            }
            val stripped = key.substring(PROP_LOCATION_PREFIX.length)
            // The BE's S3ClientFactory::convert_properties_to_s3_conf
            // (be/src/util/s3_util.cpp) reads `AWS_*` keys verbatim — the
            // FE-side S3ObjStorage normaliser does NOT run on the parquet
            // reader path. So we emit both the FE-style ("s3.*") key and the
            // canonical BE-style ("AWS_*") alias. Belt + suspenders: if the
            // user supplies one form, the BE sees the other too.
            out[stripped] = value
            val alias = canonicalAwsAlias(stripped)
            if (alias != null) {
                out.putIfAbsent(alias, value)
            }
        }
    }

    companion object {
        // Plugin-private prefix used to ferry storage credentials through the
        // scan-node-properties → populateScanLevelParams transit. Mirrors the
        // pattern in IcebergScanPlanProvider#PROP_LOCATION_PREFIX; the prefix is
        // stripped before the keys land in TFileScanRangeParams so the BE sees
        // the canonical "s3.*" form it normalises in S3ObjStorage.
        const val PROP_LOCATION_PREFIX: String = "ducklake.location."

        // PluginDrivenScanNode reads this key out of getScanNodeProperties() to
        // decide which BE reader to dispatch to (PluginDrivenScanNode.java
        // mapFileFormatType: "parquet" → FORMAT_PARQUET, default FORMAT_JNI).
        // ConnectorScanRange#getFileFormat() is NOT consumed there — the engine
        // never reads it directly for the plugin-driven path.
        const val PROP_FILE_FORMAT_TYPE: String = "file_format_type"

        // DuckLake's writer always produces parquet. The catalog records the
        // format per data file (DucklakeDataFile.fileFormat) and our scan
        // plan validates it matches, but for the scan-node-level reader
        // dispatch one shared answer is sufficient.
        const val PARQUET_FORMAT: String = "parquet"

        // The ConnectorScanRange "no precomputed count" sentinel every normal
        // range carries; only the collapsed count-pushdown range differs.
        private const val PUSH_DOWN_COUNT_NONE: Long = -1L

        /**
         * Maps an FE-form storage key to its BE-canonical `AWS_*` alias,
         * or `null` when the key has no alias. Mirrors the lookups in
         * `be/src/util/s3_util.cpp` constants:
         * ```
         *   AWS_ACCESS_KEY / AWS_SECRET_KEY / AWS_ENDPOINT / AWS_REGION / AWS_TOKEN
         * ```
         */
        fun canonicalAwsAlias(unprefixedKey: String): String? =
            when (unprefixedKey) {
                "s3.access_key" -> "AWS_ACCESS_KEY"
                "s3.secret_key" -> "AWS_SECRET_KEY"
                "s3.endpoint" -> "AWS_ENDPOINT"
                "s3.region" -> "AWS_REGION"
                "s3.session_token" -> "AWS_TOKEN"
                else -> null
            }

        /**
         * Recognises the storage-credential keys our plugin forwards to the BE.
         * Covers Doris's primary FE-form (`s3.*`), the canonical
         * `AWS_*` aliases, the legacy `aws.*` form, and the
         * path-style toggle. Engine-injected DuckLake-specific keys
         * (`metadata.*`, `storage.warehouse`, `type`,
         * `enable.mapping.varbinary`) are deliberately excluded — they're
         * either FE-only or carry no meaning on the BE.
         */
        fun isStorageProperty(key: String): Boolean =
            key.startsWith("s3.") ||
                key.startsWith("AWS_") ||
                key.startsWith("aws.") ||
                key.startsWith("fs.") ||
                key == "use_path_style"

        private fun normalizeFileFormat(catalogFormat: String?): String =
            // The catalog stores the format as-recorded by the writer
            // ("parquet" / "PARQUET"). Doris's BE reader dispatch is case-sensitive
            // on the file_format string in the thrift range descriptor.
            if (catalogFormat == null) {
                "parquet"
            } else {
                catalogFormat.lowercase(java.util.Locale.getDefault())
            }
    }
}
