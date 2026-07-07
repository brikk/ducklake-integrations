package dev.brikk.ducklake.doris.plugin

import java.util.Locale
import java.util.Objects
import java.util.Optional

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
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

        // DuckLake inlines small INSERTs/DELETEs into metadata-catalog rows
        // (ducklake_inlined_data_* / ducklake_inlined_delete_*) by default on
        // PG backends; the Doris BE reads FILES only. Inlined DELETEs (of file
        // rows) are still unsupported — fail loud, never silently over-return.
        // Inlined DATA rows are synthesized into a temp Parquet the BE can scan
        // (Stage 1: scalar columns, local-fs warehouse) — see
        // planInlinedDataRanges; unsupported inlined-data cases fail loud too.
        failOnInlinedDeletes(dlHandle)

        val tableDataPath = pathResolver.resolveTableDataPath(schema, table)
        val dataFiles = catalog.getDataFiles(dlHandle.tableId, dlHandle.snapshotId)

        // Correctness gate (corpus stats/count_star_optimization_time_travel):
        // a partial (cross-snapshot compacted) data file physically holds rows
        // from snapshots NEWER than the one pinned here, tagged by a hidden
        // `_ducklake_internal_snapshot_id` column that a read AS OF an older
        // snapshot must drop (`partial_max > snapshot`). Trino filters those
        // rows in its page source; the Doris BE reader has no hook to apply a
        // hidden-column snapshot predicate, so scanning the file wholesale
        // would SILENTLY over-return (300 rows where 100 were live). Fail
        // loudly until the BE can snapshot-filter partial files.
        failOnUnfilterablePartialFile(dlHandle, dataFiles)

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
        // Inlined data rows: synthesize a temp Parquet the BE scans alongside
        // the file ranges (union is implicit — more ranges for the same scan).
        // ALWAYS added, even under a file-level prune: `prunedFileIds` targets
        // catalog DATA files by id, but inlined rows live in no file, so the
        // prune can't (and must not) exclude them — the BE re-applies the
        // pushed filter to the inlined range's rows. (Dropping them under a
        // prune was a real bug: `WHERE ...` returned empty for inlined tables.)
        ranges.addAll(planInlinedDataRanges(dlHandle, tableDataPath))
        return ranges
    }

    /**
     * Throws [DorisConnectorException] when the table has inlined DELETEs of
     * file rows at the pinned snapshot. Those tombstone file-resident rows via
     * catalog positions the BE can't apply, so scanning would over-return —
     * fail loud (Stage 2 item). Inlined DATA rows are NOT gated here; they're
     * served by [planInlinedDataRanges].
     */
    private fun failOnInlinedDeletes(handle: DuckLakeTableHandle) {
        if (catalog.hasInlinedDeletes(handle.tableId, handle.snapshotId)) {
            throw DorisConnectorException(
                "DuckLake table has inlined DELETEs in the metadata catalog (rows in " +
                    "ducklake_inlined_delete_*, not a delete file); the Doris connector can't apply " +
                    "them at scan time yet. Flush them (DuckDB: CALL <lake>.flush_inlined_data()) or " +
                    "disable inlining (CALL <lake>.set_option('data_inlining_row_limit', '0')) and rewrite.",
            )
        }
    }

    /**
     * Materializes DuckLake inlined DATA rows into a temp Parquet the BE scans
     * as a normal file range (Stage 1). The catalog snapshot-filters the rows;
     * [DuckLakeInlinedParquetWriter] writes them with `field_id == column_id`
     * so the schema dictionary + BE field-id matching resolve them exactly like
     * a real file. One range per live `(schemaVersion)` inlined table.
     *
     * Stage-1 guards (fail loud, never silently wrong):
     *  - **scalar columns only** — a nested (list/struct/map) inlined column
     *    needs the DuckDB-text recursive parser (Stage 2);
     *  - **local-filesystem warehouse only** — the temp file is written next to
     *    the table's data files so the BE reaches it at the same path; writing
     *    to an S3 warehouse from the FE (and its lifecycle) is Stage 2.
     */
    private fun planInlinedDataRanges(
        handle: DuckLakeTableHandle,
        tableDataPath: String,
    ): List<ConnectorScanRange> {
        val infos = catalog.getInlinedDataInfos(handle.tableId, handle.snapshotId)
            .filter { it.hasLiveRows }
        if (infos.isEmpty()) {
            return emptyList()
        }
        val columns = topLevelScalarColumnsOrThrow(handle)
        val localDir = localWarehouseDirOrThrow(tableDataPath)
        val ranges = ArrayList<ConnectorScanRange>(infos.size)
        for (info in infos) {
            val rows = catalog.readInlinedData(handle.tableId, info.schemaVersion, handle.snapshotId, columns)
            if (rows.isEmpty()) {
                continue
            }
            val fileName = "ducklake-inlined-${handle.tableId}-${info.schemaVersion}-${handle.snapshotId}.parquet"
            val target = localDir.resolve(fileName)
            DuckLakeInlinedParquetWriter.write(target, columns, rows)
            val size = java.nio.file.Files.size(target)
            ranges.add(
                DuckLakeScanRange.Builder()
                    .path(target.toString())
                    .start(0L)
                    .length(size)
                    .fileSize(size)
                    .fileFormat(PARQUET_FORMAT)
                    .positionDeletes(emptyList())
                    .partitionBearing(false)
                    .partitionValues(emptyMap())
                    .pushDownRowCount(PUSH_DOWN_COUNT_NONE)
                    .build(),
            )
        }
        return ranges
    }

    /**
     * Top-level columns at the pinned snapshot, or throw if any is nested or an
     * inlined-unsupported scalar (Stage 2). Nested (list/struct/map) needs the
     * DuckDB-text recursive parser. `timestamptz` is excluded for now: the
     * synthesized parquet's zone-aware timestamp surfaces as an UNSUPPORTED
     * type in Nereids on read-back (`CheckDataTypes`), unlike a real
     * DuckLake-written file — needs a BE round-trip fix (Stage 2).
     */
    private fun topLevelScalarColumnsOrThrow(handle: DuckLakeTableHandle): List<DucklakeColumn> {
        val columns = catalog.getTableColumns(handle.tableId, handle.snapshotId)
            .filter { it.parentColumn == null }
            .sortedBy { it.columnOrder }
        val unsupported = columns.firstOrNull { !isInlinedWritableScalar(it.columnType) }
        if (unsupported != null) {
            // "not supported" wording is deliberate — the corpus mirror's error
            // classifier recognizes it as a documented engine-skip.
            throw DorisConnectorException(
                "reading inlined data with column '${unsupported.columnName}' (${unsupported.columnType}) " +
                    "is not supported yet by the Doris connector (nested types, timestamptz, and " +
                    "degraded-to-string types are Stage 2). Flush inlined data " +
                    "(CALL <lake>.flush_inlined_data()) or disable inlining and rewrite.",
            )
        }
        return columns
    }

    /**
     * Whether [DuckLakeInlinedParquetWriter] can encode this column type. The
     * writer covers the non-degraded scalar core (bool, signed/small-unsigned
     * ints, floats, date, timestamp[_s/_ms/_ns], varchar, blob, decimal).
     * Excluded (Stage 2): nested (list/struct/map), timestamptz (Nereids
     * read-back gap), and the degraded-to-STRING/other types the writer's
     * schema builder rejects (time/timetz, json/variant/interval, uuid,
     * uint32/64/128, int128, geometry family).
     */
    private fun isInlinedWritableScalar(ducklakeType: String): Boolean {
        val t = ducklakeType.trim().lowercase(java.util.Locale.getDefault())
        if (isNestedType(t)) {
            return false
        }
        if (t.startsWith("decimal(")) {
            return true
        }
        return t in INLINED_WRITABLE_SCALARS
    }

    /**
     * The table's data dir as a local filesystem path, or throw for a non-local
     * (e.g. `s3://`) warehouse. Stage-1 writes the temp Parquet here so the BE
     * resolves it at the same absolute path.
     */
    private fun localWarehouseDirOrThrow(tableDataPath: String): java.nio.file.Path {
        val uri = try {
            java.net.URI(tableDataPath)
        } catch (_: Exception) {
            null
        }
        val scheme = uri?.scheme
        val localPath = when {
            scheme == null -> tableDataPath // bare path
            scheme == "file" -> uri.path
            else -> throw DorisConnectorException(
                "DuckLake table has inlined data but its warehouse is '$scheme://…'; the Doris connector " +
                    "can only materialize inlined rows to a local-filesystem warehouse yet. Flush inlined " +
                    "data (CALL <lake>.flush_inlined_data()) or disable inlining and rewrite.",
            )
        }
        return java.nio.file.Paths.get(localPath)
    }

    private fun isNestedType(ducklakeType: String): Boolean {
        val t = ducklakeType.trim().lowercase(java.util.Locale.getDefault())
        return t.startsWith("list<") || t.startsWith("struct<") || t.startsWith("map<")
    }

    /**
     * Throws when a partial (cross-snapshot compacted) data file needs
     * hidden-column snapshot filtering the Doris BE can't apply — i.e. a time
     * travel read AS OF a snapshot OLDER than the file's `partial_max`. At the
     * latest snapshot every partial file's rows are all live, so this never
     * fires for a normal (non-time-travel) read.
     */
    private fun failOnUnfilterablePartialFile(
        handle: DuckLakeTableHandle,
        dataFiles: List<DucklakeDataFile>,
    ) {
        if (dataFiles.any { (it.partialMax ?: Long.MIN_VALUE) > handle.snapshotId }) {
            throw DorisConnectorException(
                "Time travel to snapshot ${handle.snapshotId} reads a compacted (partial) data file " +
                    "that also contains rows from newer snapshots; the Doris connector cannot yet apply " +
                    "the per-row snapshot filter (hidden _ducklake_internal_snapshot_id column) the BE " +
                    "would need, so this read is not supported. Reads at the latest snapshot are unaffected.",
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
     *  - the handle is pinned to a NON-LATEST snapshot (time travel): a
     *    compaction merges several snapshots' rows into one file whose
     *    `record_count` is the MERGED total (e.g. 300) while only a subset was
     *    live at the older pinned snapshot (e.g. 100). DuckLake tracks that
     *    per-snapshot visibility in ways a raw record_count sum cannot express
     *    — even for a fully-merged file where `partial_max` is null — so a
     *    metadata count is only exact AT LATEST. Older snapshots fall back to
     *    a normal read (BE applies snapshot visibility). This subsumes the
     *    partial-file guard below for the time-travel case.
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
        // Time travel: metadata record_count is exact only at the latest
        // snapshot (compaction merges cross-snapshot rows into one file whose
        // count overcounts an older pin). Older snapshots read normally.
        if (dlHandle.snapshotId != catalog.currentSnapshotId) {
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
        // Schema dictionary: make the BE match file↔table columns by field id
        // (renamed/reordered columns read correctly instead of NULL), plus the
        // per-file name_mapping fallback for add_files/legacy files. Built from
        // the requested column handles + the active files' name maps; decoded
        // back onto the params in populateScanLevelParams. See
        // DuckLakeSchemaDictionary.
        schemaDictionaryProp(handle, columns)?.let { out[PROP_SCHEMA_DICTIONARY] = it }
        return out
    }

    /**
     * Base64 schema dictionary for the requested columns, carrying the union of
     * the active data files' `ducklake_name_mapping` alternate names. Returns
     * null when there are no DuckLake column handles to describe.
     */
    private fun schemaDictionaryProp(
        handle: ConnectorTableHandle,
        columns: List<ConnectorColumnHandle>,
    ): String? {
        val dlColumns = columns.mapNotNull { it as? DuckLakeColumnHandle }
        if (dlColumns.isEmpty()) {
            return null
        }
        val dlHandle = handle.asDuckLakeHandle<DuckLakeTableHandle>()
        val mappingIds = catalog.getDataFiles(dlHandle.tableId, dlHandle.snapshotId)
            .mapNotNull { it.mappingId }
            .toSet()
        val nameMaps = if (mappingIds.isEmpty()) emptyList() else catalog.getNameMaps(mappingIds).values
        return DuckLakeSchemaDictionary.encode(dlColumns, nameMaps)
    }

    override fun populateScanLevelParams(
        params: TFileScanRangeParams,
        nodeProperties: Map<String, String>?,
    ) {
        Objects.requireNonNull(params, "params")
        if (nodeProperties == null || nodeProperties.isEmpty()) {
            return
        }
        // Decode the field-id schema dictionary onto the real scan params
        // (current_schema_id + history_schema_info) — see DuckLakeSchemaDictionary.
        DuckLakeSchemaDictionary.apply(params, nodeProperties[PROP_SCHEMA_DICTIONARY])
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

        // Ferries the base64 field-id schema dictionary from getScanNodeProperties
        // to populateScanLevelParams (the two SPI methods share no instance
        // state), mirroring iceberg's "iceberg.schema_evolution" prop.
        const val PROP_SCHEMA_DICTIONARY: String = "ducklake.schema_dictionary"

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

        // Scalar DuckLake types DuckLakeInlinedParquetWriter can encode
        // (decimal(p,s) handled separately). Keep in sync with the writer.
        private val INLINED_WRITABLE_SCALARS: Set<String> = setOf(
            "boolean",
            "int8", "int16", "int32", "uint8", "uint16",
            "int64", "uint32",
            "float32", "float64",
            "date",
            "timestamp", "timestamp_s", "timestamp_ms", "timestamp_ns",
            "varchar", "blob",
        )

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
