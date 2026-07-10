package dev.brikk.ducklake.doris.plugin

import java.time.Instant
import java.util.Locale

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.ExpireSnapshotsResult
import dev.brikk.ducklake.catalog.TransactionConflictException

import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.ConnectorType
import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.handle.ConnectorTableHandle
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult
import org.apache.doris.connector.api.procedure.ProcedureExecutionMode
import org.apache.doris.connector.api.pushdown.ConnectorPredicate

/**
 * Table-procedure surface of the DuckLake connector — the procedure-side analogue of
 * [DuckLakeScanPlanProvider] / [DuckLakeWritePlanProvider], returned from
 * [DuckLakeConnector.getProcedureOps]. Exposes the maintenance procedures
 * `ALTER TABLE <t> EXECUTE <proc>(...)`:
 *  - `expire_snapshots` — metadata-only: end-snapshots old versions and SCHEDULES their now-dead
 *    files for later deletion (a catalog transaction; no storage I/O).
 *  - `cleanup_old_files` — the second phase: physically deletes the blobs `expire_snapshots`
 *    scheduled, once older than a grace period, via the connector's own S3 client
 *    ([WarehouseBlobStore]); then drops their schedule rows.
 *  - `remove_orphan_files` — deletes objects under a table's data path that NO catalog row
 *    references (residue of aborted commits) and are older than a grace period. Unlike the
 *    catalog-wide procedures above, this one is inherently **table-scoped** (it diffs one table's
 *    storage against `listReferencedFilePaths`), so it uses the named table and REQUIRES a handle.
 *
 * ## Why one ops class per connector (not one per procedure)
 * Doris routes every `ALTER TABLE EXECUTE` for a connector through this one
 * [ConnectorProcedureOps] and dispatches on [procedureName] inside [execute] (unlike Trino's
 * one-`Provider<Procedure>`-per-procedure model). So the argument contract, validation, and
 * result shaping all live here rather than in a per-procedure class.
 *
 * ## Catalog-wide vs. table-scoped (the important semantic mismatch — read this)
 * DuckLake snapshots are **catalog-wide** versions, not per-table ones (Trino models the same
 * catalog with a catalog-scoped `system.expire_snapshots`, no table argument). But Doris only
 * offers the *table-scoped* `ALTER TABLE <t> EXECUTE proc(...)` syntax and hands us a
 * [ConnectorTableHandle]. We resolve this honestly rather than faking per-table expiry (which
 * does not exist in DuckLake and would be silently wrong): the procedure runs **catalog-wide**,
 * ignoring which table the operator named for *selection* purposes, and the returned result row
 * says so explicitly (`scope = "catalog-wide (ignores the named table)"`) so nobody is surprised
 * that `ALTER TABLE a EXECUTE expire_snapshots` also affected snapshots involving table `b`. The
 * table handle may be null (in case Doris ever allows a catalog-scoped invocation); we work either
 * way.
 */
internal class DuckLakeProcedureOps(
    private val catalog: DucklakeCatalog,
    private val properties: Map<String, String>,
    blobStoreProvider: (() -> WarehouseBlobStore)? = null,
) : ConnectorProcedureOps {

    // Retention floor shared by expire_snapshots (protect recent time-travel snapshots) and
    // cleanup_old_files (grace period protecting in-flight, possibly cross-engine, readers of a
    // scheduled file). From catalog prop `maintenance.min-retention` (mirroring Trino's
    // `ducklake.maintenance.min-retention`); absent/blank → conservative 7d. Parsed once at
    // construction so a misconfigured catalog fails fast, not per call.
    private val minRetentionMillis: Long =
        parseDuration(
            properties[DuckLakeConnectorProperties.MAINTENANCE_MIN_RETENTION]
                ?.takeIf { it.isNotBlank() }
                ?: DEFAULT_RETENTION,
            DuckLakeConnectorProperties.MAINTENANCE_MIN_RETENTION,
        )

    // The warehouse root that relative scheduled-file paths resolve against (the catalog data_path,
    // NOT a per-table dir — per the DucklakeScheduledFile contract). Falls back to the configured
    // storage.warehouse property. Lazy: only cleanup_old_files needs it, so expire_snapshots never
    // calls getDataPath() (keeps read-only / expire-only paths from touching it).
    private val warehouseRoot: String? by lazy {
        catalog.getDataPath() ?: properties[DuckLakeConnectorProperties.STORAGE_WAREHOUSE]
    }

    // Lazily built so a read-only catalog / expire_snapshots never constructs an S3 client, and a
    // missing-credential failure surfaces only when cleanup_old_files actually runs. Tests inject a
    // provider returning a fake [WarehouseBlobStore]; prod defaults to the S3/MinIO client.
    private val blobStore: WarehouseBlobStore by lazy {
        (blobStoreProvider ?: { S3WarehouseBlobStore.fromProperties(properties) })()
    }

    // Resolves a table's data path (for remove_orphan_files). Lazy so only that procedure builds it.
    private val pathResolver: DuckLakePathResolver by lazy {
        DuckLakePathResolver(catalog, properties[DuckLakeConnectorProperties.STORAGE_WAREHOUSE])
    }

    override fun getSupportedProcedures(): List<String> =
        listOf(EXPIRE_SNAPSHOTS, CLEANUP_OLD_FILES, REMOVE_ORPHAN_FILES)

    /**
     * Both procedures are metadata + FE-side storage ops (never a distributed data rewrite), so they
     * run as a single synchronous in-FE call. `planRewrite` is left as the interface default (throws).
     */
    override fun getExecutionMode(procedureName: String): ProcedureExecutionMode =
        ProcedureExecutionMode.SINGLE_CALL

    @Suppress("ThrowsCount")
    override fun execute(
        session: ConnectorSession?,
        table: ConnectorTableHandle?,
        procedureName: String?,
        properties: Map<String, String>?,
        whereCondition: ConnectorPredicate?,
        partitionNames: List<String>?,
    ): ConnectorProcedureResult {
        val proc = SUPPORTED_BY_LOWER[procedureName?.lowercase(Locale.ROOT)]
            ?: throw DorisConnectorException(
                "DuckLake connector does not support procedure '$procedureName'; " +
                    "supported: ${getSupportedProcedures()}",
            )
        // A WHERE / PARTITION clause is meaningless for these GC procedures; reject it loud rather
        // than silently ignoring an operator's intent to scope the operation.
        if (whereCondition != null) {
            throw DorisConnectorException("$proc does not accept a WHERE condition")
        }
        if (!partitionNames.isNullOrEmpty()) {
            throw DorisConnectorException("$proc does not accept a PARTITION clause")
        }
        return when (proc) {
            EXPIRE_SNAPSHOTS -> expireSnapshots(properties.orEmpty())
            CLEANUP_OLD_FILES -> cleanupOldFiles(properties.orEmpty())
            REMOVE_ORPHAN_FILES -> removeOrphanFiles(table, properties.orEmpty())
            else -> error("unreachable: $proc") // SUPPORTED_BY_LOWER only maps known procedures
        }
    }

    /**
     * Core body of `expire_snapshots`. Selection is mutually exclusive: an explicit
     * `snapshot_ids` list (passed to the catalog as the `versions` set, which intersects it with
     * the non-latest snapshots there), OR everything older than `retention_threshold` (floored by
     * [minRetentionMillis]). The latest snapshot is NEVER expirable (the catalog enforces that).
     * `dry_run = true` reports what WOULD be expired and calls only the read-only
     * [DucklakeCatalog.listExpirableSnapshots] — never [DucklakeCatalog.expireSnapshots].
     */
    @Suppress("ThrowsCount")
    private fun expireSnapshots(properties: Map<String, String>): ConnectorProcedureResult {
        val args = CaseInsensitiveArgs(properties)
        val dryRun = parseBoolean(args[DRY_RUN], DRY_RUN)
        val explicitRaw = args[SNAPSHOT_IDS]
        val retentionRaw = args[RETENTION_THRESHOLD]

        if (!explicitRaw.isNullOrBlank() && !retentionRaw.isNullOrBlank()) {
            throw DorisConnectorException(
                "snapshot_ids and retention_threshold are mutually exclusive; pass only one",
            )
        }

        val expirable: List<Long> = if (!explicitRaw.isNullOrBlank()) {
            val explicit = parseSnapshotIds(explicitRaw)
            catalog.listExpirableSnapshots(null, explicit)
        } else {
            val retention = retentionRaw?.takeIf { it.isNotBlank() } ?: DEFAULT_RETENTION
            val retentionMillis = parseDuration(retention, RETENTION_THRESHOLD)
            if (retentionMillis < minRetentionMillis) {
                throw DorisConnectorException(
                    "retention_threshold '$retention' is below the minimum " +
                        "'${DuckLakeConnectorProperties.MAINTENANCE_MIN_RETENTION}' floor " +
                        "(${minRetentionMillis}ms); refusing to expire recent snapshots. " +
                        "Pass an explicit snapshot_ids list to override.",
                )
            }
            val cutoff: Instant = Instant.now().minusMillis(retentionMillis)
            catalog.listExpirableSnapshots(cutoff, null)
        }

        if (dryRun || expirable.isEmpty()) {
            return result(dryRun = dryRun, expiredCount = 0, scheduledFileCount = -1, ids = expirable)
        }
        val result: ExpireSnapshotsResult = try {
            catalog.expireSnapshots(expirable.toSet())
        } catch (e: TransactionConflictException) {
            throw DorisConnectorException(
                "expire_snapshots conflicted with a concurrent catalog commit; retry: ${e.message}", e,
            )
        }
        return result(
            dryRun = false,
            expiredCount = result.expiredSnapshotCount,
            scheduledFileCount = result.scheduledFileCount,
            ids = expirable,
        )
    }

    /**
     * Build the one-row result table. `scheduled_file_count` is reported as the string `"n/a"`
     * for a dry run (nothing was scheduled), otherwise the count the catalog scheduled for later
     * physical deletion (reclaimed by a separate age-gated cleanup, not by this call).
     */
    private fun result(
        dryRun: Boolean,
        expiredCount: Int,
        scheduledFileCount: Int,
        ids: List<Long>,
    ): ConnectorProcedureResult {
        val row = listOf(
            dryRun.toString(),
            CATALOG_WIDE_SCOPE,
            (if (dryRun) ids.size else expiredCount).toString(),
            if (scheduledFileCount < 0) "n/a" else scheduledFileCount.toString(),
            ids.joinToString(","),
        )
        return ConnectorProcedureResult(RESULT_SCHEMA, listOf(row))
    }

    /**
     * Core body of `cleanup_old_files` — the physical-deletion phase that pairs with
     * `expire_snapshots`'s scheduling phase. Deletes the warehouse blobs in
     * `ducklake_files_scheduled_for_deletion` older than the grace period (`retention_threshold`,
     * floored by [minRetentionMillis] to protect in-flight, possibly cross-engine, readers), then
     * drops the schedule rows of the files actually deleted (a file that fails to delete keeps its
     * row for a later retry). `dry_run = true` reports what WOULD be deleted and touches no storage.
     *
     * Delete is best-effort per file (see [WarehouseBlobStore.delete]); we only
     * [DucklakeCatalog.removeScheduledFileRows] for the successes, so a partial run makes real
     * progress and is safely resumable.
     */
    @Suppress("ThrowsCount")
    private fun cleanupOldFiles(properties: Map<String, String>): ConnectorProcedureResult {
        val args = CaseInsensitiveArgs(properties)
        val dryRun = parseBoolean(args[DRY_RUN], DRY_RUN)
        val retention = args[RETENTION_THRESHOLD]?.takeIf { it.isNotBlank() } ?: DEFAULT_RETENTION
        val retentionMillis = parseDuration(retention, RETENTION_THRESHOLD)
        if (retentionMillis < minRetentionMillis) {
            throw DorisConnectorException(
                "retention_threshold '$retention' is below the minimum " +
                    "'${DuckLakeConnectorProperties.MAINTENANCE_MIN_RETENTION}' floor " +
                    "(${minRetentionMillis}ms); refusing to delete recently-scheduled files.",
            )
        }
        val cutoff: Instant = Instant.now().minusMillis(retentionMillis)
        val scheduled = catalog.listFilesScheduledForDeletion(cutoff)
        if (scheduled.isEmpty() || dryRun) {
            return fileGcResult(dryRun, retention, deleted = 0, wouldDelete = scheduled.size, failed = 0)
        }

        // Map each resolved absolute URI back to its data_file_id so we can drop exactly the rows we
        // deleted. Multiple ids can't share a URI in practice, but last-writer-wins is fine here.
        val idByUri = LinkedHashMap<String, Long>(scheduled.size)
        for (f in scheduled) {
            idByUri[resolveScheduledUri(f.path, f.pathIsRelative)] = f.dataFileId
        }

        val outcome = blobStore.delete(idByUri.keys)
        val deletedIds = outcome.deleted.mapNotNull { idByUri[it] }
        if (deletedIds.isNotEmpty()) {
            catalog.removeScheduledFileRows(deletedIds)
        }
        return fileGcResult(
            dryRun = false,
            retention = retention,
            deleted = outcome.deleted.size,
            wouldDelete = scheduled.size,
            failed = outcome.failed.size,
        )
    }

    /**
     * Core body of `remove_orphan_files` — deletes objects under the target table's data path that
     * NO catalog row references (residue of aborted commits) and that are older than the grace
     * period (`retention_threshold`, floored by [minRetentionMillis] to protect files an in-flight,
     * possibly cross-engine, writer produced but hasn't yet committed). Storage-only: touches no
     * catalog state (orphans have no rows). `dry_run = true` reports without deleting.
     *
     * Table-scoped (unlike expire_snapshots/cleanup_old_files): it diffs ONE table's storage against
     * [DucklakeCatalog.listReferencedFilePaths], so a target table handle is REQUIRED. The known set
     * and the storage listing are resolved to the SAME absolute-URI form so the diff can't
     * misclassify a live file as an orphan (see [OrphanFiles.find]).
     */
    @Suppress("ThrowsCount")
    private fun removeOrphanFiles(table: ConnectorTableHandle?, properties: Map<String, String>): ConnectorProcedureResult {
        val handle = table?.asDuckLakeHandle<DuckLakeTableHandle>()
            ?: throw DorisConnectorException(
                "remove_orphan_files requires a target table; run " +
                    "ALTER TABLE <table> EXECUTE remove_orphan_files(...)",
            )
        val args = CaseInsensitiveArgs(properties)
        val dryRun = parseBoolean(args[DRY_RUN], DRY_RUN)
        val retention = args[RETENTION_THRESHOLD]?.takeIf { it.isNotBlank() } ?: DEFAULT_RETENTION
        val retentionMillis = parseDuration(retention, RETENTION_THRESHOLD)
        if (retentionMillis < minRetentionMillis) {
            throw DorisConnectorException(
                "retention_threshold '$retention' is below the minimum " +
                    "'${DuckLakeConnectorProperties.MAINTENANCE_MIN_RETENTION}' floor " +
                    "(${minRetentionMillis}ms); refusing to delete recently-written files.",
            )
        }

        val schema = catalog.getSchema(handle.database, handle.snapshotId)
            ?: throw DorisConnectorException("schema not found: ${handle.database}")
        val tbl = catalog.getTable(handle.database, handle.table, handle.snapshotId)
            ?: throw DorisConnectorException("table not found: ${handle.database}.${handle.table}")
        val tableDataPath = pathResolver.resolveTableDataPath(schema, tbl)

        // Known set: every path the catalog references for this table (live + end-snapshotted +
        // already-scheduled), resolved to the same absolute-URI form the listing produces.
        val knownPaths = catalog.listReferencedFilePaths(handle.tableId)
            .map { pathResolver.resolveFilePath(it.path, it.pathIsRelative, tableDataPath) }
            .toSet()

        val cutoff: Instant = Instant.now().minusMillis(retentionMillis)
        val orphans = OrphanFiles.find(blobStore.list(tableDataPath), knownPaths, cutoff)

        if (orphans.isEmpty() || dryRun) {
            return fileGcResult(dryRun, retention, deleted = 0, wouldDelete = orphans.size, failed = 0)
        }
        val outcome = blobStore.delete(orphans)
        return fileGcResult(
            dryRun = false,
            retention = retention,
            deleted = outcome.deleted.size,
            wouldDelete = orphans.size,
            failed = outcome.failed.size,
        )
    }

    /**
     * Resolve a scheduled file's stored path to an absolute storage URI. Relative paths join the
     * catalog data_path ROOT ([warehouseRoot]); absolute paths pass through. Fails loud if a
     * relative path has no known root (we can't safely guess where to delete).
     */
    private fun resolveScheduledUri(path: String, isRelative: Boolean): String {
        if (!isRelative) {
            return path
        }
        val root = warehouseRoot
            ?: throw DorisConnectorException(
                "cleanup_old_files: relative scheduled path '$path' but no warehouse root " +
                    "('storage.warehouse') is configured to resolve it against",
            )
        return if (root.endsWith("/")) root + path else "$root/$path"
    }

    /** One-row result shared by cleanup_old_files + remove_orphan_files. On a dry run,
     * `deleted_file_count` reports what WOULD be deleted. */
    private fun fileGcResult(
        dryRun: Boolean,
        retention: String,
        deleted: Int,
        wouldDelete: Int,
        failed: Int,
    ): ConnectorProcedureResult {
        val row = listOf(
            dryRun.toString(),
            retention,
            (if (dryRun) wouldDelete else deleted).toString(),
            failed.toString(),
        )
        return ConnectorProcedureResult(FILE_GC_RESULT_SCHEMA, listOf(row))
    }

    /** Parse a comma-separated `snapshot_ids` list; every element must be a bigint. */
    private fun parseSnapshotIds(raw: String): Set<Long> {
        val ids = raw.split(',')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .map { token ->
                token.toLongOrNull() ?: throw DorisConnectorException(
                    "Invalid snapshot_ids element '$token': expected a bigint",
                )
            }
        if (ids.isEmpty()) {
            throw DorisConnectorException("snapshot_ids was given but empty")
        }
        return ids.toSet()
    }

    /** Case-insensitive view over the raw `properties` map (Doris may lowercase argument keys). */
    private class CaseInsensitiveArgs(properties: Map<String, String>) {
        private val byLowerKey: Map<String, String> =
            properties.entries.associate { (k, v) -> k.lowercase(Locale.ROOT) to v }

        operator fun get(key: String): String? = byLowerKey[key.lowercase(Locale.ROOT)]
    }

    internal companion object {
        internal const val EXPIRE_SNAPSHOTS = "expire_snapshots"
        internal const val CLEANUP_OLD_FILES = "cleanup_old_files"
        internal const val REMOVE_ORPHAN_FILES = "remove_orphan_files"

        // Case-insensitive dispatch: lowercased procedure name → canonical name.
        private val SUPPORTED_BY_LOWER: Map<String, String> =
            listOf(EXPIRE_SNAPSHOTS, CLEANUP_OLD_FILES, REMOVE_ORPHAN_FILES).associateBy { it }

        internal const val RETENTION_THRESHOLD = "retention_threshold"
        internal const val SNAPSHOT_IDS = "snapshot_ids"
        internal const val DRY_RUN = "dry_run"

        internal const val DEFAULT_RETENTION = "7d"

        private const val CATALOG_WIDE_SCOPE = "catalog-wide (ignores the named table)"

        private const val MILLIS_PER_SECOND = 1_000L
        private const val SECONDS_PER_MINUTE = 60L
        private const val MINUTES_PER_HOUR = 60L
        private const val HOURS_PER_DAY = 24L

        /**
         * Result schema (all values are Strings in the row):
         *  - `dry_run` — whether this was a dry run (`true`/`false`)
         *  - `scope` — the catalog-wide-vs-table disclaimer (see class KDoc)
         *  - `expired_snapshot_count` — snapshots expired (dry run: how many WOULD be)
         *  - `scheduled_file_count` — files scheduled for later deletion (dry run: `n/a`)
         *  - `snapshot_ids` — the affected snapshot ids, comma-separated
         */
        private val RESULT_SCHEMA: List<ConnectorColumn> = listOf(
            column("dry_run", ConnectorType.of("BOOLEAN")),
            column("scope", ConnectorType.of("STRING")),
            column("expired_snapshot_count", ConnectorType.of("BIGINT")),
            column("scheduled_file_count", ConnectorType.of("STRING")),
            column("snapshot_ids", ConnectorType.of("STRING")),
        )

        /**
         * Result schema shared by cleanup_old_files + remove_orphan_files (all values Strings):
         *  - `dry_run` — whether this was a dry run
         *  - `retention_threshold` — the effective grace period used
         *  - `deleted_file_count` — blobs deleted (dry run: how many WOULD be)
         *  - `failed_file_count` — blobs that couldn't be deleted (left for a later retry)
         */
        private val FILE_GC_RESULT_SCHEMA: List<ConnectorColumn> = listOf(
            column("dry_run", ConnectorType.of("BOOLEAN")),
            column("retention_threshold", ConnectorType.of("STRING")),
            column("deleted_file_count", ConnectorType.of("BIGINT")),
            column("failed_file_count", ConnectorType.of("BIGINT")),
        )

        private fun column(name: String, type: ConnectorType): ConnectorColumn =
            ConnectorColumn(name, type, "", true, null)

        /**
         * Parse a `<number><unit>` duration (airlift-`Duration` grammar subset) into milliseconds.
         * Supported units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days), **lowercase only**
         * — e.g. `7d`, `24h`, `30m`, `60s`. Optional whitespace between number and unit. Rejects a
         * missing unit, an unknown or uppercase unit, a multi-letter unit (`7dd`, `7 days`), a
         * non-numeric/negative/decimal magnitude, or overflow with a [DorisConnectorException]
         * (loud over silently-wrong). `argName` names the offending argument in the error message.
         */
        @Suppress("ThrowsCount")
        internal fun parseDuration(value: String, argName: String): Long {
            val trimmed = value.trim()
            val match = DURATION_PATTERN.matchEntire(trimmed)
                ?: throw DorisConnectorException(
                    "Invalid $argName '$value': expected <number><unit> where unit is one of " +
                        "s/m/h/d (e.g. 7d, 24h, 30m, 60s)",
                )
            val magnitude = match.groupValues[1].toLongOrNull()
                ?: throw DorisConnectorException("Invalid $argName '$value': magnitude out of range")
            val unitMillis = when (match.groupValues[2]) {
                "s" -> MILLIS_PER_SECOND
                "m" -> MILLIS_PER_SECOND * SECONDS_PER_MINUTE
                "h" -> MILLIS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR
                "d" -> MILLIS_PER_SECOND * SECONDS_PER_MINUTE * MINUTES_PER_HOUR * HOURS_PER_DAY
                else -> throw DorisConnectorException("Invalid $argName '$value': unknown unit")
            }
            return try {
                Math.multiplyExact(magnitude, unitMillis)
            } catch (e: ArithmeticException) {
                throw DorisConnectorException("Invalid $argName '$value': duration overflow", e)
            }
        }

        // <number> then optional spaces then letters. Anchored via matchEntire, so trailing junk
        // ("7 days") can't match at all; a multi-letter or uppercase unit ("7dd", "7D") DOES match
        // the regex but is then rejected by the exhaustive `when` on the unit — either way it throws.
        private val DURATION_PATTERN = Regex("""(\d+)\s*([a-zA-Z]+)""")

        /** Parse a boolean argument case-insensitively; blank/absent = false. Rejects other tokens. */
        internal fun parseBoolean(value: String?, argName: String): Boolean {
            val v = value?.trim()?.lowercase(Locale.ROOT)
            return when {
                v.isNullOrEmpty() -> false
                v == "true" -> true
                v == "false" -> false
                else -> throw DorisConnectorException(
                    "Invalid $argName '$value': expected true or false",
                )
            }
        }
    }
}
