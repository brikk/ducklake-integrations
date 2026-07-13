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
import com.google.inject.Inject
import com.google.inject.Provider
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.ExpireSnapshotsResult
import dev.brikk.ducklake.catalog.TransactionConflictException
import io.airlift.log.Logger
import io.airlift.units.Duration
import io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT
import io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.procedure.Procedure
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.VarcharType.VARCHAR
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.time.Instant

/**
 * Implements `CALL <catalog>.system.expire_snapshots(retention_threshold => '7d', snapshot_ids =>
 * NULL, dry_run => false)` — removes old DuckLake snapshots and reclaims the data/delete files they
 * (and only they) referenced. DuckLake snapshots are **catalog-wide** versions, so this procedure
 * is catalog-scoped (no schema/table args), mirroring upstream `ducklake_expire_snapshots`.
 *
 * Selection (mutually exclusive): an explicit `snapshot_ids` array, OR everything older than
 * `retention_threshold` (default 7d, floored by `ducklake.maintenance.min-retention` so recent
 * time-travel can't be nuked). The latest snapshot is NEVER expirable.
 *
 * Safety (see dev-docs/DESIGN-maintenance.md): this is a plain catalog transaction (no new
 * snapshot) that only **schedules** the now-dead files into `ducklake_files_scheduled_for_deletion`
 * — it never physically deletes (that's `cleanup_old_files`, age-gated). File liveness is the
 * half-open `[begin_snapshot, end_snapshot)` survivor test; dropped-table files are caught too, so
 * nothing leaks. `dry_run => true` lists the snapshots that would be expired and changes nothing.
 *
 * v1 reclaims the files; it does not yet GC dead *metadata* rows of fully-expired dropped
 * tables/schemas/views (harmless dangling rows — see the catalog contract). Not supported on the
 * Quack backend yet.
 */
class DucklakeExpireSnapshotsProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        config: DucklakeConfig,
) : Provider<Procedure> {
    private val minRetention: Duration = config.getMaintenanceMinRetention()

    override fun get(): Procedure =
        Procedure(
                "system",
                "expire_snapshots",
                ImmutableList.of(
                        Procedure.Argument("RETENTION_THRESHOLD", VARCHAR, false, "7d"),
                        Procedure.Argument("SNAPSHOT_IDS", ArrayType(BIGINT), false, null),
                        Procedure.Argument("DRY_RUN", BOOLEAN, false, false)),
                EXPIRE_SNAPSHOTS.bindTo(this),
                true)

    @Suppress("unused") // invoked via MethodHandle
    fun expireSnapshots(
            @Suppress("UNUSED_PARAMETER") session: ConnectorSession,
            retentionThreshold: String?,
            snapshotIds: List<Long>?,
            dryRun: Boolean,
    ) {
        // Selection is chosen by whether the caller PASSED snapshot_ids at all, not by whether the
        // resulting set is non-empty: an explicit empty array means "expire nothing", and must NOT
        // silently widen into retention-threshold expiry (which would drop every old non-latest
        // snapshot). NULL members are rejected rather than discarded.
        val expirable: List<Long> = if (snapshotIds != null) {
            val explicit: List<Long> = snapshotIds.filterNotNull()
            if (explicit.size != snapshotIds.size) {
                throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "snapshot_ids must not contain NULL")
            }
            if (explicit.isEmpty()) {
                log.info("expire_snapshots: explicit snapshot_ids is empty; nothing to expire")
                return
            }
            // Explicit ids: intersected with non-latest inside the catalog (the latest is dropped).
            catalog.listExpirableSnapshots(null, explicit.toSet())
        }
        else {
            val cutoff: Instant = Instant.now().minusMillis(parseRetention(retentionThreshold).toMillis())
            catalog.listExpirableSnapshots(cutoff, null)
        }

        if (expirable.isEmpty()) {
            log.info("expire_snapshots: no snapshots eligible for expiry")
            return
        }
        if (dryRun) {
            log.info("expire_snapshots (dry_run): %d snapshot(s) would be expired: %s", expirable.size, expirable)
            return
        }
        try {
            val result: ExpireSnapshotsResult = catalog.expireSnapshots(expirable.toSet())
            log.info("expire_snapshots: expired %d snapshot(s); scheduled %d file(s) for deletion "
                    + "(run cleanup_old_files to reclaim the space)",
                    result.expiredSnapshotCount, result.scheduledFileCount)
        }
        catch (e: TransactionConflictException) {
            throw TrinoException(TRANSACTION_CONFLICT, e.message, e)
        }
    }

    private fun parseRetention(value: String?): Duration {
        val raw = if (value.isNullOrBlank()) "7d" else value
        val retention = try {
            Duration.valueOf(raw)
        }
        catch (e: IllegalArgumentException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid retention_threshold '$raw': ${e.message}", e)
        }
        if (retention.compareTo(minRetention) < 0) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT,
                    "retention_threshold $retention is below the minimum $minRetention "
                            + "(set by ducklake.maintenance.min-retention); refusing to expire recent snapshots. "
                            + "Pass an explicit snapshot_ids array to override.")
        }
        return retention
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeExpireSnapshotsProcedure::class.java)

        private val EXPIRE_SNAPSHOTS: MethodHandle = MethodHandles.lookup().findVirtual(
                DucklakeExpireSnapshotsProcedure::class.java,
                "expireSnapshots",
                MethodType.methodType(
                        Void.TYPE,
                        ConnectorSession::class.java,
                        String::class.java,
                        List::class.java,
                        java.lang.Boolean.TYPE))
    }
}
