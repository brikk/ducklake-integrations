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
import dev.brikk.ducklake.catalog.DucklakeScheduledFile
import io.airlift.log.Logger
import io.airlift.units.Duration
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.procedure.Procedure
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.VarcharType.VARCHAR
import java.io.IOException
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.time.Instant

/**
 * Implements `CALL <catalog>.system.cleanup_old_files(retention_threshold => '7d', dry_run =>
 * false)` — the second half of the two-phase deletion model (see dev-docs/DESIGN-maintenance.md):
 * physically deletes files that `expire_snapshots` (or a cross-engine DuckLake op) previously
 * **scheduled** into `ducklake_files_scheduled_for_deletion`, once they are older than
 * `retention_threshold` (the grace period that protects in-flight, possibly cross-engine, readers).
 * Catalog-scoped (drains the whole schedule table); mirrors upstream `ducklake_cleanup_old_files`.
 *
 * `retention_threshold` defaults to 7d and is floored by `ducklake.maintenance.min-retention`.
 * `dry_run => true` logs what would be deleted and removes nothing. Per file: delete on storage,
 * then remove the schedule row (a file that fails to delete keeps its row for a later retry).
 *
 * Scheduled paths written by this connector are absolute; relative ones (written by DuckLake) are
 * resolved against the catalog `data_path` ROOT (not a per-table dir — see [DucklakeScheduledFile]).
 */
class DucklakeCleanupOldFilesProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val fileSystemFactory: DucklakeFileSystemFactory,
        config: DucklakeConfig,
) : Provider<Procedure> {
    private val minRetention: Duration = config.getMaintenanceMinRetention()
    private val configuredDataPath: String? = config.getDataPath()

    override fun get(): Procedure =
        Procedure(
                "system",
                "cleanup_old_files",
                ImmutableList.of(
                        Procedure.Argument("RETENTION_THRESHOLD", VARCHAR, false, "7d"),
                        Procedure.Argument("DRY_RUN", BOOLEAN, false, false)),
                CLEANUP_OLD_FILES.bindTo(this),
                true)

    @Suppress("unused") // invoked via MethodHandle
    fun cleanupOldFiles(
            session: ConnectorSession,
            retentionThreshold: String?,
            dryRun: Boolean,
    ) {
        val retention = parseRetention(retentionThreshold)
        val cutoff: Instant = Instant.now().minusMillis(retention.toMillis())
        val scheduled: List<DucklakeScheduledFile> = catalog.listFilesScheduledForDeletion(cutoff)
        if (scheduled.isEmpty()) {
            log.info("cleanup_old_files: nothing scheduled past the %s grace period", retention)
            return
        }
        if (dryRun) {
            log.info("cleanup_old_files (dry_run): %d scheduled file(s) would be deleted", scheduled.size)
            return
        }

        val root: String? = catalog.getDataPath() ?: configuredDataPath
        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
        val deletedIds = mutableListOf<Long>()
        for (file in scheduled) {
            val absolute: String? = resolveAbsolute(file, root)
            if (absolute == null) {
                log.warn("cleanup_old_files: cannot resolve relative scheduled path '%s' (no data_path root) — skipping", file.path)
                continue
            }
            try {
                deletePath(fileSystem, Location.of(absolute))
                deletedIds.add(file.dataFileId)
            }
            catch (e: IOException) {
                // Keep the schedule row so a later run retries; a missing file is fine to forget.
                log.warn(e, "cleanup_old_files: failed to delete %s — leaving it scheduled", absolute)
            }
        }
        catalog.removeScheduledFileRows(deletedIds)
        log.info("cleanup_old_files: deleted %d scheduled file(s)", deletedIds.size)
    }

    /**
     * Physically removes a scheduled path. LANCE data files are dataset *directories*, not single
     * files — `deleteFile` can't remove them (it errors on local FS and no-ops on object stores,
     * leaking the member objects), so a directory-shaped path is removed recursively with
     * `deleteDirectory`. `directoryExists` resolves the distinction on both local FS and s3
     * (an s3 "directory" is a key prefix with member objects). Parquet/duckdb/vortex are single
     * files and take the plain `deleteFile` path.
     */
    @Throws(IOException::class)
    private fun deletePath(fileSystem: TrinoFileSystem, location: Location) {
        if (fileSystem.directoryExists(location).orElse(false)) {
            fileSystem.deleteDirectory(location)
        }
        else {
            fileSystem.deleteFile(location)
        }
    }

    private fun resolveAbsolute(file: DucklakeScheduledFile, root: String?): String? {
        if (!file.pathIsRelative) {
            return file.path
        }
        if (root.isNullOrBlank()) {
            return null
        }
        return if (root.endsWith("/")) "$root${file.path}" else "$root/${file.path}"
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
                            + "(set by ducklake.maintenance.min-retention); refusing to delete recently-scheduled files")
        }
        return retention
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeCleanupOldFilesProcedure::class.java)

        private val CLEANUP_OLD_FILES: MethodHandle = MethodHandles.lookup().findVirtual(
                DucklakeCleanupOldFilesProcedure::class.java,
                "cleanupOldFiles",
                MethodType.methodType(
                        Void.TYPE,
                        ConnectorSession::class.java,
                        String::class.java,
                        java.lang.Boolean.TYPE))
    }
}
