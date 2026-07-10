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
import dev.brikk.ducklake.catalog.DucklakeFilePathRef
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import io.airlift.log.Logger
import io.airlift.units.Duration
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
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
 * Implements `CALL <catalog>.system.remove_orphan_files(schema_name, table_name,
 * retention_threshold => '7d', dry_run => false)` — deletes files under a table's data path that
 * **no catalog row references** and that are **older than the retention threshold**. Orphans are
 * the residue of failed/aborted commits (a data file written, then the catalog commit lost):
 * before this, Trino had no way to reclaim them.
 *
 * Safety (see dev-docs/DESIGN-maintenance.md):
 *  - Orphans have no catalog row, so this touches storage ONLY — no snapshot, no catalog mutation,
 *    no conflict matrix. The "known set" is every path the catalog references for the table at ANY
 *    snapshot (data + delete files, including end-snapshotted ones) plus files already scheduled
 *    for deletion ([DucklakeCatalog.listReferencedFilePaths]); a listed file not in that set, and
 *    not inside a known dataset directory (lance/vortex dirs), is a candidate orphan.
 *  - **Type-scoped, not a blind diff**: only files that are recognizably DuckLake residue are
 *    deleted — a `ducklake-`-prefixed data/delete file (`.parquet`/`.puffin`/`.db`/`.vortex`) or a
 *    member of a `ducklake-*.lance` dataset directory (see [isDucklakeManagedResidue]). Foreign
 *    files a user parked under the data path (`_SUCCESS`, `foo.txt`, `.crc`, their own
 *    non-`ducklake-` parquet) are never touched. This is broader than upstream DuckDB's
 *    `ducklake_delete_orphaned_files`, which is `.parquet`-only (it abandons `.db`/`.vortex`/
 *    `.puffin`/`.lance` residue — we reclaim ours), and narrower than a raw unreferenced-file sweep.
 *  - The retention threshold is the grace period that keeps the op safe without a global lock: a
 *    file young enough to still be referenced by an in-flight (possibly cross-engine) writer is
 *    never deleted. The argument is floored by `ducklake.remove-orphan-files.min-retention`
 *    (default 7d); a call below the floor is rejected so the op can't be turned into a foot-gun.
 *  - `dry_run => true` logs what would be deleted and removes nothing.
 *
 * Modeled on upstream DuckLake's `ducklake_delete_orphaned_files` (filesystem set minus known set,
 * mtime-gated), scoped to one table's data path in the Trino procedure idiom — but type-scoped to
 * DuckLake-managed residue (see above) rather than upstream's `.parquet`-only filter.
 */
class DucklakeRemoveOrphanFilesProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val fileSystemFactory: DucklakeFileSystemFactory,
        private val pathResolver: DucklakePathResolver,
        config: DucklakeConfig,
) : Provider<Procedure> {
    private val minRetention: Duration = config.getRemoveOrphanFilesMinRetention()

    override fun get(): Procedure =
        Procedure(
                "system",
                "remove_orphan_files",
                ImmutableList.of(
                        Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        Procedure.Argument("TABLE_NAME", VARCHAR),
                        Procedure.Argument("RETENTION_THRESHOLD", VARCHAR, false, "7d"),
                        Procedure.Argument("DRY_RUN", BOOLEAN, false, false)),
                REMOVE_ORPHAN_FILES.bindTo(this),
                true)

    @Suppress("unused") // invoked via MethodHandle
    fun removeOrphanFiles(
            session: ConnectorSession,
            schemaName: String?,
            tableName: String?,
            retentionThreshold: String?,
            dryRun: Boolean,
    ) {
        val schemaArg = requireArg(schemaName, "schema_name")
        val tableArg = requireArg(tableName, "table_name")
        val retention = parseRetention(retentionThreshold)
        val isDryRun = dryRun

        val snapshotId = catalog.currentSnapshotId
        val (schema, table) = resolveTable(schemaArg, tableArg, snapshotId)
        val tableDataPath: String = pathResolver.resolveTableDataPath(schema, table)

        // The known set: every path the catalog references for this table, normalized to absolute.
        val knownPaths: Set<String> = catalog.listReferencedFilePaths(table.tableId)
                .map { resolveKnown(it, tableDataPath) }
                .toSet()

        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)
        val cutoff: Instant = Instant.now().minusMillis(retention.toMillis())
        val orphans: List<Location> = findOrphans(fileSystem, tableDataPath, knownPaths, cutoff)

        if (orphans.isEmpty()) {
            log.info("remove_orphan_files: no orphans under %s for %s.%s", tableDataPath, schemaArg, tableArg)
            return
        }
        if (isDryRun) {
            log.info("remove_orphan_files (dry_run): %d orphan file(s) under %s for %s.%s would be deleted: %s",
                    orphans.size, tableDataPath, schemaArg, tableArg, orphans)
            return
        }
        try {
            fileSystem.deleteFiles(orphans)
        }
        catch (e: IOException) {
            throw TrinoException(NOT_SUPPORTED, "Failed to delete orphan files for $schemaArg.$tableArg: ${e.message}", e)
        }
        removeEmptiedDatasetDirectories(fileSystem, tableDataPath, orphans)
        log.info("remove_orphan_files: deleted %d orphan file(s) under %s for %s.%s",
                orphans.size, tableDataPath, schemaArg, tableArg)
    }

    /**
     * After deleting orphan *files*, an orphaned lance/vortex dataset *directory* (whose members
     * were all orphans) is left as an empty directory shell on filesystems that model directories
     * (local FS; no-op on object stores, which have none). For each orphan that lived under an
     * intermediate directory beneath the table data path, remove that directory if it is now empty
     * of files — the emptiness guard means this can never remove a directory that still holds live
     * data.
     */
    private fun removeEmptiedDatasetDirectories(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            orphans: List<Location>,
    ) {
        val root: String = tableDataPath.trimEnd('/')
        val candidateDirs: Set<String> = orphans
                .mapNotNull { intermediateDirUnderRoot(root, it.toString()) }
                .toSet()
        for (dir in candidateDirs) {
            try {
                if (!fileSystem.listFiles(Location.of(dir)).hasNext()) {
                    fileSystem.deleteDirectory(Location.of(dir))
                }
            }
            catch (e: IOException) {
                log.warn(e, "remove_orphan_files: could not remove emptied directory %s", dir)
            }
        }
    }

    /**
     * The immediate directory under [root] that contains [path], or null when [path] sits directly
     * under [root] (a stray file, no intermediate directory to reclaim). E.g. for root `/t` and
     * path `/t/ds.lance/data/part.lance` returns `/t/ds.lance`.
     */
    private fun intermediateDirUnderRoot(root: String, path: String): String? {
        val prefix = "$root/"
        if (!path.startsWith(prefix)) {
            return null
        }
        val remainder = path.substring(prefix.length)
        val slash = remainder.indexOf('/')
        if (slash < 0) {
            return null
        }
        return "$root/${remainder.substring(0, slash)}"
    }

    /**
     * Lists everything under the table data path and keeps files that are (a) not a known path,
     * (b) not inside a known dataset directory (lance/vortex datasets register a directory whose
     * member files must not be mistaken for orphans), and (c) older than the cutoff.
     */
    private fun findOrphans(
            fileSystem: TrinoFileSystem,
            tableDataPath: String,
            knownPaths: Set<String>,
            cutoff: Instant,
    ): List<Location> {
        val knownDirPrefixes: List<String> = knownPaths.map { "$it/" }
        val orphans = mutableListOf<Location>()
        try {
            val files = fileSystem.listFiles(Location.of(tableDataPath))
            while (files.hasNext()) {
                val entry = files.next()
                if (isDeletableOrphan(entry, knownPaths, knownDirPrefixes, cutoff)) {
                    orphans.add(entry.location())
                }
            }
        }
        catch (e: IOException) {
            // A missing data path (table never written, or already cleaned) means no orphans.
            log.info("remove_orphan_files: could not list %s (%s) — treating as no orphans", tableDataPath, e.message)
        }
        return orphans
    }

    /**
     * An entry is a deletable orphan iff it is (a) not a known catalog path, (b) not a member of a
     * registered dataset directory (lance/vortex datasets register a dir whose files must survive),
     * (c) recognizably DuckLake-managed residue (see [isDucklakeManagedResidue]), and (d) older
     * than the cutoff (the grace period protecting in-flight, possibly cross-engine, writers).
     *
     * The (c) gate is what keeps this from behaving like a blind `rm` of everything unreferenced
     * under the data path: a user may legitimately park foreign files there (`_SUCCESS`, `foo.txt`,
     * `.crc` sidecars, their own `data.parquet`), and those must never be deleted.
     */
    private fun isDeletableOrphan(
            entry: io.trino.filesystem.FileEntry,
            knownPaths: Set<String>,
            knownDirPrefixes: List<String>,
            cutoff: Instant,
    ): Boolean {
        val path = entry.location().toString()
        return path !in knownPaths &&
                knownDirPrefixes.none { prefix -> path.startsWith(prefix) } &&
                isDucklakeManagedResidue(path) &&
                entry.lastModified().isBefore(cutoff)
    }

    /**
     * Whether [path] is a file this connector (or any DuckLake engine) could have written as data
     * or delete-file residue — the only files `remove_orphan_files` may delete. Two shapes:
     *
     *  - A **single data/delete file**: basename starts with the `ducklake-` prefix that every
     *    DuckLake writer uses (`ducklake-<uuid>.parquet|.db|.vortex`, and delete files
     *    `ducklake-delete-<uuid>.<ext>` (this connector) / `ducklake-<uuid>-delete.<ext>` (DuckDB))
     *    AND ends with a managed extension (`.parquet`/`.puffin`/`.db`/`.vortex`).
     *  - A **Lance dataset member**: Lance is a *directory* (`ducklake-<uuid>.lance/`) whose internal
     *    files (the `data` and `_versions` subdir entries) do NOT carry the prefix — recognized by
     *    the enclosing `ducklake-<uuid>.lance` dataset directory anywhere in the path.
     *
     * Anything else (foreign files, a user's own non-`ducklake-` parquet, `_SUCCESS`, …) returns
     * false and is left untouched. This is intentionally narrower than a raw filesystem diff and
     * broader than upstream DuckDB's parquet-only sweep (we also reclaim our `.db`/`.vortex`/
     * `.puffin`/`.lance` residue, which DuckDB abandons).
     */
    private fun isDucklakeManagedResidue(path: String): Boolean {
        val basename = path.substringAfterLast('/')
        if (basename.startsWith(DUCKLAKE_FILE_PREFIX) &&
                MANAGED_FILE_EXTENSIONS.any { basename.endsWith(it, ignoreCase = true) }) {
            return true
        }
        return path.split('/').any { segment ->
            segment.startsWith(DUCKLAKE_FILE_PREFIX) && segment.endsWith(LANCE_DATASET_SUFFIX, ignoreCase = true)
        }
    }

    private fun resolveKnown(ref: DucklakeFilePathRef, tableDataPath: String): String =
            Location.of(pathResolver.resolveFilePath(ref.path, ref.pathIsRelative, tableDataPath)).toString()

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
                    "retention_threshold $retention is below the minimum ${minRetention} "
                            + "(set by ducklake.remove-orphan-files.min-retention); refusing to delete recently-written files")
        }
        return retention
    }

    private fun requireArg(value: String?, name: String): String {
        if (value.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "$name is required")
        }
        return value
    }

    private fun resolveTable(schemaName: String, tableName: String, snapshotId: Long): Pair<DucklakeSchema, DucklakeTable> {
        val schema: DucklakeSchema = catalog.getSchema(schemaName, snapshotId)
            ?: throw TrinoException(NOT_SUPPORTED, "Schema not found: $schemaName")
        val table: DucklakeTable = catalog.getTable(schemaName, tableName, snapshotId)
            ?: throw TrinoException(NOT_SUPPORTED, "Table not found: $schemaName.$tableName")
        return schema to table
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeRemoveOrphanFilesProcedure::class.java)

        /** Filename prefix every DuckLake data/delete-file writer uses (this connector + DuckDB). */
        private const val DUCKLAKE_FILE_PREFIX: String = "ducklake-"
        private const val LANCE_DATASET_SUFFIX: String = ".lance"

        /** Single-file data/delete formats this connector or DuckDB can write. Lance is a dir (handled separately). */
        private val MANAGED_FILE_EXTENSIONS: List<String> = listOf(".parquet", ".puffin", ".db", ".vortex")

        private val REMOVE_ORPHAN_FILES: MethodHandle = MethodHandles.lookup().findVirtual(
                DucklakeRemoveOrphanFilesProcedure::class.java,
                "removeOrphanFiles",
                MethodType.methodType(
                        Void.TYPE,
                        ConnectorSession::class.java,
                        String::class.java,
                        String::class.java,
                        String::class.java,
                        java.lang.Boolean.TYPE))
    }
}
