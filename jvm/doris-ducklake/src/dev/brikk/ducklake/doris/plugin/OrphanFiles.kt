package dev.brikk.ducklake.doris.plugin

import java.time.Instant

/**
 * Pure orphan-file selection for `remove_orphan_files` — the data-loss-critical decision, isolated
 * here so it is exhaustively unit-testable without any storage or catalog.
 *
 * An orphan is a listed warehouse object that (a) is **recognizably DuckLake-written residue**
 * ([isDucklakeManagedResidue]), (b) is NOT one of the catalog's known referenced paths, (c) is NOT a
 * member of a known dataset *directory* (a DuckLake dataset registers a directory whose member
 * objects must survive even though only the directory has a catalog row), and (d) is older than the
 * cutoff — the grace period that protects files an in-flight (possibly cross-engine) writer produced
 * but hasn't yet committed. ALL conditions must hold; when in doubt a file is KEPT, never deleted.
 *
 * ## The residue gate (a) — cross-engine contract with the Trino plugin
 * Modeled on the shared `remove_orphan_files` contract (Trino `DucklakeRemoveOrphanFilesProcedure`,
 * dev-docs DESIGN-maintenance §8.1): a file is deletable only if its **basename starts with the
 * `ducklake-` prefix that every DuckLake writer uses** (this connector + DuckDB) AND ends with a
 * managed extension. This is broader than upstream DuckDB's `.parquet`-only glob (we also reclaim
 * `.puffin`/`.db` residue) but NARROWER than a raw unreferenced-file diff — foreign files a user
 * parked under the warehouse (`_SUCCESS`, `foo.txt`, `.crc`, their own non-`ducklake-` `data.parquet`)
 * are NEVER deleted. The `ducklake-` prefix is the key safety gate.
 *
 * Doris manages `{.parquet, .puffin, .db}`: `.parquet` (data), `.puffin` (deletion vectors), `.db`
 * (DuckDB-format data) — the cross-engine core. It deliberately OMITS the Trino plugin's `.vortex`
 * and the `.lance` dataset-directory shape (Trino-only formats not present in a Doris deployment).
 */
internal object OrphanFiles {

    /** Filename prefix every DuckLake data/delete-file writer uses (this connector, Trino, DuckDB). */
    private const val DUCKLAKE_FILE_PREFIX = "ducklake-"

    /**
     * Single-file DuckLake data/delete formats Doris recognizes as reclaimable residue. Cross-engine
     * core only — no `.vortex`/`.lance` (Trino-plugin formats). `.parquet` (data + parquet deletes),
     * `.puffin` (deletion-vector deletes), `.db` (DuckDB-format data).
     */
    private val MANAGED_FILE_EXTENSIONS = listOf(".parquet", ".puffin", ".db")

    /**
     * The subset of [listed] safe to delete as orphaned, given the catalog's [knownPaths] (absolute
     * URIs, in the SAME form the listing produces) and the age [cutoff]. Comparison against the known
     * set is by exact URI identity plus a directory-prefix guard, so both sides MUST be normalized
     * identically by the caller (same scheme, bucket, no trailing-slash drift) — a mismatch would
     * misclassify a live file as an orphan.
     */
    fun find(listed: List<BlobEntry>, knownPaths: Set<String>, cutoff: Instant): List<String> {
        val knownDirPrefixes = knownPaths.map { "$it/" }
        return listed
            .filter { entry ->
                isDucklakeManagedResidue(entry.uri) &&
                    entry.uri !in knownPaths &&
                    knownDirPrefixes.none { prefix -> entry.uri.startsWith(prefix) } &&
                    entry.lastModified.isBefore(cutoff)
            }
            .map { it.uri }
    }

    /**
     * Whether [uri] is a file this connector (or any DuckLake engine) could have written as data or
     * delete-file residue — the only files `remove_orphan_files` may delete. The basename must start
     * with the `ducklake-` prefix AND end with a [managed extension][MANAGED_FILE_EXTENSIONS]. This
     * covers both delete-file namings (`ducklake-delete-<uuid>.<ext>` here, `ducklake-<uuid>-delete.<ext>`
     * DuckDB) since both start with the prefix. Anything else returns false and is left untouched.
     */
    fun isDucklakeManagedResidue(uri: String): Boolean {
        val basename = uri.substringAfterLast('/')
        return basename.startsWith(DUCKLAKE_FILE_PREFIX) &&
            MANAGED_FILE_EXTENSIONS.any { basename.endsWith(it, ignoreCase = true) }
    }
}
