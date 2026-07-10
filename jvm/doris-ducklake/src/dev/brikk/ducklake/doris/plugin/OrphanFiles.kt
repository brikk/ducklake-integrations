package dev.brikk.ducklake.doris.plugin

import java.time.Instant

/**
 * Pure orphan-file selection for `remove_orphan_files` — the data-loss-critical decision, isolated
 * here so it is exhaustively unit-testable without any storage or catalog.
 *
 * An orphan is a listed warehouse object that (a) has an extension for a DuckLake data-file format
 * we manage (b) is NOT one of the catalog's known referenced paths, (c) is NOT a member of a known
 * dataset *directory* (some DuckLake file formats register a directory whose member objects must
 * survive even though only the directory has a catalog row), and (d) is older than the cutoff — the
 * grace period that protects files an in-flight (possibly cross-engine) writer has produced but not
 * yet committed. ALL conditions must hold; when in doubt a file is KEPT, never deleted.
 *
 * The extension gate (a) mirrors upstream DuckLake's `ducklake_delete_orphaned_files`, whose glob is
 * `read_blob({DATA_PATH} || '**') WHERE suffix(filename, '.parquet')` — it only ever considers
 * `.parquet`, so foreign files (`_SUCCESS` markers, `.crc` checksums, other engines' sidecars, logs)
 * are NEVER deleted. Doris reads parquet only, so `.parquet` is the exact set (unlike our Trino
 * plugin, which additionally manages `.lance`/`.vortex` and would gate those too).
 */
internal object OrphanFiles {

    /**
     * The subset of [listed] that is safe to delete as orphaned, given the catalog's [knownPaths]
     * (absolute URIs, in the SAME form the listing produces), the age [cutoff], and the
     * [candidateSuffixes] of file formats we manage (e.g. `.parquet`). Only objects ending in one of
     * [candidateSuffixes] are ever eligible — everything else on storage is left untouched.
     * Comparison against the known set is by exact URI identity plus a directory-prefix guard, so
     * both sides MUST be normalized identically by the caller (same scheme, bucket, no trailing-slash
     * drift) — a mismatch would misclassify a live file as an orphan.
     */
    fun find(
        listed: List<BlobEntry>,
        knownPaths: Set<String>,
        cutoff: Instant,
        candidateSuffixes: Set<String>,
    ): List<String> {
        val knownDirPrefixes = knownPaths.map { "$it/" }
        return listed
            .filter { entry ->
                candidateSuffixes.any { suffix -> entry.uri.endsWith(suffix) } &&
                    entry.uri !in knownPaths &&
                    knownDirPrefixes.none { prefix -> entry.uri.startsWith(prefix) } &&
                    entry.lastModified.isBefore(cutoff)
            }
            .map { it.uri }
    }
}
