package dev.brikk.ducklake.doris.plugin

import java.time.Instant

/**
 * Pure orphan-file selection for `remove_orphan_files` — the data-loss-critical decision, isolated
 * here so it is exhaustively unit-testable without any storage or catalog.
 *
 * An orphan is a listed warehouse object that (a) is NOT one of the catalog's known referenced
 * paths, (b) is NOT a member of a known dataset *directory* (some DuckLake file formats register a
 * directory whose member objects must survive even though only the directory has a catalog row), and
 * (c) is older than the cutoff — the grace period that protects files an in-flight (possibly
 * cross-engine) writer has produced but not yet committed to the catalog. All three conditions must
 * hold; when in doubt a file is KEPT, never deleted.
 */
internal object OrphanFiles {

    /**
     * The subset of [listed] that is safe to delete as orphaned, given the catalog's [knownPaths]
     * (absolute URIs, in the SAME form the listing produces) and the age [cutoff]. Comparison is by
     * exact URI identity plus a directory-prefix guard, so both sides MUST be normalized identically
     * by the caller (same scheme, bucket, no trailing-slash drift) — a mismatch would misclassify a
     * live file as an orphan.
     */
    fun find(listed: List<BlobEntry>, knownPaths: Set<String>, cutoff: Instant): List<String> {
        val knownDirPrefixes = knownPaths.map { "$it/" }
        return listed
            .filter { entry ->
                entry.uri !in knownPaths &&
                    knownDirPrefixes.none { prefix -> entry.uri.startsWith(prefix) } &&
                    entry.lastModified.isBefore(cutoff)
            }
            .map { it.uri }
    }
}
