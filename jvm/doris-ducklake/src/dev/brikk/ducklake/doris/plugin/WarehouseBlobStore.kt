package dev.brikk.ducklake.doris.plugin

import java.time.Instant

/**
 * Minimal object-store abstraction for FE-side warehouse blob operations: physical deletion of
 * files that `expire_snapshots` scheduled (`cleanup_old_files`) and recursive listing for orphan
 * detection (`remove_orphan_files`).
 *
 * The connector talks to the warehouse itself — the sanctioned path, since an out-of-tree Doris
 * connector has no usable Doris `FileSystem` handle and must NOT assume local-drive access across
 * nodes (the warehouse is blob storage). [S3WarehouseBlobStore] is the production implementation
 * (S3/MinIO/OSS/COS via the vended `s3.*` credentials); tests inject a fake so the procedure
 * orchestration is verifiable headlessly.
 */
internal interface WarehouseBlobStore {

    /**
     * Best-effort delete of the given **absolute** storage URIs. Returns which succeeded and which
     * failed (a delete of a missing object counts as success — the goal state is "gone"). Never
     * throws for a per-object failure: a file we can't delete keeps its schedule row for a later
     * retry, so partial progress is safe.
     */
    fun delete(uris: Collection<String>): BlobDeleteResult

    /**
     * Recursively list every object under [prefixUri] (an absolute directory URI). Returns each
     * object's absolute URI and last-modified time (the latter gates orphan deletion by age). The
     * caller is responsible for directory semantics; implementations must not return objects outside
     * [prefixUri]'s directory (i.e. a prefix of `.../t` must NOT also match `.../t2/...`).
     */
    fun list(prefixUri: String): List<BlobEntry>
}

/** Outcome of [WarehouseBlobStore.delete]: the URIs removed, and the ones that failed with why. */
internal data class BlobDeleteResult(
    val deleted: Set<String>,
    val failed: Map<String, String>,
)

/** One object from [WarehouseBlobStore.list]: its absolute URI and last-modified instant. */
internal data class BlobEntry(
    val uri: String,
    val lastModified: Instant,
)
