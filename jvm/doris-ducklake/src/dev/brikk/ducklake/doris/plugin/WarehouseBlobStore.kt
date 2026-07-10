package dev.brikk.ducklake.doris.plugin

/**
 * Minimal object-store abstraction for FE-side warehouse blob operations (today: physical deletion
 * of files that `expire_snapshots` scheduled, for `cleanup_old_files`).
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
}

/** Outcome of [WarehouseBlobStore.delete]: the URIs removed, and the ones that failed with why. */
internal data class BlobDeleteResult(
    val deleted: Set<String>,
    val failed: Map<String, String>,
)
