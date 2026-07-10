package dev.brikk.ducklake.doris.plugin

import io.minio.MinioClient
import io.minio.RemoveObjectArgs

import org.apache.doris.connector.api.DorisConnectorException

/**
 * [WarehouseBlobStore] backed by the MinIO Java client, which speaks the S3 API to MinIO, AWS S3,
 * Alibaba OSS, Tencent COS, and other S3-compatible stores. Built from the catalog's `s3.*`
 * properties — the same credentials the BE parquet reader already uses to reach the warehouse — so
 * the connector deletes blobs itself from the FE without assuming any local-filesystem access.
 *
 * Only S3-compatible schemes are supported for now (`s3://`, `s3a://`, `s3n://`); a non-S3 URI is
 * reported as a per-object failure rather than throwing, so one stray path can't abort a cleanup.
 * HDFS/Azure warehouses would need their own [WarehouseBlobStore] impls (future work).
 *
 * **Not headlessly unit-tested** (needs a live MinIO); the S3-URI parsing is a pure function tested
 * directly, and the delete flow is covered by the compose smoke. The procedure orchestration that
 * drives this is headless-tested via a fake [WarehouseBlobStore].
 */
internal class S3WarehouseBlobStore(private val client: MinioClient) : WarehouseBlobStore {

    @Suppress("TooGenericExceptionCaught")
    override fun delete(uris: Collection<String>): BlobDeleteResult {
        val deleted = LinkedHashSet<String>()
        val failed = LinkedHashMap<String, String>()
        for (uri in uris) {
            try {
                val (bucket, key) = parseS3Uri(uri)
                client.removeObject(RemoveObjectArgs.builder().bucket(bucket).`object`(key).build())
                deleted.add(uri)
            } catch (e: IllegalArgumentException) {
                failed[uri] = e.message ?: "invalid S3 URI"
            } catch (e: Exception) {
                // Deliberately broad: MinIO's removeObject throws ~9 unrelated checked exceptions
                // (IOException/XmlParserException/ServerException/InsufficientDataException/…).
                // Any is a per-object failure so cleanup makes partial progress and the file keeps
                // its schedule row for a later retry. (@Suppress TooGenericExceptionCaught.)
                failed[uri] = e.message ?: e.javaClass.simpleName
            }
        }
        return BlobDeleteResult(deleted, failed)
    }

    internal companion object {
        private val S3_SCHEMES = setOf("s3", "s3a", "s3n")

        /**
         * Build an S3 blob store from the catalog `s3.*` properties (endpoint/region/keys). Fails
         * loud if endpoint or credentials are missing — cleanup can't run without them, and a silent
         * no-op would leave scheduled files leaking. Mirrors the keys the write path already emits
         * ([DuckLakeScanPlanProvider.isStorageProperty]).
         */
        fun fromProperties(properties: Map<String, String>): S3WarehouseBlobStore {
            val builder = MinioClient.builder()
                .endpoint(requireProp(properties, "s3.endpoint"))
                .credentials(requireProp(properties, "s3.access_key"), requireProp(properties, "s3.secret_key"))
            properties["s3.region"]?.takeIf { it.isNotBlank() }?.let { builder.region(it) }
            return S3WarehouseBlobStore(builder.build())
        }

        private fun requireProp(properties: Map<String, String>, key: String): String =
            properties[key]?.takeIf { it.isNotBlank() }
                ?: throw DorisConnectorException("cleanup_old_files needs '$key' set on the catalog")

        /**
         * Split an absolute `s3://bucket/key...` URI into `(bucket, key)`. Accepts `s3`/`s3a`/`s3n`
         * schemes. Throws [IllegalArgumentException] for a non-S3 scheme, a missing bucket, or an
         * empty key — caller turns that into a per-object failure.
         */
        internal fun parseS3Uri(uri: String): Pair<String, String> {
            val schemeSep = uri.indexOf("://")
            require(schemeSep > 0) { "not an absolute URI: $uri" }
            val scheme = uri.substring(0, schemeSep).lowercase()
            require(scheme in S3_SCHEMES) { "unsupported scheme '$scheme' (S3 only): $uri" }
            val rest = uri.substring(schemeSep + 3)
            val slash = rest.indexOf('/')
            require(slash > 0) { "missing bucket or key: $uri" }
            val bucket = rest.substring(0, slash)
            val key = rest.substring(slash + 1)
            require(bucket.isNotEmpty()) { "empty bucket: $uri" }
            require(key.isNotEmpty()) { "empty object key: $uri" }
            return bucket to key
        }
    }
}
