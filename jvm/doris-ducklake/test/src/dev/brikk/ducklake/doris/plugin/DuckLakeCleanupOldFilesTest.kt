package dev.brikk.ducklake.doris.plugin

import java.lang.reflect.Proxy
import java.time.Instant

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeScheduledFile

import org.apache.doris.connector.api.DorisConnectorException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Headless coverage of `cleanup_old_files` orchestration in [DuckLakeProcedureOps] — the physical
 * deletion phase that pairs with `expire_snapshots`'s scheduling phase. A recording [FakeCatalog]
 * (throwing-proxy for unused methods) and a [FakeBlobStore] stand in for Postgres + S3, so the
 * whole flow (retention floor, path resolution, delete, schedule-row removal, dry-run, partial
 * failure) is verifiable without infra. The real S3 delete is covered by the compose smoke; the
 * S3-URI parser is unit-tested directly below.
 */
internal class DuckLakeCleanupOldFilesTest {

    private fun scheduled(id: Long, path: String, relative: Boolean) =
        DucklakeScheduledFile(dataFileId = id, path = path, pathIsRelative = relative)

    private fun ops(
        catalog: FakeCatalog,
        blobStore: FakeBlobStore,
        properties: Map<String, String> = mapOf("storage.warehouse" to "s3://ducklake/data"),
    ) = DuckLakeProcedureOps(catalog, properties) { blobStore }

    @Test
    fun advertisesCleanupOldFiles() {
        val ops = DuckLakeProcedureOps(FakeCatalog(), emptyMap()) { FakeBlobStore() }
        assertThat(ops.getSupportedProcedures()).contains("cleanup_old_files")
    }

    @Test
    fun deletesScheduledBlobsAndRemovesTheirRows() {
        val cat = FakeCatalog(
            scheduled = listOf(
                scheduled(1L, "tpch/orders/f1.parquet", relative = true),
                scheduled(2L, "s3://ducklake/data/tpch/orders/f2.parquet", relative = false),
            ),
        )
        val store = FakeBlobStore() // deletes everything asked by default
        val res = ops(cat, store).execute(null, null, "cleanup_old_files", mapOf("retention_threshold" to "30d"), null, emptyList())

        // Relative path resolved against the warehouse root; absolute passed through.
        assertThat(store.deleteRequested).containsExactlyInAnyOrder(
            "s3://ducklake/data/tpch/orders/f1.parquet",
            "s3://ducklake/data/tpch/orders/f2.parquet",
        )
        // Both deleted → both rows removed.
        assertThat(cat.removedIds).containsExactlyInAnyOrder(1L, 2L)
        assertThat(cat.listCutoff).isNotNull()

        val row = res.rows.single()
        assertThat(res.resultSchema.map { it.name })
            .containsExactly("dry_run", "retention_threshold", "deleted_file_count", "failed_file_count")
        assertThat(row).containsExactly("false", "30d", "2", "0")
    }

    @Test
    fun dryRunListsButDeletesNothing() {
        val cat = FakeCatalog(scheduled = listOf(scheduled(1L, "a/f.parquet", true)))
        val store = FakeBlobStore()
        val res = ops(cat, store).execute(
            null, null, "cleanup_old_files",
            mapOf("retention_threshold" to "30d", "dry_run" to "true"), null, emptyList(),
        )
        assertThat(store.deleteRequested).isEmpty() // NEVER touched storage
        assertThat(cat.removedIds).isNull() // NEVER removed rows
        assertThat(res.rows.single()).containsExactly("true", "30d", "1", "0")
    }

    @Test
    fun onlyRemovesRowsForBlobsActuallyDeleted() {
        val cat = FakeCatalog(
            scheduled = listOf(
                scheduled(1L, "a/f1.parquet", true),
                scheduled(2L, "a/f2.parquet", true),
            ),
        )
        // f2 fails to delete → its row must survive for a retry.
        val store = FakeBlobStore(failUris = setOf("s3://ducklake/data/a/f2.parquet"))
        val res = ops(cat, store).execute(null, null, "cleanup_old_files", mapOf("retention_threshold" to "30d"), null, emptyList())

        assertThat(cat.removedIds).containsExactly(1L) // only the deleted one
        assertThat(res.rows.single()).containsExactly("false", "30d", "1", "1") // 1 deleted, 1 failed
    }

    @Test
    fun emptyScheduleDeletesNothingAndTouchesNoStorage() {
        val cat = FakeCatalog(scheduled = emptyList())
        val store = FakeBlobStore()
        val res = ops(cat, store).execute(null, null, "cleanup_old_files", mapOf("retention_threshold" to "30d"), null, emptyList())
        assertThat(store.deleteRequested).isEmpty()
        assertThat(cat.removedIds).isNull()
        assertThat(res.rows.single()).containsExactly("false", "30d", "0", "0")
    }

    @Test
    fun retentionBelowFloorIsRejectedBeforeTouchingCatalog() {
        val cat = FakeCatalog(scheduled = listOf(scheduled(1L, "a/f.parquet", true)))
        val store = FakeBlobStore()
        assertThatThrownBy {
            ops(cat, store).execute(null, null, "cleanup_old_files", mapOf("retention_threshold" to "1h"), null, emptyList())
        }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("below the minimum")
        assertThat(cat.listCutoff).isNull()
        assertThat(store.deleteRequested).isEmpty()
    }

    @Test
    fun relativePathWithNoWarehouseRootFailsLoud() {
        // No storage.warehouse prop and catalog.getDataPath() == null → can't resolve a relative path.
        val cat = FakeCatalog(scheduled = listOf(scheduled(1L, "a/f.parquet", true)), dataPath = null)
        val store = FakeBlobStore()
        assertThatThrownBy {
            ops(cat, store, properties = emptyMap()).execute(
                null, null, "cleanup_old_files", mapOf("retention_threshold" to "30d"), null, emptyList(),
            )
        }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("no warehouse root")
    }

    @Test
    fun rejectsWhereAndPartitionClauses() {
        val ops = ops(FakeCatalog(), FakeBlobStore())
        assertThatThrownBy {
            ops.execute(null, null, "cleanup_old_files", emptyMap(), null, listOf("p1"))
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("PARTITION")
    }

    // ---- S3 URI parser (pure) ----

    @Test
    fun parsesS3Uris() {
        assertThat(S3WarehouseBlobStore.parseS3Uri("s3://bucket/a/b/c.parquet")).isEqualTo("bucket" to "a/b/c.parquet")
        assertThat(S3WarehouseBlobStore.parseS3Uri("s3a://b/k")).isEqualTo("b" to "k")
        assertThat(S3WarehouseBlobStore.parseS3Uri("S3://B/Key")).isEqualTo("B" to "Key") // scheme case-insensitive
    }

    @Test
    fun rejectsBadS3Uris() {
        for (bad in listOf("hdfs://b/k", "file:///x", "s3://bucketonly", "s3:///nokey", "s3://b/", "notauri")) {
            assertThatThrownBy { S3WarehouseBlobStore.parseS3Uri(bad) }
                .describedAs("uri %s", bad)
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    /** Recording blob store: deletes everything except [failUris]; records what it was asked to delete. */
    private class FakeBlobStore(private val failUris: Set<String> = emptySet()) : WarehouseBlobStore {
        val deleteRequested = mutableListOf<String>()
        override fun delete(uris: Collection<String>): BlobDeleteResult {
            deleteRequested.addAll(uris)
            val deleted = uris.filter { it !in failUris }.toSet()
            val failed = uris.filter { it in failUris }.associateWith { "simulated failure" }
            return BlobDeleteResult(deleted, failed)
        }

        override fun list(prefixUri: String): List<BlobEntry> = emptyList() // cleanup never lists
    }

    /** Recording catalog stub; throwing-proxy for everything the procedure doesn't call. */
    private class FakeCatalog(
        private val scheduled: List<DucklakeScheduledFile> = emptyList(),
        private val dataPath: String? = null,
    ) : DucklakeCatalog by throwingDelegate() {

        var listCutoff: Instant? = null
        var removedIds: Collection<Long>? = null

        override fun getDataPath(): String? = dataPath

        override fun listFilesScheduledForDeletion(olderThan: Instant): List<DucklakeScheduledFile> {
            listCutoff = olderThan
            return scheduled
        }

        override fun removeScheduledFileRows(dataFileIds: Collection<Long>) {
            removedIds = dataFileIds
        }

        companion object {
            private fun throwingDelegate(): DucklakeCatalog =
                Proxy.newProxyInstance(
                    DucklakeCatalog::class.java.classLoader,
                    arrayOf(DucklakeCatalog::class.java),
                ) { _, method, _ ->
                    throw NotImplementedError("FakeCatalog does not implement ${method.name}")
                } as DucklakeCatalog
        }
    }
}
