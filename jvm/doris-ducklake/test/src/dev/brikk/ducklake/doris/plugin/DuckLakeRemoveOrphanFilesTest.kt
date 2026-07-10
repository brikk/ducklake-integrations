package dev.brikk.ducklake.doris.plugin

import java.lang.reflect.Proxy
import java.time.Instant
import java.util.UUID

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeFilePathRef
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable

import org.apache.doris.connector.api.DorisConnectorException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Coverage of `remove_orphan_files`. The data-loss-critical selection ([OrphanFiles.find]) is a pure
 * function, tested exhaustively here (known-set exclusion, dir-prefix guard, age grace period, exact
 * URI matching). The table-scoped orchestration in [DuckLakeProcedureOps] is tested via a fake
 * [WarehouseBlobStore] + fake catalog. The real S3 list/delete is covered by the compose smoke.
 */
internal class DuckLakeRemoveOrphanFilesTest {

    private val now: Instant = Instant.parse("2026-07-10T00:00:00Z")
    private val old: Instant = now.minusSeconds(30L * 24 * 3600) // 30d ago — past any grace period
    private val recent: Instant = now.minusSeconds(60) // 1 min ago — inside any grace period

    // ---- OrphanFiles.find (pure) ----

    @Test
    fun findsUnknownOldFilesOnly() {
        val listed = listOf(
            BlobEntry("s3://b/t/known.parquet", old),
            BlobEntry("s3://b/t/orphan.parquet", old),
            BlobEntry("s3://b/t/too-young.parquet", recent),
        )
        val known = setOf("s3://b/t/known.parquet")
        val cutoff = now.minusSeconds(7L * 24 * 3600) // 7d grace

        assertThat(OrphanFiles.find(listed, known, cutoff))
            .containsExactly("s3://b/t/orphan.parquet") // known kept, too-young kept
    }

    @Test
    fun keepsMembersOfKnownDatasetDirectories() {
        // A known path that is a directory (lance/vortex dataset): its member files must survive even
        // though only the directory has a catalog row.
        val listed = listOf(
            BlobEntry("s3://b/t/ds.lance/part-0.lance", old),
            BlobEntry("s3://b/t/ds.lance/part-1.lance", old),
            BlobEntry("s3://b/t/stray.parquet", old),
        )
        val known = setOf("s3://b/t/ds.lance")
        val cutoff = now

        assertThat(OrphanFiles.find(listed, known, cutoff))
            .containsExactly("s3://b/t/stray.parquet")
    }

    @Test
    fun keepsEverythingWhenAllYoungerThanCutoff() {
        val listed = listOf(BlobEntry("s3://b/t/a.parquet", recent), BlobEntry("s3://b/t/b.parquet", recent))
        assertThat(OrphanFiles.find(listed, emptySet(), now.minusSeconds(7L * 24 * 3600))).isEmpty()
    }

    @Test
    fun matchesKnownPathsExactly() {
        // A near-miss (different key) must NOT shield the listed file — exact identity only.
        val listed = listOf(BlobEntry("s3://b/t/data.parquet", old))
        val known = setOf("s3://b/t/data.parquet.bak", "s3://b/t/DATA.parquet")
        assertThat(OrphanFiles.find(listed, known, now)).containsExactly("s3://b/t/data.parquet")
    }

    // ---- orchestration ----

    @Test
    fun requiresATargetTableHandle() {
        val ops = DuckLakeProcedureOps(FakeCatalog(), warehouseProps()) { FakeBlobStore() }
        assertThatThrownBy {
            ops.execute(null, null, "remove_orphan_files", mapOf("retention_threshold" to "30d"), null, emptyList())
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("requires a target table")
    }

    @Test
    fun deletesOrphansUnderTableDataPathAndKeepsReferencedFiles() {
        val cat = FakeCatalog(
            referenced = listOf(DucklakeFilePathRef("live.parquet", true)), // relative → under table path
        )
        val store = FakeBlobStore(
            listing = listOf(
                BlobEntry("s3://ducklake/data/sales/orders/live.parquet", old), // referenced → keep
                BlobEntry("s3://ducklake/data/sales/orders/orphan.parquet", old), // orphan → delete
                BlobEntry("s3://ducklake/data/sales/orders/fresh.parquet", recent), // young → keep
            ),
        )
        val res = ops(cat, store).execute(
            null, handle(), "remove_orphan_files", mapOf("retention_threshold" to "7d"), null, emptyList(),
        )

        assertThat(store.listedPrefix).isEqualTo("s3://ducklake/data/sales/orders")
        assertThat(store.deleteRequested).containsExactly("s3://ducklake/data/sales/orders/orphan.parquet")
        assertThat(res.resultSchema.map { it.name })
            .containsExactly("dry_run", "retention_threshold", "deleted_file_count", "failed_file_count")
        assertThat(res.rows.single()).containsExactly("false", "7d", "1", "0")
    }

    @Test
    fun dryRunListsButDeletesNothing() {
        val cat = FakeCatalog(referenced = emptyList())
        val store = FakeBlobStore(listing = listOf(BlobEntry("s3://ducklake/data/sales/orders/o.parquet", old)))
        val res = ops(cat, store).execute(
            null, handle(), "remove_orphan_files",
            mapOf("retention_threshold" to "7d", "dry_run" to "true"), null, emptyList(),
        )
        assertThat(store.deleteRequested).isEmpty()
        assertThat(res.rows.single()).containsExactly("true", "7d", "1", "0")
    }

    @Test
    fun retentionBelowFloorIsRejectedBeforeListing() {
        val cat = FakeCatalog()
        val store = FakeBlobStore()
        assertThatThrownBy {
            ops(cat, store).execute(null, handle(), "remove_orphan_files", mapOf("retention_threshold" to "1h"), null, emptyList())
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("below the minimum")
        assertThat(store.listedPrefix).isNull() // never listed
    }

    @Test
    fun rejectsPartitionClause() {
        val ops = ops(FakeCatalog(), FakeBlobStore())
        assertThatThrownBy {
            ops.execute(null, handle(), "remove_orphan_files", emptyMap(), null, listOf("p1"))
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("PARTITION")
    }

    // ---- S3 dir-prefix normalization (foot-gun guard) ----

    @Test
    fun dirUriEndsWithExactlyOneSlash() {
        assertThat(S3WarehouseBlobStore.dirUri("s3://b/data/t")).isEqualTo("s3://b/data/t/")
        assertThat(S3WarehouseBlobStore.dirUri("s3://b/data/t/")).isEqualTo("s3://b/data/t/")
        assertThat(S3WarehouseBlobStore.dirUri("s3://b/data/t///")).isEqualTo("s3://b/data/t/")
    }

    // ---- fixtures ----

    private fun warehouseProps() = mapOf("storage.warehouse" to "s3://ducklake/data")

    private fun handle() = DuckLakeTableHandle("sales", "orders", schemaId = 10L, tableId = 20L, snapshotId = 5L)

    private fun ops(cat: FakeCatalog, store: FakeBlobStore) =
        DuckLakeProcedureOps(cat, warehouseProps()) { store }

    private class FakeBlobStore(
        private val listing: List<BlobEntry> = emptyList(),
        private val failUris: Set<String> = emptySet(),
    ) : WarehouseBlobStore {
        val deleteRequested = mutableListOf<String>()
        var listedPrefix: String? = null

        override fun delete(uris: Collection<String>): BlobDeleteResult {
            deleteRequested.addAll(uris)
            return BlobDeleteResult(uris.filter { it !in failUris }.toSet(), failUris.associateWith { "fail" })
        }

        override fun list(prefixUri: String): List<BlobEntry> {
            listedPrefix = prefixUri
            return listing
        }
    }

    private class FakeCatalog(
        private val referenced: List<DucklakeFilePathRef> = emptyList(),
    ) : DucklakeCatalog by throwingDelegate() {

        // null → DuckLakePathResolver falls back to the configured storage.warehouse.
        override fun getDataPath(): String? = null

        override fun getSchema(schemaName: String, snapshotId: Long): DucklakeSchema =
            DucklakeSchema(10L, UUID.randomUUID(), 1L, null, schemaName, "sales", pathIsRelative = true)

        override fun getTable(schemaName: String, tableName: String, snapshotId: Long): DucklakeTable =
            DucklakeTable(20L, UUID.randomUUID(), 1L, null, 10L, tableName, "orders", pathIsRelative = true)

        override fun listReferencedFilePaths(tableId: Long): List<DucklakeFilePathRef> = referenced

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
