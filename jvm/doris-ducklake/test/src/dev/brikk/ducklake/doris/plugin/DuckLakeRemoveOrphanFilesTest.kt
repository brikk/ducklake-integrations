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
 * Coverage of `remove_orphan_files`. The data-loss-critical selection ([OrphanFiles]) is pure and
 * tested exhaustively (the `ducklake-` residue gate, known-set exact match, dir-prefix guard, age
 * grace). The tiered-scope orchestration in [DuckLakeProcedureOps] (table → schema → catalog, from
 * the named table's viewpoint) is tested via a fake [WarehouseBlobStore] + fake catalog. The real S3
 * list/delete is covered by the compose smoke.
 */
internal class DuckLakeRemoveOrphanFilesTest {

    private val now: Instant = Instant.parse("2026-07-10T00:00:00Z")
    private val old: Instant = now.minusSeconds(30L * 24 * 3600) // 30d ago — past any grace period
    private val recent: Instant = now.minusSeconds(60) // 1 min ago — inside any grace period

    // ---- OrphanFiles.find + isDucklakeManagedResidue (pure) ----

    @Test
    fun onlyDeletesDucklakeManagedResidue() {
        // The critical safety gate: only ducklake-prefixed managed residue is a candidate. Foreign
        // files — including a user's OWN non-ducklake parquet — are never deleted.
        val listed = listOf(
            BlobEntry("s3://b/t/ducklake-1.parquet", old), // data residue → delete
            BlobEntry("s3://b/t/ducklake-delete-2.parquet", old), // delete-file residue → delete
            BlobEntry("s3://b/t/ducklake-3-delete.puffin", old), // DuckDB-named puffin delete → delete
            BlobEntry("s3://b/t/ducklake-4.db", old), // DuckDB-format data → delete
            BlobEntry("s3://b/t/data.parquet", old), // user's own parquet (no prefix) → KEEP
            BlobEntry("s3://b/t/_SUCCESS", old), // Hive marker → KEEP
            BlobEntry("s3://b/t/ducklake-5.parquet.crc", old), // checksum sidecar (wrong ext) → KEEP
            BlobEntry("s3://b/t/ducklake-6.vortex", old), // vortex not managed by Doris → KEEP
            BlobEntry("s3://b/t/ducklake-7.lance/part.lance", old), // lance member (no prefix on basename) → KEEP
            BlobEntry("s3://b/t/notes.txt", old), // stray → KEEP
        )
        assertThat(OrphanFiles.find(listed, emptySet(), now)).containsExactlyInAnyOrder(
            "s3://b/t/ducklake-1.parquet",
            "s3://b/t/ducklake-delete-2.parquet",
            "s3://b/t/ducklake-3-delete.puffin",
            "s3://b/t/ducklake-4.db",
        )
    }

    @Test
    fun isDucklakeManagedResidueRules() {
        assertThat(OrphanFiles.isDucklakeManagedResidue("s3://b/t/ducklake-x.parquet")).isTrue()
        assertThat(OrphanFiles.isDucklakeManagedResidue("s3://b/t/ducklake-x.PARQUET")).isTrue() // case-insensitive ext
        assertThat(OrphanFiles.isDucklakeManagedResidue("s3://b/t/ducklake-x.puffin")).isTrue()
        assertThat(OrphanFiles.isDucklakeManagedResidue("s3://b/t/ducklake-x.db")).isTrue()
        assertThat(OrphanFiles.isDucklakeManagedResidue("s3://b/t/data.parquet")).isFalse() // no prefix
        assertThat(OrphanFiles.isDucklakeManagedResidue("s3://b/t/ducklake-x.vortex")).isFalse() // Doris omits
        assertThat(OrphanFiles.isDucklakeManagedResidue("s3://b/t/ducklake-x.txt")).isFalse() // unmanaged ext
        assertThat(OrphanFiles.isDucklakeManagedResidue("s3://b/t/_SUCCESS")).isFalse()
    }

    @Test
    fun keepsKnownReferencedAndYoungResidue() {
        val listed = listOf(
            BlobEntry("s3://b/t/ducklake-known.parquet", old), // referenced → keep
            BlobEntry("s3://b/t/ducklake-orphan.parquet", old), // orphan → delete
            BlobEntry("s3://b/t/ducklake-young.parquet", recent), // young → keep
        )
        val known = setOf("s3://b/t/ducklake-known.parquet")
        val cutoff = now.minusSeconds(7L * 24 * 3600) // 7d grace
        assertThat(OrphanFiles.find(listed, known, cutoff)).containsExactly("s3://b/t/ducklake-orphan.parquet")
    }

    @Test
    fun keepsMembersOfKnownDatasetDirectories() {
        val listed = listOf(
            BlobEntry("s3://b/t/ducklake-ds/ducklake-part.parquet", old),
            BlobEntry("s3://b/t/ducklake-stray.parquet", old),
        )
        val known = setOf("s3://b/t/ducklake-ds") // a known directory
        assertThat(OrphanFiles.find(listed, known, now)).containsExactly("s3://b/t/ducklake-stray.parquet")
    }

    // ---- orchestration: scope tiers from the named table's viewpoint ----

    @Test
    fun tableScopeDefaultSweepsOnlyTheNamedTable() {
        val cat = catalog()
        val store = FakeBlobStore(
            listing = listOf(
                BlobEntry("s3://dl/data/sales/orders/ducklake-live.parquet", old), // referenced → keep
                BlobEntry("s3://dl/data/sales/orders/ducklake-orphan.parquet", old), // orphan → delete
                BlobEntry("s3://dl/data/sales/orders/data.parquet", old), // foreign → keep
            ),
        )
        val res = ops(cat, store).execute(
            null, handle("sales", "orders"), "remove_orphan_files", mapOf("retention_threshold" to "7d"), null, emptyList(),
        )
        assertThat(store.listedPrefixes).containsExactly("s3://dl/data/sales/orders") // table dir only
        assertThat(store.deleteRequested).containsExactly("s3://dl/data/sales/orders/ducklake-orphan.parquet")
        assertThat(res.rows.single()).containsExactly("false", "7d", "1", "0")
    }

    @Test
    fun schemaScopeSweepsTheSchemaDirAndUnionsItsTables() {
        val cat = catalog()
        val store = FakeBlobStore(
            listing = listOf(
                BlobEntry("s3://dl/data/sales/orders/ducklake-live.parquet", old), // referenced (orders) → keep
                BlobEntry("s3://dl/data/sales/customers/ducklake-live-c.parquet", old), // referenced (customers) → keep
                BlobEntry("s3://dl/data/sales/failed_create/ducklake-ghost.parquet", old), // failed CREATE residue → delete
            ),
        )
        val res = ops(cat, store).execute(
            null, handle("sales", "orders"), "remove_orphan_files",
            mapOf("scope" to "schema", "retention_threshold" to "7d"), null, emptyList(),
        )
        assertThat(store.listedPrefixes).containsExactly("s3://dl/data/sales") // schema dir
        assertThat(store.deleteRequested).containsExactly("s3://dl/data/sales/failed_create/ducklake-ghost.parquet")
        assertThat(res.rows.single()).containsExactly("false", "7d", "1", "0")
    }

    @Test
    fun catalogScopeSweepsRootAndUnionsAllSchemas() {
        val cat = catalog()
        val store = FakeBlobStore(
            listing = listOf(
                BlobEntry("s3://dl/data/sales/orders/ducklake-live.parquet", old), // referenced → keep
                BlobEntry("s3://dl/data/analytics/events/ducklake-live-e.parquet", old), // referenced (other schema) → keep
                BlobEntry("s3://dl/data/ghost_schema/ducklake-ghost.parquet", old), // dropped/failed residue → delete
            ),
        )
        val res = ops(cat, store).execute(
            null, handle("sales", "orders"), "remove_orphan_files",
            mapOf("scope" to "catalog", "retention_threshold" to "7d"), null, emptyList(),
        )
        assertThat(store.listedPrefixes).containsExactly("s3://dl/data") // warehouse root
        assertThat(store.deleteRequested).containsExactly("s3://dl/data/ghost_schema/ducklake-ghost.parquet")
        assertThat(res.rows.single()).containsExactly("false", "7d", "1", "0")
    }

    @Test
    fun databaseIsAnAliasForSchemaScope() {
        val cat = catalog()
        val store = FakeBlobStore(listing = emptyList())
        ops(cat, store).execute(
            null, handle("sales", "orders"), "remove_orphan_files",
            mapOf("scope" to "database"), null, emptyList(),
        )
        assertThat(store.listedPrefixes).containsExactly("s3://dl/data/sales") // schema dir
    }

    @Test
    fun dryRunListsButDeletesNothing() {
        val cat = catalog()
        val store = FakeBlobStore(listing = listOf(BlobEntry("s3://dl/data/sales/orders/ducklake-o.parquet", old)))
        val res = ops(cat, store).execute(
            null, handle("sales", "orders"), "remove_orphan_files",
            mapOf("retention_threshold" to "7d", "dry_run" to "true"), null, emptyList(),
        )
        assertThat(store.deleteRequested).isEmpty()
        assertThat(res.rows.single()).containsExactly("true", "7d", "1", "0")
    }

    @Test
    fun retentionBelowFloorIsRejectedBeforeListing() {
        val store = FakeBlobStore()
        assertThatThrownBy {
            ops(catalog(), store).execute(null, handle("sales", "orders"), "remove_orphan_files", mapOf("retention_threshold" to "1h"), null, emptyList())
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("below the minimum")
        assertThat(store.listedPrefixes).isEmpty()
    }

    @Test
    fun invalidScopeIsRejected() {
        assertThatThrownBy {
            ops(catalog(), FakeBlobStore()).execute(null, handle("sales", "orders"), "remove_orphan_files", mapOf("scope" to "cluster"), null, emptyList())
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("Invalid scope")
    }

    @Test
    fun rejectsPartitionClause() {
        assertThatThrownBy {
            ops(catalog(), FakeBlobStore()).execute(null, handle("sales", "orders"), "remove_orphan_files", emptyMap(), null, listOf("p1"))
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

    private fun ops(cat: FakeCatalog, store: FakeBlobStore) =
        DuckLakeProcedureOps(cat, mapOf("storage.warehouse" to "s3://dl/data")) { store }

    private fun handle(db: String, table: String) =
        DuckLakeTableHandle(db, table, schemaId = if (db == "sales") 10L else 11L, tableId = 20L, snapshotId = 5L)

    /** Two schemas (sales: orders+customers, analytics: events), each table with one referenced file. */
    private fun catalog() = FakeCatalog(
        schemas = mapOf(
            "sales" to sch(10L, "sales"),
            "analytics" to sch(11L, "analytics"),
        ),
        tablesBySchemaId = mapOf(
            10L to listOf(tbl(20L, 10L, "orders"), tbl(21L, 10L, "customers")),
            11L to listOf(tbl(30L, 11L, "events")),
        ),
        refsByTableId = mapOf(
            20L to listOf(DucklakeFilePathRef("ducklake-live.parquet", true)),
            21L to listOf(DucklakeFilePathRef("ducklake-live-c.parquet", true)),
            30L to listOf(DucklakeFilePathRef("ducklake-live-e.parquet", true)),
        ),
    )

    private fun sch(id: Long, name: String) = DucklakeSchema(id, UUID.randomUUID(), 1L, null, name, name, pathIsRelative = true)
    private fun tbl(id: Long, schemaId: Long, name: String) = DucklakeTable(id, UUID.randomUUID(), 1L, null, schemaId, name, name, pathIsRelative = true)

    private class FakeBlobStore(private val listing: List<BlobEntry> = emptyList()) : WarehouseBlobStore {
        val deleteRequested = mutableListOf<String>()
        val listedPrefixes = mutableListOf<String>()
        override fun delete(uris: Collection<String>): BlobDeleteResult {
            deleteRequested.addAll(uris)
            return BlobDeleteResult(uris.toSet(), emptyMap())
        }
        override fun list(prefixUri: String): List<BlobEntry> {
            listedPrefixes.add(prefixUri)
            // Return only entries under this prefix (dir-safe), mimicking a real listing.
            val dir = prefixUri.trimEnd('/') + "/"
            return listing.filter { it.uri.startsWith(dir) }
        }
    }

    private class FakeCatalog(
        private val schemas: Map<String, DucklakeSchema> = emptyMap(),
        private val tablesBySchemaId: Map<Long, List<DucklakeTable>> = emptyMap(),
        private val refsByTableId: Map<Long, List<DucklakeFilePathRef>> = emptyMap(),
    ) : DucklakeCatalog by throwingDelegate() {

        override fun getDataPath(): String = "s3://dl/data"
        override fun getSchema(schemaName: String, snapshotId: Long): DucklakeSchema? = schemas[schemaName]
        override fun getTable(schemaName: String, tableName: String, snapshotId: Long): DucklakeTable? =
            tablesBySchemaId[schemas[schemaName]?.schemaId]?.firstOrNull { it.tableName == tableName }
        override fun listSchemas(snapshotId: Long): List<DucklakeSchema> = schemas.values.toList()
        override fun listTables(schemaId: Long, snapshotId: Long): List<DucklakeTable> = tablesBySchemaId[schemaId].orEmpty()
        override fun listReferencedFilePaths(tableId: Long): List<DucklakeFilePathRef> = refsByTableId[tableId].orEmpty()

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
