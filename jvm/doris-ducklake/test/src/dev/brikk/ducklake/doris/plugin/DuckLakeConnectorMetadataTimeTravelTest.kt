package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec
import org.apache.doris.thrift.TIcebergColumnStats
import org.apache.doris.thrift.TIcebergCommitData
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TBinaryProtocol
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Exercises [DuckLakeConnectorMetadata.resolveTimeTravel] against the real
 * Postgres-backed DuckLake catalog.
 *
 * The P-series SPI consolidated the former `getSnapshotAt(timestampMillis)` +
 * `getSnapshotById(snapshotId)` overrides into a single `resolveTimeTravel` that
 * takes a [ConnectorTimeTravelSpec]. DuckLake has linear history only, so it
 * honours `SNAPSHOT_ID` (FOR VERSION AS OF) and `TIMESTAMP` (FOR TIME AS OF) and
 * returns empty for the provider-specific `TAG` / `BRANCH` / `INCREMENTAL` kinds.
 */
internal class DuckLakeConnectorMetadataTimeTravelTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "timetravel")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server.close()
        }
    }

    private fun openCatalog(): JdbcDucklakeCatalog {
        val config = DucklakeCatalogConfig().apply {
            catalogDatabaseUrl = isolated.jdbcUrl()
            catalogDatabaseUser = isolated.user()
            catalogDatabasePassword = isolated.password()
            dataPath = isolated.dataDir().toAbsolutePath().toString()
        }
        return JdbcDucklakeCatalog(config)
    }

    private inline fun <R> withCatalog(block: (JdbcDucklakeCatalog) -> R): R {
        val catalog = openCatalog()
        return try {
            block(catalog)
        } finally {
            catalog.close()
        }
    }

    private fun handleFor(catalog: JdbcDucklakeCatalog): DuckLakeTableHandle {
        val snap = catalog.currentSnapshotId
        val table = requireNotNull(catalog.getTable("sales", "orders", snap)) { "expected sales.orders" }
        return DuckLakeTableHandle("sales", "orders", table.schemaId, table.tableId, snap)
    }

    @Test
    fun resolvesSnapshotIdSpecToThatSnapshot() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val current = catalog.currentSnapshotId

            val resolved = md.resolveTimeTravel(
                null,
                handleFor(catalog),
                ConnectorTimeTravelSpec.snapshotId(current.toString()),
            )

            assertThat(resolved).isPresent
            assertThat(resolved.get().snapshotId).isEqualTo(current)
        }
    }

    @Test
    fun resolvesTimestampSpecToSnapshotAtOrBeforeThatInstant() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val current = catalog.currentSnapshotId

            // A digital TIMESTAMP spec carries raw epoch-millis. "now" must resolve to
            // the latest committed snapshot (at-or-before semantics).
            val nowMillis = System.currentTimeMillis()
            val resolved = md.resolveTimeTravel(
                null,
                handleFor(catalog),
                ConnectorTimeTravelSpec.timestamp(nowMillis.toString(), true),
            )

            assertThat(resolved).isPresent
            assertThat(resolved.get().snapshotId).isEqualTo(current)
        }
    }

    @Test
    fun timestampBeforeAnyCommitResolvesToEmpty() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)

            // Epoch-millis 0 (1970) predates every catalog snapshot → no snapshot at-or-before.
            val resolved = md.resolveTimeTravel(
                null,
                handleFor(catalog),
                ConnectorTimeTravelSpec.timestamp("0", true),
            )

            assertThat(resolved).isEmpty
        }
    }

    @Test
    fun unknownSnapshotIdResolvesToEmpty() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)

            val resolved = md.resolveTimeTravel(
                null,
                handleFor(catalog),
                ConnectorTimeTravelSpec.snapshotId(Long.MAX_VALUE.toString()),
            )

            assertThat(resolved).isEmpty
        }
    }

    @Test
    fun providerSpecificKindsAreUnsupportedAndResolveToEmpty() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val handle = handleFor(catalog)

            // DuckLake has no tags / branches / incremental reads.
            assertThat(md.resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.tag("v1"))).isEmpty
            assertThat(md.resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.branch("main"))).isEmpty
            assertThat(
                md.resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.incremental(emptyMap())),
            ).isEmpty
            // VERSION_REF (P6: FOR VERSION AS OF '<name>', non-numeric) — DuckLake
            // snapshots are numeric-id only, so a named ref resolves to empty and the
            // engine surfaces its "not found" user error.
            assertThat(
                md.resolveTimeTravel(null, handle, ConnectorTimeTravelSpec.versionRef("release-1")),
            ).isEmpty
        }
    }

    // ---- applySnapshot — threads the resolved snapshot onto the table handle ----

    @Test
    fun applySnapshotThreadsResolvedSnapshotIdOntoHandle() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val handle = handleFor(catalog)
            val targetId = handle.snapshotId - 1 // any different, non-negative id

            val applied = md.applySnapshot(null, handle, mvcc(targetId)) as DuckLakeTableHandle

            assertThat(applied.snapshotId).isEqualTo(targetId)
            // Identity fields are preserved; only the snapshot pin moves.
            assertThat(applied.database).isEqualTo(handle.database)
            assertThat(applied.table).isEqualTo(handle.table)
            assertThat(applied.tableId).isEqualTo(handle.tableId)
        }
    }

    @Test
    fun applySnapshotLeavesHandleUnchangedForNullNegativeOrSameSnapshot() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val handle = handleFor(catalog)

            // null pin (no time travel) → read latest, handle untouched.
            assertThat(md.applySnapshot(null, handle, null)).isSameAs(handle)
            // negative id (empty-table / invalid pin) → handle untouched.
            assertThat(md.applySnapshot(null, handle, mvcc(-1))).isSameAs(handle)
            // already pinned at that snapshot → no-op.
            assertThat(md.applySnapshot(null, handle, mvcc(handle.snapshotId))).isSameAs(handle)
        }
    }

    @Test
    fun timeTravelPinResolvesTheHistoricalDataFileSet() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val snap0 = catalog.currentSnapshotId
            val tableId = requireNotNull(catalog.getTable("sales", "orders", snap0)) {
                "expected sales.orders"
            }.tableId
            val filesAtSnap0 = catalog.getDataFiles(tableId, snap0).size

            // Advance history: commit one data file → a new current snapshot.
            val snap1 = commitOneInsertSnapshot(catalog, tableId, snap0)
            assertThat(snap1).isGreaterThan(snap0)
            assertThat(catalog.getDataFiles(tableId, snap1)).hasSize(filesAtSnap0 + 1)

            // getTableHandle pins the latest (snap1); FOR VERSION AS OF snap0 resolves the
            // historical snapshot, and applySnapshot threads it onto the handle.
            val handleNow = md.getTableHandle(null, "sales", "orders").orElseThrow() as DuckLakeTableHandle
            assertThat(handleNow.snapshotId).isEqualTo(snap1)

            val resolved = md.resolveTimeTravel(
                null,
                handleNow,
                ConnectorTimeTravelSpec.snapshotId(snap0.toString()),
            ).orElseThrow()
            val pinned = md.applySnapshot(null, handleNow, resolved) as DuckLakeTableHandle

            // The pinned handle resolves the OLD data-file set — historical state, not current.
            assertThat(pinned.snapshotId).isEqualTo(snap0)
            assertThat(catalog.getDataFiles(pinned.tableId, pinned.snapshotId)).hasSize(filesAtSnap0)
        }
    }

    private fun mvcc(snapshotId: Long): ConnectorMvccSnapshot =
        ConnectorMvccSnapshot.builder().snapshotId(snapshotId).build()

    private fun leInt(value: Int): ByteBuffer =
        ByteBuffer.allocate(Int.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).flip() as ByteBuffer

    /** Commits one synthetic data-file fragment, advancing the catalog by a new snapshot. */
    private fun commitOneInsertSnapshot(
        catalog: JdbcDucklakeCatalog,
        tableId: Long,
        baseSnapshot: Long,
    ): Long {
        val idColumn = catalog.getTableColumns(tableId, baseSnapshot).first { it.columnName == "id" }
        val commit = TIcebergCommitData().apply {
            filePath = "sales/orders/timetravel-${System.nanoTime()}.parquet"
            rowCount = 3
            fileSize = 999
            columnStats = TIcebergColumnStats().apply {
                columnSizes = mapOf(idColumn.columnId.toInt() to 64L)
                valueCounts = mapOf(idColumn.columnId.toInt() to 3L)
                nullValueCounts = mapOf(idColumn.columnId.toInt() to 0L)
                lowerBounds = mapOf(idColumn.columnId.toInt() to leInt(1000))
                upperBounds = mapOf(idColumn.columnId.toInt() to leInt(2000))
            }
        }
        val serialized = TSerializer(TBinaryProtocol.Factory()).serialize(commit)
        val txn = DuckLakeConnectorTransaction(System.nanoTime(), catalog)
        txn.addCommitData(serialized)
        txn.bindTarget(tableId, baseSnapshot)
        txn.commit()
        return catalog.currentSnapshotId
    }
}
