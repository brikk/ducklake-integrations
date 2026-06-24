package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

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
        }
    }
}
