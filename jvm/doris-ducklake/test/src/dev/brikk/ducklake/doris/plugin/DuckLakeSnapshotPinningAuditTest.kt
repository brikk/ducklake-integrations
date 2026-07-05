package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TableColumnSpec
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.api.ConnectorType
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * Snapshot-pinning audit at the metadata level: a [DuckLakeTableHandle] pins the
 * snapshot it was resolved at, and every metadata read through that handle must
 * keep answering from THAT snapshot while the catalog advances underneath —
 * schema reads after ADD/RENAME COLUMN, data-file sets after inserts, even
 * whole-table reads after DROP TABLE.
 *
 * Ports the pinning semantics of trino-ducklake's
 * `TestDucklakeCatalogSnapshotPinningIntegration.kt` (a pinned read keeps seeing
 * the historical row count while the live catalog moves on) to the doris FE
 * surface, where the pin lives on the table handle instead of a session/catalog
 * property. The scan planner is out of scope here (BE-side); what the FE owns is
 * that a query planned against handle@snapN never sees snapN+1 metadata — the
 * repeatable-read guarantee everything downstream assumes.
 *
 * Complements [DuckLakeConnectorMetadataTimeTravelTest]: that suite covers
 * resolving a PAST snapshot onto a handle (FOR VERSION/TIME AS OF); this one
 * covers the dual — a handle staying stable while the FUTURE happens around it.
 */
internal class DuckLakeSnapshotPinningAuditTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeAuditFixtureSeeder.seed(
                server,
                "pinningaudit",
                listOf(
                    "CREATE SCHEMA dl.pin",
                    // Real inserts so the table owns real data files to count.
                    "CREATE TABLE dl.pin.facts (id INTEGER, name VARCHAR)",
                    "INSERT INTO dl.pin.facts VALUES (1, 'a'), (2, 'b')",
                    "INSERT INTO dl.pin.facts VALUES (3, 'c')",
                    "CREATE TABLE dl.pin.doomed (id INTEGER)",
                ),
            )
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

    private fun columnNames(md: DuckLakeConnectorMetadata, handle: DuckLakeTableHandle): List<String> =
        md.getTableSchema(null, handle).columns.map(ConnectorColumn::getName)

    private fun handleFor(md: DuckLakeConnectorMetadata, table: String): DuckLakeTableHandle =
        md.getTableHandle(null, "pin", table).orFail("expected pin.$table handle") as DuckLakeTableHandle

    @Test
    fun pinnedHandleSchemaIsStableAcrossAddColumn() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val pinned = handleFor(md, "facts")
            assertThat(columnNames(md, pinned)).containsExactly("id", "name")

            // Advance the catalog underneath the handle: ADD COLUMN commits a new
            // snapshot (columns carry begin/end-snapshot ranges in DuckLake).
            catalog.addColumn(pinned.tableId, TableColumnSpec.leaf("added_later", "int32", true))
            assertThat(catalog.currentSnapshotId).isGreaterThan(pinned.snapshotId)

            // The OLD handle keeps resolving the OLD schema — both surfaces.
            assertThat(columnNames(md, pinned)).containsExactly("id", "name")
            assertThat(md.getColumnHandles(null, pinned).keys).containsExactly("id", "name")

            // A freshly resolved handle pins the new snapshot and sees the new column.
            val fresh = handleFor(md, "facts")
            assertThat(fresh.snapshotId).isGreaterThan(pinned.snapshotId)
            assertThat(columnNames(md, fresh)).containsExactly("id", "name", "added_later")
        }
    }

    @Test
    fun pinnedHandleSchemaIsStableAcrossRenameColumn() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            // Own scratch table so this test doesn't depend on / interfere with the
            // add-column mutation of pin.facts (JUnit method order is unspecified).
            md.createTable(
                null,
                ConnectorCreateTableRequest.builder()
                    .dbName("pin")
                    .tableName("rename_scratch")
                    .columns(
                        listOf(
                            ConnectorColumn("id", ConnectorType.of("INT"), "", false, null),
                            ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null),
                        ),
                    )
                    .build(),
            )
            val pinned = handleFor(md, "rename_scratch")

            val nameColumnId = catalog.getTableColumns(pinned.tableId, pinned.snapshotId)
                .first { it.columnName == "name" }
                .columnId
            catalog.renameColumn(pinned.tableId, nameColumnId, "label")

            // Rename is the nastier mutation: same columnId, new name. The pinned
            // handle must keep the OLD name (a plan compiled against it references
            // "name"); only a fresh handle may observe "label".
            assertThat(columnNames(md, pinned)).containsExactly("id", "name")
            assertThat(columnNames(md, handleFor(md, "rename_scratch")))
                .containsExactly("id", "label")
        }
    }

    @Test
    fun pinnedHandleDataFileSetIsStableWhileInsertsLand() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val pinned = handleFor(md, "facts")
            val filesAtPin = catalog.getDataFiles(pinned.tableId, pinned.snapshotId).size
            // The seed INSERTs guarantee at least one real data file; the exact count
            // is DuckDB's business (it may consolidate at CHECKPOINT) — the invariant
            // under test is stability at the pin, not the initial file layout.
            assertThat(filesAtPin).isPositive()

            // A catalog-level insert commit (the same primitive the write path uses)
            // registers one more data file in a new snapshot.
            catalog.commitInsert(
                pinned.tableId,
                listOf(
                    DucklakeWriteFragment(
                        "pin/facts/pinning-audit-${System.nanoTime()}.parquet",
                        true,
                        "parquet",
                        128L,
                        0L,
                        1L,
                        emptyList(),
                        emptyMap(),
                        null,
                        null,
                    ),
                ),
            )

            // trino's pinning test asserts the pinned read keeps counting 2 rows while
            // current counts 4; the FE-metadata equivalent is the file set: unchanged
            // at the pin, grown by one at the fresh handle's snapshot.
            assertThat(catalog.getDataFiles(pinned.tableId, pinned.snapshotId)).hasSize(filesAtPin)
            val fresh = handleFor(md, "facts")
            assertThat(catalog.getDataFiles(fresh.tableId, fresh.snapshotId)).hasSize(filesAtPin + 1)

            // beginQuerySnapshot must pin the query to the HANDLE's snapshot, not to
            // whatever is current at call time — this is what the engine serializes
            // toward the BE, so a drift here re-introduces read skew end to end.
            val mvcc = md.beginQuerySnapshot(null, pinned).orFail("expected an mvcc snapshot")
            assertThat(mvcc.snapshotId).isEqualTo(pinned.snapshotId)
        }
    }

    @Test
    fun droppedTableStaysReadableThroughItsPinnedHandle() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)
            val pinned = handleFor(md, "doomed")

            md.dropTable(null, pinned)

            // Listings and fresh lookups follow the CURRENT snapshot: gone.
            assertThat(md.listTableNames(null, "pin")).doesNotContain("doomed")
            assertThat(md.getTableHandle(null, "pin", "doomed")).isEmpty

            // But DuckLake drops are soft (end-snapshot stamping), so the pinned
            // handle still resolves its historical schema — an in-flight query that
            // resolved the handle before the DROP committed must not blow up mid-plan.
            assertThat(columnNames(md, pinned)).containsExactly("id")
        }
    }
}
