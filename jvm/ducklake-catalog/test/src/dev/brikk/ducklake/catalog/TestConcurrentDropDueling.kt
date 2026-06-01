/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.catalog

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.Optional

/**
 * Pins behavior for two writers concurrently dropping the SAME catalog
 * object. Upstream's matrix flags
 * `dropped_tables × dropped_tables` (ducklake_transaction.cpp:1188–1190),
 * `dropped_schemas × dropped_schemas` (:1206), and
 * `dropped_views × dropped_views` (:1192–1194).
 *
 * In our short-lived-transaction model, the loser's retry's action calls
 * `resolveTableId` / `resolveSchemaId` / `resolveActiveViewRow`
 * before the matrix runs, so the abort actually surfaces as a generic
 * "{Table,Schema,View} not found" [RuntimeException] from the action,
 * not as a [LogicalConflictException] from the matrix. Either way
 * the loser fails non-silently and the catalog ends in a clean state —
 * which is what these tests pin.
 *
 * The matrix entries remain live for the same scenarios in upstream's
 * deferred-write model and would fire in our flow too if any future action
 * accumulated a [WriteChange.DroppedTable] / etc. without re-resolving
 * its target.
 */
class TestConcurrentDropDueling {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                server,
                "concurrent-drop-dueling",
            )

            val config = DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl)
                .setCatalogDatabaseUser(isolated.user)
                .setCatalogDatabasePassword(isolated.password)
                .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                .setMaxCatalogConnections(5)
            catalog = JdbcDucklakeCatalog(config)
        }

        @JvmStatic
        @AfterAll
        fun tearDownClass() {
            if (::catalog.isInitialized) {
                catalog.close()
            }
            if (::server.isInitialized) {
                server.close()
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun duelingDropTableLoserFails() {
        catalog.createTable(
            "test_schema", "drop_dueling_table",
            listOf(TableColumnSpec.leaf("id", "integer", false)),
            Optional.empty(),
            Optional.empty(),
        )

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.dropTable("test_schema", "drop_dueling_table") },
            Runnable { catalog.dropTable("test_schema", "drop_dueling_table") },
        )

        assertThat(result.loserException)
            .`as`("loser must fail when the table is already dropped")
            .isNotNull()
        assertThat(result.loserAttemptCount)
            .`as`("loser tried at least twice (parked, then retry)")
            .isGreaterThanOrEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getTable("test_schema", "drop_dueling_table", latestSnapshot))
            .`as`("table is dropped at the latest snapshot — winner's drop landed exactly once")
            .isEmpty
    }

    @Test
    @Throws(Exception::class)
    fun duelingDropSchemaLoserFails() {
        catalog.createSchema("drop_dueling_schema")

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.dropSchema("drop_dueling_schema") },
            Runnable { catalog.dropSchema("drop_dueling_schema") },
        )

        assertThat(result.loserException)
            .`as`("loser must fail when the schema is already dropped")
            .isNotNull()

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getSchema("drop_dueling_schema", latestSnapshot))
            .`as`("schema is dropped at the latest snapshot — winner's drop landed exactly once")
            .isEmpty
    }

    @Test
    @Throws(Exception::class)
    fun duelingDropViewLoserFails() {
        catalog.createView(
            "test_schema", "drop_dueling_view",
            "SELECT 1 AS x", "trino", null,
        )

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.dropView("test_schema", "drop_dueling_view") },
            Runnable { catalog.dropView("test_schema", "drop_dueling_view") },
        )

        assertThat(result.loserException)
            .`as`("loser must fail when the view is already dropped")
            .isNotNull()

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getView("test_schema", "drop_dueling_view", latestSnapshot))
            .`as`("view is dropped at the latest snapshot — winner's drop landed exactly once")
            .isEmpty
    }
}
