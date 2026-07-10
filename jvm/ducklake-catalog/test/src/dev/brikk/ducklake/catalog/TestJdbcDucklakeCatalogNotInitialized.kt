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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.nio.file.Files

/**
 * When the catalog database is reachable but its DuckLake metadata schema was never
 * bootstrapped (no `ducklake_*` tables), catalog reads must fail with a clear, actionable
 * [DucklakeCatalogNotInitializedException] — not the opaque low-level "table does not exist"
 * SQL error. Uses a bare (empty) DuckDB `.db` file — DuckDB creates the file on open but the
 * ducklake schema is absent, exactly the un-initialized state.
 */
class TestJdbcDucklakeCatalogNotInitialized {

    @Test
    fun catalogReadOnUnbootstrappedDbThrowsClearError() {
        val dir = Files.createTempDirectory("uninitialized-catalog")
        val dbFile = dir.resolve("empty.db")
        val config = DucklakeCatalogConfig().apply {
            // Point at a DuckDB file that has never been ATTACHed as a DuckLake catalog, so the
            // ducklake_* schema does not exist. No bootstrap step here (that is the whole point).
            catalogDatabaseUrl = "jdbc:duckdb:" + dbFile.toAbsolutePath()
            dataPath = dir.toAbsolutePath().toString()
            maxCatalogConnections = 2
        }

        val catalog = JdbcDucklakeCatalog(config)
        try {
            assertThatThrownBy { catalog.currentSnapshotId }
                .isInstanceOf(DucklakeCatalogNotInitializedException::class.java)
                .hasMessageContaining("has no metadata schema")
                .hasMessageContaining("Initialize the catalog once")

            // listSnapshots / getSnapshot go through the same guard.
            assertThatThrownBy { catalog.listSnapshots() }
                .isInstanceOf(DucklakeCatalogNotInitializedException::class.java)
            assertThatThrownBy { catalog.getSnapshot(1) }
                .isInstanceOf(DucklakeCatalogNotInitializedException::class.java)
        }
        finally {
            catalog.close()
        }
    }

    @Test
    fun detectorMatchesBackendMissingTableErrors() {
        // Guards against message-shape drift across backends (PG / MySQL / DuckDB / SQLite).
        assertThat(JdbcDucklakeCatalog.isMissingCatalogSchema(
            java.sql.SQLException("relation \"ducklake_snapshot\" does not exist", "42P01"))).isTrue()
        assertThat(JdbcDucklakeCatalog.isMissingCatalogSchema(
            java.sql.SQLException("Table 'ducklake.ducklake_snapshot' doesn't exist", "42S02", 1146))).isTrue()
        assertThat(JdbcDucklakeCatalog.isMissingCatalogSchema(
            java.sql.SQLException("Catalog Error: Table with name ducklake_snapshot does not exist!"))).isTrue()
        assertThat(JdbcDucklakeCatalog.isMissingCatalogSchema(
            java.sql.SQLException("no such table: ducklake_snapshot"))).isTrue()
        // A genuinely missing USER table must NOT be misclassified as an un-initialized catalog.
        assertThat(JdbcDucklakeCatalog.isMissingCatalogSchema(
            java.sql.SQLException("relation \"orders\" does not exist"))).isFalse()
    }
}
