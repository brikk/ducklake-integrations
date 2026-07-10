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
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Coverage smoke for [JdbcDucklakeCatalog] against a MySQL 8+ -backed DuckLake catalog. This is
 * the "trust our tests" verification for the MySQL backend: it proves the same jOOQ DSL we run
 * against PostgreSQL / DuckDB round-trips correctly against MySQL, exercising a representative
 * slice of catalog CRUD (schema + table create/list/drop) through the MySQL Connector/J driver
 * (dialect auto-detected as `SQLDialect.MYSQL` from the `jdbc:mysql:` URL).
 *
 * Backend-specific notes:
 *   - Booleans are stored `tinyint(1)`, `*_uuid` columns as `text` (DuckLake-on-MySQL physical
 *     types) — jOOQ's MYSQL Boolean mapping and our `*_uuid` forcedType UUID binding handle both.
 *   - The one MySQL-specific code path is [JdbcDucklakeCatalog]'s duplicate-key detection
 *     (error 1062 / SQLState 23000), which gates optimistic-concurrency retries.
 *
 * Connection model (mirrors [TestJdbcDucklakeCatalogOnLocalDuckDbSmoke]): bootstrap the empty
 * `ducklake_*` schema with a ONE-SHOT DuckDB `ATTACH 'ducklake:mysql:...'` (verified reliable),
 * then close DuckDB. [JdbcDucklakeCatalog] connects directly with `jdbc:mysql://.../<db>` — the
 * flaky DuckDB `mysql` extension is never used at runtime, so this test does not depend on it.
 *
 * Cross-engine (DuckDB reading a MySQL-backed catalog) is intentionally NOT tested — it is
 * deferred pending an upstream fix to DuckDB's `mysql` extension. See dev-docs/CATALOG-BACKENDS.md.
 */
class TestJdbcDucklakeCatalogOnMySqlSmoke {
    companion object {
        private var server: TestingDucklakeMySqlCatalogServer? = null
        private var catalog: JdbcDucklakeCatalog? = null
        private lateinit var dataDir: Path

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakeMySqlCatalogServer()
            dataDir = Files.createTempDirectory("mysql-smoke-data")

            // Bootstrap the ducklake_* metadata schema in MySQL via a one-shot DuckDB ATTACH,
            // then DETACH/close. Only the bootstrap touches DuckDB's mysql extension; all
            // subsequent operations go through JdbcDucklakeCatalog's MySQL JDBC connection.
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSTALL ducklake")
                    stmt.execute("LOAD ducklake")
                    stmt.execute("INSTALL mysql")
                    stmt.execute("LOAD mysql")
                    stmt.execute(
                        "ATTACH '" + server!!.getDuckDbAttachUri() + "' AS lake " +
                            "(DATA_PATH '" + dataDir.toAbsolutePath() + "')",
                    )
                    stmt.execute("DETACH lake")
                }
            }

            val config = DucklakeCatalogConfig().apply {
                catalogDatabaseUrl = server!!.getJdbcUrl()
                catalogDatabaseUser = server!!.getUser()
                catalogDatabasePassword = server!!.getPassword()
                dataPath = dataDir.toAbsolutePath().toString()
                maxCatalogConnections = 3
            }
            catalog = JdbcDucklakeCatalog(config)
        }

        @JvmStatic
        @AfterAll
        fun tearDownClass() {
            catalog?.close()
            server?.close()
        }
    }

    @Test
    fun listSchemasReturnsTheBootstrappedDefault() {
        val catalog = catalog!!
        val snapshotId = catalog.currentSnapshotId
        assertThat(catalog.listSchemas(snapshotId))
            .extracting(java.util.function.Function<DucklakeSchema, String> { it.schemaName })
            .contains("main")
    }

    @Test
    fun createSchemaTableDropTableDropSchemaRoundTrip() {
        val catalog = catalog!!
        val schemaName = "smoke_sch"
        catalog.createSchema(schemaName)

        val schema = catalog.listSchemas(catalog.currentSnapshotId).stream()
            .filter { it.schemaName == schemaName }
            .findFirst()
            .orElseThrow { AssertionError("schema not visible after createSchema") }

        // Mix of types — exercise integer + varchar + boolean + date paths through the
        // column-spec -> ducklake_column insert codepath (boolean lands in a tinyint(1) column).
        val cols = listOf(
            TableColumnSpec.leaf("id", "INTEGER", false),
            TableColumnSpec.leaf("name", "VARCHAR", true),
            TableColumnSpec.leaf("active", "BOOLEAN", true),
            TableColumnSpec.leaf("event_date", "DATE", true),
        )
        catalog.createTable(schemaName, "smoke_tbl", cols, null, null)

        val snapshotId = catalog.currentSnapshotId
        assertThat(catalog.listTables(schema.schemaId, snapshotId))
            .extracting(java.util.function.Function<DucklakeTable, String> { it.tableName })
            .containsExactly("smoke_tbl")

        catalog.dropTable(schemaName, "smoke_tbl")
        assertThat(catalog.listTables(schema.schemaId, catalog.currentSnapshotId))
            .`as`("dropTable must end-snapshot the row (UPDATE on ducklake_table) and listTables must respect it")
            .isEmpty()

        catalog.dropSchema(schemaName)
        assertThat(catalog.listSchemas(catalog.currentSnapshotId))
            .extracting(java.util.function.Function<DucklakeSchema, String> { it.schemaName })
            .doesNotContain(schemaName)
    }
}
