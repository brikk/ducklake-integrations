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
import java.util.Optional

/**
 * Coverage smoke for [JdbcDucklakeCatalog] against a local DuckDB
 * `.db` file. Exercises a representative slice of catalog CRUD —
 * schema and table create/list/drop — to validate that the same jOOQ DSL we
 * run against PostgreSQL round-trips correctly against DuckDB-as-catalog.
 *
 * Why this exists: upstream Quack RPC still has multi-table-query and
 * UPDATE/DELETE limitations that block the connector's natural read/write
 * patterns from working end-to-end. We don't want to compromise our SQL to
 * work around those — they'll lift as Quack matures. In the meantime this
 * test proves that the SQL itself works against DuckDB, so when Quack
 * catches up the connector will Just Work without a second refactor. See
 * `dev-docs/TODO-WRITE-MODE.md § Quack Catalog Backend`.
 *
 * Connection model: bootstrap the `.db` file via an in-process
 * DuckDB JDBC connection that ATTACHes it as a DuckLake catalog (creates the
 * `ducklake_*` metadata schema), then close. [JdbcDucklakeCatalog]
 * opens the same file directly with a plain `jdbc:duckdb:/path/to/lake.db`
 * URL — no DuckLake-on-X wrapper at runtime, so UPDATE/DELETE on metadata
 * rows hit regular base tables.
 */
class TestJdbcDucklakeCatalogOnLocalDuckDbSmoke {
    companion object {
        private var fixture: TestingDucklakeLocalDuckDbCatalogFixture? = null
        private var catalog: JdbcDucklakeCatalog? = null
        private lateinit var catalogFile: Path
        private lateinit var dataDir: Path

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun setUpClass() {
            fixture = TestingDucklakeLocalDuckDbCatalogFixture()
            val catalogDir = fixture!!.catalogDirectory("local-duckdb-smoke")
            Files.createDirectories(catalogDir)
            catalogFile = catalogDir.resolve("lake.db")
            dataDir = catalogDir.resolve("data")
            Files.createDirectories(dataDir)

            // Bootstrap the .db file by ATTACHing it as a DuckLake catalog via an
            // in-memory JDBC connection. This materialises the ducklake_* metadata
            // schema inside lake.db. The connection is closed before the catalog
            // pool opens the file directly.
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("INSTALL ducklake")
                    stmt.execute("LOAD ducklake")
                    stmt.execute(
                        "ATTACH 'ducklake:" + catalogFile.toAbsolutePath() + "' AS lake " +
                            "(DATA_PATH '" + dataDir.toAbsolutePath() + "')",
                    )
                    stmt.execute("DETACH lake")
                }
            }

            val config = DucklakeCatalogConfig()
                .setCatalogDatabaseUrl("jdbc:duckdb:" + catalogFile.toAbsolutePath())
                .setDataPath(dataDir.toAbsolutePath().toString())
                .setMaxCatalogConnections(3)
            catalog = JdbcDucklakeCatalog(config)
        }

        @JvmStatic
        @AfterAll
        fun tearDownClass() {
            catalog?.close()
            fixture?.close()
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

        // Mix of types — exercise integer + varchar + boolean + date paths through
        // the column-spec → ducklake_column insert codepath.
        val cols = listOf(
            TableColumnSpec.leaf("id", "INTEGER", false),
            TableColumnSpec.leaf("name", "VARCHAR", true),
            TableColumnSpec.leaf("active", "BOOLEAN", true),
            TableColumnSpec.leaf("event_date", "DATE", true),
        )
        catalog.createTable(schemaName, "smoke_tbl", cols, Optional.empty(), Optional.empty())

        val snapshotId = catalog.currentSnapshotId
        assertThat(catalog.listTables(schema.schemaId, snapshotId))
            .extracting(java.util.function.Function<DucklakeTable, String> { it.tableName })
            .containsExactly("smoke_tbl")

        catalog.dropTable(schemaName, "smoke_tbl")
        assertThat(catalog.listTables(schema.schemaId, catalog.currentSnapshotId))
            .`as`(
                "dropTable must end-snapshot the row (UPDATE on ducklake_table) " +
                    "and listTables must respect it",
            )
            .isEmpty()

        catalog.dropSchema(schemaName)
        assertThat(catalog.listSchemas(catalog.currentSnapshotId))
            .extracting(java.util.function.Function<DucklakeSchema, String> { it.schemaName })
            .doesNotContain(schemaName)
    }
}
