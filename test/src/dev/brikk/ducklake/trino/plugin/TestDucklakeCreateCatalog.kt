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
package dev.brikk.ducklake.trino.plugin

import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.QueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Exercises Trino's SQL `CREATE CATALOG <name> USING ducklake WITH (...)` dynamic-catalog path
 * (https://trino.io/docs/current/sql/create-catalog.html) against our connector — the runtime
 * equivalent of an `etc/catalog/<name>.properties` file. This goes through the SAME
 * `DucklakeConnectorFactory.create(catalogName, config, context)` + Airlift config binding as a
 * file-based catalog, so it proves the connector instantiates cleanly from SQL and that our config
 * surface tolerates the stricter fail-at-query-time behavior (all values arrive as varchars).
 *
 * Requires the coordinator's catalog-management type set to `dynamic`; the runner is built with
 * `catalog.management=dynamic` and NO catalog pre-created — every catalog here is made via SQL.
 *
 * A self-contained local DuckDB catalog is used as the backing store (no shared-catalog
 * dependency), created fresh by DuckDB so the connector has real metadata + data to read back.
 *
 * SAME_THREAD: single shared on-disk catalog/data dir.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCreateCatalog : AbstractTestQueryFramework() {

    private lateinit var rootDir: Path
    private lateinit var lakeDb: String
    private lateinit var dataDir: String

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        rootDir = Files.createTempDirectory("ducklake-create-catalog-")
        lakeDb = rootDir.resolve("lake.db").toAbsolutePath().toString()
        dataDir = rootDir.resolve("data").toAbsolutePath().toString()

        // Seed a real DuckLake catalog + table through DuckDB so a dynamically-created Trino
        // catalog has something to read back.
        DriverManager.getConnection("jdbc:duckdb:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSTALL ducklake")
                st.execute("LOAD ducklake")
                st.execute("ATTACH 'ducklake:$lakeDb' AS lake (DATA_PATH '$dataDir/')")
                st.execute("USE lake")
                st.execute("CREATE SCHEMA test_schema")
                st.execute("CREATE TABLE test_schema.t AS SELECT i::INTEGER AS id FROM range(5) t(i)")
            }
        }

        // No default catalog/schema: the tests create catalogs via SQL. DistributedQueryRunner
        // defaults the coordinator's catalog-management type to `dynamic` when no default catalog
        // is set, which is exactly what enables the CREATE CATALOG statement to succeed.
        val session = testSessionBuilder().build()
        val runner = DistributedQueryRunner.builder(session).build()
        try {
            runner.installPlugin(DucklakePlugin())
            return runner
        }
        catch (e: Throwable) {
            runner.close()
            throw e
        }
    }

    @AfterAll
    fun cleanup() {
        if (::rootDir.isInitialized && Files.exists(rootDir)) {
            Files.walk(rootDir).use { w -> w.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    private fun createDucklakeCatalogSql(name: String): String =
            """
            CREATE CATALOG $name USING ducklake
            WITH (
              "ducklake.catalog.database-url" = 'jdbc:duckdb:$lakeDb',
              "ducklake.data-path" = '$dataDir/',
              "fs.hadoop.enabled" = 'true'
            )
            """.trimIndent()

    @Test
    fun createCatalogViaSqlThenQueryIt() {
        computeActual(createDucklakeCatalogSql("dyn_ducklake"))
        try {
            // The dynamically-created catalog shows up and its data is readable through the same
            // connector code path as a file-based catalog.
            assertThat(computeActual("SHOW CATALOGS").materializedRows.map { it.getField(0) as String })
                    .contains("dyn_ducklake")
            assertThat(computeScalar("SELECT count(*) FROM dyn_ducklake.test_schema.t")).isEqualTo(5L)
            assertThat(computeActual("SELECT id FROM dyn_ducklake.test_schema.t ORDER BY id").materializedRows
                    .map { it.getField(0) as Int })
                    .containsExactly(0, 1, 2, 3, 4)
        }
        finally {
            computeActual("DROP CATALOG dyn_ducklake")
        }
    }

    @Test
    fun createCatalogWithUnknownPropertyFails() {
        // The doc guarantees CREATE CATALOG fails at query time on an invalid property — proves our
        // Airlift config binding rejects unknown/typo'd properties instead of silently ignoring them.
        assertThatThrownBy {
            computeActual(
                    """
                    CREATE CATALOG dyn_bad USING ducklake
                    WITH (
                      "ducklake.catalog.database-url" = 'jdbc:duckdb:$lakeDb',
                      "ducklake.data-path" = '$dataDir/',
                      "ducklake.this-property-does-not-exist" = 'oops'
                    )
                    """.trimIndent())
        }
                .hasMessageContaining("ducklake.this-property-does-not-exist")
    }
}
