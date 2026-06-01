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
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator

/**
 * End-to-end smoke for [JdbcDucklakeCatalog] against a Quack-backed remote
 * DuckLake catalog. The catalog is configured with the synthetic
 * `jdbc:duckdb:quack://host:port?metadata_catalog=name` URL; HikariCP's
 * per-connection init script bootstraps the remote metadata schema via
 * `ATTACH 'ducklake:quack:...'` on first use, then bare jOOQ DSL against
 * `ducklake_*` resolves through the `USE meta.main` that ends the
 * init.
 *
 * Validates that:
 *  - HikariCP accepts the multi-statement init script we generate.
 *  - jOOQ + the DuckDB JDBC driver round-trip a basic read
 *    (`listSchemas`) against the Quack-attached metadata.
 *  - A write transaction (createSchema) commits via the same connection path
 *    that `attemptWriteTransaction` uses for PG today.
 *
 * If this passes, we have evidence that the existing
 * `JdbcDucklakeCatalog` works against DuckDB-as-catalog without further
 * SQL-dialect surgery, which unblocks the backend-selector wiring.
 */
open class TestJdbcDucklakeCatalogOnQuackSmoke {

    @Test
    fun initialListSchemasReturnsBootstrappedMetadata() {
        val snapshotId = catalog!!.currentSnapshotId
        val schemas = catalog!!.listSchemas(snapshotId)
        assertThat(schemas)
            .`as`(
                "ATTACH 'ducklake:quack:...' must auto-bootstrap the metadata schema and " +
                    "create the default 'main' DuckLake schema visible to listSchemas"
            )
            .extracting(java.util.function.Function<DucklakeSchema, String> { it.schemaName() })
            .contains("main")
    }

    @Test
    fun createSchemaCommitsAndListSchemasSeesIt() {
        val name = "smoke_created_schema"
        catalog!!.createSchema(name)

        val snapshotId = catalog!!.currentSnapshotId
        assertThat(catalog!!.listSchemas(snapshotId))
            .extracting(java.util.function.Function<DucklakeSchema, String> { it.schemaName() })
            .contains(name)
    }

    companion object {
        private var server: TestingDucklakeDuckDbQuackCatalogServer? = null
        private var catalog: JdbcDucklakeCatalog? = null
        private var dataDir: Path? = null

        @BeforeAll
        @JvmStatic
        fun setUpClass() {
            server = TestingDucklakeDuckDbQuackCatalogServer()
            dataDir = Files.createTempDirectory("ducklake-quack-jdbc-smoke-")

            val url = "jdbc:duckdb:quack://" + server!!.getHost() + ":" + server!!.getMappedPort() +
                "?metadata_catalog=smoke_meta"
            val config = DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(url)
                .setCatalogDatabasePassword(server!!.getToken())
                .setDataPath(dataDir!!.toAbsolutePath().toString())
                .setMaxCatalogConnections(3)
            catalog = JdbcDucklakeCatalog(config)
        }

        @AfterAll
        @JvmStatic
        fun tearDownClass() {
            catalog?.close()
            server?.close()
            dataDir?.let { deleteRecursively(it) }
        }

        private fun deleteRecursively(dir: Path) {
            if (!Files.exists(dir)) {
                return
            }
            try {
                Files.walk(dir).use { walk ->
                    walk.sorted(Comparator.reverseOrder()).forEach { p ->
                        try {
                            Files.deleteIfExists(p)
                        }
                        catch (ignored: IOException) {
                        }
                    }
                }
            }
            catch (ignored: IOException) {
            }
        }
    }
}
