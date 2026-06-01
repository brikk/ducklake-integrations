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
import java.sql.DriverManager
import java.util.Comparator

/**
 * Architecture-validation probe for the Quack-as-DuckLake-catalog path. Today's
 * `JdbcDucklakeCatalog` (PG-backed) reads/writes the raw `ducklake_*`
 * metadata tables directly via JDBC. We need an equivalent path under Quack.
 *
 * **Finding:** `METADATA_CATALOG '<name>'` on the DuckLake-on-Quack
 * ATTACH publishes the underlying metadata storage as a sibling catalog on the
 * client side. So a single client ATTACH gives us both:
 *
 *  - `lake.<schema>.<table>` — the DuckLake user catalog (high-level
 *    CREATE/INSERT semantics).
 *  - `<metadata_catalog>.main.ducklake_*` — raw access to the metadata
 *    tables, which is what `JdbcDucklakeCatalog` needs.
 *
 * That means `JdbcDucklakeCatalog`'s HikariCP `connectionInitSql` can
 * be a single multi-statement script that installs the extensions, creates the
 * Quack secret, ATTACHes with a METADATA_CATALOG name, and `USE`s that
 * catalog's main schema. No separate bootstrap connection required.
 *
 * `duckdb_databases()` / `SHOW DATABASES` surfaces a "Not
 * implemented Error: InMemory not implemented yet" error on Quack-attached
 * remotes, so don't rely on those to introspect catalog topology — query the
 * specific metadata catalog by name instead.
 */
internal class QuackMetadataAccessProbeTest {

    @Test
    fun singleAttachExposesMetadataCatalogAsSiblingCatalog() {
        val metadataCatalog = "probe_meta"

        DriverManager.getConnection("jdbc:duckdb:").use { client ->
            client.createStatement().use { s ->
                s.execute("INSTALL quack")
                s.execute("LOAD quack")
                s.execute("INSTALL ducklake")
                s.execute("LOAD ducklake")
                s.execute("CREATE SECRET (TYPE quack, TOKEN '" + server!!.getToken() + "')")
                s.execute(
                    "ATTACH '" + server!!.getDucklakeAttachUri() + "' AS lake " +
                        "(DATA_PATH '" + dataDir!!.toAbsolutePath() + "', METADATA_CATALOG '" + metadataCatalog + "')"
                )
                s.execute("USE lake")
                s.execute("CREATE TABLE t (x INTEGER)")
                s.execute("INSERT INTO t VALUES (1), (2)")

                // The DuckLake metadata tables must be queryable via the sibling
                // catalog name set in METADATA_CATALOG. This is the contract
                // JdbcDucklakeCatalog will rely on for raw catalog-table access.
                val rows = ArrayList<String>()
                s.executeQuery(
                    "SELECT table_name FROM " + metadataCatalog + ".main.ducklake_table ORDER BY table_name"
                ).use { rs ->
                    while (rs.next()) {
                        rows.add(rs.getString(1))
                    }
                }
                assertThat(rows).containsExactly("t")

                s.executeQuery(
                    "SELECT schema_name FROM " + metadataCatalog + ".main.ducklake_schema " +
                        "WHERE schema_name = 'main'"
                ).use { rs ->
                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getString(1)).isEqualTo("main")
                }
            }
        }
    }

    companion object {
        private var server: TestingDucklakeDuckDbQuackCatalogServer? = null
        private var dataDir: Path? = null

        @BeforeAll
        @JvmStatic
        fun setUp() {
            server = TestingDucklakeDuckDbQuackCatalogServer()
            dataDir = Files.createTempDirectory("ducklake-quack-meta-probe-")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
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
