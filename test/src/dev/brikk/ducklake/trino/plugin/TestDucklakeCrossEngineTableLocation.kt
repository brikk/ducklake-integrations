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

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE
import dev.brikk.ducklake.catalog.testing.CatalogPredicates.activeTableNamed
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.sql.DriverManager

/**
 * Cross-engine acceptance for the `location` table property:
 * a Trino-created table with an explicit `WITH (location = '...')`
 * lands the path verbatim in `ducklake_table.path` (with
 * `path_is_relative` set per scheme detection), the writer drops
 * data files at the requested location, and DuckDB reads the same rows
 * back from the shared catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCrossEngineTableLocation
        : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String {
        return "cross-engine-table-location"
    }

    @Test
    fun testRelativeLocationRoundTrip() {
        val tableName = "rel_loc_tbl"
        val fullTrino = "test_schema.$tableName"

        try {
            computeActual("CREATE TABLE $fullTrino (id INTEGER, name VARCHAR) "
                    + "WITH (location = 'custom_rel_dir/')")
            computeActual("INSERT INTO $fullTrino VALUES (1, 'alice'), (2, 'bob')")

            // Catalog row holds the requested path + relative flag verbatim.
            val catalog = getIsolatedCatalog()
            DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val tab = DUCKLAKE_TABLE.`as`("tab")
                val row = dsl.select(tab.PATH, tab.PATH_IS_RELATIVE)
                        .from(tab)
                        .where(activeTableNamed(tab, tableName))
                        .fetchOne()
                assertThat(row).`as`("ducklake_table row for %s", tableName).isNotNull()
                assertThat(row!!.value1()).isEqualTo("custom_rel_dir/")
                assertThat(row.value2()).isTrue()
            }

            // Data files landed under the relative directory under the schema path.
            val tableDir = getIsolatedCatalog().dataDir
                    .resolve("test_schema")
                    .resolve("custom_rel_dir")
            assertThat(tableDir).exists()
            Files.list(tableDir).use { files ->
                assertThat(files.filter { p -> p.fileName.toString().endsWith(".parquet") }
                        .findAny())
                        .`as`("at least one parquet file under %s", tableDir)
                        .isPresent()
            }

            // Round-trip through DuckDB on the same catalog.
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT id, name FROM ducklake_db.$fullTrino ORDER BY id").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(1)
                        assertThat(rs.getString("name")).isEqualTo("alice")
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(2)
                        assertThat(rs.getString("name")).isEqualTo("bob")
                        assertThat(rs.next()).isFalse()
                    }
                }
            }
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    @Test
    fun testAbsoluteLocationStoredVerbatimInCatalog() {
        val tableName = "abs_loc_meta_only"
        val fullTrino = "test_schema.$tableName"
        // No INSERT — s3:// isn't backed by a real bucket in this test. The catalog row
        // is the contract we verify; the runtime data path is covered by the relative case.
        val location = "s3://example-bucket/path/to/dir/"

        try {
            computeActual("CREATE TABLE $fullTrino (id INTEGER) "
                    + "WITH (location = '$location')")

            val catalog = getIsolatedCatalog()
            DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val tab = DUCKLAKE_TABLE.`as`("tab")
                val row = dsl.select(tab.PATH, tab.PATH_IS_RELATIVE)
                        .from(tab)
                        .where(activeTableNamed(tab, tableName))
                        .fetchOne()
                assertThat(row).isNotNull()
                assertThat(row!!.value1()).isEqualTo(location)
                assertThat(row.value2())
                        .`as`("URI-scheme prefix should land path_is_relative=false")
                        .isFalse()
            }
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    @Test
    fun testLocationWithMissingTrailingSlashIsNormalized() {
        val tableName = "loc_no_slash"
        val fullTrino = "test_schema.$tableName"

        try {
            computeActual("CREATE TABLE $fullTrino (id INTEGER) "
                    + "WITH (location = 'no_slash_dir')")

            val catalog = getIsolatedCatalog()
            DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val tab = DUCKLAKE_TABLE.`as`("tab")
                val row = dsl.select(tab.PATH, tab.PATH_IS_RELATIVE)
                        .from(tab)
                        .where(activeTableNamed(tab, tableName))
                        .fetchOne()
                assertThat(row).isNotNull()
                assertThat(row!!.value1())
                        .`as`("DuckLake convention: trailing slash appended")
                        .isEqualTo("no_slash_dir/")
                assertThat(row.value2()).isTrue()
            }
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    @Test
    fun testPathTraversalIsRejected() {
        assertThatThrownBy {
            computeActual("CREATE TABLE test_schema.bad_loc (id INTEGER) WITH (location = '../escape/')")
        }
                .hasMessageContaining("..")
        // No table created.
        val rs = computeActual("SHOW TABLES FROM test_schema LIKE 'bad_loc'")
        assertThat(rs.rowCount).isZero()
    }

    @Test
    fun testDefaultLocationFallsBackToTableName() {
        val tableName = "default_loc_tbl"
        val fullTrino = "test_schema.$tableName"

        try {
            computeActual("CREATE TABLE $fullTrino (id INTEGER)")

            val catalog = getIsolatedCatalog()
            DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val tab = DUCKLAKE_TABLE.`as`("tab")
                val row = dsl.select(tab.PATH, tab.PATH_IS_RELATIVE)
                        .from(tab)
                        .where(activeTableNamed(tab, tableName))
                        .fetchOne()
                assertThat(row).isNotNull()
                assertThat(row!!.value1())
                        .`as`("default path matches existing behavior — <tableName>/")
                        .isEqualTo("$tableName/")
                assertThat(row.value2()).isTrue()
            }
        }
        finally {
            tryDropTable(fullTrino)
        }
    }
}
