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
package dev.brikk.ducklake.trino.plugin;

import dev.brikk.ducklake.catalog.testing.CatalogTestSupport;
import io.trino.testing.MaterializedResult;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.stream.Stream;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE;
import static dev.brikk.ducklake.catalog.testing.CatalogPredicates.activeTableNamed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Cross-engine acceptance for the {@code location} table property:
 * a Trino-created table with an explicit {@code WITH (location = '...')}
 * lands the path verbatim in {@code ducklake_table.path} (with
 * {@code path_is_relative} set per scheme detection), the writer drops
 * data files at the requested location, and DuckDB reads the same rows
 * back from the shared catalog.
 */
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeCrossEngineTableLocation
        extends AbstractDucklakeCrossEngineTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "cross-engine-table-location";
    }

    @Test
    public void testRelativeLocationRoundTrip()
            throws Exception
    {
        String tableName = "rel_loc_tbl";
        String fullTrino = "test_schema." + tableName;

        try {
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER, name VARCHAR) "
                    + "WITH (location = 'custom_rel_dir/')");
            computeActual("INSERT INTO " + fullTrino + " VALUES (1, 'alice'), (2, 'bob')");

            // Catalog row holds the requested path + relative flag verbatim.
            DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
            try (Connection conn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
                DSLContext dsl = CatalogTestSupport.dsl(conn);
                var tab = DUCKLAKE_TABLE.as("tab");
                var row = dsl.select(tab.PATH, tab.PATH_IS_RELATIVE)
                        .from(tab)
                        .where(activeTableNamed(tab, tableName))
                        .fetchOne();
                assertThat(row).as("ducklake_table row for %s", tableName).isNotNull();
                assertThat(row.value1()).isEqualTo("custom_rel_dir/");
                assertThat(row.value2()).isTrue();
            }

            // Data files landed under the relative directory under the schema path.
            Path tableDir = getIsolatedCatalog().dataDir()
                    .resolve("test_schema")
                    .resolve("custom_rel_dir");
            assertThat(tableDir).exists();
            try (Stream<Path> files = Files.list(tableDir)) {
                assertThat(files.filter(p -> p.getFileName().toString().endsWith(".parquet"))
                        .findAny())
                        .as("at least one parquet file under %s", tableDir)
                        .isPresent();
            }

            // Round-trip through DuckDB on the same catalog.
            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id, name FROM ducklake_db." + fullTrino + " ORDER BY id")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("alice");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.getString("name")).isEqualTo("bob");
                assertThat(rs.next()).isFalse();
            }
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    @Test
    public void testAbsoluteLocationStoredVerbatimInCatalog()
            throws Exception
    {
        String tableName = "abs_loc_meta_only";
        String fullTrino = "test_schema." + tableName;
        // No INSERT — s3:// isn't backed by a real bucket in this test. The catalog row
        // is the contract we verify; the runtime data path is covered by the relative case.
        String location = "s3://example-bucket/path/to/dir/";

        try {
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER) "
                    + "WITH (location = '" + location + "')");

            DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
            try (Connection conn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
                DSLContext dsl = CatalogTestSupport.dsl(conn);
                var tab = DUCKLAKE_TABLE.as("tab");
                var row = dsl.select(tab.PATH, tab.PATH_IS_RELATIVE)
                        .from(tab)
                        .where(activeTableNamed(tab, tableName))
                        .fetchOne();
                assertThat(row).isNotNull();
                assertThat(row.value1()).isEqualTo(location);
                assertThat(row.value2())
                        .as("URI-scheme prefix should land path_is_relative=false")
                        .isFalse();
            }
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    @Test
    public void testLocationWithMissingTrailingSlashIsNormalized()
            throws Exception
    {
        String tableName = "loc_no_slash";
        String fullTrino = "test_schema." + tableName;

        try {
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER) "
                    + "WITH (location = 'no_slash_dir')");

            DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
            try (Connection conn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
                DSLContext dsl = CatalogTestSupport.dsl(conn);
                var tab = DUCKLAKE_TABLE.as("tab");
                var row = dsl.select(tab.PATH, tab.PATH_IS_RELATIVE)
                        .from(tab)
                        .where(activeTableNamed(tab, tableName))
                        .fetchOne();
                assertThat(row).isNotNull();
                assertThat(row.value1())
                        .as("DuckLake convention: trailing slash appended")
                        .isEqualTo("no_slash_dir/");
                assertThat(row.value2()).isTrue();
            }
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    @Test
    public void testPathTraversalIsRejected()
    {
        assertThatThrownBy(() -> computeActual(
                "CREATE TABLE test_schema.bad_loc (id INTEGER) WITH (location = '../escape/')"))
                .hasMessageContaining("..");
        // No table created.
        MaterializedResult rs = computeActual("SHOW TABLES FROM test_schema LIKE 'bad_loc'");
        assertThat(rs.getRowCount()).isZero();
    }

    @Test
    public void testDefaultLocationFallsBackToTableName()
            throws Exception
    {
        String tableName = "default_loc_tbl";
        String fullTrino = "test_schema." + tableName;

        try {
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER)");

            DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
            try (Connection conn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
                DSLContext dsl = CatalogTestSupport.dsl(conn);
                var tab = DUCKLAKE_TABLE.as("tab");
                var row = dsl.select(tab.PATH, tab.PATH_IS_RELATIVE)
                        .from(tab)
                        .where(activeTableNamed(tab, tableName))
                        .fetchOne();
                assertThat(row).isNotNull();
                assertThat(row.value1())
                        .as("default path matches existing behavior — <tableName>/")
                        .isEqualTo(tableName + "/");
                assertThat(row.value2()).isTrue();
            }
        }
        finally {
            tryDropTable(fullTrino);
        }
    }
}
