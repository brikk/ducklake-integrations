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

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Cross-engine validation of the {@code add_files} surface: DuckDB writes a raw
 * parquet file outside the table's data directory, Trino registers it with
 * {@code CALL ducklake.system.add_files(...)}, and DuckDB reads the resulting
 * DuckLake table back. Closes the loop on the {@code ducklake_name_mapping} +
 * {@code ducklake_column_mapping} rows our catalog writer produces — DuckDB's
 * reader consults those tables, so an end-to-end DuckDB read proves they're
 * well-formed (cross-engine compatibility, not a Trino-only happy path).
 */
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeAddFilesCrossEngine
        extends AbstractDucklakeCrossEngineTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "cross-engine-add-files";
    }

    @Test
    public void testDuckdbWritesParquetTrinoAddFilesDuckdbReads()
            throws Exception
    {
        // 1. DuckDB writes a parquet file outside any DuckLake-managed directory. The
        // file's column order intentionally differs from the table's column order
        // (parquet has name first, then id) to prove the name_map round-trip.
        Path duckdbOutputDir = getIsolatedCatalog().dataDir().getParent().resolve("add_files_xengine");
        java.nio.file.Files.createDirectories(duckdbOutputDir);
        Path parquetPath = duckdbOutputDir.resolve("rows.parquet").toAbsolutePath();

        try (Connection duckdb = createDuckdbConnection();
                Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "COPY (SELECT 'alice' AS name, 1 AS id UNION ALL SELECT 'bob' AS name, 2 AS id) "
                            + "TO '%s' (FORMAT PARQUET)",
                    parquetPath));
        }

        // 2. Trino creates the destination DuckLake table and registers the file.
        computeActual("CREATE TABLE test_schema.xengine_add_files_dst (id INTEGER, name VARCHAR)");
        try {
            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'xengine_add_files_dst', "
                            + "files => ARRAY['%s'])",
                    parquetPath));

            // 3. Trino reads — sanity check the connector's own side
            MaterializedResult trinoResult = computeActual(
                    "SELECT id, name FROM test_schema.xengine_add_files_dst ORDER BY id");
            assertThat(trinoResult.getRowCount()).isEqualTo(2);
            assertThat(trinoResult.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(trinoResult.getMaterializedRows().get(0).getField(1)).isEqualTo("alice");
            assertThat(trinoResult.getMaterializedRows().get(1).getField(0)).isEqualTo(2);
            assertThat(trinoResult.getMaterializedRows().get(1).getField(1)).isEqualTo("bob");

            // 4. DuckDB reads the catalog-registered table — proves name_map is consumed
            // by upstream DuckLake.
            try (Connection duckdb = createDuckdbConnection();
                    Statement stmt = duckdb.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT id, name FROM ducklake_db.test_schema.xengine_add_files_dst ORDER BY id")) {
                assertThat(rs.next()).as("DuckDB should find row 1").isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("alice");
                assertThat(rs.next()).as("DuckDB should find row 2").isTrue();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.getString("name")).isEqualTo("bob");
                assertThat(rs.next()).as("DuckDB should not find more rows").isFalse();
            }
        }
        finally {
            tryDropTable("test_schema.xengine_add_files_dst");
        }
    }
}
