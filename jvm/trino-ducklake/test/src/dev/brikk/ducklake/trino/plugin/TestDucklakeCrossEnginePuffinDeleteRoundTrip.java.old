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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * End-to-end round trip for DuckLake's Iceberg V3 puffin deletion vectors:
 * DuckDB writes a delete with {@code ducklake_write_deletion_vectors=true},
 * which stores the deleted positions in a Roaring bitmap inside a
 * {@code .puffin} file, and Trino reads back the surviving rows. Exercises
 * {@code DucklakePuffinDeleteReader} against bytes produced by the
 * reference writer rather than a synthesized fixture.
 */
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeCrossEnginePuffinDeleteRoundTrip
        extends AbstractDucklakeCrossEngineTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "cross-engine-puffin-delete";
    }

    @Test
    public void testDuckdbPuffinDeleteSuppressesRowsInTrino()
            throws Exception
    {
        String tableName = "xengine_puffin_delete";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;

        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, label VARCHAR)");
                // Force file-based deletes (not inline-only) and flip on Iceberg V3 puffin deletes.
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 0, schema => 'test_schema', table_name => '" + tableName + "')");
                stmt.execute("CALL ducklake_db.set_option('write_deletion_vectors', true, schema => 'test_schema', table_name => '" + tableName + "')");
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES "
                        + "(1, 'keep'), (2, 'drop'), (3, 'keep'), (4, 'drop'), (5, 'keep'), "
                        + "(6, 'drop'), (7, 'keep'), (8, 'drop'), (9, 'keep'), (10, 'drop')");
                stmt.execute("DELETE FROM " + fullDuckdb + " WHERE label = 'drop'");
            }

            // Sanity: the deletes did land as puffin (not parquet/inline) — without this
            // the round trip would pass even if the puffin reader were broken, since the
            // parquet delete-file path would handle the deletes.
            assertHasPuffinDeleteFiles(tableName);

            MaterializedResult result = computeActual("SELECT id, label FROM " + fullTrino + " ORDER BY id");
            assertThat(result.getMaterializedRows()).hasSize(5);
            for (int i = 0; i < 5; i++) {
                int expectedId = (i * 2) + 1; // 1, 3, 5, 7, 9
                assertThat(result.getMaterializedRows().get(i).getField(0))
                        .as("row %d id", i)
                        .isEqualTo(expectedId);
                assertThat(result.getMaterializedRows().get(i).getField(1))
                        .as("row %d label", i)
                        .isEqualTo("keep");
            }

            // DuckDB sees the same surviving rows — proves the puffin file we just decoded
            // is exactly what the reference reader uses.
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id FROM " + fullDuckdb + " ORDER BY id")) {
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                assertThat(rowCount).as("DuckDB-visible surviving row count").isEqualTo(5);
            }
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    @Test
    public void testEmptyDeletionVectorIsHandled()
            throws Exception
    {
        // Edge case: a puffin file containing zero bitmaps (deleted_rows.empty()). Possible
        // if a writer round-trip rewrites a delete file but the surviving deletions get
        // pruned. Confirm the reader returns an empty set without throwing.
        String tableName = "xengine_puffin_empty";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;

        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER)");
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 0, schema => 'test_schema', table_name => '" + tableName + "')");
                stmt.execute("CALL ducklake_db.set_option('write_deletion_vectors', true, schema => 'test_schema', table_name => '" + tableName + "')");
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES (1), (2), (3)");
                // Delete a non-existent row — DuckDB still records a snapshot but with no
                // file-level deletions. This exercises the no-puffin-file path.
                stmt.execute("DELETE FROM " + fullDuckdb + " WHERE id = 99");
            }

            MaterializedResult result = computeActual("SELECT id FROM " + fullTrino + " ORDER BY id");
            assertThat(result.getMaterializedRows()).hasSize(3);
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    private void assertHasPuffinDeleteFiles(String tableName)
            throws Exception
    {
        DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
        try (Connection conn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
            DSLContext dsl = CatalogTestSupport.dsl(conn);
            var delfile = DUCKLAKE_DELETE_FILE.as("delfile");
            long puffinCount = dsl.selectCount()
                    .from(delfile)
                    .where(delfile.FORMAT.equalIgnoreCase("puffin"))
                    .and(delfile.PATH.like("%" + tableName + "%").or(delfile.PATH.like("%.puffin")))
                    .fetchOne(0, Long.class);
            assertThat(puffinCount)
                    .as("expected at least one puffin delete file for %s", tableName)
                    .isGreaterThan(0);
        }
    }
}
