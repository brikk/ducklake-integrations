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

import io.trino.Session;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 1 Step 2 — DuckDB-format <i>writes</i>.
 *
 * <p>Validates that CTAS / INSERT with {@code data_file_format = 'duckdb'} actually
 * produces a {@code .db} file on disk that DuckDB itself can open and read. Reads
 * back through Trino are not yet implemented (Step 3), so verification goes through
 * the DuckDB JDBC driver directly, opening the artifact the connector wrote.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDuckDbFormatWrite
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "duckdb-format-write";
    }

    private Session duckDbSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build();
    }

    private Path dataDir()
    {
        return Path.of("build", "test-data", "test-catalog-isolated-" + isolatedCatalogName(), "data")
                .toAbsolutePath();
    }

    private Path findDuckDbFileForTable(String relativePath)
            throws Exception
    {
        // The path column on $files is relative to the table data path. We resolve it
        // against the catalog data root by walking — for these isolated tests there
        // are only a handful of files in the directory.
        try (Stream<Path> stream = Files.walk(dataDir())) {
            List<Path> matches = stream
                    .filter(p -> p.toString().endsWith(relativePath))
                    .toList();
            assertThat(matches)
                    .as("data file matching %s under %s", relativePath, dataDir())
                    .isNotEmpty();
            return matches.getFirst();
        }
    }

    @Test
    public void testCtasIntoDuckDbFormatProducesValidDbFile()
            throws Exception
    {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_simple AS " +
                        "SELECT * FROM (VALUES (1, 'alice', true, 3.14), (2, 'bob', false, 2.71)) " +
                        "AS t(id, name, flag, score)");
        try {
            // Catalog metadata reports the new format
            MaterializedResult files = computeActual(
                    "SELECT path, file_format, record_count FROM \"duck_simple$files\"");
            assertThat(files.getRowCount()).isEqualTo(1);
            MaterializedRow row = files.getMaterializedRows().getFirst();
            String relPath = (String) row.getField(0);
            assertThat(relPath).endsWith(".db");
            assertThat(row.getField(1)).isEqualTo("duckdb");
            assertThat(row.getField(2)).isEqualTo(2L);

            // The file is openable by DuckDB and the contents match
            Path absPath = findDuckDbFileForTable(relPath);
            assertThat(Files.size(absPath)).isPositive();

            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement stmt = conn.createStatement()) {
                stmt.execute("ATTACH '" + absPath.toString().replace("'", "''") + "' AS s (READ_ONLY)");
                try (ResultSet rs = stmt.executeQuery("SELECT id, name, flag, score FROM s.t ORDER BY id")) {
                    List<List<Object>> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(List.of(
                                rs.getInt(1),
                                rs.getString(2),
                                rs.getBoolean(3),
                                rs.getDouble(4)));
                    }
                    assertThat(rows).containsExactly(
                            List.of(1, "alice", true, 3.14),
                            List.of(2, "bob", false, 2.71));
                }
                stmt.execute("DETACH s");
            }
        }
        finally {
            tryDropTable("test_schema.duck_simple");
        }
    }

    @Test
    public void testCtasWithNullsRoundTripsThroughDuckDb()
            throws Exception
    {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_nulls AS " +
                        "SELECT * FROM (VALUES (1, CAST('alice' AS VARCHAR), CAST(NULL AS VARCHAR)), " +
                        "(2, CAST(NULL AS VARCHAR), CAST('present' AS VARCHAR))) " +
                        "AS t(id, opt_a, opt_b)");
        try {
            MaterializedResult files = computeActual("SELECT path FROM \"duck_nulls$files\"");
            String relPath = (String) files.getMaterializedRows().getFirst().getField(0);
            Path absPath = findDuckDbFileForTable(relPath);

            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement stmt = conn.createStatement()) {
                stmt.execute("ATTACH '" + absPath.toString().replace("'", "''") + "' AS s (READ_ONLY)");
                try (ResultSet rs = stmt.executeQuery("SELECT id, opt_a, opt_b FROM s.t ORDER BY id")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt(1)).isEqualTo(1);
                    assertThat(rs.getString(2)).isEqualTo("alice");
                    assertThat(rs.getString(3)).isNull();

                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt(1)).isEqualTo(2);
                    assertThat(rs.getString(2)).isNull();
                    assertThat(rs.getString(3)).isEqualTo("present");

                    assertThat(rs.next()).isFalse();
                }
                stmt.execute("DETACH s");
            }
        }
        finally {
            tryDropTable("test_schema.duck_nulls");
        }
    }

    @Test
    public void testInsertAfterParquetCreateWritesDuckDbFile()
            throws Exception
    {
        // Table created without the session prop — initial schema only, no data files.
        computeActual("CREATE TABLE test_schema.duck_insert (id INTEGER, name VARCHAR)");
        try {
            // Insert with the duckdb session — should produce a .db file even though the
            // table was originally created on the default parquet path.
            computeActual(duckDbSession(),
                    "INSERT INTO test_schema.duck_insert VALUES (10, 'x'), (20, 'y'), (30, 'z')");

            MaterializedResult files = computeActual(
                    "SELECT path, file_format, record_count FROM \"duck_insert$files\"");
            assertThat(files.getRowCount()).isEqualTo(1);
            MaterializedRow row = files.getMaterializedRows().getFirst();
            assertThat(row.getField(1)).isEqualTo("duckdb");
            assertThat(row.getField(2)).isEqualTo(3L);

            Path absPath = findDuckDbFileForTable((String) row.getField(0));
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement stmt = conn.createStatement()) {
                stmt.execute("ATTACH '" + absPath.toString().replace("'", "''") + "' AS s (READ_ONLY)");
                try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM s.t")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(3L);
                }
                stmt.execute("DETACH s");
            }
        }
        finally {
            tryDropTable("test_schema.duck_insert");
        }
    }

    @Test
    public void testReadbackThroughTrino()
    {
        // After Step 3 the duckdb-format reader is wired in. SELECT through Trino
        // should round-trip the data we just wrote.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_readback AS SELECT 7 AS id, CAST('hello' AS VARCHAR) AS name");
        try {
            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.duck_readback");
            assertThat(result.getRowCount()).isEqualTo(1);
            MaterializedRow row = result.getMaterializedRows().getFirst();
            assertThat(row.getField(0)).isEqualTo(7);
            assertThat(row.getField(1)).isEqualTo("hello");
        }
        finally {
            tryDropTable("test_schema.duck_readback");
        }
    }

    @Test
    public void testParquetPathUnchangedAtDefaultSession()
    {
        // No session override — parquet baseline.
        computeActual("CREATE TABLE test_schema.duck_parquet_default AS SELECT 1 AS id, 'hello' AS msg");
        try {
            MaterializedResult result = computeActual("SELECT * FROM test_schema.duck_parquet_default");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo("hello");

            MaterializedResult files = computeActual(
                    "SELECT file_format FROM \"duck_parquet_default$files\"");
            assertThat(files.getRowCount()).isEqualTo(1);
            assertThat(files.getMaterializedRows().getFirst().getField(0)).isEqualTo("parquet");
        }
        finally {
            tryDropTable("test_schema.duck_parquet_default");
        }
    }
}
