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

import dev.brikk.ducklake.catalog.testing.CatalogQueries;
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport;
import io.trino.Session;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.jooq.DSLContext;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_COLUMN_STATS;
import static dev.brikk.ducklake.catalog.testing.CatalogPredicates.currentlyActive;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_WRITER_MODE;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.WRITER_MODE_APPENDER;
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
    public void testColumnStatsWrittenForDuckDbFormat()
            throws Exception
    {
        // Per-column min/max/null_count rows must be persisted in
        // ducklake_column_stats. The DuckLake DuckDB extension's
        // duckdb_tables() / TransformGlobalStats path crashes if any data file
        // has missing stats — so this is a cross-engine compatibility guard,
        // not just a Trino-side optimization.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_stats AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('alpha' AS VARCHAR), 100), " +
                        "  (2, CAST('beta'  AS VARCHAR), 200), " +
                        "  (3, CAST(NULL    AS VARCHAR), 300), " +
                        "  (4, CAST('delta' AS VARCHAR), NULL)" +
                        ") AS t(id, label, amount)");
        try (Connection conn = openCatalogConnection()) {
            DSLContext dsl = CatalogTestSupport.dsl(conn);
            long tableId = CatalogQueries.activeTableId(dsl, "duck_stats");

            // Three-table join: file_column_stats → column (active rows only) → data_file,
            // restricted to the duckdb-format files of the currently-active "duck_stats" table.
            // No canned helper covers the shape; the predicates compose cleanly inline.
            Map<String, StatsRow> rows = new HashMap<>();
            dsl.select(
                            DUCKLAKE_COLUMN.COLUMN_NAME,
                            DUCKLAKE_FILE_COLUMN_STATS.VALUE_COUNT,
                            DUCKLAKE_FILE_COLUMN_STATS.NULL_COUNT,
                            DUCKLAKE_FILE_COLUMN_STATS.MIN_VALUE,
                            DUCKLAKE_FILE_COLUMN_STATS.MAX_VALUE)
                    .from(DUCKLAKE_FILE_COLUMN_STATS)
                    .join(DUCKLAKE_COLUMN)
                            .on(DUCKLAKE_COLUMN.COLUMN_ID.eq(DUCKLAKE_FILE_COLUMN_STATS.COLUMN_ID)
                                    .and(currentlyActive(DUCKLAKE_COLUMN.END_SNAPSHOT)))
                    .join(DUCKLAKE_DATA_FILE)
                            .on(DUCKLAKE_DATA_FILE.DATA_FILE_ID.eq(DUCKLAKE_FILE_COLUMN_STATS.DATA_FILE_ID))
                    .where(DUCKLAKE_DATA_FILE.FILE_FORMAT.eq("duckdb")
                            .and(DUCKLAKE_COLUMN.TABLE_ID.eq(tableId)))
                    .forEach(r -> rows.put(
                            r.get(DUCKLAKE_COLUMN.COLUMN_NAME),
                            new StatsRow(
                                    orZero(r.get(DUCKLAKE_FILE_COLUMN_STATS.VALUE_COUNT)),
                                    orZero(r.get(DUCKLAKE_FILE_COLUMN_STATS.NULL_COUNT)),
                                    r.get(DUCKLAKE_FILE_COLUMN_STATS.MIN_VALUE),
                                    r.get(DUCKLAKE_FILE_COLUMN_STATS.MAX_VALUE))));
            assertThat(rows).containsKeys("id", "label", "amount");

            // id: 1..4, no nulls
            assertThat(rows.get("id").valueCount).isEqualTo(4L);
            assertThat(rows.get("id").nullCount).isEqualTo(0L);
            assertThat(rows.get("id").minValue).isEqualTo("1");
            assertThat(rows.get("id").maxValue).isEqualTo("4");

            // label: alpha/beta/delta + 1 null
            assertThat(rows.get("label").valueCount).isEqualTo(3L);
            assertThat(rows.get("label").nullCount).isEqualTo(1L);
            assertThat(rows.get("label").minValue).isEqualTo("alpha");
            assertThat(rows.get("label").maxValue).isEqualTo("delta");

            // amount: 100/200/300 + 1 null
            assertThat(rows.get("amount").valueCount).isEqualTo(3L);
            assertThat(rows.get("amount").nullCount).isEqualTo(1L);
            assertThat(rows.get("amount").minValue).isEqualTo("100");
            assertThat(rows.get("amount").maxValue).isEqualTo("300");
        }
        finally {
            tryDropTable("test_schema.duck_stats");
        }
    }

    private record StatsRow(long valueCount, long nullCount, String minValue, String maxValue) {}

    private static long orZero(Long value)
    {
        // ducklake_file_column_stats columns are nullable in the schema; the original
        // raw-SQL path used ResultSet#getLong which returns 0 on SQL NULL, and the
        // assertions baked that in. Preserve that behavior here.
        return value == null ? 0L : value;
    }

    @Test
    public void testTablePropertyOverridesSessionToDuckDb()
            throws Exception
    {
        // Default session is parquet. The CREATE TABLE WITH override should still
        // produce a duckdb-format file.
        computeActual("CREATE TABLE test_schema.tp_duckdb (id INTEGER, label VARCHAR) WITH (data_file_format = 'duckdb')");
        try {
            computeActual("INSERT INTO test_schema.tp_duckdb VALUES (1, 'a'), (2, 'b')");
            // INSERTs use the session default (parquet), so this row goes to a parquet file.
            // But CREATE TABLE AS would use the property — let's verify with a CTAS instead.
        }
        finally {
            tryDropTable("test_schema.tp_duckdb");
        }

        // CTAS form: property drives the writer for the materialized rows.
        computeActual("CREATE TABLE test_schema.tp_duckdb_ctas WITH (data_file_format = 'duckdb') AS " +
                "SELECT * FROM (VALUES (1, CAST('alpha' AS VARCHAR)), (2, CAST('beta' AS VARCHAR))) AS t(id, label)");
        try {
            MaterializedResult files = computeActual(
                    "SELECT file_format, record_count FROM \"tp_duckdb_ctas$files\"");
            assertThat(files.getRowCount()).isEqualTo(1);
            assertThat(files.getMaterializedRows().getFirst().getField(0)).isEqualTo("duckdb");
            assertThat(files.getMaterializedRows().getFirst().getField(1)).isEqualTo(2L);
        }
        finally {
            tryDropTable("test_schema.tp_duckdb_ctas");
        }
    }

    @Test
    public void testTablePropertyOverridesSessionToParquet()
    {
        // Session set to duckdb, but the CREATE TABLE WITH override forces parquet.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.tp_parquet_override WITH (data_file_format = 'parquet') AS " +
                        "SELECT 1 AS id, CAST('hello' AS VARCHAR) AS label");
        try {
            MaterializedResult files = computeActual(
                    "SELECT file_format FROM \"tp_parquet_override$files\"");
            assertThat(files.getMaterializedRows().getFirst().getField(0)).isEqualTo("parquet");
        }
        finally {
            tryDropTable("test_schema.tp_parquet_override");
        }
    }

    @Test
    public void testInvalidTablePropertyRejected()
    {
        assertThatThrownBy(() -> computeActual(
                "CREATE TABLE test_schema.tp_bad WITH (data_file_format = 'vortex') AS SELECT 1 AS id"))
                .hasMessageContaining("data_file_format must be one of");
        tryDropTable("test_schema.tp_bad");
    }

    @Test
    public void testPartitionedCtasInDuckDbFormat()
    {
        // CTAS into a duckdb-format table partitioned by an identity column.
        // The page sink routes positions per partition into separate writers — should
        // produce one .db file per partition and the data should round-trip correctly.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_part " +
                        "WITH (partitioned_by = ARRAY['region']) AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('us' AS VARCHAR), 100), " +
                        "  (2, CAST('eu' AS VARCHAR), 200), " +
                        "  (3, CAST('us' AS VARCHAR), 300), " +
                        "  (4, CAST('jp' AS VARCHAR), 400), " +
                        "  (5, CAST('eu' AS VARCHAR), 500)" +
                        ") AS t(id, region, amount)");
        try {
            MaterializedResult files = computeActual(
                    "SELECT path, file_format, record_count FROM \"duck_part$files\" ORDER BY path");
            // Three distinct region values → three .db files.
            assertThat(files.getRowCount()).isEqualTo(3);
            for (MaterializedRow row : files.getMaterializedRows()) {
                String path = (String) row.getField(0);
                assertThat(path).endsWith(".db");
                assertThat(path).matches(".*region=(us|eu|jp)/ducklake-.+\\.db");
                assertThat(row.getField(1)).isEqualTo("duckdb");
            }

            MaterializedResult all = computeActual(
                    "SELECT id, region, amount FROM test_schema.duck_part ORDER BY id");
            assertThat(all.getRowCount()).isEqualTo(5);

            // Predicate on the partition column — should prune to a single .db file.
            MaterializedResult pruned = computeActual(
                    "SELECT id, amount FROM test_schema.duck_part WHERE region = 'us' ORDER BY id");
            assertThat(pruned.getRowCount()).isEqualTo(2);
            assertThat(pruned.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(pruned.getMaterializedRows().get(0).getField(1)).isEqualTo(100);
            assertThat(pruned.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
            assertThat(pruned.getMaterializedRows().get(1).getField(1)).isEqualTo(300);

            // GROUP BY across partitions — exercises both readers in one query.
            MaterializedResult agg = computeActual(
                    "SELECT region, sum(amount) FROM test_schema.duck_part GROUP BY region ORDER BY region");
            assertThat(agg.getRowCount()).isEqualTo(3);
            assertThat(agg.getMaterializedRows().get(0).getField(0)).isEqualTo("eu");
            assertThat(agg.getMaterializedRows().get(0).getField(1)).isEqualTo(700L);
            assertThat(agg.getMaterializedRows().get(1).getField(0)).isEqualTo("jp");
            assertThat(agg.getMaterializedRows().get(1).getField(1)).isEqualTo(400L);
            assertThat(agg.getMaterializedRows().get(2).getField(0)).isEqualTo("us");
            assertThat(agg.getMaterializedRows().get(2).getField(1)).isEqualTo(400L);
        }
        finally {
            tryDropTable("test_schema.duck_part");
        }
    }

    @Test
    public void testUuidRoundTripThroughAppender()
    {
        // The legacy {@code DuckDbFileWriter} (Appender API) supported UUID since
        // Phase 1 (DuckDB JDBC's appender takes a {@code java.util.UUID} natively).
        // The reader, however, had no UUID converter case until the same fix that
        // added it for the arrow_stream writer landed — so even appender-written
        // UUID files were unreadable through Trino. This test pins both halves.
        Session appenderSession = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .setCatalogSessionProperty("ducklake", DUCKDB_WRITER_MODE, WRITER_MODE_APPENDER)
                .build();

        computeActual(appenderSession,
                "CREATE TABLE test_schema.appender_uuids AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('00000000-0000-0000-0000-000000000001' AS UUID)), " +
                        "  (2, CAST('aabbccdd-eeff-0011-2233-445566778899' AS UUID)), " +
                        "  (3, CAST(NULL AS UUID))" +
                        ") AS t(id, u)");
        try {
            MaterializedResult files = computeActual("SELECT file_format FROM \"appender_uuids$files\"");
            assertThat(files.getMaterializedRows().getFirst().getField(0)).isEqualTo("duckdb");

            MaterializedResult result = computeActual(
                    "SELECT id, u FROM test_schema.appender_uuids ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(1).toString())
                    .isEqualTo("00000000-0000-0000-0000-000000000001");
            assertThat(result.getMaterializedRows().get(1).getField(1).toString())
                    .isEqualTo("aabbccdd-eeff-0011-2233-445566778899");
            assertThat(result.getMaterializedRows().get(2).getField(1)).isNull();
        }
        finally {
            tryDropTable("test_schema.appender_uuids");
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
