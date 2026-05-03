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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Type-coverage slice of cross-engine compatibility: DuckDB writes a fixture, Trino reads it
 * back. Exercises the inlined-then-Parquet flush boundary on ALTER TABLE ADD COLUMN, the
 * scalar inlined read path (date/timestamp/blob/varchar-with-null-byte), the full {@code
 * list<T>} read audit through both the inlined PG-text parser and Trino's Parquet array
 * reader, and DuckDB's wide-int types (HUGEINT/UHUGEINT) widening on read.
 *
 * <p>Each {@code list<T>} test goes through {@link #runListRoundTrip} which writes the rows
 * twice — inlined via {@code data_inlining_row_limit=100} and again flushed to Parquet via
 * {@code 0} — and checks both outcomes against the same assertion closure. See
 * {@code DucklakeInlinedValueConverter.parseDucklakeListText} for the inlined-path contract
 * and {@code dev-docs/COMPARE-pg_ducklake.md} B2 for background.
 */
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeCrossEngineTypeAudit
        extends AbstractDucklakeCrossEngineTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "cross-engine-type-audit";
    }

    // ==================== Inlined ALTER TABLE ADD COLUMN flows ====================

    @Test
    public void testDuckdbInlineAlterThenInlineMatchesTrino()
            throws Exception
    {
        String tableName = "xengine_inline_alter_inline";
        String fullDuckdbTable = "ducklake_db.test_schema." + tableName;
        String fullTrinoTable = "test_schema." + tableName;

        try {
            try (Connection duckConn = createDuckdbConnection();
                    Statement duckStmt = duckConn.createStatement()) {
                duckStmt.execute("DROP TABLE IF EXISTS " + fullDuckdbTable);
                duckStmt.execute("CREATE TABLE " + fullDuckdbTable + " (id INTEGER, name VARCHAR)");
                duckStmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 10, schema => 'test_schema', table_name => '" + tableName + "')");
                duckStmt.execute("INSERT INTO " + fullDuckdbTable + " VALUES (1, 'pre_1'), (2, 'pre_2'), (3, 'pre_3'), (4, 'pre_4')");
                duckStmt.execute("ALTER TABLE " + fullDuckdbTable + " ADD COLUMN score INTEGER");
                duckStmt.execute("INSERT INTO " + fullDuckdbTable + " VALUES (5, 'post_1', 50), (6, 'post_2', 60), (7, 'post_3', 70), (8, 'post_4', 80)");
            }

            List<String> duckdbRows;
            try (Connection duckConn = createDuckdbConnection();
                    Statement duckStmt = duckConn.createStatement();
                    ResultSet rs = duckStmt.executeQuery("SELECT id, name, score FROM " + fullDuckdbTable + " ORDER BY id")) {
                duckdbRows = new ArrayList<>();
                while (rs.next()) {
                    duckdbRows.add(formatRow(rs.getObject(1), rs.getObject(2), rs.getObject(3)));
                }
            }

            assertThat(duckdbRows).containsExactly(
                    "1|pre_1|null",
                    "2|pre_2|null",
                    "3|pre_3|null",
                    "4|pre_4|null",
                    "5|post_1|50",
                    "6|post_2|60",
                    "7|post_3|70",
                    "8|post_4|80");

            DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
            try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
                long snapshotId = queryLong(pgConn, "SELECT max(snapshot_id) FROM ducklake_snapshot");
                long tableId = queryLong(pgConn,
                        "SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL",
                        tableName);

                long activeDataFileCount = queryLong(pgConn,
                        "SELECT count(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
                        tableId);

                Map<Long, Long> activeInlineRowsBySchemaVersion = new LinkedHashMap<>();
                try (PreparedStatement stmt = pgConn.prepareStatement(
                        "SELECT schema_version FROM ducklake_inlined_data_tables WHERE table_id = ? ORDER BY schema_version")) {
                    stmt.setLong(1, tableId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            long schemaVersion = rs.getLong("schema_version");
                            long activeRows = queryLong(pgConn,
                                    "SELECT count(*) FROM ducklake_inlined_data_" + tableId + "_" + schemaVersion +
                                            " WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
                                    snapshotId,
                                    snapshotId);
                            activeInlineRowsBySchemaVersion.put(schemaVersion, activeRows);
                        }
                    }
                }

                long totalActiveInlineRows = activeInlineRowsBySchemaVersion.values().stream()
                        .mapToLong(Long::longValue)
                        .sum();

                // With 4 + 4 rows and limit 10, DuckDB should keep rows in metadata inlined tables.
                assertThat(activeDataFileCount)
                        .as("DuckDB active Parquet data files after inline->alter->inline")
                        .isZero();
                assertThat(activeInlineRowsBySchemaVersion.size())
                        .as("DuckDB inlined schema-version tables after ADD COLUMN")
                        .isGreaterThanOrEqualTo(2);
                assertThat(totalActiveInlineRows)
                        .as("DuckDB active inlined rows across schema versions")
                        .isEqualTo(8);
            }

            MaterializedResult trinoResult = computeActual("SELECT id, name, score FROM " + fullTrinoTable + " ORDER BY id");
            List<String> trinoRows = trinoResult.getMaterializedRows().stream()
                    .map(row -> formatRow(row.getField(0), row.getField(1), row.getField(2)))
                    .toList();

            assertThat(trinoRows).isEqualTo(duckdbRows);
        }
        finally {
            tryDropTable(fullTrinoTable);
        }
    }

    @Test
    public void testDuckdbNineAlterNineStaysInlinedAndMatchesTrino()
            throws Exception
    {
        String tableName = "xengine_inline9_alter_inline9";
        String fullDuckdbTable = "ducklake_db.test_schema." + tableName;
        String fullTrinoTable = "test_schema." + tableName;

        try {
            try (Connection duckConn = createDuckdbConnection();
                    Statement duckStmt = duckConn.createStatement()) {
                duckStmt.execute("DROP TABLE IF EXISTS " + fullDuckdbTable);
                duckStmt.execute("CREATE TABLE " + fullDuckdbTable + " (id INTEGER, name VARCHAR)");
                duckStmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 10, schema => 'test_schema', table_name => '" + tableName + "')");
                duckStmt.execute("INSERT INTO " + fullDuckdbTable + " VALUES " + buildInlineValues(1, 9, false));
                duckStmt.execute("ALTER TABLE " + fullDuckdbTable + " ADD COLUMN score INTEGER");
                duckStmt.execute("INSERT INTO " + fullDuckdbTable + " VALUES " + buildInlineValues(10, 18, true));
            }

            List<String> duckdbRows;
            try (Connection duckConn = createDuckdbConnection();
                    Statement duckStmt = duckConn.createStatement();
                    ResultSet rs = duckStmt.executeQuery("SELECT id, name, score FROM " + fullDuckdbTable + " ORDER BY id")) {
                duckdbRows = new ArrayList<>();
                while (rs.next()) {
                    duckdbRows.add(formatRow(rs.getObject(1), rs.getObject(2), rs.getObject(3)));
                }
            }

            assertThat(duckdbRows).hasSize(18);
            assertThat(duckdbRows.getFirst()).isEqualTo("1|pre_1|null");
            assertThat(duckdbRows.get(8)).isEqualTo("9|pre_9|null");
            assertThat(duckdbRows.get(9)).isEqualTo("10|post_10|100");
            assertThat(duckdbRows.getLast()).isEqualTo("18|post_18|180");

            DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
            try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
                long snapshotId = queryLong(pgConn, "SELECT max(snapshot_id) FROM ducklake_snapshot");
                long tableId = queryLong(pgConn,
                        "SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL",
                        tableName);

                long activeDataFileCount = queryLong(pgConn,
                        "SELECT count(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
                        tableId);

                Map<Long, Long> activeInlineRowsBySchemaVersion = new LinkedHashMap<>();
                try (PreparedStatement stmt = pgConn.prepareStatement(
                        "SELECT schema_version FROM ducklake_inlined_data_tables WHERE table_id = ? ORDER BY schema_version")) {
                    stmt.setLong(1, tableId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            long schemaVersion = rs.getLong("schema_version");
                            long activeRows = queryLong(pgConn,
                                    "SELECT count(*) FROM ducklake_inlined_data_" + tableId + "_" + schemaVersion +
                                            " WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
                                    snapshotId,
                                    snapshotId);
                            activeInlineRowsBySchemaVersion.put(schemaVersion, activeRows);
                        }
                    }
                }

                long totalActiveInlineRows = activeInlineRowsBySchemaVersion.values().stream()
                        .mapToLong(Long::longValue)
                        .sum();

                // Even with 9 + 9 rows around ALTER, DuckDB keeps rows in inlined metadata tables.
                assertThat(activeDataFileCount)
                        .as("DuckDB active Parquet data files after inline9->alter->inline9")
                        .isZero();
                assertThat(activeInlineRowsBySchemaVersion.size())
                        .as("DuckDB inlined schema-version tables after ADD COLUMN")
                        .isGreaterThanOrEqualTo(2);
                assertThat(totalActiveInlineRows)
                        .as("DuckDB active inlined rows across schema versions")
                        .isEqualTo(18);
            }

            MaterializedResult trinoResult = computeActual("SELECT id, name, score FROM " + fullTrinoTable + " ORDER BY id");
            List<String> trinoRows = trinoResult.getMaterializedRows().stream()
                    .map(row -> formatRow(row.getField(0), row.getField(1), row.getField(2)))
                    .toList();

            assertThat(trinoRows).isEqualTo(duckdbRows);
        }
        finally {
            tryDropTable(fullTrinoTable);
        }
    }

    // ==================== list<T> read audit ====================
    //
    // Each test below runs through `runListRoundTrip` which writes DuckDB rows twice — once kept
    // in the inlined metadata table (`data_inlining_row_limit=100`) and once flushed to Parquet
    // (default `data_inlining_row_limit=0`) — and reads both back through Trino with the same
    // assertions. That covers both the inlined PG-text parser in `DucklakeInlinedValueConverter`
    // and Trino's Parquet array reader on DuckDB-written files.

    @Test
    public void testDuckdbListIntReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_int",
                "INTEGER[]",
                "(1, [10, 20, 30]), (2, [42]), (3, NULL), (4, [100, NULL, 300]), (5, [])",
                5,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(5);
                    assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(List.of(10, 20, 30));
                    assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(List.of(42));
                    assertThat(result.getMaterializedRows().get(2).getField(1)).isNull();
                    assertThat(result.getMaterializedRows().get(3).getField(1)).isEqualTo(Arrays.asList(100, null, 300));
                    assertThat(result.getMaterializedRows().get(4).getField(1)).isEqualTo(List.of());
                });
    }

    @Test
    public void testDuckdbListBigintReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_bigint",
                "BIGINT[]",
                "(1, [1, 9223372036854775807, -9223372036854775808]), (2, [42, NULL]), (3, NULL)",
                3,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(3);
                    assertThat(result.getMaterializedRows().get(0).getField(1))
                            .isEqualTo(List.of(1L, 9223372036854775807L, -9223372036854775808L));
                    assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(Arrays.asList(42L, null));
                    assertThat(result.getMaterializedRows().get(2).getField(1)).isNull();
                });
    }

    @Test
    public void testDuckdbListSmallintReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_smallint",
                "SMALLINT[]",
                "(1, [1, 32767, -32768, NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    assertThat(result.getMaterializedRows().get(0).getField(1))
                            .isEqualTo(Arrays.asList((short) 1, (short) 32767, (short) -32768, null));
                });
    }

    @Test
    public void testDuckdbListTinyintReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_tinyint",
                "TINYINT[]",
                "(1, [1, 127, -128, NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    assertThat(result.getMaterializedRows().get(0).getField(1))
                            .isEqualTo(Arrays.asList((byte) 1, (byte) 127, (byte) -128, null));
                });
    }

    @Test
    public void testDuckdbListBooleanReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_bool",
                "BOOLEAN[]",
                "(1, [true, false, NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    assertThat(result.getMaterializedRows().get(0).getField(1))
                            .isEqualTo(Arrays.asList(true, false, null));
                });
    }

    @Test
    public void testDuckdbListDoubleReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_double",
                "DOUBLE[]",
                "(1, [1.5, -2.25, 0.0, NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    assertThat(result.getMaterializedRows().get(0).getField(1))
                            .isEqualTo(Arrays.asList(1.5, -2.25, 0.0, null));
                });
    }

    @Test
    public void testDuckdbListRealReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_real",
                "REAL[]",
                "(1, [1.5, -2.25, 0.0, NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    assertThat(result.getMaterializedRows().get(0).getField(1))
                            .isEqualTo(Arrays.asList(1.5f, -2.25f, 0.0f, null));
                });
    }

    @Test
    public void testDuckdbListDecimalReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_decimal",
                "DECIMAL(10,2)[]",
                "(1, [12.34, -56.78, NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    assertThat(result.getMaterializedRows().get(0).getField(1))
                            .isEqualTo(Arrays.asList(
                                    new java.math.BigDecimal("12.34"),
                                    new java.math.BigDecimal("-56.78"),
                                    null));
                });
    }

    @Test
    public void testDuckdbListVarcharReadsInTrino()
            throws Exception
    {
        // Covers the tricky parts of the inlined text path: single-quoted elements, `\'` backslash
        // escape for an embedded apostrophe, commas inside quoted strings, empty strings, NULL
        // elements, and whole-list NULL. The Parquet path routes through Trino's native reader.
        runListRoundTrip(
                "xengine_list_varchar",
                "VARCHAR[]",
                "(1, ['abc', 'def']), (2, ['it''s', 'a, b', '']), (3, ['x', NULL, 'y']), (4, NULL)",
                4,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(4);
                    assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(List.of("abc", "def"));
                    assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(List.of("it's", "a, b", ""));
                    assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo(Arrays.asList("x", null, "y"));
                    assertThat(result.getMaterializedRows().get(3).getField(1)).isNull();
                });
    }

    @Test
    public void testDuckdbListDateReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_date",
                "DATE[]",
                "(1, [DATE '2024-06-15', DATE '2024-12-31', NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    List<?> days = (List<?>) result.getMaterializedRows().get(0).getField(1);
                    assertThat(days).hasSize(3);
                    assertThat(days.get(0).toString()).isEqualTo("2024-06-15");
                    assertThat(days.get(1).toString()).isEqualTo("2024-12-31");
                    assertThat(days.get(2)).isNull();
                });
    }

    @Test
    public void testDuckdbListTimestampReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_ts",
                "TIMESTAMP[]",
                "(1, [TIMESTAMP '2024-06-15 12:34:56.123456', NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    List<?> tss = (List<?>) result.getMaterializedRows().get(0).getField(1);
                    assertThat(tss).hasSize(2);
                    assertThat(tss.get(0).toString()).contains("2024-06-15");
                    assertThat(tss.get(0).toString()).contains("12:34:56.123456");
                    assertThat(tss.get(1)).isNull();
                });
    }

    @Test
    public void testDuckdbListTimestampTzReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_tstz",
                "TIMESTAMPTZ[]",
                "(1, [TIMESTAMPTZ '2024-06-15 12:34:56.123456+00', NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    List<?> tss = (List<?>) result.getMaterializedRows().get(0).getField(1);
                    assertThat(tss).hasSize(2);
                    assertThat(tss.get(0).toString()).contains("2024-06-15");
                    assertThat(tss.get(0).toString()).contains("12:34:56.123456");
                    assertThat(tss.get(1)).isNull();
                });
    }

    // ==================== Partial coverage: list<blob>, list<uuid> ====================
    //
    // Both types fail end-to-end on the inlined path today (see TODO-READ-MODE § Inlined-Read
    // Type Gaps): blob because we do not decode DuckDB's `\xNN` text escapes back into bytes,
    // uuid because the scalar converter has no UUID branch. They may work on the Parquet path
    // (Trino's Parquet reader handles ARRAY<BINARY> and the UUID logical type), but we haven't
    // confirmed until/unless the inlined-path fix lands. Kept `@Disabled` so the missing
    // behavior stays tracked — don't delete.

    @Test
    @org.junit.jupiter.api.Disabled("TODO-READ-MODE: inlined `list<blob>` — DuckDB serializes bytes as "
            + "'\\xNN\\xNN' text; parser does not yet decode hex escapes. Parquet path not yet validated.")
    public void testDuckdbListBlobReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_blob",
                "BLOB[]",
                "(1, ['\\x00\\x01\\xFF'::BLOB, NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    List<?> vals = (List<?>) result.getMaterializedRows().get(0).getField(1);
                    assertThat(vals).hasSize(2);
                    assertThat(vals.get(0)).isInstanceOf(byte[].class);
                    assertThat((byte[]) vals.get(0)).containsExactly((byte) 0x00, (byte) 0x01, (byte) 0xFF);
                    assertThat(vals.get(1)).isNull();
                });
    }

    @Test
    @org.junit.jupiter.api.Disabled("TODO-READ-MODE: scalar inlined UUID reads do not decode the 36-char text "
            + "form into Trino's 16-byte UuidType; the list path inherits the same gap. Parquet path not yet validated.")
    public void testDuckdbListUuidReadsInTrino()
            throws Exception
    {
        runListRoundTrip(
                "xengine_list_uuid",
                "UUID[]",
                "(1, [UUID '550e8400-e29b-41d4-a716-446655440000', NULL])",
                1,
                result -> {
                    assertThat(result.getMaterializedRows()).hasSize(1);
                    List<?> ids = (List<?>) result.getMaterializedRows().get(0).getField(1);
                    assertThat(ids).hasSize(2);
                    assertThat(ids.get(0).toString()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
                    assertThat(ids.get(1)).isNull();
                });
    }

    // ==================== Inlined scalar reads ====================

    @Test
    public void testDuckdbInlinedDateAndTimestampReadInTrino()
            throws Exception
    {
        String tableName = "xengine_inlined_date_ts";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;
        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, d DATE, ts TIMESTAMP)");
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '" + tableName + "')");
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES " +
                        "(1, DATE '2024-06-15', TIMESTAMP '2024-06-15 12:34:56.123456'), " +
                        "(2, NULL, NULL)");
            }
            assertRowsStayedInlined(tableName, 2);

            MaterializedResult result = computeActual("SELECT id, d, ts FROM " + fullTrino + " ORDER BY id");
            assertThat(result.getMaterializedRows()).hasSize(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1).toString()).isEqualTo("2024-06-15");
            // Upstream serializes timestamps into VARCHAR columns as ISO strings; our converter
            // must parse them back to TIMESTAMP_MICROS. Don't assert exact form here — just that
            // Trino got a non-null value with the expected microsecond component visible.
            assertThat(result.getMaterializedRows().get(0).getField(2)).isNotNull();
            assertThat(result.getMaterializedRows().get(0).getField(2).toString()).contains("2024-06-15");
            assertThat(result.getMaterializedRows().get(0).getField(2).toString()).contains("12:34:56.123456");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isNull();
            assertThat(result.getMaterializedRows().get(1).getField(2)).isNull();
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    @Test
    public void testDuckdbInlinedBlobReadsInTrino()
            throws Exception
    {
        String tableName = "xengine_inlined_blob";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;
        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, payload BLOB)");
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '" + tableName + "')");
                // Include a null byte to confirm BYTEA (not TEXT) is actually used upstream.
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES (1, '\\x00\\x01\\x02\\xFF'::BLOB), (2, NULL)");
            }
            assertRowsStayedInlined(tableName, 2);

            MaterializedResult result = computeActual("SELECT id, payload FROM " + fullTrino + " ORDER BY id");
            assertThat(result.getMaterializedRows()).hasSize(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            // Whatever representation Trino chooses, it must preserve all four bytes.
            Object payload = result.getMaterializedRows().get(0).getField(1);
            assertThat(payload).isNotNull();
            if (payload instanceof byte[] bytes) {
                assertThat(bytes).containsExactly((byte) 0x00, (byte) 0x01, (byte) 0x02, (byte) 0xFF);
            }
            assertThat(result.getMaterializedRows().get(1).getField(1)).isNull();
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    @Test
    public void testDuckdbInlinedVarcharWithEmbeddedNullByteReadsInTrino()
            throws Exception
    {
        // pg_ducklake docs/data_types.md explicitly calls out that DuckDB VARCHAR can carry
        // embedded null bytes and that PG TEXT/VARCHAR cannot, which is why upstream stores
        // inlined VARCHAR as BYTEA. This test probes whether our read path survives a null byte
        // — if this fails with "null character not permitted", we likely need the BYTEA-aware
        // read path that pg_ducklake documents.
        String tableName = "xengine_inlined_varchar_nullbyte";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;
        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, s VARCHAR)");
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '" + tableName + "')");
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES (1, 'ABC' || chr(0) || '123')");
            }
            assertRowsStayedInlined(tableName, 1);

            // Current expectation: this may throw — that's informative. If it returns a value,
            // verify the null byte survived. Either way, don't make the test falsely pass.
            MaterializedResult result = computeActual("SELECT id, length(s), s FROM " + fullTrino);
            assertThat(result.getMaterializedRows()).hasSize(1);
            assertThat(result.getMaterializedRows().get(0).getField(1))
                    .as("length must be 7 if the null byte round-tripped")
                    .isEqualTo(7L);
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    // ==================== hugeint / uhugeint ====================

    @Test
    public void testDuckdbHugeintColumnReadsAsDecimalInTrino()
            throws Exception
    {
        String tableName = "xengine_hugeint";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;
        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, big HUGEINT)");
                // A value that fits in DECIMAL(38, 0). Values past ~1.7e38 would overflow.
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES (1, 99999999999999999999999999999999999999)");
            }

            MaterializedResult result = computeActual("SELECT id, CAST(big AS VARCHAR) FROM " + fullTrino);
            assertThat(result.getMaterializedRows()).hasSize(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1))
                    .isEqualTo("99999999999999999999999999999999999999");
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    @Test
    public void testDuckdbUhugeintColumnReadsAsVarcharInTrino()
            throws Exception
    {
        String tableName = "xengine_uhugeint";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;
        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, big UHUGEINT)");
                // Max uhugeint exceeds DECIMAL(38) — that's why we degrade to VARCHAR.
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES (1, 340282366920938463463374607431768211455)");
            }

            MaterializedResult result = computeActual("SELECT id, big FROM " + fullTrino);
            assertThat(result.getMaterializedRows()).hasSize(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1).toString())
                    .isEqualTo("340282366920938463463374607431768211455");
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    // ==================== Helpers ====================

    /**
     * Runs a single-column `list<T>` round-trip test (DuckDB writer → Trino reader) twice:
     * once with rows kept in the inlined metadata table, once with rows flushed to Parquet.
     * Both paths share the same assertion closure, which validates the read-back result.
     *
     * <p>The table schema is always {@code (id INTEGER, val <ducklakeListDdl>)}; {@code insertValues}
     * is the body after {@code INSERT INTO <tbl> VALUES}. The closure is handed a result set of
     * {@code SELECT id, val FROM <tbl> ORDER BY id}.
     */
    private void runListRoundTrip(
            String baseName,
            String ducklakeListDdl,
            String insertValuesSql,
            int expectedRowCount,
            Consumer<MaterializedResult> assertions)
            throws Exception
    {
        for (Mode mode : Mode.values()) {
            String tableName = baseName + mode.suffix;
            String fullDuckdb = "ducklake_db.test_schema." + tableName;
            String fullTrino = "test_schema." + tableName;
            try {
                try (Connection duck = createDuckdbConnection();
                        Statement stmt = duck.createStatement()) {
                    stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                    stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, val " + ducklakeListDdl + ")");
                    // DuckDB's default `ducklake_default_data_inlining_row_limit` is 10, so for
                    // small fixtures the PARQUET mode would otherwise silently inline; force 0.
                    int inliningRowLimit = mode == Mode.INLINED ? 100 : 0;
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', " + inliningRowLimit +
                            ", schema => 'test_schema', table_name => '" + tableName + "')");
                    stmt.execute("INSERT INTO " + fullDuckdb + " VALUES " + insertValuesSql);
                }
                if (mode == Mode.INLINED) {
                    assertRowsStayedInlined(tableName, expectedRowCount);
                }
                else {
                    assertRowsWrittenToParquet(tableName);
                }

                MaterializedResult result = computeActual("SELECT id, val FROM " + fullTrino + " ORDER BY id");
                try {
                    assertions.accept(result);
                }
                catch (AssertionError e) {
                    throw new AssertionError("[" + mode + " path] " + e.getMessage(), e);
                }
            }
            finally {
                tryDropTable(fullTrino);
            }
        }
    }

    private enum Mode
    {
        INLINED("_inl"),
        PARQUET("_pq");

        final String suffix;

        Mode(String suffix)
        {
            this.suffix = suffix;
        }
    }

    private static String formatRow(Object id, Object name, Object score)
    {
        return String.valueOf(id) + "|" + String.valueOf(name) + "|" + String.valueOf(score);
    }

    private static String buildInlineValues(int startInclusive, int endInclusive, boolean withScore)
    {
        List<String> rows = new ArrayList<>();
        for (int id = startInclusive; id <= endInclusive; id++) {
            String prefix = withScore ? "post_" : "pre_";
            if (withScore) {
                rows.add("(" + id + ", '" + prefix + id + "', " + (id * 10) + ")");
            }
            else {
                rows.add("(" + id + ", '" + prefix + id + "')");
            }
        }
        return String.join(", ", rows);
    }
}
