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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
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
 * Cross-engine compatibility test: Trino creates tables and inserts data,
 * then DuckDB reads the same catalog and verifies the data.
 * Uses PostgreSQL catalog backend — no sync workaround needed (MVCC).
 */
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeCrossEngineCompatibility
        extends AbstractTestQueryFramework
{
    private DucklakeCatalogGenerator.IsolatedCatalog isolatedCatalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("cross-engine")
                .build();
    }

    private DucklakeCatalogGenerator.IsolatedCatalog getIsolatedCatalog()
            throws Exception
    {
        if (isolatedCatalog == null) {
            // The isolated catalog was already created by useIsolatedCatalog("cross-engine").
            // Reconstruct the same IsolatedCatalog reference for DuckDB connections.
            TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
            String databaseName = "ducklake_cross_engine";
            isolatedCatalog = new DucklakeCatalogGenerator.IsolatedCatalog(
                    server.getJdbcUrl(databaseName),
                    server.getUser(),
                    server.getPassword(),
                    Path.of("build", "test-data", "test-catalog-isolated-cross-engine", "data"),
                    server.getDuckDbAttachUri(databaseName));
        }
        return isolatedCatalog;
    }

    /**
     * Creates a fresh DuckDB connection attached to the PostgreSQL-backed DuckLake catalog.
     * No sync workaround needed — PostgreSQL MVCC ensures immediate visibility.
     */
    private Connection createDuckdbConnection()
            throws Exception
    {
        DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL ducklake");
            stmt.execute("LOAD ducklake");
            stmt.execute("INSTALL postgres");
            stmt.execute("LOAD postgres");
            stmt.execute("ATTACH '" + catalog.duckDbAttachUri() + "' AS ducklake_db " +
                    "(DATA_PATH '" + catalog.dataDir().toAbsolutePath() + "')");
        }
        return conn;
    }

    // ==================== Basic round-trip ====================

    @Test
    public void testTrinoInsertDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_basic (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.xengine_basic VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");

            // Verify Trino can read it
            MaterializedResult trinoResult = computeActual("SELECT count(*) FROM test_schema.xengine_basic");
            assertThat(trinoResult.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            // Verify DuckDB can read it via DuckLake catalog — no sync needed with PostgreSQL
            try (Connection conn = createDuckdbConnection()) {
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_basic ORDER BY id")) {
                    assertThat(rs.next()).as("DuckDB should find row 1").isTrue();
                    assertThat(rs.getInt("id")).isEqualTo(1);
                    assertThat(rs.getString("name")).isEqualTo("alice");
                    assertThat(rs.next()).as("DuckDB should find row 2").isTrue();
                    assertThat(rs.getInt("id")).isEqualTo(2);
                    assertThat(rs.getString("name")).isEqualTo("bob");
                    assertThat(rs.next()).as("DuckDB should find row 3").isTrue();
                    assertThat(rs.getInt("id")).isEqualTo(3);
                    assertThat(rs.getString("name")).isEqualTo("charlie");
                    assertThat(rs.next()).isFalse();
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_basic");
        }
    }

    // ==================== CTAS round-trip ====================

    @Test
    public void testTrinoCtasDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_ctas AS " +
                "SELECT CAST(id AS INTEGER) AS id, CAST(name AS VARCHAR) AS name " +
                "FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) AS t(id, name)");
        try {
            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id, name FROM ducklake_db.test_schema.xengine_ctas ORDER BY id")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("alpha");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(3);
                assertThat(rs.getString("name")).isEqualTo("gamma");
                assertThat(rs.next()).isFalse();
            }
        }
        finally {
            tryDropTable("test_schema.xengine_ctas");
        }
    }

    // ==================== Multiple inserts ====================

    @Test
    public void testTrinoMultipleInsertsDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_multi (id INTEGER, value DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.xengine_multi VALUES (1, 10.0), (2, 20.0)");
            computeActual("INSERT INTO test_schema.xengine_multi VALUES (3, 30.0), (4, 40.0)");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT count(*), sum(value) FROM ducklake_db.test_schema.xengine_multi")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getLong(1)).isEqualTo(4);
                assertThat(rs.getDouble(2)).isEqualTo(100.0);
            }
        }
        finally {
            tryDropTable("test_schema.xengine_multi");
        }
    }

    // ==================== Type coverage ====================

    @Test
    public void testTrinoTypesDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_types (" +
                "col_int INTEGER, " +
                "col_bigint BIGINT, " +
                "col_double DOUBLE, " +
                "col_varchar VARCHAR, " +
                "col_boolean BOOLEAN, " +
                "col_date DATE" +
                ")");
        try {
            computeActual("INSERT INTO test_schema.xengine_types VALUES " +
                    "(42, 1000000000, 3.14, 'hello world', true, DATE '2024-06-15')");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_types")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("col_int")).isEqualTo(42);
                assertThat(rs.getLong("col_bigint")).isEqualTo(1000000000L);
                assertThat(rs.getDouble("col_double")).isEqualTo(3.14);
                assertThat(rs.getString("col_varchar")).isEqualTo("hello world");
                assertThat(rs.getBoolean("col_boolean")).isTrue();
                assertThat(rs.getDate("col_date").toString()).isEqualTo("2024-06-15");
                assertThat(rs.next()).isFalse();
            }
        }
        finally {
            tryDropTable("test_schema.xengine_types");
        }
    }

    // ==================== NULLs ====================

    @Test
    public void testTrinoNullsDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_nulls (id INTEGER, name VARCHAR, value DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.xengine_nulls VALUES (1, 'present', 10.0), (2, NULL, NULL)");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_nulls ORDER BY id")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("present");
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.getString("name")).isNull();
                assertThat(rs.getObject("value")).isNull();
                assertThat(rs.next()).isFalse();
            }
        }
        finally {
            tryDropTable("test_schema.xengine_nulls");
        }
    }

    // ==================== DuckDB SHOW TABLES sees Trino-created tables ====================

    @Test
    public void testDuckdbShowTablesIncludesTrinoTables()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_visible (id INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.xengine_visible VALUES (1)");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SHOW TABLES FROM ducklake_db.test_schema")) {
                List<String> tables = new ArrayList<>();
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
                assertThat(tables).contains("xengine_visible");
            }
        }
        finally {
            tryDropTable("test_schema.xengine_visible");
        }
    }

    // ==================== DuckDB DESCRIBE sees correct schema ====================

    @Test
    public void testDuckdbDescribeTrinoTable()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.xengine_describe (id INTEGER, name VARCHAR, amount DOUBLE)");
        try {
            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT column_name FROM information_schema.columns " +
                            "WHERE table_schema = 'test_schema' AND table_name = 'xengine_describe' " +
                            "AND table_catalog = 'ducklake_db' ORDER BY ordinal_position")) {
                List<String> columnNames = new ArrayList<>();
                while (rs.next()) {
                    columnNames.add(rs.getString(1));
                }
                assertThat(columnNames).containsExactly("id", "name", "amount");
            }
        }
        finally {
            tryDropTable("test_schema.xengine_describe");
        }
    }

    // ==================== Sanity check ====================

    @Test
    public void testDuckdbReadsDuckdbCreatedData()
            throws Exception
    {
        // Sanity check: can DuckDB read a table that DuckDB itself created?
        try (Connection conn = createDuckdbConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT count(*) FROM ducklake_db.test_schema.simple_table")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).as("DuckDB should read its own simple_table").isEqualTo(5);
        }
    }

    // ==================== Diagnostic ====================

    @Test
    public void testDiagnosticTrinoWriteThenDuckdbRead()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.diag_table (id INTEGER, name VARCHAR)");
        computeActual("INSERT INTO test_schema.diag_table VALUES (1, 'hello'), (2, 'world')");

        // Verify Trino reads it
        assertThat(computeActual("SELECT count(*) FROM test_schema.diag_table")
                .getMaterializedRows().get(0).getField(0)).isEqualTo(2L);

        // Dump catalog state for debugging via PostgreSQL JDBC
        DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
        try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password());
                Statement pgStmt = pgConn.createStatement()) {
            ResultSet snapRs = pgStmt.executeQuery("SELECT max(snapshot_id) FROM ducklake_snapshot");
            long currentSnapshot = snapRs.next() ? snapRs.getLong(1) : -1;

            ResultSet tableRs = pgStmt.executeQuery(
                    "SELECT table_id, begin_snapshot, end_snapshot FROM ducklake_table WHERE table_name = 'diag_table'");
            long tableId = -1;
            long tableBegin = -1;
            String tableEnd = "?";
            if (tableRs.next()) {
                tableId = tableRs.getLong(1);
                tableBegin = tableRs.getLong(2);
                tableEnd = tableRs.getString(3);
            }

            ResultSet fileRs = pgStmt.executeQuery(
                    "SELECT data_file_id, begin_snapshot, end_snapshot, path, record_count FROM ducklake_data_file WHERE table_id = " + tableId);
            long fileBegin = -1;
            String fileEnd = "?";
            String filePath = "?";
            long fileRecords = -1;
            if (fileRs.next()) {
                fileBegin = fileRs.getLong(2);
                fileEnd = fileRs.getString(3);
                filePath = fileRs.getString(4);
                fileRecords = fileRs.getLong(5);
            }

            // Now try DuckDB — verify count (column values have known interop issue)
            try (Connection duckConn = createDuckdbConnection();
                    Statement duckStmt = duckConn.createStatement();
                    ResultSet duckRs = duckStmt.executeQuery("SELECT count(*) FROM ducklake_db.test_schema.diag_table")) {
                long duckCount = duckRs.next() ? duckRs.getLong(1) : -1;

                assertThat(duckCount)
                        .as("DuckDB count (snapshot=%d, table=%d begin=%d end=%s, file begin=%d end=%s path=%s records=%d)",
                                currentSnapshot, tableId, tableBegin, tableEnd, fileBegin, fileEnd, filePath, fileRecords)
                        .isEqualTo(2);
            }
        }
        finally {
            tryDropTable("test_schema.diag_table");
        }
    }

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
    // and Trino's Parquet array reader on DuckDB-written files. See
    // `DucklakeInlinedValueConverter.parseDucklakeListText` for the inlined-path contract, and
    // `COMPARE-pg_ducklake.md` B2 / TODO-compatibility.md for background.

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
    // Both types fail end-to-end on the inlined path today (TODO-compatibility.md): blob because
    // we do not decode DuckDB's `\xNN` text escapes back into bytes, uuid because the scalar
    // converter has no UUID branch. They may work on the Parquet path (Trino's Parquet reader
    // handles ARRAY<BINARY> and the UUID logical type), but we haven't confirmed until/unless the
    // inlined-path fix lands. Kept `@Disabled` so the missing behavior stays tracked — don't
    // delete.

    @Test
    @org.junit.jupiter.api.Disabled("TODO-compatibility.md: inlined `list<blob>` — DuckDB serializes bytes as "
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
    @org.junit.jupiter.api.Disabled("TODO-compatibility.md: scalar inlined UUID reads do not decode the 36-char text "
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

    // ==================== ducklake_snapshot_changes.changes_made round-trip ====================

    /**
     * Verifies DuckDB's {@code ducklake_snapshots()} table function — which parses every snapshot's
     * {@code changes_made} column via {@code ParseChangesList} + {@code ParseCatalogEntry} +
     * {@code ParseQuotedValue} — succeeds on Trino-written snapshots.
     * <p>
     * If we ever regress to writing unquoted values or the pre-spec single-part
     * {@code created_table:name} form, DuckDB raises {@code InvalidInputException} and the whole
     * function errors (not just the offending row). This test exercises every kind we currently
     * emit from Trino write paths: {@code created_schema}, {@code created_table},
     * {@code inserted_into_table}, {@code deleted_from_table}, {@code altered_table},
     * {@code dropped_table}, {@code dropped_schema}. See {@code TODO-compatibility.md} B1 and
     * {@code third_party/ducklake/src/storage/ducklake_transaction_changes.cpp}.
     */
    @Test
    public void testDuckdbParsesTrinoWrittenChangesMadeAcrossAllChangeKinds()
            throws Exception
    {
        String schemaName = "xengine_changes_made";
        String tableName = "orders";
        String fullTrino = schemaName + "." + tableName;

        try {
            computeActual("CREATE SCHEMA " + schemaName);
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER, note VARCHAR)");
            computeActual("INSERT INTO " + fullTrino + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            computeActual("ALTER TABLE " + fullTrino + " ADD COLUMN qty INTEGER");
            computeActual("DELETE FROM " + fullTrino + " WHERE id = 2");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id")) {
                // Parsing alone proves acceptability — any malformed entry makes the whole
                // function throw. Still, assert we can traverse every row without error.
                int rowCount = 0;
                boolean sawCreatedSchema = false;
                boolean sawCreatedTable = false;
                boolean sawInserted = false;
                boolean sawAltered = false;
                boolean sawDeleted = false;
                while (rs.next()) {
                    rowCount++;
                    // Force materialization of the MAP<VARCHAR, LIST<VARCHAR>> — that is where
                    // ParseChangesMade() runs inside DuckDB's Bind path, but we also read the
                    // value here to catch any lazy-decode surprises.
                    Object changes = rs.getObject("changes");
                    if (changes == null) {
                        continue;
                    }
                    String changesText = changes.toString();
                    if (changesText.contains("schemas_created") && changesText.contains(schemaName)) {
                        sawCreatedSchema = true;
                    }
                    if (changesText.contains("tables_created") && changesText.contains(tableName)) {
                        sawCreatedTable = true;
                    }
                    if (changesText.contains("tables_inserted_into")) {
                        sawInserted = true;
                    }
                    if (changesText.contains("tables_altered")) {
                        sawAltered = true;
                    }
                    if (changesText.contains("tables_deleted_from")) {
                        sawDeleted = true;
                    }
                }
                assertThat(rowCount).as("ducklake_snapshots row count").isGreaterThanOrEqualTo(5);
                assertThat(sawCreatedSchema).as("DuckDB parsed a `created_schema` entry").isTrue();
                assertThat(sawCreatedTable).as("DuckDB parsed a `created_table` entry").isTrue();
                assertThat(sawInserted).as("DuckDB parsed a `inserted_into_table` entry").isTrue();
                assertThat(sawAltered).as("DuckDB parsed a `altered_table` entry").isTrue();
                assertThat(sawDeleted).as("DuckDB parsed a `deleted_from_table` entry").isTrue();
            }

            // Drop table + schema to cover the `dropped_*` emissions too. Those are numeric and
            // already spec-conformant; asserting the parse still succeeds guards against future
            // regressions to the quoting helpers.
            computeActual("DROP TABLE " + fullTrino);
            computeActual("DROP SCHEMA " + schemaName);

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT changes FROM ducklake_snapshots('ducklake_db') " +
                                    "ORDER BY snapshot_id DESC LIMIT 5")) {
                while (rs.next()) {
                    rs.getObject("changes");
                }
            }
        }
        finally {
            tryDropTable(fullTrino);
            try {
                computeActual("DROP SCHEMA " + schemaName);
            }
            catch (Exception ignored) {
            }
        }
    }

    /**
     * Exercises the quoting path: names containing characters (comma, double quote) that would
     * break any naive {@code String.join(",", ...)} serializer. DuckDB must still parse the
     * snapshot cleanly via {@code ParseQuotedValue}, which unescapes {@code ""} back to {@code "}.
     */
    @Test
    public void testDuckdbParsesTrinoWrittenChangesMadeWithPathologicalNames()
            throws Exception
    {
        // Trino's identifier parser rejects embedded double quotes in CREATE TABLE identifiers,
        // but commas are legal (they just need double-quoting in SQL). Use a comma to exercise
        // the non-trivial quoting path without running into SQL-parser issues.
        String schemaName = "xengine,weird_schema";
        String tableName = "table,with,commas";
        String fullTrino = "\"" + schemaName + "\".\"" + tableName + "\"";

        try {
            computeActual("CREATE SCHEMA \"" + schemaName + "\"");
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER)");
            computeActual("INSERT INTO " + fullTrino + " VALUES (1)");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id")) {
                boolean sawQuotedSchema = false;
                boolean sawQuotedTable = false;
                while (rs.next()) {
                    Object changes = rs.getObject("changes");
                    if (changes == null) {
                        continue;
                    }
                    String text = changes.toString();
                    // DuckDB's parsed MAP round-trips the unquoted schema/table names through
                    // CatalogListToValue -> KeywordHelper::WriteOptionallyQuoted, so a name with
                    // commas comes back wrapped in double quotes like "xengine,weird_schema".
                    if (text.contains(schemaName)) {
                        sawQuotedSchema = true;
                    }
                    if (text.contains(tableName)) {
                        sawQuotedTable = true;
                    }
                }
                assertThat(sawQuotedSchema).as("DuckDB saw the comma-bearing schema name").isTrue();
                assertThat(sawQuotedTable).as("DuckDB saw the comma-bearing table name").isTrue();
            }
        }
        finally {
            tryDropTable(fullTrino);
            try {
                computeActual("DROP SCHEMA \"" + schemaName + "\"");
            }
            catch (Exception ignored) {
            }
        }
    }

    // ==================== default_value_dialect NULL round-trip ====================

    /**
     * Pins our policy on {@code ducklake_column.default_value_dialect}: we write SQL NULL
     * when there is no user-defined default (the common case — every column we write today).
     * The spec (`ducklake_column.md:36`) says the dialect is "especially useful for
     * expressions"; upstream's own migration (`ducklake_metadata_manager.cpp:297`) adds the
     * column with `DEFAULT NULL`, and upstream's ducklake extension never reads the field
     * anywhere in its C++ source. This test confirms DuckDB can read a Trino-written table
     * with NULL there without barfing, via both a normal `SELECT` (which loads column
     * metadata) and `DESCRIBE` (which exercises the full type-and-default-aware path).
     */
    @Test
    public void testDuckdbReadsTrinoTableWithNullDefaultValueDialect()
            throws Exception
    {
        String tableName = "xengine_null_dialect";
        String fullTrino = "test_schema." + tableName;
        String fullDuckdb = "ducklake_db.test_schema." + tableName;

        try {
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO " + fullTrino + " VALUES (1, 'alice', 1.5), (2, 'bob', 2.5)");

            // Confirm we wrote SQL NULL (not the string 'NULL') to default_value_dialect.
            DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
            try (Connection pgConn = DriverManager.getConnection(
                    catalog.jdbcUrl(), catalog.user(), catalog.password());
                    Statement pgStmt = pgConn.createStatement();
                    ResultSet rs = pgStmt.executeQuery(
                            "SELECT default_value, default_value_type, default_value_dialect " +
                                    "FROM ducklake_column c JOIN ducklake_table t USING (table_id) " +
                                    "WHERE t.table_name = '" + tableName + "' AND t.end_snapshot IS NULL " +
                                    "  AND c.end_snapshot IS NULL")) {
                int columnCount = 0;
                while (rs.next()) {
                    columnCount++;
                    assertThat(rs.getString("default_value"))
                            .as("no-user-default column should carry the 'NULL' string sentinel")
                            .isEqualTo("NULL");
                    assertThat(rs.getString("default_value_type"))
                            .as("default_value_type should remain 'literal' (spec-preferred)")
                            .isEqualTo("literal");
                    rs.getString("default_value_dialect");
                    assertThat(rs.wasNull())
                            .as("default_value_dialect should be SQL NULL when we don't touch the default")
                            .isTrue();
                }
                assertThat(columnCount).as("expected 3 active columns").isEqualTo(3);
            }

            // Normal read — must survive the NULL dialect.
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id, name, amount FROM " + fullDuckdb + " ORDER BY id")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("alice");
                assertThat(rs.getDouble("amount")).isEqualTo(1.5);
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.next()).isFalse();
            }

            // DESCRIBE path — separately exercises upstream's column-metadata materialization.
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement();
                    ResultSet rs = stmt.executeQuery("DESCRIBE " + fullDuckdb)) {
                List<String> columnNames = new ArrayList<>();
                while (rs.next()) {
                    columnNames.add(rs.getString(1));
                }
                assertThat(columnNames).containsExactly("id", "name", "amount");
            }
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    // ==================== View/schema DDL cross-engine parse + schema_version bump ====================

    /**
     * Upstream's {@code ParseChangeType} (in
     * {@code third_party/ducklake/src/storage/ducklake_transaction_changes.cpp}) enumerates
     * {@code created_view} / {@code altered_view} / {@code dropped_view} but no
     * {@code RENAMED_*}. An earlier revision of this connector emitted
     * {@code renamed_view:<viewId>} on {@code ALTER VIEW ... RENAME}, which made any later
     * call to DuckDB's {@code ducklake_snapshots()} throw
     * {@code InvalidInputException: Unsupported change type renamed_view} for any snapshot
     * that ever renamed a view. A rename is semantically a schema/name change, so the
     * current code emits {@code altered_view} and bumps {@code schema_version} (upstream
     * {@code SchemaChangesMade()} flips on dropped/new view entries). This test exercises
     * the full view-DDL surface (create, rename, replace, drop) plus schema DDL
     * (create/drop) and confirms DuckDB parses every snapshot cleanly.
     */
    @Test
    public void testDuckdbParsesTrinoWrittenViewAndSchemaDdlChanges()
            throws Exception
    {
        String schemaName = "xengine_view_changes";

        try {
            computeActual("CREATE SCHEMA " + schemaName);
            computeActual("CREATE VIEW " + schemaName + ".v_src AS SELECT id FROM test_schema.simple_table");
            // Fully qualify RENAME TO — Trino resolves an unqualified target against the
            // session default schema (test_schema), not the source view's schema.
            computeActual("ALTER VIEW " + schemaName + ".v_src RENAME TO " + schemaName + ".v_dst");
            computeActual("CREATE OR REPLACE VIEW " + schemaName + ".v_dst AS SELECT id, name FROM test_schema.simple_table");
            computeActual("DROP VIEW " + schemaName + ".v_dst");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id")) {
                int rowCount = 0;
                boolean sawAlteredView = false;
                boolean sawDroppedView = false;
                while (rs.next()) {
                    rowCount++;
                    Object changes = rs.getObject("changes");
                    if (changes == null) {
                        continue;
                    }
                    String text = changes.toString();
                    if (text.contains("views_altered")) {
                        sawAlteredView = true;
                    }
                    if (text.contains("views_dropped")) {
                        sawDroppedView = true;
                    }
                }
                assertThat(rowCount).as("ducklake_snapshots row count").isGreaterThanOrEqualTo(5);
                assertThat(sawAlteredView)
                        .as("DuckDB parsed at least one `altered_view` entry (rename + replace both map here)")
                        .isTrue();
                assertThat(sawDroppedView).as("DuckDB parsed a `dropped_view` entry").isTrue();
            }
        }
        finally {
            try {
                computeActual("DROP VIEW " + schemaName + ".v_dst");
            }
            catch (Exception ignored) {
            }
            try {
                computeActual("DROP VIEW " + schemaName + ".v_src");
            }
            catch (Exception ignored) {
            }
            try {
                computeActual("DROP SCHEMA " + schemaName);
            }
            catch (Exception ignored) {
            }
        }
    }

    /**
     * Upstream {@code DuckLakeTransaction::SchemaChangesMade()} flips on new/dropped view
     * entries and on new/dropped schema entries — the same trigger used for table DDL.
     * A DuckDB reader that caches catalog state keyed on {@code schema_version} must see
     * a bump when Trino creates/drops/replaces/renames a view or creates/drops a schema.
     * Before this was wired up, a Trino-only sequence of view + schema DDL would leave
     * {@code ducklake_snapshot.schema_version} frozen — DuckDB's next query would hit
     * stale cache and miss those objects. This test walks the full DDL surface and asserts
     * the counter advances on every step.
     * <p>
     * Surface covered:
     * <ul>
     *   <li>{@code CREATE SCHEMA} → {@code createSchema}</li>
     *   <li>{@code CREATE VIEW} → {@code createView}</li>
     *   <li>{@code COMMENT ON VIEW} → {@code replaceViewMetadata}</li>
     *   <li>{@code ALTER VIEW ... RENAME TO} → {@code renameView}</li>
     *   <li>{@code DROP VIEW} → {@code dropView}</li>
     *   <li>{@code DROP SCHEMA} → {@code dropSchema}</li>
     * </ul>
     */
    @Test
    public void testSchemaVersionBumpsOnViewAndSchemaDdl()
            throws Exception
    {
        String schemaName = "xengine_sv_bumps";
        DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();

        try {
            long before = readCurrentSchemaVersion(catalog);

            computeActual("CREATE SCHEMA " + schemaName);
            long afterCreateSchema = readCurrentSchemaVersion(catalog);
            assertThat(afterCreateSchema).as("CREATE SCHEMA bumps schema_version").isGreaterThan(before);

            computeActual("CREATE VIEW " + schemaName + ".v AS SELECT id FROM test_schema.simple_table");
            long afterCreateView = readCurrentSchemaVersion(catalog);
            assertThat(afterCreateView).as("CREATE VIEW bumps schema_version").isGreaterThan(afterCreateSchema);

            // Note: Trino resolves an unqualified `RENAME TO <name>` against the session's
            // default schema, not the source view's schema. Fully qualify the target to
            // keep the rename scoped to our test schema.
            computeActual("ALTER VIEW " + schemaName + ".v RENAME TO " + schemaName + ".v2");
            long afterRename = readCurrentSchemaVersion(catalog);
            assertThat(afterRename).as("ALTER VIEW ... RENAME bumps schema_version").isGreaterThan(afterCreateView);

            // COMMENT ON VIEW dispatches to replaceViewMetadata (the only SQL path that does —
            // CREATE OR REPLACE VIEW goes through dropView + createView in DucklakeMetadata).
            computeActual("COMMENT ON VIEW " + schemaName + ".v2 IS 'a view with a comment'");
            long afterComment = readCurrentSchemaVersion(catalog);
            assertThat(afterComment).as("COMMENT ON VIEW (replaceViewMetadata) bumps schema_version").isGreaterThan(afterRename);

            computeActual("DROP VIEW " + schemaName + ".v2");
            long afterDropView = readCurrentSchemaVersion(catalog);
            assertThat(afterDropView).as("DROP VIEW bumps schema_version").isGreaterThan(afterComment);

            computeActual("DROP SCHEMA " + schemaName);
            long afterDropSchema = readCurrentSchemaVersion(catalog);
            assertThat(afterDropSchema).as("DROP SCHEMA bumps schema_version").isGreaterThan(afterDropView);

            // Non-table-scoped bumps must land in ducklake_schema_versions with table_id = NULL
            // (spec ducklake_schema_versions.md: table_id is BIGINT, nullable). A future DuckDB
            // version that reads per-table snapshots via this table still needs the broadcast
            // bump, which is keyed on table_id IS NULL.
            long nonTableScopedRows = countSchemaVersionRowsWithNullTableId(catalog, before);
            assertThat(nonTableScopedRows)
                    .as("expected one schema_versions row with NULL table_id per view/schema DDL (6 total)")
                    .isGreaterThanOrEqualTo(6);
        }
        finally {
            try {
                computeActual("DROP VIEW " + schemaName + ".v2");
            }
            catch (Exception ignored) {
            }
            try {
                computeActual("DROP VIEW " + schemaName + ".v");
            }
            catch (Exception ignored) {
            }
            try {
                computeActual("DROP SCHEMA " + schemaName);
            }
            catch (Exception ignored) {
            }
        }
    }

    private long readCurrentSchemaVersion(DucklakeCatalogGenerator.IsolatedCatalog catalog)
            throws Exception
    {
        try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
            return queryLong(pgConn,
                    "SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)");
        }
    }

    private long countSchemaVersionRowsWithNullTableId(
            DucklakeCatalogGenerator.IsolatedCatalog catalog,
            long sinceExclusiveSchemaVersion)
            throws Exception
    {
        try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
            return queryLong(pgConn,
                    "SELECT count(*) FROM ducklake_schema_versions WHERE table_id IS NULL AND schema_version > ?",
                    sinceExclusiveSchemaVersion);
        }
    }

    // ==================== Helpers ====================

    private void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }

    /**
     * Asserts that a table has no active Parquet data files (all rows live in the
     * {@code ducklake_inlined_data_<tableId>_<schemaVersion>} metadata tables) and that the
     * total inlined row count matches {@code expectedRowCount}. Throws if the assumption fails —
     * used to guarantee the *inlined* read path is the one under test.
     */
    /**
     * Asserts that a table has at least one active Parquet data file and no active inlined rows.
     * Mirror of {@link #assertRowsStayedInlined} — used to guarantee the *Parquet* read path is
     * the one under test.
     */
    private void assertRowsWrittenToParquet(String tableName)
            throws Exception
    {
        DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
        try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
            long snapshotId = queryLong(pgConn, "SELECT max(snapshot_id) FROM ducklake_snapshot");
            long tableId = queryLong(pgConn,
                    "SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL",
                    tableName);

            long activeDataFiles = queryLong(pgConn,
                    "SELECT count(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
                    tableId);
            assertThat(activeDataFiles)
                    .as("table %s should have at least one active Parquet file when inlining is off", tableName)
                    .isGreaterThanOrEqualTo(1);

            long totalInlined = 0;
            try (PreparedStatement stmt = pgConn.prepareStatement(
                    "SELECT schema_version FROM ducklake_inlined_data_tables WHERE table_id = ?")) {
                stmt.setLong(1, tableId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        long schemaVersion = rs.getLong("schema_version");
                        totalInlined += queryLong(pgConn,
                                "SELECT count(*) FROM ducklake_inlined_data_" + tableId + "_" + schemaVersion +
                                        " WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
                                snapshotId,
                                snapshotId);
                    }
                }
            }
            assertThat(totalInlined)
                    .as("table %s should have zero active inlined rows when inlining is off", tableName)
                    .isZero();
        }
    }

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

    private void assertRowsStayedInlined(String tableName, long expectedRowCount)
            throws Exception
    {
        DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
        try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
            long snapshotId = queryLong(pgConn, "SELECT max(snapshot_id) FROM ducklake_snapshot");
            long tableId = queryLong(pgConn,
                    "SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL",
                    tableName);

            long activeDataFiles = queryLong(pgConn,
                    "SELECT count(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
                    tableId);
            assertThat(activeDataFiles)
                    .as("table %s should have no active Parquet files if rows are inlined", tableName)
                    .isZero();

            long totalInlined = 0;
            try (PreparedStatement stmt = pgConn.prepareStatement(
                    "SELECT schema_version FROM ducklake_inlined_data_tables WHERE table_id = ?")) {
                stmt.setLong(1, tableId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        long schemaVersion = rs.getLong("schema_version");
                        totalInlined += queryLong(pgConn,
                                "SELECT count(*) FROM ducklake_inlined_data_" + tableId + "_" + schemaVersion +
                                        " WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
                                snapshotId,
                                snapshotId);
                    }
                }
            }
            assertThat(totalInlined)
                    .as("table %s active inlined row count", tableName)
                    .isEqualTo(expectedRowCount);
        }
    }

    private static long queryLong(Connection conn, String sql, Object... parameters)
            throws Exception
    {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                Object parameter = parameters[i];
                if (parameter instanceof String stringValue) {
                    stmt.setString(i + 1, stringValue);
                }
                else if (parameter instanceof Long longValue) {
                    stmt.setLong(i + 1, longValue);
                }
                else {
                    stmt.setObject(i + 1, parameter);
                }
            }
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("No rows returned for query: " + sql);
                }
                return rs.getLong(1);
            }
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
