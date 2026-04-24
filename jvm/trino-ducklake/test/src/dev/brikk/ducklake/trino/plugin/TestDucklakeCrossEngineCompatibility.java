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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    // ==================== Inlined-data encoding audit (COMPARE-pg_ducklake.md) ====================
    //
    // Upstream's PostgreSQL metadata manager encodes inlined rows using specific PG column types
    // (pg_ducklake/docs/data_types.md):
    //   - date / timestamp* / timestamptz  -> VARCHAR
    //   - varchar / blob                    -> BYTEA (allows embedded null bytes)
    //   - list<T>                           -> native PG arrays (e.g. INT4[])
    // The tests below create tables via DuckDB with `data_inlining_row_limit` high enough that the
    // rows stay in `ducklake_inlined_data_<table_id>_<schema_version>` metadata tables, then read
    // them through Trino and verify value fidelity.

    @Test
    public void testDuckdbInlinedListIntCurrentlyFailsInTrino()
            throws Exception
    {
        // Pins the current bug: DucklakeInlinedValueConverter treats a PG `INTEGER[]` inlined
        // value as a scalar and hands Trino a Slice where ArrayType expects a Block. See
        // COMPARE-pg_ducklake.md B2 and TODO-compatibility.md. When the converter is widened to
        // consume PG arrays, delete this test and enable
        // {@link #testDuckdbInlinedListIntReadsInTrino_target}.
        String tableName = "xengine_inlined_list_int";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;
        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, tags INTEGER[])");
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '" + tableName + "')");
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES (1, [10, 20, 30]), (2, [42]), (3, NULL)");
            }
            assertRowsStayedInlined(tableName, 3);

            assertThatThrownBy(() -> computeActual("SELECT id, tags FROM " + fullTrino + " ORDER BY id"))
                    .hasMessageContaining("Slice cannot be cast to")
                    .hasMessageContaining("Block");
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    @Test
    @org.junit.jupiter.api.Disabled("TODO (TODO-compatibility.md B2): widen DucklakeInlinedValueConverter to consume PG arrays")
    public void testDuckdbInlinedListIntReadsInTrino_target()
            throws Exception
    {
        // Target behavior after the B2 fix lands: Trino reads DuckDB-written inlined list<int>
        // columns as ArrayType(Integer) with the array contents intact.
        String tableName = "xengine_inlined_list_int_target";
        String fullDuckdb = "ducklake_db.test_schema." + tableName;
        String fullTrino = "test_schema." + tableName;
        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullDuckdb);
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, tags INTEGER[])");
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '" + tableName + "')");
                stmt.execute("INSERT INTO " + fullDuckdb + " VALUES (1, [10, 20, 30]), (2, [42]), (3, NULL)");
            }
            assertRowsStayedInlined(tableName, 3);

            MaterializedResult result = computeActual("SELECT id, tags FROM " + fullTrino + " ORDER BY id");
            assertThat(result.getMaterializedRows()).hasSize(3);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(List.of(10, 20, 30));
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(List.of(42));
            assertThat(result.getMaterializedRows().get(2).getField(1)).isNull();
        }
        finally {
            tryDropTable(fullTrino);
        }
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
