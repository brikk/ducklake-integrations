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

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared fixture + helpers for cross-engine compatibility test suites: Trino writes through the
 * connector, DuckDB reads the same PostgreSQL-backed DuckLake catalog, and we assert that what
 * one engine emits the other can parse without bespoke sync because PostgreSQL MVCC takes care
 * of visibility.
 *
 * <p>Subclasses declare a unique {@link #isolatedCatalogName()} so each suite gets its own
 * fresh PostgreSQL database (created via {@link DucklakeQueryRunner.Builder#useIsolatedCatalog})
 * and they cannot interfere with each other's catalog state.
 */
abstract class AbstractDucklakeCrossEngineTest
        extends AbstractTestQueryFramework
{
    private DucklakeCatalogGenerator.IsolatedCatalog isolatedCatalog;

    /**
     * Unique catalog identifier for this suite. Becomes the suffix on the PostgreSQL database
     * name and the on-disk data directory, so each suite is fully isolated.
     */
    protected abstract String isolatedCatalogName();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog(isolatedCatalogName())
                .build();
    }

    protected DucklakeCatalogGenerator.IsolatedCatalog getIsolatedCatalog()
            throws Exception
    {
        if (isolatedCatalog == null) {
            // The isolated catalog was already created by useIsolatedCatalog(name) inside
            // createQueryRunner(); reconstruct the same descriptor here so we can open
            // direct DuckDB / PostgreSQL JDBC connections against it.
            TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
            String name = isolatedCatalogName();
            String databaseName = "ducklake_" + name.replace('-', '_');
            isolatedCatalog = new DucklakeCatalogGenerator.IsolatedCatalog(
                    server.getJdbcUrl(databaseName),
                    server.getUser(),
                    server.getPassword(),
                    Path.of("build", "test-data", "test-catalog-isolated-" + name, "data"),
                    server.getDuckDbAttachUri(databaseName));
        }
        return isolatedCatalog;
    }

    /**
     * Creates a fresh DuckDB connection attached to the PostgreSQL-backed DuckLake catalog.
     * No sync workaround needed — PostgreSQL MVCC ensures immediate visibility.
     */
    protected Connection createDuckdbConnection()
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

    protected void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }

    /**
     * Asserts that {@code tableName} has all rows kept in the
     * {@code ducklake_inlined_data_<tableId>_<schemaVersion>} metadata tables and zero active
     * Parquet files, with the inlined row count matching {@code expectedRowCount}. Used to
     * guarantee the *inlined* read path is the one under test.
     */
    protected void assertRowsStayedInlined(String tableName, long expectedRowCount)
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

    /**
     * Mirror of {@link #assertRowsStayedInlined}: asserts at least one active Parquet data file
     * and zero active inlined rows. Used to guarantee the *Parquet* read path is the one under
     * test.
     */
    protected void assertRowsWrittenToParquet(String tableName)
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

    protected static long queryLong(Connection conn, String sql, Object... parameters)
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
}
