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

import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;

import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the connector refuses to read a snapshot that has inlined deletions
 * (DuckLake stores small deletes in {@code ducklake_inlined_delete_<tableId>}
 * when {@code DATA_INLINING_ROW_LIMIT} is set on the writer). Without this
 * guard the inlined deletions are silently ignored and rows that should have
 * been deleted are returned to the user.
 *
 * <p>Mirrors {@link TestDucklakePuffinDeleteFileGuard}: inject the metadata,
 * then assert {@code DucklakeSplitManager.getSplits} throws
 * {@code NOT_SUPPORTED} with a clear actionable message.
 */
public class TestDucklakeInlinedDeleteGuard
{
    @Test
    public void inlinedDeleteFailsWithClearError()
            throws Exception
    {
        TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
        DucklakeCatalogGenerator.IsolatedCatalog isolated =
                DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(
                        server, "inlined-delete-guard-" + UUID.randomUUID().toString().substring(0, 8));

        DucklakeConfig config = new DucklakeConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);

        DucklakeCatalog catalog = new JdbcDucklakeCatalog(config.toCatalogConfig());
        try {
            DucklakeSplitManager splitManager = new DucklakeSplitManager(
                    catalog, config, new DucklakePathResolver(catalog, config));

            long snapshotId = catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(catalog, "test_schema", "simple_table", snapshotId);
            DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                    "test_schema", "simple_table", table.tableId(), snapshotId);

            DucklakeDataFile victim = catalog.getDataFiles(table.tableId(), snapshotId).stream()
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No data files found for simple_table"));

            // Materialize the per-table inlined-delete table DuckDB would create
            // and inject one deletion row visible at the current snapshot.
            // Schema per upstream data_inlining.md: file_id, row_id, begin_snapshot.
            createAndPopulateInlinedDeleteTable(
                    isolated.jdbcUrl(),
                    isolated.user(),
                    isolated.password(),
                    table.tableId(),
                    snapshotId,
                    victim.dataFileId());

            assertThatThrownBy(() -> splitManager.getSplits(
                    null,
                    SESSION,
                    tableHandle,
                    DynamicFilter.EMPTY,
                    Constraint.alwaysTrue()))
                    .isInstanceOfSatisfying(TrinoException.class, ex -> {
                        assertThat(ex.getErrorCode()).isEqualTo(StandardErrorCode.NOT_SUPPORTED.toErrorCode());
                        assertThat(ex.getMessage())
                                .contains("test_schema")
                                .contains("simple_table")
                                .contains("ducklake_inlined_delete_" + table.tableId())
                                .contains("DATA_INLINING_ROW_LIMIT")
                                .contains("ducklake_flush_inlined_data");
                    });
        }
        finally {
            catalog.close();
        }
    }

    @Test
    public void absentInlinedDeleteTableDoesNotTrip()
            throws Exception
    {
        // Sanity: the probe must return false (and the read must succeed) when
        // ducklake_inlined_delete_<tableId> doesn't exist at all — the common
        // case for any table DuckDB never inlined deletes for. Otherwise every
        // query would hit a "table doesn't exist" SQLException leak from the
        // JDBC driver.
        TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
        DucklakeCatalogGenerator.IsolatedCatalog isolated =
                DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(
                        server, "no-inlined-delete-" + UUID.randomUUID().toString().substring(0, 8));

        DucklakeConfig config = new DucklakeConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);

        DucklakeCatalog catalog = new JdbcDucklakeCatalog(config.toCatalogConfig());
        try {
            long snapshotId = catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(catalog, "test_schema", "simple_table", snapshotId);

            assertThat(catalog.hasInlinedDeletes(table.tableId(), snapshotId))
                    .as("no inlined-delete table exists for this fresh catalog; probe must return false")
                    .isFalse();
        }
        finally {
            catalog.close();
        }
    }

    private static void createAndPopulateInlinedDeleteTable(
            String jdbcUrl,
            String user,
            String password,
            long tableId,
            long snapshotId,
            long dataFileId)
            throws Exception
    {
        String inlinedDeleteName = "ducklake_inlined_delete_" + tableId;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    "CREATE TABLE %s (file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)",
                    inlinedDeleteName));
            stmt.execute(String.format(
                    "INSERT INTO %s VALUES (%d, 0, %d)",
                    inlinedDeleteName, dataFileId, snapshotId));
        }
    }

    private static DucklakeSchema getSchema(DucklakeCatalog catalog, String schemaName, long snapshotId)
    {
        return catalog.listSchemas(snapshotId).stream()
                .filter(schema -> schema.schemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing schema: " + schemaName));
    }

    private static DucklakeTable getTable(DucklakeCatalog catalog, String schemaName, String tableName, long snapshotId)
    {
        DucklakeSchema schema = getSchema(catalog, schemaName, snapshotId);
        return catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(table -> table.tableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing table: " + schemaName + "." + tableName));
    }
}
