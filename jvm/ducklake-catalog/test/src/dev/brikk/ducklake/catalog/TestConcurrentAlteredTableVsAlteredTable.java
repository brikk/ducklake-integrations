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
package dev.brikk.ducklake.catalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the behavior for two writers concurrently adding distinct columns to
 * the same table. Each {@code addColumn} re-resolves {@code maxOrder} per
 * attempt, so the loser's retry sees the winner's column and allocates a
 * non-colliding {@code column_order}. Both columns must land at the latest
 * snapshot.
 *
 * <p>This is the third-step counterpart to {@link TestConcurrentInsertVsDropColumn}:
 * the {@code AlteredTable(tableId)} branch of {@link LogicalConflictCheck}
 * fires only when the table itself was dropped — disjoint additions are
 * compatible and must commit cleanly.
 */
public class TestConcurrentAlteredTableVsAlteredTable
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;
    private static long tableId;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-altered-table-vs-altered-table");

        DucklakeCatalogConfig config = new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);
        catalog = new JdbcDucklakeCatalog(config);

        long snapshotId = catalog.getCurrentSnapshotId();
        tableId = catalog.getTable("test_schema", "simple_table", snapshotId).orElseThrow().tableId();
    }

    @AfterAll
    public static void tearDownClass()
    {
        if (catalog != null) {
            catalog.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void concurrentAddColumnsBothLand()
            throws Exception
    {
        TableColumnSpec winnerColumn = TableColumnSpec.leaf("winner_col", "varchar", true);
        TableColumnSpec loserColumn = TableColumnSpec.leaf("loser_col", "integer", true);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.addColumn(tableId, winnerColumn),
                () -> catalog.addColumn(tableId, loserColumn));

        assertThat(result.loserException())
                .as("disjoint addColumn ops are compatible; loser must commit cleanly on retry")
                .isNull();
        assertThat(result.loserAttemptCount())
                .as("loser must hit the hook twice: first attempt (paused) + retry (no-op)")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getTableColumns(tableId, latestSnapshot))
                .extracting(DucklakeColumn::columnName)
                .as("both columns must be visible at the latest snapshot")
                .contains("winner_col", "loser_col");
    }
}
