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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test for the {@code tables_deleted_from × dropped_tables}
 * matrix entry at {@code ducklake_transaction.cpp:1254}. T1 drops a
 * table; T2 races with a delete against that table. The delete must
 * abort non-retryably — without this check, T2's
 * {@code applyDeleteFragments} would insert a {@code ducklake_delete_file}
 * row referencing the dropped table's now end-snapshotted data files.
 *
 * <p>Either {@link LogicalConflictCheck} (state-based: the data_file_id
 * is no longer active) or {@link ConflictMatrix} (change-based:
 * {@code DeletedFromTable} hits the {@code dropped_tables} set) catches
 * this. Both throw {@link LogicalConflictException}.
 */
public class TestConcurrentDeleteVsDropTable
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;
    private static long tableId;
    private static long sharedDataFileId;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-delete-vs-drop-table");

        DucklakeCatalogConfig config = new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);
        catalog = new JdbcDucklakeCatalog(config);

        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = catalog.getTable("test_schema", "simple_table", snapshotId).orElseThrow();
        tableId = table.tableId();
        sharedDataFileId = catalog.getDataFiles(tableId, snapshotId).getFirst().dataFileId();
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
    public void deleteRacingDropTableConflicts()
            throws Exception
    {
        DucklakeDeleteFragment loserFragment = new DucklakeDeleteFragment(
                sharedDataFileId,
                "test_data/delete_vs_drop_table_loser.parquet",
                /* deleteCount */ 1L,
                /* fileSizeBytes */ 256L,
                /* footerSize */ 64L);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.dropTable("test_schema", "simple_table"),
                () -> catalog.commitDelete(tableId, List.of(loserFragment)));

        assertThat(result.loserException())
                .as("delete racing a DROP TABLE must conflict")
                .isInstanceOf(LogicalConflictException.class);

        assertThat(((TransactionConflictException) result.loserException()).retryable())
                .as("logical conflicts are non-retryable")
                .isFalse();
        assertThat(result.loserAttemptCount())
                .as("loser must NOT burn the retry budget — exactly two attempts: parked + check-failed")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getTable("test_schema", "simple_table", latestSnapshot))
                .as("winner's DROP TABLE visible at the latest snapshot")
                .isEmpty();
    }
}
