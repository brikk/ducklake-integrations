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
 * Acceptance test for the {@code tables_deleted_from × altered_tables}
 * matrix entry at {@code ducklake_transaction.cpp:1255}. T1 alters a
 * table's schema; T2 races with a delete against the same table. The
 * delete must abort non-retryably so we don't end-snapshot data files
 * under a now-shifted schema.
 */
public class TestConcurrentDeleteVsAlterTable
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;
    private static long tableId;
    private static long nameColumnId;
    private static long sharedDataFileId;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-delete-vs-alter-table");

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
        nameColumnId = catalog.getTableColumns(tableId, snapshotId).stream()
                .filter(c -> c.columnName().equals("name"))
                .findFirst()
                .orElseThrow()
                .columnId();
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
    public void deleteRacingAlterTableConflicts()
            throws Exception
    {
        DucklakeDeleteFragment loserFragment = new DucklakeDeleteFragment(
                sharedDataFileId,
                "test_data/delete_vs_alter_loser.parquet",
                /* deleteCount */ 1L,
                /* fileSizeBytes */ 256L,
                /* footerSize */ 64L,
                /* newDeleteCount */ 1L);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.dropColumn(tableId, nameColumnId),
                () -> catalog.commitDelete(tableId, List.of(loserFragment)));

        assertThat(result.loserException())
                .as("delete racing an ALTER TABLE on the same table must conflict")
                .isInstanceOf(LogicalConflictException.class);
        assertThat(result.loserException().getMessage())
                .as("error message must name the contended table and reference the alter")
                .contains("delete from table")
                .contains("id=" + tableId)
                .contains("altered it");

        assertThat(((TransactionConflictException) result.loserException()).retryable())
                .as("logical conflicts are non-retryable")
                .isFalse();
        assertThat(result.loserAttemptCount())
                .as("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getTableColumns(tableId, latestSnapshot))
                .as("winner's DROP COLUMN visible at the latest snapshot")
                .extracting(DucklakeColumn::columnId)
                .doesNotContain(nameColumnId);
    }
}
