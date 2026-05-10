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
 * Acceptance test for the finer-grained delete-vs-delete file-overlap check
 * (port of {@code ducklake_transaction.cpp:1259–1283}). Two writers that
 * each commit a delete file targeting the SAME {@code data_file_id} would
 * silently lose one set of deletions — the second commit's INSERT into
 * {@code ducklake_delete_file} end-snapshots the first commit's delete file,
 * so only the second writer's deletions remain visible to readers.
 *
 * <p>This is the case neither the basic conflict matrix
 * ({@code tables_deleted_from × tables_deleted_from} alone is not a conflict)
 * nor the state-based {@link LogicalConflictCheck} catch — the data file
 * itself stays active across delete commits. Detection requires querying
 * {@code ducklake_delete_file} for intervening rows referencing the
 * contended {@code data_file_id}s.
 */
public class TestConcurrentDeleteVsDelete
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
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-delete-vs-delete");

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
    public void duelingDeletesOnSameDataFileConflict()
            throws Exception
    {
        DucklakeDeleteFragment winnerFragment = new DucklakeDeleteFragment(
                sharedDataFileId,
                "test_data/delete_vs_delete_winner.parquet",
                /* deleteCount */ 2L,
                /* fileSizeBytes */ 256L,
                /* footerSize */ 64L);
        DucklakeDeleteFragment loserFragment = new DucklakeDeleteFragment(
                sharedDataFileId,
                "test_data/delete_vs_delete_loser.parquet",
                /* deleteCount */ 1L,
                /* fileSizeBytes */ 256L,
                /* footerSize */ 64L);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.commitDelete(tableId, List.of(winnerFragment)),
                () -> catalog.commitDelete(tableId, List.of(loserFragment)));

        assertThat(result.loserException())
                .as("two concurrent delete-files on the same data_file_id must conflict")
                .isInstanceOf(LogicalConflictException.class);
        assertThat(result.loserException().getMessage())
                .as("error message must name the contended data_file_id")
                .contains(Long.toString(sharedDataFileId))
                .contains("delete files");

        assertThat(((TransactionConflictException) result.loserException()).retryable())
                .as("logical conflicts are non-retryable")
                .isFalse();
        assertThat(result.loserAttemptCount())
                .as("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
                .isEqualTo(2);
    }
}
