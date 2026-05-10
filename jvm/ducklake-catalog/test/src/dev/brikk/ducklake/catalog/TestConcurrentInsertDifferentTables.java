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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins today's behavior for two writers concurrently committing INSERTs into
 * <em>different</em> tables. Both must commit; the loser still goes through
 * one retry because today's lineage check rejects any intervening commit, but
 * the retry must succeed cleanly. Catches snapshot-id cross-talk: the loser
 * must not orphan rows on the wrong table or collide on file IDs.
 *
 * <p>Mirrors pg_ducklake's {@code concurrent_cross_table_writes} isolation
 * spec.
 */
public class TestConcurrentInsertDifferentTables
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;
    private static long simpleTableId;
    private static long simpleIdColumnId;
    private static long partitionedTableId;
    private static long partitionedIdColumnId;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-different-tables");

        DucklakeCatalogConfig config = new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);
        catalog = new JdbcDucklakeCatalog(config);

        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable simpleTable = catalog.getTable("test_schema", "simple_table", snapshotId).orElseThrow();
        simpleTableId = simpleTable.tableId();
        simpleIdColumnId = catalog.getTableColumns(simpleTableId, snapshotId).stream()
                .filter(c -> c.columnName().equals("id"))
                .findFirst()
                .orElseThrow()
                .columnId();

        DucklakeTable partitionedTable = catalog.getTable("test_schema", "partitioned_table", snapshotId).orElseThrow();
        partitionedTableId = partitionedTable.tableId();
        partitionedIdColumnId = catalog.getTableColumns(partitionedTableId, snapshotId).stream()
                .filter(c -> c.columnName().equals("id"))
                .findFirst()
                .orElseThrow()
                .columnId();
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
    public void concurrentInsertsAcrossTablesBothCommit()
            throws Exception
    {
        long preSimpleFileCount = catalog.getDataFiles(simpleTableId, catalog.getCurrentSnapshotId()).size();
        long prePartitionedFileCount = catalog.getDataFiles(partitionedTableId, catalog.getCurrentSnapshotId()).size();

        DucklakeWriteFragment winnerFragment = fragmentFor(
                "test_data/concurrent_diff_tables_winner.parquet",
                simpleIdColumnId,
                100L,
                110L);
        DucklakeWriteFragment loserFragment = fragmentFor(
                "test_data/concurrent_diff_tables_loser.parquet",
                partitionedIdColumnId,
                200L,
                210L);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.commitInsert(simpleTableId, List.of(winnerFragment)),
                () -> catalog.commitInsert(partitionedTableId, List.of(loserFragment)));

        assertThat(result.loserException())
                .as("loser must commit cleanly on retry; cross-table INSERTs are not a conflict")
                .isNull();
        assertThat(result.loserAttemptCount())
                .as("loser must hit the hook twice: first attempt (paused) + retry (no-op)")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();

        List<DucklakeDataFile> simpleFiles = catalog.getDataFiles(simpleTableId, latestSnapshot);
        assertThat(simpleFiles)
                .as("winner's data file must land on simple_table")
                .hasSize((int) preSimpleFileCount + 1);
        assertThat(simpleFiles)
                .extracting(DucklakeDataFile::path)
                .contains(winnerFragment.path());

        List<DucklakeDataFile> partitionedFiles = catalog.getDataFiles(partitionedTableId, latestSnapshot);
        assertThat(partitionedFiles)
                .as("loser's data file must land on partitioned_table (proves no cross-talk)")
                .hasSize((int) prePartitionedFileCount + 1);
        assertThat(partitionedFiles)
                .extracting(DucklakeDataFile::path)
                .contains(loserFragment.path());

        // Cross-talk negative checks: winner's file must NOT appear on the loser's table and vice versa.
        assertThat(simpleFiles)
                .extracting(DucklakeDataFile::path)
                .doesNotContain(loserFragment.path());
        assertThat(partitionedFiles)
                .extracting(DucklakeDataFile::path)
                .doesNotContain(winnerFragment.path());
    }

    private static DucklakeWriteFragment fragmentFor(String path, long columnId, long minId, long maxId)
    {
        DucklakeFileColumnStats idStats = new DucklakeFileColumnStats(
                columnId,
                32L,
                /* valueCount */ 10L,
                /* nullCount */ 0L,
                Optional.of(Long.toString(minId)),
                Optional.of(Long.toString(maxId)),
                /* containsNan */ false);
        return new DucklakeWriteFragment(
                path,
                /* fileSizeBytes */ 1024L,
                /* footerSize */ 64L,
                /* recordCount */ 10L,
                List.of(idStats));
    }
}
