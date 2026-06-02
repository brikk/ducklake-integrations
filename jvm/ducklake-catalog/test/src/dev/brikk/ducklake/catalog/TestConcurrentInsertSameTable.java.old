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
 * Pins today's behavior for two writers concurrently committing INSERTs to the
 * same table. The loser parks before its first mutation; the winner commits;
 * the loser releases, fails the strict lineage check, retries, and commits
 * cleanly. Both data files must be visible at the latest snapshot.
 *
 * <p>This is a baseline that must hold against unmodified production code —
 * Step 3 of the logical-conflict-checking work must not regress it (concurrent
 * INSERTs into the same table are <em>not</em> a logical conflict; they're
 * the expected concurrent-write workload).
 */
public class TestConcurrentInsertSameTable
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;
    private static long tableId;
    private static long idColumnId;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-same-table");

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
        idColumnId = catalog.getTableColumns(tableId, snapshotId).stream()
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
    public void concurrentInsertsBothCommit()
            throws Exception
    {
        long preDataFileCount = catalog.getDataFiles(tableId, catalog.getCurrentSnapshotId()).size();

        DucklakeWriteFragment winnerFragment = fragmentFor("test_data/concurrent_same_table_winner.parquet", 1L, 11L);
        DucklakeWriteFragment loserFragment = fragmentFor("test_data/concurrent_same_table_loser.parquet", 12L, 22L);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.commitInsert(tableId, List.of(winnerFragment)),
                () -> catalog.commitInsert(tableId, List.of(loserFragment)));

        assertThat(result.loserException())
                .as("loser must commit cleanly on retry; concurrent same-table INSERTs are not a conflict")
                .isNull();
        assertThat(result.loserAttemptCount())
                .as("loser must hit the hook twice: first attempt (paused) + retry (no-op)")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(tableId, latestSnapshot);
        assertThat(dataFiles)
                .as("both fragments must be present at the latest snapshot")
                .hasSize((int) preDataFileCount + 2);
        assertThat(dataFiles)
                .extracting(DucklakeDataFile::path)
                .contains(winnerFragment.path(), loserFragment.path());
    }

    private static DucklakeWriteFragment fragmentFor(String path, long minId, long maxId)
    {
        DucklakeFileColumnStats idStats = new DucklakeFileColumnStats(
                idColumnId,
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
