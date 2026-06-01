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
 * Gold acceptance test for {@link LogicalConflictCheck}: an INSERT whose
 * fragment column-stats reference a column an intervening {@code DROP COLUMN}
 * end-snapshotted must abort with a {@link LogicalConflictException} naming
 * the dropped column. This is the case lineage-only checking misses — the
 * retry's action would otherwise re-run with the stale fragment payload and
 * insert {@code ducklake_file_column_stats} rows pointing at end-snapshotted
 * columns.
 *
 * <p>Also pins non-retryable behavior: the loser must hit the hook exactly
 * twice (first attempt parked, then retry fails the logical check), not
 * {@code MAX_RETRY_COUNT} times.
 */
public class TestConcurrentInsertVsDropColumn
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;
    private static long tableId;
    private static long nameColumnId;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-vs-drop-column");

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
    public void loserInsertReferencingDroppedColumnFailsLogicalCheck()
            throws Exception
    {
        long preDataFileCount = catalog.getDataFiles(tableId, catalog.getCurrentSnapshotId()).size();

        DucklakeWriteFragment loserFragment = fragmentReferencingColumn(
                "test_data/insert_vs_drop_column_loser.parquet",
                nameColumnId);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.dropColumn(tableId, nameColumnId),
                () -> catalog.commitInsert(tableId, List.of(loserFragment)));

        assertThat(result.loserException())
                .as("loser must abort with a logical conflict naming the dropped column")
                .isInstanceOf(LogicalConflictException.class);
        assertThat(result.loserException().getMessage())
                .as("error message must name the dropped column_id and the table_id")
                .contains(Long.toString(nameColumnId))
                .contains("table_id=" + tableId)
                .contains("DROP COLUMN");

        assertThat(((TransactionConflictException) result.loserException()).retryable())
                .as("logical conflicts are non-retryable")
                .isFalse();

        assertThat(result.loserAttemptCount())
                .as("loser must NOT burn the retry budget — exactly two attempts: parked + retry-failed")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(tableId, latestSnapshot);
        assertThat(dataFiles)
                .as("loser's data file must NOT be present — its INSERT was aborted before commit")
                .extracting(DucklakeDataFile::path)
                .doesNotContain(loserFragment.path());
        assertThat(dataFiles)
                .as("no new data files should land on the table — winner only dropped a column")
                .hasSize((int) preDataFileCount);

        assertThat(catalog.getTableColumns(tableId, latestSnapshot))
                .as("winner's DROP COLUMN must be visible at the latest snapshot")
                .extracting(DucklakeColumn::columnId)
                .doesNotContain(nameColumnId);
    }

    private static DucklakeWriteFragment fragmentReferencingColumn(String path, long columnId)
    {
        DucklakeFileColumnStats colStats = new DucklakeFileColumnStats(
                columnId,
                64L,
                /* valueCount */ 5L,
                /* nullCount */ 0L,
                Optional.of("a"),
                Optional.of("z"),
                /* containsNan */ false);
        return new DucklakeWriteFragment(
                path,
                /* fileSizeBytes */ 1024L,
                /* footerSize */ 64L,
                /* recordCount */ 5L,
                List.of(colStats));
    }
}
