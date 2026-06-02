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
 * Covers the table-active branch of {@link LogicalConflictCheck}: an INSERT
 * whose payload references a {@code table_id} an intervening {@code DROP TABLE}
 * end-snapshotted must abort with a {@link LogicalConflictException} naming
 * the dropped table. Without this check, the retry's action would re-run and
 * insert {@code ducklake_data_file} rows pointing at a now-dropped table.
 */
public class TestConcurrentInsertVsDropTable
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
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-vs-drop-table");

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
    public void loserInsertReferencingDroppedTableFailsLogicalCheck()
            throws Exception
    {
        DucklakeWriteFragment loserFragment = fragmentReferencingColumn(
                "test_data/insert_vs_drop_table_loser.parquet",
                idColumnId);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.dropTable("test_schema", "simple_table"),
                () -> catalog.commitInsert(tableId, List.of(loserFragment)));

        assertThat(result.loserException())
                .as("loser must abort with a logical conflict naming the dropped table")
                .isInstanceOf(LogicalConflictException.class);
        assertThat(result.loserException().getMessage())
                .as("error message must name the dropped table_id and the cause")
                .contains("table_id=" + tableId)
                .contains("DROP TABLE");

        assertThat(((TransactionConflictException) result.loserException()).retryable())
                .as("logical conflicts are non-retryable")
                .isFalse();

        assertThat(result.loserAttemptCount())
                .as("loser must NOT burn the retry budget — exactly two attempts: parked + retry-failed")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getTable("test_schema", "simple_table", latestSnapshot))
                .as("winner's DROP TABLE must be visible at the latest snapshot")
                .isEmpty();
    }

    private static DucklakeWriteFragment fragmentReferencingColumn(String path, long columnId)
    {
        DucklakeFileColumnStats colStats = new DucklakeFileColumnStats(
                columnId,
                32L,
                /* valueCount */ 5L,
                /* nullCount */ 0L,
                Optional.of("1"),
                Optional.of("100"),
                /* containsNan */ false);
        return new DucklakeWriteFragment(
                path,
                /* fileSizeBytes */ 1024L,
                /* footerSize */ 64L,
                /* recordCount */ 5L,
                List.of(colStats));
    }
}
