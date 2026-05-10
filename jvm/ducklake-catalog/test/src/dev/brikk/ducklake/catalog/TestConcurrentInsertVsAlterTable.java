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
 * Acceptance test for the {@code tables_inserted_into × altered_tables}
 * matrix entry at {@code ducklake_transaction.cpp:1247}. Distinct from
 * {@link TestConcurrentInsertVsDropColumn}: there, the loser's fragment
 * column-stats reference a column that's been end-snapshotted, so the
 * <em>state-based</em> {@link LogicalConflictCheck} fires. Here T1
 * <em>renames</em> a column the loser's fragment references — the
 * {@code column_id} stays active across rename, so the state-based
 * check passes and the matrix is the only thing that catches the conflict.
 */
public class TestConcurrentInsertVsAlterTable
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
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-vs-alter-table");

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
    public void insertRacingColumnRenameConflicts()
            throws Exception
    {
        DucklakeWriteFragment loserFragment = new DucklakeWriteFragment(
                "test_data/insert_vs_alter_table_loser.parquet",
                /* fileSizeBytes */ 1024L,
                /* footerSize */ 64L,
                /* recordCount */ 5L,
                List.of(new DucklakeFileColumnStats(
                        idColumnId,
                        32L,
                        /* valueCount */ 5L,
                        /* nullCount */ 0L,
                        Optional.of("1"),
                        Optional.of("100"),
                        /* containsNan */ false)));

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.renameColumn(tableId, idColumnId, "id_renamed"),
                () -> catalog.commitInsert(tableId, List.of(loserFragment)));

        assertThat(result.loserException())
                .as("insert racing a column rename must conflict via the matrix "
                        + "(state-based check passes because column_id stays active across rename)")
                .isInstanceOf(LogicalConflictException.class);
        assertThat(result.loserException().getMessage())
                .as("error message must reference the matrix's insert-vs-alter pair")
                .contains("insert into table")
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
                .as("winner's rename visible at the latest snapshot")
                .extracting(DucklakeColumn::columnName)
                .contains("id_renamed");
        assertThat(catalog.getDataFiles(tableId, latestSnapshot))
                .as("loser's data file did NOT land — its INSERT was aborted before commit")
                .extracting(DucklakeDataFile::path)
                .doesNotContain(loserFragment.path());
    }
}
