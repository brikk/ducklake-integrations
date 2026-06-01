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
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mirrors {@link TestConcurrentInsertVsDropColumn} for {@code commitAddFiles}.
 * Pins that {@code add_files} participates in the conflict matrix the same way
 * as {@code INSERT} — both record {@code WriteChange.InsertedIntoTable}, so the
 * logical conflict check rejects a fragment whose column-stats reference a
 * column an intervening {@code DROP COLUMN} end-snapshotted.
 */
public class TestConcurrentAddFilesVsDropColumn
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
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-add-files-vs-drop-column");

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
    public void loserAddFilesReferencingDroppedColumnFailsLogicalCheck()
            throws Exception
    {
        DucklakeWriteFragment loserFragment = addFilesFragment(
                "/abs/path/add_files_vs_drop_column_loser.parquet",
                nameColumnId);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.dropColumn(tableId, nameColumnId),
                () -> catalog.commitAddFiles(tableId, List.of(loserFragment)));

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
                .as("loser's data file must NOT be present — its add_files was aborted before commit")
                .extracting(DucklakeDataFile::path)
                .doesNotContain(loserFragment.path());
    }

    /**
     * Build an add_files-shape fragment: absolute path, a name-map that references
     * the (about-to-be-dropped) column by source name → target field_id, and column
     * stats keyed by the same target field_id. The logical-conflict check inspects
     * the column-stats payload, so this faithfully reproduces what
     * {@code DucklakeAddFilesProcedure} sends in production.
     */
    private static DucklakeWriteFragment addFilesFragment(String path, long columnId)
    {
        DucklakeFileColumnStats colStats = new DucklakeFileColumnStats(
                columnId,
                64L,
                5L,
                0L,
                Optional.of("a"),
                Optional.of("z"),
                false);
        DucklakeNameMap nameMap = new DucklakeNameMap(List.of(
                new DucklakeNameMapEntry("name", columnId)));
        return new DucklakeWriteFragment(
                path,
                /* pathIsRelative */ false,
                "parquet",
                /* fileSizeBytes */ 1024L,
                /* footerSize */ 64L,
                /* recordCount */ 5L,
                List.of(colStats),
                Map.of(),
                OptionalLong.empty(),
                Optional.of(nameMap));
    }
}
