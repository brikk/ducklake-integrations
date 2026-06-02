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

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the DuckLake spec invariant that ≤1 active delete file may exist per
 * {@code data_file_id} per snapshot (README:223, checkDeleteFileOverlap:1311-1312).
 *
 * <p>Pre-fix behavior of {@code applyDeleteFragments}: each commitDelete simply
 * inserts a new delete-file row and decrements {@code record_count} by the new
 * file's {@code deleteCount}. After two commits against the same {@code data_file_id}
 * the catalog held two active rows and {@code record_count} was double-decremented
 * if the second fragment was written as the union of (prior ∪ new) positions.
 *
 * <p>Post-fix behavior asserted here:
 * <ul>
 *   <li>The prior active delete file for that {@code data_file_id} is end-snapshotted
 *       in the same transaction (≤1 active row).</li>
 *   <li>{@code record_count} decrements by {@code newDeleteCount} (the delta —
 *       positions added by THIS commit only), not by {@code deleteCount} (the total
 *       union size stored in the new parquet file).</li>
 * </ul>
 *
 * <p>The sink-side coordination (read prior, union with new, write union) lives in
 * {@code DucklakeMergeSink}; this test exercises only the catalog half of the fix by
 * directly invoking {@code commitDelete} with hand-constructed fragments that mimic
 * what a corrected sink would produce. The fragments point to placeholder paths
 * (never opened — catalog-only storage).
 */
public class TestApplyDeleteFragmentsEndSnapshotsPrior
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated;
    private static JdbcDucklakeCatalog catalog;
    private static long tableId;
    private static long dataFileId;
    private static long initialRecordCount;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "apply-delete-fragments-end-snapshot");

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
        dataFileId = catalog.getDataFiles(tableId, snapshotId).getFirst().dataFileId();
        initialRecordCount = catalog.getTableStats(tableId).orElseThrow().recordCount();
        // simple_table is seeded with 5 rows in a single data file
        assertThat(initialRecordCount).isEqualTo(5L);
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
    public void twoCommitsOnSameDataFileLeaveExactlyOneActiveDeleteFile()
            throws Exception
    {
        // First DELETE — one position. No prior delete file; trivially the only active row.
        DucklakeDeleteFragment first = new DucklakeDeleteFragment(
                dataFileId,
                "test_data/apply_delete_end_snapshot_first.parquet",
                /* deleteCount (positions in new file) */ 1L,
                /* fileSizeBytes */ 256L,
                /* footerSize */ 64L,
                /* newDeleteCount (delta for record_count) */ 1L);
        catalog.commitDelete(tableId, List.of(first));

        long activeAfterFirst = countActiveDeleteFiles(dataFileId);
        long recordCountAfterFirst = catalog.getTableStats(tableId).orElseThrow().recordCount();
        assertThat(activeAfterFirst)
                .as("exactly one active delete file after the first commit")
                .isEqualTo(1L);
        assertThat(recordCountAfterFirst)
                .as("record_count decremented by the first delta")
                .isEqualTo(initialRecordCount - 1L);

        // Second DELETE — sink would have unioned the prior position with one new one,
        // so the new file's deleteCount is 2 (total in file) but newDeleteCount is 1 (delta).
        DucklakeDeleteFragment second = new DucklakeDeleteFragment(
                dataFileId,
                "test_data/apply_delete_end_snapshot_second.parquet",
                /* deleteCount (positions in new file = prior ∪ new) */ 2L,
                /* fileSizeBytes */ 256L,
                /* footerSize */ 64L,
                /* newDeleteCount (delta only) */ 1L);
        catalog.commitDelete(tableId, List.of(second));

        long activeAfterSecond = countActiveDeleteFiles(dataFileId);
        long recordCountAfterSecond = catalog.getTableStats(tableId).orElseThrow().recordCount();

        assertThat(activeAfterSecond)
                .as("DuckLake spec: ≤1 active delete file per data_file_id per snapshot — "
                        + "prior file must be end-snapshotted in the same transaction "
                        + "the new (union) file is inserted")
                .isEqualTo(1L);
        assertThat(recordCountAfterSecond)
                .as("record_count must decrement by newDeleteCount (delta), not deleteCount (union total). "
                        + "Decrementing by the union total double-counts the prior delete's positions, "
                        + "which were already subtracted at the first commit.")
                .isEqualTo(initialRecordCount - 2L);
    }

    private long countActiveDeleteFiles(long forDataFileId)
            throws Exception
    {
        var delfile = DUCKLAKE_DELETE_FILE.as("delfile");
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                isolated.jdbcUrl(), isolated.user(), isolated.password())) {
            org.jooq.DSLContext dsl = dev.brikk.ducklake.catalog.testing.CatalogTestSupport.dsl(conn);
            Long count = dsl.selectCount()
                    .from(delfile)
                    .where(delfile.DATA_FILE_ID.eq(forDataFileId))
                    .and(delfile.END_SNAPSHOT.isNull())
                    .fetchOne(0, Long.class);
            return count == null ? 0L : count;
        }
    }
}
