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
package dev.brikk.ducklake.trino.plugin;

import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins what the connector commits to the catalog around an INSERT: a fresh snapshot row,
 * a {@code changes_made} entry containing {@code inserted_into_table:}, a {@code $files}
 * row with the right Parquet metadata, and a non-empty {@code SHOW STATS} response.
 *
 * <p>These assertions are about catalog/snapshot side effects of writes, not about row
 * data. Pure data-in / data-out lives in {@link TestDucklakeInsertAndCtas}; partition-
 * specific writes live in {@link TestDucklakePartitionedWrite}.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeWriteMetadataAndSnapshot
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "write-metadata-and-snapshot";
    }

    @Test
    public void testSnapshotMetadataAfterInsert()
    {
        computeActual("CREATE TABLE test_schema.snapshot_verify (id INTEGER, name VARCHAR)");
        try {
            // Get snapshot before insert
            MaterializedResult beforeInsert = computeActual(
                    "SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
            long snapshotBeforeInsert = (long) beforeInsert.getMaterializedRows().get(0).getField(0);

            computeActual("INSERT INTO test_schema.snapshot_verify VALUES (1, 'test')");

            // Verify new snapshot was created
            MaterializedResult afterInsert = computeActual(
                    "SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
            long snapshotAfterInsert = (long) afterInsert.getMaterializedRows().get(0).getField(0);
            assertThat(snapshotAfterInsert).isGreaterThan(snapshotBeforeInsert);

            // Verify change record
            MaterializedResult changes = computeActual(
                    "SELECT changes_made FROM \"simple_table$snapshot_changes\" WHERE snapshot_id = " + snapshotAfterInsert);
            assertThat(changes.getRowCount()).isGreaterThan(0);
            assertThat(changes.getMaterializedRows().get(0).getField(0).toString()).contains("inserted_into_table:");
        }
        finally {
            tryDropTable("test_schema.snapshot_verify");
        }
    }

    @Test
    public void testDataFileMetadataAfterInsert()
    {
        computeActual("CREATE TABLE test_schema.file_verify (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.file_verify VALUES (1, 'test'), (2, 'test2')");

            MaterializedResult files = computeActual("SELECT * FROM \"file_verify$files\"");
            assertThat(files.getRowCount()).isGreaterThanOrEqualTo(1);

            // Verify the file has correct record count
            MaterializedResult fileCounts = computeActual(
                    "SELECT record_count, file_format FROM \"file_verify$files\"");
            MaterializedRow fileRow = fileCounts.getMaterializedRows().get(0);
            assertThat((long) fileRow.getField(0)).isEqualTo(2L);
            assertThat(fileRow.getField(1)).isEqualTo("parquet");
        }
        finally {
            tryDropTable("test_schema.file_verify");
        }
    }

    @Test
    public void testTableStatsAfterInsert()
    {
        computeActual("CREATE TABLE test_schema.stats_verify (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.stats_verify VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");

            // Verify the data is actually there and countable
            MaterializedResult countResult = computeActual("SELECT count(*) FROM test_schema.stats_verify");
            assertThat(countResult.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);

            // Verify SHOW STATS returns something (exact format varies)
            MaterializedResult stats = computeActual("SHOW STATS FOR test_schema.stats_verify");
            assertThat(stats.getRowCount()).isGreaterThan(0);
        }
        finally {
            tryDropTable("test_schema.stats_verify");
        }
    }
}
