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
package dev.brikk.ducklake.trino.plugin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Pins what the connector commits to the catalog around an INSERT: a fresh snapshot row,
 * a `changes_made` entry containing `inserted_into_table:`, a `$files`
 * row with the right Parquet metadata, and a non-empty `SHOW STATS` response.
 *
 *
 * These assertions are about catalog/snapshot side effects of writes, not about row
 * data. Pure data-in / data-out lives in [TestDucklakeInsertAndCtas]; partition-
 * specific writes live in [TestDucklakePartitionedWrite].
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeWriteMetadataAndSnapshot : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "write-metadata-and-snapshot"
    }

    @Test
    fun testSnapshotMetadataAfterInsert() {
        computeActual("CREATE TABLE test_schema.snapshot_verify (id INTEGER, name VARCHAR)")
        try {
            // Get snapshot before insert
            val beforeInsert = computeActual(
                    "SELECT max(snapshot_id) FROM \"simple_table\$snapshots\"")
            val snapshotBeforeInsert = beforeInsert.materializedRows[0].getField(0) as Long

            computeActual("INSERT INTO test_schema.snapshot_verify VALUES (1, 'test')")

            // Verify new snapshot was created
            val afterInsert = computeActual(
                    "SELECT max(snapshot_id) FROM \"simple_table\$snapshots\"")
            val snapshotAfterInsert = afterInsert.materializedRows[0].getField(0) as Long
            assertThat(snapshotAfterInsert).isGreaterThan(snapshotBeforeInsert)

            // Verify change record
            val changes = computeActual(
                    "SELECT changes_made FROM \"simple_table\$snapshot_changes\" WHERE snapshot_id = " + snapshotAfterInsert)
            assertThat(changes.rowCount).isGreaterThan(0)
            assertThat(changes.materializedRows[0].getField(0).toString()).contains("inserted_into_table:")
        }
        finally {
            tryDropTable("test_schema.snapshot_verify")
        }
    }

    @Test
    fun testDataFileMetadataAfterInsert() {
        computeActual("CREATE TABLE test_schema.file_verify (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.file_verify VALUES (1, 'test'), (2, 'test2')")

            val files = computeActual("SELECT * FROM \"file_verify\$files\"")
            assertThat(files.rowCount).isGreaterThanOrEqualTo(1)

            // Verify the file has correct record count
            val fileCounts = computeActual(
                    "SELECT record_count, file_format FROM \"file_verify\$files\"")
            val fileRow = fileCounts.materializedRows[0]
            assertThat(fileRow.getField(0) as Long).isEqualTo(2L)
            assertThat(fileRow.getField(1)).isEqualTo("parquet")
        }
        finally {
            tryDropTable("test_schema.file_verify")
        }
    }

    @Test
    fun testTableStatsAfterInsert() {
        computeActual("CREATE TABLE test_schema.stats_verify (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.stats_verify VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")

            // Verify the data is actually there and countable
            val countResult = computeActual("SELECT count(*) FROM test_schema.stats_verify")
            assertThat(countResult.materializedRows[0].getField(0)).isEqualTo(5L)

            // Verify SHOW STATS returns something (exact format varies)
            val stats = computeActual("SHOW STATS FOR test_schema.stats_verify")
            assertThat(stats.rowCount).isGreaterThan(0)
        }
        finally {
            tryDropTable("test_schema.stats_verify")
        }
    }
}
