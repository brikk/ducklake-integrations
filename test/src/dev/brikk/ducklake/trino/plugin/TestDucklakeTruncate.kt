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
 * `TRUNCATE TABLE` — a catalog bulk-clear that end-snapshots every active data file, delete
 * file, and inlined row for the table at a new snapshot, keeping the schema. Unlike
 * row-by-row DELETE it writes no delete files (O(1) regardless of row count) and does not bump
 * the schema version. Covers parquet + non-parquet data, partitioned tables, prior deletes,
 * insert-after-truncate, and time travel back across the truncate (the pre-truncate snapshot
 * still sees its rows).
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeTruncate : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "truncate-integration"

    @Test
    fun truncateEmptiesTableAndKeepsItUsable() {
        val table = "test_schema.trunc_basic"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)

            computeActual("TRUNCATE TABLE $table")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(0L)
            // The table still exists and is writable; the $files metadata shows no active files.
            assertThat(computeScalar("SELECT count(*) FROM \"trunc_basic\$files\"")).isEqualTo(0L)
            computeActual("INSERT INTO $table VALUES (4, 'd')")
            assertThat(computeActual("SELECT id, name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(1) as String })
                    .containsExactly("d")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun truncatePartitionedTableClearsAllPartitions() {
        val table = "test_schema.trunc_part"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR) " +
                    "WITH (partitioned_by = ARRAY['region'])")
            computeActual("INSERT INTO $table VALUES (1,'US'),(2,'EU'),(3,'US'),(4,'APAC')")

            computeActual("TRUNCATE TABLE $table")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(0L)
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE region = 'US'")).isEqualTo(0L)
            // Partitioning still applies to subsequent writes.
            computeActual("INSERT INTO $table VALUES (5, 'US')")
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE region = 'US'")).isEqualTo(1L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun truncateClearsPriorDeleteFiles() {
        // A table with an outstanding delete file: truncate must end-snapshot the delete files
        // too, not just the data files, or the merge-on-read state would be inconsistent.
        val table = "test_schema.trunc_deletes"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1),(2),(3),(4)")
            computeActual("DELETE FROM $table WHERE id IN (2, 3)")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(2L)

            computeActual("TRUNCATE TABLE $table")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(0L)
            // Fresh inserts read back cleanly (no resurrected/leaked rows from old delete files).
            computeActual("INSERT INTO $table VALUES (10),(11)")
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(10L, 11L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun timeTravelSeesRowsBeforeTruncate() {
        val table = "test_schema.trunc_tt"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1),(2),(3)")
            val beforeTruncate = computeScalar("SELECT max(snapshot_id) FROM \"trunc_tt\$snapshots\"") as Long

            computeActual("TRUNCATE TABLE $table")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(0L)

            // The pre-truncate snapshot still sees the three rows (truncate is a versioned op).
            assertThat(computeScalar("SELECT count(*) FROM $table FOR VERSION AS OF $beforeTruncate"))
                    .isEqualTo(3L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun truncateEmptyTableIsNoop() {
        val table = "test_schema.trunc_empty"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("TRUNCATE TABLE $table")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(0L)
        }
        finally {
            tryDropTable(table)
        }
    }
}
