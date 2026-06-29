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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Guard for cross-snapshot compacted ("partial") files. DuckLake's `merge_adjacent_files` can merge
 * rows from multiple snapshots into one Parquet file that physically carries each row's origin
 * snapshot in a `_ducklake_internal_snapshot_id` column; the file's `ducklake_data_file.partial_max`
 * records the MAX such snapshot. A correct read at snapshot S must filter
 * `_ducklake_internal_snapshot_id <= S` — which this connector does not yet do. So rather than
 * silently OVER-INCLUDE rows newer than S, the split manager rejects a read whose snapshot is below
 * a partial file's `partial_max` (see DESIGN-maintenance.md § 6). Reads at the latest snapshot (or
 * any S >= partial_max) need no filter and must still work.
 *
 * We can't produce a partial file from Trino (the connector never runs merge_adjacent_files and
 * gates its writes), so we simulate the catalog state DuckLake would leave — set `partial_max`
 * directly on the data file — and assert the connector's gate fires for time-travel below it and
 * stays out of the way otherwise.
 *
 * SAME_THREAD: writes to the shared isolated catalog.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePartialFileGuard : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "partial-file-guard"

    @Throws(Exception::class)
    private fun setPartialMax(tableName: String, beginSnapshot: Long, partialMax: Long) {
        openCatalogConnection().use { conn ->
            conn.prepareStatement(
                    "UPDATE ducklake_data_file SET partial_max = ? "
                            + "WHERE begin_snapshot = ? AND table_id = "
                            + "(SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL)").use { ps ->
                ps.setLong(1, partialMax)
                ps.setLong(2, beginSnapshot)
                ps.setString(3, tableName)
                assertThat(ps.executeUpdate()).`as`("partial_max applied to the data file").isEqualTo(1)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun timeTravelBelowPartialMaxIsRejected() {
        val table = "test_schema.partial_guard_reject"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
            val s1 = computeScalar("SELECT max(snapshot_id) FROM \"partial_guard_reject\$snapshots\"") as Long
            computeActual("INSERT INTO $table VALUES (2, 'b')")
            val s2 = computeScalar("SELECT max(snapshot_id) FROM \"partial_guard_reject\$snapshots\"") as Long

            // Simulate: the file written at s1 is now a partial file holding rows up to s2.
            setPartialMax("partial_guard_reject", beginSnapshot = s1, partialMax = s2)

            // Reading AS OF s1 would over-include the s2 rows in that file → must be gated.
            assertThatThrownBy { computeActual("SELECT * FROM $table FOR VERSION AS OF $s1") }
                    .hasMessageContaining("partial")
                    .hasMessageContaining("snapshot $s1")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    @Throws(Exception::class)
    fun latestReadNotGatedWhenSnapshotAtOrAbovePartialMax() {
        val table = "test_schema.partial_guard_latest"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
            val s1 = computeScalar("SELECT max(snapshot_id) FROM \"partial_guard_latest\$snapshots\"") as Long
            computeActual("INSERT INTO $table VALUES (2, 'b')")
            val s2 = computeScalar("SELECT max(snapshot_id) FROM \"partial_guard_latest\$snapshots\"") as Long

            // partial_max == s2: at the latest snapshot every row in the file is valid → no filter
            // needed → no gate.
            setPartialMax("partial_guard_latest", beginSnapshot = s1, partialMax = s2)

            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows.map { it.getField(0) as Int })
                    .`as`("read at latest (snapshot >= partial_max) is not gated").containsExactly(1, 2)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun normalTableNeverGated() {
        val table = "test_schema.partial_guard_normal"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
            val s1 = computeScalar("SELECT max(snapshot_id) FROM \"partial_guard_normal\$snapshots\"") as Long
            computeActual("INSERT INTO $table VALUES (2, 'b')")
            // No partial_max set anywhere → time travel and latest both read normally.
            assertThat(computeActual("SELECT id FROM $table FOR VERSION AS OF $s1").materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1)
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(2L)
        }
        finally {
            tryDropTable(table)
        }
    }
}
