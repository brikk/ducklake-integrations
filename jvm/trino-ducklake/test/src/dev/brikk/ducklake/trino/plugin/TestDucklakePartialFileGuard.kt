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
 * Guard for the one partial-file case not yet filtered on read: a consolidated **PUFFIN**
 * (deletion-vector) delete file whose `partial_max` exceeds the read snapshot. Parquet partial data
 * AND delete files are now filtered (see [TestDucklakePartialFileFilter] /
 * [TestDucklakePartialDeleteFilter]); the puffin reader does not yet snapshot-filter, so a
 * time-travel read below such a file's `partial_max` is rejected rather than over-deleting.
 *
 * The gate fires in the split manager BEFORE any delete-file IO, so we simulate the state by
 * setting `format='puffin'` + `partial_max` on a real (parquet) delete-file row — no puffin bytes
 * needed (mirrors the original TestDucklakePuffinDeleteFileGuard technique). See DESIGN-maintenance.md § 6.
 *
 * SAME_THREAD: writes to the shared isolated catalog.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePartialFileGuard : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "partial-file-guard"

    @Throws(Exception::class)
    private fun simulatePuffinPartialDelete(tableName: String, partialMax: Long) {
        openCatalogConnection().use { conn ->
            conn.prepareStatement(
                    "UPDATE ducklake_delete_file SET partial_max = ?, format = 'puffin' WHERE table_id = "
                            + "(SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL)").use { ps ->
                ps.setLong(1, partialMax)
                ps.setString(2, tableName)
                assertThat(ps.executeUpdate()).`as`("simulated puffin partial delete file").isGreaterThanOrEqualTo(1)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun timeTravelBelowPuffinPartialDeleteMaxIsRejected() {
        val table = "test_schema.partial_del_reject"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
            computeActual("DELETE FROM $table WHERE id = 2")   // writes a delete file (begin = s2)
            val s2 = computeScalar("SELECT max(snapshot_id) FROM \"partial_del_reject\$snapshots\"") as Long

            // Simulate a consolidated PUFFIN delete file holding deletions past s2.
            simulatePuffinPartialDelete("partial_del_reject", partialMax = s2 + 1)

            // Read AS OF s2 (where the delete file is active); puffin partial_max > s2 → gated.
            assertThatThrownBy { computeActual("SELECT * FROM $table FOR VERSION AS OF $s2") }
                    .hasMessageContaining("delete file")
                    .hasMessageContaining("snapshot $s2")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    @Throws(Exception::class)
    fun latestReadNotGatedByPuffinPartialDelete() {
        val table = "test_schema.partial_del_latest"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
            computeActual("DELETE FROM $table WHERE id = 2")
            // A normal (non-partial) delete file is never gated and applies at the latest read.
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1, 3)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun normalTableNeverGated() {
        val table = "test_schema.partial_normal"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
            val s1 = computeScalar("SELECT max(snapshot_id) FROM \"partial_normal\$snapshots\"") as Long
            computeActual("INSERT INTO $table VALUES (2, 'b')")
            assertThat(computeActual("SELECT id FROM $table FOR VERSION AS OF $s1").materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1)
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(2L)
        }
        finally {
            tryDropTable(table)
        }
    }
}
