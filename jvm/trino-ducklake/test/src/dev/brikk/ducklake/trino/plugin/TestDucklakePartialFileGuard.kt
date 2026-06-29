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
 * Guard for cross-snapshot consolidated DELETE files. Partial DATA files are now FILTERED on read
 * (see [TestDucklakePartialFileFilter]); the one remaining unhandled case is a partial DELETE file
 * (`ducklake_delete_file.partial_max` set) — its deletions span snapshots and applying it at an
 * older snapshot would over-delete. Until per-row delete-snapshot filtering lands, a time-travel
 * read below such a delete file's `partial_max` is rejected rather than returning wrong rows.
 *
 * We simulate the catalog state by setting `partial_max` directly on a delete file (we can't
 * produce a consolidated partial delete file from Trino), and assert the gate fires below it and
 * stays out of the way at/above it. See DESIGN-maintenance.md § 6.
 *
 * SAME_THREAD: writes to the shared isolated catalog.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePartialFileGuard : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "partial-file-guard"

    @Throws(Exception::class)
    private fun setDeleteFilePartialMax(tableName: String, partialMax: Long) {
        openCatalogConnection().use { conn ->
            conn.prepareStatement(
                    "UPDATE ducklake_delete_file SET partial_max = ? WHERE table_id = "
                            + "(SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL)").use { ps ->
                ps.setLong(1, partialMax)
                ps.setString(2, tableName)
                assertThat(ps.executeUpdate()).`as`("partial_max applied to the delete file").isGreaterThanOrEqualTo(1)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun timeTravelBelowPartialDeleteMaxIsRejected() {
        val table = "test_schema.partial_del_reject"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
            computeActual("DELETE FROM $table WHERE id = 2")   // writes a delete file (begin = s2)
            val s2 = computeScalar("SELECT max(snapshot_id) FROM \"partial_del_reject\$snapshots\"") as Long

            // Simulate the delete file being a consolidated one holding deletions past s2.
            setDeleteFilePartialMax("partial_del_reject", partialMax = s2 + 1)

            // Read AS OF s2 (where the delete file is active); partial_max > s2 → gated.
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
    fun latestReadNotGatedByPartialDelete() {
        val table = "test_schema.partial_del_latest"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
            computeActual("DELETE FROM $table WHERE id = 2")
            val s2 = computeScalar("SELECT max(snapshot_id) FROM \"partial_del_latest\$snapshots\"") as Long
            // partial_max == latest: at the latest snapshot every deletion is valid → no gate.
            setDeleteFilePartialMax("partial_del_latest", partialMax = s2)

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
