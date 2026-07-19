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
 * Sanity that ordinary (non-partial) delete files and time-travel are never gated. The former
 * partial-PUFFIN read gate is now LIFTED — consolidated puffin delete files are snapshot-filtered
 * per blob (`DucklakePuffinDeleteReader`, pinned by `TestDucklakePuffinDeleteReader` +
 * `TestDucklakePuffinPartialDelete`); only an unknown delete-file format still hard-fails (via
 * `validateDeleteFileFormats`). See DESIGN-maintenance.md § 6.
 *
 * SAME_THREAD: writes to the shared isolated catalog.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePartialFileGuard : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "partial-file-guard"

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
