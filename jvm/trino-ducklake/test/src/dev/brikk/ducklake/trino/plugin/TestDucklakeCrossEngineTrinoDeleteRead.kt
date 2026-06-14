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
 * The reverse direction of [TestDucklakeCrossEnginePuffinDeleteRoundTrip]: TRINO performs the
 * row-level DELETE/UPDATE and DUCKDB reads the surviving rows. This is the verification spike
 * for a suspected interop hole — Trino's merge sink writes positional delete files as a single
 * `row_id` column holding GLOBAL row ids, while the DuckLake spec (and DuckDB's reader) expects
 * `(file_path, pos)` with FILE-LOCAL positions. Whatever this proves becomes the contract.
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCrossEngineTrinoDeleteRead : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "xengine-trino-delete"

    @Test
    fun bothEnginesAgreeAfterAlternatingDeletes() {
        val table = "test_schema.alternating_deletes"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1), (2), (3), (4), (5), (6)")

            // DuckDB deletes first — a DuckLake-spec (file_path, pos) delete file (inline
            // deletes are off by default for fresh tables; if this ever changes, force it
            // with set_option('data_inlining_row_limit', 0)).
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { it.execute("DELETE FROM ducklake_db.$table WHERE id = 2") }
            }
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L, 4L, 5L, 6L)

            // Trino deletes more: the superseding delete file must UNION the prior DuckDB
            // pos-file positions (B3a) and stay spec-shaped so DuckDB keeps reading.
            computeActual("DELETE FROM $table WHERE id = 4")

            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L, 5L, 6L)
            assertThat(duckdbQueryLongs("SELECT id FROM ducklake_db.$table ORDER BY id"))
                    .`as`("DuckDB read after DuckDB-then-Trino deletes (superseding union)")
                    .containsExactly(1L, 3L, 5L, 6L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun duckdbReadsCorrectSurvivorsAfterTrinoDelete() {
        val table = "test_schema.trino_deleted"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave'), (5, 'eve')")

            computeActual("DELETE FROM $table WHERE id IN (2, 4)")

            // Trino agrees with itself...
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L, 5L)

            // ...and DuckDB must see the same survivors through its own delete-file reader.
            assertThat(duckdbQueryLongs("SELECT id FROM ducklake_db.$table ORDER BY id"))
                    .`as`("DuckDB read after a Trino DELETE (delete-file interop)")
                    .containsExactly(1L, 3L, 5L)
        }
        finally {
            tryDropTable(table)
        }
    }

    /** Runs [sql] on a fresh attached DuckDB connection, returning the single BIGINT column. */
    private fun duckdbQueryLongs(sql: String): List<Long> {
        val ids = mutableListOf<Long>()
        createDuckdbConnection().use { duck ->
            val rs = duck.createStatement().executeQuery(sql)
            while (rs.next()) {
                ids.add(rs.getLong(1))
            }
        }
        return ids
    }
}
