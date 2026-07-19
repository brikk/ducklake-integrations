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
 * Upstream issue 1135 semantics: rows written BEFORE `ALTER TABLE ADD COLUMN b
 * ... DEFAULT v` must project the INITIAL default `v` (not NULL), and a filter
 * on `b = v` must match them (no stats row exists for the added column on old
 * files — pruning must retain them). DuckDB performs the ALTER; Trino reads.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeAddColumnDefaultCrossEngine : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "add-column-default-xengine"

    @Test
    @Throws(Exception::class)
    fun addedColumnDefaultProjectsAndFilters() {
        val trino = "test_schema.add_default"
        val duckdb = "ducklake_db.test_schema.add_default"
        try {
            computeActual("CREATE TABLE $trino (a INTEGER)")
            computeActual("INSERT INTO $trino VALUES (1), (2), (3)")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("ALTER TABLE $duckdb ADD COLUMN b INTEGER DEFAULT 42")
                }
            }

            // Stage 1 — projection: old rows must read b = 42, not NULL.
            val projected = computeActual("SELECT a, b FROM $trino ORDER BY a").materializedRows
                    .map { it.getField(0) as Int to it.getField(1) as Int? }
            println("PROJECTED: $projected")
            assertThat(projected).containsExactly(1 to 42, 2 to 42, 3 to 42)

            // Stage 2 — filter: WHERE b = 42 must match all old rows (stats-less
            // column must not prune the file).
            val count = computeActual("SELECT count(*) FROM $trino WHERE b = 42").materializedRows[0].getField(0) as Long
            assertThat(count).`as`("filter on the added-default column").isEqualTo(3L)
        }
        finally {
            tryDropTable(trino)
        }
    }
}
