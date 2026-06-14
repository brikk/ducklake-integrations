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
 * Row-level DELETE/UPDATE/MERGE over `lance` dataset directories — see
 * [AbstractDucklakeRowLevelFormatTest] for the test matrix. Reads run through the FileScan
 * `__lance_scan('<dir>')` path, so position-delete filtering depends on the lance scan
 * returning rows in stable dataset order across scans.
 *
 * Also pins the lance-search v1 gate: the search PTFs reject tables with row-level deletes
 * (positions returned by the search functions would bypass the delete filter), and that
 * rejection must stay a clean error — loosening it is a deliberate act once plain reads
 * over deleted lance data are proven (which this suite does).
 *
 * Network/platform-gated like [TestDucklakeLanceFormat]: skips when the lance extension
 * can't be installed (404 on osx_amd64). SAME_THREAD: writes to the shared catalog;
 * concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeRowLevelLanceFormat : AbstractDucklakeRowLevelFormatTest() {
    override fun isolatedCatalogName(): String = "rowlevel-lance"

    override fun format(): String = DucklakeSessionProperties.FORMAT_LANCE

    override fun assumeFormatAvailable() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("lance")
    }

    @Test
    fun lanceSearchRejectsTablesWithRowLevelDeletes() {
        val table = "rl_lance_search_gate"
        try {
            computeActual("CREATE TABLE $table WITH (data_file_format = 'lance') AS "
                    + "SELECT * FROM (VALUES "
                    + "(1, ARRAY[CAST(1.0 AS REAL), CAST(0.0 AS REAL)]), "
                    + "(2, ARRAY[CAST(0.0 AS REAL), CAST(1.0 AS REAL)]), "
                    + "(3, ARRAY[CAST(0.5 AS REAL), CAST(0.5 AS REAL)])) AS t(id, emb)")
            computeActual("DELETE FROM $table WHERE id = 2")

            // Plain reads must already apply the delete...
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L)

            // ...while the search PTFs still gate on row-level deletes (v1 scope).
            assertThatThrownBy {
                computeActual("SELECT id FROM TABLE(ducklake.system.lance_vector_search("
                        + "'test_schema', '$table', 'emb', ARRAY[1.0, 0.0], 2))")
            }.hasMessageContaining("row-level deletes")
        }
        finally {
            tryDropTable(table)
        }
    }
}
