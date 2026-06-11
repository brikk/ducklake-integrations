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
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.sql.DriverManager

/**
 * End-to-end coverage of `lance_fts` + `lance_hybrid_search` (Step 7 remainder / Phase A3),
 * mirroring [TestDucklakeLanceVectorSearch]'s proven shape over one shared fixture: a lance
 * dataset with a text column (`body`) and an embedding column (`emb`), registered via
 * `add_files(file_format => 'lance')`, queried through both table functions.
 *
 * Probe facts this pins (lance extension, June 2026): FTS works WITHOUT an inverted index
 * (brute force); output is matching rows only + `_score FLOAT` descending; the extension treats
 * `k` as best-effort for FTS (may return more than k matches). Hybrid takes both halves
 * positionally `(dir, vec_col, vec, fts_col, query)` and appends `_distance`, `_score`
 * (NULL for rows with no text match), and `_hybrid_score`, descending by hybrid score.
 *
 * Network/platform-gated like the other lance tests (404 on osx_amd64). SAME_THREAD: writes to
 * the shared catalog.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeLanceFtsAndHybridSearch : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "integration-lance-fts-hybrid"

    @Test
    fun ftsReturnsMatchingRowsWithDescendingScore() {
        assumeLanceExtensionAvailable()
        val table = "lance_fts_t"
        writeAndRegisterFixture(table)
        try {
            // 'quick fox' matches id=1 (quick + fox) strongest, id=3 (quick + foxes) weaker;
            // ids 2/4 don't match and must be absent (FTS returns matching rows only).
            val rows = computeActual(
                    "SELECT id, _score FROM TABLE(ducklake.system.lance_fts("
                            + "schema_name => 'test_schema', "
                            + "table_name => '$table', "
                            + "column_name => 'body', "
                            + "query => 'quick fox', "
                            + "k => 5))").materializedRows
            assertThat(rows.map { (it.getField(0) as Number).toLong() })
                    .`as`("matching rows, descending by BM25 score")
                    .containsExactly(1L, 3L)
            val scores = rows.map { (it.getField(1) as Number).toFloat() }
            assertThat(scores[0]).isGreaterThan(scores[1])
            assertThat(scores[1]).isGreaterThan(0.0f)

            // No-match query → zero rows.
            assertThat(computeActual(
                    "SELECT id FROM TABLE(ducklake.system.lance_fts("
                            + "'test_schema', '$table', 'body', 'zebra unicorn', 5))").materializedRows)
                    .isEmpty()

            // Composes with ordinary SQL above it (the documented exact-top-k recipe).
            assertThat(computeActual(
                    "SELECT id FROM TABLE(ducklake.system.lance_fts("
                            + "'test_schema', '$table', 'body', 'quick fox', 5)) "
                            + "ORDER BY _score DESC LIMIT 1").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun ftsValidatesArguments() {
        assumeLanceExtensionAvailable()
        val table = "lance_fts_args"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, body VARCHAR)")
            assertQueryFails(
                    "SELECT * FROM TABLE(ducklake.system.lance_fts("
                            + "'test_schema', '$table', 'id', 'fox', 5))",
                    ".*not a text \\(varchar\\) column.*")
            assertQueryFails(
                    "SELECT * FROM TABLE(ducklake.system.lance_fts("
                            + "'test_schema', '$table', 'body', 'fox', 0))",
                    ".*K must be positive.*")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun hybridSearchCombinesVectorAndTextScores() {
        assumeLanceExtensionAvailable()
        val table = "lance_hybrid_t"
        writeAndRegisterFixture(table)
        try {
            // Vector half points at id=1 ([1,0]); text half also favors id=1 ('quick fox').
            // k=3 brings in id=4 ([0.5,0.5], no text match) whose _score must be NULL while
            // _distance and _hybrid_score are populated.
            val rows = computeActual(
                    "SELECT id, _distance, _score, _hybrid_score FROM TABLE(ducklake.system.lance_hybrid_search("
                            + "schema_name => 'test_schema', "
                            + "table_name => '$table', "
                            + "vector_column => 'emb', "
                            + "query_vec => ARRAY[1.0, 0.0], "
                            + "text_column => 'body', "
                            + "query => 'quick fox', "
                            + "k => 3))").materializedRows
            assertThat(rows).hasSize(3)
            assertThat((rows[0].getField(0) as Number).toLong())
                    .`as`("id=1 wins both halves → top hybrid score")
                    .isEqualTo(1L)
            val hybridScores = rows.map { (it.getField(3) as Number).toFloat() }
            assertThat(hybridScores)
                    .`as`("descending by _hybrid_score")
                    .isSortedAccordingTo(reverseOrder())
            val noTextMatchRow = rows.first { (it.getField(0) as Number).toLong() == 4L }
            assertThat(noTextMatchRow.getField(2))
                    .`as`("_score is NULL for rows with no text match")
                    .isNull()
            assertThat(noTextMatchRow.getField(1)).isNotNull()

            // ALPHA is forwarded when given (1.0 = vector-only weighting still returns rows).
            assertThat(computeActual(
                    "SELECT id FROM TABLE(ducklake.system.lance_hybrid_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0, 0.0], 'body', 'quick fox', 3, 1.0, false)) "
                            + "ORDER BY _hybrid_score DESC LIMIT 1").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L)

            assertQueryFails(
                    "SELECT * FROM TABLE(ducklake.system.lance_hybrid_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0, 0.0], 'body', 'fox', 3, 1.5, false))",
                    ".*ALPHA must be between 0 and 1.*")
        }
        finally {
            tryDropTable(table)
        }
    }

    /**
     * Writes the shared 4-row lance dataset (text + 2-dim embedding) out-of-band and registers
     * it as [table] via `add_files(file_format => 'lance')`.
     */
    private fun writeAndRegisterFixture(table: String) {
        val dataset = Files.createTempDirectory("lance-fts-hybrid").resolve("$table.lance")
        val escaped = dataset.toString().replace("'", "''")
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("INSTALL lance")
                s.execute("LOAD lance")
                s.execute("CREATE TABLE t AS SELECT * FROM (VALUES "
                        + "(1, 'the quick brown fox jumps', [1.0::FLOAT, 0.0::FLOAT]), "
                        + "(2, 'lazy dogs sleep all day',   [0.0::FLOAT, 1.0::FLOAT]), "
                        + "(3, 'quick silver foxes run',    [0.7::FLOAT, 0.3::FLOAT]), "
                        + "(4, 'the cat sat on the mat',    [0.5::FLOAT, 0.5::FLOAT])) v(id, body, emb)")
                s.execute("COPY t TO '$escaped' (FORMAT lance)")
            }
        }
        computeActual("CREATE TABLE $table (id INTEGER, body VARCHAR, emb ARRAY(REAL))")
        val datasetPath = dataset.toAbsolutePath().toString().replace("'", "''")
        computeActual(
                "CALL ducklake.system.add_files("
                        + "schema_name => 'test_schema', "
                        + "table_name => '$table', "
                        + "files => ARRAY['$datasetPath'], "
                        + "file_format => 'lance')")
    }

    private fun assumeLanceExtensionAvailable() {
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL lance")
                    s.execute("LOAD lance")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "lance DuckDB extension unavailable (offline / unsupported platform — 404 on osx_amd64): ${e.message}")
        }
    }

    companion object {
        private fun reverseOrder(): Comparator<Float> = Comparator.reverseOrder()
    }
}
