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
 * End-to-end coverage of the connector's first table function (Step 7 / Phase A3):
 * `SELECT ... FROM TABLE(ducklake.system.lance_vector_search(...))` over a lance dataset
 * registered via `add_files(file_format => 'lance')`. Exercises the whole novel SPI chain —
 * `ConnectorTableFunction.analyze` (including the `ARRAY(DOUBLE)` scalar `query_vec` argument),
 * `ConnectorSplitManager.getSplits(ConnectorTableFunctionHandle)`, and the
 * `TableFunctionSplitProcessor` running DuckDB's `lance_vector_search`.
 *
 * The fixture's `emb` vectors are chosen so distances to the query `[1, 0, 0]` are unambiguous:
 * id=1 is exact (distance 0), id=4 is near, ids 2/3 are far.
 *
 * Network/platform-gated like the other lance tests: needs `INSTALL lance` (404 on osx_amd64).
 * PostgreSQL-testcontainer catalog + in-process executor, so it runs on Apple Silicon.
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeLanceVectorSearch : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "integration-lance-vector-search"

    @Test
    fun vectorSearchReturnsNearestRowsWithDistance() {
        assumeLanceExtensionAvailable()
        val table = "lance_vs"

        // Externally-written lance dataset with an embedding column; names must match the table.
        val dataset = Files.createTempDirectory("lance-vector-search").resolve("$table.lance")
        val escaped = dataset.toString().replace("'", "''")
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("INSTALL lance")
                s.execute("LOAD lance")
                s.execute("CREATE TABLE t AS SELECT * FROM (VALUES "
                        + "(1, 'alpha', [1.0::FLOAT, 0.0::FLOAT, 0.0::FLOAT]), "
                        + "(2, 'beta',  [0.0::FLOAT, 1.0::FLOAT, 0.0::FLOAT]), "
                        + "(3, 'gamma', [0.0::FLOAT, 0.0::FLOAT, 1.0::FLOAT]), "
                        + "(4, 'delta', [0.9::FLOAT, 0.1::FLOAT, 0.0::FLOAT])) v(id, name, emb)")
                s.execute("COPY t TO '$escaped' (FORMAT lance)")
            }
        }

        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR, emb ARRAY(REAL))")
            val datasetPath = dataset.toAbsolutePath().toString().replace("'", "''")
            computeActual(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => '$table', "
                            + "files => ARRAY['$datasetPath'], "
                            + "file_format => 'lance')")

            // Top-2 nearest to [1,0,0]: exact match id=1 (distance 0), then id=4. The bare
            // ARRAY[...] literal is array(decimal)/array(double) — analysis must coerce it
            // to the declared ARRAY(DOUBLE) argument type.
            val rows = computeActual(
                    "SELECT id, name, _distance FROM TABLE(ducklake.system.lance_vector_search("
                            + "schema_name => 'test_schema', "
                            + "table_name => '$table', "
                            + "column_name => 'emb', "
                            + "query_vec => ARRAY[1.0, 0.0, 0.0], "
                            + "k => 2))").materializedRows
            assertThat(rows).hasSize(2)
            assertThat(rows.map { (it.getField(0) as Number).toLong() })
                    .`as`("nearest two ids, ascending by distance")
                    .containsExactly(1L, 4L)
            assertThat(rows.map { it.getField(1) as String }).containsExactly("alpha", "delta")
            val distances = rows.map { (it.getField(2) as Number).toFloat() }
            assertThat(distances[0]).`as`("exact match distance").isEqualTo(0.0f)
            assertThat(distances[1]).`as`("distances ascend").isGreaterThan(0.0f)

            // SELECT * exposes all table columns + _distance, embedding included (ARRAY(REAL)
            // through the shared Arrow converter).
            val star = computeActual(
                    "SELECT * FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'emb', ARRAY[0.0, 1.0, 0.0], 1))").materializedRows
            assertThat(star).hasSize(1)
            assertThat((star[0].getField(0) as Number).toLong()).isEqualTo(2L)
            assertThat(star[0].getField(1)).isEqualTo("beta")
            @Suppress("UNCHECKED_CAST")
            val embedding = (star[0].getField(2) as List<Number>).map { it.toFloat() }
            assertThat(embedding).containsExactly(0.0f, 1.0f, 0.0f)

            // The function output composes with ordinary SQL above it.
            val filtered = computeActual(
                    "SELECT id FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0, 0.0, 0.0], 3)) "
                            + "WHERE id <> 1 ORDER BY _distance LIMIT 1").materializedRows
            assertThat(filtered.map { (it.getField(0) as Number).toLong() }).containsExactly(4L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun vectorSearchValidatesArguments() {
        assumeLanceExtensionAvailable()
        val table = "lance_vs_args"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, emb ARRAY(REAL))")

            assertQueryFails(
                    "SELECT * FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'nope', ARRAY[1.0], 2))",
                    ".*Column 'nope' not found.*")
            assertQueryFails(
                    "SELECT * FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'id', ARRAY[1.0], 2))",
                    ".*not an embedding \\(array\\) column.*")
            assertQueryFails(
                    "SELECT * FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0], 0))",
                    ".*K must be positive.*")
            assertQueryFails(
                    "SELECT * FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', 'no_such_table', 'emb', ARRAY[1.0], 2))",
                    ".*Table not found.*")

            // Empty table (no data files): analysis succeeds, zero splits, zero rows.
            assertThat(computeActual(
                    "SELECT * FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0, 0.0, 0.0], 2))").materializedRows)
                    .isEmpty()
        }
        finally {
            tryDropTable(table)
        }
    }

    /**
     * A table with several lance dataset fragments searches each fragment independently (one
     * split per dataset directory): k=1 over two fragments returns one row PER fragment — a
     * superset of the global top-1 — and `ORDER BY _distance LIMIT 1` recovers the exact answer.
     * This pins the documented per-fragment top-k contract.
     */
    @Test
    fun vectorSearchOverMultipleFragmentsReturnsPerFragmentTopK() {
        assumeLanceExtensionAvailable()
        val table = "lance_vs_multi"

        val dir = Files.createTempDirectory("lance-vector-search-multi")
        val datasets = listOf(
                Triple(dir.resolve("a.lance"), 1, "[1.0::FLOAT, 0.0::FLOAT, 0.0::FLOAT]"),
                Triple(dir.resolve("b.lance"), 2, "[0.0::FLOAT, 1.0::FLOAT, 0.0::FLOAT]"))
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("INSTALL lance")
                s.execute("LOAD lance")
                for ((path, id, emb) in datasets) {
                    val escaped = path.toString().replace("'", "''")
                    s.execute("CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES ($id, $emb)) v(id, emb)")
                    s.execute("COPY t TO '$escaped' (FORMAT lance)")
                }
            }
        }

        try {
            computeActual("CREATE TABLE $table (id INTEGER, emb ARRAY(REAL))")
            val files = datasets.joinToString(", ") { "'${it.first.toAbsolutePath().toString().replace("'", "''")}'" }
            computeActual(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => '$table', "
                            + "files => ARRAY[$files], "
                            + "file_format => 'lance')")

            // k=1, two fragments: one local winner each — both rows come back.
            assertThat(computeActual(
                    "SELECT id FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0, 0.0, 0.0], 1))").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactlyInAnyOrder(1L, 2L)

            // The documented recipe for exact global top-k.
            assertThat(computeActual(
                    "SELECT id FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0, 0.0, 0.0], 1)) "
                            + "ORDER BY _distance LIMIT 1").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L)
        }
        finally {
            tryDropTable(table)
        }
    }

    /** A parquet-format table must be rejected — vector search runs only over lance datasets. */
    @Test
    fun vectorSearchRejectsNonLanceTable() {
        assumeLanceExtensionAvailable()
        val table = "lance_vs_parquet"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, emb ARRAY(REAL))")
            computeActual("INSERT INTO $table VALUES (1, ARRAY[CAST(1.0 AS REAL)])")
            assertQueryFails(
                    "SELECT * FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0], 2))",
                    ".*requires every data file .* to be lance-format.*")
        }
        finally {
            tryDropTable(table)
        }
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
}
