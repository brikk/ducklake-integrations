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
import java.sql.DriverManager

/**
 * End-to-end `lance` format round-trip through the connector: CTAS with `data_file_format = 'lance'`
 * writes a `.lance` dataset *directory* (DuckDB `COPY … (FORMAT lance)` to local temp, then the
 * Arrow-stream writer walks + uploads every file in the directory — see [DuckDbArrowStreamFileWriter]
 * with FORMAT_LANCE), then SELECT reads it back via the FileScan `__lance_scan('<dir>')` path. Also
 * verifies the catalog records `file_format = 'lance'`, that a predicate pushes down, and that a
 * second write (INSERT → a second dataset directory) reads back correctly.
 *
 * This is the sibling of [TestDucklakeVortexFormat] for Lance — the key difference being that lance
 * produces a directory, so the writer's upload/size/cleanup walk the tree rather than handle a
 * single file. Embedding (ARRAY-of-scalar) columns are writable too — the Arrow-stream writer maps
 * them to Arrow List and lance materializes uniform float lists as FixedSizeList — so embeddings
 * can be CTAS'd/INSERTed directly and then searched via `lance_vector_search` (see the embedding
 * round-trip test below; `add_files` registration remains for externally-produced datasets).
 *
 * Network/platform-gated: writing and reading both `INSTALL lance` (404 on osx_amd64); skips if the
 * extension is unavailable. PostgreSQL-catalog testcontainer + in-process reads, so this runs on
 * Apple Silicon without the amd64-only Quack parity container.
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeLanceFormat : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "integration-lance"

    @Test
    fun ctasThenSelectRoundTripsLance() {
        assumeLanceExtensionAvailable()
        val table = "lance_roundtrip"
        try {
            computeActual(
                    "CREATE TABLE $table WITH (data_file_format = 'lance') AS "
                            + "SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) AS t(id, name)")

            // The catalog must record the new data file as lance-format.
            assertThat(computeScalar("SELECT DISTINCT file_format FROM \"$table\$files\"") as String)
                    .isEqualTo("lance")

            // Read back through the FileScan(__lance_scan) path.
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(3L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alpha", "beta", "gamma")

            // A predicate over the lance read must push down and filter correctly.
            assertThat(computeActual("SELECT id FROM $table WHERE name = 'beta'").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(2L)

            // A second write produces a second lance dataset directory; both must read back.
            computeActual("INSERT INTO $table VALUES (4, 'delta')")
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(4L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alpha", "beta", "gamma", "delta")
        }
        finally {
            tryDropTable(table)
        }
    }

    /**
     * The closing of the lance Route-A loop: embeddings written BY TRINO (CTAS + INSERT through
     * the Arrow-stream writer's new ARRAY support) and searched by the `lance_vector_search`
     * table function — no out-of-band dataset, no `add_files`. CTAS and INSERT produce two
     * dataset fragments, so the search runs per-fragment and `ORDER BY _distance LIMIT n` gives
     * the exact global answer. Also pins NULL array rows (writable; unwritten Arrow slot) and
     * the ARRAY(REAL) read-back through `__lance_scan`.
     */
    @Test
    fun embeddingCtasInsertThenVectorSearchRoundTrip() {
        assumeLanceExtensionAvailable()
        val table = "lance_embedding_ctas"
        try {
            computeActual(
                    "CREATE TABLE $table WITH (data_file_format = 'lance') AS "
                            + "SELECT * FROM (VALUES "
                            + "(1, 'alpha', ARRAY[CAST(1.0 AS REAL), CAST(0.0 AS REAL)]), "
                            + "(2, 'beta',  ARRAY[CAST(0.0 AS REAL), CAST(1.0 AS REAL)])) AS t(id, name, emb)")
            computeActual("INSERT INTO $table VALUES "
                    + "(3, 'gamma', ARRAY[CAST(0.9 AS REAL), CAST(0.1 AS REAL)]), "
                    + "(4, 'delta', NULL)")

            assertThat(computeScalar("SELECT DISTINCT file_format FROM \"$table\$files\"") as String)
                    .isEqualTo("lance")
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(4L)

            // The embedding column round-trips by value through __lance_scan, NULL row included.
            @Suppress("UNCHECKED_CAST")
            val embedding = (computeScalar("SELECT emb FROM $table WHERE id = 1") as List<Number>)
                    .map { it.toFloat() }
            assertThat(embedding).containsExactly(1.0f, 0.0f)
            assertThat(computeScalar("SELECT emb FROM $table WHERE id = 4")).isNull()

            // Trino-written embeddings, searched by the table function: nearest to [1,0] is the
            // CTAS row id=1 (distance 0), then the INSERTed row id=3 across the second fragment.
            assertThat(computeActual(
                    "SELECT id FROM TABLE(ducklake.system.lance_vector_search("
                            + "'test_schema', '$table', 'emb', ARRAY[1.0, 0.0], 2)) "
                            + "ORDER BY _distance LIMIT 2").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L)
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
