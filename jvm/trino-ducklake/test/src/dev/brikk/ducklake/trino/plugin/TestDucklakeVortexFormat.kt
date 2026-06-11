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
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager

/**
 * End-to-end `vortex` format round-trip through the connector: CTAS with
 * `data_file_format = 'vortex'` writes a `.vortex` data file (DuckDB `COPY … (FORMAT vortex)`),
 * then SELECT reads it back via the FileScan `read_vortex('path')` path. Also verifies the
 * catalog records `file_format = 'vortex'` and that a predicate over the vortex read works.
 *
 * Network-gated: writing and reading both `INSTALL vortex`. If the extension can't be installed
 * (offline / unsupported platform), the test SKIPS rather than fails.
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeVortexFormat : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "integration-vortex"

    @Test
    fun ctasThenSelectRoundTripsVortex() {
        assumeVortexExtensionAvailable()
        val table = "vortex_roundtrip"
        try {
            computeActual(
                    "CREATE TABLE $table WITH (data_file_format = 'vortex') AS " +
                    "SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) AS t(id, name)")

            // The catalog must record the new data file as vortex-format.
            assertThat(computeScalar("SELECT DISTINCT file_format FROM \"$table\$files\"") as String)
                    .isEqualTo("vortex")

            // Read back through the FileScan(read_vortex) path.
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(3L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alpha", "beta", "gamma")

            // A predicate over the vortex read must filter correctly.
            assertThat(computeActual("SELECT id FROM $table WHERE name = 'beta'").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(2L)
        }
        finally {
            tryDropTable(table)
        }
    }

    /**
     * ARRAY columns through the vortex COPY path: the Arrow-stream writer's ARRAY support is
     * shared with lance, but until now only the lance side was round-trip-tested
     * ([TestDucklakeLanceFormat.embeddingCtasInsertThenVectorSearchRoundTrip]). Pins CTAS +
     * INSERT of `ARRAY(REAL)` (NULL row included) and the value-level read-back through
     * `read_vortex` + the shared Arrow-to-page converter.
     */
    @Test
    fun embeddingArrayCtasInsertRoundTripsVortex() {
        assumeVortexExtensionAvailable()
        val table = "vortex_embedding"
        try {
            computeActual(
                    "CREATE TABLE $table WITH (data_file_format = 'vortex') AS "
                            + "SELECT * FROM (VALUES "
                            + "(1, ARRAY[CAST(1.0 AS REAL), CAST(0.0 AS REAL)]), "
                            + "(2, ARRAY[CAST(0.5 AS REAL), CAST(0.5 AS REAL)])) AS t(id, emb)")
            computeActual("INSERT INTO $table VALUES (3, NULL)")

            assertThat(computeScalar("SELECT DISTINCT file_format FROM \"$table\$files\"") as String)
                    .isEqualTo("vortex")
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(3L)

            @Suppress("UNCHECKED_CAST")
            val embedding = (computeScalar("SELECT emb FROM $table WHERE id = 1") as List<Number>)
                    .map { it.toFloat() }
            assertThat(embedding).containsExactly(1.0f, 0.0f)
            assertThat(computeScalar("SELECT emb FROM $table WHERE id = 3")).isNull()
        }
        finally {
            tryDropTable(table)
        }
    }

    /** ROW columns through the vortex COPY path (probed upstream-supported), null row included. */
    @Test
    fun rowColumnRoundTripsVortex() {
        assumeVortexExtensionAvailable()
        val table = "vortex_row"
        try {
            computeActual(
                    "CREATE TABLE $table WITH (data_file_format = 'vortex') AS "
                            + "SELECT * FROM (VALUES "
                            + "(1, CAST(ROW(10, 'alpha') AS ROW(i INTEGER, s VARCHAR))), "
                            + "(2, CAST(NULL AS ROW(i INTEGER, s VARCHAR)))) AS t(id, st)")
            assertThat(computeScalar("SELECT st.s FROM $table WHERE id = 1") as String)
                    .isEqualTo("alpha")
            assertThat(computeScalar("SELECT st FROM $table WHERE id = 2")).isNull()
        }
        finally {
            tryDropTable(table)
        }
    }

    /**
     * MAP writes are hard-gated for vortex: the DuckDB vortex extension fails NATIVELY (crash or
     * hang, not a clean error — probed 2026-06-11) on MAP COPY, so the writer must reject at
     * schema time before any native code runs.
     */
    @Test
    fun mapColumnIsRejectedForVortexWrites() {
        assumeVortexExtensionAvailable()
        assertThatThrownBy {
            computeActual("CREATE TABLE vortex_map WITH (data_file_format = 'vortex') AS "
                    + "SELECT MAP(ARRAY['a'], ARRAY[1]) AS mp")
        }.hasMessageContaining("vortex write of MAP columns is not supported")
        tryDropTable("vortex_map")
    }

    private fun assumeVortexExtensionAvailable() {
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL vortex")
                    s.execute("LOAD vortex")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "vortex DuckDB extension unavailable (offline / unsupported platform): ${e.message}")
        }
    }
}
