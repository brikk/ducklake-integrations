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
