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
 * single file. (Only scalar columns are writable: the Arrow-stream writer's type mapping is
 * scalar-only, so embedding/ARRAY columns must be registered via `add_files` instead — see
 * [TestDucklakeLanceAddFiles].)
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
