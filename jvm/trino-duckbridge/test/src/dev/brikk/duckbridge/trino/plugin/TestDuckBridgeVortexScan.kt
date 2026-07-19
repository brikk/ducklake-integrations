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
package dev.brikk.duckbridge.trino.plugin

import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Integration tests for the P5 `vortex_scan` PTF. Ported from the DuckLake `TestDucklakeVortex*`
 * suites; rewritten against a plain vortex file written directly via DuckDB, then queried through the
 * duckbridge PTF. Requires the DuckDB `vortex` extension (network-gated). Skips cleanly when absent.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDuckBridgeVortexScan : AbstractTestQueryFramework() {
    private lateinit var vortexFile: Path
    private var vortexAvailable = false

    override fun createQueryRunner(): QueryRunner {
        vortexFile = Files.createTempFile("duckbridge-vortex-", ".vortex")
        Files.delete(vortexFile)
        vortexAvailable = writeVortexFixture()
        return DuckBridgeQueryRunner.create(
            DuckBridgeQueryRunner.freshDatabaseUrl(),
            mapOf("duckbridge.vortex.enabled" to "true", "duckbridge.parity.enabled" to "false"),
        )
    }

    private fun writeVortexFixture(): Boolean =
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { s ->
                    s.execute("INSTALL vortex")
                    s.execute("LOAD vortex")
                    val f = vortexFile.toString().replace("'", "''")
                    s.execute(
                        "COPY (SELECT * FROM (VALUES (1,'apple'),(2,'bänana'),(3,'çherry 世界')) AS t(id, name)) " +
                            "TO '$f' (FORMAT vortex)",
                    )
                }
            }
            true
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") e: Exception) {
            // Fixture-availability signal: vortex not installable → tests assumeTrue and skip.
            false
        }

    @AfterAll
    fun cleanup() {
        if (::vortexFile.isInitialized) {
            Files.deleteIfExists(vortexFile)
        }
    }

    private fun scan(): String =
        "TABLE(${DuckBridgeQueryRunner.CATALOG}.system.vortex_scan(path => '$vortexFile'))"

    @Test
    fun vortexScanReturnsAllRowsWithUnicode() {
        assumeTrue(vortexAvailable, "DuckDB vortex extension not installable — skipping")
        val rows = computeActual("SELECT id, name FROM ${scan()} ORDER BY id").materializedRows
        assertThat(rows.map { it.getField(0) as Int }).containsExactly(1, 2, 3)
        assertThat(rows.map { it.getField(1) as String }).containsExactly("apple", "bänana", "çherry 世界")
    }

    @Test
    fun vortexScanProjectionAndFilter() {
        assumeTrue(vortexAvailable, "DuckDB vortex extension not installable — skipping")
        // Filter applied above the PTF (no domain pushdown into the scan SQL).
        val names =
            computeActual("SELECT name FROM ${scan()} WHERE id >= 2 ORDER BY id").materializedRows.map { it.getField(0) as String }
        assertThat(names).containsExactly("bänana", "çherry 世界")
    }

    @Test
    fun vortexScanCount() {
        assumeTrue(vortexAvailable, "DuckDB vortex extension not installable — skipping")
        assertThat(computeActual("SELECT count(*) FROM ${scan()}").materializedRows.single().getField(0)).isEqualTo(3L)
    }
}
