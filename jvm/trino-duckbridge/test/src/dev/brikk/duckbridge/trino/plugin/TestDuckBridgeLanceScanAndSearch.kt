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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Integration tests for the P5 lance scan + search PTFs through a duckbridge catalog. Ported from
 * the DuckLake `TestLanceSearchSql` / `TestDucklakeLance*` suites; rewritten against a plain lance
 * dataset written directly via DuckDB (the fixture writes it with the extension's `COPY … (FORMAT
 * lance)`), then queried through duckbridge PTFs.
 *
 * Requires the DuckDB `lance` extension to be installable (network). Skips cleanly when it isn't.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDuckBridgeLanceScanAndSearch : AbstractTestQueryFramework() {
    private lateinit var datasetDir: Path
    private lateinit var datasetPath: String
    private var lanceAvailable = false

    override fun createQueryRunner(): QueryRunner {
        datasetDir = Files.createTempDirectory("duckbridge-lance-")
        datasetPath = datasetDir.resolve("data.lance").toString()
        lanceAvailable = writeLanceFixture()
        // Enable the lance PTFs; parity off keeps the fixture minimal (the lance extension is the
        // only one under test here). connection-url is embedded T1.
        return DuckBridgeQueryRunner.create(
            DuckBridgeQueryRunner.freshDatabaseUrl(),
            mapOf("duckbridge.lance.enabled" to "true", "duckbridge.parity.enabled" to "false"),
        )
    }

    /** Write a small lance dataset via DuckDB directly. Returns false if lance can't be installed. */
    private fun writeLanceFixture(): Boolean =
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { s ->
                    s.execute("INSTALL lance")
                    s.execute("LOAD lance")
                    val ds = datasetPath.replace("'", "''")
                    // id, unicode text, 3-dim embedding. Each row's embedding points at one axis so
                    // vector search results are deterministic.
                    s.execute(
                        "COPY (SELECT * FROM (VALUES " +
                            "(1, 'apple pie recipe', [1.0,0.0,0.0]::FLOAT[3]), " +
                            "(2, 'banana bread löaf', [0.0,1.0,0.0]::FLOAT[3]), " +
                            "(3, 'cherry cake 日本', [0.0,0.0,1.0]::FLOAT[3])) " +
                            "AS t(id, txt, emb)) TO '$ds' (FORMAT lance)",
                    )
                }
            }
            true
        } catch (@Suppress("TooGenericExceptionCaught", "SwallowedException") e: Exception) {
            // Fixture-availability signal: lance not installable (offline / unsupported platform) →
            // the tests assumeTrue(lanceAvailable) and skip. The message is logged by the assume.
            false
        }

    @AfterAll
    fun cleanup() {
        if (::datasetDir.isInitialized) {
            datasetDir.toFile().deleteRecursively()
        }
    }

    private fun ptf(inner: String): String = "SELECT * FROM TABLE(${DuckBridgeQueryRunner.CATALOG}.system.$inner)"

    private fun requireLance() = assumeTrue(lanceAvailable, "DuckDB lance extension not installable — skipping")

    @Test
    fun lanceScanReturnsAllRows() {
        requireLance()
        val rows =
            computeActual("${ptf("lance_scan(path => '$datasetPath')")} ORDER BY id").materializedRows
        assertThat(rows.map { it.getField(0) as Int }).containsExactly(1, 2, 3)
        // Unicode content survives the scan.
        assertThat(rows.map { it.getField(1) as String }).containsExactly(
            "apple pie recipe",
            "banana bread löaf",
            "cherry cake 日本",
        )
    }

    @Test
    fun lanceScanProjection() {
        requireLance()
        val txt =
            computeActual("SELECT txt FROM TABLE(${DuckBridgeQueryRunner.CATALOG}.system.lance_scan(path => '$datasetPath')) ORDER BY id")
                .materializedRows
                .map { it.getField(0) as String }
        assertThat(txt).containsExactly("apple pie recipe", "banana bread löaf", "cherry cake 日本")
    }

    @Test
    fun lanceScanDomainFilterAppliedAbovePtf() {
        requireLance()
        // A WHERE over the PTF is applied by Trino above the function (P5 does not push domains into
        // the scan SQL — see AbstractDuckBridgeScanFunction doc). Result correctness is what matters.
        val ids =
            computeActual(
                "SELECT id FROM TABLE(${DuckBridgeQueryRunner.CATALOG}.system.lance_scan(path => '$datasetPath')) WHERE id >= 2 ORDER BY id",
            ).materializedRows.map { it.getField(0) as Int }
        assertThat(ids).containsExactly(2, 3)
    }

    @Test
    fun lanceVectorSearchRanksByDistance() {
        requireLance()
        // Query vector on the first axis → row 1 (distance 0) is nearest.
        val rows =
            computeActual(
                "SELECT id, _distance FROM TABLE(${DuckBridgeQueryRunner.CATALOG}.system.lance_vector_search(" +
                    "path => '$datasetPath', column => 'emb', query_vector => ARRAY[1.0, 0.0, 0.0], k => 2)) " +
                    "ORDER BY _distance",
            ).materializedRows
        assertThat(rows.first().getField(0) as Int).isEqualTo(1)
        // _distance is a REAL column; nearest is ~0.
        assertThat((rows.first().getField(1) as Float).toDouble()).isCloseTo(0.0, org.assertj.core.data.Offset.offset(1e-6))
    }

    @Test
    fun lanceFtsMatchesText() {
        requireLance()
        val rows =
            computeActual(
                "SELECT id, txt FROM TABLE(${DuckBridgeQueryRunner.CATALOG}.system.lance_fts(" +
                    "path => '$datasetPath', column => 'txt', query => 'banana', k => 3))",
            ).materializedRows
        // Only the banana row matches.
        assertThat(rows.map { it.getField(0) as Int }).containsExactly(2)
    }

    @Test
    fun lanceHybridSearchReturnsScoredRows() {
        requireLance()
        val rows =
            computeActual(
                "SELECT id, _hybrid_score FROM TABLE(${DuckBridgeQueryRunner.CATALOG}.system.lance_hybrid_search(" +
                    "path => '$datasetPath', vector_column => 'emb', query_vector => ARRAY[0.0, 1.0, 0.0], " +
                    "text_column => 'txt', query => 'banana', k => 3)) ORDER BY _hybrid_score DESC",
            ).materializedRows
        // The banana row (row 2) matches both the vector axis and the text — it must appear.
        assertThat(rows.map { it.getField(0) as Int }).contains(2)
    }

    @Test
    fun lanceScanRejectsWhenExtensionDisabled() {
        // A second catalog with lance disabled must fail loud (not silently return empty).
        DuckBridgeQueryRunner.create(
            DuckBridgeQueryRunner.freshDatabaseUrl(),
            mapOf("duckbridge.lance.enabled" to "false", "duckbridge.parity.enabled" to "false"),
        ).use { runner ->
            runner.installPlugin(io.trino.plugin.tpch.TpchPlugin())
            val ex =
                org.assertj.core.api.Assertions.catchThrowable {
                    runner.execute("SELECT * FROM TABLE(${DuckBridgeQueryRunner.CATALOG}.system.lance_scan(path => '$datasetPath'))")
                }
            assertThat(ex).isNotNull()
            assertThat(ex.message).contains("lance", "not enabled")
        }
    }
}
