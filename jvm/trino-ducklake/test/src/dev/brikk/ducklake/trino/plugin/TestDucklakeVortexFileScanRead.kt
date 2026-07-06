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

import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager
import java.util.Optional

/**
 * Exercises the [DuckDbAttachTarget.FileScan] read path end-to-end through the real
 * [InProcessDuckDbExecutor]: write a `.vortex` file with raw DuckDB, then read it back via a
 * FileScan target (`read_vortex('path')`) and assert the rows stream through. This is the only
 * automated coverage of the FileScan branch added with vortex support (the rest of the suite
 * exercises only the `.db` ATTACH path).
 *
 * Network-gated: `INSTALL vortex` downloads the extension on first use; if that fails (offline,
 * or no published build for this platform — e.g. lance has no osx_amd64), the test SKIPS rather
 * than fails, so CI without network stays green.
 */
class TestDucklakeVortexFileScanRead {
    @Test
    fun fileScanReadsVortexFileViaInProcessDuckDb() {
        val dir = Files.createTempDirectory("vortex-filescan")
        val file = dir.resolve("t.vortex")
        val escaped = file.toString().replace("'", "''")

        // Write the fixture via raw DuckDB; skip the whole test if the vortex extension can't be
        // installed/loaded (offline or unsupported platform).
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL vortex")
                    s.execute("LOAD vortex")
                    s.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) v(id, name)")
                    s.execute("COPY t TO '$escaped' (FORMAT vortex)")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "vortex DuckDB extension unavailable (offline / unsupported platform): ${e.message}")
            return
        }

        val projection = listOf(
                DucklakeColumnHandle(1L, "id", INTEGER, false),
                DucklakeColumnHandle(2L, "name", VARCHAR, false))
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan(file.toString(), "read_vortex", "vortex", null),
                projection,
                TupleDomain.all<DucklakeColumnHandle>())

        var rowCount = 0L
        InProcessDuckDbExecutor().execute(request).use { ctx ->
            val reader = ctx.arrowReader()
            while (reader.loadNextBatch()) {
                rowCount += reader.vectorSchemaRoot.rowCount.toLong()
            }
        }

        assertThat(rowCount)
                .`as`("FileScan(read_vortex) must stream all rows of the vortex file")
                .isEqualTo(3L)
    }

    /**
     * V2 — predicate pushdown. A `TupleDomain` on `id` must render into the WHERE clause of the
     * `read_vortex(...)` query (via DuckDbWhereClauseTranslator inside DuckDbSelectSqlBuilder)
     * and reach DuckDB, so only the matching row streams back. Mirrors the lance A2 test
     * (`fileScanPushesPredicateIntoLanceScan`) — the rendering plumbing is shared and
     * format-blind; this pins it for the vortex branch.
     */
    @Test
    fun fileScanPushesPredicateIntoVortexScan() {
        val dir = Files.createTempDirectory("vortex-pushdown")
        val file = dir.resolve("p.vortex")
        val escaped = file.toString().replace("'", "''")

        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL vortex")
                    s.execute("LOAD vortex")
                    s.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) v(id, name)")
                    s.execute("COPY t TO '$escaped' (FORMAT vortex)")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "vortex DuckDB extension unavailable (offline / unsupported platform): ${e.message}")
            return
        }

        val idHandle = DucklakeColumnHandle(1L, "id", INTEGER, false)
        val nameHandle = DucklakeColumnHandle(2L, "name", VARCHAR, false)
        val predicate = TupleDomain.withColumnDomains(
                mapOf(idHandle to Domain.singleValue(INTEGER, 2L)))
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan(file.toString(), "read_vortex", "vortex", null),
                listOf(idHandle, nameHandle),
                predicate)

        val ids = mutableListOf<Int>()
        InProcessDuckDbExecutor().execute(request).use { ctx ->
            val reader = ctx.arrowReader()
            while (reader.loadNextBatch()) {
                val root = reader.vectorSchemaRoot
                val idVector = root.getVector(0)
                for (r in 0 until root.rowCount) {
                    ids.add((idVector.getObject(r) as Number).toInt())
                }
            }
        }

        assertThat(ids)
                .`as`("pushed predicate id=2 must reach DuckDB so read_vortex returns only that row")
                .containsExactly(2)
    }

    /**
     * V2 — exploitation probe: does the vortex extension BIND DuckDB's optimizer filter
     * pushdown (filter shown inside the READ_VORTEX scan operator), or does DuckDB post-filter
     * a full scan (separate FILTER operator above it)? Either way the results are correct —
     * this pins WHICH, so the tracker verdict is measured rather than assumed.
     */
    @Test
    fun vortexScanFilterPushdownProbe() {
        val dir = Files.createTempDirectory("vortex-explain")
        val file = dir.resolve("e.vortex")
        val escaped = file.toString().replace("'", "''")

        val planText =
                try {
                    explainVortexFilteredScan(escaped)
                }
                catch (e: Exception) {
                    assumeTrue(false, "vortex DuckDB extension unavailable (offline / unsupported platform): ${e.message}")
                    return
                }
        println("=== EXPLAIN read_vortex WHERE id=42 ===\n$planText")
        // Measured 2026-07-07 (vortex extension @ duckdb 1.5.4): the plan is a single
        // READ_VORTEX operator with `Filters: id=42` INSIDE it and NO separate FILTER
        // node — the extension binds DuckDB's optimizer filter pushdown, so pushed
        // predicates reach the vortex scan itself (pruned decoding), not a post-filter.
        // This canary trips if a vortex extension update loses that binding.
        assertThat(planText).containsIgnoringCase("READ_VORTEX")
        assertThat(planText)
                .`as`("filter must be bound INSIDE the vortex scan operator")
                .containsIgnoringCase("Filters: id=42")
        assertThat(planText.uppercase())
                .`as`("no separate FILTER operator above the scan (would mean post-filtering)")
                .doesNotContain("│           FILTER")
    }

    /** Writes a 10k-row fixture and returns the EXPLAIN plan of a filtered read_vortex scan. */
    private fun explainVortexFilteredScan(escapedPath: String): String {
        val plan = StringBuilder()
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("INSTALL vortex")
                s.execute("LOAD vortex")
                s.execute("CREATE TABLE t AS SELECT range::INTEGER AS id, 'v' || range AS name FROM range(10000)")
                s.execute("COPY t TO '$escapedPath' (FORMAT vortex)")
                val rs = s.executeQuery("EXPLAIN SELECT id, name FROM read_vortex('$escapedPath') WHERE id = 42")
                while (rs.next()) {
                    plan.append(rs.getString(2)).append('\n')
                }
            }
        }
        return plan.toString()
    }
}
