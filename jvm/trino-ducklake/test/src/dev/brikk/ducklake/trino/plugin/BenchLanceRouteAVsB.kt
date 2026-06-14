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

import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.lance.Dataset
import org.lance.ipc.Query
import org.lance.ipc.ScanOptions
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.util.Locale
import kotlin.random.Random

/**
 * The Route A-vs-B decision benchmark (TODO-lance §"Route A vs B decision",
 * HANDOFF-lance-route-a NEXT §1): the SAME lance dataset, the SAME query vectors, the SAME `k` —
 * vector search through
 *
 *  - **Route A**: the DuckDB `lance` extension (`lance_vector_search(...)` over in-process
 *    duckdb JDBC — the engine our connector ships, including the SQL-literal vector rendering
 *    the connector really performs), vs
 *  - **Route B**: `org.lance:lance-core` JNI directly (`Dataset.newScan(nearest)` →
 *    `scanBatches()` — the engine the vendored lance-trino plugin wraps).
 *
 * Measures COLD (fresh engine, first query — where the extension indirection is likeliest to
 * lose) and WARM (per-query latency after warmup) over a brute-force (unindexed) search, which
 * both engines execute as the same flat scan + top-k. Results print to stdout; they are recorded
 * in dev-docs/archive/REPORT-lance-route-a-vs-b.md with the decision.
 *
 * NOT part of the suite: gated on `-Dducklake.bench`, run manually:
 * ```
 * ./gradlew :trino-ducklake:test --tests "*.BenchLanceRouteAVsB" -Dducklake.bench=true
 * ```
 * Tunables (system properties): `ducklake.bench.rows` (default 200000), `.dim` (384),
 * `.k` (10), `.warmup` (5), `.iters` (30).
 */
class BenchLanceRouteAVsB {

    @Test
    fun vectorSearchRouteAVsRouteB() {
        assumeTrue(System.getProperty("ducklake.bench") != null,
                "benchmark is manual-only: pass -Dducklake.bench=true")

        val rows = intProp("ducklake.bench.rows", 200_000)
        val dim = intProp("ducklake.bench.dim", 384)
        val k = intProp("ducklake.bench.k", 10)
        val warmup = intProp("ducklake.bench.warmup", 5)
        val iters = intProp("ducklake.bench.iters", 30)

        val dataset = generateDataset(rows, dim)
        val queries = List(QUERY_VECTORS) { q -> FloatArray(dim) { Random(SEED + q * 1000 + it).nextFloat() } }

        println("=== lance Route A vs B: rows=$rows dim=$dim k=$k warmup=$warmup iters=$iters ===")
        val a = benchRouteA(dataset, queries, k, warmup, iters)
        val b = benchRouteB(dataset, queries, k, warmup, iters)
        println(format("Route A (duckdb lance ext)", a))
        println(format("Route B (lance-core JNI)  ", b))
    }

    private data class BenchResult(val coldMs: Double, val warmMs: List<Double>)

    /** Writes the benchmark dataset via the Route-A-native tooling (deterministic via setseed). */
    private fun generateDataset(rows: Int, dim: Int): Path {
        val dir = Files.createTempDirectory("lance-bench")
        val target = dir.resolve("bench.lance")
        val escaped = target.toString().replace("'", "''")
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                try {
                    s.execute("INSTALL lance")
                    s.execute("LOAD lance")
                }
                catch (e: Exception) {
                    assumeTrue(false, "lance DuckDB extension unavailable: ${e.message}")
                }
                s.execute("SELECT setseed(0.42)")
                s.execute("COPY (SELECT i::INTEGER AS id, "
                        + "list_transform(range($dim), x -> random()::FLOAT)::FLOAT[$dim] AS emb "
                        + "FROM range($rows) t(i)) TO '$escaped' (FORMAT lance)")
            }
        }
        return target
    }

    private fun benchRouteA(dataset: Path, queries: List<FloatArray>, k: Int, warmup: Int, iters: Int): BenchResult {
        val path = dataset.toString().replace("'", "''")

        fun runQuery(connection: Connection, vector: FloatArray): Int {
            val literal = vector.joinToString(", ", "[", "]::DOUBLE[]")
            connection.createStatement().use { s ->
                s.executeQuery("SELECT id, _distance FROM lance_vector_search("
                        + "'$path', 'emb', $literal, k := $k, prefilter := false)").use { rs ->
                    var n = 0
                    while (rs.next()) {
                        rs.getInt(1)
                        rs.getFloat(2)
                        n++
                    }
                    return n
                }
            }
        }

        // COLD: fresh duckdb instance; first query pays extension LOAD + dataset open + scan.
        val coldMs: Double
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s -> s.execute("INSTALL lance"); s.execute("LOAD lance") }
            val t0 = System.nanoTime()
            check(runQuery(c, queries[0]) == k)
            coldMs = (System.nanoTime() - t0) / 1e6
        }

        // WARM: one long-lived instance, like the connector's per-split engines amortized.
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s -> s.execute("INSTALL lance"); s.execute("LOAD lance") }
            repeat(warmup) { check(runQuery(c, queries[it % queries.size]) == k) }
            val warm = (0 until iters).map {
                val v = queries[it % queries.size]
                val t0 = System.nanoTime()
                check(runQuery(c, v) == k)
                (System.nanoTime() - t0) / 1e6
            }
            return BenchResult(coldMs, warm)
        }
    }

    private fun benchRouteB(dataset: Path, queries: List<FloatArray>, k: Int, warmup: Int, iters: Int): BenchResult {
        fun runQuery(ds: Dataset, vector: FloatArray): Int {
            val options = ScanOptions.Builder()
                    .columns(listOf("id"))
                    .nearest(Query.Builder()
                            .setColumn("emb")
                            .setKey(vector)
                            .setK(k)
                            .setUseIndex(false)
                            .build())
                    .build()
            ds.newScan(options).use { scanner ->
                scanner.scanBatches().use { reader ->
                    var n = 0
                    while (reader.loadNextBatch()) {
                        n += reader.vectorSchemaRoot.rowCount
                    }
                    return n
                }
            }
        }

        RootAllocator().use { allocator ->
            // COLD: dataset open + first query in a fresh JNI session.
            val coldMs: Double
            Dataset.open(dataset.toString(), allocator).use { ds ->
                val t0 = System.nanoTime()
                check(runQuery(ds, queries[0]) == k)
                coldMs = (System.nanoTime() - t0) / 1e6
            }

            Dataset.open(dataset.toString(), allocator).use { ds ->
                repeat(warmup) { check(runQuery(ds, queries[it % queries.size]) == k) }
                val warm = (0 until iters).map {
                    val v = queries[it % queries.size]
                    val t0 = System.nanoTime()
                    check(runQuery(ds, v) == k)
                    (System.nanoTime() - t0) / 1e6
                }
                return BenchResult(coldMs, warm)
            }
        }
    }

    private fun format(label: String, r: BenchResult): String {
        val sorted = r.warmMs.sorted()
        fun pct(p: Double) = sorted[((sorted.size - 1) * p).toInt()]
        return String.format(Locale.ROOT,
                "%s cold=%8.1fms | warm min=%7.1f p50=%7.1f p90=%7.1f max=%7.1f (ms over %d iters)",
                label, r.coldMs, sorted.first(), pct(0.50), pct(0.90), sorted.last(), sorted.size)
    }

    private fun intProp(name: String, default: Int): Int =
        System.getProperty(name)?.toIntOrNull() ?: default

    companion object {
        private const val SEED = 42
        private const val QUERY_VECTORS = 10
    }
}
