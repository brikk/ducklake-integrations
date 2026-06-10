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
import io.trino.spi.type.ArrayType
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager
import java.util.Optional

/**
 * Exercises the Lance Route-A read path end-to-end through the real [InProcessDuckDbExecutor]:
 * write a `.lance` **dataset directory** with raw DuckDB, then read it back via a
 * [DuckDbAttachTarget.FileScan] target (`__lance_scan('<dir>')`) and assert the rows stream through.
 *
 * This is the sibling of [TestDucklakeVortexFileScanRead] for Lance, and the first automated
 * coverage of the Lance dataset-directory FileScan branch. The key difference from vortex is that
 * Lance produces a *directory*, not a single file — `__lance_scan` takes that directory path
 * (which is why [DucklakePageSourceProvider.resolveDuckDbReadTarget] bypasses the single-file
 * materialize cache for lance).
 *
 * **Platform-gated.** The `lance` DuckDB extension is published for
 * `osx_arm64` / `linux_amd64` / `linux_arm64` / `windows_amd64` only — it is 404 for `osx_amd64`,
 * so `INSTALL lance` fails on an Intel mac and the test skips (`assumeTrue`). It RUNS on an
 * Apple-Silicon (osx_arm64) box or the linux quack container. The shipped extension's scan table
 * function is `__lance_scan` (double-underscore), not `lance_scan`.
 * See dev-docs/HANDOFF-lance-route-a.md.
 */
class TestDucklakeLanceFileScanRead {
    @Test
    fun fileScanReadsLanceDatasetViaInProcessDuckDb() {
        val dir = Files.createTempDirectory("lance-filescan")
        // Lance COPY TO produces a *directory* dataset; __lance_scan reads that directory path.
        val dataset = dir.resolve("t.lance")
        val escaped = dataset.toString().replace("'", "''")

        // Write the fixture via raw DuckDB; skip the whole test if the lance extension can't be
        // installed/loaded (offline, or no published build for this platform — osx_amd64).
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL lance")
                    s.execute("LOAD lance")
                    s.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) v(id, name)")
                    s.execute("COPY t TO '$escaped' (FORMAT lance)")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "lance DuckDB extension unavailable (offline / unsupported platform — 404 on osx_amd64): ${e.message}")
            return
        }

        val projection = listOf(
                DucklakeColumnHandle(1L, "id", INTEGER, false),
                DucklakeColumnHandle(2L, "name", VARCHAR, false))
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan(dataset.toString(), "__lance_scan", "lance", Optional.empty()),
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
                .`as`("FileScan(__lance_scan) must stream all rows of the lance dataset")
                .isEqualTo(3L)
    }

    /**
     * Embedding columns are the one Lance type that transforms: a source `FLOAT[]` is stored and
     * returned by `__lance_scan` as Arrow `FixedSizeList<float>` (`FLOAT[N]`) and must map to Trino
     * `ARRAY(REAL)`. This drives the array branch of [DucklakeArrowToPageConverter] end-to-end —
     * read the dataset through the real executor, convert the Arrow batch to a Trino [Page], and
     * assert the array block carries the exact float vectors. Same platform gate as above.
     */
    @Test
    fun fileScanReadsLanceEmbeddingColumnAsArrayOfReal() {
        val dir = Files.createTempDirectory("lance-embedding")
        val dataset = dir.resolve("emb.lance")
        val escaped = dataset.toString().replace("'", "''")

        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL lance")
                    s.execute("LOAD lance")
                    // emb is FLOAT[3]; lance stores it as a fixed-size list (FixedSizeList<float>).
                    s.execute("CREATE TABLE t AS SELECT * FROM (VALUES " +
                            "(1, [1.0::FLOAT, 2.0::FLOAT, 3.0::FLOAT]), " +
                            "(2, [4.0::FLOAT, 5.0::FLOAT, 6.0::FLOAT])) v(id, emb)")
                    s.execute("COPY t TO '$escaped' (FORMAT lance)")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "lance DuckDB extension unavailable (offline / unsupported platform — 404 on osx_amd64): ${e.message}")
            return
        }

        val embType = ArrayType(REAL)
        val projection = listOf(
                DucklakeColumnHandle(1L, "id", INTEGER, false),
                DucklakeColumnHandle(2L, "emb", embType, false))
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan(dataset.toString(), "__lance_scan", "lance", Optional.empty()),
                projection,
                TupleDomain.all<DucklakeColumnHandle>())

        val columnTypes: List<Type> = listOf(INTEGER, embType)
        val converter = DucklakeArrowToPageConverter(columnTypes)
        val vectors = mutableListOf<List<Float>>()
        InProcessDuckDbExecutor().execute(request).use { ctx ->
            val reader = ctx.arrowReader()
            while (reader.loadNextBatch()) {
                val page = converter.convert(reader.vectorSchemaRoot)
                val arrayBlock = page.getBlock(1)
                for (row in 0 until page.positionCount) {
                    val elements = embType.getObject(arrayBlock, row)
                    vectors.add((0 until elements.positionCount).map { REAL.getFloat(elements, it) })
                }
            }
        }

        assertThat(vectors)
                .`as`("FixedSizeList<float> embedding column must convert to ARRAY(REAL) vectors")
                .containsExactlyInAnyOrder(
                        listOf(1.0f, 2.0f, 3.0f),
                        listOf(4.0f, 5.0f, 6.0f))
    }

    /**
     * Step 4 (Phase A2) — predicate pushdown. A `TupleDomain` on `id` must render into the
     * `WHERE` clause of the `__lance_scan(...)` query (via [DuckDbWhereClauseTranslator] inside
     * [DuckDbSelectSqlBuilder]) and actually reach DuckDB, so only the matching row streams back.
     * There is no ATTACH alias on the FileScan path — this proves the bare `WHERE` over the scan
     * source filters correctly. Same platform gate as above.
     */
    @Test
    fun fileScanPushesPredicateIntoLanceScan() {
        val dir = Files.createTempDirectory("lance-pushdown")
        val dataset = dir.resolve("p.lance")
        val escaped = dataset.toString().replace("'", "''")

        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL lance")
                    s.execute("LOAD lance")
                    s.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) v(id, name)")
                    s.execute("COPY t TO '$escaped' (FORMAT lance)")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "lance DuckDB extension unavailable (offline / unsupported platform — 404 on osx_amd64): ${e.message}")
            return
        }

        val idHandle = DucklakeColumnHandle(1L, "id", INTEGER, false)
        val nameHandle = DucklakeColumnHandle(2L, "name", VARCHAR, false)
        val predicate = TupleDomain.withColumnDomains(
                mapOf(idHandle to Domain.singleValue(INTEGER, 2L)))
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan(dataset.toString(), "__lance_scan", "lance", Optional.empty()),
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
                .`as`("pushed predicate id=2 must reach DuckDB so __lance_scan returns only that row")
                .containsExactly(2)
    }
}
