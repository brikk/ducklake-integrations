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
 * Exercises the Lance Route-A read path end-to-end through the real [InProcessDuckDbExecutor]:
 * write a `.lance` **dataset directory** with raw DuckDB, then read it back via a
 * [DuckDbAttachTarget.FileScan] target (`lance_scan('<dir>')`) and assert the rows stream through.
 *
 * This is the sibling of [TestDucklakeVortexFileScanRead] for Lance, and the first automated
 * coverage of the Lance dataset-directory FileScan branch. The key difference from vortex is that
 * Lance produces a *directory*, not a single file — `lance_scan` takes that directory path
 * (which is why [DucklakePageSourceProvider.resolveDuckDbReadTarget] bypasses the single-file
 * materialize cache for lance).
 *
 * **Platform-gated: SKIPS on this Intel dev box.** The `lance` DuckDB extension is published for
 * `osx_arm64` / `linux_amd64` / `linux_arm64` / `windows_amd64` only — it is 404 for `osx_amd64`,
 * so `INSTALL lance` fails here and the test skips (`assumeTrue`). It RUNS on an Apple-Silicon
 * (osx_arm64) box or the linux quack container. See dev-docs/HANDOFF-lance-route-a.md.
 */
class TestDucklakeLanceFileScanRead {
    @Test
    fun fileScanReadsLanceDatasetViaInProcessDuckDb() {
        val dir = Files.createTempDirectory("lance-filescan")
        // Lance COPY TO produces a *directory* dataset; lance_scan reads that directory path.
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
                DuckDbAttachTarget.FileScan(dataset.toString(), "lance_scan", "lance", Optional.empty()),
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
                .`as`("FileScan(lance_scan) must stream all rows of the lance dataset")
                .isEqualTo(3L)
    }
}
