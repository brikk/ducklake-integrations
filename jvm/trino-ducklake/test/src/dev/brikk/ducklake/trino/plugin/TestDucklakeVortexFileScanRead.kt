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
                DuckDbAttachTarget.FileScan(file.toString(), "read_vortex", "vortex", Optional.empty()),
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
}
