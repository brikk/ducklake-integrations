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

import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager

/**
 * Server-free integration test of the T2 in-process executor + Arrow→Page converter: writes a temp
 * `.db`, runs a SELECT through [InProcessDuckBridgeExecutor], and decodes the Arrow batches to Trino
 * Pages via [DuckBridgeArrowToPageConverter]. Exercises the full DUCKDB_LOCAL data plane without any
 * Quack server. Parity LOAD is included so the `arrowExportStream` + tuning + parity path is real.
 */
class TestDuckBridgeInProcessExecutor {
    @Test
    fun executeAndDecodeArrowBatches() {
        val dbFile = Files.createTempFile("duckbridge-t2-", ".db")
        Files.delete(dbFile)
        try {
            DriverManager.getConnection("jdbc:duckdb:$dbFile").use { conn ->
                conn.createStatement().use { s ->
                    s.execute("CREATE TABLE t (id bigint, name varchar)")
                    s.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'béta'), (3, 'γάμμα')")
                }
            }

            val parityPath = TrinoParityExtensionResolver.resolveBundledExtensionPath()
            val executor = InProcessDuckBridgeExecutor("jdbc:duckdb:$dbFile", DuckDbTuning.defaults(), parityPath)
            val request = DuckBridgeExecutor.ExecutionRequest("SELECT id, name FROM t ORDER BY id", "UTC")

            val converter = DuckBridgeArrowToPageConverter(listOf(BIGINT, VARCHAR))
            val ids = mutableListOf<Long>()
            val names = mutableListOf<String>()
            executor.execute(request).use { ctx ->
                val reader = ctx.arrowReader()
                while (reader.loadNextBatch()) {
                    val page = converter.convert(reader.vectorSchemaRoot)
                    val idBlock = page.getBlock(0)
                    val nameBlock = page.getBlock(1)
                    for (i in 0 until page.positionCount) {
                        ids.add(BIGINT.getLong(idBlock, i))
                        names.add(VARCHAR.getSlice(nameBlock, i).toStringUtf8())
                    }
                }
            }
            assertThat(ids).containsExactly(1L, 2L, 3L)
            assertThat(names).containsExactly("alpha", "béta", "γάμμα")
        } finally {
            Files.deleteIfExists(dbFile)
        }
    }

    @Test
    fun executeWithParityFunctionInSql() {
        val dbFile = Files.createTempFile("duckbridge-t2p-", ".db")
        Files.delete(dbFile)
        try {
            DriverManager.getConnection("jdbc:duckdb:$dbFile").use { conn ->
                conn.createStatement().use { s ->
                    s.execute("CREATE TABLE t (name varchar)")
                    s.execute("INSERT INTO t VALUES ('straße'), ('abc')")
                }
            }
            val parityPath =
                TrinoParityExtensionResolver.resolveBundledExtensionPath()
                    ?: return // no bundled binary for this platform — skip (parity SQL can't resolve)
            val executor = InProcessDuckBridgeExecutor("jdbc:duckdb:$dbFile", DuckDbTuning.defaults(), parityPath)
            // The T2 SQL carries the parity function; the extension LOADed by the executor resolves it.
            val request = DuckBridgeExecutor.ExecutionRequest("SELECT trino_upper(name) AS u FROM t ORDER BY name", null)
            val converter = DuckBridgeArrowToPageConverter(listOf(VARCHAR))
            val out = mutableListOf<String>()
            executor.execute(request).use { ctx ->
                val reader = ctx.arrowReader()
                while (reader.loadNextBatch()) {
                    val page = converter.convert(reader.vectorSchemaRoot)
                    val block = page.getBlock(0)
                    for (i in 0 until page.positionCount) {
                        out.add(VARCHAR.getSlice(block, i).toStringUtf8())
                    }
                }
            }
            // 'abc' → 'ABC', 'straße' → 'STRASSE' (full case fold via the extension).
            assertThat(out).containsExactly("ABC", "STRASSE")
        } finally {
            Files.deleteIfExists(dbFile)
        }
    }
}
