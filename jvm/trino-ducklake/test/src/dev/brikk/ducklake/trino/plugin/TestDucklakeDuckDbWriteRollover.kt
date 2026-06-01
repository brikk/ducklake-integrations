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

import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_WRITER_MODE
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.WRITER_MODE_APPENDER
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.WRITER_MODE_ARROW_STREAM
import io.trino.Session
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Pins write-time rollover for the DuckDB-format writer: when accumulated input
 * bytes exceed `ducklake.duckdb.target-write-bytes`, the page sink rolls
 * to a new `.db` file. Catalog ends up with N `ducklake_data_file`
 * rows and read-back returns all rows.
 *
 *
 * Threshold is forced to 16 kB for the test — small enough that a 10k-row
 * CTAS rolls many times.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeDuckDbWriteRollover : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return CATALOG_NAME
    }

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog(CATALOG_NAME)
                .addConnectorProperty("ducklake.duckdb.target-write-bytes", "16kB")
                .build()
    }

    @Test
    fun testArrowStreamWriterRollsToMultipleFiles() {
        runRolloverScenario(WRITER_MODE_ARROW_STREAM, "duck_rollover_arrow")
    }

    @Test
    fun testAppenderWriterRollsToMultipleFiles() {
        runRolloverScenario(WRITER_MODE_APPENDER, "duck_rollover_appender")
    }

    private fun runRolloverScenario(writerMode: String, tableName: String) {
        // ~10k rows × (8 BIGINT + 32-char VARCHAR) = ~400 KB of logical input bytes.
        // Threshold 16 kB → many files. (repeat() in Trino returns an array; lpad
        // is the canonical way to get a fixed-width VARCHAR.)
        computeActual(duckDbSession(queryRunner, writerMode),
                "CREATE TABLE test_schema." + tableName + " AS " +
                        "SELECT id, lpad(cast(id AS varchar), 32, '0') AS payload " +
                        "FROM UNNEST(sequence(1, 10000)) AS t(id)")
        try {
            val files = computeActual(
                    "SELECT count(*) FROM \"" + tableName + "\$files\"")
            val fileCount = files.materializedRows.first().getField(0) as Long
            assertThat(fileCount)
                    .`as`("16 kB target on a ~440 KB write must roll to multiple files")
                    .isGreaterThan(1L)

            val count = computeActual(
                    "SELECT count(*) FROM test_schema." + tableName)
            assertThat(count.materializedRows.first().getField(0))
                    .`as`("all rows round-trip across the rolled files")
                    .isEqualTo(10000L)

            val agg = computeActual(
                    "SELECT min(id), max(id), sum(id) FROM test_schema." + tableName)
            val row = agg.materializedRows.first()
            assertThat(row.getField(0)).isEqualTo(1L)
            assertThat(row.getField(1)).isEqualTo(10000L)
            assertThat(row.getField(2)).isEqualTo(50005000L)
        }
        finally {
            tryDropTable("test_schema." + tableName)
        }
    }

    companion object {
        private const val CATALOG_NAME = "duckdb-write-rollover"

        private fun duckDbSession(runner: QueryRunner, writerMode: String): Session {
            return Session.builder(runner.defaultSession)
                    .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                    .setCatalogSessionProperty("ducklake", DUCKDB_WRITER_MODE, writerMode)
                    .build()
        }
    }
}
