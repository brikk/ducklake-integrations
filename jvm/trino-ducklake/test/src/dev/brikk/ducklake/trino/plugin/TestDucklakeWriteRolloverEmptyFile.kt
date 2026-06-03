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

import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.DATA_FILE_FORMAT
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_DUCKDB
import io.trino.Session
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Regression for the page-sink rollover that used to eagerly open a successor writer the
 * instant a writer crossed `target-write-bytes`. When the rollover landed on the final page
 * (or the final page for a partition) the successor never received another write, yet
 * `finish()` still closed it — emitting a real header+footer data object plus a
 * `record_count = 0` catalog row.
 *
 *
 * Threshold is pinned to its 1 kB minimum so a single page of a few hundred rows comfortably
 * crosses it: the write triggers exactly one rollover whose successor stays empty under the
 * old eager-open behaviour. With the lazy-open fix the slot is left null and no zero-row file
 * is registered. The suite asserts the `$files` metadata table contains no `record_count = 0`
 * rows while the data still round-trips in full.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeWriteRolloverEmptyFile : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return CATALOG_NAME
    }

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog(CATALOG_NAME)
                .addConnectorProperty("ducklake.duckdb.target-write-bytes", "1kB")
                .build()
    }

    @Test
    fun testRolloverOnFinalPageLeavesNoEmptyFile() {
        val tableName = "rollover_empty_unpartitioned"
        computeActual(duckDbSession(queryRunner),
                "CREATE TABLE test_schema.$tableName AS " +
                        "SELECT id, lpad(cast(id AS varchar), 64, '0') AS payload " +
                        "FROM UNNEST(sequence(1, 500)) AS t(id)")
        try {
            assertNoEmptyDataFiles(tableName)

            assertThat(computeScalar("SELECT count(*) FROM test_schema.$tableName"))
                    .`as`("all rows round-trip after rollover")
                    .isEqualTo(500L)
        }
        finally {
            tryDropTable("test_schema.$tableName")
        }
    }

    @Test
    fun testPartitionedRolloverLeavesNoEmptyFile() {
        val tableName = "rollover_empty_partitioned"
        computeActual("CREATE TABLE test_schema.$tableName (id INTEGER, region VARCHAR, payload VARCHAR) " +
                "WITH (partitioned_by = ARRAY['region'])")
        try {
            // Each region's sub-page crosses the 1 kB threshold on its own, so the partitioned
            // append path rolls every partition's writer and (pre-fix) left an empty successor
            // per partition.
            computeActual(duckDbSession(queryRunner),
                    "INSERT INTO test_schema.$tableName " +
                            "SELECT id, IF(id % 2 = 0, 'EU', 'US'), lpad(cast(id AS varchar), 64, '0') " +
                            "FROM UNNEST(sequence(1, 500)) AS t(id)")

            assertNoEmptyDataFiles(tableName)

            assertThat(computeScalar("SELECT count(*) FROM test_schema.$tableName"))
                    .`as`("all rows round-trip after partitioned rollover")
                    .isEqualTo(500L)
            assertThat(computeScalar("SELECT count(*) FROM test_schema.$tableName WHERE region = 'US'"))
                    .isEqualTo(250L)
        }
        finally {
            tryDropTable("test_schema.$tableName")
        }
    }

    private fun assertNoEmptyDataFiles(tableName: String) {
        assertThat(computeScalar("SELECT count(*) FROM \"$tableName\$files\" WHERE record_count = 0"))
                .`as`("rollover must not register any zero-row data file")
                .isEqualTo(0L)
        assertThat(computeScalar("SELECT sum(record_count) FROM \"$tableName\$files\""))
                .`as`("registered record counts must sum to the live row count")
                .isEqualTo(500L)
    }

    companion object {
        private const val CATALOG_NAME = "duckdb-rollover-empty"

        private fun duckDbSession(runner: QueryRunner): Session {
            return Session.builder(runner.defaultSession)
                    .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                    .build()
        }
    }
}
