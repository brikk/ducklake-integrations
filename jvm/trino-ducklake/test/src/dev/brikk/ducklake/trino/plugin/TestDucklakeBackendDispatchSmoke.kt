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

import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.MaterializedResult.resultBuilder
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Smoke test that exercises the Trino connector end-to-end against whichever
 * catalog backend is selected by [DucklakeTestCatalogBackend.fromSystemProperty].
 * Runs under both POSTGRES (default) and DUCKDB_QUACK when the system property
 * is set, proving the backend selector actually swaps the catalog plumbing.
 *
 *
 * Stays deliberately minimal: CREATE SCHEMA / CREATE TABLE / INSERT / SELECT.
 * Wider Quack coverage is gated on the catalog-bootstrap parity work tracked in
 * `TODO-WRITE-MODE.md § Quack Catalog Backend`.
 */
internal class TestDucklakeBackendDispatchSmoke : AbstractTestQueryFramework() {
    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("backend-dispatch-smoke")
                .build()
    }

    @Test
    fun createInsertSelectRoundTrip() {
        assertUpdate("CREATE SCHEMA IF NOT EXISTS test_schema")
        assertUpdate("CREATE TABLE test_schema.dispatch_smoke (id INTEGER, name VARCHAR)")
        try {
            assertUpdate("INSERT INTO test_schema.dispatch_smoke VALUES (1, 'alpha'), (2, 'beta')", 2)

            val rows = computeActual(
                    "SELECT id, name FROM test_schema.dispatch_smoke ORDER BY id")
            val expected = resultBuilder(session, INTEGER, VARCHAR)
                    .row(1, "alpha")
                    .row(2, "beta")
                    .build()

            assertThat(rows.materializedRows)
                    .containsExactlyElementsOf(expected.materializedRows)
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_schema.dispatch_smoke")
        }
    }
}
