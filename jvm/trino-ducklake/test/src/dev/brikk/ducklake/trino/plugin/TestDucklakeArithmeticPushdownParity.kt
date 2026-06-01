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
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_READ_MODE
import io.trino.Session
import io.trino.testing.MaterializedResult
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.util.stream.Collectors

/**
 * Trino ↔ DuckDB semantic-parity tests for `$divide` and `$modulo`.
 *
 *
 * `DuckDbExpressionTranslator.arithmeticOperator` maps these to bare
 * DuckDB `/` and `%` via `translateCall`. The SQL fragment is
 * AND-ed into `pushedExpressions` on the table handle — but the page source
 * only applies that list on the `data_file_format='duckdb'` (.db) path;
 * parquet splits ignore it and Trino's filter pipeline re-evaluates above the
 * scan, hiding the divergence.
 *
 *
 * On the .db path the SQL fragment IS applied. For DIVIDE-BY-ZERO, DuckDB
 * silently returns `NULL`/`Infinity` for the offending row; the
 * comparison evaluates to UNKNOWN/false and DuckDB strips the row from its
 * output. Trino's re-eval above the scan never sees the row, so Trino's
 * `DIVISION_BY_ZERO` exception is SUPPRESSED — the query silently
 * succeeds with the offending row missing instead of failing.
 *
 *
 * All tests use the session-property opt-in for `data_file_format=duckdb`,
 * mirroring the pattern in [TestDucklakeDuckDbReadMode] — that's the only
 * shape this connector actually honors for both the write and read paths.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeArithmeticPushdownParity
        : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "arithmetic-pushdown-parity"
    }

    private fun writeDuckDbSession(): Session {
        return Session.builder(session)
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build()
    }

    private fun readDuckDbSession(): Session {
        return Session.builder(session)
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .setCatalogSessionProperty("ducklake", DUCKDB_READ_MODE, READ_MODE_MATERIALIZE)
                .build()
    }

    @Test
    fun integerDivideOnDuckDbFormatReturnsTrinoTruncatedRowSet() {
        try {
            computeActual(writeDuckDbSession(),
                    "CREATE TABLE test_schema.div_trunc_db AS SELECT * FROM (VALUES (4), (5), (3), (6)) AS t(id)")

            val result = computeActual(readDuckDbSession(),
                    "SELECT id FROM test_schema.div_trunc_db WHERE id / 2 = 2 ORDER BY id")
            val ids = result.materializedRows.stream()
                    .map { row -> row.getField(0) as Int }
                    .collect(Collectors.toList())

            // Even though DuckDB pre-filters wrong (5/2=2.5 ≠ 2), Trino re-evaluates
            // above the scan and restores 5. This documents the safety net for
            // truncation divergence on .db reads.
            assertThat(ids)
                    .`as`("Trino post-scan re-evaluation must restore truncation semantics on .db reads")
                    .containsExactly(4, 5)
        }
        finally {
            tryDropTable("test_schema.div_trunc_db")
        }
    }

    @Test
    fun integerDivideByZeroOnDuckDbFormatMustThrowNotSilentlyFilter() {
        try {
            computeActual(writeDuckDbSession(),
                    "CREATE TABLE test_schema.div_zero_db AS SELECT * FROM (VALUES (10, 2), (20, 0), (30, 5)) AS t(id, divisor)")

            // Trino-native: evaluating `id / divisor` on row (20, 0) throws DIVISION_BY_ZERO.
            // Under the bug, DuckDB's pre-filter returns 20/0=Infinity, Infinity=5 is false,
            // the (20,0) row gets stripped from DuckDB's output, Trino never sees it, never throws.
            val thrown = catchThrowable {
                drain(computeActual(readDuckDbSession(),
                        "SELECT id FROM test_schema.div_zero_db WHERE id / divisor = 5"))
            }

            assertThat(thrown)
                    .`as`("Trino integer divide-by-zero must throw end-to-end on .db reads — "
                            + "the pushed predicate must not silently strip the offending row from DuckDB's output "
                            + "before Trino's above-scan re-evaluation can see it")
                    .isNotNull()
            assertThat(if (thrown.message == null) "" else thrown.message!!.lowercase())
                    .`as`("the thrown exception must actually be a divide-by-zero, not some unrelated failure")
                    .containsAnyOf("division by zero", "divide by zero", "/ by zero")
        }
        finally {
            tryDropTable("test_schema.div_zero_db")
        }
    }

    @Test
    fun integerModuloOnDuckDbFormatMatchesTrinoSemantics() {
        try {
            computeActual(writeDuckDbSession(),
                    "CREATE TABLE test_schema.mod_basic_db AS SELECT * FROM (VALUES (10), (11), (12), (13)) AS t(id)")

            val result = computeActual(readDuckDbSession(),
                    "SELECT id FROM test_schema.mod_basic_db WHERE id % 3 = 1 ORDER BY id")
            val ids = result.materializedRows.stream()
                    .map { row -> row.getField(0) as Int }
                    .collect(Collectors.toList())

            // Positive integer modulo aligns in both engines — pins the wire shape
            // on the .db path before exercising the modulo-by-zero divergence.
            assertThat(ids).containsExactly(10, 13)
        }
        finally {
            tryDropTable("test_schema.mod_basic_db")
        }
    }

    @Test
    fun integerModuloByZeroOnDuckDbFormatMustThrowNotSilentlyFilter() {
        try {
            computeActual(writeDuckDbSession(),
                    "CREATE TABLE test_schema.mod_zero_db AS SELECT * FROM (VALUES (10, 2), (20, 0), (30, 5)) AS t(id, divisor)")

            // Same divergence as divide-by-zero: DuckDB returns NULL for 20%0, the
            // comparison evaluates to UNKNOWN, the row is stripped before Trino's
            // re-evaluation can throw.
            val thrown = catchThrowable {
                drain(computeActual(readDuckDbSession(),
                        "SELECT id FROM test_schema.mod_zero_db WHERE id % divisor = 0"))
            }

            assertThat(thrown)
                    .`as`("Trino integer modulo-by-zero must throw end-to-end on .db reads")
                    .isNotNull()
            assertThat(if (thrown.message == null) "" else thrown.message!!.lowercase())
                    .`as`("the thrown exception must actually be a divide-by-zero, not some unrelated failure")
                    .containsAnyOf("division by zero", "divide by zero", "/ by zero")
        }
        finally {
            tryDropTable("test_schema.mod_zero_db")
        }
    }

    companion object {
        private const val FORMAT_DUCKDB = "duckdb"
        private const val READ_MODE_MATERIALIZE = "materialize"

        private fun drain(result: MaterializedResult) {
            val rows = result.materializedRows
            for (row in rows) {
                for (i in 0 until row.fieldCount) {
                    @Suppress("UNUSED_VARIABLE")
                    val unused = row.getField(i)
                }
            }
        }
    }
}
