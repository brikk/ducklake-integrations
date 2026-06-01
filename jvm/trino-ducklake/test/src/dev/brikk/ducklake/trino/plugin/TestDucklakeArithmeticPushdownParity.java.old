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
package dev.brikk.ducklake.trino.plugin;

import io.trino.Session;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;
import java.util.stream.Collectors;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_READ_MODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Trino ↔ DuckDB semantic-parity tests for {@code $divide} and {@code $modulo}.
 *
 * <p>{@code DuckDbExpressionTranslator.arithmeticOperator} maps these to bare
 * DuckDB {@code /} and {@code %} via {@code translateCall}. The SQL fragment is
 * AND-ed into {@code pushedExpressions} on the table handle — but the page source
 * only applies that list on the {@code data_file_format='duckdb'} (.db) path;
 * parquet splits ignore it and Trino's filter pipeline re-evaluates above the
 * scan, hiding the divergence.
 *
 * <p>On the .db path the SQL fragment IS applied. For DIVIDE-BY-ZERO, DuckDB
 * silently returns {@code NULL}/{@code Infinity} for the offending row; the
 * comparison evaluates to UNKNOWN/false and DuckDB strips the row from its
 * output. Trino's re-eval above the scan never sees the row, so Trino's
 * {@code DIVISION_BY_ZERO} exception is SUPPRESSED — the query silently
 * succeeds with the offending row missing instead of failing.
 *
 * <p>All tests use the session-property opt-in for {@code data_file_format=duckdb},
 * mirroring the pattern in {@link TestDucklakeDuckDbReadMode} — that's the only
 * shape this connector actually honors for both the write and read paths.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeArithmeticPushdownParity
        extends AbstractDucklakeIntegrationTest
{
    private static final String FORMAT_DUCKDB = "duckdb";
    private static final String READ_MODE_MATERIALIZE = "materialize";

    @Override
    protected String isolatedCatalogName()
    {
        return "arithmetic-pushdown-parity";
    }

    private Session writeDuckDbSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build();
    }

    private Session readDuckDbSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .setCatalogSessionProperty("ducklake", DUCKDB_READ_MODE, READ_MODE_MATERIALIZE)
                .build();
    }

    @Test
    public void integerDivideOnDuckDbFormatReturnsTrinoTruncatedRowSet()
    {
        try {
            computeActual(writeDuckDbSession(),
                    "CREATE TABLE test_schema.div_trunc_db AS SELECT * FROM (VALUES (4), (5), (3), (6)) AS t(id)");

            MaterializedResult result = computeActual(readDuckDbSession(),
                    "SELECT id FROM test_schema.div_trunc_db WHERE id / 2 = 2 ORDER BY id");
            List<Integer> ids = result.getMaterializedRows().stream()
                    .map(row -> (Integer) row.getField(0))
                    .collect(Collectors.toList());

            // Even though DuckDB pre-filters wrong (5/2=2.5 ≠ 2), Trino re-evaluates
            // above the scan and restores 5. This documents the safety net for
            // truncation divergence on .db reads.
            assertThat(ids)
                    .as("Trino post-scan re-evaluation must restore truncation semantics on .db reads")
                    .containsExactly(4, 5);
        }
        finally {
            tryDropTable("test_schema.div_trunc_db");
        }
    }

    @Test
    public void integerDivideByZeroOnDuckDbFormatMustThrowNotSilentlyFilter()
    {
        try {
            computeActual(writeDuckDbSession(),
                    "CREATE TABLE test_schema.div_zero_db AS SELECT * FROM (VALUES (10, 2), (20, 0), (30, 5)) AS t(id, divisor)");

            // Trino-native: evaluating `id / divisor` on row (20, 0) throws DIVISION_BY_ZERO.
            // Under the bug, DuckDB's pre-filter returns 20/0=Infinity, Infinity=5 is false,
            // the (20,0) row gets stripped from DuckDB's output, Trino never sees it, never throws.
            Throwable thrown = catchThrowable(() ->
                    drain(computeActual(readDuckDbSession(),
                            "SELECT id FROM test_schema.div_zero_db WHERE id / divisor = 5")));

            assertThat(thrown)
                    .as("Trino integer divide-by-zero must throw end-to-end on .db reads — "
                            + "the pushed predicate must not silently strip the offending row from DuckDB's output "
                            + "before Trino's above-scan re-evaluation can see it")
                    .isNotNull();
            assertThat(thrown.getMessage() == null ? "" : thrown.getMessage().toLowerCase())
                    .as("the thrown exception must actually be a divide-by-zero, not some unrelated failure")
                    .containsAnyOf("division by zero", "divide by zero", "/ by zero");
        }
        finally {
            tryDropTable("test_schema.div_zero_db");
        }
    }

    @Test
    public void integerModuloOnDuckDbFormatMatchesTrinoSemantics()
    {
        try {
            computeActual(writeDuckDbSession(),
                    "CREATE TABLE test_schema.mod_basic_db AS SELECT * FROM (VALUES (10), (11), (12), (13)) AS t(id)");

            MaterializedResult result = computeActual(readDuckDbSession(),
                    "SELECT id FROM test_schema.mod_basic_db WHERE id % 3 = 1 ORDER BY id");
            List<Integer> ids = result.getMaterializedRows().stream()
                    .map(row -> (Integer) row.getField(0))
                    .collect(Collectors.toList());

            // Positive integer modulo aligns in both engines — pins the wire shape
            // on the .db path before exercising the modulo-by-zero divergence.
            assertThat(ids).containsExactly(10, 13);
        }
        finally {
            tryDropTable("test_schema.mod_basic_db");
        }
    }

    @Test
    public void integerModuloByZeroOnDuckDbFormatMustThrowNotSilentlyFilter()
    {
        try {
            computeActual(writeDuckDbSession(),
                    "CREATE TABLE test_schema.mod_zero_db AS SELECT * FROM (VALUES (10, 2), (20, 0), (30, 5)) AS t(id, divisor)");

            // Same divergence as divide-by-zero: DuckDB returns NULL for 20%0, the
            // comparison evaluates to UNKNOWN, the row is stripped before Trino's
            // re-evaluation can throw.
            Throwable thrown = catchThrowable(() ->
                    drain(computeActual(readDuckDbSession(),
                            "SELECT id FROM test_schema.mod_zero_db WHERE id % divisor = 0")));

            assertThat(thrown)
                    .as("Trino integer modulo-by-zero must throw end-to-end on .db reads")
                    .isNotNull();
            assertThat(thrown.getMessage() == null ? "" : thrown.getMessage().toLowerCase())
                    .as("the thrown exception must actually be a divide-by-zero, not some unrelated failure")
                    .containsAnyOf("division by zero", "divide by zero", "/ by zero");
        }
        finally {
            tryDropTable("test_schema.mod_zero_db");
        }
    }

    private static void drain(MaterializedResult result)
    {
        List<MaterializedRow> rows = result.getMaterializedRows();
        for (MaterializedRow row : rows) {
            for (int i = 0; i < row.getFieldCount(); i++) {
                Object unused = row.getField(i);
            }
        }
    }
}
