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

import java.math.BigDecimal;
import java.time.LocalDate;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 Step 3 — DuckDB-format <i>reads</i>.
 *
 * <p>Validates round-trip through the connector: write a duckdb-format table with the
 * session property, then SELECT it back via Trino and confirm the rows match the
 * source data. Also exercises projection, Trino-side predicate filtering, and a
 * mixed-format table layout (parquet + duckdb tables in the same query).
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDuckDbFormatRead
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "duckdb-format-read";
    }

    private Session duckDbSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build();
    }

    @Test
    public void testScalarRoundTrip()
    {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.scalars AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, true, CAST(127 AS TINYINT), CAST(32000 AS SMALLINT), CAST(2147483600 AS INTEGER), 9223372036854775000, CAST(1.5 AS REAL), CAST(2.718281828 AS DOUBLE), CAST('hello' AS VARCHAR)), " +
                        "  (2, false, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), CAST(-2147483648 AS INTEGER), -9223372036854775000, CAST(-3.5 AS REAL), CAST(-1.0 AS DOUBLE), CAST('there' AS VARCHAR))" +
                        ") AS t(id, flag, ti, si, i, bi, r, d, s)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT id, flag, ti, si, i, bi, r, d, s FROM test_schema.scalars ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);

            MaterializedRow r1 = result.getMaterializedRows().get(0);
            assertThat(r1.getField(0)).isEqualTo(1);
            assertThat(r1.getField(1)).isEqualTo(true);
            assertThat(r1.getField(2)).isEqualTo((byte) 127);
            assertThat(r1.getField(3)).isEqualTo((short) 32000);
            assertThat(r1.getField(4)).isEqualTo(2147483600);
            assertThat(r1.getField(5)).isEqualTo(9223372036854775000L);
            assertThat(((Number) r1.getField(6)).floatValue()).isEqualTo(1.5f);
            assertThat(((Number) r1.getField(7)).doubleValue()).isEqualTo(2.718281828);
            assertThat(r1.getField(8)).isEqualTo("hello");

            MaterializedRow r2 = result.getMaterializedRows().get(1);
            assertThat(r2.getField(0)).isEqualTo(2);
            assertThat(r2.getField(1)).isEqualTo(false);
            assertThat(r2.getField(2)).isEqualTo((byte) -128);
            assertThat(r2.getField(8)).isEqualTo("there");
        }
        finally {
            tryDropTable("test_schema.scalars");
        }
    }

    @Test
    public void testNullsRoundTrip()
    {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.nulls AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST(NULL AS VARCHAR), CAST('a' AS VARCHAR)), " +
                        "  (2, CAST('b' AS VARCHAR), CAST(NULL AS VARCHAR))" +
                        ") AS t(id, opt1, opt2)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT id, opt1, opt2 FROM test_schema.nulls ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            MaterializedRow r1 = result.getMaterializedRows().get(0);
            assertThat(r1.getField(1)).isNull();
            assertThat(r1.getField(2)).isEqualTo("a");
            MaterializedRow r2 = result.getMaterializedRows().get(1);
            assertThat(r2.getField(1)).isEqualTo("b");
            assertThat(r2.getField(2)).isNull();
        }
        finally {
            tryDropTable("test_schema.nulls");
        }
    }

    @Test
    public void testTemporalAndDecimalRoundTrip()
    {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.temporal_decimal AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, DATE '2026-05-04', TIMESTAMP '2026-05-04 12:34:56.123456', CAST('123.45' AS DECIMAL(10,2))), " +
                        "  (2, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00.000000', CAST('-9999.99' AS DECIMAL(10,2)))" +
                        ") AS t(id, d, ts, dec)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT id, d, ts, dec FROM test_schema.temporal_decimal ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);

            MaterializedRow r1 = result.getMaterializedRows().get(0);
            assertThat(r1.getField(1)).isEqualTo(LocalDate.of(2026, 5, 4));
            assertThat(r1.getField(3)).isEqualTo(new BigDecimal("123.45"));

            MaterializedRow r2 = result.getMaterializedRows().get(1);
            assertThat(r2.getField(1)).isEqualTo(LocalDate.of(2024, 1, 1));
            assertThat(r2.getField(3)).isEqualTo(new BigDecimal("-9999.99"));
        }
        finally {
            tryDropTable("test_schema.temporal_decimal");
        }
    }

    @Test
    public void testProjection()
    {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.proj AS " +
                        "SELECT * FROM (VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)) AS t(id, name, amount)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT name FROM test_schema.proj ORDER BY name");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("a");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("b");
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("c");
        }
        finally {
            tryDropTable("test_schema.proj");
        }
    }

    @Test
    public void testTrinoSidePredicateFilter()
    {
        // Predicate pushdown into DuckDB SQL is Step 4. For now Trino's filter pipeline
        // applies the WHERE after rows leave the page source. Either way the result
        // should be correct.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.pred AS " +
                        "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300), (4, 400)) AS t(id, amount)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT id FROM test_schema.pred WHERE amount > 150 ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(4);
        }
        finally {
            tryDropTable("test_schema.pred");
        }
    }

    @Test
    public void testAggregation()
    {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.agg AS " +
                        "SELECT * FROM (VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)) AS t(id, grp, amt)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT grp, sum(amt) FROM test_schema.agg GROUP BY grp ORDER BY grp");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("A");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(30L);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("B");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(70L);
        }
        finally {
            tryDropTable("test_schema.agg");
        }
    }

    @Test
    public void testMixedFormatJoin()
    {
        // Two tables, one parquet (default) and one duckdb. Joining them exercises
        // both readers in a single query.
        computeActual("CREATE TABLE test_schema.mix_parquet AS " +
                "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(id, label)");
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.mix_duckdb AS " +
                        "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300)) AS t(id, amount)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT p.label, d.amount " +
                            "FROM test_schema.mix_parquet p JOIN test_schema.mix_duckdb d ON p.id = d.id " +
                            "ORDER BY p.id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("one");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(100);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("three");
            assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo(300);

            // Sanity check that the formats really differ in storage
            MaterializedResult parquetFiles = computeActual(
                    "SELECT file_format FROM \"mix_parquet$files\"");
            assertThat(parquetFiles.getMaterializedRows().get(0).getField(0)).isEqualTo("parquet");
            MaterializedResult duckdbFiles = computeActual(
                    "SELECT file_format FROM \"mix_duckdb$files\"");
            assertThat(duckdbFiles.getMaterializedRows().get(0).getField(0)).isEqualTo("duckdb");
        }
        finally {
            tryDropTable("test_schema.mix_parquet");
            tryDropTable("test_schema.mix_duckdb");
        }
    }
}
