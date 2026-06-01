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
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB
import io.trino.Session
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.math.BigDecimal
import java.time.LocalDate

/**
 * Phase 1 Step 3 — DuckDB-format *reads*.
 *
 * Validates round-trip through the connector: write a duckdb-format table with the
 * session property, then SELECT it back via Trino and confirm the rows match the
 * source data. Also exercises projection, Trino-side predicate filtering, and a
 * mixed-format table layout (parquet + duckdb tables in the same query).
 */
@Execution(ExecutionMode.SAME_THREAD)
open class TestDucklakeDuckDbFormatRead : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "duckdb-format-read"
    }

    private fun duckDbSession(): Session {
        return Session.builder(session)
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build()
    }

    @Test
    fun testScalarRoundTrip() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.scalars AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, true, CAST(127 AS TINYINT), CAST(32000 AS SMALLINT), CAST(2147483600 AS INTEGER), 9223372036854775000, CAST(1.5 AS REAL), CAST(2.718281828 AS DOUBLE), CAST('hello' AS VARCHAR)), " +
                        "  (2, false, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), CAST(-2147483648 AS INTEGER), -9223372036854775000, CAST(-3.5 AS REAL), CAST(-1.0 AS DOUBLE), CAST('there' AS VARCHAR))" +
                        ") AS t(id, flag, ti, si, i, bi, r, d, s)")
        try {
            val result = computeActual(
                    "SELECT id, flag, ti, si, i, bi, r, d, s FROM test_schema.scalars ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)

            val r1 = result.materializedRows[0]
            assertThat(r1.getField(0)).isEqualTo(1)
            assertThat(r1.getField(1)).isEqualTo(true)
            assertThat(r1.getField(2)).isEqualTo(127.toByte())
            assertThat(r1.getField(3)).isEqualTo(32000.toShort())
            assertThat(r1.getField(4)).isEqualTo(2147483600)
            assertThat(r1.getField(5)).isEqualTo(9223372036854775000L)
            assertThat((r1.getField(6) as Number).toFloat()).isEqualTo(1.5f)
            assertThat((r1.getField(7) as Number).toDouble()).isEqualTo(2.718281828)
            assertThat(r1.getField(8)).isEqualTo("hello")

            val r2 = result.materializedRows[1]
            assertThat(r2.getField(0)).isEqualTo(2)
            assertThat(r2.getField(1)).isEqualTo(false)
            assertThat(r2.getField(2)).isEqualTo((-128).toByte())
            assertThat(r2.getField(8)).isEqualTo("there")
        }
        finally {
            tryDropTable("test_schema.scalars")
        }
    }

    @Test
    fun testNullsRoundTrip() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.nulls AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST(NULL AS VARCHAR), CAST('a' AS VARCHAR)), " +
                        "  (2, CAST('b' AS VARCHAR), CAST(NULL AS VARCHAR))" +
                        ") AS t(id, opt1, opt2)")
        try {
            val result = computeActual(
                    "SELECT id, opt1, opt2 FROM test_schema.nulls ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            val r1 = result.materializedRows[0]
            assertThat(r1.getField(1)).isNull()
            assertThat(r1.getField(2)).isEqualTo("a")
            val r2 = result.materializedRows[1]
            assertThat(r2.getField(1)).isEqualTo("b")
            assertThat(r2.getField(2)).isNull()
        }
        finally {
            tryDropTable("test_schema.nulls")
        }
    }

    @Test
    fun testTemporalAndDecimalRoundTrip() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.temporal_decimal AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, DATE '2026-05-04', TIMESTAMP '2026-05-04 12:34:56.123456', CAST('123.45' AS DECIMAL(10,2))), " +
                        "  (2, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00.000000', CAST('-9999.99' AS DECIMAL(10,2)))" +
                        ") AS t(id, d, ts, dec)")
        try {
            val result = computeActual(
                    "SELECT id, d, ts, dec FROM test_schema.temporal_decimal ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)

            val r1 = result.materializedRows[0]
            assertThat(r1.getField(1)).isEqualTo(LocalDate.of(2026, 5, 4))
            assertThat(r1.getField(3)).isEqualTo(BigDecimal("123.45"))

            val r2 = result.materializedRows[1]
            assertThat(r2.getField(1)).isEqualTo(LocalDate.of(2024, 1, 1))
            assertThat(r2.getField(3)).isEqualTo(BigDecimal("-9999.99"))
        }
        finally {
            tryDropTable("test_schema.temporal_decimal")
        }
    }

    @Test
    fun testProjection() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.proj AS " +
                        "SELECT * FROM (VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)) AS t(id, name, amount)")
        try {
            val result = computeActual(
                    "SELECT name FROM test_schema.proj ORDER BY name")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("a")
            assertThat(result.materializedRows[1].getField(0)).isEqualTo("b")
            assertThat(result.materializedRows[2].getField(0)).isEqualTo("c")
        }
        finally {
            tryDropTable("test_schema.proj")
        }
    }

    @Test
    fun testPredicatePushdownEquality() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.pred_eq AS " +
                        "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300), (4, 400)) AS t(id, amount)")
        try {
            val result = computeActual(
                    "SELECT id FROM test_schema.pred_eq WHERE amount = 300")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows.first().getField(0)).isEqualTo(3)
        }
        finally {
            tryDropTable("test_schema.pred_eq")
        }
    }

    @Test
    fun testPredicatePushdownRange() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.pred_range AS " +
                        "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300), (4, 400)) AS t(id, amount)")
        try {
            val result = computeActual(
                    "SELECT id FROM test_schema.pred_range WHERE amount > 150 ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(2)
            assertThat(result.materializedRows[1].getField(0)).isEqualTo(3)
            assertThat(result.materializedRows[2].getField(0)).isEqualTo(4)
        }
        finally {
            tryDropTable("test_schema.pred_range")
        }
    }

    @Test
    fun testPredicatePushdownInList() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.pred_in AS " +
                        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) AS t(id, label)")
        try {
            val result = computeActual(
                    "SELECT id FROM test_schema.pred_in WHERE label IN ('b', 'd') ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(2)
            assertThat(result.materializedRows[1].getField(0)).isEqualTo(4)
        }
        finally {
            tryDropTable("test_schema.pred_in")
        }
    }

    @Test
    fun testPredicatePushdownIsNullAndIsNotNull() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.pred_nulls AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('a' AS VARCHAR)), " +
                        "  (2, CAST(NULL AS VARCHAR)), " +
                        "  (3, CAST('c' AS VARCHAR)), " +
                        "  (4, CAST(NULL AS VARCHAR))" +
                        ") AS t(id, opt)")
        try {
            val onlyNull = computeActual(
                    "SELECT id FROM test_schema.pred_nulls WHERE opt IS NULL ORDER BY id")
            assertThat(onlyNull.rowCount).isEqualTo(2)
            assertThat(onlyNull.materializedRows[0].getField(0)).isEqualTo(2)
            assertThat(onlyNull.materializedRows[1].getField(0)).isEqualTo(4)

            val notNull = computeActual(
                    "SELECT id FROM test_schema.pred_nulls WHERE opt IS NOT NULL ORDER BY id")
            assertThat(notNull.rowCount).isEqualTo(2)
            assertThat(notNull.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(notNull.materializedRows[1].getField(0)).isEqualTo(3)
        }
        finally {
            tryDropTable("test_schema.pred_nulls")
        }
    }

    @Test
    fun testPredicatePushdownDateAndDecimal() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.pred_typed AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, DATE '2026-01-01', CAST('100.50' AS DECIMAL(10,2))), " +
                        "  (2, DATE '2026-06-15', CAST('200.75' AS DECIMAL(10,2))), " +
                        "  (3, DATE '2026-12-31', CAST('300.00' AS DECIMAL(10,2)))" +
                        ") AS t(id, d, amt)")
        try {
            val byDate = computeActual(
                    "SELECT id FROM test_schema.pred_typed WHERE d >= DATE '2026-06-01' ORDER BY id")
            assertThat(byDate.rowCount).isEqualTo(2)
            assertThat(byDate.materializedRows[0].getField(0)).isEqualTo(2)
            assertThat(byDate.materializedRows[1].getField(0)).isEqualTo(3)

            val byDecimal = computeActual(
                    "SELECT id FROM test_schema.pred_typed WHERE amt = CAST('200.75' AS DECIMAL(10,2))")
            assertThat(byDecimal.rowCount).isEqualTo(1)
            assertThat(byDecimal.materializedRows.first().getField(0)).isEqualTo(2)
        }
        finally {
            tryDropTable("test_schema.pred_typed")
        }
    }

    @Test
    fun testPredicateOnUnpushedColumnStillCorrect() {
        // VARBINARY isn't pushed down (the translator skips it), but Trino's filter
        // operator above still applies. Result must remain correct.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.pred_unpushed AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, X'AABB'), " +
                        "  (2, X'CCDD'), " +
                        "  (3, X'EEFF')" +
                        ") AS t(id, payload)")
        try {
            val result = computeActual(
                    "SELECT id FROM test_schema.pred_unpushed WHERE payload = X'CCDD'")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows.first().getField(0)).isEqualTo(2)
        }
        finally {
            tryDropTable("test_schema.pred_unpushed")
        }
    }

    @Test
    fun testAggregation() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.agg AS " +
                        "SELECT * FROM (VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40)) AS t(id, grp, amt)")
        try {
            val result = computeActual(
                    "SELECT grp, sum(amt) FROM test_schema.agg GROUP BY grp ORDER BY grp")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("A")
            assertThat(result.materializedRows[0].getField(1)).isEqualTo(30L)
            assertThat(result.materializedRows[1].getField(0)).isEqualTo("B")
            assertThat(result.materializedRows[1].getField(1)).isEqualTo(70L)
        }
        finally {
            tryDropTable("test_schema.agg")
        }
    }

    @Test
    fun testCountStarNoPredicate() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.cnt_all AS " +
                        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) AS t(id, label)")
        try {
            val result = computeActual("SELECT count(*) FROM test_schema.cnt_all")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows.first().getField(0)).isEqualTo(5L)
        }
        finally {
            tryDropTable("test_schema.cnt_all")
        }
    }

    @Test
    fun testCountStarWithPushedPredicate() {
        // The predicate pushes into the DuckDB SELECT, so the row count comes back
        // pre-filtered. The empty-projection path still needs to honor it.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.cnt_pred AS " +
                        "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300), (4, 400)) AS t(id, amount)")
        try {
            val result = computeActual(
                    "SELECT count(*) FROM test_schema.cnt_pred WHERE amount > 150")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows.first().getField(0)).isEqualTo(3L)
        }
        finally {
            tryDropTable("test_schema.cnt_pred")
        }
    }

    @Test
    fun testCountColumnWithEqualityPredicate() {
        // Mirrors a TPC-H lookup: COUNT of a specific column with an equality on the
        // same column. Non-empty projection (because of the named column) plus a
        // pushed-down predicate.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.cnt_col AS " +
                        "SELECT * FROM (VALUES (1, 100), (2, 200), (12345, 300), (4, 400)) AS t(orderkey, amount)")
        try {
            val result = computeActual(
                    "SELECT count(orderkey) FROM test_schema.cnt_col WHERE orderkey = 12345")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows.first().getField(0)).isEqualTo(1L)
        }
        finally {
            tryDropTable("test_schema.cnt_col")
        }
    }

    @Test
    fun testTpchQ1Shape() {
        // Replicates the shape of TPC-H Q1: GROUP BY on two columns, multiple
        // aggregates including one over a derived expression, with a date predicate
        // that should push into DuckDB SQL. Exercises the side-by-side pattern from
        // the compose/sql/tpch-duckdb-format.sql test script.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.q1 AS " +
                        "SELECT * FROM (VALUES " +
                        "  (DATE '1998-08-01', CAST('A' AS VARCHAR), CAST('F' AS VARCHAR), CAST(10 AS DOUBLE), CAST(100 AS DOUBLE), CAST(0.05 AS DOUBLE)), " +
                        "  (DATE '1998-08-15', CAST('A' AS VARCHAR), CAST('F' AS VARCHAR), CAST(20 AS DOUBLE), CAST(200 AS DOUBLE), CAST(0.10 AS DOUBLE)), " +
                        "  (DATE '1998-09-01', CAST('N' AS VARCHAR), CAST('O' AS VARCHAR), CAST(30 AS DOUBLE), CAST(300 AS DOUBLE), CAST(0.00 AS DOUBLE)), " +
                        "  (DATE '1998-12-01', CAST('R' AS VARCHAR), CAST('F' AS VARCHAR), CAST(40 AS DOUBLE), CAST(400 AS DOUBLE), CAST(0.20 AS DOUBLE))" +
                        ") AS t(shipdate, returnflag, linestatus, qty, extprice, discount)")
        try {
            val result = computeActual(
                    "SELECT returnflag, linestatus, " +
                            "  sum(qty) AS sum_qty, " +
                            "  sum(extprice * (1 - discount)) AS sum_disc_price, " +
                            "  count(*) AS cnt " +
                            "FROM test_schema.q1 " +
                            "WHERE shipdate <= DATE '1998-09-02' " +
                            "GROUP BY returnflag, linestatus " +
                            "ORDER BY returnflag, linestatus")
            assertThat(result.rowCount).isEqualTo(2)

            // Group ('A', 'F'): 2 rows, qty 10+20=30, disc_price 100*0.95 + 200*0.90 = 95 + 180 = 275
            val groupAF = result.materializedRows[0]
            assertThat(groupAF.getField(0)).isEqualTo("A")
            assertThat(groupAF.getField(1)).isEqualTo("F")
            assertThat((groupAF.getField(2) as Number).toDouble()).isEqualTo(30.0)
            assertThat((groupAF.getField(3) as Number).toDouble()).isEqualTo(275.0)
            assertThat(groupAF.getField(4)).isEqualTo(2L)

            // Group ('N', 'O'): 1 row, qty 30, disc_price 300*1.00 = 300
            val groupNO = result.materializedRows[1]
            assertThat(groupNO.getField(0)).isEqualTo("N")
            assertThat(groupNO.getField(1)).isEqualTo("O")
            assertThat((groupNO.getField(2) as Number).toDouble()).isEqualTo(30.0)
            assertThat((groupNO.getField(3) as Number).toDouble()).isEqualTo(300.0)
            assertThat(groupNO.getField(4)).isEqualTo(1L)
        }
        finally {
            tryDropTable("test_schema.q1")
        }
    }

    @Test
    fun testMixedFormatJoin() {
        // Two tables, one parquet (default) and one duckdb. Joining them exercises
        // both readers in a single query.
        computeActual("CREATE TABLE test_schema.mix_parquet AS " +
                "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(id, label)")
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.mix_duckdb AS " +
                        "SELECT * FROM (VALUES (1, 100), (2, 200), (3, 300)) AS t(id, amount)")
        try {
            val result = computeActual(
                    "SELECT p.label, d.amount " +
                            "FROM test_schema.mix_parquet p JOIN test_schema.mix_duckdb d ON p.id = d.id " +
                            "ORDER BY p.id")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("one")
            assertThat(result.materializedRows[0].getField(1)).isEqualTo(100)
            assertThat(result.materializedRows[2].getField(0)).isEqualTo("three")
            assertThat(result.materializedRows[2].getField(1)).isEqualTo(300)

            // Sanity check that the formats really differ in storage
            val parquetFiles = computeActual(
                    "SELECT file_format FROM \"mix_parquet\$files\"")
            assertThat(parquetFiles.materializedRows[0].getField(0)).isEqualTo("parquet")
            val duckdbFiles = computeActual(
                    "SELECT file_format FROM \"mix_duckdb\$files\"")
            assertThat(duckdbFiles.materializedRows[0].getField(0)).isEqualTo("duckdb")
        }
        finally {
            tryDropTable("test_schema.mix_parquet")
            tryDropTable("test_schema.mix_duckdb")
        }
    }
}
