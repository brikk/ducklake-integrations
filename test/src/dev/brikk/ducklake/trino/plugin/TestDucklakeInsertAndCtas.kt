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

import io.trino.testing.MaterializedRow
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * The "data goes in, data comes out" half of the connector's write surface: INSERT (basic,
 * multi-batch, all-types, nested types, NULLs, empty), CTAS (basic, empty, type-bearing),
 * post-write SELECT/aggregations, lifecycle (insert-then-drop, multiple tables) and
 * boundary values (10K-char strings, BIGINT MAX, large filtered scans).
 *
 * Snapshot/metadata verification of writes lives in
 * [TestDucklakeWriteMetadataAndSnapshot]; partitioned-write specifics live in
 * [TestDucklakePartitionedWrite].
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeInsertAndCtas : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "write-insert-and-ctas"
    }

    // ==================== Basic INSERT ====================

    @Test
    fun testBasicInsert() {
        computeActual("CREATE TABLE test_schema.basic_insert (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.basic_insert VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

            val result = computeActual("SELECT * FROM test_schema.basic_insert ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("alice")
            assertThat(result.materializedRows[2].getField(0)).isEqualTo(3)
            assertThat(result.materializedRows[2].getField(1)).isEqualTo("charlie")
        }
        finally {
            tryDropTable("test_schema.basic_insert")
        }
    }

    @Test
    fun testMultipleInserts() {
        computeActual("CREATE TABLE test_schema.multi_insert (id INTEGER, value DOUBLE)")
        try {
            computeActual("INSERT INTO test_schema.multi_insert VALUES (1, 10.0), (2, 20.0)")
            computeActual("INSERT INTO test_schema.multi_insert VALUES (3, 30.0), (4, 40.0), (5, 50.0)")

            val result = computeActual("SELECT count(*) FROM test_schema.multi_insert")
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(5L)

            val sum = computeActual("SELECT sum(value) FROM test_schema.multi_insert")
            assertThat(sum.materializedRows[0].getField(0)).isEqualTo(150.0)
        }
        finally {
            tryDropTable("test_schema.multi_insert")
        }
    }

    // ==================== Type coverage ====================

    @Test
    fun testInsertAllPrimitiveTypes() {
        computeActual("CREATE TABLE test_schema.all_types (" +
                "col_tinyint TINYINT, " +
                "col_smallint SMALLINT, " +
                "col_int INTEGER, " +
                "col_bigint BIGINT, " +
                "col_real REAL, " +
                "col_double DOUBLE, " +
                "col_decimal DECIMAL(10,2), " +
                "col_varchar VARCHAR, " +
                "col_boolean BOOLEAN, " +
                "col_date DATE" +
                ")")
        try {
            computeActual("INSERT INTO test_schema.all_types VALUES " +
                    "(TINYINT '1', SMALLINT '100', 10000, BIGINT '1000000000', REAL '1.5', 2.5, DECIMAL '123.45', 'hello', true, DATE '2024-01-15')")

            val result = computeActual("SELECT * FROM test_schema.all_types")
            assertThat(result.rowCount).isEqualTo(1)
            val row: MaterializedRow = result.materializedRows[0]
            assertThat(row.getField(0)).isEqualTo(1.toByte())
            assertThat(row.getField(1)).isEqualTo(100.toShort())
            assertThat(row.getField(2)).isEqualTo(10000)
            assertThat(row.getField(3)).isEqualTo(1000000000L)
            assertThat(row.getField(7)).isEqualTo("hello")
            assertThat(row.getField(8)).isEqualTo(true)
        }
        finally {
            tryDropTable("test_schema.all_types")
        }
    }

    @Test
    fun testInsertNestedTypes() {
        computeActual("CREATE TABLE test_schema.nested_insert (" +
                "id INTEGER, " +
                "tags ARRAY(VARCHAR), " +
                "metadata ROW(key VARCHAR, value VARCHAR), " +
                "attrs MAP(VARCHAR, INTEGER)" +
                ")")
        try {
            computeActual("INSERT INTO test_schema.nested_insert VALUES " +
                    "(1, ARRAY['a', 'b', 'c'], ROW('color', 'red'), MAP(ARRAY['x', 'y'], ARRAY[10, 20]))")

            val result = computeActual("SELECT id, cardinality(tags), metadata.key FROM test_schema.nested_insert")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo(3L)
            assertThat(result.materializedRows[0].getField(2)).isEqualTo("color")
        }
        finally {
            tryDropTable("test_schema.nested_insert")
        }
    }

    // ==================== NULL handling ====================

    @Test
    fun testInsertWithNulls() {
        computeActual("CREATE TABLE test_schema.null_insert (id INTEGER, name VARCHAR, value DOUBLE)")
        try {
            computeActual("INSERT INTO test_schema.null_insert VALUES (1, 'present', 10.0), (2, NULL, NULL), (NULL, NULL, NULL)")

            val result = computeActual("SELECT count(*) FROM test_schema.null_insert")
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)

            val nullCount = computeActual("SELECT count(*) FROM test_schema.null_insert WHERE name IS NULL")
            assertThat(nullCount.materializedRows[0].getField(0)).isEqualTo(2L)
        }
        finally {
            tryDropTable("test_schema.null_insert")
        }
    }

    @Test
    fun testInsertEmptyResult() {
        computeActual("CREATE TABLE test_schema.empty_insert (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.empty_insert SELECT * FROM (VALUES (1, 'x')) t(id, name) WHERE false")

            val result = computeActual("SELECT count(*) FROM test_schema.empty_insert")
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(0L)
        }
        finally {
            tryDropTable("test_schema.empty_insert")
        }
    }

    // ==================== CTAS ====================

    @Test
    fun testCtas() {
        computeActual("CREATE TABLE test_schema.ctas_source (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.ctas_source VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

            computeActual("CREATE TABLE test_schema.ctas_target AS SELECT * FROM test_schema.ctas_source")
            try {
                val result = computeActual("SELECT * FROM test_schema.ctas_target ORDER BY id")
                assertThat(result.rowCount).isEqualTo(3)
                assertThat(result.materializedRows[0].getField(1)).isEqualTo("alpha")
                assertThat(result.materializedRows[2].getField(1)).isEqualTo("gamma")
            }
            finally {
                tryDropTable("test_schema.ctas_target")
            }
        }
        finally {
            tryDropTable("test_schema.ctas_source")
        }
    }

    @Test
    fun testCtasEmpty() {
        computeActual("CREATE TABLE test_schema.ctas_empty AS SELECT CAST(1 AS INTEGER) AS id, CAST('x' AS VARCHAR) AS name WHERE false")
        try {
            val result = computeActual("SELECT count(*) FROM test_schema.ctas_empty")
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(0L)

            // Verify table exists with correct schema
            val columns = computeActual("DESCRIBE test_schema.ctas_empty")
            val columnNames: List<String> = columns.materializedRows.stream()
                    .map { row -> row.getField(0).toString() }
                    .toList()
            assertThat(columnNames).containsExactly("id", "name")
        }
        finally {
            tryDropTable("test_schema.ctas_empty")
        }
    }

    @Test
    fun testCtasWithTypes() {
        computeActual("CREATE TABLE test_schema.ctas_types AS " +
                "SELECT CAST(1 AS INTEGER) AS int_col, CAST(2.5 AS DOUBLE) AS dbl_col, " +
                "CAST('hello' AS VARCHAR) AS str_col, true AS bool_col")
        try {
            val result = computeActual("SELECT * FROM test_schema.ctas_types")
            assertThat(result.rowCount).isEqualTo(1)
            val row: MaterializedRow = result.materializedRows[0]
            assertThat(row.getField(0)).isEqualTo(1)
            assertThat(row.getField(1)).isEqualTo(2.5)
            assertThat(row.getField(2)).isEqualTo("hello")
            assertThat(row.getField(3)).isEqualTo(true)
        }
        finally {
            tryDropTable("test_schema.ctas_types")
        }
    }

    // ==================== Aggregations ====================

    @Test
    fun testInsertThenSelectAggregations() {
        computeActual("CREATE TABLE test_schema.agg_test (category VARCHAR, amount DOUBLE, quantity INTEGER)")
        try {
            computeActual("INSERT INTO test_schema.agg_test VALUES " +
                    "('A', 10.0, 1), ('A', 20.0, 2), ('B', 30.0, 3), ('B', 40.0, 4), ('C', 50.0, 5)")

            val counts = computeActual(
                    "SELECT category, count(*), sum(amount), avg(quantity) FROM test_schema.agg_test " +
                            "GROUP BY category ORDER BY category")
            assertThat(counts.rowCount).isEqualTo(3)

            // Category A: count=2, sum=30.0
            assertThat(counts.materializedRows[0].getField(0)).isEqualTo("A")
            assertThat(counts.materializedRows[0].getField(1)).isEqualTo(2L)
            assertThat(counts.materializedRows[0].getField(2)).isEqualTo(30.0)
        }
        finally {
            tryDropTable("test_schema.agg_test")
        }
    }

    // ==================== Lifecycle ====================

    @Test
    fun testInsertThenDrop() {
        computeActual("CREATE TABLE test_schema.lifecycle_test (id INTEGER)")
        computeActual("INSERT INTO test_schema.lifecycle_test VALUES (1), (2), (3)")

        // Verify data exists
        val result = computeActual("SELECT count(*) FROM test_schema.lifecycle_test")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)

        // Drop and verify gone
        computeActual("DROP TABLE test_schema.lifecycle_test")
        val tables: List<String> = computeActual("SHOW TABLES FROM test_schema").materializedRows.stream()
                .map { row -> row.getField(0).toString() }
                .toList()
        assertThat(tables).doesNotContain("lifecycle_test")
    }

    @Test
    fun testMultipleTablesInsert() {
        computeActual("CREATE TABLE test_schema.multi_a (id INTEGER, value VARCHAR)")
        computeActual("CREATE TABLE test_schema.multi_b (id INTEGER, amount DOUBLE)")
        try {
            computeActual("INSERT INTO test_schema.multi_a VALUES (1, 'first'), (2, 'second')")
            computeActual("INSERT INTO test_schema.multi_b VALUES (10, 100.0), (20, 200.0), (30, 300.0)")

            val countA = computeActual("SELECT count(*) FROM test_schema.multi_a")
            assertThat(countA.materializedRows[0].getField(0)).isEqualTo(2L)

            val countB = computeActual("SELECT count(*) FROM test_schema.multi_b")
            assertThat(countB.materializedRows[0].getField(0)).isEqualTo(3L)
        }
        finally {
            tryDropTable("test_schema.multi_a")
            tryDropTable("test_schema.multi_b")
        }
    }

    // ==================== Large/boundary values ====================

    @Test
    fun testInsertLargeValues() {
        computeActual("CREATE TABLE test_schema.large_values (id BIGINT, long_text VARCHAR, big_number DOUBLE)")
        try {
            val longString = "x".repeat(10000)
            computeActual("INSERT INTO test_schema.large_values VALUES " +
                    "(9223372036854775807, '" + longString + "', 1.7976931348623157E308)")

            val result = computeActual("SELECT id, length(long_text), big_number FROM test_schema.large_values")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(9223372036854775807L)
            assertThat(result.materializedRows[0].getField(1) as Long).isEqualTo(10000L)
        }
        finally {
            tryDropTable("test_schema.large_values")
        }
    }

    @Test
    fun testInsertThenSelectWithFilter() {
        computeActual("CREATE TABLE test_schema.filter_test (id INTEGER, status VARCHAR, amount DOUBLE)")
        try {
            computeActual("INSERT INTO test_schema.filter_test VALUES " +
                    "(1, 'active', 10.0), (2, 'inactive', 20.0), (3, 'active', 30.0), " +
                    "(4, 'inactive', 40.0), (5, 'active', 50.0)")

            val active = computeActual(
                    "SELECT id, amount FROM test_schema.filter_test WHERE status = 'active' ORDER BY id")
            assertThat(active.rowCount).isEqualTo(3)
            assertThat(active.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(active.materializedRows[2].getField(0)).isEqualTo(5)

            val highAmount = computeActual(
                    "SELECT count(*) FROM test_schema.filter_test WHERE amount > 25.0")
            assertThat(highAmount.materializedRows[0].getField(0)).isEqualTo(3L)
        }
        finally {
            tryDropTable("test_schema.filter_test")
        }
    }

    @Test
    fun testInsertFromSelect() {
        computeActual("CREATE TABLE test_schema.insert_src (id INTEGER, name VARCHAR)")
        computeActual("CREATE TABLE test_schema.insert_dst (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.insert_src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            computeActual("INSERT INTO test_schema.insert_dst SELECT * FROM test_schema.insert_src WHERE id >= 2")

            val result = computeActual("SELECT * FROM test_schema.insert_dst ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(2)
            assertThat(result.materializedRows[1].getField(0)).isEqualTo(3)
        }
        finally {
            tryDropTable("test_schema.insert_src")
            tryDropTable("test_schema.insert_dst")
        }
    }
}
