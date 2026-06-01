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

import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The "data goes in, data comes out" half of the connector's write surface: INSERT (basic,
 * multi-batch, all-types, nested types, NULLs, empty), CTAS (basic, empty, type-bearing),
 * post-write SELECT/aggregations, lifecycle (insert-then-drop, multiple tables) and
 * boundary values (10K-char strings, BIGINT MAX, large filtered scans).
 *
 * <p>Snapshot/metadata verification of writes lives in
 * {@link TestDucklakeWriteMetadataAndSnapshot}; partitioned-write specifics live in
 * {@link TestDucklakePartitionedWrite}.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeInsertAndCtas
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "write-insert-and-ctas";
    }

    // ==================== Basic INSERT ====================

    @Test
    public void testBasicInsert()
    {
        computeActual("CREATE TABLE test_schema.basic_insert (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.basic_insert VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");

            MaterializedResult result = computeActual("SELECT * FROM test_schema.basic_insert ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("alice");
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo("charlie");
        }
        finally {
            tryDropTable("test_schema.basic_insert");
        }
    }

    @Test
    public void testMultipleInserts()
    {
        computeActual("CREATE TABLE test_schema.multi_insert (id INTEGER, value DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.multi_insert VALUES (1, 10.0), (2, 20.0)");
            computeActual("INSERT INTO test_schema.multi_insert VALUES (3, 30.0), (4, 40.0), (5, 50.0)");

            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.multi_insert");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);

            MaterializedResult sum = computeActual("SELECT sum(value) FROM test_schema.multi_insert");
            assertThat(sum.getMaterializedRows().get(0).getField(0)).isEqualTo(150.0);
        }
        finally {
            tryDropTable("test_schema.multi_insert");
        }
    }

    // ==================== Type coverage ====================

    @Test
    public void testInsertAllPrimitiveTypes()
    {
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
                ")");
        try {
            computeActual("INSERT INTO test_schema.all_types VALUES " +
                    "(TINYINT '1', SMALLINT '100', 10000, BIGINT '1000000000', REAL '1.5', 2.5, DECIMAL '123.45', 'hello', true, DATE '2024-01-15')");

            MaterializedResult result = computeActual("SELECT * FROM test_schema.all_types");
            assertThat(result.getRowCount()).isEqualTo(1);
            MaterializedRow row = result.getMaterializedRows().get(0);
            assertThat(row.getField(0)).isEqualTo((byte) 1);
            assertThat(row.getField(1)).isEqualTo((short) 100);
            assertThat(row.getField(2)).isEqualTo(10000);
            assertThat(row.getField(3)).isEqualTo(1000000000L);
            assertThat(row.getField(7)).isEqualTo("hello");
            assertThat(row.getField(8)).isEqualTo(true);
        }
        finally {
            tryDropTable("test_schema.all_types");
        }
    }

    @Test
    public void testInsertNestedTypes()
    {
        computeActual("CREATE TABLE test_schema.nested_insert (" +
                "id INTEGER, " +
                "tags ARRAY(VARCHAR), " +
                "metadata ROW(key VARCHAR, value VARCHAR), " +
                "attrs MAP(VARCHAR, INTEGER)" +
                ")");
        try {
            computeActual("INSERT INTO test_schema.nested_insert VALUES " +
                    "(1, ARRAY['a', 'b', 'c'], ROW('color', 'red'), MAP(ARRAY['x', 'y'], ARRAY[10, 20]))");

            MaterializedResult result = computeActual("SELECT id, cardinality(tags), metadata.key FROM test_schema.nested_insert");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(3L);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo("color");
        }
        finally {
            tryDropTable("test_schema.nested_insert");
        }
    }

    // ==================== NULL handling ====================

    @Test
    public void testInsertWithNulls()
    {
        computeActual("CREATE TABLE test_schema.null_insert (id INTEGER, name VARCHAR, value DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.null_insert VALUES (1, 'present', 10.0), (2, NULL, NULL), (NULL, NULL, NULL)");

            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.null_insert");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            MaterializedResult nullCount = computeActual("SELECT count(*) FROM test_schema.null_insert WHERE name IS NULL");
            assertThat(nullCount.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
        }
        finally {
            tryDropTable("test_schema.null_insert");
        }
    }

    @Test
    public void testInsertEmptyResult()
    {
        computeActual("CREATE TABLE test_schema.empty_insert (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.empty_insert SELECT * FROM (VALUES (1, 'x')) t(id, name) WHERE false");

            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.empty_insert");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);
        }
        finally {
            tryDropTable("test_schema.empty_insert");
        }
    }

    // ==================== CTAS ====================

    @Test
    public void testCtas()
    {
        computeActual("CREATE TABLE test_schema.ctas_source (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.ctas_source VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");

            computeActual("CREATE TABLE test_schema.ctas_target AS SELECT * FROM test_schema.ctas_source");
            try {
                MaterializedResult result = computeActual("SELECT * FROM test_schema.ctas_target ORDER BY id");
                assertThat(result.getRowCount()).isEqualTo(3);
                assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("alpha");
                assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo("gamma");
            }
            finally {
                tryDropTable("test_schema.ctas_target");
            }
        }
        finally {
            tryDropTable("test_schema.ctas_source");
        }
    }

    @Test
    public void testCtasEmpty()
    {
        computeActual("CREATE TABLE test_schema.ctas_empty AS SELECT CAST(1 AS INTEGER) AS id, CAST('x' AS VARCHAR) AS name WHERE false");
        try {
            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.ctas_empty");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);

            // Verify table exists with correct schema
            MaterializedResult columns = computeActual("DESCRIBE test_schema.ctas_empty");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "name");
        }
        finally {
            tryDropTable("test_schema.ctas_empty");
        }
    }

    @Test
    public void testCtasWithTypes()
    {
        computeActual("CREATE TABLE test_schema.ctas_types AS " +
                "SELECT CAST(1 AS INTEGER) AS int_col, CAST(2.5 AS DOUBLE) AS dbl_col, " +
                "CAST('hello' AS VARCHAR) AS str_col, true AS bool_col");
        try {
            MaterializedResult result = computeActual("SELECT * FROM test_schema.ctas_types");
            assertThat(result.getRowCount()).isEqualTo(1);
            MaterializedRow row = result.getMaterializedRows().get(0);
            assertThat(row.getField(0)).isEqualTo(1);
            assertThat(row.getField(1)).isEqualTo(2.5);
            assertThat(row.getField(2)).isEqualTo("hello");
            assertThat(row.getField(3)).isEqualTo(true);
        }
        finally {
            tryDropTable("test_schema.ctas_types");
        }
    }

    // ==================== Aggregations ====================

    @Test
    public void testInsertThenSelectAggregations()
    {
        computeActual("CREATE TABLE test_schema.agg_test (category VARCHAR, amount DOUBLE, quantity INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.agg_test VALUES " +
                    "('A', 10.0, 1), ('A', 20.0, 2), ('B', 30.0, 3), ('B', 40.0, 4), ('C', 50.0, 5)");

            MaterializedResult counts = computeActual(
                    "SELECT category, count(*), sum(amount), avg(quantity) FROM test_schema.agg_test " +
                            "GROUP BY category ORDER BY category");
            assertThat(counts.getRowCount()).isEqualTo(3);

            // Category A: count=2, sum=30.0
            assertThat(counts.getMaterializedRows().get(0).getField(0)).isEqualTo("A");
            assertThat(counts.getMaterializedRows().get(0).getField(1)).isEqualTo(2L);
            assertThat(counts.getMaterializedRows().get(0).getField(2)).isEqualTo(30.0);
        }
        finally {
            tryDropTable("test_schema.agg_test");
        }
    }

    // ==================== Lifecycle ====================

    @Test
    public void testInsertThenDrop()
    {
        computeActual("CREATE TABLE test_schema.lifecycle_test (id INTEGER)");
        computeActual("INSERT INTO test_schema.lifecycle_test VALUES (1), (2), (3)");

        // Verify data exists
        MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.lifecycle_test");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

        // Drop and verify gone
        computeActual("DROP TABLE test_schema.lifecycle_test");
        List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(tables).doesNotContain("lifecycle_test");
    }

    @Test
    public void testMultipleTablesInsert()
    {
        computeActual("CREATE TABLE test_schema.multi_a (id INTEGER, value VARCHAR)");
        computeActual("CREATE TABLE test_schema.multi_b (id INTEGER, amount DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.multi_a VALUES (1, 'first'), (2, 'second')");
            computeActual("INSERT INTO test_schema.multi_b VALUES (10, 100.0), (20, 200.0), (30, 300.0)");

            MaterializedResult countA = computeActual("SELECT count(*) FROM test_schema.multi_a");
            assertThat(countA.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);

            MaterializedResult countB = computeActual("SELECT count(*) FROM test_schema.multi_b");
            assertThat(countB.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
        }
        finally {
            tryDropTable("test_schema.multi_a");
            tryDropTable("test_schema.multi_b");
        }
    }

    // ==================== Large/boundary values ====================

    @Test
    public void testInsertLargeValues()
    {
        computeActual("CREATE TABLE test_schema.large_values (id BIGINT, long_text VARCHAR, big_number DOUBLE)");
        try {
            String longString = "x".repeat(10000);
            computeActual("INSERT INTO test_schema.large_values VALUES " +
                    "(9223372036854775807, '" + longString + "', 1.7976931348623157E308)");

            MaterializedResult result = computeActual("SELECT id, length(long_text), big_number FROM test_schema.large_values");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(9223372036854775807L);
            assertThat((long) result.getMaterializedRows().get(0).getField(1)).isEqualTo(10000L);
        }
        finally {
            tryDropTable("test_schema.large_values");
        }
    }

    @Test
    public void testInsertThenSelectWithFilter()
    {
        computeActual("CREATE TABLE test_schema.filter_test (id INTEGER, status VARCHAR, amount DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.filter_test VALUES " +
                    "(1, 'active', 10.0), (2, 'inactive', 20.0), (3, 'active', 30.0), " +
                    "(4, 'inactive', 40.0), (5, 'active', 50.0)");

            MaterializedResult active = computeActual(
                    "SELECT id, amount FROM test_schema.filter_test WHERE status = 'active' ORDER BY id");
            assertThat(active.getRowCount()).isEqualTo(3);
            assertThat(active.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(active.getMaterializedRows().get(2).getField(0)).isEqualTo(5);

            MaterializedResult highAmount = computeActual(
                    "SELECT count(*) FROM test_schema.filter_test WHERE amount > 25.0");
            assertThat(highAmount.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
        }
        finally {
            tryDropTable("test_schema.filter_test");
        }
    }

    @Test
    public void testInsertFromSelect()
    {
        computeActual("CREATE TABLE test_schema.insert_src (id INTEGER, name VARCHAR)");
        computeActual("CREATE TABLE test_schema.insert_dst (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.insert_src VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            computeActual("INSERT INTO test_schema.insert_dst SELECT * FROM test_schema.insert_src WHERE id >= 2");

            MaterializedResult result = computeActual("SELECT * FROM test_schema.insert_dst ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
        }
        finally {
            tryDropTable("test_schema.insert_src");
            tryDropTable("test_schema.insert_dst");
        }
    }
}
