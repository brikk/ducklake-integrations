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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeWriteIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("write-integration")
                .build();
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

    // ==================== Snapshot and metadata verification ====================

    @Test
    public void testSnapshotMetadataAfterInsert()
    {
        computeActual("CREATE TABLE test_schema.snapshot_verify (id INTEGER, name VARCHAR)");
        try {
            // Get snapshot before insert
            MaterializedResult beforeInsert = computeActual(
                    "SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
            long snapshotBeforeInsert = (long) beforeInsert.getMaterializedRows().get(0).getField(0);

            computeActual("INSERT INTO test_schema.snapshot_verify VALUES (1, 'test')");

            // Verify new snapshot was created
            MaterializedResult afterInsert = computeActual(
                    "SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
            long snapshotAfterInsert = (long) afterInsert.getMaterializedRows().get(0).getField(0);
            assertThat(snapshotAfterInsert).isGreaterThan(snapshotBeforeInsert);

            // Verify change record
            MaterializedResult changes = computeActual(
                    "SELECT changes_made FROM \"simple_table$snapshot_changes\" WHERE snapshot_id = " + snapshotAfterInsert);
            assertThat(changes.getRowCount()).isGreaterThan(0);
            assertThat(changes.getMaterializedRows().get(0).getField(0).toString()).contains("inserted_into_table:");
        }
        finally {
            tryDropTable("test_schema.snapshot_verify");
        }
    }

    @Test
    public void testDataFileMetadataAfterInsert()
    {
        computeActual("CREATE TABLE test_schema.file_verify (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.file_verify VALUES (1, 'test'), (2, 'test2')");

            MaterializedResult files = computeActual("SELECT * FROM \"file_verify$files\"");
            assertThat(files.getRowCount()).isGreaterThanOrEqualTo(1);

            // Verify the file has correct record count
            MaterializedResult fileCounts = computeActual(
                    "SELECT record_count, file_format FROM \"file_verify$files\"");
            MaterializedRow fileRow = fileCounts.getMaterializedRows().get(0);
            assertThat((long) fileRow.getField(0)).isEqualTo(2L);
            assertThat(fileRow.getField(1)).isEqualTo("parquet");
        }
        finally {
            tryDropTable("test_schema.file_verify");
        }
    }

    @Test
    public void testTableStatsAfterInsert()
    {
        computeActual("CREATE TABLE test_schema.stats_verify (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.stats_verify VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");

            // Verify the data is actually there and countable
            MaterializedResult countResult = computeActual("SELECT count(*) FROM test_schema.stats_verify");
            assertThat(countResult.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);

            // Verify SHOW STATS returns something (exact format varies)
            MaterializedResult stats = computeActual("SHOW STATS FOR test_schema.stats_verify");
            assertThat(stats.getRowCount()).isGreaterThan(0);
        }
        finally {
            tryDropTable("test_schema.stats_verify");
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

    // ==================== Partitioned writes ====================

    @Test
    public void testPartitionedInsertIdentity()
    {
        computeActual("CREATE TABLE test_schema.part_identity (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            computeActual("INSERT INTO test_schema.part_identity VALUES " +
                    "(1, 'US', 10.0), (2, 'EU', 20.0), (3, 'US', 30.0), (4, 'APAC', 40.0)");

            // Verify all data is readable
            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.part_identity");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(4L);

            // Verify partition filtering works
            MaterializedResult usData = computeActual(
                    "SELECT id, amount FROM test_schema.part_identity WHERE region = 'US' ORDER BY id");
            assertThat(usData.getRowCount()).isEqualTo(2);
            assertThat(usData.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(usData.getMaterializedRows().get(1).getField(0)).isEqualTo(3);

            MaterializedResult euData = computeActual(
                    "SELECT count(*) FROM test_schema.part_identity WHERE region = 'EU'");
            assertThat(euData.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);
        }
        finally {
            tryDropTable("test_schema.part_identity");
        }
    }

    @Test
    public void testPartitionedInsertTemporalYear()
    {
        computeActual("CREATE TABLE test_schema.part_year (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)'])");
        try {
            computeActual("INSERT INTO test_schema.part_year VALUES " +
                    "(1, DATE '2023-01-15', 10.0), " +
                    "(2, DATE '2023-06-20', 20.0), " +
                    "(3, DATE '2024-03-05', 30.0)");

            MaterializedResult all = computeActual("SELECT count(*) FROM test_schema.part_year");
            assertThat(all.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            // Verify year-based partition filtering
            MaterializedResult y2023 = computeActual(
                    "SELECT count(*) FROM test_schema.part_year WHERE event_date >= DATE '2023-01-01' AND event_date < DATE '2024-01-01'");
            assertThat(y2023.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
        }
        finally {
            tryDropTable("test_schema.part_year");
        }
    }

    @Test
    public void testPartitionedInsertTemporalYearMonth()
    {
        computeActual("CREATE TABLE test_schema.part_ym (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)', 'month(event_date)'])");
        try {
            computeActual("INSERT INTO test_schema.part_ym VALUES " +
                    "(1, DATE '2023-01-15', 100.0), " +
                    "(2, DATE '2023-01-20', 200.0), " +
                    "(3, DATE '2023-06-10', 300.0), " +
                    "(4, DATE '2024-03-05', 400.0)");

            MaterializedResult all = computeActual("SELECT count(*) FROM test_schema.part_ym");
            assertThat(all.getMaterializedRows().get(0).getField(0)).isEqualTo(4L);

            // Verify values are correct
            MaterializedResult ordered = computeActual(
                    "SELECT id, amount FROM test_schema.part_ym ORDER BY id");
            assertThat(ordered.getRowCount()).isEqualTo(4);
            assertThat(ordered.getMaterializedRows().get(0).getField(1)).isEqualTo(100.0);
            assertThat(ordered.getMaterializedRows().get(3).getField(1)).isEqualTo(400.0);
        }
        finally {
            tryDropTable("test_schema.part_ym");
        }
    }

    @Test
    public void testPartitionedCtas()
    {
        computeActual("CREATE TABLE test_schema.part_ctas_src (id INTEGER, region VARCHAR, value DOUBLE)");
        try {
            computeActual("INSERT INTO test_schema.part_ctas_src VALUES " +
                    "(1, 'US', 10.0), (2, 'EU', 20.0), (3, 'US', 30.0)");

            computeActual("CREATE TABLE test_schema.part_ctas_dst " +
                    "WITH (partitioned_by = ARRAY['region']) AS " +
                    "SELECT * FROM test_schema.part_ctas_src");

            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.part_ctas_dst");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            MaterializedResult usData = computeActual(
                    "SELECT id FROM test_schema.part_ctas_dst WHERE region = 'US' ORDER BY id");
            assertThat(usData.getRowCount()).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.part_ctas_src");
            tryDropTable("test_schema.part_ctas_dst");
        }
    }

    @Test
    public void testPartitionedMultipleInserts()
    {
        computeActual("CREATE TABLE test_schema.part_multi (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            computeActual("INSERT INTO test_schema.part_multi VALUES (1, 'US', 10.0), (2, 'EU', 20.0)");
            computeActual("INSERT INTO test_schema.part_multi VALUES (3, 'US', 30.0), (4, 'APAC', 40.0)");
            computeActual("INSERT INTO test_schema.part_multi VALUES (5, 'EU', 50.0)");

            MaterializedResult total = computeActual("SELECT count(*) FROM test_schema.part_multi");
            assertThat(total.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);

            MaterializedResult perRegion = computeActual(
                    "SELECT region, count(*) as cnt FROM test_schema.part_multi GROUP BY region ORDER BY region");
            assertThat(perRegion.getRowCount()).isEqualTo(3);
            // APAC=1, EU=2, US=2
            List<MaterializedRow> rows = perRegion.getMaterializedRows();
            assertThat(rows.get(0).getField(0)).isEqualTo("APAC");
            assertThat(rows.get(0).getField(1)).isEqualTo(1L);
            assertThat(rows.get(1).getField(0)).isEqualTo("EU");
            assertThat(rows.get(1).getField(1)).isEqualTo(2L);
            assertThat(rows.get(2).getField(0)).isEqualTo("US");
            assertThat(rows.get(2).getField(1)).isEqualTo(2L);
        }
        finally {
            tryDropTable("test_schema.part_multi");
        }
    }

    @Test
    public void testPartitionedWithNullPartitionValues()
    {
        computeActual("CREATE TABLE test_schema.part_nulls (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            computeActual("INSERT INTO test_schema.part_nulls VALUES " +
                    "(1, 'US', 10.0), (2, NULL, 20.0), (3, 'EU', 30.0)");

            MaterializedResult total = computeActual("SELECT count(*) FROM test_schema.part_nulls");
            assertThat(total.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);

            MaterializedResult nullRegion = computeActual(
                    "SELECT id FROM test_schema.part_nulls WHERE region IS NULL");
            assertThat(nullRegion.getRowCount()).isEqualTo(1);
            assertThat(nullRegion.getMaterializedRows().get(0).getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.part_nulls");
        }
    }

    // ==================== Helpers ====================

    private void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }
}
