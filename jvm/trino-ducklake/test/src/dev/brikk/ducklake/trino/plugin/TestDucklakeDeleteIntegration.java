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
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for DuckLake DELETE support (M6).
 * Tests row-level deletes via the merge-on-read pattern:
 * Trino writes Parquet delete files, subsequent reads filter deleted rows.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDeleteIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("delete-integration")
                .build();
    }

    @Test
    public void testDeleteAllRows()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_all (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.delete_all VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_all")).isEqualTo(3L);

            computeActual("DELETE FROM test_schema.delete_all");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_all")).isEqualTo(0L);
        }
        finally {
            tryDropTable("delete_all");
        }
    }

    @Test
    public void testDeleteWithWhereClause()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_where (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO test_schema.delete_where VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 150.0), (4, 'Diana', 300.0), (5, 'Eve', 50.0)");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_where")).isEqualTo(5L);

            // Delete rows with amount > 150
            computeActual("DELETE FROM test_schema.delete_where WHERE amount > 150.0");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_where")).isEqualTo(3L);

            // Verify correct rows remain
            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.delete_where ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo("Eve");
        }
        finally {
            tryDropTable("delete_where");
        }
    }

    @Test
    public void testDeleteSingleRow()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_single (id INTEGER, value VARCHAR)");
            computeActual("INSERT INTO test_schema.delete_single VALUES (1, 'keep'), (2, 'remove'), (3, 'keep')");

            computeActual("DELETE FROM test_schema.delete_single WHERE id = 2");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_single")).isEqualTo(2L);

            MaterializedResult result = computeActual("SELECT value FROM test_schema.delete_single ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("keep");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("keep");
        }
        finally {
            tryDropTable("delete_single");
        }
    }

    @Test
    public void testDeleteThenInsert()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_insert (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.delete_insert VALUES (1, 'Alice'), (2, 'Bob')");

            // Delete Bob
            computeActual("DELETE FROM test_schema.delete_insert WHERE id = 2");
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_insert")).isEqualTo(1L);

            // Insert new rows
            computeActual("INSERT INTO test_schema.delete_insert VALUES (3, 'Charlie'), (4, 'Diana')");
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_insert")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT name FROM test_schema.delete_insert ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("Diana");
        }
        finally {
            tryDropTable("delete_insert");
        }
    }

    @Test
    public void testDeleteWithNullValues()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_nulls (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.delete_nulls VALUES (1, 'Alice'), (2, NULL), (3, 'Charlie'), (4, NULL)");

            // Delete rows where name is null
            computeActual("DELETE FROM test_schema.delete_nulls WHERE name IS NULL");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_nulls")).isEqualTo(2L);

            MaterializedResult result = computeActual("SELECT name FROM test_schema.delete_nulls ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Charlie");
        }
        finally {
            tryDropTable("delete_nulls");
        }
    }

    @Test
    public void testDeleteFromMultipleInserts()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_multi (id INTEGER, batch VARCHAR)");
            // Insert in separate batches — creates multiple data files
            computeActual("INSERT INTO test_schema.delete_multi VALUES (1, 'batch1'), (2, 'batch1')");
            computeActual("INSERT INTO test_schema.delete_multi VALUES (3, 'batch2'), (4, 'batch2')");
            computeActual("INSERT INTO test_schema.delete_multi VALUES (5, 'batch3'), (6, 'batch3')");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_multi")).isEqualTo(6L);

            // Delete across multiple data files
            computeActual("DELETE FROM test_schema.delete_multi WHERE id IN (2, 4, 6)");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_multi")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT id FROM test_schema.delete_multi ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(5);
        }
        finally {
            tryDropTable("delete_multi");
        }
    }

    @Test
    public void testDeleteNoMatchingRows()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_nomatch (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.delete_nomatch VALUES (1, 'Alice'), (2, 'Bob')");

            // Delete with condition that matches nothing
            computeActual("DELETE FROM test_schema.delete_nomatch WHERE id > 100");

            // All rows should still be there
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_nomatch")).isEqualTo(2L);
        }
        finally {
            tryDropTable("delete_nomatch");
        }
    }

    @Test
    public void testDeleteThenAggregate()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_agg (category VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO test_schema.delete_agg VALUES " +
                    "('A', 10.0), ('A', 20.0), ('B', 30.0), ('B', 40.0), ('C', 50.0)");

            // Delete all category B
            computeActual("DELETE FROM test_schema.delete_agg WHERE category = 'B'");

            // Verify aggregation works correctly after delete
            assertThat(computeScalar("SELECT sum(amount) FROM test_schema.delete_agg")).isEqualTo(80.0);

            MaterializedResult result = computeActual(
                    "SELECT category, sum(amount) AS total FROM test_schema.delete_agg GROUP BY category ORDER BY category");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("A");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(30.0);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("C");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(50.0);
        }
        finally {
            tryDropTable("delete_agg");
        }
    }

    @Test
    public void testDeleteSnapshotTracking()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_snap (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.delete_snap VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

            // Get snapshot before delete
            long snapshotBefore = (Long) computeScalar(
                    "SELECT max(snapshot_id) FROM \"delete_snap$snapshots\"");

            computeActual("DELETE FROM test_schema.delete_snap WHERE id = 2");

            // A new snapshot should have been created
            long snapshotAfter = (Long) computeScalar(
                    "SELECT max(snapshot_id) FROM \"delete_snap$snapshots\"");
            assertThat(snapshotAfter).isGreaterThan(snapshotBefore);

            // Verify the snapshot change is tracked
            MaterializedResult changes = computeActual(
                    "SELECT changes_made FROM \"delete_snap$snapshot_changes\" WHERE snapshot_id = " + snapshotAfter);
            assertThat(changes.getRowCount()).isGreaterThanOrEqualTo(1);
            boolean hasDeleteChange = changes.getMaterializedRows().stream()
                    .anyMatch(row -> row.getField(0) != null && row.getField(0).toString().startsWith("deleted_from_table:"));
            assertThat(hasDeleteChange).isTrue();
        }
        finally {
            tryDropTable("delete_snap");
        }
    }

    @Test
    public void testDeleteWithMultipleTypes()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_types (" +
                    "id INTEGER, name VARCHAR, amount DOUBLE, active BOOLEAN, event_date DATE)");
            computeActual("INSERT INTO test_schema.delete_types VALUES " +
                    "(1, 'Alice', 100.0, true, DATE '2024-01-15'), " +
                    "(2, 'Bob', 200.0, false, DATE '2024-02-20'), " +
                    "(3, 'Charlie', 150.0, true, DATE '2024-03-10')");

            computeActual("DELETE FROM test_schema.delete_types WHERE active = false");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_types")).isEqualTo(2L);

            // Verify all column types are intact after delete
            MaterializedResult result = computeActual("SELECT * FROM test_schema.delete_types ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(100.0);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Charlie");
        }
        finally {
            tryDropTable("delete_types");
        }
    }

    @Test
    public void testMultipleDeletesOnSameTable()
    {
        try {
            computeActual("CREATE TABLE test_schema.multi_delete (id INTEGER, status VARCHAR)");
            computeActual("INSERT INTO test_schema.multi_delete VALUES " +
                    "(1, 'active'), (2, 'inactive'), (3, 'active'), (4, 'pending'), (5, 'inactive')");

            // First delete
            computeActual("DELETE FROM test_schema.multi_delete WHERE status = 'inactive'");
            assertThat(computeScalar("SELECT count(*) FROM test_schema.multi_delete")).isEqualTo(3L);

            // Second delete
            computeActual("DELETE FROM test_schema.multi_delete WHERE status = 'pending'");
            assertThat(computeScalar("SELECT count(*) FROM test_schema.multi_delete")).isEqualTo(2L);

            MaterializedResult result = computeActual("SELECT id FROM test_schema.multi_delete ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
        }
        finally {
            tryDropTable("multi_delete");
        }
    }

    @Test
    public void testDeleteFromPartitionedTable()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_part (id INTEGER, region VARCHAR, amount DOUBLE) " +
                    "WITH (partitioned_by = ARRAY['region'])");
            computeActual("INSERT INTO test_schema.delete_part VALUES " +
                    "(1, 'US', 100.0), (2, 'EU', 200.0), (3, 'US', 150.0), (4, 'EU', 250.0), (5, 'APAC', 300.0)");

            // Delete all EU rows
            computeActual("DELETE FROM test_schema.delete_part WHERE region = 'EU'");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_part")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT id, region FROM test_schema.delete_part ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("US");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("US");
            assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo("APAC");
        }
        finally {
            tryDropTable("delete_part");
        }
    }

    @Test
    public void testDeleteWithSubquery()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_subq (id INTEGER, name VARCHAR, score INTEGER)");
            computeActual("INSERT INTO test_schema.delete_subq VALUES " +
                    "(1, 'Alice', 90), (2, 'Bob', 60), (3, 'Charlie', 85), (4, 'Diana', 45), (5, 'Eve', 75)");

            // Delete rows with below-average scores
            computeActual("DELETE FROM test_schema.delete_subq WHERE score < (SELECT avg(score) FROM test_schema.delete_subq)");

            // Average of (90,60,85,45,75) = 71, so rows with score < 71 are deleted (Bob=60, Diana=45)
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_subq")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT name FROM test_schema.delete_subq ORDER BY name");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("Eve");
        }
        finally {
            tryDropTable("delete_subq");
        }
    }

    @Test
    public void testDeletePreservesOtherTableData()
    {
        try {
            computeActual("CREATE TABLE test_schema.delete_t1 (id INTEGER, name VARCHAR)");
            computeActual("CREATE TABLE test_schema.delete_t2 (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.delete_t1 VALUES (1, 'Alice'), (2, 'Bob')");
            computeActual("INSERT INTO test_schema.delete_t2 VALUES (1, 'Charlie'), (2, 'Diana')");

            // Delete from t1 only
            computeActual("DELETE FROM test_schema.delete_t1 WHERE id = 1");

            // t1 should have 1 row
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_t1")).isEqualTo(1L);
            // t2 should be unaffected
            assertThat(computeScalar("SELECT count(*) FROM test_schema.delete_t2")).isEqualTo(2L);
        }
        finally {
            tryDropTable("delete_t1");
            tryDropTable("delete_t2");
        }
    }

    // ==================== UPDATE tests ====================

    @Test
    public void testUpdateSingleColumn()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_single (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO test_schema.update_single VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");

            computeActual("UPDATE test_schema.update_single SET amount = 999.0 WHERE id = 2");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.update_single")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT id, name, amount FROM test_schema.update_single ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(100.0);
            assertThat(result.getMaterializedRows().get(1).getField(2)).isEqualTo(999.0);
            assertThat(result.getMaterializedRows().get(2).getField(2)).isEqualTo(300.0);
        }
        finally {
            tryDropTable("update_single");
        }
    }

    @Test
    public void testUpdateMultipleRows()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_multi (id INTEGER, status VARCHAR, score INTEGER)");
            computeActual("INSERT INTO test_schema.update_multi VALUES " +
                    "(1, 'pending', 50), (2, 'pending', 60), (3, 'complete', 90), (4, 'pending', 70)");

            computeActual("UPDATE test_schema.update_multi SET status = 'processed' WHERE status = 'pending'");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.update_multi")).isEqualTo(4L);

            // All former 'pending' rows should now be 'processed'
            assertThat(computeScalar("SELECT count(*) FROM test_schema.update_multi WHERE status = 'processed'")).isEqualTo(3L);
            assertThat(computeScalar("SELECT count(*) FROM test_schema.update_multi WHERE status = 'complete'")).isEqualTo(1L);

            // Scores should be unchanged
            assertThat(computeScalar("SELECT sum(score) FROM test_schema.update_multi")).isEqualTo(270L);
        }
        finally {
            tryDropTable("update_multi");
        }
    }

    @Test
    public void testUpdateWithExpression()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_expr (id INTEGER, price DOUBLE)");
            computeActual("INSERT INTO test_schema.update_expr VALUES (1, 10.0), (2, 20.0), (3, 30.0)");

            // Double the price for all items
            computeActual("UPDATE test_schema.update_expr SET price = price * 2");

            MaterializedResult result = computeActual("SELECT id, price FROM test_schema.update_expr ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(20.0);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(40.0);
            assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo(60.0);
        }
        finally {
            tryDropTable("update_expr");
        }
    }

    @Test
    public void testUpdateNoMatchingRows()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_nomatch (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.update_nomatch VALUES (1, 'Alice'), (2, 'Bob')");

            computeActual("UPDATE test_schema.update_nomatch SET name = 'Unknown' WHERE id > 100");

            // No rows should have changed
            MaterializedResult result = computeActual("SELECT name FROM test_schema.update_nomatch ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Bob");
        }
        finally {
            tryDropTable("update_nomatch");
        }
    }

    @Test
    public void testUpdateThenSelect()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_select (id INTEGER, category VARCHAR, value INTEGER)");
            computeActual("INSERT INTO test_schema.update_select VALUES " +
                    "(1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40)");

            // Update category A values
            computeActual("UPDATE test_schema.update_select SET value = value + 100 WHERE category = 'A'");

            MaterializedResult result = computeActual(
                    "SELECT category, sum(value) AS total FROM test_schema.update_select GROUP BY category ORDER BY category");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("A");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(240L); // (10+100) + (30+100) = 240
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("B");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(60L); // 20 + 40 = 60
        }
        finally {
            tryDropTable("update_select");
        }
    }

    // ==================== MERGE tests ====================

    @Test
    public void testMergeInsertOnly()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_target (id INTEGER, name VARCHAR, value INTEGER)");
            computeActual("INSERT INTO test_schema.merge_target VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

            // MERGE that only inserts (no matches)
            computeActual("MERGE INTO test_schema.merge_target t " +
                    "USING (VALUES (3, 'Charlie', 300), (4, 'Diana', 400)) AS s(id, name, value) " +
                    "ON t.id = s.id " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value)");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_target")).isEqualTo(4L);

            MaterializedResult result = computeActual("SELECT name FROM test_schema.merge_target ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(4);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(3).getField(0)).isEqualTo("Diana");
        }
        finally {
            tryDropTable("merge_target");
        }
    }

    @Test
    public void testMergeDeleteOnly()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_del (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.merge_del VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

            // MERGE that only deletes
            computeActual("MERGE INTO test_schema.merge_del t " +
                    "USING (VALUES 1, 3) AS s(id) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN DELETE");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_del")).isEqualTo(1L);
            assertThat(computeScalar("SELECT name FROM test_schema.merge_del")).isEqualTo("Bob");
        }
        finally {
            tryDropTable("merge_del");
        }
    }

    @Test
    public void testMergeUpdateOnly()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_upd (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO test_schema.merge_upd VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");

            // MERGE that updates matching rows
            computeActual("MERGE INTO test_schema.merge_upd t " +
                    "USING (VALUES (2, 999.0)) AS s(id, amount) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET amount = s.amount");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_upd")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT amount FROM test_schema.merge_upd ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(100.0);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(999.0);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(300.0);
        }
        finally {
            tryDropTable("merge_upd");
        }
    }

    @Test
    public void testMergeUpsert()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_upsert (id INTEGER, name VARCHAR, value INTEGER)");
            computeActual("INSERT INTO test_schema.merge_upsert VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

            // Classic upsert: update existing, insert new
            computeActual("MERGE INTO test_schema.merge_upsert t " +
                    "USING (VALUES (2, 'Bob_updated', 999), (3, 'Charlie', 300)) AS s(id, name, value) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value)");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_upsert")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT id, name, value FROM test_schema.merge_upsert ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(100);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Bob_updated");
            assertThat(result.getMaterializedRows().get(1).getField(2)).isEqualTo(999);
            assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(2).getField(2)).isEqualTo(300);
        }
        finally {
            tryDropTable("merge_upsert");
        }
    }

    @Test
    public void testMergeDeleteAndInsert()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_delinst (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.merge_delinst VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

            // MERGE: delete matches, insert non-matches
            computeActual("MERGE INTO test_schema.merge_delinst t " +
                    "USING (VALUES (2, 'Bob_replaced'), (4, 'Diana')) AS s(id, name) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN DELETE " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_delinst")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT name FROM test_schema.merge_delinst ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("Diana");
        }
        finally {
            tryDropTable("merge_delinst");
        }
    }

    private void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE test_schema." + tableName);
        }
        catch (RuntimeException _) {
            // ignore cleanup failures
        }
    }
}
