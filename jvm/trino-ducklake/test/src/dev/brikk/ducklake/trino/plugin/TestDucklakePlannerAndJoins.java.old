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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Planner-facing read tests: EXPLAIN/EXPLAIN ANALYZE, joins (including dynamic-filter and
 * distribution-strategy paths), and SQL set operations. Asserts that the connector survives
 * the planner contract (TableScan/ScanFilter/Join/Aggregate fragments) and that joins +
 * set-ops produce the right row counts and content over the standard fixtures.
 */
public class TestDucklakePlannerAndJoins
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "integration-planner-joins";
    }

    // ==================== EXPLAIN and planning ====================

    @Test
    public void testExplainSimpleSelect()
    {
        MaterializedResult result = computeActual("EXPLAIN SELECT * FROM simple_table");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("TableScan");
        assertThat(plan).containsIgnoringCase("ducklake");
    }

    @Test
    public void testExplainWithPredicate()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN SELECT * FROM simple_table WHERE price > 30.0");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).containsAnyOf("TableScan", "ScanFilter");
        assertThat(plan).contains("price");
    }

    @Test
    public void testExplainDistributed()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM simple_table");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("TableScan");
    }

    @Test
    public void testExplainDistributedAggregation()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN (TYPE DISTRIBUTED) " +
                        "SELECT category, sum(amount) FROM aggregation_table GROUP BY category");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("Fragment");
        assertThat(plan).contains("Aggregate");
        assertThat(plan).contains("TableScan");
    }

    @Test
    public void testExplainDistributedJoin()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN (TYPE DISTRIBUTED) " +
                        "SELECT s.id, p.region FROM simple_table s JOIN partitioned_table p ON s.id = p.id");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("Fragment");
        assertThat(plan).contains("Join");
        assertThat(plan).contains("TableScan");
    }

    @Test
    public void testExplainJoin()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN SELECT s.name, p.region FROM simple_table s " +
                        "JOIN partitioned_table p ON s.id = p.id");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("Join");
    }

    @Test
    public void testExplainAnalyze()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN ANALYZE SELECT count(*) FROM simple_table");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        assertThat(plan).contains("TableScan");
    }

    @Test
    public void testExplainAnalyzeWithPartitionPredicate()
    {
        MaterializedResult result = computeActual(
                "EXPLAIN ANALYZE SELECT * FROM partitioned_table WHERE region = 'US'");
        String plan = result.getMaterializedRows().get(0).getField(0).toString();
        // The plan should show the scan happened
        assertThat(plan).contains("TableScan");
    }

    // ==================== Joins (exercises dynamic filter path) ====================

    @Test
    public void testSelfJoin()
    {
        MaterializedResult result = computeActual(
                "SELECT a.id, b.name FROM simple_table a JOIN simple_table b ON a.id = b.id ORDER BY a.id");
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    @Test
    public void testCrossTableJoin()
    {
        MaterializedResult result = computeActual(
                "SELECT s.name, p.region FROM simple_table s JOIN partitioned_table p ON s.id = p.id ORDER BY s.id");
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    @Test
    public void testLeftJoin()
    {
        MaterializedResult result = computeActual(
                "SELECT s.id, n.metadata FROM simple_table s " +
                        "LEFT JOIN nested_table n ON s.id = n.id ORDER BY s.id");
        assertThat(result.getRowCount()).isEqualTo(5);
        // Only ids 1-3 have matches in nested_table
        assertThat(result.getMaterializedRows().get(0).getField(1)).isNotNull(); // id=1
        assertThat(result.getMaterializedRows().get(3).getField(1)).isNull();    // id=4
    }

    @Test
    public void testJoinWithSubquery()
    {
        MaterializedResult result = computeActual(
                "SELECT s.name FROM simple_table s " +
                        "WHERE s.id IN (SELECT id FROM partitioned_table WHERE region = 'US')");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testJoinBetweenPartitionedTables()
    {
        assertQuery(
                "SELECT p.name, t.event_name " +
                        "FROM partitioned_table p JOIN temporal_partitioned_table t ON p.id = t.id " +
                        "ORDER BY p.id",
                "VALUES " +
                        "('Alice', 'Jan Event'), " +
                        "('Bob', 'Jan Meeting'), " +
                        "('Charlie', 'Jun Event'), " +
                        "('Diana', 'Jun Meeting'), " +
                        "('Emi', 'Next Year')");
    }

    @Test
    public void testDynamicFilterJoinSmallBuildSide()
    {
        // Small build-side table (nested_table, 3 rows) joined to larger probe-side
        // This exercises the dynamic filter code path
        MaterializedResult result = computeActual(
                "SELECT a.id, a.amount FROM aggregation_table a " +
                "JOIN nested_table n ON a.id = n.id");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testPartitionedJoinDistribution()
    {
        assertQuery(
                "WITH SESSION join_distribution_type = 'PARTITIONED' " +
                        "SELECT count(*) FROM simple_table s JOIN partitioned_table p ON s.id = p.id",
                "VALUES 5");
    }

    @Test
    public void testBroadcastJoinDistribution()
    {
        assertQuery(
                "WITH SESSION join_distribution_type = 'BROADCAST' " +
                        "SELECT count(*) FROM aggregation_table a JOIN nested_table n ON a.id = n.id",
                "VALUES 3");
    }

    // ==================== Set operations ====================

    @Test
    public void testUnionAll()
    {
        MaterializedResult result = computeActual(
                "SELECT id, name FROM simple_table WHERE id <= 2 " +
                        "UNION ALL " +
                        "SELECT id, event_name FROM temporal_partitioned_table WHERE id <= 2");
        assertThat(result.getRowCount()).isEqualTo(4);
    }

    @Test
    public void testUnionDistinct()
    {
        MaterializedResult result = computeActual(
                "SELECT active FROM simple_table " +
                        "UNION " +
                        "SELECT col_boolean FROM wide_types_table");
        assertThat(result.getRowCount()).isEqualTo(2); // true and false
    }

    @Test
    public void testExceptAll()
    {
        MaterializedResult result = computeActual(
                "SELECT id FROM simple_table " +
                        "EXCEPT " +
                        "SELECT id FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(2); // ids 4 and 5
    }

    @Test
    public void testIntersect()
    {
        MaterializedResult result = computeActual(
                "SELECT id FROM simple_table " +
                        "INTERSECT " +
                        "SELECT id FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3); // ids 1, 2, 3
    }
}
