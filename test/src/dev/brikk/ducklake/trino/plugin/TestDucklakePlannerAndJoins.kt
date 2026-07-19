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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Planner-facing read tests: EXPLAIN/EXPLAIN ANALYZE, joins (including dynamic-filter and
 * distribution-strategy paths), and SQL set operations. Asserts that the connector survives
 * the planner contract (TableScan/ScanFilter/Join/Aggregate fragments) and that joins +
 * set-ops produce the right row counts and content over the standard fixtures.
 */
open class TestDucklakePlannerAndJoins : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "integration-planner-joins"
    }

    // ==================== EXPLAIN and planning ====================

    @Test
    fun testExplainSimpleSelect() {
        val result = computeActual("EXPLAIN SELECT * FROM simple_table")
        val plan = result.materializedRows[0].getField(0).toString()
        assertThat(plan).contains("TableScan")
        assertThat(plan).containsIgnoringCase("ducklake")
    }

    @Test
    fun testExplainWithPredicate() {
        val result = computeActual(
            "EXPLAIN SELECT * FROM simple_table WHERE price > 30.0")
        val plan = result.materializedRows[0].getField(0).toString()
        assertThat(plan).containsAnyOf("TableScan", "ScanFilter")
        assertThat(plan).contains("price")
    }

    @Test
    fun testExplainDistributed() {
        val result = computeActual(
            "EXPLAIN (TYPE DISTRIBUTED) SELECT * FROM simple_table")
        val plan = result.materializedRows[0].getField(0).toString()
        assertThat(plan).contains("TableScan")
    }

    @Test
    fun testExplainDistributedAggregation() {
        val result = computeActual(
            "EXPLAIN (TYPE DISTRIBUTED) " +
                    "SELECT category, sum(amount) FROM aggregation_table GROUP BY category")
        val plan = result.materializedRows[0].getField(0).toString()
        assertThat(plan).contains("Fragment")
        assertThat(plan).contains("Aggregate")
        assertThat(plan).contains("TableScan")
    }

    @Test
    fun testExplainDistributedJoin() {
        val result = computeActual(
            "EXPLAIN (TYPE DISTRIBUTED) " +
                    "SELECT s.id, p.region FROM simple_table s JOIN partitioned_table p ON s.id = p.id")
        val plan = result.materializedRows[0].getField(0).toString()
        assertThat(plan).contains("Fragment")
        assertThat(plan).contains("Join")
        assertThat(plan).contains("TableScan")
    }

    @Test
    fun testExplainJoin() {
        val result = computeActual(
            "EXPLAIN SELECT s.name, p.region FROM simple_table s " +
                    "JOIN partitioned_table p ON s.id = p.id")
        val plan = result.materializedRows[0].getField(0).toString()
        assertThat(plan).contains("Join")
    }

    @Test
    fun testExplainAnalyze() {
        val result = computeActual(
            "EXPLAIN ANALYZE SELECT count(*) FROM simple_table")
        val plan = result.materializedRows[0].getField(0).toString()
        assertThat(plan).contains("TableScan")
    }

    @Test
    fun testExplainAnalyzeWithPartitionPredicate() {
        val result = computeActual(
            "EXPLAIN ANALYZE SELECT * FROM partitioned_table WHERE region = 'US'")
        val plan = result.materializedRows[0].getField(0).toString()
        // The plan should show the scan happened
        assertThat(plan).contains("TableScan")
    }

    // ==================== Joins (exercises dynamic filter path) ====================

    @Test
    fun testSelfJoin() {
        val result = computeActual(
            "SELECT a.id, b.name FROM simple_table a JOIN simple_table b ON a.id = b.id ORDER BY a.id")
        assertThat(result.rowCount).isEqualTo(5)
    }

    @Test
    fun testCrossTableJoin() {
        val result = computeActual(
            "SELECT s.name, p.region FROM simple_table s JOIN partitioned_table p ON s.id = p.id ORDER BY s.id")
        assertThat(result.rowCount).isEqualTo(5)
    }

    @Test
    fun testLeftJoin() {
        val result = computeActual(
            "SELECT s.id, n.metadata FROM simple_table s " +
                    "LEFT JOIN nested_table n ON s.id = n.id ORDER BY s.id")
        assertThat(result.rowCount).isEqualTo(5)
        // Only ids 1-3 have matches in nested_table
        assertThat(result.materializedRows[0].getField(1)).isNotNull() // id=1
        assertThat(result.materializedRows[3].getField(1)).isNull()    // id=4
    }

    @Test
    fun testJoinWithSubquery() {
        val result = computeActual(
            "SELECT s.name FROM simple_table s " +
                    "WHERE s.id IN (SELECT id FROM partitioned_table WHERE region = 'US')")
        assertThat(result.rowCount).isEqualTo(2)
    }

    @Test
    fun testJoinBetweenPartitionedTables() {
        assertQuery(
            "SELECT p.name, t.event_name " +
                    "FROM partitioned_table p JOIN temporal_partitioned_table t ON p.id = t.id " +
                    "ORDER BY p.id",
            "VALUES " +
                    "('Alice', 'Jan Event'), " +
                    "('Bob', 'Jan Meeting'), " +
                    "('Charlie', 'Jun Event'), " +
                    "('Diana', 'Jun Meeting'), " +
                    "('Emi', 'Next Year')")
    }

    @Test
    fun testDynamicFilterJoinSmallBuildSide() {
        // Small build-side table (nested_table, 3 rows) joined to larger probe-side
        // This exercises the dynamic filter code path
        val result = computeActual(
            "SELECT a.id, a.amount FROM aggregation_table a " +
                    "JOIN nested_table n ON a.id = n.id")
        assertThat(result.rowCount).isEqualTo(3)
    }

    @Test
    fun testPartitionedJoinDistribution() {
        assertQuery(
            "WITH SESSION join_distribution_type = 'PARTITIONED' " +
                    "SELECT count(*) FROM simple_table s JOIN partitioned_table p ON s.id = p.id",
            "VALUES 5")
    }

    @Test
    fun testBroadcastJoinDistribution() {
        assertQuery(
            "WITH SESSION join_distribution_type = 'BROADCAST' " +
                    "SELECT count(*) FROM aggregation_table a JOIN nested_table n ON a.id = n.id",
            "VALUES 3")
    }

    // ==================== Set operations ====================

    @Test
    fun testUnionAll() {
        val result = computeActual(
            "SELECT id, name FROM simple_table WHERE id <= 2 " +
                    "UNION ALL " +
                    "SELECT id, event_name FROM temporal_partitioned_table WHERE id <= 2")
        assertThat(result.rowCount).isEqualTo(4)
    }

    @Test
    fun testUnionDistinct() {
        val result = computeActual(
            "SELECT active FROM simple_table " +
                    "UNION " +
                    "SELECT col_boolean FROM wide_types_table")
        assertThat(result.rowCount).isEqualTo(2) // true and false
    }

    @Test
    fun testExceptAll() {
        val result = computeActual(
            "SELECT id FROM simple_table " +
                    "EXCEPT " +
                    "SELECT id FROM nested_table")
        assertThat(result.rowCount).isEqualTo(2) // ids 4 and 5
    }

    @Test
    fun testIntersect() {
        val result = computeActual(
            "SELECT id FROM simple_table " +
                    "INTERSECT " +
                    "SELECT id FROM nested_table")
        assertThat(result.rowCount).isEqualTo(3) // ids 1, 2, 3
    }
}
