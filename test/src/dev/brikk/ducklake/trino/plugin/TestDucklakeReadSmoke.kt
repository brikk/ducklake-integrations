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
 * Read-side smoke tests over the standard fixture catalog: catalog/schema/table discovery,
 * `information_schema`, simple table reads with the common predicate forms, and SQL
 * surface edge cases (CASE/CAST/CTE/subquery). Intended as a "does the plugin still answer
 * the basic questions?" canary suite — when these break, the connector is wedged.
 *
 * Heavier read paths (complex types, nullables, aggregation, partitions, joins, inlined
 * data, time travel) live in their own focused suites.
 */
class TestDucklakeReadSmoke : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "integration-read-smoke"
    }

    // ==================== Metadata queries ====================

    @Test
    fun testShowSchemas() {
        val result = computeActual("SHOW SCHEMAS")
        assertThat(result.materializedRows.stream()
                .map { row -> row.getField(0).toString() })
                .contains("test_schema", "information_schema")
    }

    @Test
    fun testShowTables() {
        val result = computeActual("SHOW TABLES FROM test_schema")
        val tableNames: List<String> = result.materializedRows.stream()
                .map { row -> row.getField(0).toString() }
                .toList()
        assertThat(tableNames)
                .contains("simple_table", "array_table", "partitioned_table",
                        "temporal_partitioned_table", "daily_partitioned_table", "nested_table",
                        "wide_types_table", "nullable_table", "empty_table",
                        "schema_evolution_table", "aggregation_table",
                        "inlined_table", "inlined_nullable_table", "mixed_inline_table")
    }

    @Test
    fun testShowCreateTable() {
        val showCreate = computeScalar("SHOW CREATE TABLE simple_table") as String
        assertThat(showCreate).contains("CREATE TABLE ducklake.test_schema.simple_table")
        assertThat(showCreate).contains("id integer")
        assertThat(showCreate).contains("name varchar")
        assertThat(showCreate).contains("price double")
        assertThat(showCreate).contains("active boolean")
        assertThat(showCreate).contains("created_date date")
    }

    @Test
    fun testDescribeSimpleTable() {
        val result = computeActual("DESCRIBE simple_table")
        assertThat(result.materializedRows.stream()
                .map { row -> row.getField(0).toString() })
                .containsExactly("id", "name", "price", "active", "created_date")
    }

    @Test
    fun testFilesMetadataTable() {
        val describe = computeActual("DESCRIBE \"simple_table\$files\"")
        assertThat(describe.materializedRows.stream()
                .map { row -> row.getField(0).toString() })
                .containsExactly(
                        "data_file_id",
                        "path",
                        "file_format",
                        "record_count",
                        "file_size_bytes",
                        "row_id_start",
                        "partition_id",
                        "delete_file_path")

        val result = computeActual("SELECT data_file_id, file_format, record_count FROM \"simple_table\$files\"")
        assertThat(result.rowCount).isGreaterThan(0)
    }

    @Test
    fun testSnapshotsAndCurrentSnapshotMetadataTables() {
        val describe = computeActual("DESCRIBE \"simple_table\$snapshots\"")
        assertThat(describe.materializedRows.stream()
                .map { row -> row.getField(0).toString() })
                .containsExactly("snapshot_id", "snapshot_time", "schema_version", "next_catalog_id", "next_file_id")

        val snapshots = computeActual("SELECT snapshot_id, snapshot_time FROM \"simple_table\$snapshots\"")
        assertThat(snapshots.rowCount).isGreaterThan(0)

        assertQuery("SELECT count(*) FROM \"simple_table\$current_snapshot\"", "VALUES 1")
        val currentSnapshotId = (computeScalar("SELECT snapshot_id FROM \"simple_table\$current_snapshot\"") as Number).toLong()
        val maxSnapshotId = (computeScalar("SELECT max(snapshot_id) FROM \"simple_table\$snapshots\"") as Number).toLong()
        assertThat(currentSnapshotId).isEqualTo(maxSnapshotId)
    }

    @Test
    fun testSnapshotChangesMetadataTable() {
        val describe = computeActual("DESCRIBE \"simple_table\$snapshot_changes\"")
        assertThat(describe.materializedRows.stream()
                .map { row -> row.getField(0).toString() })
                .containsExactly("snapshot_id", "changes_made", "author", "commit_message", "commit_extra_info")

        val result = computeActual("SELECT snapshot_id FROM \"simple_table\$snapshot_changes\"")
        assertThat(result.rowCount).isGreaterThan(0)
    }

    // ==================== information_schema ====================

    @Test
    fun testInformationSchemaTables() {
        val result = computeActual(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema = 'test_schema' ORDER BY table_name")
        val tables: List<String> = result.materializedRows.stream()
                .map { row -> row.getField(0).toString() }
                .toList()
        assertThat(tables).contains("simple_table", "aggregation_table", "empty_table")
    }

    @Test
    fun testInformationSchemaColumns() {
        val result = computeActual(
                "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'simple_table' " +
                        "ORDER BY ordinal_position")
        assertThat(result.rowCount).isEqualTo(5)

        val columnNames: List<String> = result.materializedRows.stream()
                .map { row -> row.getField(0).toString() }
                .toList()
        assertThat(columnNames).containsExactly("id", "name", "price", "active", "created_date")

        val dataTypes: List<String> = result.materializedRows.stream()
                .map { row -> row.getField(1).toString() }
                .toList()
        assertThat(dataTypes).containsExactly("integer", "varchar", "double", "boolean", "date")
    }

    @Test
    fun testInformationSchemaSchemata() {
        val result = computeActual(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
        assertThat(result.materializedRows.stream()
                .map { row -> row.getField(0).toString() })
                .contains("test_schema", "information_schema")
    }

    // ==================== Basic reads ====================

    @Test
    fun testSelectSimpleTable() {
        val result = computeActual("SELECT * FROM simple_table")
        assertThat(result.rowCount).isEqualTo(5)
    }

    @Test
    fun testSelectCount() {
        assertQuery("SELECT count(*) FROM simple_table", "VALUES 5")
    }

    @Test
    fun testSelectWithPredicate() {
        val result = computeActual("SELECT * FROM simple_table WHERE price > 40.0")
        assertThat(result.rowCount).isEqualTo(2)
    }

    @Test
    fun testSelectWithEqualityPredicate() {
        val result = computeActual("SELECT name FROM simple_table WHERE id = 3")
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("Product C")
    }

    @Test
    fun testSelectWithBetweenPredicate() {
        val result = computeActual(
                "SELECT count(*) FROM simple_table WHERE price BETWEEN 20.0 AND 50.0")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)
    }

    @Test
    fun testSelectWithInPredicate() {
        val result = computeActual(
                "SELECT count(*) FROM simple_table WHERE id IN (1, 3, 5)")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)
    }

    @Test
    fun testSelectWithLikePredicate() {
        val result = computeActual(
                "SELECT count(*) FROM simple_table WHERE name LIKE 'Product %'")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(5L)
    }

    @Test
    fun testSelectWithBooleanPredicate() {
        val result = computeActual(
                "SELECT count(*) FROM simple_table WHERE active = true")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)
    }

    @Test
    fun testSelectWithDatePredicate() {
        // Dates: 2024-01-15, 2024-02-20, 2024-03-10, 2024-01-05, 2024-02-28
        // Three are > 2024-02-01: 2024-02-20, 2024-03-10, 2024-02-28
        val result = computeActual(
                "SELECT count(*) FROM simple_table WHERE created_date > DATE '2024-02-01'")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)
    }

    @Test
    fun testSelectWithAggregation() {
        assertQuery(
                "SELECT min(price), max(price), avg(price) FROM simple_table",
                "VALUES (19.99, 59.99, 39.99)")
    }

    @Test
    fun testSelectWithGroupBy() {
        assertQuery(
                "SELECT active, count(*) FROM simple_table GROUP BY active ORDER BY active",
                "VALUES (false, 2), (true, 3)")
    }

    @Test
    fun testSelectWithOrderByAndLimit() {
        val result = computeActual(
                "SELECT id, name FROM simple_table ORDER BY id LIMIT 3")
        assertThat(result.rowCount).isEqualTo(3)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
        assertThat(result.materializedRows[2].getField(0)).isEqualTo(3)
    }

    @Test
    fun testSelectWithOffset() {
        val result = computeActual(
                "SELECT id FROM simple_table ORDER BY id OFFSET 3")
        assertThat(result.rowCount).isEqualTo(2)
    }

    @Test
    fun testSelectDistinct() {
        val result = computeActual(
                "SELECT DISTINCT active FROM simple_table ORDER BY active")
        assertThat(result.rowCount).isEqualTo(2)
    }

    // ==================== Edge cases ====================

    @Test
    fun testFalsePredicateReturnsEmpty() {
        assertQueryReturnsEmptyResult("SELECT * FROM simple_table WHERE 1 = 0")
    }

    @Test
    fun testLimitZero() {
        assertQueryReturnsEmptyResult("SELECT * FROM simple_table LIMIT 0")
    }

    @Test
    fun testSelectStar() {
        val result = computeActual("SELECT * FROM simple_table")
        assertThat(result.types).hasSize(5)
        assertThat(result.rowCount).isEqualTo(5)
    }

    @Test
    fun testSelectConstant() {
        assertQuery("SELECT 1 FROM simple_table", "VALUES 1, 1, 1, 1, 1")
    }

    @Test
    fun testCaseExpression() {
        val result = computeActual(
                "SELECT id, CASE WHEN price > 40.0 THEN 'expensive' ELSE 'cheap' END AS tier " +
                        "FROM simple_table ORDER BY id")
        assertThat(result.rowCount).isEqualTo(5)
        assertThat(result.materializedRows[0].getField(1)).isEqualTo("cheap")    // 19.99
        assertThat(result.materializedRows[4].getField(1)).isEqualTo("expensive") // 59.99
    }

    @Test
    fun testCastExpression() {
        val result = computeActual(
                "SELECT CAST(id AS VARCHAR) FROM simple_table WHERE id = 1")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("1")
    }

    @Test
    fun testSubquery() {
        val result = computeActual(
                "SELECT * FROM simple_table WHERE price = (SELECT max(price) FROM simple_table)")
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(5)
    }

    @Test
    fun testCorrelatedSubquery() {
        val result = computeActual(
                "SELECT s.id, s.name FROM simple_table s " +
                        "WHERE EXISTS (SELECT 1 FROM partitioned_table p WHERE p.id = s.id AND p.region = 'US')")
        assertThat(result.rowCount).isEqualTo(2)
    }

    @Test
    fun testWithClause() {
        val result = computeActual(
                "WITH expensive AS (SELECT * FROM simple_table WHERE price > 40.0) " +
                        "SELECT count(*) FROM expensive")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(2L)
    }
}
