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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Read-side smoke tests over the standard fixture catalog: catalog/schema/table discovery,
 * {@code information_schema}, simple table reads with the common predicate forms, and SQL
 * surface edge cases (CASE/CAST/CTE/subquery). Intended as a "does the plugin still answer
 * the basic questions?" canary suite — when these break, the connector is wedged.
 *
 * <p>Heavier read paths (complex types, nullables, aggregation, partitions, joins, inlined
 * data, time travel) live in their own focused suites.
 */
public class TestDucklakeReadSmoke
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "integration-read-smoke";
    }

    // ==================== Metadata queries ====================

    @Test
    public void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("test_schema", "information_schema");
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM test_schema");
        List<String> tableNames = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(tableNames)
                .contains("simple_table", "array_table", "partitioned_table",
                        "temporal_partitioned_table", "daily_partitioned_table", "nested_table",
                        "wide_types_table", "nullable_table", "empty_table",
                        "schema_evolution_table", "aggregation_table",
                        "inlined_table", "inlined_nullable_table", "mixed_inline_table");
    }

    @Test
    public void testShowCreateTable()
    {
        String showCreate = (String) computeScalar("SHOW CREATE TABLE simple_table");
        assertThat(showCreate).contains("CREATE TABLE ducklake.test_schema.simple_table");
        assertThat(showCreate).contains("id integer");
        assertThat(showCreate).contains("name varchar");
        assertThat(showCreate).contains("price double");
        assertThat(showCreate).contains("active boolean");
        assertThat(showCreate).contains("created_date date");
    }

    @Test
    public void testDescribeSimpleTable()
    {
        MaterializedResult result = computeActual("DESCRIBE simple_table");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("id", "name", "price", "active", "created_date");
    }

    @Test
    public void testFilesMetadataTable()
    {
        MaterializedResult describe = computeActual("DESCRIBE \"simple_table$files\"");
        assertThat(describe.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly(
                        "data_file_id",
                        "path",
                        "file_format",
                        "record_count",
                        "file_size_bytes",
                        "row_id_start",
                        "partition_id",
                        "delete_file_path");

        MaterializedResult result = computeActual("SELECT data_file_id, file_format, record_count FROM \"simple_table$files\"");
        assertThat(result.getRowCount()).isGreaterThan(0);
    }

    @Test
    public void testSnapshotsAndCurrentSnapshotMetadataTables()
    {
        MaterializedResult describe = computeActual("DESCRIBE \"simple_table$snapshots\"");
        assertThat(describe.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("snapshot_id", "snapshot_time", "schema_version", "next_catalog_id", "next_file_id");

        MaterializedResult snapshots = computeActual("SELECT snapshot_id, snapshot_time FROM \"simple_table$snapshots\"");
        assertThat(snapshots.getRowCount()).isGreaterThan(0);

        assertQuery("SELECT count(*) FROM \"simple_table$current_snapshot\"", "VALUES 1");
        long currentSnapshotId = ((Number) computeScalar("SELECT snapshot_id FROM \"simple_table$current_snapshot\"")).longValue();
        long maxSnapshotId = ((Number) computeScalar("SELECT max(snapshot_id) FROM \"simple_table$snapshots\"")).longValue();
        assertThat(currentSnapshotId).isEqualTo(maxSnapshotId);
    }

    @Test
    public void testSnapshotChangesMetadataTable()
    {
        MaterializedResult describe = computeActual("DESCRIBE \"simple_table$snapshot_changes\"");
        assertThat(describe.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("snapshot_id", "changes_made", "author", "commit_message", "commit_extra_info");

        MaterializedResult result = computeActual("SELECT snapshot_id FROM \"simple_table$snapshot_changes\"");
        assertThat(result.getRowCount()).isGreaterThan(0);
    }

    // ==================== information_schema ====================

    @Test
    public void testInformationSchemaTables()
    {
        MaterializedResult result = computeActual(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema = 'test_schema' ORDER BY table_name");
        List<String> tables = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(tables).contains("simple_table", "aggregation_table", "empty_table");
    }

    @Test
    public void testInformationSchemaColumns()
    {
        MaterializedResult result = computeActual(
                "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'simple_table' " +
                        "ORDER BY ordinal_position");
        assertThat(result.getRowCount()).isEqualTo(5);

        List<String> columnNames = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(columnNames).containsExactly("id", "name", "price", "active", "created_date");

        List<String> dataTypes = result.getMaterializedRows().stream()
                .map(row -> row.getField(1).toString())
                .toList();
        assertThat(dataTypes).containsExactly("integer", "varchar", "double", "boolean", "date");
    }

    @Test
    public void testInformationSchemaSchemata()
    {
        MaterializedResult result = computeActual(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .contains("test_schema", "information_schema");
    }

    // ==================== Basic reads ====================

    @Test
    public void testSelectSimpleTable()
    {
        MaterializedResult result = computeActual("SELECT * FROM simple_table");
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    @Test
    public void testSelectCount()
    {
        assertQuery("SELECT count(*) FROM simple_table", "VALUES 5");
    }

    @Test
    public void testSelectWithPredicate()
    {
        MaterializedResult result = computeActual("SELECT * FROM simple_table WHERE price > 40.0");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testSelectWithEqualityPredicate()
    {
        MaterializedResult result = computeActual("SELECT name FROM simple_table WHERE id = 3");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Product C");
    }

    @Test
    public void testSelectWithBetweenPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE price BETWEEN 20.0 AND 50.0");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testSelectWithInPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE id IN (1, 3, 5)");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testSelectWithLikePredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE name LIKE 'Product %'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);
    }

    @Test
    public void testSelectWithBooleanPredicate()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE active = true");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testSelectWithDatePredicate()
    {
        // Dates: 2024-01-15, 2024-02-20, 2024-03-10, 2024-01-05, 2024-02-28
        // Three are > 2024-02-01: 2024-02-20, 2024-03-10, 2024-02-28
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM simple_table WHERE created_date > DATE '2024-02-01'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testSelectWithAggregation()
    {
        assertQuery(
                "SELECT min(price), max(price), avg(price) FROM simple_table",
                "VALUES (19.99, 59.99, 39.99)");
    }

    @Test
    public void testSelectWithGroupBy()
    {
        assertQuery(
                "SELECT active, count(*) FROM simple_table GROUP BY active ORDER BY active",
                "VALUES (false, 2), (true, 3)");
    }

    @Test
    public void testSelectWithOrderByAndLimit()
    {
        MaterializedResult result = computeActual(
                "SELECT id, name FROM simple_table ORDER BY id LIMIT 3");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(3);
    }

    @Test
    public void testSelectWithOffset()
    {
        MaterializedResult result = computeActual(
                "SELECT id FROM simple_table ORDER BY id OFFSET 3");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testSelectDistinct()
    {
        MaterializedResult result = computeActual(
                "SELECT DISTINCT active FROM simple_table ORDER BY active");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    // ==================== Edge cases ====================

    @Test
    public void testFalsePredicateReturnsEmpty()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM simple_table WHERE 1 = 0");
    }

    @Test
    public void testLimitZero()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM simple_table LIMIT 0");
    }

    @Test
    public void testSelectStar()
    {
        MaterializedResult result = computeActual("SELECT * FROM simple_table");
        assertThat(result.getTypes()).hasSize(5);
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    @Test
    public void testSelectConstant()
    {
        assertQuery("SELECT 1 FROM simple_table", "VALUES 1, 1, 1, 1, 1");
    }

    @Test
    public void testCaseExpression()
    {
        MaterializedResult result = computeActual(
                "SELECT id, CASE WHEN price > 40.0 THEN 'expensive' ELSE 'cheap' END AS tier " +
                        "FROM simple_table ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(5);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("cheap");    // 19.99
        assertThat(result.getMaterializedRows().get(4).getField(1)).isEqualTo("expensive"); // 59.99
    }

    @Test
    public void testCastExpression()
    {
        MaterializedResult result = computeActual(
                "SELECT CAST(id AS VARCHAR) FROM simple_table WHERE id = 1");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("1");
    }

    @Test
    public void testSubquery()
    {
        MaterializedResult result = computeActual(
                "SELECT * FROM simple_table WHERE price = (SELECT max(price) FROM simple_table)");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(5);
    }

    @Test
    public void testCorrelatedSubquery()
    {
        MaterializedResult result = computeActual(
                "SELECT s.id, s.name FROM simple_table s " +
                        "WHERE EXISTS (SELECT 1 FROM partitioned_table p WHERE p.id = s.id AND p.region = 'US')");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testWithClause()
    {
        MaterializedResult result = computeActual(
                "WITH expensive AS (SELECT * FROM simple_table WHERE price > 40.0) " +
                        "SELECT count(*) FROM expensive");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }
}
