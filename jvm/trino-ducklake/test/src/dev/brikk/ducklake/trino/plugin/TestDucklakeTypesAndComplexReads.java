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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Type/null/complex-type read coverage for the connector. Walks every fixture table whose
 * job is to probe a specific corner of the type system or NULL handling:
 *
 * <ul>
 *   <li>{@code wide_types_table} — full numeric, date/timestamp, blob coverage.</li>
 *   <li>{@code nullable_table} / {@code complex_nulls_table} — NULL semantics for scalars,
 *       arrays, and structs (including the full-NULL vs. null-subfield distinction).</li>
 *   <li>{@code empty_table} — empty result set / aggregation-over-empty edge cases.</li>
 *   <li>{@code nested_table} / {@code array_table} — struct/map/array dereference and
 *       projection paths.</li>
 *   <li>{@code multi_file_table} — that predicates and NULL handling work across the
 *       file boundaries of a multi-file scan.</li>
 *   <li>{@code schema_evolution_table} — that ALTER TABLE ADD COLUMN reads back the new
 *       column as NULL on pre-ALTER rows and the literal value on post-ALTER rows.</li>
 *   <li>{@code aggregation_table} — GROUP BY / HAVING / window / DISTINCT / FILTER paths.</li>
 * </ul>
 */
public class TestDucklakeTypesAndComplexReads
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "integration-types-complex";
    }

    // ==================== DESCRIBE on type-rich fixtures ====================

    @Test
    public void testDescribeWideTypesTable()
    {
        MaterializedResult result = computeActual("DESCRIBE wide_types_table");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("col_tinyint", "col_smallint", "col_integer", "col_bigint",
                        "col_float", "col_double", "col_decimal", "col_boolean",
                        "col_varchar", "col_date", "col_timestamp", "col_blob");
    }

    @Test
    public void testDescribeNestedTable()
    {
        MaterializedResult result = computeActual("DESCRIBE nested_table");
        List<String> columns = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(columns).containsExactly("id", "metadata", "tags", "nested_list", "complex_struct");

        // Verify the type strings for complex columns
        List<String> types = result.getMaterializedRows().stream()
                .map(row -> row.getField(1).toString())
                .toList();
        assertThat(types.get(1)).contains("row");   // metadata is a struct -> row type
        assertThat(types.get(2)).contains("map");    // tags is a map
        assertThat(types.get(3)).contains("array");  // nested_list is array of arrays
    }

    @Test
    public void testInformationSchemaColumnsWideTypes()
    {
        MaterializedResult result = computeActual(
                "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_schema = 'test_schema' AND table_name = 'wide_types_table' " +
                        "ORDER BY ordinal_position");
        assertThat(result.getRowCount()).isEqualTo(12);

        List<String> dataTypes = result.getMaterializedRows().stream()
                .map(row -> row.getField(1).toString())
                .toList();
        // tinyint, smallint, integer, bigint, real, double, decimal(10,2), boolean, varchar, date, timestamp(6), varbinary
        assertThat(dataTypes.get(0)).isEqualTo("tinyint");
        assertThat(dataTypes.get(1)).isEqualTo("smallint");
        assertThat(dataTypes.get(2)).isEqualTo("integer");
        assertThat(dataTypes.get(3)).isEqualTo("bigint");
        assertThat(dataTypes.get(4)).isEqualTo("real");
        assertThat(dataTypes.get(5)).isEqualTo("double");
        assertThat(dataTypes.get(6)).containsIgnoringCase("decimal");
        assertThat(dataTypes.get(7)).isEqualTo("boolean");
        assertThat(dataTypes.get(8)).isEqualTo("varchar");
        assertThat(dataTypes.get(9)).isEqualTo("date");
        assertThat(dataTypes.get(10)).containsIgnoringCase("timestamp");
        assertThat(dataTypes.get(11)).isEqualTo("varbinary");
    }

    // ==================== Wide types table ====================

    @Test
    public void testWideTypesTableRead()
    {
        assertQuery("SELECT count(*) FROM wide_types_table", "VALUES 3");
    }

    @Test
    public void testWideTypesIntegerTypes()
    {
        MaterializedResult result = computeActual(
                "SELECT col_tinyint, col_smallint, col_integer, col_bigint " +
                        "FROM wide_types_table ORDER BY col_integer");
        assertThat(result.getRowCount()).isEqualTo(3);
        // Negative row
        MaterializedRow negativeRow = result.getMaterializedRows().get(0);
        assertThat(((Number) negativeRow.getField(0)).byteValue()).isEqualTo((byte) -1);
        assertThat(((Number) negativeRow.getField(1)).shortValue()).isEqualTo((short) -100);
        assertThat(negativeRow.getField(2)).isEqualTo(-10000);
        assertThat(negativeRow.getField(3)).isEqualTo(-1000000000L);
    }

    @Test
    public void testWideTypesFloatingPoint()
    {
        MaterializedResult result = computeActual(
                "SELECT col_float, col_double, col_decimal " +
                        "FROM wide_types_table WHERE col_integer = 10000");
        assertThat(result.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testWideTypesBooleanFilter()
    {
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM wide_types_table WHERE col_boolean = true");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testWideTypesDateFilter()
    {
        MaterializedResult result = computeActual(
                "SELECT col_varchar FROM wide_types_table WHERE col_date = DATE '2024-01-01'");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("hello");
    }

    @Test
    public void testWideTypesTimestamp()
    {
        MaterializedResult result = computeActual(
                "SELECT col_timestamp FROM wide_types_table WHERE col_integer = 10000");
        assertThat(result.getRowCount()).isEqualTo(1);
    }

    @Test
    public void testWideTypesBlob()
    {
        MaterializedResult result = computeActual(
                "SELECT col_blob FROM wide_types_table WHERE col_integer = 10000");
        assertThat(result.getRowCount()).isEqualTo(1);
        // blob should be non-null
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNotNull();
    }

    // ==================== NULL handling ====================

    @Test
    public void testNullableTableRead()
    {
        assertQuery("SELECT count(*) FROM nullable_table", "VALUES 4");
    }

    @Test
    public void testNullableTableNullCount()
    {
        // id column has one NULL (row 3)
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM nullable_table WHERE id IS NULL");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);
    }

    @Test
    public void testNullableTableNullNameCount()
    {
        // name column has NULLs in rows 2 and 4
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM nullable_table WHERE name IS NULL");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testNullableTableNullComplexTypes()
    {
        // tags array is NULL in rows 2 and 4
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM nullable_table WHERE tags IS NULL");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testNullableTableNullFiltering()
    {
        // Rows 1 and 3 have non-null name, price, active, created_date, and tags.
        MaterializedResult result = computeActual(
                "SELECT id FROM nullable_table " +
                        "WHERE name IS NOT NULL AND price IS NOT NULL AND active IS NOT NULL " +
                        "AND created_date IS NOT NULL AND tags IS NOT NULL");
        assertThat(result.getRowCount()).isEqualTo(2);
    }

    @Test
    public void testNullableTableCoalesce()
    {
        MaterializedResult result = computeActual(
                "SELECT COALESCE(name, 'UNKNOWN') FROM nullable_table ORDER BY id NULLS LAST");
        List<String> names = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(names).contains("Present", "UNKNOWN", "NoId");
    }

    // ==================== Empty table ====================

    @Test
    public void testEmptyTableRead()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM empty_table");
    }

    @Test
    public void testEmptyTableCount()
    {
        assertQuery("SELECT count(*) FROM empty_table", "VALUES 0");
    }

    @Test
    public void testEmptyTableAggregation()
    {
        MaterializedResult result = computeActual(
                "SELECT min(id), max(id), avg(value) FROM empty_table");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNull();
        assertThat(result.getMaterializedRows().get(0).getField(1)).isNull();
        assertThat(result.getMaterializedRows().get(0).getField(2)).isNull();
    }

    // ==================== Complex types — dereference ====================

    @Test
    public void testStructFieldDereference()
    {
        MaterializedResult result = computeActual(
                "SELECT metadata.key, metadata.value FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("color");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("red");
    }

    @Test
    public void testMapSubscript()
    {
        MaterializedResult result = computeActual(
                "SELECT tags['priority'] FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }

    @Test
    public void testMapElementAtMissingKey()
    {
        // element_at returns NULL for missing keys (unlike subscript which throws)
        MaterializedResult result = computeActual(
                "SELECT element_at(tags, 'nonexistent') FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNull();
    }

    @Test
    public void testArraySubscript()
    {
        // nested_list for id=1 is [[1,2],[3,4]], so nested_list[1] is [1,2]
        MaterializedResult result = computeActual(
                "SELECT nested_list[1] FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isNotNull();
    }

    @Test
    public void testComplexStructFieldDereference()
    {
        MaterializedResult result = computeActual(
                "SELECT complex_struct.name FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
    }

    @Test
    public void testComplexStructNestedArrayDereference()
    {
        // complex_struct.scores for id=1 is [90, 85, 92]
        MaterializedResult result = computeActual(
                "SELECT cardinality(complex_struct.scores) FROM nested_table WHERE id = 1");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(((Number) result.getMaterializedRows().get(0).getField(0)).longValue()).isEqualTo(3L);
    }

    @Test
    public void testComplexStructNestedMapDereference()
    {
        MaterializedResult result = computeActual(
                "SELECT complex_struct.attrs['dept'] FROM nested_table WHERE id = 2");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("sales");
    }

    @Test
    public void testSelectStructColumn()
    {
        MaterializedResult result = computeActual("SELECT metadata FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testSelectMapColumn()
    {
        MaterializedResult result = computeActual("SELECT tags FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testSelectNestedArrayColumn()
    {
        MaterializedResult result = computeActual("SELECT nested_list FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testSelectComplexStructColumn()
    {
        MaterializedResult result = computeActual("SELECT complex_struct FROM nested_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testArrayTableCardinality()
    {
        MaterializedResult result = computeActual(
                "SELECT id, cardinality(tags) FROM array_table ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(5);
        // id=1 has 3 tags, id=4 has 1 tag
        assertThat(((Number) result.getMaterializedRows().get(0).getField(1)).longValue()).isEqualTo(3L);
        assertThat(((Number) result.getMaterializedRows().get(3).getField(1)).longValue()).isEqualTo(1L);
    }

    @Test
    public void testArrayTableUnnest()
    {
        MaterializedResult result = computeActual(
                "SELECT a.id, t.tag FROM array_table a CROSS JOIN UNNEST(a.tags) AS t(tag) " +
                        "WHERE a.id = 1 ORDER BY t.tag");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    // ==================== Complex NULL patterns (structs, arrays) ====================

    @Test
    public void testFullNullStructRow()
    {
        // Row 2 has pair=NULL (entire struct is null) and row 4 has pair=NULL
        MaterializedResult result = computeActual(
                "SELECT id FROM complex_nulls_table WHERE pair IS NULL ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(4);
    }

    @Test
    public void testStructWithNullSubfield()
    {
        // Row 3 has pair.a=NULL, pair.b=40
        MaterializedResult result = computeActual(
                "SELECT pair.b FROM complex_nulls_table WHERE id = 3");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(40);
    }

    @Test
    public void testStructNullVsNullSubfield()
    {
        // Distinguish full-NULL struct from struct with NULL subfields
        // Row 2: pair IS NULL (true), Row 3: pair IS NOT NULL but pair.a IS NULL
        MaterializedResult result = computeActual(
                "SELECT id, pair IS NULL AS is_null_struct FROM complex_nulls_table WHERE id IN (2, 3) ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(true);   // id=2: full null
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(false);  // id=3: has values
    }

    @Test
    public void testArrayWithNullElement()
    {
        // Row 2 has items=[NULL] — a non-null array containing a null element
        MaterializedResult result = computeActual(
                "SELECT cardinality(items) FROM complex_nulls_table WHERE id = 2");
        assertThat(((Number) result.getMaterializedRows().get(0).getField(0)).longValue()).isEqualTo(1L);
    }

    @Test
    public void testFullNullArrayRow()
    {
        // Row 3 has items=NULL (entire array is null)
        MaterializedResult result = computeActual(
                "SELECT items IS NULL FROM complex_nulls_table WHERE id = 3");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(true);
    }

    @Test
    public void testArrayNullVsNullElement()
    {
        // Row 2: items IS NOT NULL (array exists but has null element)
        // Row 3: items IS NULL (entire array is null)
        MaterializedResult result = computeActual(
                "SELECT id, items IS NULL AS is_null_array FROM complex_nulls_table WHERE id IN (2, 3) ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(false);  // [NULL] is not null
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(true);   // NULL array
    }

    @Test
    public void testVariableLengthArrays()
    {
        // Arrays of different lengths: [1,2,3], [NULL], NULL, [4,5], []
        MaterializedResult result = computeActual(
                "SELECT id, cardinality(items) FROM complex_nulls_table WHERE items IS NOT NULL ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(4);
        assertThat(((Number) result.getMaterializedRows().get(0).getField(1)).longValue()).isEqualTo(3L);  // id=1: [1,2,3]
        assertThat(((Number) result.getMaterializedRows().get(1).getField(1)).longValue()).isEqualTo(1L);  // id=2: [NULL]
        assertThat(((Number) result.getMaterializedRows().get(2).getField(1)).longValue()).isEqualTo(2L);  // id=4: [4,5]
        assertThat(((Number) result.getMaterializedRows().get(3).getField(1)).longValue()).isEqualTo(0L);  // id=5: []
    }

    @Test
    public void testEmptyArray()
    {
        // Row 5 has items=[] — an empty array (not null)
        MaterializedResult result = computeActual(
                "SELECT items IS NULL, cardinality(items) FROM complex_nulls_table WHERE id = 5");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(false);
        assertThat(((Number) result.getMaterializedRows().get(0).getField(1)).longValue()).isEqualTo(0L);
    }

    // ==================== Multi-file scan ====================

    @Test
    public void testMultiFileScanCount()
    {
        // 5 rows across 3 separate Parquet files
        MaterializedResult result = computeActual("SELECT count(*) FROM multi_file_table");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);
    }

    @Test
    public void testMultiFileScanAllRows()
    {
        MaterializedResult result = computeActual(
                "SELECT id, value FROM multi_file_table ORDER BY id NULLS LAST");
        assertThat(result.getRowCount()).isEqualTo(5);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("file1_row1");
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("file1_row2");
        assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo("file2_row1");
        assertThat(result.getMaterializedRows().get(3).getField(1)).isEqualTo("file3_row1");
        assertThat(result.getMaterializedRows().get(4).getField(0)).isNull();  // NULL id row
    }

    @Test
    public void testMultiFileScanWithPredicate()
    {
        // Predicate should work across file boundaries
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM multi_file_table WHERE id > 2");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);
    }

    @Test
    public void testMultiFileScanNullsAcrossFiles()
    {
        // The NULL row is in file 2 — tests that NULL handling works across file boundaries
        MaterializedResult result = computeActual(
                "SELECT count(*) FROM multi_file_table WHERE id IS NULL");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);
    }

    // ==================== Schema evolution ====================

    @Test
    public void testSchemaEvolutionTableRead()
    {
        assertQuery("SELECT count(*) FROM schema_evolution_table", "VALUES 4");
    }

    @Test
    public void testSchemaEvolutionTableColumns()
    {
        MaterializedResult result = computeActual("DESCRIBE schema_evolution_table");
        assertThat(result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString()))
                .containsExactly("id", "original_col", "added_col");
    }

    @Test
    public void testSchemaEvolutionOldRowsHaveNullForNewColumn()
    {
        // Rows 1 and 2 were inserted before added_col was added
        MaterializedResult result = computeActual(
                "SELECT id, added_col FROM schema_evolution_table WHERE id <= 2 ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isNull();
        assertThat(result.getMaterializedRows().get(1).getField(1)).isNull();
    }

    @Test
    public void testSchemaEvolutionNewRowsHaveValues()
    {
        MaterializedResult result = computeActual(
                "SELECT id, added_col FROM schema_evolution_table WHERE id > 2 ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(2);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(300);
        assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(400);
    }

    @Test
    public void testSchemaEvolutionSelectAll()
    {
        MaterializedResult result = computeActual(
                "SELECT * FROM schema_evolution_table ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(4);
        // Verify all columns are present including new one
        assertThat(result.getTypes()).hasSize(3);
    }

    // ==================== Aggregation table ====================

    @Test
    public void testAggregationTableCount()
    {
        assertQuery("SELECT count(*) FROM aggregation_table", "VALUES 30");
    }

    @Test
    public void testAggregationTableGroupByCategory()
    {
        assertQuery(
                "SELECT category, count(*), sum(quantity), sum(amount), avg(amount) " +
                        "FROM aggregation_table GROUP BY category ORDER BY category",
                "VALUES ('A', 10, 725, 1476.0, 147.6), " +
                        "('B', 10, 775, 1579.0, 157.9), " +
                        "('C', 10, 825, 1682.0, 168.2)");
    }

    @Test
    public void testAggregationTableHaving()
    {
        assertQuery(
                "SELECT category, sum(quantity) FROM aggregation_table " +
                        "GROUP BY category HAVING sum(quantity) > 750 ORDER BY category",
                "VALUES ('B', 775), ('C', 825)");
    }

    @Test
    public void testAggregationTableDistinctAndFilteredAggregations()
    {
        assertQuery(
                "SELECT count(*), count(DISTINCT category), " +
                        "sum(amount) FILTER (WHERE category = 'A'), " +
                        "sum(amount) FILTER (WHERE category = 'B'), " +
                        "sum(amount) FILTER (WHERE category = 'C') " +
                        "FROM aggregation_table",
                "VALUES (30, 3, 1476.0, 1579.0, 1682.0)");
    }

    @Test
    public void testNullableTableAggregationsIgnoreNulls()
    {
        assertQuery(
                "SELECT count(*), count(name), count(price), count_if(active), " +
                        "sum(price), avg(price), min(price), max(price) FROM nullable_table",
                "VALUES (4, 2, 2, 2, 30.0, 15.0, 10.0, 20.0)");
    }

    @Test
    public void testAggregationTableMinMax()
    {
        MaterializedResult result = computeActual(
                "SELECT min(id), max(id), min(amount), max(amount) FROM aggregation_table");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(30);
    }

    @Test
    public void testAggregationTableWindowFunction()
    {
        MaterializedResult result = computeActual(
                "SELECT id, category, " +
                        "row_number() OVER (PARTITION BY category ORDER BY id) AS rn " +
                        "FROM aggregation_table WHERE id <= 9 ORDER BY category, id");
        assertThat(result.getRowCount()).isEqualTo(9);
    }
}
