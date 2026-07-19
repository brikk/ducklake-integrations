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
 * Type/null/complex-type read coverage for the connector. Walks every fixture table whose
 * job is to probe a specific corner of the type system or NULL handling:
 *
 *  * `wide_types_table` — full numeric, date/timestamp, blob coverage.
 *  * `nullable_table` / `complex_nulls_table` — NULL semantics for scalars,
 *    arrays, and structs (including the full-NULL vs. null-subfield distinction).
 *  * `empty_table` — empty result set / aggregation-over-empty edge cases.
 *  * `nested_table` / `array_table` — struct/map/array dereference and
 *    projection paths.
 *  * `multi_file_table` — that predicates and NULL handling work across the
 *    file boundaries of a multi-file scan.
 *  * `schema_evolution_table` — that ALTER TABLE ADD COLUMN reads back the new
 *    column as NULL on pre-ALTER rows and the literal value on post-ALTER rows.
 *  * `aggregation_table` — GROUP BY / HAVING / window / DISTINCT / FILTER paths.
 */
class TestDucklakeTypesAndComplexReads : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "integration-types-complex"
    }

    // ==================== DESCRIBE on type-rich fixtures ====================

    @Test
    fun testDescribeWideTypesTable() {
        val result = computeActual("DESCRIBE wide_types_table")
        assertThat(
            result.materializedRows.stream()
                .map { row -> row.getField(0).toString() }
        )
            .containsExactly(
                "col_tinyint", "col_smallint", "col_integer", "col_bigint",
                "col_float", "col_double", "col_decimal", "col_boolean",
                "col_varchar", "col_date", "col_timestamp", "col_blob"
            )
    }

    @Test
    fun testDescribeNestedTable() {
        val result = computeActual("DESCRIBE nested_table")
        val columns = result.materializedRows.stream()
            .map { row -> row.getField(0).toString() }
            .toList()
        assertThat(columns).containsExactly("id", "metadata", "tags", "nested_list", "complex_struct")

        // Verify the type strings for complex columns
        val types = result.materializedRows.stream()
            .map { row -> row.getField(1).toString() }
            .toList()
        assertThat(types[1]).contains("row")   // metadata is a struct -> row type
        assertThat(types[2]).contains("map")    // tags is a map
        assertThat(types[3]).contains("array")  // nested_list is array of arrays
    }

    @Test
    fun testInformationSchemaColumnsWideTypes() {
        val result = computeActual(
            "SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_schema = 'test_schema' AND table_name = 'wide_types_table' " +
                "ORDER BY ordinal_position"
        )
        assertThat(result.rowCount).isEqualTo(12)

        val dataTypes = result.materializedRows.stream()
            .map { row -> row.getField(1).toString() }
            .toList()
        // tinyint, smallint, integer, bigint, real, double, decimal(10,2), boolean, varchar, date, timestamp(6), varbinary
        assertThat(dataTypes[0]).isEqualTo("tinyint")
        assertThat(dataTypes[1]).isEqualTo("smallint")
        assertThat(dataTypes[2]).isEqualTo("integer")
        assertThat(dataTypes[3]).isEqualTo("bigint")
        assertThat(dataTypes[4]).isEqualTo("real")
        assertThat(dataTypes[5]).isEqualTo("double")
        assertThat(dataTypes[6]).containsIgnoringCase("decimal")
        assertThat(dataTypes[7]).isEqualTo("boolean")
        assertThat(dataTypes[8]).isEqualTo("varchar")
        assertThat(dataTypes[9]).isEqualTo("date")
        assertThat(dataTypes[10]).containsIgnoringCase("timestamp")
        assertThat(dataTypes[11]).isEqualTo("varbinary")
    }

    // ==================== Wide types table ====================

    @Test
    fun testWideTypesTableRead() {
        assertQuery("SELECT count(*) FROM wide_types_table", "VALUES 3")
    }

    @Test
    fun testWideTypesIntegerTypes() {
        val result = computeActual(
            "SELECT col_tinyint, col_smallint, col_integer, col_bigint " +
                "FROM wide_types_table ORDER BY col_integer"
        )
        assertThat(result.rowCount).isEqualTo(3)
        // Negative row
        val negativeRow = result.materializedRows[0]
        assertThat((negativeRow.getField(0) as Number).toByte()).isEqualTo((-1).toByte())
        assertThat((negativeRow.getField(1) as Number).toShort()).isEqualTo((-100).toShort())
        assertThat(negativeRow.getField(2)).isEqualTo(-10000)
        assertThat(negativeRow.getField(3)).isEqualTo(-1000000000L)
    }

    @Test
    fun testWideTypesFloatingPoint() {
        val result = computeActual(
            "SELECT col_float, col_double, col_decimal " +
                "FROM wide_types_table WHERE col_integer = 10000"
        )
        assertThat(result.rowCount).isEqualTo(1)
    }

    @Test
    fun testWideTypesBooleanFilter() {
        val result = computeActual(
            "SELECT count(*) FROM wide_types_table WHERE col_boolean = true"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(2L)
    }

    @Test
    fun testWideTypesDateFilter() {
        val result = computeActual(
            "SELECT col_varchar FROM wide_types_table WHERE col_date = DATE '2024-01-01'"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("hello")
    }

    @Test
    fun testWideTypesTimestamp() {
        val result = computeActual(
            "SELECT col_timestamp FROM wide_types_table WHERE col_integer = 10000"
        )
        assertThat(result.rowCount).isEqualTo(1)
    }

    @Test
    fun testWideTypesBlob() {
        val result = computeActual(
            "SELECT col_blob FROM wide_types_table WHERE col_integer = 10000"
        )
        assertThat(result.rowCount).isEqualTo(1)
        // blob should be non-null
        assertThat(result.materializedRows[0].getField(0)).isNotNull()
    }

    // ==================== NULL handling ====================

    @Test
    fun testNullableTableRead() {
        assertQuery("SELECT count(*) FROM nullable_table", "VALUES 4")
    }

    @Test
    fun testNullableTableNullCount() {
        // id column has one NULL (row 3)
        val result = computeActual(
            "SELECT count(*) FROM nullable_table WHERE id IS NULL"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(1L)
    }

    @Test
    fun testNullableTableNullNameCount() {
        // name column has NULLs in rows 2 and 4
        val result = computeActual(
            "SELECT count(*) FROM nullable_table WHERE name IS NULL"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(2L)
    }

    @Test
    fun testNullableTableNullComplexTypes() {
        // tags array is NULL in rows 2 and 4
        val result = computeActual(
            "SELECT count(*) FROM nullable_table WHERE tags IS NULL"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(2L)
    }

    @Test
    fun testNullableTableNullFiltering() {
        // Rows 1 and 3 have non-null name, price, active, created_date, and tags.
        val result = computeActual(
            "SELECT id FROM nullable_table " +
                "WHERE name IS NOT NULL AND price IS NOT NULL AND active IS NOT NULL " +
                "AND created_date IS NOT NULL AND tags IS NOT NULL"
        )
        assertThat(result.rowCount).isEqualTo(2)
    }

    @Test
    fun testNullableTableCoalesce() {
        val result = computeActual(
            "SELECT COALESCE(name, 'UNKNOWN') FROM nullable_table ORDER BY id NULLS LAST"
        )
        val names = result.materializedRows.stream()
            .map { row -> row.getField(0).toString() }
            .toList()
        assertThat(names).contains("Present", "UNKNOWN", "NoId")
    }

    // ==================== Empty table ====================

    @Test
    fun testEmptyTableRead() {
        assertQueryReturnsEmptyResult("SELECT * FROM empty_table")
    }

    @Test
    fun testEmptyTableCount() {
        assertQuery("SELECT count(*) FROM empty_table", "VALUES 0")
    }

    @Test
    fun testEmptyTableAggregation() {
        val result = computeActual(
            "SELECT min(id), max(id), avg(value) FROM empty_table"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isNull()
        assertThat(result.materializedRows[0].getField(1)).isNull()
        assertThat(result.materializedRows[0].getField(2)).isNull()
    }

    // ==================== Complex types — dereference ====================

    @Test
    fun testStructFieldDereference() {
        val result = computeActual(
            "SELECT metadata.key, metadata.value FROM nested_table WHERE id = 1"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("color")
        assertThat(result.materializedRows[0].getField(1)).isEqualTo("red")
    }

    @Test
    fun testMapSubscript() {
        val result = computeActual(
            "SELECT tags['priority'] FROM nested_table WHERE id = 1"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
    }

    @Test
    fun testMapElementAtMissingKey() {
        // element_at returns NULL for missing keys (unlike subscript which throws)
        val result = computeActual(
            "SELECT element_at(tags, 'nonexistent') FROM nested_table WHERE id = 1"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isNull()
    }

    @Test
    fun testArraySubscript() {
        // nested_list for id=1 is [[1,2],[3,4]], so nested_list[1] is [1,2]
        val result = computeActual(
            "SELECT nested_list[1] FROM nested_table WHERE id = 1"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isNotNull()
    }

    @Test
    fun testComplexStructFieldDereference() {
        val result = computeActual(
            "SELECT complex_struct.name FROM nested_table WHERE id = 1"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("Alice")
    }

    @Test
    fun testComplexStructNestedArrayDereference() {
        // complex_struct.scores for id=1 is [90, 85, 92]
        val result = computeActual(
            "SELECT cardinality(complex_struct.scores) FROM nested_table WHERE id = 1"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat((result.materializedRows[0].getField(0) as Number).toLong()).isEqualTo(3L)
    }

    @Test
    fun testComplexStructNestedMapDereference() {
        val result = computeActual(
            "SELECT complex_struct.attrs['dept'] FROM nested_table WHERE id = 2"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("sales")
    }

    @Test
    fun testSelectStructColumn() {
        val result = computeActual("SELECT metadata FROM nested_table")
        assertThat(result.rowCount).isEqualTo(3)
    }

    @Test
    fun testSelectMapColumn() {
        val result = computeActual("SELECT tags FROM nested_table")
        assertThat(result.rowCount).isEqualTo(3)
    }

    @Test
    fun testSelectNestedArrayColumn() {
        val result = computeActual("SELECT nested_list FROM nested_table")
        assertThat(result.rowCount).isEqualTo(3)
    }

    @Test
    fun testSelectComplexStructColumn() {
        val result = computeActual("SELECT complex_struct FROM nested_table")
        assertThat(result.rowCount).isEqualTo(3)
    }

    @Test
    fun testArrayTableCardinality() {
        val result = computeActual(
            "SELECT id, cardinality(tags) FROM array_table ORDER BY id"
        )
        assertThat(result.rowCount).isEqualTo(5)
        // id=1 has 3 tags, id=4 has 1 tag
        assertThat((result.materializedRows[0].getField(1) as Number).toLong()).isEqualTo(3L)
        assertThat((result.materializedRows[3].getField(1) as Number).toLong()).isEqualTo(1L)
    }

    @Test
    fun testArrayTableUnnest() {
        val result = computeActual(
            "SELECT a.id, t.tag FROM array_table a CROSS JOIN UNNEST(a.tags) AS t(tag) " +
                "WHERE a.id = 1 ORDER BY t.tag"
        )
        assertThat(result.rowCount).isEqualTo(3)
    }

    // ==================== Complex NULL patterns (structs, arrays) ====================

    @Test
    fun testFullNullStructRow() {
        // Row 2 has pair=NULL (entire struct is null) and row 4 has pair=NULL
        val result = computeActual(
            "SELECT id FROM complex_nulls_table WHERE pair IS NULL ORDER BY id"
        )
        assertThat(result.rowCount).isEqualTo(2)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(2)
        assertThat(result.materializedRows[1].getField(0)).isEqualTo(4)
    }

    @Test
    fun testStructWithNullSubfield() {
        // Row 3 has pair.a=NULL, pair.b=40
        val result = computeActual(
            "SELECT pair.b FROM complex_nulls_table WHERE id = 3"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(40)
    }

    @Test
    fun testStructNullVsNullSubfield() {
        // Distinguish full-NULL struct from struct with NULL subfields
        // Row 2: pair IS NULL (true), Row 3: pair IS NOT NULL but pair.a IS NULL
        val result = computeActual(
            "SELECT id, pair IS NULL AS is_null_struct FROM complex_nulls_table WHERE id IN (2, 3) ORDER BY id"
        )
        assertThat(result.rowCount).isEqualTo(2)
        assertThat(result.materializedRows[0].getField(1)).isEqualTo(true)   // id=2: full null
        assertThat(result.materializedRows[1].getField(1)).isEqualTo(false)  // id=3: has values
    }

    @Test
    fun testArrayWithNullElement() {
        // Row 2 has items=[NULL] — a non-null array containing a null element
        val result = computeActual(
            "SELECT cardinality(items) FROM complex_nulls_table WHERE id = 2"
        )
        assertThat((result.materializedRows[0].getField(0) as Number).toLong()).isEqualTo(1L)
    }

    @Test
    fun testFullNullArrayRow() {
        // Row 3 has items=NULL (entire array is null)
        val result = computeActual(
            "SELECT items IS NULL FROM complex_nulls_table WHERE id = 3"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(true)
    }

    @Test
    fun testArrayNullVsNullElement() {
        // Row 2: items IS NOT NULL (array exists but has null element)
        // Row 3: items IS NULL (entire array is null)
        val result = computeActual(
            "SELECT id, items IS NULL AS is_null_array FROM complex_nulls_table WHERE id IN (2, 3) ORDER BY id"
        )
        assertThat(result.rowCount).isEqualTo(2)
        assertThat(result.materializedRows[0].getField(1)).isEqualTo(false)  // [NULL] is not null
        assertThat(result.materializedRows[1].getField(1)).isEqualTo(true)   // NULL array
    }

    @Test
    fun testVariableLengthArrays() {
        // Arrays of different lengths: [1,2,3], [NULL], NULL, [4,5], []
        val result = computeActual(
            "SELECT id, cardinality(items) FROM complex_nulls_table WHERE items IS NOT NULL ORDER BY id"
        )
        assertThat(result.rowCount).isEqualTo(4)
        assertThat((result.materializedRows[0].getField(1) as Number).toLong()).isEqualTo(3L)  // id=1: [1,2,3]
        assertThat((result.materializedRows[1].getField(1) as Number).toLong()).isEqualTo(1L)  // id=2: [NULL]
        assertThat((result.materializedRows[2].getField(1) as Number).toLong()).isEqualTo(2L)  // id=4: [4,5]
        assertThat((result.materializedRows[3].getField(1) as Number).toLong()).isEqualTo(0L)  // id=5: []
    }

    @Test
    fun testEmptyArray() {
        // Row 5 has items=[] — an empty array (not null)
        val result = computeActual(
            "SELECT items IS NULL, cardinality(items) FROM complex_nulls_table WHERE id = 5"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(false)
        assertThat((result.materializedRows[0].getField(1) as Number).toLong()).isEqualTo(0L)
    }

    // ==================== Multi-file scan ====================

    @Test
    fun testMultiFileScanCount() {
        // 5 rows across 3 separate Parquet files
        val result = computeActual("SELECT count(*) FROM multi_file_table")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(5L)
    }

    @Test
    fun testMultiFileScanAllRows() {
        val result = computeActual(
            "SELECT id, value FROM multi_file_table ORDER BY id NULLS LAST"
        )
        assertThat(result.rowCount).isEqualTo(5)
        assertThat(result.materializedRows[0].getField(1)).isEqualTo("file1_row1")
        assertThat(result.materializedRows[1].getField(1)).isEqualTo("file1_row2")
        assertThat(result.materializedRows[2].getField(1)).isEqualTo("file2_row1")
        assertThat(result.materializedRows[3].getField(1)).isEqualTo("file3_row1")
        assertThat(result.materializedRows[4].getField(0)).isNull()  // NULL id row
    }

    @Test
    fun testMultiFileScanWithPredicate() {
        // Predicate should work across file boundaries
        val result = computeActual(
            "SELECT count(*) FROM multi_file_table WHERE id > 2"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(2L)
    }

    @Test
    fun testMultiFileScanNullsAcrossFiles() {
        // The NULL row is in file 2 — tests that NULL handling works across file boundaries
        val result = computeActual(
            "SELECT count(*) FROM multi_file_table WHERE id IS NULL"
        )
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(1L)
    }

    // ==================== Schema evolution ====================

    @Test
    fun testSchemaEvolutionTableRead() {
        assertQuery("SELECT count(*) FROM schema_evolution_table", "VALUES 4")
    }

    @Test
    fun testSchemaEvolutionTableColumns() {
        val result = computeActual("DESCRIBE schema_evolution_table")
        assertThat(
            result.materializedRows.stream()
                .map { row -> row.getField(0).toString() }
        )
            .containsExactly("id", "original_col", "added_col")
    }

    @Test
    fun testSchemaEvolutionOldRowsHaveNullForNewColumn() {
        // Rows 1 and 2 were inserted before added_col was added
        val result = computeActual(
            "SELECT id, added_col FROM schema_evolution_table WHERE id <= 2 ORDER BY id"
        )
        assertThat(result.rowCount).isEqualTo(2)
        assertThat(result.materializedRows[0].getField(1)).isNull()
        assertThat(result.materializedRows[1].getField(1)).isNull()
    }

    @Test
    fun testSchemaEvolutionNewRowsHaveValues() {
        val result = computeActual(
            "SELECT id, added_col FROM schema_evolution_table WHERE id > 2 ORDER BY id"
        )
        assertThat(result.rowCount).isEqualTo(2)
        assertThat(result.materializedRows[0].getField(1)).isEqualTo(300)
        assertThat(result.materializedRows[1].getField(1)).isEqualTo(400)
    }

    @Test
    fun testSchemaEvolutionSelectAll() {
        val result = computeActual(
            "SELECT * FROM schema_evolution_table ORDER BY id"
        )
        assertThat(result.rowCount).isEqualTo(4)
        // Verify all columns are present including new one
        assertThat(result.types).hasSize(3)
    }

    // ==================== Aggregation table ====================

    @Test
    fun testAggregationTableCount() {
        assertQuery("SELECT count(*) FROM aggregation_table", "VALUES 30")
    }

    @Test
    fun testAggregationTableGroupByCategory() {
        assertQuery(
            "SELECT category, count(*), sum(quantity), sum(amount), avg(amount) " +
                "FROM aggregation_table GROUP BY category ORDER BY category",
            "VALUES ('A', 10, 725, 1476.0, 147.6), " +
                "('B', 10, 775, 1579.0, 157.9), " +
                "('C', 10, 825, 1682.0, 168.2)"
        )
    }

    @Test
    fun testAggregationTableHaving() {
        assertQuery(
            "SELECT category, sum(quantity) FROM aggregation_table " +
                "GROUP BY category HAVING sum(quantity) > 750 ORDER BY category",
            "VALUES ('B', 775), ('C', 825)"
        )
    }

    @Test
    fun testAggregationTableDistinctAndFilteredAggregations() {
        assertQuery(
            "SELECT count(*), count(DISTINCT category), " +
                "sum(amount) FILTER (WHERE category = 'A'), " +
                "sum(amount) FILTER (WHERE category = 'B'), " +
                "sum(amount) FILTER (WHERE category = 'C') " +
                "FROM aggregation_table",
            "VALUES (30, 3, 1476.0, 1579.0, 1682.0)"
        )
    }

    @Test
    fun testNullableTableAggregationsIgnoreNulls() {
        assertQuery(
            "SELECT count(*), count(name), count(price), count_if(active), " +
                "sum(price), avg(price), min(price), max(price) FROM nullable_table",
            "VALUES (4, 2, 2, 2, 30.0, 15.0, 10.0, 20.0)"
        )
    }

    @Test
    fun testAggregationTableMinMax() {
        val result = computeActual(
            "SELECT min(id), max(id), min(amount), max(amount) FROM aggregation_table"
        )
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(1)).isEqualTo(30)
    }

    @Test
    fun testAggregationTableWindowFunction() {
        val result = computeActual(
            "SELECT id, category, " +
                "row_number() OVER (PARTITION BY category ORDER BY id) AS rn " +
                "FROM aggregation_table WHERE id <= 9 ORDER BY category, id"
        )
        assertThat(result.rowCount).isEqualTo(9)
    }
}
