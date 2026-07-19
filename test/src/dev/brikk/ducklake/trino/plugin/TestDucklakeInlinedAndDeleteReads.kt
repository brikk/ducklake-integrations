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
 * Special read paths that don't go through a plain Parquet scan:
 *
 *
 *  *
 * **Delete-file (merge-on-read)** — `deleted_rows_table` has DuckDB-written
 * delete files; the connector must subtract them from the active row set on read.
 *  *
 * **Inlined data** — rows kept in the `ducklake_inlined_data_*` catalog
 * tables instead of Parquet, exercised through `DucklakeInlinedValueConverter`
 * and the inlined split source. `mixed_inline_table` additionally probes the
 * case where a single table mixes inlined + Parquet rows.
 *  *
 * **Trino DELETE** — sanity check that DELETE is wired up as a supported write,
 * using a no-match WHERE clause to avoid mutating the shared fixtures.
 *
 */
class TestDucklakeInlinedAndDeleteReads
        : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "integration-inlined-delete"
    }

    // ==================== Delete file handling (merge-on-read) ====================

    @Test
    fun testDeletedRowsTableCount() {
        // Table had 6 rows, 3 were deleted by DuckDB — only 3 should survive
        val result = computeActual("SELECT count(*) FROM deleted_rows_table")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)
    }

    @Test
    fun testDeletedRowsTableOnlyKeepRows() {
        // Only rows with name='keep' should survive the delete
        val result = computeActual("SELECT id, name, value FROM deleted_rows_table ORDER BY id")
        assertThat(result.rowCount).isEqualTo(3)
        val rows = result.materializedRows
        assertThat(rows[0].getField(0)).isEqualTo(1)
        assertThat(rows[0].getField(1)).isEqualTo("keep")
        assertThat(rows[1].getField(0)).isEqualTo(3)
        assertThat(rows[2].getField(0)).isEqualTo(5)
    }

    @Test
    fun testDeletedRowsNoDeletedValues() {
        // No rows with name='delete' should be visible
        val result = computeActual("SELECT count(*) FROM deleted_rows_table WHERE name = 'delete'")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(0L)
    }

    @Test
    fun testDeletedRowsAggregation() {
        // SUM should only include surviving rows: 10 + 30 + 50 = 90
        val result = computeActual("SELECT SUM(value) FROM deleted_rows_table")
        assertThat(result.materializedRows[0].getField(0)).isEqualTo(90.0)
    }

    // ==================== Trino DELETE wired up ====================

    @Test
    fun testDeleteSupported() {
        // DELETE is now supported — verify it doesn't throw "not supported"
        // Uses a WHERE clause that matches no rows to avoid mutating shared test data
        computeActual("DELETE FROM simple_table WHERE id = -999")
        val count = computeScalar("SELECT count(*) FROM simple_table") as Long
        assertThat(count).isGreaterThan(0)
    }

    // ==================== Inlined Data Tests ====================

    @Test
    fun testInlinedTableSelect() {
        val result = computeActual("SELECT * FROM inlined_table ORDER BY id")
        assertThat(result.rowCount).isEqualTo(3)

        val rows = result.materializedRows
        assertThat(rows[0].getField(0)).isEqualTo(1)
        assertThat(rows[0].getField(1)).isEqualTo("alpha")
        assertThat(rows[0].getField(2)).isEqualTo(10.5)

        assertThat(rows[1].getField(0)).isEqualTo(2)
        assertThat(rows[1].getField(1)).isEqualTo("beta")
        assertThat(rows[1].getField(2)).isEqualTo(20.0)

        assertThat(rows[2].getField(0)).isEqualTo(3)
        assertThat(rows[2].getField(1)).isEqualTo("gamma")
        assertThat(rows[2].getField(2)).isEqualTo(30.75)
    }

    @Test
    fun testInlinedTablePredicate() {
        val result = computeActual("SELECT name, value FROM inlined_table WHERE id = 2")
        assertThat(result.rowCount).isEqualTo(1)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("beta")
        assertThat(result.materializedRows[0].getField(1)).isEqualTo(20.0)
    }

    @Test
    fun testInlinedTableCount() {
        assertQuery("SELECT COUNT(*) FROM inlined_table", "VALUES 3")
    }

    @Test
    fun testInlinedTableProjection() {
        val result = computeActual("SELECT name FROM inlined_table ORDER BY name")
        assertThat(result.rowCount).isEqualTo(3)
        assertThat(result.materializedRows[0].getField(0)).isEqualTo("alpha")
        assertThat(result.materializedRows[1].getField(0)).isEqualTo("beta")
        assertThat(result.materializedRows[2].getField(0)).isEqualTo("gamma")
    }

    @Test
    fun testInlinedTableDescribe() {
        val result = computeActual("DESCRIBE inlined_table")
        assertThat(result.rowCount).isEqualTo(3)
    }

    @Test
    fun testInlinedNullableTableSelect() {
        val result = computeActual("SELECT * FROM inlined_nullable_table ORDER BY id NULLS FIRST")
        assertThat(result.rowCount).isEqualTo(3)

        val rows = result.materializedRows
        // Row with all NULLs sorts first
        assertThat(rows[0].getField(0)).isNull()
        assertThat(rows[0].getField(1)).isNull()
        assertThat(rows[0].getField(2)).isNull()

        assertThat(rows[1].getField(0)).isEqualTo(1)
        assertThat(rows[1].getField(1)).isEqualTo("present")

        assertThat(rows[2].getField(0)).isEqualTo(3)
        assertThat(rows[2].getField(1)).isNull()
        assertThat(rows[2].getField(2)).isEqualTo(30.0)
    }

    @Test
    fun testInlinedNullableTableNullFiltering() {
        assertQuery("SELECT COUNT(*) FROM inlined_nullable_table WHERE id IS NULL", "VALUES 1")
        assertQuery("SELECT COUNT(*) FROM inlined_nullable_table WHERE id IS NOT NULL", "VALUES 2")
        assertQuery("SELECT COUNT(*) FROM inlined_nullable_table WHERE name IS NOT NULL", "VALUES 1")
    }

    @Test
    fun testInlinedNullableTableAggregation() {
        assertQuery("SELECT COUNT(*), COUNT(id), SUM(value) FROM inlined_nullable_table", "VALUES (3, 2, 40.0)")
    }

    @Test
    fun testInlinedTableJoin() {
        val result = computeActual(
                "SELECT a.name, b.name FROM inlined_table a JOIN inlined_table b ON a.id = b.id ORDER BY a.id")
        assertThat(result.rowCount).isEqualTo(3)
    }

    @Test
    fun testMixedInlineTableCount() {
        assertQuery("SELECT COUNT(*) FROM mixed_inline_table", "VALUES 7")
    }

    @Test
    fun testMixedInlineTableIncludesInlinedAndParquetRows() {
        assertQuery(
                "SELECT source, COUNT(*) FROM mixed_inline_table GROUP BY source ORDER BY source",
                "VALUES ('inlined', 2), ('parquet', 5)")
        assertQuery("SELECT COUNT(*) FROM mixed_inline_table WHERE id < 10", "VALUES 2")
        assertQuery("SELECT COUNT(*) FROM mixed_inline_table WHERE id >= 100", "VALUES 5")
    }
}
