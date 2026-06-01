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
 * Special read paths that don't go through a plain Parquet scan:
 *
 * <ul>
 *   <li><b>Delete-file (merge-on-read)</b> — {@code deleted_rows_table} has DuckDB-written
 *       delete files; the connector must subtract them from the active row set on read.</li>
 *   <li><b>Inlined data</b> — rows kept in the {@code ducklake_inlined_data_*} catalog
 *       tables instead of Parquet, exercised through {@code DucklakeInlinedValueConverter}
 *       and the inlined split source. {@code mixed_inline_table} additionally probes the
 *       case where a single table mixes inlined + Parquet rows.</li>
 *   <li><b>Trino DELETE</b> — sanity check that DELETE is wired up as a supported write,
 *       using a no-match WHERE clause to avoid mutating the shared fixtures.</li>
 * </ul>
 */
public class TestDucklakeInlinedAndDeleteReads
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "integration-inlined-delete";
    }

    // ==================== Delete file handling (merge-on-read) ====================

    @Test
    public void testDeletedRowsTableCount()
    {
        // Table had 6 rows, 3 were deleted by DuckDB — only 3 should survive
        MaterializedResult result = computeActual("SELECT count(*) FROM deleted_rows_table");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
    }

    @Test
    public void testDeletedRowsTableOnlyKeepRows()
    {
        // Only rows with name='keep' should survive the delete
        MaterializedResult result = computeActual("SELECT id, name, value FROM deleted_rows_table ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(3);
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertThat(rows.get(0).getField(0)).isEqualTo(1);
        assertThat(rows.get(0).getField(1)).isEqualTo("keep");
        assertThat(rows.get(1).getField(0)).isEqualTo(3);
        assertThat(rows.get(2).getField(0)).isEqualTo(5);
    }

    @Test
    public void testDeletedRowsNoDeletedValues()
    {
        // No rows with name='delete' should be visible
        MaterializedResult result = computeActual("SELECT count(*) FROM deleted_rows_table WHERE name = 'delete'");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);
    }

    @Test
    public void testDeletedRowsAggregation()
    {
        // SUM should only include surviving rows: 10 + 30 + 50 = 90
        MaterializedResult result = computeActual("SELECT SUM(value) FROM deleted_rows_table");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(90.0);
    }

    // ==================== Trino DELETE wired up ====================

    @Test
    public void testDeleteSupported()
    {
        // DELETE is now supported — verify it doesn't throw "not supported"
        // Uses a WHERE clause that matches no rows to avoid mutating shared test data
        computeActual("DELETE FROM simple_table WHERE id = -999");
        long count = (Long) computeScalar("SELECT count(*) FROM simple_table");
        assertThat(count).isGreaterThan(0);
    }

    // ==================== Inlined Data Tests ====================

    @Test
    public void testInlinedTableSelect()
    {
        MaterializedResult result = computeActual("SELECT * FROM inlined_table ORDER BY id");
        assertThat(result.getRowCount()).isEqualTo(3);

        List<MaterializedRow> rows = result.getMaterializedRows();
        assertThat(rows.get(0).getField(0)).isEqualTo(1);
        assertThat(rows.get(0).getField(1)).isEqualTo("alpha");
        assertThat(rows.get(0).getField(2)).isEqualTo(10.5);

        assertThat(rows.get(1).getField(0)).isEqualTo(2);
        assertThat(rows.get(1).getField(1)).isEqualTo("beta");
        assertThat(rows.get(1).getField(2)).isEqualTo(20.0);

        assertThat(rows.get(2).getField(0)).isEqualTo(3);
        assertThat(rows.get(2).getField(1)).isEqualTo("gamma");
        assertThat(rows.get(2).getField(2)).isEqualTo(30.75);
    }

    @Test
    public void testInlinedTablePredicate()
    {
        MaterializedResult result = computeActual("SELECT name, value FROM inlined_table WHERE id = 2");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("beta");
        assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(20.0);
    }

    @Test
    public void testInlinedTableCount()
    {
        assertQuery("SELECT COUNT(*) FROM inlined_table", "VALUES 3");
    }

    @Test
    public void testInlinedTableProjection()
    {
        MaterializedResult result = computeActual("SELECT name FROM inlined_table ORDER BY name");
        assertThat(result.getRowCount()).isEqualTo(3);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("alpha");
        assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("beta");
        assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("gamma");
    }

    @Test
    public void testInlinedTableDescribe()
    {
        MaterializedResult result = computeActual("DESCRIBE inlined_table");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testInlinedNullableTableSelect()
    {
        MaterializedResult result = computeActual("SELECT * FROM inlined_nullable_table ORDER BY id NULLS FIRST");
        assertThat(result.getRowCount()).isEqualTo(3);

        List<MaterializedRow> rows = result.getMaterializedRows();
        // Row with all NULLs sorts first
        assertThat(rows.get(0).getField(0)).isNull();
        assertThat(rows.get(0).getField(1)).isNull();
        assertThat(rows.get(0).getField(2)).isNull();

        assertThat(rows.get(1).getField(0)).isEqualTo(1);
        assertThat(rows.get(1).getField(1)).isEqualTo("present");

        assertThat(rows.get(2).getField(0)).isEqualTo(3);
        assertThat(rows.get(2).getField(1)).isNull();
        assertThat(rows.get(2).getField(2)).isEqualTo(30.0);
    }

    @Test
    public void testInlinedNullableTableNullFiltering()
    {
        assertQuery("SELECT COUNT(*) FROM inlined_nullable_table WHERE id IS NULL", "VALUES 1");
        assertQuery("SELECT COUNT(*) FROM inlined_nullable_table WHERE id IS NOT NULL", "VALUES 2");
        assertQuery("SELECT COUNT(*) FROM inlined_nullable_table WHERE name IS NOT NULL", "VALUES 1");
    }

    @Test
    public void testInlinedNullableTableAggregation()
    {
        assertQuery("SELECT COUNT(*), COUNT(id), SUM(value) FROM inlined_nullable_table", "VALUES (3, 2, 40.0)");
    }

    @Test
    public void testInlinedTableJoin()
    {
        MaterializedResult result = computeActual(
                "SELECT a.name, b.name FROM inlined_table a JOIN inlined_table b ON a.id = b.id ORDER BY a.id");
        assertThat(result.getRowCount()).isEqualTo(3);
    }

    @Test
    public void testMixedInlineTableCount()
    {
        assertQuery("SELECT COUNT(*) FROM mixed_inline_table", "VALUES 7");
    }

    @Test
    public void testMixedInlineTableIncludesInlinedAndParquetRows()
    {
        assertQuery(
                "SELECT source, COUNT(*) FROM mixed_inline_table GROUP BY source ORDER BY source",
                "VALUES ('inlined', 2), ('parquet', 5)");
        assertQuery("SELECT COUNT(*) FROM mixed_inline_table WHERE id < 10", "VALUES 2");
        assertQuery("SELECT COUNT(*) FROM mixed_inline_table WHERE id >= 100", "VALUES 5");
    }
}
