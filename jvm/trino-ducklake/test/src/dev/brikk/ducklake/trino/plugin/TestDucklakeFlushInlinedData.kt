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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * `CALL system.flush_inlined_data(schema, table)` — materializes a table's inlined rows (written
 * cross-engine by DuckDB under `data_inlining_row_limit`) into a Parquet data file and clears the
 * inlined rows, atomically. Trino never inlines, so the only way to get inlined rows is a DuckDB
 * write — hence this is a cross-engine suite.
 *
 * The headline: flush UNBLOCKS DELETE/UPDATE/MERGE, which are gated while a table has inlined rows.
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeFlushInlinedData : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "flush-inlined"

    private fun writeInlinedRows(bare: String, vararg valuesTuples: String) {
        createDuckdbConnection().use { duck ->
            duck.createStatement().use { stmt ->
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 1000, " +
                        "schema => 'test_schema', table_name => '$bare')")
                stmt.execute("INSERT INTO ducklake_db.test_schema.$bare VALUES ${valuesTuples.joinToString(", ")}")
            }
        }
    }

    @Test
    fun flushMovesInlinedRowsToAFileAndUnblocksDelete() {
        val table = "test_schema.flush_basic"
        val bare = "flush_basic"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            writeInlinedRows(bare, "(1, 'a')", "(2, 'b')", "(3, 'c')")
            assertRowsStayedInlined(bare, 3L)

            // DELETE is gated while rows are inlined.
            assertThatThrownBy { computeActual("DELETE FROM $table WHERE id = 2") }
                    .hasMessageContaining("inlined rows")

            computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => '$bare')")

            // Rows are now in a Parquet file; count + values preserved.
            assertRowsWrittenToParquet(bare)
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("a", "b", "c")

            // DELETE now works (the gate is lifted), and reads stay correct.
            computeActual("DELETE FROM $table WHERE id = 2")
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun duckdbReadsFlushedRows() {
        val table = "test_schema.flush_xengine"
        val bare = "flush_xengine"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            writeInlinedRows(bare, "(1, 'x')", "(2, 'y')")

            computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => '$bare')")

            // The flushed Parquet file must read back correctly in DuckDB too.
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT count(*) FROM ducklake_db.$table").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getLong(1)).isEqualTo(2L)
                    }
                }
            }
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun flushPreservesValuesAcrossTypes() {
        val table = "test_schema.flush_types"
        val bare = "flush_types"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, amount DOUBLE, active BOOLEAN, note VARCHAR)")
            writeInlinedRows(bare,
                    "(1, 1.5, true, 'one')",
                    "(2, 2.5, false, NULL)")

            computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => '$bare')")
            assertRowsWrittenToParquet(bare)

            val rows = computeActual("SELECT id, amount, active, note FROM $table ORDER BY id").materializedRows
            assertThat(rows).hasSize(2)
            assertThat(rows[0].getField(1)).isEqualTo(1.5)
            assertThat(rows[0].getField(2)).isEqualTo(true)
            assertThat(rows[0].getField(3)).isEqualTo("one")
            assertThat(rows[1].getField(3)).isNull()
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun flushWithNoInlinedRowsIsNoop() {
        val table = "test_schema.flush_noop"
        try {
            // A Trino-written table never has inlined rows; flush should be a clean no-op.
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a')) AS t(id, name)")
            computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => 'flush_noop')")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(1L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun flushRejectsPartitionedTable() {
        val table = "test_schema.flush_part"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR) " +
                    "WITH (partitioned_by = ARRAY['region'])")
            assertThatThrownBy {
                computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => 'flush_part')")
            }.hasMessageContaining("does not support partitioned tables")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun flushRejectsMissingTable() {
        assertThatThrownBy {
            computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => 'no_such_table')")
        }.hasMessageContaining("Table not found")
    }
}
