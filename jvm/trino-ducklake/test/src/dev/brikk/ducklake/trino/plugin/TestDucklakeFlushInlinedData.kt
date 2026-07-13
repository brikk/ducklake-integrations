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
import java.sql.DriverManager

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

    /** DuckLake global `rowid`s for a table, in id order, read through DuckDB. */
    private fun duckdbRowIds(fqTable: String): List<Long> {
        createDuckdbConnection().use { duck ->
            duck.createStatement().use { stmt ->
                stmt.executeQuery("SELECT rowid FROM ducklake_db.$fqTable ORDER BY rowid").use { rs ->
                    val ids = mutableListOf<Long>()
                    while (rs.next()) ids.add(rs.getLong(1))
                    return ids
                }
            }
        }
    }

    /** (record_count, next_row_id) from ducklake_table_stats for the active table. */
    private fun tableStats(bare: String): Pair<Long, Long> {
        val catalog = getIsolatedCatalog()
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
            conn.prepareStatement(
                    "SELECT ts.record_count, ts.next_row_id FROM ducklake_table_stats ts "
                            + "JOIN ducklake_table t ON t.table_id = ts.table_id "
                            + "WHERE t.table_name = ? AND t.end_snapshot IS NULL").use { ps ->
                ps.setString(1, bare)
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "no table_stats for $bare" }
                    return rs.getLong(1) to rs.getLong(2)
                }
            }
        }
    }

    /**
     * Regression (terra P1): a flush is an identity-preserving storage MOVE. The materialized file
     * must keep each row's original `rowid` (carried via the embedded `_ducklake_internal_row_id`)
     * and must NOT inflate `record_count` / `next_row_id` (the rows were already counted and their
     * ids already allocated when inlined). Previously the flush re-inserted the file: new rowids +
     * doubled record_count + advanced allocator.
     */
    @Test
    fun flushPreservesRowIdentityAndDoesNotDoubleCount() {
        val table = "test_schema.flush_identity"
        val bare = "flush_identity"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            writeInlinedRows(bare, "(1, 'a')", "(2, 'b')")
            assertRowsStayedInlined(bare, 2L)

            val beforeRowIds = duckdbRowIds(table)
            assertThat(beforeRowIds).hasSize(2)
            val (beforeCount, beforeNextRowId) = tableStats(bare)
            assertThat(beforeCount).`as`("inlined rows are counted pre-flush").isEqualTo(2L)

            computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => '$bare')")
            assertRowsWrittenToParquet(bare)

            // Identity preserved: same rowids for the same logical rows.
            assertThat(duckdbRowIds(table)).`as`("rowids unchanged by the flush move").isEqualTo(beforeRowIds)
            // Count-neutral: a move must not inflate the live count or the allocator.
            val (afterCount, afterNextRowId) = tableStats(bare)
            assertThat(afterCount).`as`("record_count unchanged (no double-count)").isEqualTo(beforeCount)
            assertThat(afterNextRowId).`as`("next_row_id unchanged (no allocator gap)").isEqualTo(beforeNextRowId)

            // Values still read correctly on both engines.
            assertThat(computeActual("SELECT id, name FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() to (it.getField(1) as String) })
                    .containsExactly(1L to "a", 2L to "b")
        }
        finally {
            tryDropTable(table)
        }
    }

    /** DuckLake global rowid of the row with the given id, read through DuckDB. */
    private fun duckdbRowIdOf(fqTable: String, id: Int): Long {
        createDuckdbConnection().use { duck ->
            duck.createStatement().use { stmt ->
                stmt.executeQuery("SELECT rowid FROM ducklake_db.$fqTable WHERE id = $id").use { rs ->
                    check(rs.next()) { "no row id=$id in $fqTable" }
                    return rs.getLong(1)
                }
            }
        }
    }

    /**
     * Regression (terra P1 follow-up): because the flush now preserves each row's rowid via the
     * embedded lineage column, a lineage-preserving UPDATE across the flush pairs into
     * update_preimage/update_postimage ON THE ORIGINAL rowid — the change feed sees a stable
     * identity, not a delete+re-insert under a fresh id. Before the fix the flushed row got a new
     * rowid, so the update would carry that new id instead of the pre-flush one.
     */
    @Test
    fun updateAfterFlushPairsOnThePreservedRowId() {
        val table = "test_schema.flush_cf"
        val bare = "flush_cf"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, val VARCHAR)")
            writeInlinedRows(bare, "(1, 'a')", "(2, 'b')")
            val originalRowId = duckdbRowIdOf(table, 2)

            computeActual("CALL ducklake.system.flush_inlined_data(schema_name => 'test_schema', table_name => '$bare')")
            assertThat(duckdbRowIdOf(table, 2)).`as`("rowid preserved by the flush").isEqualTo(originalRowId)

            // write_row_lineage is ON by default, so the UPDATE pairs rather than delete+insert.
            computeActual("UPDATE $table SET val = 'b2' WHERE id = 2")
            val s = computeScalar("SELECT max(snapshot_id) FROM \"flush_cf\$snapshots\"") as Long

            val changes = computeActual(
                    "SELECT change_type, rowid, id, val FROM " +
                            "TABLE(ducklake.system.table_changes('test_schema', '$bare', $s, $s)) " +
                            "ORDER BY change_type").materializedRows
            assertThat(changes.map { it.getField(0) as String })
                    .`as`("UPDATE across the flush pairs (not delete+insert)")
                    .containsExactly("update_postimage", "update_preimage")
            val pre = changes.first { it.getField(0) == "update_preimage" }
            val post = changes.first { it.getField(0) == "update_postimage" }
            // Both images carry the ORIGINAL pre-flush rowid — lineage survived the flush.
            assertThat(pre.getField(1) as Long).`as`("pre-image on preserved rowid").isEqualTo(originalRowId)
            assertThat(post.getField(1) as Long).`as`("post-image on preserved rowid").isEqualTo(originalRowId)
            assertThat(pre.getField(3) as String).isEqualTo("b")
            assertThat(post.getField(3) as String).isEqualTo("b2")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun catalogWideFlushMovesEveryInlinedTable() {
        val a = "flush_cw_a"
        val b = "flush_cw_b"
        try {
            computeActual("CREATE TABLE test_schema.$a (id INTEGER, name VARCHAR)")
            computeActual("CREATE TABLE test_schema.$b (id INTEGER, name VARCHAR)")
            writeInlinedRows(a, "(1, 'a')", "(2, 'b')")
            writeInlinedRows(b, "(3, 'c')")
            assertRowsStayedInlined(a, 2L)
            assertRowsStayedInlined(b, 1L)

            // No schema_name / table_name => flush every table with inlined rows across the catalog.
            computeActual("CALL ducklake.system.flush_inlined_data()")

            assertRowsWrittenToParquet(a)
            assertRowsWrittenToParquet(b)
            assertThat(computeScalar("SELECT count(*) FROM test_schema.$a")).isEqualTo(2L)
            assertThat(computeScalar("SELECT count(*) FROM test_schema.$b")).isEqualTo(1L)
        }
        finally {
            tryDropTable("test_schema.$a")
            tryDropTable("test_schema.$b")
        }
    }

    @Test
    fun flushTableNameWithoutSchemaIsRejected() {
        assertThatThrownBy {
            computeActual("CALL ducklake.system.flush_inlined_data(table_name => 'orders')")
        }.hasMessageContaining("table_name requires schema_name")
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
