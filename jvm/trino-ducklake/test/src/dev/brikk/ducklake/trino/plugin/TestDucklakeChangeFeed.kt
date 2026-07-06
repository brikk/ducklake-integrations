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

import io.trino.Session
import io.trino.testing.MaterializedRow
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * The DuckLake change feed (F9): `table_insertions` / `table_deletions` / `table_changes`
 * exposed as `TABLE(ducklake.system.*)`. Each returns `snapshot_id`, `rowid` (+ `change_type`
 * for `table_changes`) followed by the table's columns as of the END snapshot, for the inclusive
 * snapshot window. Covers snapshot-id and timestamp bounds, non-parquet (duckdb) data, projection
 * pushdown, empty windows, and both Trino-written-UPDATE shapes: the default (separate
 * `delete` + `insert` under fresh row ids) and the lineage-preserving one (`write_row_lineage`
 * session property — F7 — pairs into `update_preimage`/`update_postimage`; the cross-engine
 * suite proves DuckDB reads the preserved rowids too).
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeChangeFeed : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "change-feed-integration"

    private fun snapshot(): Long = getCurrentSnapshotIdFromCatalog()

    private fun rows(sql: String): List<MaterializedRow> = computeActual(sql).materializedRows

    @Test
    fun insertionsReturnSnapshotRowidAndColumns() {
        val table = "test_schema.cf_insertions"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, val VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'Hello'), (2, 'DuckLake')")
            val s2 = snapshot()

            val result = rows(
                    "SELECT snapshot_id, rowid, id, val FROM " +
                            "TABLE(ducklake.system.table_insertions('test_schema', 'cf_insertions', $s2, $s2)) " +
                            "ORDER BY rowid")
            assertThat(result).hasSize(2)
            assertThat(result[0].fields).containsExactly(s2, 0L, 1, "Hello")
            assertThat(result[1].fields).containsExactly(s2, 1L, 2, "DuckLake")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun deletionsReturnDeletedRows() {
        val table = "test_schema.cf_deletions"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, val VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'Hello'), (2, 'DuckLake')")
            computeActual("DELETE FROM $table WHERE id = 1")
            val s3 = snapshot()

            val result = rows(
                    "SELECT snapshot_id, rowid, id, val FROM " +
                            "TABLE(ducklake.system.table_deletions('test_schema', 'cf_deletions', $s3, $s3)) " +
                            "ORDER BY rowid")
            assertThat(result).hasSize(1)
            assertThat(result[0].fields).containsExactly(s3, 0L, 1, "Hello")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun changesAcrossWindowTagsInsertAndDelete() {
        val table = "test_schema.cf_changes"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, val VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'Hello'), (2, 'DuckLake')")
            val s2 = snapshot()
            computeActual("DELETE FROM $table WHERE id = 1")
            // Explicitly OPT OUT of lineage (default is on) to pin the legacy
            // fresh-rowid shape: the UPDATE surfaces as separate delete + insert.
            val noLineage = Session.builder(session)
                    .setCatalogSessionProperty("ducklake", DucklakeSessionProperties.WRITE_ROW_LINEAGE, "false")
                    .build()
            computeActual(noLineage, "UPDATE $table SET val = concat(val, val, val) WHERE id = 2")
            val s4 = snapshot()

            val result = rows(
                    "SELECT change_type, id, val FROM " +
                            "TABLE(ducklake.system.table_changes('test_schema', 'cf_changes', $s2, $s4)) " +
                            "ORDER BY change_type, id, val")
            // 2 inserts at s2, 1 delete at s3, and the UPDATE at s4 = a delete of the old row +
            // an insert of the new row. Under the row_id_start+position rowid vocabulary the moved
            // row gets a fresh rowid, so it is NOT paired into update_preimage/postimage (the
            // pairing logic itself is unit-tested in TestChangeFeedPageSource).
            assertThat(result.map { it.getField(0) as String })
                    .containsExactly("delete", "delete", "insert", "insert", "insert")
            val inserts = result.filter { it.getField(0) == "insert" }.map { it.getField(2) as String }
            assertThat(inserts).containsExactlyInAnyOrder("Hello", "DuckLake", "DuckLakeDuckLakeDuckLake")
            val deletes = result.filter { it.getField(0) == "delete" }.map { it.getField(2) as String }
            assertThat(deletes).containsExactlyInAnyOrder("Hello", "DuckLake")
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun lineageSession(): Session = Session.builder(session)
            .setCatalogSessionProperty("ducklake", DucklakeSessionProperties.WRITE_ROW_LINEAGE, "true")
            .build()

    @Test
    fun updatePairsIntoPreAndPostImageWithWriteRowLineage() {
        // F7 lineage-preserving writes: with write_row_lineage the connector's own
        // UPDATE embeds the original rowid (_ducklake_internal_row_id, field-id
        // 2147483540) into the rewritten file, so the change feed pairs the rewrite
        // into update_preimage/update_postimage — same shape as DuckDB-written updates.
        val table = "test_schema.cf_lineage_update"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, val VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'Hello'), (2, 'DuckLake')")
            computeActual(lineageSession(), "UPDATE $table SET val = 'Updated' WHERE id = 2")
            val s3 = snapshot()

            val result = rows(
                    "SELECT change_type, rowid, id, val FROM " +
                            "TABLE(ducklake.system.table_changes('test_schema', 'cf_lineage_update', $s3, $s3)) " +
                            "ORDER BY change_type")
            assertThat(result.map { it.getField(0) as String })
                    .containsExactly("update_postimage", "update_preimage")
            val postImage = result.first { it.getField(0) == "update_postimage" }
            val preImage = result.first { it.getField(0) == "update_preimage" }
            assertThat(preImage.getField(1) as Long).isEqualTo(postImage.getField(1) as Long)
            assertThat(preImage.getField(3) as String).isEqualTo("DuckLake")
            assertThat(postImage.getField(3) as String).isEqualTo("Updated")

            // Table contents unaffected by the lineage column (it is schema-invisible).
            assertThat(rows("SELECT id, val FROM $table ORDER BY id").map { it.getField(1) as String })
                    .containsExactly("Hello", "Updated")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun mergeMixesLineagePairedUpdatesWithPlainInserts() {
        // MERGE with both WHEN MATCHED UPDATE and WHEN NOT MATCHED INSERT: updated
        // rows pair (lineage file), inserted rows stay plain inserts (separate
        // non-lineage file — the lineage column must be non-null for every row of
        // a carrying file).
        val table = "test_schema.cf_lineage_merge"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, val VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'keep'), (2, 'old')")
            computeActual(lineageSession(),
                    "MERGE INTO $table t USING (VALUES (2, 'new'), (3, 'added')) AS s(id, val) " +
                            "ON t.id = s.id " +
                            "WHEN MATCHED THEN UPDATE SET val = s.val " +
                            "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)")
            val s3 = snapshot()

            val result = rows(
                    "SELECT change_type, rowid, id, val FROM " +
                            "TABLE(ducklake.system.table_changes('test_schema', 'cf_lineage_merge', $s3, $s3)) " +
                            "ORDER BY change_type, id")
            assertThat(result.map { it.getField(0) as String })
                    .containsExactly("insert", "update_postimage", "update_preimage")
            val postImage = result.first { it.getField(0) == "update_postimage" }
            val preImage = result.first { it.getField(0) == "update_preimage" }
            assertThat(preImage.getField(1) as Long).isEqualTo(postImage.getField(1) as Long)
            assertThat(preImage.getField(3) as String).isEqualTo("old")
            assertThat(postImage.getField(3) as String).isEqualTo("new")
            val insert = result.first { it.getField(0) == "insert" }
            assertThat(insert.getField(3) as String).isEqualTo("added")

            assertThat(rows("SELECT val FROM $table ORDER BY id").map { it.getField(0) as String })
                    .containsExactly("keep", "new", "added")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun rowidStableAcrossChainedUpdates() {
        // Default-on lineage: consecutive UPDATEs of the same row keep its ORIGINAL
        // rowid forever. The second update reads its old rowid from the MERGE channel
        // (positional) and the sink translates it through the source file's embedded
        // lineage — without that translation the chain would break at hop two.
        val table = "test_schema.cf_chained_updates"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, val VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
            val original = rows("SELECT \"\$row_id\" FROM $table WHERE id = 2")[0].getField(0) as Long

            computeActual("UPDATE $table SET val = 'b1' WHERE id = 2")
            assertThat(rows("SELECT \"\$row_id\" FROM $table WHERE id = 2")[0].getField(0) as Long)
                    .`as`("first update preserves the rowid (and \$row_id resolves embedded lineage)")
                    .isEqualTo(original)

            computeActual("UPDATE $table SET val = 'b2' WHERE id = 2")
            val s = snapshot()
            assertThat(rows("SELECT \"\$row_id\" FROM $table WHERE id = 2")[0].getField(0) as Long)
                    .`as`("chained update still carries the ORIGINAL rowid")
                    .isEqualTo(original)

            // And the second hop pairs in the change feed on the original rowid.
            val result = rows(
                    "SELECT change_type, rowid, val FROM " +
                            "TABLE(ducklake.system.table_changes('test_schema', 'cf_chained_updates', $s, $s)) " +
                            "ORDER BY change_type")
            assertThat(result.map { it.getField(0) as String })
                    .containsExactly("update_postimage", "update_preimage")
            assertThat(result[0].getField(1) as Long).isEqualTo(original)
            assertThat(result[1].getField(1) as Long).isEqualTo(original)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun snapshotBoundsAreInclusiveAndScoped() {
        val table = "test_schema.cf_bounds"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1)")
            val s2 = snapshot()
            computeActual("INSERT INTO $table VALUES (2)")
            val s3 = snapshot()
            computeActual("INSERT INTO $table VALUES (3)")
            val s4 = snapshot()

            // Only the middle snapshot: inclusive on both ends, excludes s2 and s4.
            val middle = rows(
                    "SELECT id FROM TABLE(ducklake.system.table_insertions('test_schema', 'cf_bounds', $s3, $s3))")
            assertThat(middle.map { it.getField(0) as Int }).containsExactly(2)

            // Full window s2..s4 sees all three.
            val all = rows(
                    "SELECT id FROM TABLE(ducklake.system.table_insertions('test_schema', 'cf_bounds', $s2, $s4)) ORDER BY id")
            assertThat(all.map { it.getField(0) as Int }).containsExactly(1, 2, 3)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun endBoundDefaultsToCurrentSnapshot() {
        val table = "test_schema.cf_end_default"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1)")
            val s2 = snapshot()
            computeActual("INSERT INTO $table VALUES (2)")

            val result = rows(
                    "SELECT id FROM TABLE(ducklake.system.table_insertions(" +
                            "schema_name => 'test_schema', table_name => 'cf_end_default', start_snapshot => $s2)) ORDER BY id")
            assertThat(result.map { it.getField(0) as Int }).containsExactly(1, 2)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun timestampBoundsResolveToSnapshots() {
        val table = "test_schema.cf_timestamp"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1), (2)")
            val s2 = snapshot()

            // now() as the end timestamp bound; start as a snapshot id. Both rows inserted before now.
            val result = rows(
                    "SELECT id FROM TABLE(ducklake.system.table_insertions(" +
                            "schema_name => 'test_schema', table_name => 'cf_timestamp', " +
                            "start_snapshot => $s2, end_timestamp => now())) ORDER BY id")
            assertThat(result.map { it.getField(0) as Int }).containsExactly(1, 2)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun changeFeedOverNonParquetData() {
        val table = "test_schema.cf_duckdb"
        try {
            computeActual("CREATE TABLE $table WITH (data_file_format = 'duckdb') AS " +
                    "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, val)")
            val s2 = snapshot()
            computeActual("DELETE FROM $table WHERE id = 1")
            val s3 = snapshot()

            val inserted = rows(
                    "SELECT rowid, id, val FROM TABLE(ducklake.system.table_insertions('test_schema', 'cf_duckdb', $s2, $s2)) ORDER BY rowid")
            assertThat(inserted.map { it.getField(2) as String }).containsExactly("a", "b")

            val deleted = rows(
                    "SELECT rowid, id, val FROM TABLE(ducklake.system.table_deletions('test_schema', 'cf_duckdb', $s3, $s3))")
            assertThat(deleted).hasSize(1)
            assertThat(deleted[0].getField(2) as String).isEqualTo("a")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun projectionSelectsSubsetOfChangeColumns() {
        val table = "test_schema.cf_projection"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, val VARCHAR)")
            computeActual("INSERT INTO $table VALUES (1, 'x'), (2, 'y')")
            val s2 = snapshot()

            val onlyRowid = rows(
                    "SELECT rowid FROM TABLE(ducklake.system.table_insertions('test_schema', 'cf_projection', $s2, $s2)) ORDER BY rowid")
            assertThat(onlyRowid.map { it.getField(0) as Long }).containsExactly(0L, 1L)

            val count = computeScalar(
                    "SELECT count(*) FROM TABLE(ducklake.system.table_insertions('test_schema', 'cf_projection', $s2, $s2))")
            assertThat(count).isEqualTo(2L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun emptyWindowReturnsNoRows() {
        val table = "test_schema.cf_empty"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1)")
            val s2 = snapshot()

            // A window that made no changes: table_deletions over the insert-only snapshot.
            val deletions = rows(
                    "SELECT * FROM TABLE(ducklake.system.table_deletions('test_schema', 'cf_empty', $s2, $s2))")
            assertThat(deletions).isEmpty()
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun changesReadColumnsAsOfEndSnapshotSchema() {
        val table = "test_schema.cf_evolution"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1), (2)")
            val s2 = snapshot()
            // Add a column AFTER the insert; the change feed reads as of the end-snapshot schema,
            // so the earlier insert rows show the new column as NULL.
            computeActual("ALTER TABLE $table ADD COLUMN val VARCHAR")
            val sEnd = snapshot()

            val result = rows(
                    "SELECT rowid, id, val FROM " +
                            "TABLE(ducklake.system.table_insertions('test_schema', 'cf_evolution', $s2, $sEnd)) ORDER BY rowid")
            assertThat(result).hasSize(2)
            assertThat(result.map { it.getField(2) }).containsExactly(null, null)
        }
        finally {
            tryDropTable(table)
        }
    }
}
