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
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Cross-engine tests for the Day-1 virtual (hidden) columns `$path` and `$snapshot_id`.
 *
 * Covers: hidden-ness (excluded from SELECT * / DESCRIBE, queryable by name), `$path`
 * resolution (ends in .parquet, one distinct value per data file), `$snapshot_id` equals
 * the writing snapshot, inlined-data behaviour ($path NULL, $snapshot_id non-null), the
 * mixed inlined+parquet table, and use of the virtuals in WHERE / GROUP BY.
 *
 * Also guards the pre-existing sentinel path: the virtuals must compose with merge-on-read
 * delete filtering (deleted_rows_table) and a DELETE must still remove the right rows now
 * that getColumnHandles exposes virtual handles — i.e. the MERGE row-id channel (-100) is
 * untouched. See DESIGN-virtual-columns.md.
 *
 * SAME_THREAD: the suite's write tests (CTAS, DELETE) commit to the shared catalog; running
 * them concurrently would race the catalog's snapshot retry and exhaust it.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeVirtualColumns : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "integration-virtual-columns"
    }

    // ==================== Hidden-ness ====================

    @Test
    fun testSelectStarExcludesVirtuals() {
        // SELECT * returns only the 5 user columns — hidden virtuals must not leak in.
        assertThat(computeActual("SELECT * FROM simple_table").types).hasSize(5)
    }

    @Test
    fun testDescribeExcludesVirtuals() {
        val columns = computeActual("DESCRIBE simple_table").materializedRows
                .map { it.getField(0).toString() }
        assertThat(columns).containsExactly("id", "name", "price", "active", "created_date")
        assertThat(columns).doesNotContain(PATH_NAME, SNAPSHOT_NAME)
    }

    // ==================== $path ====================

    @Test
    fun testPathEndsWithParquet() {
        val path = computeScalar("SELECT $PATH FROM simple_table LIMIT 1") as String
        assertThat(path).endsWith(".parquet")
    }

    @Test
    fun testPathIsConstantPerFile() {
        // simple_table is a single data file → exactly one distinct path.
        assertThat(computeScalar("SELECT count(DISTINCT $PATH) FROM simple_table") as Long)
                .isEqualTo(1L)
    }

    @Test
    fun testPathDistinctCountMatchesFileCount() {
        // The number of distinct $path values must equal the number of data files the catalog
        // records for the table ($files metadata table) — i.e. one path per file. Cross-checking
        // against $files keeps this independent of how many files the fixture generator produced.
        val fileCount = computeScalar("SELECT count(*) FROM \"multi_file_table\$files\"") as Long
        assertThat(fileCount).isGreaterThanOrEqualTo(1L)
        assertThat(computeScalar("SELECT count(DISTINCT $PATH) FROM multi_file_table") as Long)
                .isEqualTo(fileCount)
    }

    // ==================== $snapshot_id ====================

    @Test
    fun testSnapshotIdIsTheTablesOwnSnapshot() {
        // The $snapshot_id of a freshly written table's rows must be a real, positive snapshot
        // id belonging to that table (present in its $snapshots metadata). We assert membership
        // rather than equality with max(snapshot_id): CTAS commits more than one snapshot, and
        // the data file's begin_snapshot is the one that wrote the file, not necessarily the
        // latest. This proves the begin_snapshot plumbing carries a genuine snapshot, not 0.
        val table = "vc_snapshot_probe"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES 1, 2, 3) AS t(id)")
            val snapshotId = computeScalar("SELECT DISTINCT $SNAPSHOT FROM $table") as Long
            assertThat(snapshotId).isPositive()
            val tableSnapshots = computeActual("SELECT snapshot_id FROM \"$table\$snapshots\"").materializedRows
                    .map { (it.getField(0) as Number).toLong() }
            assertThat(tableSnapshots).contains(snapshotId)
        }
        finally {
            tryDropTable(table)
        }
    }

    // ==================== Inlined data ====================

    @Test
    fun testInlinedRowsHaveNullPathAndNonNullSnapshot() {
        // inlined_table has 3 rows stored in the metadata catalog (no backing file).
        assertThat(computeScalar("SELECT count(*) FROM inlined_table WHERE $PATH IS NULL") as Long)
                .isEqualTo(3L)
        assertThat(computeScalar("SELECT count(*) FROM inlined_table WHERE $SNAPSHOT IS NOT NULL") as Long)
                .isEqualTo(3L)
    }

    @Test
    fun testMixedInlineTableSplitsPathByStorage() {
        // mixed_inline_table = 2 inlined rows (NULL $path) + 5 parquet rows (non-null $path).
        assertThat(computeScalar("SELECT count(*) FROM mixed_inline_table WHERE $PATH IS NULL") as Long)
                .isEqualTo(2L)
        assertThat(computeScalar("SELECT count(*) FROM mixed_inline_table WHERE $PATH IS NOT NULL") as Long)
                .isEqualTo(5L)
    }

    // ==================== Planner integration: WHERE / GROUP BY ====================

    @Test
    fun testVirtualColumnsUsableInWhereAndGroupBy() {
        assertThat(computeScalar("SELECT count(*) FROM simple_table WHERE $PATH IS NOT NULL") as Long)
                .isEqualTo(5L)
        // GROUP BY $path yields one group per data file.
        val fileCount = computeScalar("SELECT count(*) FROM \"multi_file_table\$files\"") as Long
        assertThat(computeActual("SELECT $PATH, count(*) FROM multi_file_table GROUP BY $PATH").materializedRows)
                .hasSize(fileCount.toInt())
    }

    // ==================== Sentinel-path regression (merge row-id channel) ====================

    @Test
    fun testVirtualsComposeWithMergeOnReadDeletes() {
        // deleted_rows_table: 3 of 6 rows removed via merge-on-read delete files. The virtual
        // columns are injected by a wrapper around the delete-filtered page source, so they
        // must reflect the LIVE rows only — exercising RowIdInjectingPageSource /
        // DeleteRowFilterTransform alongside the new injection.
        assertThat(computeScalar("SELECT count(*) FROM deleted_rows_table") as Long).isEqualTo(3L)
        assertThat(computeScalar("SELECT count(*) FROM deleted_rows_table WHERE $PATH IS NOT NULL") as Long)
                .isEqualTo(3L)
    }

    @Test
    fun testDeleteStillRemovesCorrectRowsWithVirtualsExposed() {
        // Proves the MERGE row-id sentinel (-100) channel is intact now that getColumnHandles
        // also returns virtual handles: a normal DELETE must still target the right rows.
        val table = "vc_delete_probe"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES 1, 2, 3, 4) AS t(id)")
            computeActual("DELETE FROM $table WHERE id IN (2, 4)")
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(2L)
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L)
        }
        finally {
            tryDropTable(table)
        }
    }

    companion object {
        // The SQL identifiers for the hidden columns; the leading `$` must be quoted.
        private const val PATH_NAME: String = "\$path"
        private const val SNAPSHOT_NAME: String = "\$snapshot_id"
        private const val PATH: String = "\"\$path\""
        private const val SNAPSHOT: String = "\"\$snapshot_id\""
    }
}
