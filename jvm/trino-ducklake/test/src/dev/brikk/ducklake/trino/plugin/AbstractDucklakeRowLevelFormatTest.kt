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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Row-level DELETE/UPDATE/MERGE over tables whose *data* files are a non-parquet format.
 * Every delete test in [TestDucklakeDelete] / [TestDucklakeUpdate] / [TestDucklakeMerge]
 * runs against parquet data; this base runs the same merge-on-read primitive against
 * `duckdb` (.db), `vortex`, and `lance` data files (one subclass per format), proving:
 *
 *  - position-delete files written by Trino (always parquet) apply correctly to data files
 *    read through the DuckDB engine (`ATTACH` / `read_vortex` / `__lance_scan`),
 *  - file positions are STABLE across scans: the global rowIds captured by the merge
 *    SELECT phase must equal the cumulative-offset positions a later scan computes, or
 *    deletes silently hit the wrong rows ([deleteSpreadRowsFromLargerTableTwice] exists
 *    to catch exactly that corruption),
 *  - rewritten rows (the UPDATE/MERGE insert side) inherit the table's data file format
 *    via the latest-format rule rather than falling back to parquet,
 *  - a second DELETE touching the same data file supersedes the prior delete file while
 *    preserving its positions (the B3a union path in [DucklakeMergeSink.writeDeleteFile]).
 */
abstract class AbstractDucklakeRowLevelFormatTest : AbstractDucklakeIntegrationTest() {
    /** The `data_file_format` table-property value under test. */
    protected abstract fun format(): String

    /** Skips when the format's DuckDB extension is unavailable (offline / platform). */
    protected open fun assumeFormatAvailable() {}

    @BeforeEach
    fun checkFormatAvailable() {
        assumeFormatAvailable()
    }

    private fun createFormatTableAs(table: String, select: String) {
        computeActual("CREATE TABLE $table WITH (data_file_format = '${format()}') AS $select")
    }

    /** Every data file of [table] must carry the format under test in the catalog. */
    private fun assertDataFilesAllInFormat(table: String) {
        assertThat(computeActual("SELECT DISTINCT file_format FROM \"$table\$files\"").materializedRows
                .map { it.getField(0) as String })
                .containsExactly(format())
    }

    @Test
    fun deleteWithWhereLeavesExactSurvivors() {
        val table = "rl_delete_where"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES " +
                    "(1, 'alice', 10.0e0), (2, 'bob', 20.0e0), (3, 'carol', 30.0e0), " +
                    "(4, 'dave', 40.0e0), (5, 'eve', 50.0e0)) AS t(id, name, amount)")
            assertDataFilesAllInFormat(table)

            computeActual("DELETE FROM $table WHERE amount > 25.0e0")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(2L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alice", "bob")

            // Second scan = fresh page source; must see the same survivors. Catches file
            // positions that differ between the merge-time scan and later reads.
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alice", "bob")

            // The data file keeps its format; the tombstones live in a parquet delete file
            // referenced from the same $files row.
            val files = computeActual("SELECT file_format, delete_file_path FROM \"$table\$files\"")
            assertThat(files.rowCount).isEqualTo(1)
            assertThat(files.materializedRows[0].getField(0)).isEqualTo(format())
            assertThat(files.materializedRows[0].getField(1)).isNotNull()
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun deleteAllRows() {
        val table = "rl_delete_all"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")

            computeActual("DELETE FROM $table")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(0L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun deleteAcrossMultipleDataFiles() {
        val table = "rl_delete_multi_file"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1, 'batch1'), (2, 'batch1')) AS t(id, batch)")
            computeActual("INSERT INTO $table VALUES (3, 'batch2'), (4, 'batch2')")
            computeActual("INSERT INTO $table VALUES (5, 'batch3'), (6, 'batch3')")
            // INSERT after format CTAS must inherit the format (latest-format rule).
            assertDataFilesAllInFormat(table)

            computeActual("DELETE FROM $table WHERE id IN (2, 4, 6)")

            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L, 5L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun deleteSpreadRowsFromLargerTableTwice() {
        val table = "rl_delete_spread"
        try {
            // 2000 rows in one data file: enough to cross several Arrow batches, so unstable
            // scan order between merge-time rowId capture and read-time position filtering
            // would visibly corrupt the survivor set.
            createFormatTableAs(table,
                    "SELECT x AS id, CAST(x AS VARCHAR) AS val FROM UNNEST(SEQUENCE(1, 2000)) AS t(x)")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(2000L)

            computeActual("DELETE FROM $table WHERE id % 7 = 0")

            // 285 multiples of 7 in [1,2000], summing 285285.
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(1715L)
            assertThat(computeScalar("SELECT sum(id) FROM $table")).isEqualTo(2001000L - 285285L)
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE id % 7 = 0")).isEqualTo(0L)
            // Whole-row integrity: the id<->val pairing must survive the position filter.
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE CAST(id AS VARCHAR) <> val")).isEqualTo(0L)

            // Second delete on the same data file exercises the supersede path: the new
            // delete file must union the prior positions or the first delete resurrects (B3a).
            computeActual("DELETE FROM $table WHERE id % 11 = 0")

            // Newly deleted: multiples of 11 that aren't multiples of 7 — 181 - 25 = 156 rows
            // summing 181181 - 25025 = 156156.
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(1559L)
            assertThat(computeScalar("SELECT sum(id) FROM $table")).isEqualTo(1715715L - 156156L)
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE id % 7 = 0 OR id % 11 = 0")).isEqualTo(0L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun updateRewritesRowsAndKeepsFormat() {
        val table = "rl_update"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'carol', 30)) AS t(id, name, score)")

            computeActual("UPDATE $table SET score = score + 100 WHERE id <= 2")

            val rows = computeActual("SELECT id, name, score FROM $table ORDER BY id").materializedRows
            assertThat(rows).hasSize(3)
            assertThat(rows.map { it.getField(1) as String }).containsExactly("alice", "bob", "carol")
            assertThat(rows.map { it.getField(2) as Int }).containsExactly(110, 120, 30)

            // The rewritten rows land in a NEW data file that must inherit the table's
            // format (latest-format rule), not fall back to parquet.
            assertDataFilesAllInFormat(table)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun mergeUpsertAcrossFormatDataFiles() {
        val table = "rl_merge"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, 'alice', 100), (2, 'bob', 200)) AS t(id, name, value)")

            computeActual("MERGE INTO $table t " +
                    "USING (VALUES (2, 'bob-updated', 250), (3, 'carol', 300)) AS s(id, name, value) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value)")

            val rows = computeActual("SELECT id, name, value FROM $table ORDER BY id").materializedRows
            assertThat(rows).hasSize(3)
            assertThat(rows.map { it.getField(1) as String }).containsExactly("alice", "bob-updated", "carol")
            assertThat(rows.map { it.getField(2) as Int }).containsExactly(100, 250, 300)

            assertDataFilesAllInFormat(table)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun deleteThenInsertReadsBothGenerations() {
        val table = "rl_delete_insert"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)")

            computeActual("DELETE FROM $table WHERE id = 2")
            computeActual("INSERT INTO $table VALUES (3, 'carol'), (4, 'dave')")

            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alice", "carol", "dave")
            assertDataFilesAllInFormat(table)
        }
        finally {
            tryDropTable(table)
        }
    }
}
