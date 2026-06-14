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
 * Schema evolution (ALTER TABLE ADD/RENAME/DROP COLUMN) over tables whose *data* files are a
 * non-parquet format. The parquet read path resolves renamed/added columns via the file's
 * embedded field_ids + a missing-column-as-NULL fallback; the DuckDB-engine read path
 * (.db / vortex / lance) reads files by their *physical* column names, which are the names
 * the columns had when the file was WRITTEN — so before this was fixed, any ALTER made the
 * table unreadable ("column not found"). The connector now resolves each column's name as of
 * the file's begin_snapshot (DuckDbSelectSqlBuilder): renames alias, added columns project
 * typed NULL. This base pins that the non-parquet path matches parquet's behavior.
 *
 * One subclass per format. Each asserts:
 *  - ADD COLUMN: rows written before the add read NULL for the new column; rows written
 *    after carry their value — a single SELECT spans both file generations.
 *  - RENAME COLUMN: data written under the old name reads back under the new name.
 *  - DROP COLUMN: the remaining columns still read (no dangling reference to the dropped one).
 *  - compound evolution and predicates over an added column (IS NULL matches old rows).
 */
abstract class AbstractDucklakeSchemaEvolutionFormatTest : AbstractDucklakeIntegrationTest() {
    protected abstract fun format(): String

    protected open fun assumeFormatAvailable() {}

    @BeforeEach
    fun checkFormatAvailable() {
        assumeFormatAvailable()
    }

    private fun createFormatTableAs(table: String, select: String) {
        computeActual("CREATE TABLE $table WITH (data_file_format = '${format()}') AS $select")
    }

    @Test
    fun addColumnProjectsNullForRowsWrittenBeforeAdd() {
        val table = "se_add"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)")
            computeActual("ALTER TABLE $table ADD COLUMN score INTEGER")

            // Old rows (file predates the column) must read NULL, not error.
            val rows = computeActual("SELECT id, name, score FROM $table ORDER BY id").materializedRows
            assertThat(rows).hasSize(2)
            assertThat(rows.map { it.getField(1) as String }).containsExactly("alice", "bob")
            assertThat(rows.map { it.getField(2) }).containsExactly(null, null)

            // A new INSERT writes a second file that DOES carry the column. The combined
            // SELECT spans the pre-add file (NULL) and the post-add file (value).
            computeActual("INSERT INTO $table VALUES (3, 'carol', 30)")
            val mixed = computeActual("SELECT id, score FROM $table ORDER BY id").materializedRows
            assertThat(mixed.map { it.getField(1) }).containsExactly(null, null, 30)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun renameColumnReadsDataWrittenUnderOldName() {
        val table = "se_rename"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, 'alice', 10), (2, 'bob', 20)) AS t(id, name, score)")
            computeActual("ALTER TABLE $table RENAME COLUMN name TO full_name")

            // The physical file still has the column under "name"; reads under "full_name".
            val rows = computeActual("SELECT id, full_name, score FROM $table ORDER BY id").materializedRows
            assertThat(rows.map { it.getField(1) as String }).containsExactly("alice", "bob")
            assertThat(rows.map { it.getField(2) as Int }).containsExactly(10, 20)

            // A predicate over the renamed column must push down/filter correctly.
            assertThat(computeScalar("SELECT id FROM $table WHERE full_name = 'bob'")).isEqualTo(2)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun dropColumnLeavesRemainingColumnsReadable() {
        val table = "se_drop"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, 'alice', 10), (2, 'bob', 20)) AS t(id, name, score)")
            computeActual("ALTER TABLE $table DROP COLUMN score")

            // The dropped column is no longer projected; the rest reads from the same file.
            val rows = computeActual("SELECT * FROM $table ORDER BY id").materializedRows
            assertThat(rows).allSatisfy { assertThat(it.fieldCount).isEqualTo(2) }
            assertThat(rows.map { it.getField(1) as String }).containsExactly("alice", "bob")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun predicateOverAddedColumnMatchesOldRowsAsNull() {
        val table = "se_pred"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1), (2)) AS t(id)")
            computeActual("ALTER TABLE $table ADD COLUMN tag VARCHAR")
            computeActual("INSERT INTO $table VALUES (3, 'x'), (4, 'y')")

            // Rows from the pre-add file are NULL for tag; IS NULL must find exactly them.
            assertThat(computeActual("SELECT id FROM $table WHERE tag IS NULL ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 2L)
            assertThat(computeActual("SELECT id FROM $table WHERE tag = 'x'").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(3L)
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(4L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun compoundEvolutionAddRenameAdd() {
        val table = "se_compound"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1, 'alice')) AS t(id, name)")
            computeActual("ALTER TABLE $table ADD COLUMN score INTEGER")
            computeActual("ALTER TABLE $table RENAME COLUMN name TO full_name")
            computeActual("ALTER TABLE $table ADD COLUMN active BOOLEAN")
            computeActual("INSERT INTO $table VALUES (2, 'bob', 50, true)")

            val rows = computeActual(
                    "SELECT id, full_name, score, active FROM $table ORDER BY id").materializedRows
            assertThat(rows).hasSize(2)
            // The original row: name read via rename, the two later-added columns NULL.
            assertThat(rows[0].getField(1) as String).isEqualTo("alice")
            assertThat(rows[0].getField(2)).isNull()
            assertThat(rows[0].getField(3)).isNull()
            // The row written after all evolution carries every value.
            assertThat(rows[1].getField(1) as String).isEqualTo("bob")
            assertThat(rows[1].getField(2) as Int).isEqualTo(50)
            assertThat(rows[1].getField(3) as Boolean).isTrue()
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun countAfterAddColumnStillWorks() {
        // The COUNT(*) / empty-projection path (synthetic SELECT 1) must be unaffected by the
        // schema-evolution projection rewrite.
        val table = "se_count"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1), (2), (3)) AS t(id)")
            computeActual("ALTER TABLE $table ADD COLUMN extra INTEGER")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun timeTravelReadsHistoricalSnapshotOfFormatTable() {
        val table = "se_tt"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
            val snap1 = snapshotId(table)
            computeActual("INSERT INTO $table VALUES (3, 'c'), (4, 'd')")

            // Current sees 4; the historical snapshot still sees the original 2.
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(4L)
            assertThat(computeScalar("SELECT count(*) FROM $table FOR VERSION AS OF $snap1")).isEqualTo(2L)
            assertThat(computeActual("SELECT name FROM $table FOR VERSION AS OF $snap1 ORDER BY id")
                    .materializedRows.map { it.getField(0) as String })
                    .containsExactly("a", "b")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun timeTravelResolvesColumnsPerSnapshotAcrossAddColumn() {
        // The schema-evolution interaction: SELECT * resolves the column set at the QUERY
        // snapshot, while data files keep their write-snapshot physical names. Reading a
        // snapshot taken BEFORE the column was added must not surface it; reading after must
        // show NULL for rows that predate it.
        val table = "se_tt_evo"
        try {
            createFormatTableAs(table, "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
            val beforeAdd = snapshotId(table)
            computeActual("ALTER TABLE $table ADD COLUMN score INTEGER")
            computeActual("INSERT INTO $table VALUES (3, 'c', 30)")

            // As of the pre-add snapshot: only id+name exist, 2 rows.
            val historical = computeActual("SELECT * FROM $table FOR VERSION AS OF $beforeAdd ORDER BY id").materializedRows
            assertThat(historical).hasSize(2)
            assertThat(historical).allSatisfy { assertThat(it.fieldCount).isEqualTo(2) }

            // Current: 3 columns; the pre-add rows are NULL for score, the new row carries it.
            val current = computeActual("SELECT id, score FROM $table ORDER BY id").materializedRows
            assertThat(current.map { it.getField(1) }).containsExactly(null, null, 30)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun snapshotId(table: String): Long =
            computeScalar("SELECT max(snapshot_id) FROM \"$table\$snapshots\"") as Long
}
