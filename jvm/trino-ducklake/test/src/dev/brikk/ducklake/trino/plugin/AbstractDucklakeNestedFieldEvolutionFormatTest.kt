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
 * Nested struct field evolution (`ALTER TABLE ADD/DROP COLUMN s.child`) over tables whose *data*
 * files are a non-parquet format. The DuckDB-engine read path reads files by their physical
 * (write-time) struct shape, so a file written before a nested change carries a struct of a
 * different shape than the current schema. The connector reconciles each file by reshaping the
 * struct with `struct_pack` (NestedFieldReshapePlanner -> DuckDbSelectSqlBuilder): added subfields
 * read NULL, dropped ones are skipped (no positional misbind), renames/reorders map by column_id,
 * and a NULL struct stays NULL. This is the nested generalization of the top-level
 * [AbstractDucklakeSchemaEvolutionFormatTest]; see dev-docs/DESIGN-nested-field-evolution.md.
 *
 * One subclass per struct-writing non-parquet format (duckdb, vortex). Lance is excluded: its ROW
 * write path is gated upstream, so a struct can't be materialized into a lance file.
 */
abstract class AbstractDucklakeNestedFieldEvolutionFormatTest : AbstractDucklakeIntegrationTest() {
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
    fun addNestedFieldNullFillsRowsFromOlderFiles() {
        val table = "nfe_add"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, CAST(ROW(10) AS ROW(a INTEGER)))) AS t(id, s)")
            computeActual("ALTER TABLE $table ADD COLUMN s.b VARCHAR")

            // The pre-add file has s = row(a) only; s.b must read NULL (reshaped), not error.
            val rows = computeActual("SELECT id, s.a, s.b FROM $table ORDER BY id").materializedRows
            assertThat(rows).hasSize(1)
            assertThat((rows[0].getField(1) as Number).toLong()).isEqualTo(10L)
            assertThat(rows[0].getField(2)).isNull()

            // A later INSERT writes a file at the evolved shape; the SELECT spans both generations.
            computeActual("INSERT INTO $table VALUES (2, CAST(ROW(20, 'x') AS ROW(a INTEGER, b VARCHAR)))")
            val mixed = computeActual("SELECT id, s.b FROM $table ORDER BY id").materializedRows
            assertThat(mixed.map { it.getField(1) }).containsExactly(null, "x")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun dropNonTrailingNestedFieldReadsRemainingCorrectly() {
        // The case that silently misbinds without reshaping: the old file has s = row(a, b, c); after
        // DROP s.b the current shape is row(a, c). Positional binding would read b into c — struct_pack
        // selects a and c BY NAME, so c reads 12, not 11.
        val table = "nfe_drop"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, CAST(ROW(10, 11, 12) AS ROW(a INTEGER, b INTEGER, c INTEGER)))) AS t(id, s)")
            computeActual("ALTER TABLE $table DROP COLUMN s.b")

            assertThat(computeScalar("SELECT typeof(s) FROM $table LIMIT 1"))
                    .isEqualTo("row(\"a\" integer, \"c\" integer)")
            val rows = computeActual("SELECT s.a, s.c FROM $table").materializedRows
            assertThat((rows[0].getField(0) as Number).toLong()).isEqualTo(10L)
            assertThat((rows[0].getField(1) as Number).toLong()).isEqualTo(12L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun addFieldToNestedStructInOlderFile() {
        val table = "nfe_nested"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, CAST(ROW(ROW(10)) AS ROW(child ROW(a INTEGER))))) AS t(id, s)")
            computeActual("ALTER TABLE $table ADD COLUMN s.child.b INTEGER")

            val rows = computeActual("SELECT s.child.a, s.child.b FROM $table").materializedRows
            assertThat((rows[0].getField(0) as Number).toLong()).isEqualTo(10L)
            assertThat(rows[0].getField(1)).isNull()
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun nullStructInOlderFileStaysNullAfterNestedAdd() {
        // The NULL-struct guard: row 1's struct is NULL. After ADD s.b, reshaping must keep it NULL
        // (a bare struct_pack would turn it into a non-null struct of NULL fields).
        val table = "nfe_nullstruct"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, CAST(NULL AS ROW(a INTEGER))), " +
                            "(2, CAST(ROW(10) AS ROW(a INTEGER)))) AS t(id, s)")
            computeActual("ALTER TABLE $table ADD COLUMN s.b INTEGER")

            assertThat(computeScalar("SELECT count(*) FROM $table WHERE s IS NULL")).isEqualTo(1L)
            val rows = computeActual("SELECT id, s.a, s.b FROM $table ORDER BY id").materializedRows
            assertThat(rows[0].getField(1)).isNull() // row 1: struct NULL -> a NULL
            assertThat((rows[1].getField(1) as Number).toLong()).isEqualTo(10L)
            assertThat(rows[1].getField(2)).isNull() // row 2: b added -> NULL
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun nestedEvolutionComposesWithTopLevelAddColumn() {
        val table = "nfe_compound"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, CAST(ROW(10) AS ROW(a INTEGER)))) AS t(id, s)")
            computeActual("ALTER TABLE $table ADD COLUMN top INTEGER")   // top-level add (T2-A path)
            computeActual("ALTER TABLE $table ADD COLUMN s.b INTEGER")   // nested add (reshape path)

            val rows = computeActual("SELECT id, top, s.a, s.b FROM $table ORDER BY id").materializedRows
            assertThat((rows[0].getField(2) as Number).toLong()).isEqualTo(10L)
            assertThat(rows[0].getField(1)).isNull() // top-level added -> NULL
            assertThat(rows[0].getField(3)).isNull() // nested added -> NULL
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun timeTravelBeforeNestedAddReadsOldShape() {
        val table = "nfe_tt"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, CAST(ROW(10) AS ROW(a INTEGER)))) AS t(id, s)")
            val beforeAdd = snapshotId(table)
            computeActual("ALTER TABLE $table ADD COLUMN s.b INTEGER")
            computeActual("INSERT INTO $table VALUES (2, CAST(ROW(20, 30) AS ROW(a INTEGER, b INTEGER)))")

            // As of the pre-add snapshot, s is row(a) — the file reads in its own era shape (no reshape).
            assertThat(computeScalar("SELECT count(*) FROM $table FOR VERSION AS OF $beforeAdd")).isEqualTo(1L)
            assertThat((computeScalar("SELECT s.a FROM $table FOR VERSION AS OF $beforeAdd") as Number).toLong())
                    .isEqualTo(10L)
            // Current: row 1 reshaped (b NULL), row 2 native (b 30).
            assertThat(computeActual("SELECT id, s.b FROM $table ORDER BY id").materializedRows
                    .map { it.getField(1) })
                    .containsExactly(null, 30)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun deleteAfterNestedAddFiltersAcrossGenerations() {
        // DELETE drives a merge scan requesting $row_id (positional) alongside the reshaped struct —
        // pins that row-level CRUD composes with nested reshaping on non-parquet data.
        val table = "nfe_delete"
        try {
            createFormatTableAs(table,
                    "SELECT * FROM (VALUES (1, CAST(ROW(10) AS ROW(a INTEGER))), " +
                            "(2, CAST(ROW(20) AS ROW(a INTEGER))), (3, CAST(ROW(30) AS ROW(a INTEGER)))) AS t(id, s)")
            computeActual("ALTER TABLE $table ADD COLUMN s.b INTEGER")
            computeActual("INSERT INTO $table VALUES (4, CAST(ROW(40, 41) AS ROW(a INTEGER, b INTEGER)))")

            computeActual("DELETE FROM $table WHERE id IN (2, 4)")

            val rows = computeActual("SELECT id, s.a, s.b FROM $table ORDER BY id").materializedRows
            assertThat(rows.map { (it.getField(0) as Number).toLong() }).containsExactly(1L, 3L)
            assertThat(rows.map { it.getField(1) as Number? }.map { it?.toLong() }).containsExactly(10L, 30L)
            assertThat(rows.map { it.getField(2) }).containsExactly(null, null)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun snapshotId(table: String): Long =
            computeScalar("SELECT max(snapshot_id) FROM \"$table\$snapshots\"") as Long
}
