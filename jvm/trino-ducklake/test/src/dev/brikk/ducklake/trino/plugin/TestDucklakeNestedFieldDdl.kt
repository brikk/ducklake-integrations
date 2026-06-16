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
 * Nested struct field DDL — `ALTER TABLE ... ADD COLUMN parent.child <type>` / `DROP COLUMN
 * parent.child`. Step 1: fully supported on parquet (the default format — old files self-heal via
 * the parquet reader's name/field-id struct binding + NULL-fill), and cleanly GATED on tables with
 * non-parquet data files until the per-file struct-reshaping read path lands
 * (dev-docs/DESIGN-nested-field-evolution.md).
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeNestedFieldDdl : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "nested-field-ddl"

    @Test
    fun addFieldNullFillsRowsFromOlderFiles() {
        val table = "test_schema.nf_add"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(a INTEGER))")
            // File 1 — written with s = row(a) only.
            computeActual("INSERT INTO $table VALUES (1, CAST(ROW(10) AS ROW(a INTEGER)))")

            computeActual("ALTER TABLE $table ADD COLUMN s.b VARCHAR")
            assertThat(computeScalar("SELECT typeof(s) FROM $table LIMIT 1"))
                    .isEqualTo("row(\"a\" integer, \"b\" varchar)")

            // File 2 — written with the evolved s = row(a, b).
            computeActual("INSERT INTO $table VALUES (2, CAST(ROW(20, 'x') AS ROW(a INTEGER, b VARCHAR)))")

            val rows = computeActual("SELECT id, s.a, s.b FROM $table ORDER BY id").materializedRows
            assertThat(rows).hasSize(2)
            // Old file's row: the added subfield reads back NULL (parquet self-heal).
            assertThat((rows[0].getField(1) as Number).toLong()).isEqualTo(10L)
            assertThat(rows[0].getField(2)).isNull()
            // New file's row: both subfields present.
            assertThat((rows[1].getField(1) as Number).toLong()).isEqualTo(20L)
            assertThat(rows[1].getField(2)).isEqualTo("x")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun dropFieldRemovesItFromTheStruct() {
        val table = "test_schema.nf_drop"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(a INTEGER, b INTEGER))")
            computeActual("INSERT INTO $table VALUES (1, CAST(ROW(10, 11) AS ROW(a INTEGER, b INTEGER)))")

            computeActual("ALTER TABLE $table DROP COLUMN s.b")
            assertThat(computeScalar("SELECT typeof(s) FROM $table LIMIT 1"))
                    .isEqualTo("row(\"a\" integer)")
            // The surviving subfield still reads; the dropped one is gone from the schema.
            assertThat((computeScalar("SELECT s.a FROM $table") as Number).toLong()).isEqualTo(10L)
            assertQueryFails("SELECT s.b FROM $table", ".*")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun addFieldToNestedStruct() {
        val table = "test_schema.nf_nested"
        try {
            // `child` (not the reserved word `inner`) as the intermediate struct field name. The
            // `id` column keeps the single-row VALUES from being read as a flattened row literal.
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(child ROW(a INTEGER)))")
            computeActual("INSERT INTO $table VALUES (1, CAST(ROW(ROW(10)) AS ROW(child ROW(a INTEGER))))")

            // parentPath = [s, child]
            computeActual("ALTER TABLE $table ADD COLUMN s.child.b INTEGER")
            computeActual("INSERT INTO $table VALUES " +
                    "(2, CAST(ROW(ROW(20, 30)) AS ROW(child ROW(a INTEGER, b INTEGER))))")

            val rows = computeActual("SELECT s.child.a, s.child.b FROM $table ORDER BY s.child.a")
                    .materializedRows
            assertThat(rows).hasSize(2)
            assertThat((rows[0].getField(0) as Number).toLong()).isEqualTo(10L)
            assertThat(rows[0].getField(1)).isNull()
            assertThat((rows[1].getField(0) as Number).toLong()).isEqualTo(20L)
            assertThat((rows[1].getField(1) as Number).toLong()).isEqualTo(30L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun dropNestedStructFieldRemovesWholeSubtree() {
        // Dropping `s.child` (itself a struct with subfields) must end-snapshot the entire subtree,
        // not just the one row — exercises the recursive descendant collection.
        val table = "test_schema.nf_drop_subtree"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(keep INTEGER, child ROW(a INTEGER, b INTEGER)))")
            computeActual("INSERT INTO $table VALUES " +
                    "(1, CAST(ROW(7, ROW(10, 11)) AS ROW(keep INTEGER, child ROW(a INTEGER, b INTEGER))))")

            computeActual("ALTER TABLE $table DROP COLUMN s.child")
            assertThat(computeScalar("SELECT typeof(s) FROM $table LIMIT 1"))
                    .isEqualTo("row(\"keep\" integer)")
            assertThat((computeScalar("SELECT s.keep FROM $table") as Number).toLong()).isEqualTo(7L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun addFieldIfNotExistsIsNoopWhenPresent() {
        val table = "test_schema.nf_ifnotexists"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(a INTEGER))")
            // `a` already exists — IF NOT EXISTS makes this a clean no-op (no error, type unchanged).
            computeActual("ALTER TABLE $table ADD COLUMN IF NOT EXISTS s.a INTEGER")
            computeActual("INSERT INTO $table VALUES (1, CAST(ROW(10) AS ROW(a INTEGER)))")
            assertThat(computeScalar("SELECT typeof(s) FROM $table LIMIT 1")).isEqualTo("row(\"a\" integer)")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun nestedFieldDdlGatedOnNonParquetData() {
        // A duckdb-format data file exists, so the read path can't yet reshape the struct per file —
        // both ADD and DROP FIELD must be rejected with a clear message (step-1 gate).
        val table = "test_schema.nf_gate"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(a INTEGER, b INTEGER)) " +
                    "WITH (data_file_format = 'duckdb')")
            computeActual("INSERT INTO $table VALUES (1, CAST(ROW(10, 11) AS ROW(a INTEGER, b INTEGER)))")

            assertQueryFails("ALTER TABLE $table ADD COLUMN s.c INTEGER",
                    ".*not yet supported for tables with non-parquet data files.*")
            assertQueryFails("ALTER TABLE $table DROP COLUMN s.b",
                    ".*not yet supported for tables with non-parquet data files.*")
        }
        finally {
            tryDropTable(table)
        }
    }
}
