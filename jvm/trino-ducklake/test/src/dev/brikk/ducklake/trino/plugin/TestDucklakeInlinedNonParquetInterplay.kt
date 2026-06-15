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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * The grid cell the driving list (T1) explicitly flags: "inlined-data interplay with
 * non-parquet splits." Trino's page sink never inlines (it always writes files), so a table
 * holding BOTH inlined rows AND non-parquet data files can only arise cross-engine — DuckDB
 * writes the inlined rows (under `data_inlining_row_limit`), Trino appends a non-parquet file.
 * A SELECT then fans out to an inlined split ([createInlinedPageSource]) AND a DuckDB-engine
 * data-file split ([createDuckDbPageSource]) in the same query — this pins that the two READ
 * paths compose. DELETE/UPDATE/MERGE, by contrast, can't tombstone an inlined row (the merge
 * sink only writes parquet position deletes), so it is gated with a clear error rather than
 * failing mid-scan — pinned here too.
 *
 * Lance is the format under test (the most structurally different data-file shape — a
 * directory read via `__lance_scan`). Network/platform-gated: skips when lance is unavailable.
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeInlinedNonParquetInterplay : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "inlined-nonparquet"

    @Test
    fun selectSpansInlinedRowsAndLanceFile() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("lance")
        val table = "test_schema.inlined_lance"
        val bare = "inlined_lance"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")

            // DuckDB writes two rows that stay INLINED (limit above the batch size).
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, " +
                            "schema => 'test_schema', table_name => '$bare')")
                    stmt.execute("INSERT INTO ducklake_db.$table VALUES (1, 'inlined-a'), (2, 'inlined-b')")
                }
            }
            assertRowsStayedInlined(bare, 2L)

            // Trino appends a lance data file (the non-parquet split).
            computeActual(sessionWithFormat(DucklakeSessionProperties.FORMAT_LANCE),
                    "INSERT INTO $table VALUES (3, 'lance-c'), (4, 'lance-d')")
            assertThat(computeScalar("SELECT DISTINCT file_format FROM \"$bare\$files\"") as String)
                    .isEqualTo("lance")

            // A SELECT must union the inlined split and the lance data-file split.
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("inlined-a", "inlined-b", "lance-c", "lance-d")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(4L)
            // A predicate that needs both splits.
            assertThat(computeActual("SELECT id FROM $table WHERE name LIKE '%-%' AND id >= 2 ORDER BY id")
                    .materializedRows.map { (it.getField(0) as Number).toLong() })
                    .containsExactly(2L, 3L, 4L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun truncateClearsInlinedRowsWhereDeleteCannot() {
        // TRUNCATE is a bulk metadata clear, not merge-on-read, so it empties inlined rows too —
        // the case DELETE is gated on. Cross-engine: DuckDB writes inlined rows + Trino a lance
        // file, Trino truncates, and BOTH engines then see an empty table.
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("lance")
        val table = "test_schema.inlined_lance_trunc"
        val bare = "inlined_lance_trunc"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, " +
                            "schema => 'test_schema', table_name => '$bare')")
                    stmt.execute("INSERT INTO ducklake_db.$table VALUES (1, 'inlined-a'), (2, 'inlined-b')")
                }
            }
            computeActual(sessionWithFormat(DucklakeSessionProperties.FORMAT_LANCE),
                    "INSERT INTO $table VALUES (3, 'lance-c')")
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)

            computeActual("TRUNCATE TABLE $table")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(0L)
            // DuckDB must also see the table as empty (the inlined rows were end-snapshotted).
            assertThat(duckdbScalarLong("SELECT count(*) FROM ducklake_db.$table")).isEqualTo(0L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun deleteOverInlinedRowsIsRejectedWithGuidance() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("lance")
        val table = "test_schema.inlined_lance_del"
        val bare = "inlined_lance_del"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, " +
                            "schema => 'test_schema', table_name => '$bare')")
                    stmt.execute("INSERT INTO ducklake_db.$table VALUES (1, 'inlined-a'), (2, 'inlined-b')")
                }
            }
            computeActual(sessionWithFormat(DucklakeSessionProperties.FORMAT_LANCE),
                    "INSERT INTO $table VALUES (3, 'lance-c')")

            // The merge sink can't tombstone an inlined row, so DELETE/UPDATE are gated with a
            // clear, actionable error instead of an opaque mid-scan failure.
            assertThatThrownBy { computeActual("DELETE FROM $table WHERE id = 3") }
                    .hasMessageContaining("inlined rows")
            assertThatThrownBy { computeActual("UPDATE $table SET name = 'x' WHERE id = 1") }
                    .hasMessageContaining("inlined rows")
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun sessionWithFormat(format: String): Session =
            Session.builder(session)
                    .setCatalogSessionProperty("ducklake", DucklakeSessionProperties.DATA_FILE_FORMAT, format)
                    .build()

    private fun duckdbScalarLong(sql: String): Long {
        createDuckdbConnection().use { duck ->
            duck.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    assertThat(rs.next()).isTrue()
                    return rs.getLong(1)
                }
            }
        }
    }
}
