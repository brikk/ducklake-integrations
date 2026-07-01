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

import dev.brikk.ducklake.catalog.testing.CatalogQueries
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import io.trino.testing.MaterializedRow
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager

/**
 * Cross-engine change feed: DuckDB writes an UPDATE against the shared PostgreSQL-backed DuckLake
 * catalog, and Trino's `table_changes` reads it back through the connector's data/delete-file read
 * path. This proves the feed's file diffing (a DuckDB-written delete file minus its predecessor,
 * plus the DuckDB-written replacement data file) works across engines.
 *
 * On `change_type`: the UPDATE surfaces as a `delete` of the old row + an `insert` of the new row
 * with a DISTINCT `rowid`. DuckLake preserves row lineage internally, but this connector's `rowid`
 * is `row_id_start + file position` — the rewritten row lands in a new file with a fresh
 * `row_id_start`, so its `rowid` differs and the two are not paired into update pre/post-image.
 * (The pairing logic itself is unit-tested directly in `TestChangeFeedPageSource`.)
 *
 * SAME_THREAD: writes to the shared catalog.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeChangeFeedCrossEngine : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "change-feed-xengine"

    private fun rows(sql: String): List<MaterializedRow> = computeActual(sql).materializedRows

    private fun currentSnapshot(): Long {
        val catalog = getIsolatedCatalog()
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
            return CatalogQueries.latestSnapshotId(CatalogTestSupport.dsl(conn))
        }
    }

    @Test
    @Throws(Exception::class)
    fun duckdbWrittenUpdateIsReadAsDeletePlusInsert() {
        val trino = "test_schema.cf_xengine_update"
        val duckdb = "ducklake_db.test_schema.cf_xengine_update"
        try {
            computeActual("CREATE TABLE $trino (id INTEGER, val VARCHAR)")
            computeActual("INSERT INTO $trino VALUES (1, 'Hello'), (2, 'DuckLake')")
            val afterInsert = currentSnapshot()

            // DuckDB UPDATE with data inlining disabled, so the change lands in real data/delete
            // files (the feed reads file-based changes; inlined changes are gated at analyze time).
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 0, " +
                            "schema => 'test_schema', table_name => 'cf_xengine_update')")
                    stmt.execute("UPDATE $duckdb SET val = concat(val, val, val) WHERE id = 2")
                }
            }
            val afterUpdate = currentSnapshot()

            // Just the UPDATE snapshot window: the DuckDB-written delete + insert.
            val result = rows(
                    "SELECT change_type, id, val FROM " +
                            "TABLE(ducklake.system.table_changes('test_schema', 'cf_xengine_update', " +
                            "${afterInsert + 1}, $afterUpdate)) ORDER BY change_type")
            assertThat(result.map { it.getField(0) as String }).containsExactly("delete", "insert")
            val delete = result.first { it.getField(0) == "delete" }
            val insert = result.first { it.getField(0) == "insert" }
            assertThat(delete.getField(2) as String).isEqualTo("DuckLake")
            assertThat(insert.getField(2) as String).isEqualTo("DuckLakeDuckLakeDuckLake")

            // And the deletion side alone reports the retired original row.
            val deletions = rows(
                    "SELECT id, val FROM TABLE(ducklake.system.table_deletions('test_schema', " +
                            "'cf_xengine_update', ${afterInsert + 1}, $afterUpdate))")
            assertThat(deletions).hasSize(1)
            assertThat(deletions[0].getField(1) as String).isEqualTo("DuckLake")
        }
        finally {
            tryDropTable(trino)
        }
    }

    @Test
    @Throws(Exception::class)
    fun feedRejectsTablesWithInlinedData() {
        // DuckDB writes small rows that land in the inlined-data table (default row limit 10). The
        // file-based change feed does not surface inlined changes, so it rejects with a clear
        // pointer to flush_inlined_data rather than silently returning incomplete results.
        val trino = "test_schema.cf_xengine_inlined"
        val duckdb = "ducklake_db.test_schema.cf_xengine_inlined"
        try {
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $duckdb (id INTEGER, val VARCHAR)")
                    stmt.execute("INSERT INTO $duckdb VALUES (1, 'a'), (2, 'b')")
                }
            }
            val end = currentSnapshot()
            org.assertj.core.api.Assertions.assertThatThrownBy {
                computeActual("SELECT * FROM TABLE(ducklake.system.table_insertions(" +
                        "'test_schema', 'cf_xengine_inlined', 1, $end))")
            }.hasMessageContaining("flush_inlined_data")
        }
        finally {
            tryDropTable(trino)
        }
    }
}
