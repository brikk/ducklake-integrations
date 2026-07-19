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
import java.sql.Connection
import java.sql.DriverManager

/**
 * ALTER COLUMN over DuckLake **inlined** data (rows DuckDB keeps in the per-schema-version
 * `ducklake_inlined_data_<tableId>_<schemaVersion>` tables). ALTER COLUMN is metadata-only — it
 * bumps the schema version and never touches the inlined tables — so old inlined rows stay in their
 * original versioned table and are read back by resolving each requested column's `column_id`
 * against the schema as of that version (added -> NULL, renamed -> aliased, dropped -> omitted).
 *
 * Closes the coverage gaps beyond the ADD-COLUMN case in `TestDucklakeCrossEngineTypeAudit`:
 * RENAME/DROP COLUMN over inlined reads, and the change feed reading inlined inserts across an
 * ADD COLUMN that landed inside the window.
 *
 * SAME_THREAD: shared catalog.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCrossEngineInlinedAlter : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "xengine-inlined-alter"

    private fun rows(sql: String): List<MaterializedRow> = computeActual(sql).materializedRows

    private fun currentSnapshot(): Long {
        val catalog = getIsolatedCatalog()
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
            return CatalogQueries.latestSnapshotId(CatalogTestSupport.dsl(conn))
        }
    }

    private fun inlineInto(table: String, valuesSql: String) {
        createDuckdbConnection().use { conn: Connection ->
            conn.createStatement().use { stmt ->
                stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, " +
                        "schema => 'test_schema', table_name => '$table')")
                stmt.execute("INSERT INTO ducklake_db.test_schema.$table VALUES $valuesSql")
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun renameAndDropColumnOverInlinedRows() {
        val trino = "test_schema.inl_rename_drop"
        try {
            computeActual("CREATE TABLE $trino (id INTEGER, name VARCHAR, tmp INTEGER)")
            inlineInto("inl_rename_drop", "(1, 'alpha', 10), (2, 'beta', 20)")

            // Metadata-only ALTERs — the inlined rows stay put in the v1 inlined table.
            computeActual("ALTER TABLE $trino RENAME COLUMN name TO label")
            computeActual("ALTER TABLE $trino DROP COLUMN tmp")

            // Read resolves label by column_id to its old physical name in the v1 inlined table;
            // dropped tmp is simply not requested. Values must survive (not all-NULL).
            val result = rows("SELECT id, label FROM $trino ORDER BY id")
            assertThat(result.map { it.getField(0) as Int }).containsExactly(1, 2)
            assertThat(result.map { it.getField(1) as String }).containsExactly("alpha", "beta")
            // The dropped column is gone from the schema.
            assertThat(computeActual("DESCRIBE $trino").materializedRows.map { it.getField(0) as String })
                    .contains("label").doesNotContain("name", "tmp")
        }
        finally {
            tryDropTable(trino)
        }
    }

    @Test
    @Throws(Exception::class)
    fun changeFeedReadsInlinedInsertsAcrossAddColumn() {
        val trino = "test_schema.inl_cf_alter"
        try {
            computeActual("CREATE TABLE $trino (id INTEGER, val VARCHAR)")
            // All writes through DuckDB (the engine that owns inlining): inline a batch, ADD COLUMN
            // (bumps schema version), inline a second batch under the new version.
            createDuckdbConnection().use { conn: Connection ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, " +
                            "schema => 'test_schema', table_name => 'inl_cf_alter')")
                    stmt.execute("INSERT INTO ducklake_db.test_schema.inl_cf_alter VALUES (1, 'a'), (2, 'b')")
                    stmt.execute("ALTER TABLE ducklake_db.test_schema.inl_cf_alter ADD COLUMN score INTEGER")
                    stmt.execute("INSERT INTO ducklake_db.test_schema.inl_cf_alter VALUES (3, 'c', 30)")
                }
            }
            val end = currentSnapshot()

            // The change feed reads inlined inserts from BOTH schema-version tables, as of the END
            // schema (id, val, score): the v1 rows show score = NULL, the v2 row shows score = 30.
            val result = rows("SELECT id, val, score FROM " +
                    "TABLE(ducklake.system.table_insertions('test_schema', 'inl_cf_alter', 1, $end)) ORDER BY id")
            assertThat(result.map { it.getField(0) as Int }).containsExactly(1, 2, 3)
            assertThat(result.map { it.getField(1) as String }).containsExactly("a", "b", "c")
            assertThat(result.map { it.getField(2) }).containsExactly(null, null, 30)
        }
        finally {
            tryDropTable(trino)
        }
    }
}
