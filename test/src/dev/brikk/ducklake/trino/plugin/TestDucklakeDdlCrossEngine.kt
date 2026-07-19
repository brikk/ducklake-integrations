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
import java.sql.Connection

/**
 * Cross-engine validation of the F1 DDL operations: after Trino renames a table or schema
 * or sets comments, a raw DuckDB connection ATTACHed to the same catalog must still load
 * the lake (its change-type parser throws on unknown `changes_made` vocabulary — the schema
 * rename writes `dropped_schema` + `created_schema` exactly because of that), resolve the
 * renamed objects, read their data, and surface the comments (upstream reads the same
 * `comment` tags via `duckdb_tables()` / `duckdb_columns()`).
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeDdlCrossEngine : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "ddl-cross-engine"

    @Test
    fun duckdbSeesTrinoRenamedTable() {
        try {
            computeActual("CREATE TABLE test_schema.xe_rename (id INTEGER)")
            computeActual("INSERT INTO test_schema.xe_rename VALUES (5)")

            computeActual("ALTER TABLE test_schema.xe_rename RENAME TO test_schema.xe_renamed")

            createDuckdbConnection().use { duck ->
                assertThat(queryLong(duck, "SELECT id FROM ducklake_db.test_schema.xe_renamed"))
                        .isEqualTo(5L)
            }
        }
        finally {
            tryDropTable("test_schema.xe_rename")
            tryDropTable("test_schema.xe_renamed")
        }
    }

    @Test
    fun duckdbSeesTrinoRenamedSchema() {
        try {
            computeActual("CREATE SCHEMA xe_schema_src")
            computeActual("CREATE TABLE xe_schema_src.t (id INTEGER)")
            computeActual("INSERT INTO xe_schema_src.t VALUES (11)")

            computeActual("ALTER SCHEMA xe_schema_src RENAME TO xe_schema_dst")

            // DuckDB must parse our snapshot changes, resolve the new schema row (new
            // schema_id, old path), follow the re-pointed table row, and read the data
            // from its unmoved files.
            createDuckdbConnection().use { duck ->
                assertThat(queryLong(duck, "SELECT id FROM ducklake_db.xe_schema_dst.t"))
                        .isEqualTo(11L)
                // And DuckDB can keep writing into the renamed schema.
                duck.createStatement().use { it.execute("INSERT INTO ducklake_db.xe_schema_dst.t VALUES (12)") }
            }
            assertThat(computeScalar("SELECT count(*) FROM xe_schema_dst.t")).isEqualTo(2L)
        }
        finally {
            tryDropTable("xe_schema_dst.t")
            try {
                computeActual("DROP SCHEMA xe_schema_dst")
            }
            catch (ignored: Exception) {
            }
        }
    }

    @Test
    fun duckdbSeesTrinoComments() {
        try {
            computeActual("CREATE TABLE test_schema.xe_comment (id INTEGER, name VARCHAR)")
            computeActual("COMMENT ON TABLE test_schema.xe_comment IS 'trino table comment'")
            computeActual("COMMENT ON COLUMN test_schema.xe_comment.name IS 'trino column comment'")

            createDuckdbConnection().use { duck ->
                assertThat(queryString(duck,
                        "SELECT comment FROM duckdb_tables() WHERE database_name = 'ducklake_db' AND table_name = 'xe_comment'"))
                        .isEqualTo("trino table comment")
                assertThat(queryString(duck,
                        "SELECT comment FROM duckdb_columns() WHERE database_name = 'ducklake_db' " +
                        "AND table_name = 'xe_comment' AND column_name = 'name'"))
                        .isEqualTo("trino column comment")
            }
        }
        finally {
            tryDropTable("test_schema.xe_comment")
        }
    }

    private fun queryLong(conn: Connection, sql: String): Long {
        conn.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                assertThat(rs.next()).`as`("expected one row from: %s", sql).isTrue()
                return rs.getLong(1)
            }
        }
    }

    private fun queryString(conn: Connection, sql: String): String? {
        conn.createStatement().use { stmt ->
            stmt.executeQuery(sql).use { rs ->
                assertThat(rs.next()).`as`("expected one row from: %s", sql).isTrue()
                return rs.getString(1)
            }
        }
    }
}
