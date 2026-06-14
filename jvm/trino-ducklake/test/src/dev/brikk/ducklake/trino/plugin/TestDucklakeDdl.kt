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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * The small catalog DDL operations (driving-list F1): `ALTER TABLE RENAME TO` (same-schema
 * and cross-schema), `ALTER SCHEMA RENAME TO`, and `COMMENT ON TABLE/COLUMN`. All are
 * versioned-row catalog mutations — data files never move, so every rename test re-reads the
 * data afterwards to prove the table's path/id wiring survived. Comments are stored in
 * `ducklake_tag` / `ducklake_column_tag` with the same `comment` key upstream COMMENT ON
 * uses, so they round-trip cross-engine.
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeDdl : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "ddl-integration"

    @Test
    fun testRenameTable() {
        try {
            computeActual("CREATE TABLE test_schema.rename_src (id INTEGER, name VARCHAR)")
            computeActual("INSERT INTO test_schema.rename_src VALUES (1, 'alice'), (2, 'bob')")

            computeActual("ALTER TABLE test_schema.rename_src RENAME TO test_schema.rename_dst")

            // Data must survive the rename (the table id and data path are unchanged).
            assertThat(computeActual("SELECT name FROM test_schema.rename_dst ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alice", "bob")
            // The old name is gone.
            assertThatThrownBy { computeActual("SELECT * FROM test_schema.rename_src") }
                    .hasMessageContaining("does not exist")

            // The renamed table stays writable and deletable (id-keyed paths still resolve).
            computeActual("INSERT INTO test_schema.rename_dst VALUES (3, 'carol')")
            computeActual("DELETE FROM test_schema.rename_dst WHERE id = 1")
            assertThat(computeActual("SELECT name FROM test_schema.rename_dst ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("bob", "carol")
        }
        finally {
            tryDropTable("test_schema.rename_src")
            tryDropTable("test_schema.rename_dst")
        }
    }

    @Test
    fun testRenameTableAcrossSchemasIsRejected() {
        try {
            computeActual("CREATE SCHEMA ddl_target_schema")
            computeActual("CREATE TABLE test_schema.move_src (id INTEGER)")
            computeActual("INSERT INTO test_schema.move_src VALUES (42)")

            // Table data paths are schema-relative, so a cross-schema move would orphan the
            // data files — rejected with a clean error, and the table must stay untouched.
            assertThatThrownBy {
                computeActual("ALTER TABLE test_schema.move_src RENAME TO ddl_target_schema.move_dst")
            }.hasMessageContaining("schema-relative")

            assertThat(computeScalar("SELECT id FROM test_schema.move_src")).isEqualTo(42)
        }
        finally {
            tryDropTable("test_schema.move_src")
            tryDropSchema("ddl_target_schema")
        }
    }

    @Test
    fun testRenameTableRejectsTakenName() {
        try {
            computeActual("CREATE TABLE test_schema.rename_clash_a (id INTEGER)")
            computeActual("CREATE TABLE test_schema.rename_clash_b (id INTEGER)")

            assertThatThrownBy {
                computeActual("ALTER TABLE test_schema.rename_clash_a RENAME TO test_schema.rename_clash_b")
            }.hasMessageContaining("already exists")
        }
        finally {
            tryDropTable("test_schema.rename_clash_a")
            tryDropTable("test_schema.rename_clash_b")
        }
    }

    @Test
    fun testRenameSchema() {
        try {
            computeActual("CREATE SCHEMA ddl_rename_me")
            computeActual("CREATE TABLE ddl_rename_me.t (id INTEGER)")
            computeActual("INSERT INTO ddl_rename_me.t VALUES (7)")

            computeActual("ALTER SCHEMA ddl_rename_me RENAME TO ddl_renamed")

            // Tables follow the schema id, and their data stays readable and writable.
            assertThat(computeScalar("SELECT id FROM ddl_renamed.t")).isEqualTo(7)
            computeActual("INSERT INTO ddl_renamed.t VALUES (8)")
            assertThat(computeScalar("SELECT count(*) FROM ddl_renamed.t")).isEqualTo(2L)
            assertThatThrownBy { computeActual("SELECT * FROM ddl_rename_me.t") }
                    .hasMessageContaining("does not exist")
        }
        finally {
            tryDropTable("ddl_renamed.t")
            tryDropSchema("ddl_renamed")
            tryDropSchema("ddl_rename_me")
        }
    }

    @Test
    fun testRenameSchemaRejectsTakenName() {
        try {
            computeActual("CREATE SCHEMA ddl_clash_src")

            assertThatThrownBy {
                computeActual("ALTER SCHEMA ddl_clash_src RENAME TO test_schema")
            }.hasMessageContaining("already exists")
        }
        finally {
            tryDropSchema("ddl_clash_src")
        }
    }

    @Test
    fun testCommentOnTable() {
        try {
            computeActual("CREATE TABLE test_schema.comment_table (id INTEGER)")

            computeActual("COMMENT ON TABLE test_schema.comment_table IS 'fact table'")
            assertThat(computeScalar(
                    "SELECT comment FROM system.metadata.table_comments " +
                    "WHERE catalog_name = 'ducklake' AND schema_name = 'test_schema' AND table_name = 'comment_table'"))
                    .isEqualTo("fact table")

            // Comments are replaceable and clearable.
            computeActual("COMMENT ON TABLE test_schema.comment_table IS 'dimension table'")
            assertThat(computeScalar(
                    "SELECT comment FROM system.metadata.table_comments " +
                    "WHERE catalog_name = 'ducklake' AND schema_name = 'test_schema' AND table_name = 'comment_table'"))
                    .isEqualTo("dimension table")

            computeActual("COMMENT ON TABLE test_schema.comment_table IS NULL")
            assertThat(computeScalar(
                    "SELECT comment FROM system.metadata.table_comments " +
                    "WHERE catalog_name = 'ducklake' AND schema_name = 'test_schema' AND table_name = 'comment_table'"))
                    .isNull()
        }
        finally {
            tryDropTable("test_schema.comment_table")
        }
    }

    @Test
    fun testCommentOnColumn() {
        try {
            computeActual("CREATE TABLE test_schema.comment_col (id INTEGER, name VARCHAR)")

            computeActual("COMMENT ON COLUMN test_schema.comment_col.name IS 'customer name'")

            // SHOW COLUMNS surfaces (Column, Type, Extra, Comment).
            val rows = computeActual("SHOW COLUMNS FROM test_schema.comment_col").materializedRows
                    .associate { it.getField(0) as String to it.getField(3) as String }
            assertThat(rows["name"]).isEqualTo("customer name")
            assertThat(rows["id"]).isEmpty()

            // Clearing restores the empty comment.
            computeActual("COMMENT ON COLUMN test_schema.comment_col.name IS NULL")
            val cleared = computeActual("SHOW COLUMNS FROM test_schema.comment_col").materializedRows
                    .associate { it.getField(0) as String to it.getField(3) as String }
            assertThat(cleared["name"]).isEmpty()
        }
        finally {
            tryDropTable("test_schema.comment_col")
        }
    }

    @Test
    fun testCommentSurvivesRename() {
        try {
            computeActual("CREATE TABLE test_schema.comment_keep (id INTEGER)")
            computeActual("COMMENT ON TABLE test_schema.comment_keep IS 'sticky'")
            computeActual("COMMENT ON COLUMN test_schema.comment_keep.id IS 'key'")

            // Comments are keyed by table/column id, so a rename must not lose them.
            computeActual("ALTER TABLE test_schema.comment_keep RENAME TO test_schema.comment_kept")

            assertThat(computeScalar(
                    "SELECT comment FROM system.metadata.table_comments " +
                    "WHERE catalog_name = 'ducklake' AND schema_name = 'test_schema' AND table_name = 'comment_kept'"))
                    .isEqualTo("sticky")
            val rows = computeActual("SHOW COLUMNS FROM test_schema.comment_kept").materializedRows
                    .associate { it.getField(0) as String to it.getField(3) as String }
            assertThat(rows["id"]).isEqualTo("key")
        }
        finally {
            tryDropTable("test_schema.comment_keep")
            tryDropTable("test_schema.comment_kept")
        }
    }
}
