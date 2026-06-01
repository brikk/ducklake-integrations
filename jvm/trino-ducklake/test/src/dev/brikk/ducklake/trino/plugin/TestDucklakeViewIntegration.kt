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

import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_ID
import io.trino.Session
import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Integration tests for DuckLake view support (Trino-dialect views).
 * Uses an isolated catalog to avoid cross-test interference from write operations.
 *
 * DuckDB-created views are not exposed until a SQL transpiler is integrated.
 */
@Execution(ExecutionMode.SAME_THREAD)
open class TestDucklakeViewIntegration : AbstractTestQueryFramework() {
    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("view-integration")
                .build()
    }

    // ==================== DuckDB-created views are hidden ====================

    @Test
    fun testDuckdbViewsNotVisible() {
        // DuckDB-created views (simple_view, aliased_view, duckdb_specific_view)
        // should not be visible — they use dialect='duckdb' and transpiler is not configured
        val result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema'")
        val viewNames = result.materializedRows.stream()
                .map { row -> row.getField(0).toString() }
                .toList()
        assertThat(viewNames).doesNotContain("simple_view", "aliased_view", "duckdb_specific_view")
    }

    // ==================== Trino-created views (CREATE VIEW / DROP VIEW) ====================

    @Test
    fun testCreateAndQueryView() {
        try {
            computeActual("CREATE VIEW test_schema.trino_test_view AS SELECT id, name FROM simple_table WHERE id <= 2")

            val result = computeActual("SELECT id, name FROM test_schema.trino_test_view ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("Product A")
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("Product B")
        }
        finally {
            tryDropView("test_schema.trino_test_view")
        }
    }

    @Test
    fun testCreateViewShowsInListViews() {
        try {
            computeActual("CREATE VIEW test_schema.listed_view AS SELECT id FROM simple_table")

            val result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema'")
            val viewNames = result.materializedRows.stream()
                    .map { row -> row.getField(0).toString() }
                    .toList()
            assertThat(viewNames).contains("listed_view")
        }
        finally {
            tryDropView("test_schema.listed_view")
        }
    }

    @Test
    fun testDropView() {
        computeActual("CREATE VIEW test_schema.drop_me_view AS SELECT id FROM simple_table")

        val beforeDrop = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'drop_me_view'")
        assertThat(beforeDrop.rowCount).isEqualTo(1)

        computeActual("DROP VIEW test_schema.drop_me_view")

        val afterDrop = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'drop_me_view'")
        assertThat(afterDrop.rowCount).isEqualTo(0)
    }

    @Test
    fun testRenameView() {
        try {
            computeActual("CREATE VIEW test_schema.rename_src_view AS SELECT id, name FROM simple_table WHERE id <= 2")

            computeActual("ALTER VIEW test_schema.rename_src_view RENAME TO rename_dst_view")

            val source = computeActual(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'rename_src_view'")
            assertThat(source.rowCount).isEqualTo(0)

            val target = computeActual(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'rename_dst_view'")
            assertThat(target.rowCount).isEqualTo(1)

            val data = computeActual("SELECT id, name FROM test_schema.rename_dst_view ORDER BY id")
            assertThat(data.rowCount).isEqualTo(2)
            assertThat(data.materializedRows[0].getField(1)).isEqualTo("Product A")
            assertThat(data.materializedRows[1].getField(1)).isEqualTo("Product B")
        }
        finally {
            tryDropView("test_schema.rename_src_view")
            tryDropView("test_schema.rename_dst_view")
        }
    }

    @Test
    fun testViewComment() {
        try {
            computeActual("CREATE VIEW test_schema.view_comment_test AS SELECT id, name FROM simple_table")

            computeActual("COMMENT ON VIEW test_schema.view_comment_test IS 'view-level comment'")
            val showCreateWithComment = computeActual("SHOW CREATE VIEW test_schema.view_comment_test").onlyValue as String
            assertThat(showCreateWithComment).contains("COMMENT 'view-level comment'")

            computeActual("COMMENT ON VIEW test_schema.view_comment_test IS NULL")
            val showCreateWithoutComment = computeActual("SHOW CREATE VIEW test_schema.view_comment_test").onlyValue as String
            assertThat(showCreateWithoutComment).doesNotContain("COMMENT 'view-level comment'")
        }
        finally {
            tryDropView("test_schema.view_comment_test")
        }
    }

    @Test
    fun testViewColumnComment() {
        try {
            computeActual("CREATE VIEW test_schema.view_column_comment_test AS SELECT id, name FROM simple_table")

            computeActual("COMMENT ON COLUMN test_schema.view_column_comment_test.name IS 'name comment'")
            val describedWithComment = computeActual("DESCRIBE test_schema.view_column_comment_test")
            assertThat(describedWithComment.materializedRows.stream()
                    .filter { row -> row.getField(0) == "name" }
                    .findFirst()
                    .orElseThrow()
                    .getField(3)).isEqualTo("name comment")

            computeActual("COMMENT ON COLUMN test_schema.view_column_comment_test.name IS NULL")
            val describedWithoutComment = computeActual("DESCRIBE test_schema.view_column_comment_test")
            assertThat(describedWithoutComment.materializedRows.stream()
                    .filter { row -> row.getField(0) == "name" }
                    .findFirst()
                    .orElseThrow()
                    .getField(3)).isIn(null as Any?, "")
        }
        finally {
            tryDropView("test_schema.view_column_comment_test")
        }
    }

    @Test
    fun testCreateOrReplaceView() {
        try {
            computeActual("CREATE VIEW test_schema.replaceable_view AS SELECT id, name FROM simple_table WHERE id = 1")

            val before = computeActual("SELECT count(*) FROM test_schema.replaceable_view")
            assertThat(before.materializedRows[0].getField(0)).isEqualTo(1L)

            computeActual("CREATE OR REPLACE VIEW test_schema.replaceable_view AS SELECT id, name FROM simple_table WHERE id <= 3")

            val after = computeActual("SELECT count(*) FROM test_schema.replaceable_view")
            assertThat(after.materializedRows[0].getField(0)).isEqualTo(3L)
        }
        finally {
            tryDropView("test_schema.replaceable_view")
        }
    }

    @Test
    fun testDropNonexistentViewFails() {
        assertThatThrownBy { computeActual("DROP VIEW test_schema.nonexistent_view") }
                .hasMessageContaining("nonexistent_view")
    }

    @Test
    fun testViewPreservesColumnTypes() {
        try {
            computeActual("CREATE VIEW test_schema.typed_view AS SELECT id, name, price, active, created_date FROM simple_table")

            val result = computeActual("DESCRIBE test_schema.typed_view")
            val rows = result.materializedRows.stream()
                    .map { row -> listOf<Any>(row.getField(0), row.getField(1)) }
                    .toList()

            assertThat(rows).containsExactly(
                    listOf<Any>("id", "integer"),
                    listOf<Any>("name", "varchar"),
                    listOf<Any>("price", "double"),
                    listOf<Any>("active", "boolean"),
                    listOf<Any>("created_date", "date"))
        }
        finally {
            tryDropView("test_schema.typed_view")
        }
    }

    // ==================== Snapshot isolation ====================

    @Test
    fun testViewSnapshotIsolation() {
        try {
            computeActual("CREATE VIEW test_schema.snapshot_test_view AS SELECT id FROM simple_table")

            // View should be visible at current snapshot
            val current = computeActual("SELECT count(*) FROM test_schema.snapshot_test_view")
            assertThat(current.materializedRows[0].getField(0)).isEqualTo(5L)

            // View should NOT be visible when pinned to snapshot 1 (initial catalog, before any views)
            val pinnedSession = Session.builder(session)
                    .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_ID, "1")
                    .build()

            val pinnedViews = computeActual(pinnedSession,
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'snapshot_test_view'")
            assertThat(pinnedViews.rowCount).isEqualTo(0)
        }
        finally {
            tryDropView("test_schema.snapshot_test_view")
        }
    }

    // ==================== Complex view queries ====================

    @Test
    fun testViewWithJoin() {
        try {
            computeActual("""
                    CREATE VIEW test_schema.join_view AS
                        SELECT s.id, s.name, p.region
                        FROM simple_table s
                        JOIN partitioned_table p ON s.id = p.id
                    """.trimIndent())

            val result = computeActual("SELECT id, name, region FROM test_schema.join_view ORDER BY id")
            assertThat(result.rowCount).isGreaterThan(0)

            // Verify column types are correct
            val desc = computeActual("DESCRIBE test_schema.join_view")
            val colNames = desc.materializedRows.stream()
                    .map { row -> row.getField(0).toString() }
                    .toList()
            assertThat(colNames).containsExactly("id", "name", "region")
        }
        finally {
            tryDropView("test_schema.join_view")
        }
    }

    @Test
    fun testViewWithAggregation() {
        try {
            computeActual("""
                    CREATE VIEW test_schema.agg_view AS
                        SELECT active, count(*) AS cnt, sum(price) AS total_price
                        FROM simple_table
                        GROUP BY active
                    """.trimIndent())

            val result = computeActual("SELECT active, cnt, total_price FROM test_schema.agg_view ORDER BY active")
            assertThat(result.rowCount).isEqualTo(2) // true and false groups

            val desc = computeActual("DESCRIBE test_schema.agg_view")
            val cols = desc.materializedRows.stream()
                    .map { row -> listOf<Any>(row.getField(0), row.getField(1)) }
                    .toList()
            assertThat(cols).containsExactly(
                    listOf<Any>("active", "boolean"),
                    listOf<Any>("cnt", "bigint"),
                    listOf<Any>("total_price", "double"))
        }
        finally {
            tryDropView("test_schema.agg_view")
        }
    }

    @Test
    fun testViewWithSubquery() {
        try {
            computeActual("""
                    CREATE VIEW test_schema.subquery_view AS
                        SELECT id, name FROM simple_table
                        WHERE price > (SELECT avg(price) FROM simple_table)
                    """.trimIndent())

            val result = computeActual("SELECT id, name FROM test_schema.subquery_view ORDER BY id")
            // avg price is (19.99+29.99+39.99+49.99+59.99)/5 = 39.99, so id 4 (49.99) and 5 (59.99)
            assertThat(result.rowCount).isEqualTo(2)
        }
        finally {
            tryDropView("test_schema.subquery_view")
        }
    }

    // ==================== Nested views ====================

    @Test
    fun testViewOfView() {
        try {
            computeActual("CREATE VIEW test_schema.base_view AS SELECT id, name, price FROM simple_table WHERE active = true")
            computeActual("CREATE VIEW test_schema.nested_view AS SELECT id, name FROM test_schema.base_view WHERE price > 25")

            val result = computeActual("SELECT id, name FROM test_schema.nested_view ORDER BY id")
            // active=true: Products A(19.99), B(29.99), D(49.99). price>25: B and D
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("Product B")
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("Product D")
        }
        finally {
            tryDropView("test_schema.nested_view")
            tryDropView("test_schema.base_view")
        }
    }

    @Test
    fun testViewOfViewWithAliases() {
        try {
            computeActual("""
                    CREATE VIEW test_schema.aliased_base AS
                        SELECT id AS product_id, name AS product_name, price AS unit_price
                        FROM simple_table WHERE active = true
                    """.trimIndent())
            computeActual("""
                    CREATE VIEW test_schema.aliased_nested AS
                        SELECT product_id AS pid, product_name AS label, unit_price * 1.1 AS price_with_tax
                        FROM test_schema.aliased_base WHERE unit_price > 25
                    """.trimIndent())

            // active=true: A(19.99), B(29.99), D(49.99). unit_price>25: B and D
            val result = computeActual("SELECT pid, label, price_with_tax FROM test_schema.aliased_nested ORDER BY pid")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("Product B")
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("Product D")

            // Verify aliased column names and types propagate
            val desc = computeActual("DESCRIBE test_schema.aliased_nested")
            val cols = desc.materializedRows.stream()
                    .map { row -> listOf<Any>(row.getField(0), row.getField(1)) }
                    .toList()
            assertThat(cols).containsExactly(
                    listOf<Any>("pid", "integer"),
                    listOf<Any>("label", "varchar"),
                    listOf<Any>("price_with_tax", "double"))
        }
        finally {
            tryDropView("test_schema.aliased_nested")
            tryDropView("test_schema.aliased_base")
        }
    }

    // ==================== Edge cases ====================

    @Test
    fun testViewWithSameNameAsTable() {
        // Should fail — can't create a view with the same name as an existing table
        assertThatThrownBy { computeActual("CREATE VIEW test_schema.simple_table AS SELECT 1 AS x") }
                .hasMessageContaining("simple_table")
    }

    @Test
    fun testViewReturningEmptyResult() {
        try {
            computeActual("CREATE VIEW test_schema.empty_view AS SELECT id, name FROM simple_table WHERE id < 0")

            val result = computeActual("SELECT * FROM test_schema.empty_view")
            assertThat(result.rowCount).isEqualTo(0)

            // Column metadata should still be available
            val desc = computeActual("DESCRIBE test_schema.empty_view")
            assertThat(desc.rowCount).isEqualTo(2)
        }
        finally {
            tryDropView("test_schema.empty_view")
        }
    }

    @Test
    fun testMultipleViewsInListViews() {
        try {
            computeActual("CREATE VIEW test_schema.multi_a AS SELECT id FROM simple_table")
            computeActual("CREATE VIEW test_schema.multi_b AS SELECT name FROM simple_table")
            computeActual("CREATE VIEW test_schema.multi_c AS SELECT price FROM simple_table")

            val result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' ORDER BY table_name")
            val viewNames = result.materializedRows.stream()
                    .map { row -> row.getField(0).toString() }
                    .toList()
            assertThat(viewNames).contains("multi_a", "multi_b", "multi_c")
        }
        finally {
            tryDropView("test_schema.multi_c")
            tryDropView("test_schema.multi_b")
            tryDropView("test_schema.multi_a")
        }
    }

    // ==================== Helpers ====================

    private fun tryDropView(viewName: String) {
        try {
            computeActual("DROP VIEW IF EXISTS " + viewName)
        }
        catch (_: Exception) {
            // Ignore cleanup failures
        }
    }
}
