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
package dev.brikk.ducklake.trino.plugin;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for DuckLake view support (Trino-dialect views).
 * Uses an isolated catalog to avoid cross-test interference from write operations.
 *
 * DuckDB-created views are not exposed until a SQL transpiler is integrated.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeViewIntegration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("view-integration")
                .build();
    }

    // ==================== DuckDB-created views are hidden ====================

    @Test
    public void testDuckdbViewsNotVisible()
    {
        // DuckDB-created views (simple_view, aliased_view, duckdb_specific_view)
        // should not be visible — they use dialect='duckdb' and transpiler is not configured
        MaterializedResult result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema'");
        List<String> viewNames = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(viewNames).doesNotContain("simple_view", "aliased_view", "duckdb_specific_view");
    }

    // ==================== Trino-created views (CREATE VIEW / DROP VIEW) ====================

    @Test
    public void testCreateAndQueryView()
    {
        try {
            computeActual("CREATE VIEW test_schema.trino_test_view AS SELECT id, name FROM simple_table WHERE id <= 2");

            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.trino_test_view ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Product A");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Product B");
        }
        finally {
            tryDropView("test_schema.trino_test_view");
        }
    }

    @Test
    public void testCreateViewShowsInListViews()
    {
        try {
            computeActual("CREATE VIEW test_schema.listed_view AS SELECT id FROM simple_table");

            MaterializedResult result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema'");
            List<String> viewNames = result.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(viewNames).contains("listed_view");
        }
        finally {
            tryDropView("test_schema.listed_view");
        }
    }

    @Test
    public void testDropView()
    {
        computeActual("CREATE VIEW test_schema.drop_me_view AS SELECT id FROM simple_table");

        MaterializedResult beforeDrop = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'drop_me_view'");
        assertThat(beforeDrop.getRowCount()).isEqualTo(1);

        computeActual("DROP VIEW test_schema.drop_me_view");

        MaterializedResult afterDrop = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'drop_me_view'");
        assertThat(afterDrop.getRowCount()).isEqualTo(0);
    }

    @Test
    public void testRenameView()
    {
        try {
            computeActual("CREATE VIEW test_schema.rename_src_view AS SELECT id, name FROM simple_table WHERE id <= 2");

            computeActual("ALTER VIEW test_schema.rename_src_view RENAME TO rename_dst_view");

            MaterializedResult source = computeActual(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'rename_src_view'");
            assertThat(source.getRowCount()).isEqualTo(0);

            MaterializedResult target = computeActual(
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'rename_dst_view'");
            assertThat(target.getRowCount()).isEqualTo(1);

            MaterializedResult data = computeActual("SELECT id, name FROM test_schema.rename_dst_view ORDER BY id");
            assertThat(data.getRowCount()).isEqualTo(2);
            assertThat(data.getMaterializedRows().get(0).getField(1)).isEqualTo("Product A");
            assertThat(data.getMaterializedRows().get(1).getField(1)).isEqualTo("Product B");
        }
        finally {
            tryDropView("test_schema.rename_src_view");
            tryDropView("test_schema.rename_dst_view");
        }
    }

    @Test
    public void testViewComment()
    {
        try {
            computeActual("CREATE VIEW test_schema.view_comment_test AS SELECT id, name FROM simple_table");

            computeActual("COMMENT ON VIEW test_schema.view_comment_test IS 'view-level comment'");
            String showCreateWithComment = (String) computeActual("SHOW CREATE VIEW test_schema.view_comment_test").getOnlyValue();
            assertThat(showCreateWithComment).contains("COMMENT 'view-level comment'");

            computeActual("COMMENT ON VIEW test_schema.view_comment_test IS NULL");
            String showCreateWithoutComment = (String) computeActual("SHOW CREATE VIEW test_schema.view_comment_test").getOnlyValue();
            assertThat(showCreateWithoutComment).doesNotContain("COMMENT 'view-level comment'");
        }
        finally {
            tryDropView("test_schema.view_comment_test");
        }
    }

    @Test
    public void testViewColumnComment()
    {
        try {
            computeActual("CREATE VIEW test_schema.view_column_comment_test AS SELECT id, name FROM simple_table");

            computeActual("COMMENT ON COLUMN test_schema.view_column_comment_test.name IS 'name comment'");
            MaterializedResult describedWithComment = computeActual("DESCRIBE test_schema.view_column_comment_test");
            assertThat(describedWithComment.getMaterializedRows().stream()
                    .filter(row -> row.getField(0).equals("name"))
                    .findFirst()
                    .orElseThrow()
                    .getField(3)).isEqualTo("name comment");

            computeActual("COMMENT ON COLUMN test_schema.view_column_comment_test.name IS NULL");
            MaterializedResult describedWithoutComment = computeActual("DESCRIBE test_schema.view_column_comment_test");
            assertThat(describedWithoutComment.getMaterializedRows().stream()
                    .filter(row -> row.getField(0).equals("name"))
                    .findFirst()
                    .orElseThrow()
                    .getField(3)).isIn((Object) null, "");
        }
        finally {
            tryDropView("test_schema.view_column_comment_test");
        }
    }

    @Test
    public void testCreateOrReplaceView()
    {
        try {
            computeActual("CREATE VIEW test_schema.replaceable_view AS SELECT id, name FROM simple_table WHERE id = 1");

            MaterializedResult before = computeActual("SELECT count(*) FROM test_schema.replaceable_view");
            assertThat(before.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);

            computeActual("CREATE OR REPLACE VIEW test_schema.replaceable_view AS SELECT id, name FROM simple_table WHERE id <= 3");

            MaterializedResult after = computeActual("SELECT count(*) FROM test_schema.replaceable_view");
            assertThat(after.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
        }
        finally {
            tryDropView("test_schema.replaceable_view");
        }
    }

    @Test
    public void testDropNonexistentViewFails()
    {
        assertThatThrownBy(() -> computeActual("DROP VIEW test_schema.nonexistent_view"))
                .hasMessageContaining("nonexistent_view");
    }

    @Test
    public void testViewPreservesColumnTypes()
    {
        try {
            computeActual("CREATE VIEW test_schema.typed_view AS SELECT id, name, price, active, created_date FROM simple_table");

            MaterializedResult result = computeActual("DESCRIBE test_schema.typed_view");
            List<List<Object>> rows = result.getMaterializedRows().stream()
                    .map(row -> List.of(row.getField(0), row.getField(1)))
                    .toList();

            assertThat(rows).containsExactly(
                    List.of("id", "integer"),
                    List.of("name", "varchar"),
                    List.of("price", "double"),
                    List.of("active", "boolean"),
                    List.of("created_date", "date"));
        }
        finally {
            tryDropView("test_schema.typed_view");
        }
    }

    // ==================== Snapshot isolation ====================

    @Test
    public void testViewSnapshotIsolation()
    {
        try {
            computeActual("CREATE VIEW test_schema.snapshot_test_view AS SELECT id FROM simple_table");

            // View should be visible at current snapshot
            MaterializedResult current = computeActual("SELECT count(*) FROM test_schema.snapshot_test_view");
            assertThat(current.getMaterializedRows().get(0).getField(0)).isEqualTo(5L);

            // View should NOT be visible when pinned to snapshot 1 (initial catalog, before any views)
            Session pinnedSession = Session.builder(getSession())
                    .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_ID, "1")
                    .build();

            MaterializedResult pinnedViews = computeActual(pinnedSession,
                    "SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'snapshot_test_view'");
            assertThat(pinnedViews.getRowCount()).isEqualTo(0);
        }
        finally {
            tryDropView("test_schema.snapshot_test_view");
        }
    }

    // ==================== Complex view queries ====================

    @Test
    public void testViewWithJoin()
    {
        try {
            computeActual("""
                    CREATE VIEW test_schema.join_view AS
                        SELECT s.id, s.name, p.region
                        FROM simple_table s
                        JOIN partitioned_table p ON s.id = p.id
                    """);

            MaterializedResult result = computeActual("SELECT id, name, region FROM test_schema.join_view ORDER BY id");
            assertThat(result.getRowCount()).isGreaterThan(0);

            // Verify column types are correct
            MaterializedResult desc = computeActual("DESCRIBE test_schema.join_view");
            List<String> colNames = desc.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(colNames).containsExactly("id", "name", "region");
        }
        finally {
            tryDropView("test_schema.join_view");
        }
    }

    @Test
    public void testViewWithAggregation()
    {
        try {
            computeActual("""
                    CREATE VIEW test_schema.agg_view AS
                        SELECT active, count(*) AS cnt, sum(price) AS total_price
                        FROM simple_table
                        GROUP BY active
                    """);

            MaterializedResult result = computeActual("SELECT active, cnt, total_price FROM test_schema.agg_view ORDER BY active");
            assertThat(result.getRowCount()).isEqualTo(2); // true and false groups

            MaterializedResult desc = computeActual("DESCRIBE test_schema.agg_view");
            List<List<Object>> cols = desc.getMaterializedRows().stream()
                    .map(row -> List.of(row.getField(0), row.getField(1)))
                    .toList();
            assertThat(cols).containsExactly(
                    List.of("active", "boolean"),
                    List.of("cnt", "bigint"),
                    List.of("total_price", "double"));
        }
        finally {
            tryDropView("test_schema.agg_view");
        }
    }

    @Test
    public void testViewWithSubquery()
    {
        try {
            computeActual("""
                    CREATE VIEW test_schema.subquery_view AS
                        SELECT id, name FROM simple_table
                        WHERE price > (SELECT avg(price) FROM simple_table)
                    """);

            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.subquery_view ORDER BY id");
            // avg price is (19.99+29.99+39.99+49.99+59.99)/5 = 39.99, so id 4 (49.99) and 5 (59.99)
            assertThat(result.getRowCount()).isEqualTo(2);
        }
        finally {
            tryDropView("test_schema.subquery_view");
        }
    }

    // ==================== Nested views ====================

    @Test
    public void testViewOfView()
    {
        try {
            computeActual("CREATE VIEW test_schema.base_view AS SELECT id, name, price FROM simple_table WHERE active = true");
            computeActual("CREATE VIEW test_schema.nested_view AS SELECT id, name FROM test_schema.base_view WHERE price > 25");

            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.nested_view ORDER BY id");
            // active=true: Products A(19.99), B(29.99), D(49.99). price>25: B and D
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Product B");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Product D");
        }
        finally {
            tryDropView("test_schema.nested_view");
            tryDropView("test_schema.base_view");
        }
    }

    @Test
    public void testViewOfViewWithAliases()
    {
        try {
            computeActual("""
                    CREATE VIEW test_schema.aliased_base AS
                        SELECT id AS product_id, name AS product_name, price AS unit_price
                        FROM simple_table WHERE active = true
                    """);
            computeActual("""
                    CREATE VIEW test_schema.aliased_nested AS
                        SELECT product_id AS pid, product_name AS label, unit_price * 1.1 AS price_with_tax
                        FROM test_schema.aliased_base WHERE unit_price > 25
                    """);

            // active=true: A(19.99), B(29.99), D(49.99). unit_price>25: B and D
            MaterializedResult result = computeActual("SELECT pid, label, price_with_tax FROM test_schema.aliased_nested ORDER BY pid");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Product B");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Product D");

            // Verify aliased column names and types propagate
            MaterializedResult desc = computeActual("DESCRIBE test_schema.aliased_nested");
            List<List<Object>> cols = desc.getMaterializedRows().stream()
                    .map(row -> List.of(row.getField(0), row.getField(1)))
                    .toList();
            assertThat(cols).containsExactly(
                    List.of("pid", "integer"),
                    List.of("label", "varchar"),
                    List.of("price_with_tax", "double"));
        }
        finally {
            tryDropView("test_schema.aliased_nested");
            tryDropView("test_schema.aliased_base");
        }
    }

    // ==================== Edge cases ====================

    @Test
    public void testViewWithSameNameAsTable()
    {
        // Should fail — can't create a view with the same name as an existing table
        assertThatThrownBy(() -> computeActual("CREATE VIEW test_schema.simple_table AS SELECT 1 AS x"))
                .hasMessageContaining("simple_table");
    }

    @Test
    public void testViewReturningEmptyResult()
    {
        try {
            computeActual("CREATE VIEW test_schema.empty_view AS SELECT id, name FROM simple_table WHERE id < 0");

            MaterializedResult result = computeActual("SELECT * FROM test_schema.empty_view");
            assertThat(result.getRowCount()).isEqualTo(0);

            // Column metadata should still be available
            MaterializedResult desc = computeActual("DESCRIBE test_schema.empty_view");
            assertThat(desc.getRowCount()).isEqualTo(2);
        }
        finally {
            tryDropView("test_schema.empty_view");
        }
    }

    @Test
    public void testMultipleViewsInListViews()
    {
        try {
            computeActual("CREATE VIEW test_schema.multi_a AS SELECT id FROM simple_table");
            computeActual("CREATE VIEW test_schema.multi_b AS SELECT name FROM simple_table");
            computeActual("CREATE VIEW test_schema.multi_c AS SELECT price FROM simple_table");

            MaterializedResult result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' ORDER BY table_name");
            List<String> viewNames = result.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(viewNames).contains("multi_a", "multi_b", "multi_c");
        }
        finally {
            tryDropView("test_schema.multi_c");
            tryDropView("test_schema.multi_b");
            tryDropView("test_schema.multi_a");
        }
    }

    // ==================== Helpers ====================

    private void tryDropView(String viewName)
    {
        try {
            computeActual("DROP VIEW IF EXISTS " + viewName);
        }
        catch (Exception _) {
            // Ignore cleanup failures
        }
    }
}
