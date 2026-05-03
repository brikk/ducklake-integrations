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

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Trino UPDATE coverage. UPDATE flows through the same merge-on-read primitive as DELETE
 * but takes a separate test surface because the SQL shape and the assertions are
 * different (assert mutated values, not absent rows). Walks single-column updates,
 * multi-row predicates, expression targets ({@code price = price * 2}), no-match
 * updates, and post-update aggregation correctness.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeUpdate
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "update-integration";
    }

    @Test
    public void testUpdateSingleColumn()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_single (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO test_schema.update_single VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");

            computeActual("UPDATE test_schema.update_single SET amount = 999.0 WHERE id = 2");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.update_single")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT id, name, amount FROM test_schema.update_single ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(100.0);
            assertThat(result.getMaterializedRows().get(1).getField(2)).isEqualTo(999.0);
            assertThat(result.getMaterializedRows().get(2).getField(2)).isEqualTo(300.0);
        }
        finally {
            tryDropTable("test_schema.update_single");
        }
    }

    @Test
    public void testUpdateMultipleRows()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_multi (id INTEGER, status VARCHAR, score INTEGER)");
            computeActual("INSERT INTO test_schema.update_multi VALUES " +
                    "(1, 'pending', 50), (2, 'pending', 60), (3, 'complete', 90), (4, 'pending', 70)");

            computeActual("UPDATE test_schema.update_multi SET status = 'processed' WHERE status = 'pending'");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.update_multi")).isEqualTo(4L);

            // All former 'pending' rows should now be 'processed'
            assertThat(computeScalar("SELECT count(*) FROM test_schema.update_multi WHERE status = 'processed'")).isEqualTo(3L);
            assertThat(computeScalar("SELECT count(*) FROM test_schema.update_multi WHERE status = 'complete'")).isEqualTo(1L);

            // Scores should be unchanged
            assertThat(computeScalar("SELECT sum(score) FROM test_schema.update_multi")).isEqualTo(270L);
        }
        finally {
            tryDropTable("test_schema.update_multi");
        }
    }

    @Test
    public void testUpdateWithExpression()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_expr (id INTEGER, price DOUBLE)");
            computeActual("INSERT INTO test_schema.update_expr VALUES (1, 10.0), (2, 20.0), (3, 30.0)");

            // Double the price for all items
            computeActual("UPDATE test_schema.update_expr SET price = price * 2");

            MaterializedResult result = computeActual("SELECT id, price FROM test_schema.update_expr ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(20.0);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(40.0);
            assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo(60.0);
        }
        finally {
            tryDropTable("test_schema.update_expr");
        }
    }

    @Test
    public void testUpdateNoMatchingRows()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_nomatch (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.update_nomatch VALUES (1, 'Alice'), (2, 'Bob')");

            computeActual("UPDATE test_schema.update_nomatch SET name = 'Unknown' WHERE id > 100");

            // No rows should have changed
            MaterializedResult result = computeActual("SELECT name FROM test_schema.update_nomatch ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Bob");
        }
        finally {
            tryDropTable("test_schema.update_nomatch");
        }
    }

    @Test
    public void testUpdateThenSelect()
    {
        try {
            computeActual("CREATE TABLE test_schema.update_select (id INTEGER, category VARCHAR, value INTEGER)");
            computeActual("INSERT INTO test_schema.update_select VALUES " +
                    "(1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40)");

            // Update category A values
            computeActual("UPDATE test_schema.update_select SET value = value + 100 WHERE category = 'A'");

            MaterializedResult result = computeActual(
                    "SELECT category, sum(value) AS total FROM test_schema.update_select GROUP BY category ORDER BY category");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("A");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(240L); // (10+100) + (30+100) = 240
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("B");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(60L); // 20 + 40 = 60
        }
        finally {
            tryDropTable("test_schema.update_select");
        }
    }
}
