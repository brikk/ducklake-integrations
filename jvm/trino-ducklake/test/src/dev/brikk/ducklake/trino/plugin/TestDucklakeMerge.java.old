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
 * Trino MERGE coverage. Each shape gets its own test so a regression points at the
 * specific MERGE arm that broke:
 *
 * <ul>
 *   <li>MERGE with only {@code WHEN NOT MATCHED THEN INSERT} (insert-only).</li>
 *   <li>MERGE with only {@code WHEN MATCHED THEN DELETE} (delete-only).</li>
 *   <li>MERGE with only {@code WHEN MATCHED THEN UPDATE} (update-only).</li>
 *   <li>MERGE with both update-on-match and insert-when-not-matched (the canonical
 *       upsert).</li>
 *   <li>MERGE with delete-on-match and insert-when-not-matched (replace-by-identity).</li>
 * </ul>
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeMerge
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "merge-integration";
    }

    @Test
    public void testMergeInsertOnly()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_target (id INTEGER, name VARCHAR, value INTEGER)");
            computeActual("INSERT INTO test_schema.merge_target VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

            // MERGE that only inserts (no matches)
            computeActual("MERGE INTO test_schema.merge_target t " +
                    "USING (VALUES (3, 'Charlie', 300), (4, 'Diana', 400)) AS s(id, name, value) " +
                    "ON t.id = s.id " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value)");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_target")).isEqualTo(4L);

            MaterializedResult result = computeActual("SELECT name FROM test_schema.merge_target ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(4);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(3).getField(0)).isEqualTo("Diana");
        }
        finally {
            tryDropTable("test_schema.merge_target");
        }
    }

    @Test
    public void testMergeDeleteOnly()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_del (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.merge_del VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

            // MERGE that only deletes
            computeActual("MERGE INTO test_schema.merge_del t " +
                    "USING (VALUES 1, 3) AS s(id) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN DELETE");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_del")).isEqualTo(1L);
            assertThat(computeScalar("SELECT name FROM test_schema.merge_del")).isEqualTo("Bob");
        }
        finally {
            tryDropTable("test_schema.merge_del");
        }
    }

    @Test
    public void testMergeUpdateOnly()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_upd (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO test_schema.merge_upd VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");

            // MERGE that updates matching rows
            computeActual("MERGE INTO test_schema.merge_upd t " +
                    "USING (VALUES (2, 999.0)) AS s(id, amount) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET amount = s.amount");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_upd")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT amount FROM test_schema.merge_upd ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(100.0);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(999.0);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(300.0);
        }
        finally {
            tryDropTable("test_schema.merge_upd");
        }
    }

    @Test
    public void testMergeUpsert()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_upsert (id INTEGER, name VARCHAR, value INTEGER)");
            computeActual("INSERT INTO test_schema.merge_upsert VALUES (1, 'Alice', 100), (2, 'Bob', 200)");

            // Classic upsert: update existing, insert new
            computeActual("MERGE INTO test_schema.merge_upsert t " +
                    "USING (VALUES (2, 'Bob_updated', 999), (3, 'Charlie', 300)) AS s(id, name, value) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value)");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_upsert")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT id, name, value FROM test_schema.merge_upsert ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(100);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Bob_updated");
            assertThat(result.getMaterializedRows().get(1).getField(2)).isEqualTo(999);
            assertThat(result.getMaterializedRows().get(2).getField(1)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(2).getField(2)).isEqualTo(300);
        }
        finally {
            tryDropTable("test_schema.merge_upsert");
        }
    }

    @Test
    public void testMergeDeleteAndInsert()
    {
        try {
            computeActual("CREATE TABLE test_schema.merge_delinst (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.merge_delinst VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

            // MERGE: delete matches, insert non-matches
            computeActual("MERGE INTO test_schema.merge_delinst t " +
                    "USING (VALUES (2, 'Bob_replaced'), (4, 'Diana')) AS s(id, name) " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN DELETE " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)");

            assertThat(computeScalar("SELECT count(*) FROM test_schema.merge_delinst")).isEqualTo(3L);

            MaterializedResult result = computeActual("SELECT name FROM test_schema.merge_delinst ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("Charlie");
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo("Diana");
        }
        finally {
            tryDropTable("test_schema.merge_delinst");
        }
    }
}
