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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ALTER TABLE flows: ADD COLUMN, DROP COLUMN, RENAME COLUMN, plus the data-preservation
 * combinators (add-then-DML, drop-then-INSERT, rename-with-existing-rows). Pins the
 * connector's promise that schema changes leave existing rows readable as NULL for new
 * columns and that the new shape behaves end-to-end with INSERT/DELETE/UPDATE.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeAlterTable
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "ddl-alter-table";
    }

    @Test
    public void testAddColumn()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_add (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_add VALUES (1, 'Alice'), (2, 'Bob')");

            computeActual("ALTER TABLE test_schema.alter_add ADD COLUMN age INTEGER");

            // DESCRIBE should show 3 columns
            MaterializedResult describe = computeActual("DESCRIBE test_schema.alter_add");
            assertThat(describe.getRowCount()).isEqualTo(3);

            // Existing rows should have NULL for the new column
            MaterializedResult result = computeActual("SELECT id, name, age FROM test_schema.alter_add ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isNull();
            assertThat(result.getMaterializedRows().get(1).getField(2)).isNull();

            // Insert a row with the new column
            computeActual("INSERT INTO test_schema.alter_add VALUES (3, 'Charlie', 30)");

            result = computeActual("SELECT id, name, age FROM test_schema.alter_add ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isNull();
            assertThat(result.getMaterializedRows().get(2).getField(2)).isEqualTo(30);
        }
        finally {
            tryDropTable("test_schema.alter_add");
        }
    }

    @Test
    public void testAddMultipleColumns()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_add_multi (id INTEGER)");

            computeActual("ALTER TABLE test_schema.alter_add_multi ADD COLUMN name VARCHAR");
            computeActual("ALTER TABLE test_schema.alter_add_multi ADD COLUMN score DOUBLE");

            MaterializedResult describe = computeActual("DESCRIBE test_schema.alter_add_multi");
            assertThat(describe.getRowCount()).isEqualTo(3);

            // Verify all columns work with INSERT
            computeActual("INSERT INTO test_schema.alter_add_multi VALUES (1, 'Alice', 95.5)");
            MaterializedResult result = computeActual("SELECT * FROM test_schema.alter_add_multi");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(95.5);
        }
        finally {
            tryDropTable("test_schema.alter_add_multi");
        }
    }

    @Test
    public void testDropColumn()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_drop (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO test_schema.alter_drop VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)");

            computeActual("ALTER TABLE test_schema.alter_drop DROP COLUMN amount");

            // DESCRIBE should show 2 columns
            MaterializedResult describe = computeActual("DESCRIBE test_schema.alter_drop");
            assertThat(describe.getRowCount()).isEqualTo(2);

            // Data should be readable without the dropped column
            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.alter_drop ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("Bob");

            // Insert with remaining columns
            computeActual("INSERT INTO test_schema.alter_drop VALUES (3, 'Charlie')");
            assertThat(computeScalar("SELECT count(*) FROM test_schema.alter_drop")).isEqualTo(3L);
        }
        finally {
            tryDropTable("test_schema.alter_drop");
        }
    }

    @Test
    public void testRenameColumn()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_rename (id INTEGER, full_name VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_rename VALUES (1, 'Alice'), (2, 'Bob')");

            computeActual("ALTER TABLE test_schema.alter_rename RENAME COLUMN full_name TO name");

            // DESCRIBE should show new column name
            MaterializedResult describe = computeActual("DESCRIBE test_schema.alter_rename");
            List<String> columnNames = describe.getMaterializedRows().stream()
                    .map(row -> (String) row.getField(0))
                    .toList();
            assertThat(columnNames).contains("name");
            assertThat(columnNames).doesNotContain("full_name");

            // Data should be accessible via the new name
            MaterializedResult result = computeActual("SELECT id, name FROM test_schema.alter_rename ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("Alice");

            // Insert with new column name
            computeActual("INSERT INTO test_schema.alter_rename VALUES (3, 'Charlie')");
            assertThat(computeScalar("SELECT count(*) FROM test_schema.alter_rename")).isEqualTo(3L);
        }
        finally {
            tryDropTable("test_schema.alter_rename");
        }
    }

    @Test
    public void testAddColumnThenDelete()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_del (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_del VALUES (1, 'Alice'), (2, 'Bob')");

            // Add column, then delete from the table
            computeActual("ALTER TABLE test_schema.alter_del ADD COLUMN status VARCHAR");
            computeActual("INSERT INTO test_schema.alter_del VALUES (3, 'Charlie', 'active')");

            computeActual("DELETE FROM test_schema.alter_del WHERE id = 2");

            MaterializedResult result = computeActual("SELECT id, name, status FROM test_schema.alter_del ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isNull(); // old row, no status
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(1).getField(2)).isEqualTo("active");
        }
        finally {
            tryDropTable("test_schema.alter_del");
        }
    }

    @Test
    public void testAddColumnThenUpdate()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_upd (id INTEGER, name VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_upd VALUES (1, 'Alice'), (2, 'Bob')");

            computeActual("ALTER TABLE test_schema.alter_upd ADD COLUMN score INTEGER");

            // Update an old row to set the new column
            computeActual("UPDATE test_schema.alter_upd SET score = 100 WHERE id = 1");

            MaterializedResult result = computeActual("SELECT id, name, score FROM test_schema.alter_upd ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(2)).isEqualTo(100);
            assertThat(result.getMaterializedRows().get(1).getField(2)).isNull(); // not updated
        }
        finally {
            tryDropTable("test_schema.alter_upd");
        }
    }

    @Test
    public void testDropColumnThenInsert()
    {
        try {
            computeActual("CREATE TABLE test_schema.alter_drop_ins (id INTEGER, name VARCHAR, temp VARCHAR)");
            computeActual("INSERT INTO test_schema.alter_drop_ins VALUES (1, 'Alice', 'x')");

            computeActual("ALTER TABLE test_schema.alter_drop_ins DROP COLUMN temp");
            computeActual("INSERT INTO test_schema.alter_drop_ins VALUES (2, 'Bob')");

            MaterializedResult result = computeActual("SELECT * FROM test_schema.alter_drop_ins ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            // Both rows should have exactly 2 columns
            assertThat(result.getMaterializedRows().get(0).getFieldCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(1).getFieldCount()).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.alter_drop_ins");
        }
    }

    @Test
    public void testRenameColumnPreservesData()
    {
        try {
            computeActual("CREATE TABLE test_schema.rename_data (id INTEGER, value DOUBLE)");
            computeActual("INSERT INTO test_schema.rename_data VALUES (1, 42.5), (2, 99.9)");

            computeActual("ALTER TABLE test_schema.rename_data RENAME COLUMN value TO score");

            // Data should be intact under new name
            MaterializedResult result = computeActual("SELECT sum(score) FROM test_schema.rename_data");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(142.4);
        }
        finally {
            tryDropTable("test_schema.rename_data");
        }
    }
}
