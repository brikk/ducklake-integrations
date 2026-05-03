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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract between DDL/DML and the catalog's snapshot lineage:
 *
 * <ul>
 *   <li>{@code ducklake_snapshot_changes} captures every DDL operation in spec form
 *       ({@code created_table:"schema"."name"}). DuckDB's {@code ParseCatalogEntry} relies
 *       on the quoted, dotted form — see dev-docs/COMPARE-pg_ducklake.md B1.</li>
 *   <li>{@code ducklake_schema_versions} advances on every schema-changing DDL and stays
 *       fixed across pure-DML INSERT/UPDATE/DELETE/MERGE. {@code schema_version} is
 *       table-scoped to {@code table_id}; the same row appears in
 *       {@code ducklake_snapshot.schema_version}.</li>
 * </ul>
 *
 * Exercises directly against the catalog DB (via {@link #openCatalogConnection}) because
 * these tables aren't projected through {@code information_schema}.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeSnapshotAndSchemaVersion
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "ddl-snapshot-and-schema-version";
    }

    @Test
    public void testSnapshotChangesTracked()
    {
        // Get baseline snapshot count
        MaterializedResult before = computeActual("SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
        long baselineSnapshot = (long) before.getMaterializedRows().get(0).getField(0);

        computeActual("CREATE TABLE test_schema.snapshot_test (id INTEGER)");
        try {
            // Should have created a new snapshot
            MaterializedResult after = computeActual("SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
            long newSnapshot = (long) after.getMaterializedRows().get(0).getField(0);
            assertThat(newSnapshot).isGreaterThan(baselineSnapshot);

            // Check changes contain the table creation
            MaterializedResult changes = computeActual(
                    "SELECT changes_made FROM \"simple_table$snapshot_changes\" WHERE snapshot_id = " + newSnapshot);
            assertThat(changes.getRowCount()).isGreaterThan(0);
            String changeMade = changes.getMaterializedRows().get(0).getField(0).toString();
            // Spec form is `created_table:"schema"."name"` — upstream's ParseCatalogEntry expects
            // both parts quoted and separated by a dot. See dev-docs/COMPARE-pg_ducklake.md B1.
            assertThat(changeMade).contains("created_table:\"test_schema\".\"snapshot_test\"");
        }
        finally {
            tryDropTable("test_schema.snapshot_test");
        }
    }

    @Test
    public void testSchemaVersionsTrackSchemaChangingOperations()
            throws Exception
    {
        String tableName = "schema_versions_track";
        String qualifiedTableName = "test_schema." + tableName;
        try {
            computeActual("CREATE TABLE " + qualifiedTableName + " (id INTEGER, name VARCHAR)");
            long tableId = getActiveTableId(tableName);

            List<SchemaVersionRow> schemaVersionRows = getSchemaVersionRows(tableId);
            assertThat(schemaVersionRows).hasSize(1);
            assertSchemaVersionRowConsistent(schemaVersionRows.getFirst());
            assertThat(schemaVersionRows).allMatch(row -> row.tableId() == tableId);

            long previousSchemaVersion = schemaVersionRows.getFirst().schemaVersion();
            long previousBeginSnapshot = schemaVersionRows.getFirst().beginSnapshot();

            computeActual("ALTER TABLE " + qualifiedTableName + " ADD COLUMN age INTEGER");
            schemaVersionRows = getSchemaVersionRows(tableId);
            assertThat(schemaVersionRows).hasSize(2);
            SchemaVersionRow addColumnRow = schemaVersionRows.getLast();
            assertThat(addColumnRow.schemaVersion()).isGreaterThan(previousSchemaVersion);
            assertThat(addColumnRow.beginSnapshot()).isGreaterThan(previousBeginSnapshot);
            assertSchemaVersionRowConsistent(addColumnRow);

            previousSchemaVersion = addColumnRow.schemaVersion();
            previousBeginSnapshot = addColumnRow.beginSnapshot();

            computeActual("ALTER TABLE " + qualifiedTableName + " RENAME COLUMN name TO full_name");
            schemaVersionRows = getSchemaVersionRows(tableId);
            assertThat(schemaVersionRows).hasSize(3);
            SchemaVersionRow renameColumnRow = schemaVersionRows.getLast();
            assertThat(renameColumnRow.schemaVersion()).isGreaterThan(previousSchemaVersion);
            assertThat(renameColumnRow.beginSnapshot()).isGreaterThan(previousBeginSnapshot);
            assertSchemaVersionRowConsistent(renameColumnRow);

            previousSchemaVersion = renameColumnRow.schemaVersion();
            previousBeginSnapshot = renameColumnRow.beginSnapshot();

            computeActual("ALTER TABLE " + qualifiedTableName + " DROP COLUMN age");
            schemaVersionRows = getSchemaVersionRows(tableId);
            assertThat(schemaVersionRows).hasSize(4);
            SchemaVersionRow dropColumnRow = schemaVersionRows.getLast();
            assertThat(dropColumnRow.schemaVersion()).isGreaterThan(previousSchemaVersion);
            assertThat(dropColumnRow.beginSnapshot()).isGreaterThan(previousBeginSnapshot);
            assertSchemaVersionRowConsistent(dropColumnRow);

            previousSchemaVersion = dropColumnRow.schemaVersion();
            previousBeginSnapshot = dropColumnRow.beginSnapshot();

            computeActual("DROP TABLE " + qualifiedTableName);
            schemaVersionRows = getSchemaVersionRows(tableId);
            assertThat(schemaVersionRows).hasSize(5);
            SchemaVersionRow dropTableRow = schemaVersionRows.getLast();
            assertThat(dropTableRow.schemaVersion()).isGreaterThan(previousSchemaVersion);
            assertThat(dropTableRow.beginSnapshot()).isGreaterThan(previousBeginSnapshot);
            assertSchemaVersionRowConsistent(dropTableRow);
            assertThat(schemaVersionRows).allMatch(row -> row.tableId() == tableId);
        }
        finally {
            tryDropTable(qualifiedTableName);
        }
    }

    @Test
    public void testSchemaVersionsDoNotChangeOnDml()
            throws Exception
    {
        String tableName = "schema_versions_dml";
        String qualifiedTableName = "test_schema." + tableName;
        try {
            computeActual("CREATE TABLE " + qualifiedTableName + " (id INTEGER, metric INTEGER)");
            long tableId = getActiveTableId(tableName);

            List<SchemaVersionRow> initialRows = getSchemaVersionRows(tableId);
            assertThat(initialRows).hasSize(1);
            SchemaVersionRow createTableRow = initialRows.getFirst();
            assertSchemaVersionRowConsistent(createTableRow);

            long schemaVersionBeforeDml = getCurrentSchemaVersion();
            long snapshotBeforeDml = getCurrentSnapshotIdFromCatalog();

            computeActual("INSERT INTO " + qualifiedTableName + " VALUES (1, 10), (2, 20)");
            computeActual("UPDATE " + qualifiedTableName + " SET metric = 110 WHERE id = 1");
            computeActual("DELETE FROM " + qualifiedTableName + " WHERE id = 2");
            computeActual("MERGE INTO " + qualifiedTableName + " t " +
                    "USING (VALUES (1, 111), (3, 300)) s(id, metric) " +
                    "ON (t.id = s.id) " +
                    "WHEN MATCHED THEN UPDATE SET metric = s.metric " +
                    "WHEN NOT MATCHED THEN INSERT (id, metric) VALUES (s.id, s.metric)");

            long schemaVersionAfterDml = getCurrentSchemaVersion();
            long snapshotAfterDml = getCurrentSnapshotIdFromCatalog();

            assertThat(snapshotAfterDml).isGreaterThan(snapshotBeforeDml);
            assertThat(schemaVersionAfterDml).isEqualTo(schemaVersionBeforeDml);

            List<SchemaVersionRow> rowsAfterDml = getSchemaVersionRows(tableId);
            assertThat(rowsAfterDml).containsExactly(createTableRow);
        }
        finally {
            tryDropTable(qualifiedTableName);
        }
    }

    private long getActiveTableId(String tableName)
            throws Exception
    {
        try (Connection connection = openCatalogConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL")) {
            statement.setString(1, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new AssertionError("Missing active table: " + tableName);
                }
                return resultSet.getLong("table_id");
            }
        }
    }

    private List<SchemaVersionRow> getSchemaVersionRows(long tableId)
            throws Exception
    {
        try (Connection connection = openCatalogConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT begin_snapshot, schema_version, table_id " +
                                "FROM ducklake_schema_versions WHERE table_id = ? ORDER BY begin_snapshot")) {
            statement.setLong(1, tableId);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<SchemaVersionRow> rows = new ArrayList<>();
                while (resultSet.next()) {
                    rows.add(new SchemaVersionRow(
                            resultSet.getLong("begin_snapshot"),
                            resultSet.getLong("schema_version"),
                            resultSet.getLong("table_id")));
                }
                return rows;
            }
        }
    }

    private long getCurrentSchemaVersion()
            throws Exception
    {
        try (Connection connection = openCatalogConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT schema_version FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1");
                ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                throw new AssertionError("No snapshots in catalog");
            }
            return resultSet.getLong(1);
        }
    }

    private void assertSchemaVersionRowConsistent(SchemaVersionRow row)
            throws Exception
    {
        try (Connection connection = openCatalogConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = ?")) {
            statement.setLong(1, row.beginSnapshot());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new AssertionError("Missing snapshot for begin_snapshot=" + row.beginSnapshot());
                }
                assertThat(resultSet.getLong("schema_version")).isEqualTo(row.schemaVersion());
            }
        }
    }

    private record SchemaVersionRow(long beginSnapshot, long schemaVersion, long tableId) {}
}
