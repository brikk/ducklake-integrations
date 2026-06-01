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
import dev.brikk.ducklake.catalog.testing.CatalogQueries.SchemaVersionRow
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Pins the contract between DDL/DML and the catalog's snapshot lineage:
 *
 *
 *  *
 * `ducklake_snapshot_changes` captures every DDL operation in spec form
 * (`created_table:"schema"."name"`). DuckDB's `ParseCatalogEntry` relies
 * on the quoted, dotted form — see dev-docs/COMPARE-pg_ducklake.md B1.
 *  *
 * `ducklake_schema_versions` advances on every schema-changing DDL and stays
 * fixed across pure-DML INSERT/UPDATE/DELETE/MERGE. `schema_version` is
 * table-scoped to `table_id`; the same row appears in
 * `ducklake_snapshot.schema_version`.
 *
 *
 * Exercises directly against the catalog DB (via [openCatalogConnection]) because
 * these tables aren't projected through `information_schema`.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeSnapshotAndSchemaVersion
        : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "ddl-snapshot-and-schema-version"
    }

    @Test
    fun testSnapshotChangesTracked() {
        // Get baseline snapshot count
        val before = computeActual("SELECT max(snapshot_id) FROM \"simple_table\$snapshots\"")
        val baselineSnapshot = before.materializedRows[0].getField(0) as Long

        computeActual("CREATE TABLE test_schema.snapshot_test (id INTEGER)")
        try {
            // Should have created a new snapshot
            val after = computeActual("SELECT max(snapshot_id) FROM \"simple_table\$snapshots\"")
            val newSnapshot = after.materializedRows[0].getField(0) as Long
            assertThat(newSnapshot).isGreaterThan(baselineSnapshot)

            // Check changes contain the table creation
            val changes = computeActual(
                    "SELECT changes_made FROM \"simple_table\$snapshot_changes\" WHERE snapshot_id = $newSnapshot")
            assertThat(changes.rowCount).isGreaterThan(0)
            val changeMade = changes.materializedRows[0].getField(0).toString()
            // Spec form is `created_table:"schema"."name"` — upstream's ParseCatalogEntry expects
            // both parts quoted and separated by a dot. See dev-docs/COMPARE-pg_ducklake.md B1.
            assertThat(changeMade).contains("created_table:\"test_schema\".\"snapshot_test\"")
        }
        finally {
            tryDropTable("test_schema.snapshot_test")
        }
    }

    @Test
    fun testSchemaVersionsTrackSchemaChangingOperations() {
        val tableName = "schema_versions_track"
        val qualifiedTableName = "test_schema.$tableName"
        try {
            computeActual("CREATE TABLE $qualifiedTableName (id INTEGER, name VARCHAR)")
            val tableId = getActiveTableId(tableName)

            var schemaVersionRows = getSchemaVersionRows(tableId)
            assertThat(schemaVersionRows).hasSize(1)
            assertSchemaVersionRowConsistent(schemaVersionRows.first())
            assertThat(schemaVersionRows).allMatch { row -> row.tableId == tableId }

            var previousSchemaVersion = schemaVersionRows.first().schemaVersion
            var previousBeginSnapshot = schemaVersionRows.first().beginSnapshot

            computeActual("ALTER TABLE $qualifiedTableName ADD COLUMN age INTEGER")
            schemaVersionRows = getSchemaVersionRows(tableId)
            assertThat(schemaVersionRows).hasSize(2)
            val addColumnRow = schemaVersionRows.last()
            assertThat(addColumnRow.schemaVersion).isGreaterThan(previousSchemaVersion)
            assertThat(addColumnRow.beginSnapshot).isGreaterThan(previousBeginSnapshot)
            assertSchemaVersionRowConsistent(addColumnRow)

            previousSchemaVersion = addColumnRow.schemaVersion
            previousBeginSnapshot = addColumnRow.beginSnapshot

            computeActual("ALTER TABLE $qualifiedTableName RENAME COLUMN name TO full_name")
            schemaVersionRows = getSchemaVersionRows(tableId)
            assertThat(schemaVersionRows).hasSize(3)
            val renameColumnRow = schemaVersionRows.last()
            assertThat(renameColumnRow.schemaVersion).isGreaterThan(previousSchemaVersion)
            assertThat(renameColumnRow.beginSnapshot).isGreaterThan(previousBeginSnapshot)
            assertSchemaVersionRowConsistent(renameColumnRow)

            previousSchemaVersion = renameColumnRow.schemaVersion
            previousBeginSnapshot = renameColumnRow.beginSnapshot

            computeActual("ALTER TABLE $qualifiedTableName DROP COLUMN age")
            schemaVersionRows = getSchemaVersionRows(tableId)
            assertThat(schemaVersionRows).hasSize(4)
            val dropColumnRow = schemaVersionRows.last()
            assertThat(dropColumnRow.schemaVersion).isGreaterThan(previousSchemaVersion)
            assertThat(dropColumnRow.beginSnapshot).isGreaterThan(previousBeginSnapshot)
            assertSchemaVersionRowConsistent(dropColumnRow)

            previousSchemaVersion = dropColumnRow.schemaVersion
            previousBeginSnapshot = dropColumnRow.beginSnapshot

            computeActual("DROP TABLE $qualifiedTableName")
            schemaVersionRows = getSchemaVersionRows(tableId)
            assertThat(schemaVersionRows).hasSize(5)
            val dropTableRow = schemaVersionRows.last()
            assertThat(dropTableRow.schemaVersion).isGreaterThan(previousSchemaVersion)
            assertThat(dropTableRow.beginSnapshot).isGreaterThan(previousBeginSnapshot)
            assertSchemaVersionRowConsistent(dropTableRow)
            assertThat(schemaVersionRows).allMatch { row -> row.tableId == tableId }
        }
        finally {
            tryDropTable(qualifiedTableName)
        }
    }

    @Test
    fun testSchemaVersionsDoNotChangeOnDml() {
        val tableName = "schema_versions_dml"
        val qualifiedTableName = "test_schema.$tableName"
        try {
            computeActual("CREATE TABLE $qualifiedTableName (id INTEGER, metric INTEGER)")
            val tableId = getActiveTableId(tableName)

            val initialRows = getSchemaVersionRows(tableId)
            assertThat(initialRows).hasSize(1)
            val createTableRow = initialRows.first()
            assertSchemaVersionRowConsistent(createTableRow)

            val schemaVersionBeforeDml = getCurrentSchemaVersion()
            val snapshotBeforeDml = getCurrentSnapshotIdFromCatalog()

            computeActual("INSERT INTO $qualifiedTableName VALUES (1, 10), (2, 20)")
            computeActual("UPDATE $qualifiedTableName SET metric = 110 WHERE id = 1")
            computeActual("DELETE FROM $qualifiedTableName WHERE id = 2")
            computeActual("MERGE INTO $qualifiedTableName t " +
                    "USING (VALUES (1, 111), (3, 300)) s(id, metric) " +
                    "ON (t.id = s.id) " +
                    "WHEN MATCHED THEN UPDATE SET metric = s.metric " +
                    "WHEN NOT MATCHED THEN INSERT (id, metric) VALUES (s.id, s.metric)")

            val schemaVersionAfterDml = getCurrentSchemaVersion()
            val snapshotAfterDml = getCurrentSnapshotIdFromCatalog()

            assertThat(snapshotAfterDml).isGreaterThan(snapshotBeforeDml)
            assertThat(schemaVersionAfterDml).isEqualTo(schemaVersionBeforeDml)

            val rowsAfterDml = getSchemaVersionRows(tableId)
            assertThat(rowsAfterDml).containsExactly(createTableRow)
        }
        finally {
            tryDropTable(qualifiedTableName)
        }
    }

    private fun getActiveTableId(tableName: String): Long {
        openCatalogConnection().use { connection ->
            return CatalogQueries.activeTableId(CatalogTestSupport.dsl(connection), tableName)
        }
    }

    private fun getSchemaVersionRows(tableId: Long): List<SchemaVersionRow> {
        openCatalogConnection().use { connection ->
            return CatalogQueries.schemaVersionsByTable(CatalogTestSupport.dsl(connection), tableId)
        }
    }

    private fun getCurrentSchemaVersion(): Long {
        openCatalogConnection().use { connection ->
            return CatalogQueries.currentSchemaVersion(CatalogTestSupport.dsl(connection))
        }
    }

    private fun assertSchemaVersionRowConsistent(row: SchemaVersionRow) {
        openCatalogConnection().use { connection ->
            val dsl = CatalogTestSupport.dsl(connection)
            val observed = CatalogQueries.snapshotSchemaVersion(dsl, row.beginSnapshot)
                    .orElseThrow {
                        AssertionError("Missing snapshot for begin_snapshot=" + row.beginSnapshot)
                    }
            assertThat(observed).isEqualTo(row.schemaVersion)
        }
    }
}
