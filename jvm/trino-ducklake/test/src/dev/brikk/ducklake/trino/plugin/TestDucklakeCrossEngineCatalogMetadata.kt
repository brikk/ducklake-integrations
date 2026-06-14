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

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA_VERSIONS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE
import dev.brikk.ducklake.catalog.testing.CatalogPredicates.activeTableNamed
import dev.brikk.ducklake.catalog.testing.CatalogPredicates.currentlyActive
import dev.brikk.ducklake.catalog.testing.CatalogQueries
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager

/**
 * Catalog-metadata slice of cross-engine compatibility: covers the wire-format contracts that
 * the connector emits into the shared catalog and that DuckDB has to parse without choking.
 *
 *  * `ducklake_snapshot_changes.changes_made` — every change-kind we emit must round-
 *    trip through DuckDB's `ParseChangeType`/`ParseQuotedValue`, including
 *    names with characters that exercise the quoting helpers.
 *  * `ducklake_column.default_value_dialect` — we write SQL NULL on no-user-default
 *    columns; this guards against a regression to writing the string `'duckdb'`
 *    (which would lie about the dialect of a NULL).
 *  * View/schema DDL — view rename folds into `altered_view` (upstream's
 *    `ParseChangeType` has no `RENAMED_*`), and view/schema DDL bumps
 *    `schema_version` so DuckDB readers don't miss writes that bypass table DDL.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
open class TestDucklakeCrossEngineCatalogMetadata
    : AbstractDucklakeCrossEngineTest()
{
    override fun isolatedCatalogName(): String
    {
        return "cross-engine-catalog-metadata"
    }

    // ==================== ducklake_snapshot_changes.changes_made round-trip ====================

    /**
     * Verifies DuckDB's `ducklake_snapshots()` table function — which parses every snapshot's
     * `changes_made` column via `ParseChangesList` + `ParseCatalogEntry` +
     * `ParseQuotedValue` — succeeds on Trino-written snapshots.
     *
     * If we ever regress to writing unquoted values or the pre-spec single-part
     * `created_table:name` form, DuckDB raises `InvalidInputException` and the whole
     * function errors (not just the offending row). This test exercises every kind we currently
     * emit from Trino write paths: `created_schema`, `created_table`,
     * `inserted_into_table`, `deleted_from_table`, `altered_table`,
     * `dropped_table`, `dropped_schema`. See `dev-docs/archive/COMPARE-pg_ducklake.md` B1
     * and `third_party/ducklake/src/storage/ducklake_transaction_changes.cpp`.
     */
    @Test
    @Throws(Exception::class)
    fun testDuckdbParsesTrinoWrittenChangesMadeAcrossAllChangeKinds()
    {
        val schemaName = "xengine_changes_made"
        val tableName = "orders"
        val fullTrino = "$schemaName.$tableName"

        try {
            computeActual("CREATE SCHEMA $schemaName")
            computeActual("CREATE TABLE $fullTrino (id INTEGER, note VARCHAR)")
            computeActual("INSERT INTO $fullTrino VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            computeActual("ALTER TABLE $fullTrino ADD COLUMN qty INTEGER")
            computeActual("DELETE FROM $fullTrino WHERE id = 2")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id").use { rs ->
                        // Parsing alone proves acceptability — any malformed entry makes the whole
                        // function throw. Still, assert we can traverse every row without error.
                        var rowCount = 0
                        var sawCreatedSchema = false
                        var sawCreatedTable = false
                        var sawInserted = false
                        var sawAltered = false
                        var sawDeleted = false
                        while (rs.next()) {
                            rowCount++
                            // Force materialization of the MAP<VARCHAR, LIST<VARCHAR>> — that is where
                            // ParseChangesMade() runs inside DuckDB's Bind path, but we also read the
                            // value here to catch any lazy-decode surprises.
                            val changes = rs.getObject("changes") ?: continue
                            val changesText = changes.toString()
                            if (changesText.contains("schemas_created") && changesText.contains(schemaName)) {
                                sawCreatedSchema = true
                            }
                            if (changesText.contains("tables_created") && changesText.contains(tableName)) {
                                sawCreatedTable = true
                            }
                            if (changesText.contains("tables_inserted_into")) {
                                sawInserted = true
                            }
                            if (changesText.contains("tables_altered")) {
                                sawAltered = true
                            }
                            if (changesText.contains("tables_deleted_from")) {
                                sawDeleted = true
                            }
                        }
                        assertThat(rowCount).`as`("ducklake_snapshots row count").isGreaterThanOrEqualTo(5)
                        assertThat(sawCreatedSchema).`as`("DuckDB parsed a `created_schema` entry").isTrue()
                        assertThat(sawCreatedTable).`as`("DuckDB parsed a `created_table` entry").isTrue()
                        assertThat(sawInserted).`as`("DuckDB parsed a `inserted_into_table` entry").isTrue()
                        assertThat(sawAltered).`as`("DuckDB parsed a `altered_table` entry").isTrue()
                        assertThat(sawDeleted).`as`("DuckDB parsed a `deleted_from_table` entry").isTrue()
                    }
                }
            }

            // Drop table + schema to cover the `dropped_*` emissions too. Those are numeric and
            // already spec-conformant; asserting the parse still succeeds guards against future
            // regressions to the quoting helpers.
            computeActual("DROP TABLE $fullTrino")
            computeActual("DROP SCHEMA $schemaName")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT changes FROM ducklake_snapshots('ducklake_db') " +
                                    "ORDER BY snapshot_id DESC LIMIT 5").use { rs ->
                        while (rs.next()) {
                            rs.getObject("changes")
                        }
                    }
                }
            }
        }
        finally {
            tryDropTable(fullTrino)
            try {
                computeActual("DROP SCHEMA $schemaName")
            }
            catch (ignored: Exception) {
            }
        }
    }

    /**
     * Exercises the quoting path: names containing characters (comma, double quote) that would
     * break any naive `String.join(",", ...)` serializer. DuckDB must still parse the
     * snapshot cleanly via `ParseQuotedValue`, which unescapes `""` back to `"`.
     */
    @Test
    @Throws(Exception::class)
    fun testDuckdbParsesTrinoWrittenChangesMadeWithPathologicalNames()
    {
        // Trino's identifier parser rejects embedded double quotes in CREATE TABLE identifiers,
        // but commas are legal (they just need double-quoting in SQL). Use a comma to exercise
        // the non-trivial quoting path without running into SQL-parser issues.
        val schemaName = "xengine,weird_schema"
        val tableName = "table,with,commas"
        val fullTrino = "\"" + schemaName + "\".\"" + tableName + "\""

        try {
            computeActual("CREATE SCHEMA \"" + schemaName + "\"")
            computeActual("CREATE TABLE $fullTrino (id INTEGER)")
            computeActual("INSERT INTO $fullTrino VALUES (1)")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id").use { rs ->
                        var sawQuotedSchema = false
                        var sawQuotedTable = false
                        while (rs.next()) {
                            val changes = rs.getObject("changes") ?: continue
                            val text = changes.toString()
                            // DuckDB's parsed MAP round-trips the unquoted schema/table names through
                            // CatalogListToValue -> KeywordHelper::WriteOptionallyQuoted, so a name with
                            // commas comes back wrapped in double quotes like "xengine,weird_schema".
                            if (text.contains(schemaName)) {
                                sawQuotedSchema = true
                            }
                            if (text.contains(tableName)) {
                                sawQuotedTable = true
                            }
                        }
                        assertThat(sawQuotedSchema).`as`("DuckDB saw the comma-bearing schema name").isTrue()
                        assertThat(sawQuotedTable).`as`("DuckDB saw the comma-bearing table name").isTrue()
                    }
                }
            }
        }
        finally {
            tryDropTable(fullTrino)
            try {
                computeActual("DROP SCHEMA \"" + schemaName + "\"")
            }
            catch (ignored: Exception) {
            }
        }
    }

    // ==================== default_value_dialect NULL round-trip ====================

    /**
     * Pins our policy on `ducklake_column.default_value_dialect`: we write SQL NULL
     * when there is no user-defined default (the common case — every column we write today).
     * The spec (`ducklake_column.md:36`) says the dialect is "especially useful for
     * expressions"; upstream's own migration (`ducklake_metadata_manager.cpp:297`) adds the
     * column with `DEFAULT NULL`, and upstream's ducklake extension never reads the field
     * anywhere in its C++ source. This test confirms DuckDB can read a Trino-written table
     * with NULL there without barfing, via both a normal `SELECT` (which loads column
     * metadata) and `DESCRIBE` (which exercises the full type-and-default-aware path).
     */
    @Test
    @Throws(Exception::class)
    fun testDuckdbReadsTrinoTableWithNullDefaultValueDialect()
    {
        val tableName = "xengine_null_dialect"
        val fullTrino = "test_schema.$tableName"
        val fullDuckdb = "ducklake_db.test_schema.$tableName"

        try {
            computeActual("CREATE TABLE $fullTrino (id INTEGER, name VARCHAR, amount DOUBLE)")
            computeActual("INSERT INTO $fullTrino VALUES (1, 'alice', 1.5), (2, 'bob', 2.5)")

            // Confirm we wrote SQL NULL (not the string 'NULL') to default_value_dialect.
            val catalog = getIsolatedCatalog()
            DriverManager.getConnection(
                    catalog.jdbcUrl, catalog.user, catalog.password).use { pgConn ->
                val dsl = CatalogTestSupport.dsl(pgConn)
                val col = DUCKLAKE_COLUMN.`as`("col")
                val tab = DUCKLAKE_TABLE.`as`("tab")
                val defaults = dsl.select(col.DEFAULT_VALUE, col.DEFAULT_VALUE_TYPE, col.DEFAULT_VALUE_DIALECT)
                        .from(col)
                        .join(tab).on(col.TABLE_ID.eq(tab.TABLE_ID))
                        .where(activeTableNamed(tab, tableName)
                                .and(currentlyActive(col.END_SNAPSHOT)))
                        .fetch()
                assertThat(defaults).`as`("expected 3 active columns").hasSize(3)
                for (r in defaults) {
                    assertThat(r.get(col.DEFAULT_VALUE))
                            .`as`("no-user-default column should carry the 'NULL' string sentinel")
                            .isEqualTo("NULL")
                    assertThat(r.get(col.DEFAULT_VALUE_TYPE))
                            .`as`("default_value_type should remain 'literal' (spec-preferred)")
                            .isEqualTo("literal")
                    assertThat(r.get(col.DEFAULT_VALUE_DIALECT))
                            .`as`("default_value_dialect should be SQL NULL when we don't touch the default")
                            .isNull()
                }
            }

            // Normal read — must survive the NULL dialect.
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT id, name, amount FROM $fullDuckdb ORDER BY id").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(1)
                        assertThat(rs.getString("name")).isEqualTo("alice")
                        assertThat(rs.getDouble("amount")).isEqualTo(1.5)
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(2)
                        assertThat(rs.next()).isFalse()
                    }
                }
            }

            // DESCRIBE path — separately exercises upstream's column-metadata materialization.
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.executeQuery("DESCRIBE $fullDuckdb").use { rs ->
                        val columnNames = ArrayList<String>()
                        while (rs.next()) {
                            columnNames.add(rs.getString(1))
                        }
                        assertThat(columnNames).containsExactly("id", "name", "amount")
                    }
                }
            }
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    // ==================== View/schema DDL cross-engine parse + schema_version bump ====================

    /**
     * Upstream's `ParseChangeType` (in
     * `third_party/ducklake/src/storage/ducklake_transaction_changes.cpp`) enumerates
     * `created_view` / `altered_view` / `dropped_view` but no
     * `RENAMED_*`. An earlier revision of this connector emitted
     * `renamed_view:<viewId>` on `ALTER VIEW ... RENAME`, which made any later
     * call to DuckDB's `ducklake_snapshots()` throw
     * `InvalidInputException: Unsupported change type renamed_view` for any snapshot
     * that ever renamed a view. A rename is semantically a schema/name change, so the
     * current code emits `altered_view` and bumps `schema_version` (upstream
     * `SchemaChangesMade()` flips on dropped/new view entries). This test exercises
     * the full view-DDL surface (create, rename, replace, drop) plus schema DDL
     * (create/drop) and confirms DuckDB parses every snapshot cleanly.
     */
    @Test
    @Throws(Exception::class)
    fun testDuckdbParsesTrinoWrittenViewAndSchemaDdlChanges()
    {
        val schemaName = "xengine_view_changes"

        try {
            computeActual("CREATE SCHEMA $schemaName")
            computeActual("CREATE VIEW $schemaName.v_src AS SELECT id FROM test_schema.simple_table")
            // Fully qualify RENAME TO — Trino resolves an unqualified target against the
            // session default schema (test_schema), not the source view's schema.
            computeActual("ALTER VIEW $schemaName.v_src RENAME TO $schemaName.v_dst")
            computeActual("CREATE OR REPLACE VIEW $schemaName.v_dst AS SELECT id, name FROM test_schema.simple_table")
            computeActual("DROP VIEW $schemaName.v_dst")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id").use { rs ->
                        var rowCount = 0
                        var sawAlteredView = false
                        var sawDroppedView = false
                        while (rs.next()) {
                            rowCount++
                            val changes = rs.getObject("changes") ?: continue
                            val text = changes.toString()
                            if (text.contains("views_altered")) {
                                sawAlteredView = true
                            }
                            if (text.contains("views_dropped")) {
                                sawDroppedView = true
                            }
                        }
                        assertThat(rowCount).`as`("ducklake_snapshots row count").isGreaterThanOrEqualTo(5)
                        assertThat(sawAlteredView)
                                .`as`("DuckDB parsed at least one `altered_view` entry (rename + replace both map here)")
                                .isTrue()
                        assertThat(sawDroppedView).`as`("DuckDB parsed a `dropped_view` entry").isTrue()
                    }
                }
            }
        }
        finally {
            try {
                computeActual("DROP VIEW $schemaName.v_dst")
            }
            catch (ignored: Exception) {
            }
            try {
                computeActual("DROP VIEW $schemaName.v_src")
            }
            catch (ignored: Exception) {
            }
            try {
                computeActual("DROP SCHEMA $schemaName")
            }
            catch (ignored: Exception) {
            }
        }
    }

    /**
     * Upstream `DuckLakeTransaction::SchemaChangesMade()` flips on new/dropped view
     * entries and on new/dropped schema entries — the same trigger used for table DDL.
     * A DuckDB reader that caches catalog state keyed on `schema_version` must see
     * a bump when Trino creates/drops/replaces/renames a view or creates/drops a schema.
     * Before this was wired up, a Trino-only sequence of view + schema DDL would leave
     * `ducklake_snapshot.schema_version` frozen — DuckDB's next query would hit
     * stale cache and miss those objects. This test walks the full DDL surface and asserts
     * the counter advances on every step.
     *
     * Surface covered:
     *  * `CREATE SCHEMA` → `createSchema`
     *  * `CREATE VIEW` → `createView`
     *  * `COMMENT ON VIEW` → `replaceViewMetadata`
     *  * `ALTER VIEW ... RENAME TO` → `renameView`
     *  * `DROP VIEW` → `dropView`
     *  * `DROP SCHEMA` → `dropSchema`
     */
    @Test
    @Throws(Exception::class)
    fun testSchemaVersionBumpsOnViewAndSchemaDdl()
    {
        val schemaName = "xengine_sv_bumps"
        val catalog = getIsolatedCatalog()

        try {
            val before = readCurrentSchemaVersion(catalog)

            computeActual("CREATE SCHEMA $schemaName")
            val afterCreateSchema = readCurrentSchemaVersion(catalog)
            assertThat(afterCreateSchema).`as`("CREATE SCHEMA bumps schema_version").isGreaterThan(before)

            computeActual("CREATE VIEW $schemaName.v AS SELECT id FROM test_schema.simple_table")
            val afterCreateView = readCurrentSchemaVersion(catalog)
            assertThat(afterCreateView).`as`("CREATE VIEW bumps schema_version").isGreaterThan(afterCreateSchema)

            // Note: Trino resolves an unqualified `RENAME TO <name>` against the session's
            // default schema, not the source view's schema. Fully qualify the target to
            // keep the rename scoped to our test schema.
            computeActual("ALTER VIEW $schemaName.v RENAME TO $schemaName.v2")
            val afterRename = readCurrentSchemaVersion(catalog)
            assertThat(afterRename).`as`("ALTER VIEW ... RENAME bumps schema_version").isGreaterThan(afterCreateView)

            // COMMENT ON VIEW dispatches to replaceViewMetadata (the only SQL path that does —
            // CREATE OR REPLACE VIEW goes through dropView + createView in DucklakeMetadata).
            computeActual("COMMENT ON VIEW $schemaName.v2 IS 'a view with a comment'")
            val afterComment = readCurrentSchemaVersion(catalog)
            assertThat(afterComment).`as`("COMMENT ON VIEW (replaceViewMetadata) bumps schema_version").isGreaterThan(afterRename)

            computeActual("DROP VIEW $schemaName.v2")
            val afterDropView = readCurrentSchemaVersion(catalog)
            assertThat(afterDropView).`as`("DROP VIEW bumps schema_version").isGreaterThan(afterComment)

            computeActual("DROP SCHEMA $schemaName")
            val afterDropSchema = readCurrentSchemaVersion(catalog)
            assertThat(afterDropSchema).`as`("DROP SCHEMA bumps schema_version").isGreaterThan(afterDropView)

            // Non-table-scoped bumps must land in ducklake_schema_versions with table_id = NULL
            // (spec ducklake_schema_versions.md: table_id is BIGINT, nullable). A future DuckDB
            // version that reads per-table snapshots via this table still needs the broadcast
            // bump, which is keyed on table_id IS NULL.
            val nonTableScopedRows = countSchemaVersionRowsWithNullTableId(catalog, before)
            assertThat(nonTableScopedRows)
                    .`as`("expected one schema_versions row with NULL table_id per view/schema DDL (6 total)")
                    .isGreaterThanOrEqualTo(6)
        }
        finally {
            try {
                computeActual("DROP VIEW $schemaName.v2")
            }
            catch (ignored: Exception) {
            }
            try {
                computeActual("DROP VIEW $schemaName.v")
            }
            catch (ignored: Exception) {
            }
            try {
                computeActual("DROP SCHEMA $schemaName")
            }
            catch (ignored: Exception) {
            }
        }
    }

    @Throws(Exception::class)
    private fun readCurrentSchemaVersion(catalog: DucklakeCatalogGenerator.IsolatedCatalog): Long
    {
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { pgConn ->
            return CatalogQueries.currentSchemaVersion(CatalogTestSupport.dsl(pgConn))
        }
    }

    @Throws(Exception::class)
    private fun countSchemaVersionRowsWithNullTableId(
            catalog: DucklakeCatalogGenerator.IsolatedCatalog,
            sinceExclusiveSchemaVersion: Long): Long
    {
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { pgConn ->
            val dsl = CatalogTestSupport.dsl(pgConn)
            // Schema-version rows with table_id IS NULL track view/schema-level DDL events
            // (CREATE VIEW, DROP SCHEMA, etc.) — the predicate isn't generic enough to live
            // in CatalogQueries, so it's expressed inline against the generated columns.
            val schver = DUCKLAKE_SCHEMA_VERSIONS.`as`("schver")
            val count = dsl.selectCount()
                    .from(schver)
                    .where(schver.TABLE_ID.isNull
                            .and(schver.SCHEMA_VERSION.gt(sinceExclusiveSchemaVersion)))
                    .fetchOne(0, Int::class.javaObjectType)
            return count?.toLong() ?: 0L
        }
    }
}
