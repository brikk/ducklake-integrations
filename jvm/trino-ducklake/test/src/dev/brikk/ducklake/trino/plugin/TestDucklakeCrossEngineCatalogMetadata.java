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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Catalog-metadata slice of cross-engine compatibility: covers the wire-format contracts that
 * the connector emits into the shared catalog and that DuckDB has to parse without choking.
 *
 * <ul>
 *   <li>{@code ducklake_snapshot_changes.changes_made} — every change-kind we emit must round-
 *       trip through DuckDB's {@code ParseChangeType}/{@code ParseQuotedValue}, including
 *       names with characters that exercise the quoting helpers.</li>
 *   <li>{@code ducklake_column.default_value_dialect} — we write SQL NULL on no-user-default
 *       columns; this guards against a regression to writing the string {@code 'duckdb'}
 *       (which would lie about the dialect of a NULL).</li>
 *   <li>View/schema DDL — view rename folds into {@code altered_view} (upstream's
 *       {@code ParseChangeType} has no {@code RENAMED_*}), and view/schema DDL bumps
 *       {@code schema_version} so DuckDB readers don't miss writes that bypass table DDL.</li>
 * </ul>
 */
@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeCrossEngineCatalogMetadata
        extends AbstractDucklakeCrossEngineTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "cross-engine-catalog-metadata";
    }

    // ==================== ducklake_snapshot_changes.changes_made round-trip ====================

    /**
     * Verifies DuckDB's {@code ducklake_snapshots()} table function — which parses every snapshot's
     * {@code changes_made} column via {@code ParseChangesList} + {@code ParseCatalogEntry} +
     * {@code ParseQuotedValue} — succeeds on Trino-written snapshots.
     * <p>
     * If we ever regress to writing unquoted values or the pre-spec single-part
     * {@code created_table:name} form, DuckDB raises {@code InvalidInputException} and the whole
     * function errors (not just the offending row). This test exercises every kind we currently
     * emit from Trino write paths: {@code created_schema}, {@code created_table},
     * {@code inserted_into_table}, {@code deleted_from_table}, {@code altered_table},
     * {@code dropped_table}, {@code dropped_schema}. See {@code dev-docs/COMPARE-pg_ducklake.md} B1
     * and {@code third_party/ducklake/src/storage/ducklake_transaction_changes.cpp}.
     */
    @Test
    public void testDuckdbParsesTrinoWrittenChangesMadeAcrossAllChangeKinds()
            throws Exception
    {
        String schemaName = "xengine_changes_made";
        String tableName = "orders";
        String fullTrino = schemaName + "." + tableName;

        try {
            computeActual("CREATE SCHEMA " + schemaName);
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER, note VARCHAR)");
            computeActual("INSERT INTO " + fullTrino + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            computeActual("ALTER TABLE " + fullTrino + " ADD COLUMN qty INTEGER");
            computeActual("DELETE FROM " + fullTrino + " WHERE id = 2");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id")) {
                // Parsing alone proves acceptability — any malformed entry makes the whole
                // function throw. Still, assert we can traverse every row without error.
                int rowCount = 0;
                boolean sawCreatedSchema = false;
                boolean sawCreatedTable = false;
                boolean sawInserted = false;
                boolean sawAltered = false;
                boolean sawDeleted = false;
                while (rs.next()) {
                    rowCount++;
                    // Force materialization of the MAP<VARCHAR, LIST<VARCHAR>> — that is where
                    // ParseChangesMade() runs inside DuckDB's Bind path, but we also read the
                    // value here to catch any lazy-decode surprises.
                    Object changes = rs.getObject("changes");
                    if (changes == null) {
                        continue;
                    }
                    String changesText = changes.toString();
                    if (changesText.contains("schemas_created") && changesText.contains(schemaName)) {
                        sawCreatedSchema = true;
                    }
                    if (changesText.contains("tables_created") && changesText.contains(tableName)) {
                        sawCreatedTable = true;
                    }
                    if (changesText.contains("tables_inserted_into")) {
                        sawInserted = true;
                    }
                    if (changesText.contains("tables_altered")) {
                        sawAltered = true;
                    }
                    if (changesText.contains("tables_deleted_from")) {
                        sawDeleted = true;
                    }
                }
                assertThat(rowCount).as("ducklake_snapshots row count").isGreaterThanOrEqualTo(5);
                assertThat(sawCreatedSchema).as("DuckDB parsed a `created_schema` entry").isTrue();
                assertThat(sawCreatedTable).as("DuckDB parsed a `created_table` entry").isTrue();
                assertThat(sawInserted).as("DuckDB parsed a `inserted_into_table` entry").isTrue();
                assertThat(sawAltered).as("DuckDB parsed a `altered_table` entry").isTrue();
                assertThat(sawDeleted).as("DuckDB parsed a `deleted_from_table` entry").isTrue();
            }

            // Drop table + schema to cover the `dropped_*` emissions too. Those are numeric and
            // already spec-conformant; asserting the parse still succeeds guards against future
            // regressions to the quoting helpers.
            computeActual("DROP TABLE " + fullTrino);
            computeActual("DROP SCHEMA " + schemaName);

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT changes FROM ducklake_snapshots('ducklake_db') " +
                                    "ORDER BY snapshot_id DESC LIMIT 5")) {
                while (rs.next()) {
                    rs.getObject("changes");
                }
            }
        }
        finally {
            tryDropTable(fullTrino);
            try {
                computeActual("DROP SCHEMA " + schemaName);
            }
            catch (Exception ignored) {
            }
        }
    }

    /**
     * Exercises the quoting path: names containing characters (comma, double quote) that would
     * break any naive {@code String.join(",", ...)} serializer. DuckDB must still parse the
     * snapshot cleanly via {@code ParseQuotedValue}, which unescapes {@code ""} back to {@code "}.
     */
    @Test
    public void testDuckdbParsesTrinoWrittenChangesMadeWithPathologicalNames()
            throws Exception
    {
        // Trino's identifier parser rejects embedded double quotes in CREATE TABLE identifiers,
        // but commas are legal (they just need double-quoting in SQL). Use a comma to exercise
        // the non-trivial quoting path without running into SQL-parser issues.
        String schemaName = "xengine,weird_schema";
        String tableName = "table,with,commas";
        String fullTrino = "\"" + schemaName + "\".\"" + tableName + "\"";

        try {
            computeActual("CREATE SCHEMA \"" + schemaName + "\"");
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER)");
            computeActual("INSERT INTO " + fullTrino + " VALUES (1)");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id")) {
                boolean sawQuotedSchema = false;
                boolean sawQuotedTable = false;
                while (rs.next()) {
                    Object changes = rs.getObject("changes");
                    if (changes == null) {
                        continue;
                    }
                    String text = changes.toString();
                    // DuckDB's parsed MAP round-trips the unquoted schema/table names through
                    // CatalogListToValue -> KeywordHelper::WriteOptionallyQuoted, so a name with
                    // commas comes back wrapped in double quotes like "xengine,weird_schema".
                    if (text.contains(schemaName)) {
                        sawQuotedSchema = true;
                    }
                    if (text.contains(tableName)) {
                        sawQuotedTable = true;
                    }
                }
                assertThat(sawQuotedSchema).as("DuckDB saw the comma-bearing schema name").isTrue();
                assertThat(sawQuotedTable).as("DuckDB saw the comma-bearing table name").isTrue();
            }
        }
        finally {
            tryDropTable(fullTrino);
            try {
                computeActual("DROP SCHEMA \"" + schemaName + "\"");
            }
            catch (Exception ignored) {
            }
        }
    }

    // ==================== default_value_dialect NULL round-trip ====================

    /**
     * Pins our policy on {@code ducklake_column.default_value_dialect}: we write SQL NULL
     * when there is no user-defined default (the common case — every column we write today).
     * The spec (`ducklake_column.md:36`) says the dialect is "especially useful for
     * expressions"; upstream's own migration (`ducklake_metadata_manager.cpp:297`) adds the
     * column with `DEFAULT NULL`, and upstream's ducklake extension never reads the field
     * anywhere in its C++ source. This test confirms DuckDB can read a Trino-written table
     * with NULL there without barfing, via both a normal `SELECT` (which loads column
     * metadata) and `DESCRIBE` (which exercises the full type-and-default-aware path).
     */
    @Test
    public void testDuckdbReadsTrinoTableWithNullDefaultValueDialect()
            throws Exception
    {
        String tableName = "xengine_null_dialect";
        String fullTrino = "test_schema." + tableName;
        String fullDuckdb = "ducklake_db.test_schema." + tableName;

        try {
            computeActual("CREATE TABLE " + fullTrino + " (id INTEGER, name VARCHAR, amount DOUBLE)");
            computeActual("INSERT INTO " + fullTrino + " VALUES (1, 'alice', 1.5), (2, 'bob', 2.5)");

            // Confirm we wrote SQL NULL (not the string 'NULL') to default_value_dialect.
            DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();
            try (Connection pgConn = DriverManager.getConnection(
                    catalog.jdbcUrl(), catalog.user(), catalog.password());
                    Statement pgStmt = pgConn.createStatement();
                    ResultSet rs = pgStmt.executeQuery(
                            "SELECT default_value, default_value_type, default_value_dialect " +
                                    "FROM ducklake_column c JOIN ducklake_table t USING (table_id) " +
                                    "WHERE t.table_name = '" + tableName + "' AND t.end_snapshot IS NULL " +
                                    "  AND c.end_snapshot IS NULL")) {
                int columnCount = 0;
                while (rs.next()) {
                    columnCount++;
                    assertThat(rs.getString("default_value"))
                            .as("no-user-default column should carry the 'NULL' string sentinel")
                            .isEqualTo("NULL");
                    assertThat(rs.getString("default_value_type"))
                            .as("default_value_type should remain 'literal' (spec-preferred)")
                            .isEqualTo("literal");
                    rs.getString("default_value_dialect");
                    assertThat(rs.wasNull())
                            .as("default_value_dialect should be SQL NULL when we don't touch the default")
                            .isTrue();
                }
                assertThat(columnCount).as("expected 3 active columns").isEqualTo(3);
            }

            // Normal read — must survive the NULL dialect.
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id, name, amount FROM " + fullDuckdb + " ORDER BY id")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(1);
                assertThat(rs.getString("name")).isEqualTo("alice");
                assertThat(rs.getDouble("amount")).isEqualTo(1.5);
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("id")).isEqualTo(2);
                assertThat(rs.next()).isFalse();
            }

            // DESCRIBE path — separately exercises upstream's column-metadata materialization.
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement();
                    ResultSet rs = stmt.executeQuery("DESCRIBE " + fullDuckdb)) {
                List<String> columnNames = new ArrayList<>();
                while (rs.next()) {
                    columnNames.add(rs.getString(1));
                }
                assertThat(columnNames).containsExactly("id", "name", "amount");
            }
        }
        finally {
            tryDropTable(fullTrino);
        }
    }

    // ==================== View/schema DDL cross-engine parse + schema_version bump ====================

    /**
     * Upstream's {@code ParseChangeType} (in
     * {@code third_party/ducklake/src/storage/ducklake_transaction_changes.cpp}) enumerates
     * {@code created_view} / {@code altered_view} / {@code dropped_view} but no
     * {@code RENAMED_*}. An earlier revision of this connector emitted
     * {@code renamed_view:<viewId>} on {@code ALTER VIEW ... RENAME}, which made any later
     * call to DuckDB's {@code ducklake_snapshots()} throw
     * {@code InvalidInputException: Unsupported change type renamed_view} for any snapshot
     * that ever renamed a view. A rename is semantically a schema/name change, so the
     * current code emits {@code altered_view} and bumps {@code schema_version} (upstream
     * {@code SchemaChangesMade()} flips on dropped/new view entries). This test exercises
     * the full view-DDL surface (create, rename, replace, drop) plus schema DDL
     * (create/drop) and confirms DuckDB parses every snapshot cleanly.
     */
    @Test
    public void testDuckdbParsesTrinoWrittenViewAndSchemaDdlChanges()
            throws Exception
    {
        String schemaName = "xengine_view_changes";

        try {
            computeActual("CREATE SCHEMA " + schemaName);
            computeActual("CREATE VIEW " + schemaName + ".v_src AS SELECT id FROM test_schema.simple_table");
            // Fully qualify RENAME TO — Trino resolves an unqualified target against the
            // session default schema (test_schema), not the source view's schema.
            computeActual("ALTER VIEW " + schemaName + ".v_src RENAME TO " + schemaName + ".v_dst");
            computeActual("CREATE OR REPLACE VIEW " + schemaName + ".v_dst AS SELECT id, name FROM test_schema.simple_table");
            computeActual("DROP VIEW " + schemaName + ".v_dst");

            try (Connection conn = createDuckdbConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT snapshot_id, changes FROM ducklake_snapshots('ducklake_db') ORDER BY snapshot_id")) {
                int rowCount = 0;
                boolean sawAlteredView = false;
                boolean sawDroppedView = false;
                while (rs.next()) {
                    rowCount++;
                    Object changes = rs.getObject("changes");
                    if (changes == null) {
                        continue;
                    }
                    String text = changes.toString();
                    if (text.contains("views_altered")) {
                        sawAlteredView = true;
                    }
                    if (text.contains("views_dropped")) {
                        sawDroppedView = true;
                    }
                }
                assertThat(rowCount).as("ducklake_snapshots row count").isGreaterThanOrEqualTo(5);
                assertThat(sawAlteredView)
                        .as("DuckDB parsed at least one `altered_view` entry (rename + replace both map here)")
                        .isTrue();
                assertThat(sawDroppedView).as("DuckDB parsed a `dropped_view` entry").isTrue();
            }
        }
        finally {
            try {
                computeActual("DROP VIEW " + schemaName + ".v_dst");
            }
            catch (Exception ignored) {
            }
            try {
                computeActual("DROP VIEW " + schemaName + ".v_src");
            }
            catch (Exception ignored) {
            }
            try {
                computeActual("DROP SCHEMA " + schemaName);
            }
            catch (Exception ignored) {
            }
        }
    }

    /**
     * Upstream {@code DuckLakeTransaction::SchemaChangesMade()} flips on new/dropped view
     * entries and on new/dropped schema entries — the same trigger used for table DDL.
     * A DuckDB reader that caches catalog state keyed on {@code schema_version} must see
     * a bump when Trino creates/drops/replaces/renames a view or creates/drops a schema.
     * Before this was wired up, a Trino-only sequence of view + schema DDL would leave
     * {@code ducklake_snapshot.schema_version} frozen — DuckDB's next query would hit
     * stale cache and miss those objects. This test walks the full DDL surface and asserts
     * the counter advances on every step.
     * <p>
     * Surface covered:
     * <ul>
     *   <li>{@code CREATE SCHEMA} → {@code createSchema}</li>
     *   <li>{@code CREATE VIEW} → {@code createView}</li>
     *   <li>{@code COMMENT ON VIEW} → {@code replaceViewMetadata}</li>
     *   <li>{@code ALTER VIEW ... RENAME TO} → {@code renameView}</li>
     *   <li>{@code DROP VIEW} → {@code dropView}</li>
     *   <li>{@code DROP SCHEMA} → {@code dropSchema}</li>
     * </ul>
     */
    @Test
    public void testSchemaVersionBumpsOnViewAndSchemaDdl()
            throws Exception
    {
        String schemaName = "xengine_sv_bumps";
        DucklakeCatalogGenerator.IsolatedCatalog catalog = getIsolatedCatalog();

        try {
            long before = readCurrentSchemaVersion(catalog);

            computeActual("CREATE SCHEMA " + schemaName);
            long afterCreateSchema = readCurrentSchemaVersion(catalog);
            assertThat(afterCreateSchema).as("CREATE SCHEMA bumps schema_version").isGreaterThan(before);

            computeActual("CREATE VIEW " + schemaName + ".v AS SELECT id FROM test_schema.simple_table");
            long afterCreateView = readCurrentSchemaVersion(catalog);
            assertThat(afterCreateView).as("CREATE VIEW bumps schema_version").isGreaterThan(afterCreateSchema);

            // Note: Trino resolves an unqualified `RENAME TO <name>` against the session's
            // default schema, not the source view's schema. Fully qualify the target to
            // keep the rename scoped to our test schema.
            computeActual("ALTER VIEW " + schemaName + ".v RENAME TO " + schemaName + ".v2");
            long afterRename = readCurrentSchemaVersion(catalog);
            assertThat(afterRename).as("ALTER VIEW ... RENAME bumps schema_version").isGreaterThan(afterCreateView);

            // COMMENT ON VIEW dispatches to replaceViewMetadata (the only SQL path that does —
            // CREATE OR REPLACE VIEW goes through dropView + createView in DucklakeMetadata).
            computeActual("COMMENT ON VIEW " + schemaName + ".v2 IS 'a view with a comment'");
            long afterComment = readCurrentSchemaVersion(catalog);
            assertThat(afterComment).as("COMMENT ON VIEW (replaceViewMetadata) bumps schema_version").isGreaterThan(afterRename);

            computeActual("DROP VIEW " + schemaName + ".v2");
            long afterDropView = readCurrentSchemaVersion(catalog);
            assertThat(afterDropView).as("DROP VIEW bumps schema_version").isGreaterThan(afterComment);

            computeActual("DROP SCHEMA " + schemaName);
            long afterDropSchema = readCurrentSchemaVersion(catalog);
            assertThat(afterDropSchema).as("DROP SCHEMA bumps schema_version").isGreaterThan(afterDropView);

            // Non-table-scoped bumps must land in ducklake_schema_versions with table_id = NULL
            // (spec ducklake_schema_versions.md: table_id is BIGINT, nullable). A future DuckDB
            // version that reads per-table snapshots via this table still needs the broadcast
            // bump, which is keyed on table_id IS NULL.
            long nonTableScopedRows = countSchemaVersionRowsWithNullTableId(catalog, before);
            assertThat(nonTableScopedRows)
                    .as("expected one schema_versions row with NULL table_id per view/schema DDL (6 total)")
                    .isGreaterThanOrEqualTo(6);
        }
        finally {
            try {
                computeActual("DROP VIEW " + schemaName + ".v2");
            }
            catch (Exception ignored) {
            }
            try {
                computeActual("DROP VIEW " + schemaName + ".v");
            }
            catch (Exception ignored) {
            }
            try {
                computeActual("DROP SCHEMA " + schemaName);
            }
            catch (Exception ignored) {
            }
        }
    }

    private long readCurrentSchemaVersion(DucklakeCatalogGenerator.IsolatedCatalog catalog)
            throws Exception
    {
        try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
            return queryLong(pgConn,
                    "SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)");
        }
    }

    private long countSchemaVersionRowsWithNullTableId(
            DucklakeCatalogGenerator.IsolatedCatalog catalog,
            long sinceExclusiveSchemaVersion)
            throws Exception
    {
        try (Connection pgConn = DriverManager.getConnection(catalog.jdbcUrl(), catalog.user(), catalog.password())) {
            return queryLong(pgConn,
                    "SELECT count(*) FROM ducklake_schema_versions WHERE table_id IS NULL AND schema_version > ?",
                    sinceExclusiveSchemaVersion);
        }
    }
}
