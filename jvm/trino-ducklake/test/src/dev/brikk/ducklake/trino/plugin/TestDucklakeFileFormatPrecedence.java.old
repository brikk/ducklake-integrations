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
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * N1: precedence chain that decides which file format an INSERT (or the insert leg
 * of MERGE/UPDATE) writes. The chain — covered exhaustively here because a regression
 * silently corrupts the cross-engine catalog metadata — is:
 * <ol>
 *   <li>CTAS-only: explicit {@code WITH (data_file_format = ...)} statement override.</li>
 *   <li>Session property {@code ducklake.data_file_format}, when explicitly set.</li>
 *   <li>{@code file_format} of the most recent active data file in the table.</li>
 *   <li>Connector default ({@code parquet}).</li>
 * </ol>
 * Each test pins exactly one cell of the matrix and asserts both the on-disk extension
 * pattern and the value persisted in {@code ducklake_data_file.file_format} (visible via
 * the {@code $files} system table). The combined assertion is what catches a writer
 * that drifts away from what the catalog claims it wrote.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeFileFormatPrecedence
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "file-format-precedence";
    }

    private Session sessionWith(String format)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, format)
                .build();
    }

    /**
     * Read the per-file ({@code path}, {@code file_format}) tuples that the catalog
     * has recorded for a table, sorted by file_format then path so assertions are
     * deterministic regardless of insertion order. Drives the catalog-vs-disk
     * cross-check at the heart of every test below.
     */
    private List<List<Object>> filesByFormat(String table)
    {
        MaterializedResult result = computeActual(
                "SELECT file_format, path FROM \"" + table + "$files\" ORDER BY file_format, path");
        return result.getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .toList();
    }

    private void assertAllFilesAre(String table, String expectedFormat)
    {
        List<List<Object>> rows = filesByFormat(table);
        assertThat(rows).as("data files for %s", table).isNotEmpty();
        for (List<Object> row : rows) {
            assertThat(row.get(0)).as("file_format on $files row %s", row).isEqualTo(expectedFormat);
            String suffix = expectedFormat.equals(FORMAT_DUCKDB) ? ".db" : ".parquet";
            assertThat((String) row.get(1)).as("path extension on $files row %s", row).endsWith(suffix);
        }
    }

    private void assertFileCountByFormat(String table, int duckdbFiles, int parquetFiles)
    {
        List<List<Object>> rows = filesByFormat(table);
        long duck = rows.stream().filter(r -> FORMAT_DUCKDB.equals(r.get(0))).count();
        long parq = rows.stream().filter(r -> FORMAT_PARQUET.equals(r.get(0))).count();
        assertThat(duck).as("duckdb file count for %s (rows=%s)", table, rows).isEqualTo(duckdbFiles);
        assertThat(parq).as("parquet file count for %s (rows=%s)", table, rows).isEqualTo(parquetFiles);
    }

    // ==================== CTAS resolution ====================
    // Rule (c) "match latest file" can never apply to CTAS: the table is fresh, so
    // there are no prior data files. CTAS is purely WITH > session > default.

    @Test
    public void testCtasSessionParquetExplicit()
    {
        // Explicit session=parquet behaves identically to default — but covers the
        // path where rule (b) is exercised positively (was previously masked by
        // session default == "parquet" before N1's null-default change).
        computeActual(sessionWith(FORMAT_PARQUET),
                "CREATE TABLE test_schema.ctas_session_parquet AS SELECT 1 AS id");
        try {
            assertAllFilesAre("ctas_session_parquet", FORMAT_PARQUET);
        }
        finally {
            tryDropTable("test_schema.ctas_session_parquet");
        }
    }

    @Test
    public void testCtasWithParquetExplicit()
    {
        // Explicit WITH=parquet — exercises the WITH plumbing for the non-default value.
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.ctas_with_parquet WITH (data_file_format = 'parquet') AS SELECT 1 AS id");
        try {
            assertAllFilesAre("ctas_with_parquet", FORMAT_PARQUET);
        }
        finally {
            tryDropTable("test_schema.ctas_with_parquet");
        }
    }

    @Test
    public void testCtasWithDuckdbBeatsSessionParquet()
    {
        computeActual(sessionWith(FORMAT_PARQUET),
                "CREATE TABLE test_schema.ctas_with_beats_session WITH (data_file_format = 'duckdb') AS SELECT 1 AS id");
        try {
            assertAllFilesAre("ctas_with_beats_session", FORMAT_DUCKDB);
        }
        finally {
            tryDropTable("test_schema.ctas_with_beats_session");
        }
    }

    @Test
    public void testEmptyCtasProducesNoFiles()
    {
        // CTAS that selects zero rows produces zero data files — rule (c) on the
        // next INSERT therefore has nothing to match and falls through to default.
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.empty_ctas WITH (data_file_format = 'duckdb') AS " +
                        "SELECT 1 AS id WHERE 1 = 0");
        try {
            assertThat(filesByFormat("empty_ctas")).isEmpty();

            // Plain INSERT, session unset → connector default parquet (no prior file
            // to inherit from). This is the documented edge case in N1 — the user's
            // CTAS-time intent doesn't carry through an empty materialization.
            computeActual("INSERT INTO test_schema.empty_ctas VALUES (10), (20)");
            assertAllFilesAre("empty_ctas", FORMAT_PARQUET);
        }
        finally {
            tryDropTable("test_schema.empty_ctas");
        }
    }

    // ==================== INSERT resolution ====================
    // The new N1 behavior. Rule (c) is the only thing being added relative to the
    // pre-N1 chain, so these tests are the load-bearing ones.

    @Test
    public void testInsertInheritsDuckDbFromCtas()
    {
        // The headline case. CTAS lays down a .db file; subsequent plain INSERTs
        // (session unset, no WITH) must continue to write .db.
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.inherit_duck AS SELECT 1 AS id, CAST('a' AS VARCHAR) AS s");
        try {
            assertAllFilesAre("inherit_duck", FORMAT_DUCKDB);

            // Plain INSERT — no session prop, no WITH (which is illegal on INSERT anyway).
            computeActual("INSERT INTO test_schema.inherit_duck VALUES (2, 'b')");
            assertFileCountByFormat("inherit_duck", 2, 0);

            // Round-trip — verifies the table is queryable end-to-end after rule (c).
            MaterializedResult all = computeActual("SELECT id FROM test_schema.inherit_duck ORDER BY id");
            assertThat(all.getMaterializedRows()).hasSize(2);
            assertThat(all.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(all.getMaterializedRows().get(1).getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.inherit_duck");
        }
    }

    @Test
    public void testInsertInheritsParquetFromCtas()
    {
        // Symmetric guard: rule (c) must not accidentally pin everything to duckdb.
        // Default-CTAS lays parquet, plain INSERT must continue parquet.
        computeActual("CREATE TABLE test_schema.inherit_parq AS SELECT 1 AS id");
        try {
            computeActual("INSERT INTO test_schema.inherit_parq VALUES (2)");
            assertFileCountByFormat("inherit_parq", 0, 2);
        }
        finally {
            tryDropTable("test_schema.inherit_parq");
        }
    }

    @Test
    public void testSessionParquetOverridesDuckDbTable()
    {
        // Rule (b) beats rule (c): even though the table is currently all .db,
        // an explicit session=parquet INSERT writes parquet.
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.session_overrides_duck AS SELECT 1 AS id");
        try {
            computeActual(sessionWith(FORMAT_PARQUET),
                    "INSERT INTO test_schema.session_overrides_duck VALUES (2)");
            assertFileCountByFormat("session_overrides_duck", 1, 1);
        }
        finally {
            tryDropTable("test_schema.session_overrides_duck");
        }
    }

    @Test
    public void testSessionDuckDbOverridesParquetTable()
    {
        // Same direction reversed — making sure rule (b) works both ways.
        computeActual("CREATE TABLE test_schema.session_overrides_parq AS SELECT 1 AS id");
        try {
            computeActual(sessionWith(FORMAT_DUCKDB),
                    "INSERT INTO test_schema.session_overrides_parq VALUES (2)");
            assertFileCountByFormat("session_overrides_parq", 1, 1);
        }
        finally {
            tryDropTable("test_schema.session_overrides_parq");
        }
    }

    @Test
    public void testTwoConsecutivePlainInsertsBothInherit()
    {
        // Rule (c) must be stable across multiple plain inserts — not just hold on
        // the first one and then drift back to default.
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.two_inherits AS SELECT 1 AS id");
        try {
            computeActual("INSERT INTO test_schema.two_inherits VALUES (2)");
            computeActual("INSERT INTO test_schema.two_inherits VALUES (3)");
            assertFileCountByFormat("two_inherits", 3, 0);
        }
        finally {
            tryDropTable("test_schema.two_inherits");
        }
    }

    @Test
    public void testFlipStaysFlipped()
    {
        // Once the user explicitly inserts the other format, the "latest file" is now
        // that format — so subsequent plain inserts inherit *that*, not the original.
        // This is the compound case: rule (c) keys on the most recent data file, not
        // the first one.
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.flip_stays AS SELECT 1 AS id");
        try {
            // Flip to parquet via session.
            computeActual(sessionWith(FORMAT_PARQUET),
                    "INSERT INTO test_schema.flip_stays VALUES (2)");
            assertFileCountByFormat("flip_stays", 1, 1);

            // Plain INSERT, session unset → must pick up parquet (most recent), not
            // duckdb (original). This is what "stays flipped" means.
            computeActual("INSERT INTO test_schema.flip_stays VALUES (3)");
            assertFileCountByFormat("flip_stays", 1, 2);

            // And again — still parquet.
            computeActual("INSERT INTO test_schema.flip_stays VALUES (4)");
            assertFileCountByFormat("flip_stays", 1, 3);
        }
        finally {
            tryDropTable("test_schema.flip_stays");
        }
    }

    @Test
    public void testFlipStaysFlippedInverse()
    {
        // Same as testFlipStaysFlipped but the original is parquet and the flip is
        // to duckdb — guards against any direction-asymmetric bugs in rule (c).
        computeActual("CREATE TABLE test_schema.flip_stays_inv AS SELECT 1 AS id");
        try {
            computeActual(sessionWith(FORMAT_DUCKDB),
                    "INSERT INTO test_schema.flip_stays_inv VALUES (2)");
            computeActual("INSERT INTO test_schema.flip_stays_inv VALUES (3)");
            assertFileCountByFormat("flip_stays_inv", 2, 1);
        }
        finally {
            tryDropTable("test_schema.flip_stays_inv");
        }
    }

    @Test
    public void testEmptyCreateThenPlainInsertDefaults()
    {
        // Empty CREATE TABLE (no AS, no rows): rule (c) finds nothing. Plain INSERT
        // with no session prop, no WITH → connector default parquet. Documented edge
        // case in N1 — user can avoid it by using CTAS or setting the session prop.
        computeActual("CREATE TABLE test_schema.empty_create_default (id INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.empty_create_default VALUES (1)");
            assertAllFilesAre("empty_create_default", FORMAT_PARQUET);
        }
        finally {
            tryDropTable("test_schema.empty_create_default");
        }
    }

    @Test
    public void testEmptyCreateThenSessionDuckDbThenInherits()
    {
        // Empty CREATE → first INSERT with session=duckdb writes .db (rule b).
        // Second plain INSERT (no overrides) inherits .db via rule (c) — the session
        // prop is gone but the file is still there for rule (c) to find.
        computeActual("CREATE TABLE test_schema.empty_create_then_inherit (id INTEGER)");
        try {
            computeActual(sessionWith(FORMAT_DUCKDB),
                    "INSERT INTO test_schema.empty_create_then_inherit VALUES (1)");
            assertAllFilesAre("empty_create_then_inherit", FORMAT_DUCKDB);

            computeActual("INSERT INTO test_schema.empty_create_then_inherit VALUES (2)");
            assertFileCountByFormat("empty_create_then_inherit", 2, 0);
        }
        finally {
            tryDropTable("test_schema.empty_create_then_inherit");
        }
    }

    // ==================== UPDATE / MERGE resolution ====================
    // beginMerge runs the same precedence chain. UPDATE rewrites changed rows into
    // new files, so it goes through the insert leg.

    @Test
    public void testUpdateInheritsDuckDb()
    {
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.update_inherits AS SELECT * FROM (VALUES (1, 100), (2, 200)) AS t(id, amount)");
        try {
            // UPDATE — produces new data files via the merge path. Rule (c) → .db.
            computeActual("UPDATE test_schema.update_inherits SET amount = amount + 1 WHERE id = 1");

            // Original .db file is still on disk (the data files of an UPDATE table
            // are appended; the changed rows get new files plus a delete file).
            // What matters for N1 is that the *new* files are .db, not parquet.
            List<List<Object>> rows = filesByFormat("update_inherits");
            assertThat(rows).isNotEmpty();
            for (List<Object> row : rows) {
                assertThat(row.get(0)).as("file_format on update_inherits row %s", row).isEqualTo(FORMAT_DUCKDB);
            }

            MaterializedResult result = computeActual("SELECT id, amount FROM test_schema.update_inherits ORDER BY id");
            assertThat(result.getMaterializedRows()).hasSize(2);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(101);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(200);
        }
        finally {
            tryDropTable("test_schema.update_inherits");
        }
    }

    @Test
    public void testUpdateSessionOverridesTableFormat()
    {
        // Rule (b) wins on UPDATE too — session=parquet writes new files as parquet
        // even when the table is currently all .db.
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.update_overrides AS SELECT * FROM (VALUES (1, 100), (2, 200)) AS t(id, amount)");
        try {
            computeActual(sessionWith(FORMAT_PARQUET),
                    "UPDATE test_schema.update_overrides SET amount = 999 WHERE id = 1");

            // Mix: original .db + a parquet file from the update's row rewrite.
            List<List<Object>> rows = filesByFormat("update_overrides");
            long duck = rows.stream().filter(r -> FORMAT_DUCKDB.equals(r.get(0))).count();
            long parq = rows.stream().filter(r -> FORMAT_PARQUET.equals(r.get(0))).count();
            assertThat(duck).as("expected the original .db survives (rows=%s)", rows).isGreaterThanOrEqualTo(1);
            assertThat(parq).as("expected at least one new parquet file from the UPDATE (rows=%s)", rows).isGreaterThanOrEqualTo(1);

            MaterializedResult result = computeActual("SELECT id, amount FROM test_schema.update_overrides ORDER BY id");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(999);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo(200);
        }
        finally {
            tryDropTable("test_schema.update_overrides");
        }
    }

    // ==================== Cross-table isolation ====================
    // Two tables in the same session must keep independent format histories. A bug
    // where rule (c) leaked across tables (e.g. cached "last format" globally) would
    // be silent at small scale and corrupting at large scale, hence the explicit guard.

    @Test
    public void testCrossTableIsolation()
    {
        computeActual(sessionWith(FORMAT_DUCKDB),
                "CREATE TABLE test_schema.cross_duck AS SELECT 1 AS id");
        computeActual("CREATE TABLE test_schema.cross_parq AS SELECT 1 AS id");
        try {
            // Plain inserts in *both* tables, in alternating order, no session prop.
            computeActual("INSERT INTO test_schema.cross_duck VALUES (2)");
            computeActual("INSERT INTO test_schema.cross_parq VALUES (2)");
            computeActual("INSERT INTO test_schema.cross_duck VALUES (3)");
            computeActual("INSERT INTO test_schema.cross_parq VALUES (3)");

            assertFileCountByFormat("cross_duck", 3, 0);
            assertFileCountByFormat("cross_parq", 0, 3);
        }
        finally {
            tryDropTable("test_schema.cross_duck");
            tryDropTable("test_schema.cross_parq");
        }
    }

    // ==================== Negative / validation ====================

    @Test
    public void testInvalidSessionValueRejected()
    {
        // The validator only fires on explicit SET — null (unset) skips it. We pin
        // the validation behavior here so the null-default change in N1 doesn't
        // accidentally weaken the validator for explicit sets.
        Session badSession = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, "vortex")
                .build();
        assertThatThrownBy(() -> computeActual(badSession,
                "CREATE TABLE test_schema.invalid_session AS SELECT 1 AS id"))
                .hasMessageContaining(DATA_FILE_FORMAT + " must be one of");
        tryDropTable("test_schema.invalid_session");
    }
}
