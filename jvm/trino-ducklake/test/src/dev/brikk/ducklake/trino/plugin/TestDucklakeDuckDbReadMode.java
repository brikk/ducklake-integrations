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

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_READ_MODE;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_MODE_AUTO;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_MODE_HTTPFS;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_MODE_MATERIALIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * N2 routing tests. The connector picks per-split between materialize-then-ATTACH
 * (download {@code .db} to local tmp first) and ATTACH-via-httpfs (DuckDB streams
 * blocks from {@code s3://} directly). Choice is driven by {@code duckdb_read_mode}
 * + the {@code ducklake.duckdb.auto-httpfs-threshold} config; this test verifies the
 * wiring without requiring an actual S3 backend.
 *
 * <p>The test environment is local-FS, so any execution that takes the httpfs branch
 * will hit a clean error from the routing code ("requires an s3:// data file path"):
 * the negative path is the test signal. The materialize branch — which works on
 * local-FS — is the positive path. Together they pin the routing decision in both
 * directions.
 *
 * <p>This catalog uses {@code ducklake.duckdb.auto-httpfs-threshold=1B}, so any
 * non-empty {@code .db} file falls on the httpfs side of the {@code auto} threshold —
 * makes the threshold-driven decision testable without needing a multi-MB fixture.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDuckDbReadMode
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "duckdb-read-mode";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Forces 'auto' to take the httpfs branch for any non-empty file in this suite.
        // The integration tests in TestDucklakeDuckDbFormatRead use the default (64MiB)
        // threshold and verify the opposite — small files take the materialize branch.
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog(CATALOG_NAME)
                .addConnectorProperty("ducklake.duckdb.auto-httpfs-threshold", "1B")
                .build();
    }

    private Session sessionWith(String readMode)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .setCatalogSessionProperty("ducklake", DUCKDB_READ_MODE, readMode)
                .build();
    }

    private Session writeDuckDbSession()
    {
        // Writer-only session. We don't set DUCKDB_READ_MODE here — writes don't read
        // from the data file path, so the read mode is irrelevant during INSERT/CTAS.
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build();
    }

    @Test
    public void testMaterializeModeReadsFromLocalFs()
    {
        // Baseline: with read_mode=materialize, the file is pulled into the local cache
        // and DuckDB ATTACHes the local path. Local FS is fine for this — the cache
        // just copies the file alongside the original.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.read_mode_materialize AS SELECT 1 AS id, CAST('a' AS VARCHAR) AS s");
        try {
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id, s FROM test_schema.read_mode_materialize");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo("a");
        }
        finally {
            tryDropTable("test_schema.read_mode_materialize");
        }
    }

    @Test
    public void testHttpfsModeFallsBackToMaterializeOnLocalFs()
    {
        // httpfs against a non-s3 path silently degrades to materialize — the local
        // file is already directly attachable, no streaming protocol required.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.read_mode_httpfs_localfs AS SELECT 1 AS id");
        try {
            var result = computeActual(
                    sessionWith(READ_MODE_HTTPFS),
                    "SELECT * FROM test_schema.read_mode_httpfs_localfs");
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.read_mode_httpfs_localfs");
        }
    }

    @Test
    public void testAutoModeWithLowThresholdFallsBackToMaterializeOnLocalFs()
    {
        // 'auto' + threshold=1B picks httpfs by size; against a non-s3 path that
        // degrades to materialize same as explicit httpfs would.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.read_mode_auto_lo AS SELECT 1 AS id");
        try {
            var result = computeActual(
                    sessionWith(READ_MODE_AUTO),
                    "SELECT * FROM test_schema.read_mode_auto_lo");
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.read_mode_auto_lo");
        }
    }

    @Test
    public void testInvalidReadModeRejected()
    {
        Session bad = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DUCKDB_READ_MODE, "ftp")
                .build();
        assertThatThrownBy(() -> computeActual(bad, "SELECT 1"))
                .hasMessageContaining(DUCKDB_READ_MODE + " must be one of");
    }

    @Test
    public void testParquetTablesIgnoreReadMode()
    {
        // duckdb_read_mode is a no-op for parquet tables (it gates only the duckdb
        // file format's ATTACH path). Pin this so a future refactor doesn't
        // accidentally couple the two.
        computeActual("CREATE TABLE test_schema.read_mode_parquet AS SELECT 7 AS id");
        try {
            // Setting httpfs explicitly on a parquet read must NOT trip the routing —
            // parquet path doesn't go anywhere near the httpfs branch.
            MaterializedResult result = computeActual(
                    Session.builder(getSession())
                            .setCatalogSessionProperty("ducklake", DUCKDB_READ_MODE, READ_MODE_HTTPFS)
                            .build(),
                    "SELECT id FROM test_schema.read_mode_parquet");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(7);
        }
        finally {
            tryDropTable("test_schema.read_mode_parquet");
        }
    }

    @Test
    public void testFunctionPredicatePushesDownThroughTrinoMacro()
    {
        // End-to-end proof that the trino_* macro layer fires for a real query.
        // length/1 and substring/3 are both in PUSHABLE_FUNCTIONS, so Trino's
        // applyFilter hands the ConnectorExpression to DuckDbExpressionTranslator,
        // which emits trino_length("name") / trino_substring("name", 1, 1) into the
        // WHERE clause the page source sends to DuckDB. The macros resolve
        // server-side via trino-function-aliases.sql. Correct result = the whole
        // chain works.
        //
        // Translated conjuncts are also kept in remainingExpression (Regime 1 in
        // dev-docs/TODO-pushdown-duckdb.md), so correctness does not depend on the
        // pushdown firing — but the test runs on the duckdb-format path, so the
        // macro DOES execute on every row and a missing macro would fail the query.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.fn_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, CAST('Apple' AS VARCHAR)), "
                        + "  (2, CAST('banana' AS VARCHAR)), "
                        + "  (3, CAST('Cherry' AS VARCHAR))"
                        + ") AS t(id, name)");
        try {
            // length(name) = 5 → only 'Apple' matches (5 code points).
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id, name FROM test_schema.fn_pushdown WHERE length(name) = 5");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo("Apple");

            // Combine TupleDomain pushdown (id IN (1,3)) with function pushdown.
            // substring(name, 1, 1) = 'C' → only 'Cherry' matches, and id=3 ∈ {1,3}.
            MaterializedResult combined = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.fn_pushdown WHERE id IN (1, 3) "
                            + "AND substring(name, 1, 1) = 'C'");
            assertThat(combined.getRowCount()).isEqualTo(1);
            assertThat(combined.getMaterializedRows().getFirst().getField(0)).isEqualTo(3);
        }
        finally {
            tryDropTable("test_schema.fn_pushdown");
        }
    }

    @Test
    public void testLikePredicatePushesDown()
    {
        // End-to-end proof for the LIKE translator branch. Trino delivers LIKE
        // as Call($like, [value, Constant(LikePattern)]); DuckDbExpressionTranslator
        // emits `("name" LIKE 'App%')` directly into the WHERE clause sent to
        // DuckDB. No macro is involved — it's a translator emit. NOT LIKE travels
        // through the existing $not handler and reuses the LIKE branch.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.like_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, CAST('Apple' AS VARCHAR)), "
                        + "  (2, CAST('Application' AS VARCHAR)), "
                        + "  (3, CAST('banana' AS VARCHAR)), "
                        + "  (4, CAST('Cherry' AS VARCHAR))"
                        + ") AS t(id, name)");
        try {
            // name LIKE 'App%' → only 'Apple' (id=1) and 'Application' (id=2) match.
            MaterializedResult like = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.like_pushdown WHERE name LIKE 'App%' ORDER BY id");
            assertThat(like.getRowCount()).isEqualTo(2);
            assertThat(like.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(like.getMaterializedRows().get(1).getField(0)).isEqualTo(2);

            // name NOT LIKE 'App%' → 'banana' (id=3) and 'Cherry' (id=4) survive.
            MaterializedResult notLike = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.like_pushdown WHERE name NOT LIKE 'App%' ORDER BY id");
            assertThat(notLike.getRowCount()).isEqualTo(2);
            assertThat(notLike.getMaterializedRows().get(0).getField(0)).isEqualTo(3);
            assertThat(notLike.getMaterializedRows().get(1).getField(0)).isEqualTo(4);
        }
        finally {
            tryDropTable("test_schema.like_pushdown");
        }
    }

    @Test
    public void testConcatPredicatePushesDownAsOperatorChain()
    {
        // End-to-end proof for the concat → `||` translator rewrite. DuckDB's
        // built-in `concat` silently skips NULLs while Trino's NULL-propagates
        // (REPORT-hash-null-handling.md), so the translator rewrites Trino's
        // `Call(concat, [args])` into `(arg1 || arg2 || ...)` — the `||` operator
        // NULL-propagates in both engines, giving Trino-aligned semantics.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.concat_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, CAST('Apple' AS VARCHAR), CAST('X' AS VARCHAR)), "
                        + "  (2, CAST('banana' AS VARCHAR), CAST('Y' AS VARCHAR)), "
                        + "  (3, CAST('Cherry' AS VARCHAR), CAST('Z' AS VARCHAR))"
                        + ") AS t(id, name, suffix)");
        try {
            // concat(name, suffix) = 'AppleX' → only id=1 matches.
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.concat_pushdown WHERE concat(name, suffix) = 'AppleX'");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);

            // Variadic: concat(name, '-', suffix) = 'banana-Y' → id=2.
            MaterializedResult variadic = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.concat_pushdown WHERE concat(name, '-', suffix) = 'banana-Y'");
            assertThat(variadic.getRowCount()).isEqualTo(1);
            assertThat(variadic.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.concat_pushdown");
        }
    }

    @Test
    public void testConcatWithNullPropagatesTrinoSemantics()
    {
        // The key correctness guarantee for the rewrite: when an argument is NULL,
        // Trino's `concat(...)` returns NULL — DuckDB's built-in `concat` would
        // return the non-NULL fragments. Routing through `||` preserves Trino's
        // NULL-propagation on the duckdb-format read path.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.concat_null_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, CAST('a' AS VARCHAR), CAST('b' AS VARCHAR)), "
                        + "  (2, CAST('a' AS VARCHAR), CAST(NULL AS VARCHAR))"
                        + ") AS t(id, a, b)");
        try {
            // concat(a, b) IS NULL → only id=2 (Trino semantics; DuckDB built-in concat
            // would return 'a' for id=2 and the predicate would match nothing).
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.concat_null_pushdown WHERE concat(a, b) IS NULL");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.concat_null_pushdown");
        }
    }

    @Test
    public void testBetweenPredicateReturnsCorrectRows()
    {
        // BETWEEN is grammar in both engines; Trino's planner typically decomposes
        // `x BETWEEN a AND b` into `x >= a AND x <= b` before applyFilter — both
        // halves then push through TupleDomain (range constraint). No translator
        // code is needed; this test confirms correctness end-to-end on the
        // duckdb-format path.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.between_pushdown AS "
                        + "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(id)");
        try {
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.between_pushdown WHERE id BETWEEN 2 AND 4 ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(4);
        }
        finally {
            tryDropTable("test_schema.between_pushdown");
        }
    }

    @Test
    public void testArithmeticAndCoalescePushdownReturnsCorrectRows()
    {
        // End-to-end proof for the round-6e translator additions:
        // arithmetic ($add) and $coalesce both push down via the translator
        // (no macros involved — these are standard-function operator emits).
        // The duckdb-format path means the WHERE clause runs through DuckDB; a
        // missing translator branch would fall back to Trino-side filter, still
        // returning the right rows. So this test asserts correctness, and the
        // unit tests in TestDuckDbExpressionTranslator pin the SQL shape.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.arith_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, CAST('Apple' AS VARCHAR)), "
                        + "  (2, CAST(NULL AS VARCHAR)), "
                        + "  (3, CAST('Cherry' AS VARCHAR))"
                        + ") AS t(id, name)");
        try {
            // Arithmetic — id + 1 = 2 → only id=1 matches.
            MaterializedResult arith = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.arith_pushdown WHERE id + 1 = 2");
            assertThat(arith.getRowCount()).isEqualTo(1);
            assertThat(arith.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);

            // COALESCE — name IS NULL on row 2 → COALESCE(name, 'fallback') = 'fallback' matches row 2.
            MaterializedResult coal = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.arith_pushdown "
                            + "WHERE COALESCE(name, 'fallback') = 'fallback'");
            assertThat(coal.getRowCount()).isEqualTo(1);
            assertThat(coal.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);

            // Combined — id * 2 > 4 AND COALESCE(name, '') <> '' → id=3 (Cherry, id*2=6>4, name non-null).
            MaterializedResult combined = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.arith_pushdown "
                            + "WHERE id * 2 > 4 AND COALESCE(name, '') <> ''");
            assertThat(combined.getRowCount()).isEqualTo(1);
            assertThat(combined.getMaterializedRows().getFirst().getField(0)).isEqualTo(3);
        }
        finally {
            tryDropTable("test_schema.arith_pushdown");
        }
    }

    @Test
    public void testPlaceholderLowerPushdownIsCorrectForAsciiData()
    {
        // lower/1 IS in PUSHABLE_FUNCTIONS as a placeholder — the translator pushes
        // it (and fires a one-shot WARN), and DuckDB resolves trino_lower via the
        // installed macro. ASCII data is in the safe range; the test confirms the
        // result is correct, which validates the macro install + push + WARN path.
        // Divergent Unicode inputs are documented separately in REPORT-string-unicode-audit.md.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.fn_placeholder AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, CAST('Apple' AS VARCHAR)), "
                        + "  (2, CAST('banana' AS VARCHAR))"
                        + ") AS t(id, name)");
        try {
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.fn_placeholder WHERE lower(name) = 'apple'");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.fn_placeholder");
        }
    }

    private void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }
}
