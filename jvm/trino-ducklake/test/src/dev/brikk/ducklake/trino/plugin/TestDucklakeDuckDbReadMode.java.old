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
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_READ_MODE;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.PUSHDOWN_TIMESTAMP_WITH_TIMEZONE;
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
        // (archive/REPORT-hash-null-handling.md), so the translator rewrites Trino's
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
    public void testYearOfWeekPredicatePushesDown()
    {
        // End-to-end proof for the round-6j year_of_week macro on the duckdb-format
        // read path. Pressure point: '2024-12-30' is a Monday but ISO week 1 of
        // 2025 — so year_of_week returns 2025 while year returns 2024. A regression
        // that aliased year_of_week to year would silently miss this row.
        // (Trino spells it `year_of_week`; DuckDB has no bare `isoyear` function
        // so the macro routes through `extract('isoyear' FROM d)`.)
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.year_of_week_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, DATE '2024-12-29'), "  // Sunday → ISO week 52 of 2024
                        + "  (2, DATE '2024-12-30'), "  // Monday → ISO week 1 of 2025 (forward boundary)
                        + "  (3, DATE '2025-01-05'), "  // Sunday → ISO week 1 of 2025
                        + "  (4, DATE '2025-01-06'), "  // Monday → ISO week 2 of 2025
                        + "  (5, DATE '2021-01-01'), "  // Friday → ISO week 53 of 2020 (backward boundary)
                        + "  (6, DATE '2020-12-31')"    // Thursday → also ISO week 53 of 2020
                        + ") AS t(id, d)");
        try {
            // year_of_week(d) = 2025 → rows 2, 3, 4 (the Sunday 2025-01-05 still falls
            // in ISO week 1 of 2025). Row 1 stays under year_of_week 2024.
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.year_of_week_pushdown WHERE year_of_week(d) = 2025 ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(3);
            assertThat(result.getMaterializedRows().get(2).getField(0)).isEqualTo(4);

            // Cross-check that calendar year() still disagrees with year_of_week() on row 2.
            MaterializedResult calendarYear = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.year_of_week_pushdown "
                            + "WHERE year(d) = 2024 AND year_of_week(d) = 2025");
            assertThat(calendarYear.getRowCount()).isEqualTo(1);
            assertThat(calendarYear.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);

            // Backward-boundary smoking gun: rows 5 and 6 have calendar years 2021/2020
            // but BOTH belong to ISO year 2020. A regression that aliased year_of_week
            // to year() would split them between 2021 and 2020 here.
            MaterializedResult backward = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.year_of_week_pushdown "
                            + "WHERE year_of_week(d) = 2020 AND week(d) = 53 ORDER BY id");
            assertThat(backward.getRowCount()).isEqualTo(2);
            assertThat(backward.getMaterializedRows().get(0).getField(0)).isEqualTo(5);
            assertThat(backward.getMaterializedRows().get(1).getField(0)).isEqualTo(6);
        }
        finally {
            tryDropTable("test_schema.year_of_week_pushdown");
        }
    }

    @Test
    public void testDayOfWeekIsoNumberingPushesDown()
    {
        // End-to-end proof that day_of_week emits ISO 1=Mon..7=Sun. A regression
        // that used DuckDB's bare dayofweek() (0=Sun..6=Sat) would return 0 for
        // Sunday and the WHERE clause would match nothing.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.dow_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, DATE '2024-01-07'), "  // Sunday → ISO 7 (NOT 0)
                        + "  (2, DATE '2024-01-08'), "  // Monday → ISO 1
                        + "  (3, DATE '2024-01-13')"    // Saturday → ISO 6
                        + ") AS t(id, d)");
        try {
            // day_of_week(d) = 7 → Sunday → id=1.
            MaterializedResult sunday = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.dow_pushdown WHERE day_of_week(d) = 7");
            assertThat(sunday.getRowCount()).isEqualTo(1);
            assertThat(sunday.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);

            // day_of_week(d) = 1 → Monday → id=2.
            MaterializedResult monday = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.dow_pushdown WHERE day_of_week(d) = 1");
            assertThat(monday.getRowCount()).isEqualTo(1);
            assertThat(monday.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.dow_pushdown");
        }
    }

    @Test
    public void testHourExtractPushesDownOnTimestamp()
    {
        // End-to-end proof for the Tier B hour() macro on a TIMESTAMP (no TZ)
        // column. Wall-clock components are TZ-invariant in both engines, so the
        // result must match Trino regardless of session zone (the duckdb-format
        // path runs the predicate on DuckDB's side; a divergence would surface
        // here as a missed row).
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.hour_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, TIMESTAMP '2024-12-31 22:30:00'), "
                        + "  (2, TIMESTAMP '2024-12-31 23:30:00'), "
                        + "  (3, TIMESTAMP '2025-01-01 00:30:00')"
                        + ") AS t(id, ts)");
        try {
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.hour_pushdown WHERE hour(ts) = 22");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);

            // Combine with minute() to exercise two Tier B extractors in one predicate.
            MaterializedResult combined = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.hour_pushdown "
                            + "WHERE hour(ts) = 23 AND minute(ts) = 30");
            assertThat(combined.getRowCount()).isEqualTo(1);
            assertThat(combined.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.hour_pushdown");
        }
    }

    @Test
    public void testTierABPushdownStableUnderSessionTimeZoneChange()
    {
        // Chunk 2 plumbing wired Trino's session TimeZoneKey through the page
        // source provider so both executors run `SET TimeZone = '<normalised>'`
        // on attach. Tier A/B functions are wall-clock invariant (DATE / TIMESTAMP
        // no-TZ extracts ignore session zone in both engines), so the same
        // queries that worked under the JVM-default zone must keep producing
        // identical results under any other zone. A regression that broke the
        // SET TimeZone wiring (e.g. malformed SQL, wrong-shape normaliser
        // output) would either fail the attach or shift the date components.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.tz_stable AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, DATE '2024-01-07', TIMESTAMP '2024-12-31 22:30:00'), "
                        + "  (2, DATE '2024-12-30', TIMESTAMP '2024-06-15 12:00:00')"
                        + ") AS t(id, d, ts)");
        try {
            for (String zoneId : new String[]{
                    "UTC", "America/Los_Angeles", "Europe/Berlin", "Asia/Singapore"}) {
                Session zonedSession = Session.builder(sessionWith(READ_MODE_MATERIALIZE))
                        .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId))
                        .build();
                // Tier A (DATE-only): day_of_week ISO numbering must stay stable.
                MaterializedResult dow = computeActual(zonedSession,
                        "SELECT id FROM test_schema.tz_stable WHERE day_of_week(d) = 7");
                assertThat(dow.getRowCount()).as("zone=%s: ISO Sunday match must be stable", zoneId).isEqualTo(1);
                assertThat(dow.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);

                // Tier A: year_of_week boundary ('2024-12-30' Monday → ISO year 2025)
                // is wall-clock-invariant for a DATE column. Must hold under every session zone.
                MaterializedResult yow = computeActual(zonedSession,
                        "SELECT id FROM test_schema.tz_stable WHERE year_of_week(d) = 2025");
                assertThat(yow.getRowCount()).as("zone=%s: ISO year smoking gun must be stable", zoneId).isEqualTo(1);
                assertThat(yow.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);

                // Tier B (TIMESTAMP no-TZ): hour() reads the wall clock directly.
                MaterializedResult hr = computeActual(zonedSession,
                        "SELECT id FROM test_schema.tz_stable WHERE hour(ts) = 22");
                assertThat(hr.getRowCount()).as("zone=%s: wall-clock hour must be stable", zoneId).isEqualTo(1);
                assertThat(hr.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
            }
        }
        finally {
            tryDropTable("test_schema.tz_stable");
        }
    }

    @Test
    public void testTierCYearBoundarySmokingGunSingaporeSession()
    {
        // Chunk 3.5 end-to-end smoking gun. The instant '2024-12-31 22:00 UTC'
        // is a year-boundary case:
        //   - UTC reader: year = 2024
        //   - America/Los_Angeles reader: year = 2024 (14:00 LA)
        //   - Asia/Singapore reader: year = 2025 (06:00 next day in SG)
        //
        // Pre-3.5 the connector's Arrow converter hardcoded UTC_KEY on the
        // incoming WTZ value, so Trino's above-scan year() ALWAYS returned UTC
        // year regardless of session. 3.5 fixed that — the converter now uses
        // the Arrow schema TZ (= chunk-2's SET TimeZone = session zone), so
        // Trino above-scan year() in a Singapore session is 2025 for this
        // instant, matching DuckDB's pushed year() in its Singapore session.
        //
        // Test: Singapore session + property on + `WHERE year(ts) = 2025` must
        // match the year-boundary row. Property off must match the same row
        // (Trino-side eval) — invariance under the property toggle is the
        // chunk-3.5 correctness claim made strong.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.tier_c_smoking_gun ("
                        + "  id INTEGER, "
                        + "  ts TIMESTAMP(3) WITH TIME ZONE)");
        computeActual(writeDuckDbSession(),
                "INSERT INTO test_schema.tier_c_smoking_gun VALUES "
                        + "(1, TIMESTAMP '2024-12-31 22:00:00.000 UTC'), "
                        + "(2, TIMESTAMP '2024-06-15 12:00:00.000 UTC')");
        try {
            Session singaporeOn = Session.builder(sessionWith(READ_MODE_MATERIALIZE))
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Singapore"))
                    .setCatalogSessionProperty("ducklake", PUSHDOWN_TIMESTAMP_WITH_TIMEZONE, "true")
                    .build();
            Session singaporeOff = Session.builder(sessionWith(READ_MODE_MATERIALIZE))
                    .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Asia/Singapore"))
                    .build();

            // Property ON: pushed year(ts) executes server-side under DuckDB
            // session=Singapore → returns 2025 for row 1 → DuckDB filter passes
            // row 1. Above-scan Trino re-evaluates year(value) with value
            // constructed by the converter using Arrow schema TZ=Singapore →
            // Trino computes year=2025 → keeps row 1. Both agree.
            MaterializedResult sgOn = computeActual(singaporeOn,
                    "SELECT id FROM test_schema.tier_c_smoking_gun WHERE year(ts) = 2025");
            assertThat(sgOn.getRowCount())
                    .as("Singapore session + property on: the year-boundary row matches year=2025")
                    .isEqualTo(1);
            assertThat(sgOn.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);

            // Property OFF: no pushdown; Trino above-scan year() of the converter-
            // constructed value (Singapore-zoned) computes 2025 too → same answer,
            // slower path. The invariance is the load-bearing claim.
            MaterializedResult sgOff = computeActual(singaporeOff,
                    "SELECT id FROM test_schema.tier_c_smoking_gun WHERE year(ts) = 2025");
            assertThat(sgOff.getMaterializedRows())
                    .as("Property toggle must not change visible result")
                    .isEqualTo(sgOn.getMaterializedRows());

            // Sibling case to pin: Singapore session asks year=2024 → row 2 (mid-year).
            MaterializedResult sg2024 = computeActual(singaporeOn,
                    "SELECT id FROM test_schema.tier_c_smoking_gun WHERE year(ts) = 2024");
            assertThat(sg2024.getRowCount()).isEqualTo(1);
            assertThat(sg2024.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.tier_c_smoking_gun");
        }
    }

    @Test
    public void testTierCToUnixtimeOnTimestampWithTimeZonePushesWhenPropertyOn()
    {
        // The shipped Tier C surface in chunk 3: to_unixtime over TIMESTAMP WITH
        // TIME ZONE. Both engines return the absolute UTC epoch regardless of
        // any session or stored zone, so pushdown is byte-equivalent to Trino's
        // above-scan eval. Predicate returns identical results under any session
        // zone and either property setting; the difference is whether DuckDB
        // does the filtering server-side (property on) or Trino does it
        // above the scan (property off).
        //
        // Test pins the property toggle round-trips correctly for to_unixtime
        // across three session zones — the strongest end-to-end correctness
        // guarantee we can make for Tier C without first changing the Arrow
        // converter to honour Arrow schema TZ (see chunk 3 shipped notes in
        // dev-docs/archive/TODO-pushdown-datetime.md for the broader Tier C plan).
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.tier_c_to_unixtime ("
                        + "  id INTEGER, "
                        + "  ts TIMESTAMP(3) WITH TIME ZONE)");
        computeActual(writeDuckDbSession(),
                "INSERT INTO test_schema.tier_c_to_unixtime VALUES "
                        // 2024-06-15 12:00:00 UTC → epoch 1718452800 (deterministic)
                        + "(1, TIMESTAMP '2024-06-15 12:00:00.000 UTC'), "
                        // 1970-01-01 00:00:01 UTC → epoch 1
                        + "(2, TIMESTAMP '1970-01-01 00:00:01.000 UTC')");
        try {
            for (String zoneId : new String[]{"UTC", "America/Los_Angeles", "Asia/Singapore"}) {
                Session off = Session.builder(sessionWith(READ_MODE_MATERIALIZE))
                        .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId))
                        .build();
                Session on = Session.builder(off)
                        .setCatalogSessionProperty("ducklake", PUSHDOWN_TIMESTAMP_WITH_TIMEZONE, "true")
                        .build();
                String query = "SELECT id FROM test_schema.tier_c_to_unixtime "
                        + "WHERE to_unixtime(ts) = 1.0";

                MaterializedResult offResult = computeActual(off, query);
                MaterializedResult onResult = computeActual(on, query);

                assertThat(offResult.getRowCount())
                        .as("zone=%s, property off: epoch=1 picks the 1970-01-01 row", zoneId).isEqualTo(1);
                assertThat(offResult.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);

                assertThat(onResult.getMaterializedRows())
                        .as("zone=%s: to_unixtime() pushed result must match Trino above-scan result "
                                + "(to_unixtime is zone-invariant in both engines)", zoneId)
                        .isEqualTo(offResult.getMaterializedRows());
            }
        }
        finally {
            tryDropTable("test_schema.tier_c_to_unixtime");
        }
    }

    @Test
    public void testTierCZoneDependentExtractsNeverPushOverWtz()
    {
        // Companion to the to_unixtime test above. The chunk-3 ship explicitly
        // refuses to push zone-dependent extracts (year/month/day/hour/...) over
        // TIMESTAMP WITH TIME ZONE columns even when the property is on. Reason:
        // the connector's Arrow converter hardcodes UTC_KEY for incoming WTZ
        // values, so Trino above-scan uses UTC year while DuckDB-side would use
        // the session zone — they diverge whenever the session is non-UTC and
        // pushing would silently drop rows Trino would keep.
        //
        // This test pins that the visible result is invariant under the
        // property toggle for a year() query — proving the gate correctly
        // refuses to push.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.tier_c_year_no_push ("
                        + "  id INTEGER, "
                        + "  ts TIMESTAMP(3) WITH TIME ZONE)");
        computeActual(writeDuckDbSession(),
                "INSERT INTO test_schema.tier_c_year_no_push VALUES "
                        + "(1, TIMESTAMP '2024-06-15 12:00:00.000 UTC'), "
                        + "(2, TIMESTAMP '2024-12-31 22:00:00.000 UTC')");
        try {
            for (String zoneId : new String[]{"UTC", "America/Los_Angeles", "Asia/Singapore"}) {
                Session off = Session.builder(sessionWith(READ_MODE_MATERIALIZE))
                        .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(zoneId))
                        .build();
                Session on = Session.builder(off)
                        .setCatalogSessionProperty("ducklake", PUSHDOWN_TIMESTAMP_WITH_TIMEZONE, "true")
                        .build();
                String query = "SELECT id FROM test_schema.tier_c_year_no_push "
                        + "WHERE year(ts) = 2024 ORDER BY id";

                MaterializedResult offResult = computeActual(off, query);
                MaterializedResult onResult = computeActual(on, query);

                assertThat(onResult.getMaterializedRows())
                        .as("zone=%s: year(WTZ) must NOT push even when property on — gate refuses "
                                + "the WTZ arg type; visible result invariant under toggle", zoneId)
                        .isEqualTo(offResult.getMaterializedRows());
            }
        }
        finally {
            tryDropTable("test_schema.tier_c_year_no_push");
        }
    }

    @Test
    public void testFromUnixtimeAndWithTimezonePushdownEndToEnd()
    {
        // Chunk-4 Tier C extras: from_unixtime(double) → WTZ and
        // with_timezone(TIMESTAMP, varchar) → WTZ. Both push as macros to DuckDB's
        // to_timestamp() / timezone() respectively. Wrap the WTZ output in
        // to_unixtime() (chunk-3 zone-invariant pushable) for the predicate value
        // so the whole expression composes without needing a WTZ-literal constant.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.fn_unixtime_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, DOUBLE '1718452800.0'), "  // 2024-06-15 12:00:00 UTC
                        + "  (2, DOUBLE '0.0'), "          // 1970 epoch
                        + "  (3, DOUBLE '-1.0')"           // pre-epoch
                        + ") AS t(id, epoch_d)");
        try {
            // from_unixtime → WTZ → to_unixtime → DOUBLE: identity. The intermediate
            // WTZ value goes through DuckDB's to_timestamp() and the round-trip
            // exposes any byte mishandling.
            MaterializedResult roundtrip = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.fn_unixtime_pushdown "
                            + "WHERE to_unixtime(from_unixtime(epoch_d)) = 1718452800.0");
            assertThat(roundtrip.getRowCount()).isEqualTo(1);
            assertThat(roundtrip.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.fn_unixtime_pushdown");
        }
    }

    @Test
    public void testWithTimezonePushdownEndToEnd()
    {
        // with_timezone(TIMESTAMP, varchar) attaches a zone to a wall-clock. Verify
        // by extracting the epoch back: the wall-clock 2024-06-15 12:00:00
        // interpreted in 'America/Los_Angeles' is the instant 1718478000 UTC
        // (12:00 in LA on that day is 19:00 UTC due to UTC-7 in summer).
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.fn_with_tz_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, TIMESTAMP '2024-06-15 12:00:00'), "
                        + "  (2, TIMESTAMP '2024-06-15 13:00:00')"
                        + ") AS t(id, ts)");
        try {
            // 2024-06-15 12:00 LA wall-clock = 2024-06-15 19:00 UTC (PDT, UTC-7).
            MaterializedResult la = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.fn_with_tz_pushdown "
                            + "WHERE to_unixtime(with_timezone(ts, 'America/Los_Angeles')) = 1718478000.0");
            assertThat(la.getRowCount()).isEqualTo(1);
            assertThat(la.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.fn_with_tz_pushdown");
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
