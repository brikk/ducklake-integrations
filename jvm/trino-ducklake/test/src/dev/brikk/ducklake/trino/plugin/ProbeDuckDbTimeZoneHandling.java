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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Probe answering the four open questions parked at the bottom of
 * {@code dev-docs/PLAN-pushdown-datetime.md}. Findings are written into
 * {@code dev-docs/REPORT-datetime-tz-handling.md}. This class follows the
 * {@code ProbeConcatNullHandling} / {@code ProbeHashNullHandling} precedent:
 * each test method prints a labelled result block to stdout; the test asserts
 * nothing (its purpose is empirical discovery, not regression). Delete after
 * the report is committed.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class ProbeDuckDbTimeZoneHandling
{
    /**
     * Q3 — What does a fresh {@code jdbc:duckdb:} connection default to for
     * {@code TimeZone}? Answer depends on the platform's system TZ; we want to
     * know whether DuckDB picks it up or hard-defaults to UTC.
     */
    @Test
    public void probeQ3_embeddedJdbcDefaultTimezone() throws Exception
    {
        System.out.println("\n=== Q3: jdbc:duckdb: default TimeZone ===");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            print(stmt, "current_setting('TimeZone')",
                    "SELECT current_setting('TimeZone')");
            print(stmt, "current_setting('Calendar')",
                    "SELECT current_setting('Calendar')");
            // Render a fixed instant in the default zone — gives a human-readable
            // sanity check that the session zone matches what current_setting says.
            print(stmt, "fixed-instant render",
                    "SELECT TIMESTAMPTZ '2024-06-15 12:00:00+00'");
            // Java system TZ for reference; if DuckDB picks it up, these should align.
            System.out.printf("  java.time.ZoneId.systemDefault() = %s%n",
                    java.time.ZoneId.systemDefault().getId());
        }
    }

    /**
     * Q2 — Does {@code SET TimeZone} work without ICU loaded? Probe the three
     * shapes a Trino session zone can take: fixed-offset ({@code +05:00}),
     * named UTC, and a full IANA zone with DST history.
     */
    @Test
    public void probeQ2_setTimezoneWithoutIcu() throws Exception
    {
        // DuckDB's JDBC driver closes the underlying Statement after a SET
        // failure, so each probe needs its own fresh Statement (or fresh
        // Connection) to get an independent answer. Per-zone fresh connection
        // is simplest and removes any cross-test ICU-state leak.
        System.out.println("\n=== Q2: SET TimeZone WITHOUT ICU loaded ===");
        for (String zone : List.of(
                "UTC", "+00:00", "+05:00", "-08:00",
                "America/Los_Angeles", "Europe/Berlin",
                "Asia/Singapore", "Pacific/Chatham",
                "GMT", "EST")) {
            trySetFreshConnection(zone, false);
        }

        System.out.println("\n=== Q2 follow-up: SET TimeZone AFTER INSTALL icu; LOAD icu ===");
        for (String zone : List.of(
                "UTC", "+00:00", "+05:00", "-08:00",
                "America/Los_Angeles", "Europe/Berlin",
                "Asia/Singapore", "Pacific/Chatham",
                "GMT", "EST")) {
            trySetFreshConnection(zone, true);
        }
    }

    private static void trySetFreshConnection(String zone, boolean loadIcu)
    {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            if (loadIcu) {
                try { stmt.execute("INSTALL icu"); stmt.execute("LOAD icu"); }
                catch (SQLException e) { /* best-effort */ }
            }
            String sql = "SET TimeZone = '" + zone + "'";
            try {
                stmt.execute(sql);
                try (ResultSet rs = stmt.executeQuery("SELECT current_setting('TimeZone')")) {
                    rs.next();
                    System.out.printf("  [OK  ] %-45s -> current=%s%n", sql, rs.getObject(1));
                }
            }
            catch (SQLException e) {
                System.out.printf("  [FAIL] %-45s -> %s%n", sql, firstLine(e.getMessage()));
            }
        }
        catch (SQLException e) {
            System.out.printf("  [FAIL] open-connection -> %s%n", e.getMessage());
        }
    }

    private static String firstLine(String s)
    {
        if (s == null) return "<null>";
        int nl = s.indexOf('\n');
        return nl < 0 ? s : s.substring(0, nl);
    }

    /**
     * Q1 — When a TIMESTAMPTZ column is stored in a DuckDB database file
     * (the duckdb-format storage shape used by the connector), does the
     * stored value preserve its original zone, or does it become an instant
     * that the reading session interprets through its own {@code TimeZone}?
     *
     * <p>Test: write the same instant from three sessions with different
     * {@code TimeZone} settings, then read all rows from a fourth session
     * with yet another zone. Compare what {@code year()}/{@code hour()}
     * return for each row.
     */
    @Test
    public void probeQ1_duckdbTimestamptzStorageRoundtrip() throws Exception
    {
        System.out.println("\n=== Q1: DuckDB TIMESTAMPTZ storage round-trip ===");
        Path dbFile = Files.createTempFile("probe-tztz-", ".db");
        Files.deleteIfExists(dbFile);

        // Write three rows of the same wall-clock instant from sessions with
        // different TimeZone settings. The question is whether the stored row
        // remembers which zone wrote it, or just stores an instant.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbFile);
                Statement stmt = conn.createStatement()) {
            tryExec(stmt, "INSTALL icu");
            tryExec(stmt, "LOAD icu");
            stmt.execute("CREATE TABLE wtz_probe (label VARCHAR, ts TIMESTAMPTZ, "
                    + "ts_instant TIMESTAMPTZ, ts_local TIMESTAMP)");

            writeRow(stmt, "wrote_from_utc",       "UTC",
                    "TIMESTAMPTZ '2024-06-15 12:00:00+00'",
                    "TIMESTAMP '2024-06-15 12:00:00'");
            writeRow(stmt, "wrote_from_la",        "America/Los_Angeles",
                    "TIMESTAMPTZ '2024-06-15 05:00:00-07'",  // same instant as 12:00 UTC
                    "TIMESTAMP '2024-06-15 12:00:00'");
            writeRow(stmt, "wrote_from_singapore", "Asia/Singapore",
                    "TIMESTAMPTZ '2024-06-15 20:00:00+08'",  // same instant as 12:00 UTC
                    "TIMESTAMP '2024-06-15 12:00:00'");
        }

        // Read from a fresh connection, in three different session zones, and
        // observe what year/hour each row reports.
        for (String readerZone : List.of("UTC", "America/Los_Angeles", "Asia/Singapore")) {
            System.out.printf("%n--- reader session TimeZone = %s ---%n", readerZone);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbFile);
                    Statement stmt = conn.createStatement()) {
                tryExec(stmt, "INSTALL icu");
                tryExec(stmt, "LOAD icu");
                trySet(stmt, readerZone);

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT label, "
                                + "ts::VARCHAR AS ts_rendered, "
                                + "year(ts) AS y, hour(ts) AS h, day(ts) AS d, "
                                + "ts_instant::VARCHAR AS ts_instant_rendered, "
                                + "year(ts_instant) AS yi, "
                                + "ts_local::VARCHAR AS ts_local_rendered, "
                                + "year(ts_local) AS yl, hour(ts_local) AS hl "
                                + "FROM wtz_probe")) {
                    int cols = rs.getMetaData().getColumnCount();
                    StringBuilder header = new StringBuilder("  ");
                    for (int i = 1; i <= cols; i++) {
                        if (i > 1) header.append(" | ");
                        header.append(rs.getMetaData().getColumnLabel(i));
                    }
                    System.out.println(header);
                    while (rs.next()) {
                        StringBuilder row = new StringBuilder("  ");
                        for (int i = 1; i <= cols; i++) {
                            if (i > 1) row.append(" | ");
                            row.append(rs.getObject(i));
                        }
                        System.out.println(row);
                    }
                }
            }
        }

        Files.deleteIfExists(dbFile);
    }

    /**
     * Q1 sister probe — does DuckLake-format storage (full DuckLake extension
     * over a DuckDB metadata DB + parquet data path) change the round-trip
     * answer? Skips cleanly if the {@code ducklake} extension isn't available
     * for INSTALL/LOAD on this DuckDB version.
     */
    @Test
    public void probeQ1b_ducklakeFormatRoundtrip() throws Exception
    {
        System.out.println("\n=== Q1b: DuckLake-format TIMESTAMPTZ round-trip ===");
        Path metaDb = Files.createTempFile("probe-tztz-lake-meta-", ".db");
        Path dataDir = Files.createTempDirectory("probe-tztz-lake-data-");
        Files.deleteIfExists(metaDb);

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            tryExec(stmt, "INSTALL ducklake");
            tryExec(stmt, "LOAD ducklake");
            tryExec(stmt, "INSTALL icu");
            tryExec(stmt, "LOAD icu");
            // ATTACH-as-DuckLake; if extension missing, this throws and we skip.
            try {
                stmt.execute(String.format(
                        "ATTACH 'ducklake:%s' AS lake (DATA_PATH '%s/')",
                        metaDb.toAbsolutePath(),
                        dataDir.toAbsolutePath()));
            }
            catch (SQLException e) {
                System.out.println("  SKIP — ducklake extension unavailable: " + e.getMessage());
                return;
            }

            stmt.execute("CREATE TABLE lake.main.wtz_lake (label VARCHAR, ts TIMESTAMPTZ)");

            // Write from three different writer-session TZs.
            for (String writerZone : List.of("UTC", "America/Los_Angeles", "Asia/Singapore")) {
                trySet(stmt, writerZone);
                stmt.execute(String.format(
                        "INSERT INTO lake.main.wtz_lake VALUES ('wrote_from_%s', "
                                + "TIMESTAMPTZ '2024-06-15 12:00:00+00')",
                        writerZone.replace('/', '_')));
            }
        }

        for (String readerZone : List.of("UTC", "America/Los_Angeles", "Asia/Singapore")) {
            System.out.printf("%n--- DuckLake reader session TimeZone = %s ---%n", readerZone);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement stmt = conn.createStatement()) {
                tryExec(stmt, "INSTALL ducklake");
                tryExec(stmt, "LOAD ducklake");
                tryExec(stmt, "INSTALL icu");
                tryExec(stmt, "LOAD icu");
                stmt.execute(String.format(
                        "ATTACH 'ducklake:%s' AS lake (DATA_PATH '%s/', READ_ONLY)",
                        metaDb.toAbsolutePath(),
                        dataDir.toAbsolutePath()));
                trySet(stmt, readerZone);

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT label, ts::VARCHAR AS ts_rendered, "
                                + "year(ts) AS y, hour(ts) AS h FROM lake.main.wtz_lake")) {
                    int cols = rs.getMetaData().getColumnCount();
                    StringBuilder header = new StringBuilder("  ");
                    for (int i = 1; i <= cols; i++) {
                        if (i > 1) header.append(" | ");
                        header.append(rs.getMetaData().getColumnLabel(i));
                    }
                    System.out.println(header);
                    while (rs.next()) {
                        StringBuilder row = new StringBuilder("  ");
                        for (int i = 1; i <= cols; i++) {
                            if (i > 1) row.append(" | ");
                            row.append(rs.getObject(i));
                        }
                        System.out.println(row);
                    }
                }
            }
        }

        // Cleanup
        deleteRecursive(dataDir);
        Files.deleteIfExists(metaDb);
    }

    /**
     * Q4 — Does the in-process driver behave identically to the Quack
     * (separate-process) executor for {@code SET TimeZone} + extract? We can
     * only check the in-process side directly here; the Quack side is run by
     * the broader test suite. Probe verifies the in-process semantics that the
     * REPORT will claim Quack also satisfies, and lists the assumed parity
     * points so the Q4 follow-up has a checklist to run inside the Quack
     * container.
     */
    @Test
    public void probeQ4_inProcessExtractSemantics() throws Exception
    {
        System.out.println("\n=== Q4: in-process SET TimeZone + extract semantics ===");
        for (String zone : List.of("UTC", "America/Los_Angeles", "Asia/Singapore")) {
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement stmt = conn.createStatement()) {
                tryExec(stmt, "INSTALL icu");
                tryExec(stmt, "LOAD icu");
                trySet(stmt, zone);

                System.out.printf("%n--- session TimeZone = %s ---%n", zone);
                // Same instant; year/hour depend on session TZ for TIMESTAMPTZ,
                // and DO NOT depend on session TZ for TIMESTAMP.
                print(stmt, "year(TIMESTAMPTZ instant) — TZ-sensitive",
                        "SELECT year(TIMESTAMPTZ '2024-12-31 22:00:00+00')");
                print(stmt, "hour(TIMESTAMPTZ instant) — TZ-sensitive",
                        "SELECT hour(TIMESTAMPTZ '2024-12-31 22:00:00+00')");
                print(stmt, "year(TIMESTAMP wall clock) — TZ-invariant",
                        "SELECT year(TIMESTAMP '2024-12-31 22:00:00')");
                print(stmt, "hour(TIMESTAMP wall clock) — TZ-invariant",
                        "SELECT hour(TIMESTAMP '2024-12-31 22:00:00')");
                print(stmt, "year(DATE) — TZ-invariant",
                        "SELECT year(DATE '2024-12-31')");
            }
        }
    }

    /**
     * Q2 follow-up — Etc/GMT translation table.
     *
     * <p>POSIX-style zones: {@code Etc/GMT-5} means UTC+5 (sign inverted!),
     * {@code Etc/GMT+8} means UTC-8. Probe whether DuckDB accepts these names,
     * what they actually resolve to (via {@code TIMESTAMPTZ} render), and how
     * non-zero-minute offsets behave (India = UTC+05:30 — no integer-hour
     * {@code Etc/GMT-N} can express this).
     */
    @Test
    public void probeQ2b_etcGmtTranslation() throws Exception
    {
        System.out.println("\n=== Q2b: Etc/GMT translation table (probe POSIX sign convention) ===");
        // Reference instant: '2024-06-15 12:00:00+00'. Reading in each zone, the
        // rendered offset tells us what zone the name resolved to.
        for (String zone : List.of(
                "Etc/GMT",       // UTC
                "Etc/GMT0",      // UTC alt form
                "Etc/GMT-5",     // expected: UTC+05:00 (POSIX inversion)
                "Etc/GMT+5",     // expected: UTC-05:00
                "Etc/GMT+8",     // expected: UTC-08:00
                "Etc/GMT-8",     // expected: UTC+08:00
                "Etc/GMT-12",    // expected: UTC+12:00
                "Etc/GMT+12",    // expected: UTC-12:00
                "Etc/GMT-14",    // expected: UTC+14:00 (Kiribati)
                "Etc/GMT-5:30",  // expected: probably FAIL — integer-hour only
                "Etc/UTC")) {    // expected: UTC alias
            probeRenderInZone(zone);
        }
    }

    private static void probeRenderInZone(String zone)
    {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            String setSql = "SET TimeZone = '" + zone + "'";
            try {
                stmt.execute(setSql);
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT current_setting('TimeZone'), "
                                + "(TIMESTAMPTZ '2024-06-15 12:00:00+00')::VARCHAR, "
                                + "year(TIMESTAMPTZ '2024-12-31 22:00:00+00'), "
                                + "hour(TIMESTAMPTZ '2024-12-31 22:00:00+00')")) {
                    rs.next();
                    System.out.printf("  [OK  ] %-25s -> set=%s | render=%s | yearBoundary=%s | hour=%s%n",
                            zone, rs.getObject(1), rs.getObject(2), rs.getObject(3), rs.getObject(4));
                }
            }
            catch (SQLException e) {
                System.out.printf("  [FAIL] %-25s -> %s%n", zone, firstLine(e.getMessage()));
            }
        }
        catch (SQLException e) {
            System.out.printf("  [FAIL] %-25s -> open-connection: %s%n", zone, e.getMessage());
        }
    }

    /**
     * Q3 follow-up — End-to-end "read Trino TZ → normalise → SET DuckDB TZ" loop.
     *
     * <p>Simulates the connector flow: take a {@code TimeZoneKey}-shaped string
     * (named IANA or {@code +HH:MM} offset), normalise it to a DuckDB-acceptable
     * string, and verify (a) {@code SET TimeZone} succeeds and (b) the resulting
     * extract matches {@code java.time}'s ground truth for the same instant.
     * The normaliser implemented here is the candidate for the production code path.
     */
    @Test
    public void probeQ3b_trinoToDuckDbTzPropagation() throws Exception
    {
        System.out.println("\n=== Q3b: Trino TimeZoneKey → DuckDB SET TimeZone normalisation ===");
        // Reference instant for extract probes.
        Instant referenceInstant = Instant.parse("2024-12-31T22:00:00Z");

        // Curated set of zone strings Trino's TimeZoneKey can produce:
        // named IANA, fixed offsets, UTC aliases, and one non-integer-hour offset
        // that must EITHER round-trip via the named-IANA fallback or be refused.
        List<String> trinoZoneStrings = List.of(
                "UTC", "GMT", "Z",
                "America/Los_Angeles", "Europe/Berlin", "Asia/Singapore",
                "Asia/Kolkata",     // India — UTC+05:30; only named-IANA can express
                "Pacific/Chatham",  // UTC+12:45 — even more fractional
                "+00:00", "+05:00", "-08:00", "+14:00",
                "+05:30",           // tricky: fractional offset, can't translate to Etc/GMT-N
                "-03:30");          // also fractional

        for (String trinoZone : trinoZoneStrings) {
            String normalised = normaliseTrinoZoneForDuckDb(trinoZone);
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement stmt = conn.createStatement()) {

                String setSql = "SET TimeZone = '" + normalised + "'";
                try {
                    stmt.execute(setSql);
                    try (ResultSet rs = stmt.executeQuery(
                            "SELECT year(TIMESTAMPTZ '2024-12-31 22:00:00+00'), "
                                    + "hour(TIMESTAMPTZ '2024-12-31 22:00:00+00'), "
                                    + "current_setting('TimeZone')")) {
                        rs.next();
                        int duckYear = ((Number) rs.getObject(1)).intValue();
                        int duckHour = ((Number) rs.getObject(2)).intValue();
                        String duckResolved = (String) rs.getObject(3);

                        // Ground truth: java.time.
                        ZonedDateTime java;
                        try {
                            java = referenceInstant.atZone(ZoneId.of(trinoZone));
                        }
                        catch (java.time.DateTimeException e) {
                            java = null;
                        }

                        if (java != null) {
                            boolean match = (java.getYear() == duckYear)
                                    && (java.getHour() == duckHour);
                            System.out.printf("  [%s] trino=%-22s norm=%-22s duck=(year=%d hr=%d setting=%s) java=(year=%d hr=%d zone=%s)%n",
                                    match ? "MATCH" : "DIVERGE",
                                    trinoZone, normalised,
                                    duckYear, duckHour, duckResolved,
                                    java.getYear(), java.getHour(), java.getZone().getId());
                        }
                        else {
                            System.out.printf("  [JTIME-REJECT] trino=%-22s norm=%-22s duck=(year=%d hr=%d setting=%s)%n",
                                    trinoZone, normalised,
                                    duckYear, duckHour, duckResolved);
                        }
                    }
                }
                catch (SQLException e) {
                    System.out.printf("  [DUCK-FAIL] trino=%-22s norm=%-22s -> %s%n",
                            trinoZone, normalised, firstLine(e.getMessage()));
                }
            }
        }
    }

    /**
     * Candidate normaliser the production code path could use. Translates a
     * Trino-style zone string into one DuckDB accepts.
     *
     * <p>Rules:
     * <ul>
     *   <li>{@code Z} → {@code UTC}.
     *   <li>{@code +HH:MM} / {@code -HH:MM} with {@code MM == 00} → {@code Etc/GMT∓HH}
     *       (POSIX sign inversion).
     *   <li>{@code +HH:MM} / {@code -HH:MM} with {@code MM != 00} → pass through unchanged;
     *       DuckDB will reject and the caller falls back to "don't push".
     *   <li>Everything else (named IANA, {@code UTC}, {@code GMT}) → pass through.
     * </ul>
     */
    private static String normaliseTrinoZoneForDuckDb(String trinoZone)
    {
        if ("Z".equals(trinoZone)) {
            return "UTC";
        }
        if (trinoZone.length() == 6
                && (trinoZone.charAt(0) == '+' || trinoZone.charAt(0) == '-')
                && trinoZone.charAt(3) == ':') {
            int hours = Integer.parseInt(trinoZone.substring(1, 3));
            int mins = Integer.parseInt(trinoZone.substring(4, 6));
            if (mins == 0) {
                // POSIX inversion: UTC+5 → Etc/GMT-5
                char invertedSign = (trinoZone.charAt(0) == '+') ? '-' : '+';
                return "Etc/GMT" + invertedSign + hours;
            }
            // Non-integer-hour offset: no Etc/GMT translation; leave as-is so DuckDB
            // rejects it cleanly and the caller decides whether to skip pushdown.
            return trinoZone;
        }
        return trinoZone;
    }

    /**
     * Q4 follow-up — Quack-container parity for SET TimeZone + extract.
     *
     * <p>Spins {@link TestingDucklakeQuackEngineServer} (Testcontainers), opens
     * a local JDBC connection wired to it via the quack extension, then ships
     * {@code SET TimeZone = '...'} and {@code SELECT year(...)} statements
     * server-side through {@code quack_query_by_name}. Asserts the same
     * three-zone results in-process baseline produced.
     */
    @Test
    public void probeQ4b_quackContainerParity() throws Exception
    {
        System.out.println("\n=== Q4b: Quack container SET TimeZone + extract parity ===");
        java.nio.file.Path sharedDir = Files.createTempDirectory("probe-quack-tz-");
        try (TestingDucklakeQuackEngineServer quack =
                new TestingDucklakeQuackEngineServer(sharedDir)) {
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                    Statement stmt = conn.createStatement()) {
                stmt.execute("INSTALL quack");
                stmt.execute("LOAD quack");
                stmt.execute("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '"
                        + quack.getToken().replace("'", "''") + "')");
                stmt.execute(String.format(
                        "ATTACH 'quack:%s:%d' AS engine (disable_ssl true)",
                        quack.getHost(), quack.getMappedPort()));

                // Three sessions, each setting the server-side zone and reading
                // back year/hour of the year-boundary smoking-gun instant.
                for (String zone : List.of("UTC", "America/Los_Angeles", "Asia/Singapore")) {
                    quackExec(stmt, "SET TimeZone = '" + zone + "'");
                    Object[] result = quackScalarRow(stmt,
                            "SELECT current_setting('TimeZone'), "
                                    + "year(TIMESTAMPTZ '2024-12-31 22:00:00+00'), "
                                    + "hour(TIMESTAMPTZ '2024-12-31 22:00:00+00'), "
                                    + "year(TIMESTAMP '2024-12-31 22:00:00'), "
                                    + "hour(TIMESTAMP '2024-12-31 22:00:00')");
                    System.out.printf("  zone=%-22s | setting=%-22s | tz(year=%s hr=%s) | walltime(year=%s hr=%s)%n",
                            zone, result[0], result[1], result[2], result[3], result[4]);
                }
            }
        }
        finally {
            deleteRecursive(sharedDir);
        }
    }

    /** Ship SQL server-side via the quack_query_by_name wrapper. */
    private static void quackExec(Statement stmt, String innerSql) throws SQLException
    {
        String wrapped = "SELECT * FROM system.main.quack_query_by_name('engine', '"
                + innerSql.replace("'", "''") + "')";
        try (ResultSet rs = stmt.executeQuery(wrapped)) {
            while (rs.next()) {
                // discard
            }
        }
    }

    /** Run a single-row server-side SELECT and return its column values. */
    private static Object[] quackScalarRow(Statement stmt, String innerSql) throws SQLException
    {
        String wrapped = "SELECT * FROM system.main.quack_query_by_name('engine', '"
                + innerSql.replace("'", "''") + "')";
        try (ResultSet rs = stmt.executeQuery(wrapped)) {
            rs.next();
            int cols = rs.getMetaData().getColumnCount();
            Object[] out = new Object[cols];
            for (int i = 0; i < cols; i++) {
                out[i] = rs.getObject(i + 1);
            }
            return out;
        }
    }

    // --- helpers --------------------------------------------------------------

    private static void writeRow(Statement stmt, String label, String writerZone,
            String tsLiteral, String tsLocalLiteral) throws SQLException
    {
        trySet(stmt, writerZone);
        stmt.execute(String.format(
                "INSERT INTO wtz_probe VALUES ('%s', %s, %s, %s)",
                label, tsLiteral, tsLiteral, tsLocalLiteral));
    }

    private static void trySet(Statement stmt, String zone)
    {
        String sql = "SET TimeZone = '" + zone + "'";
        try {
            stmt.execute(sql);
            // Confirm the set actually took.
            try (ResultSet rs = stmt.executeQuery("SELECT current_setting('TimeZone')")) {
                rs.next();
                System.out.printf("  [OK  ] %-50s -> current=%s%n", sql, rs.getObject(1));
            }
        }
        catch (SQLException e) {
            System.out.printf("  [FAIL] %-50s -> %s%n", sql, e.getMessage());
        }
    }

    private static void tryExec(Statement stmt, String sql)
    {
        try {
            stmt.execute(sql);
            System.out.printf("  [OK  ] %s%n", sql);
        }
        catch (SQLException e) {
            System.out.printf("  [FAIL] %s -> %s%n", sql, e.getMessage());
        }
    }

    private static void print(Statement stmt, String label, String sql) throws SQLException
    {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            System.out.printf("  %-50s = %s%n", label, rs.getObject(1));
        }
    }

    private static void deleteRecursive(Path dir) throws java.io.IOException
    {
        if (!Files.exists(dir)) return;
        try (var stream = Files.walk(dir)) {
            List<Path> paths = new ArrayList<>(stream.toList());
            paths.sort((a, b) -> b.getNameCount() - a.getNameCount());
            for (Path p : paths) {
                Files.deleteIfExists(p);
            }
        }
    }
}
