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

import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end exercise of the two implemented {@link DucklakeDuckDbExecutor}
 * backends: {@link InProcessDuckDbExecutor} (embedded DuckDB JDBC) and
 * {@link QuackDuckDbExecutor} (remote DuckDB reached via Quack RPC, hosted in
 * a testcontainer with a shared data volume).
 *
 * <p>Both executors read the <em>same</em> {@code .db} file with the same
 * projection and predicate; the test asserts they produce identical row sets.
 * This proves the abstraction's correctness — switching engines via the
 * catalog property changes how rows are fetched, not what rows are returned.
 *
 * <p>Test fixture topology:
 * <pre>
 *   host tempdir ── (bind mount, RW) ──▶ container /data
 *   build .db ──────────────────────────▶ /data/cache.db
 *   QuackDuckDbExecutor.execute(LocalPath('/data/cache.db'))
 * </pre>
 *
 * <p>Quack server-side ATTACH of {@code /data/cache.db} is dynamic (not
 * baked into init SQL) so this fixture mirrors the production cache-write
 * pattern: a manager writes files into the shared volume, executors ATTACH
 * them on demand.
 */
@Execution(ExecutionMode.SAME_THREAD)  // Quack server is stateful + single-instance per @BeforeAll; concurrent CREATE OR REPLACE MACRO races server-side.
final class TestDucklakeDuckDbExecutorBackends
{
    private static final String TABLE_NAME = "t";

    private static Path sharedDir;
    private static Path dbFile;
    private static TestingDucklakeQuackEngineServer quackServer;

    @BeforeAll
    static void setUp() throws Exception
    {
        sharedDir = Files.createTempDirectory("ducklake-executor-test-");
        dbFile = sharedDir.resolve("cache.db");
        // Write a deterministic .db file via embedded DuckDB. The QuackDuckDbExecutor
        // expects the SERVER to be able to read this path under /data; the host writes
        // to the same dir via the bind mount.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbFile.toAbsolutePath());
                Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE " + TABLE_NAME + " (id INTEGER, name VARCHAR)");
            s.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");
        }
        // When -Dducklake.test.parityExtensionPath is set (forwarded by Gradle),
        // copy the extension binary into the engine container so the
        // testServerSideParityExtensionLoad case can exercise the LOAD path
        // through QuackDuckDbExecutor's 5-arg constructor.
        Optional<Path> parityExtensionPath = Optional
                .ofNullable(System.getProperty("ducklake.test.parityExtensionPath"))
                .map(String::strip)
                .filter(s -> !s.isEmpty())
                .map(Path::of);
        quackServer = new TestingDucklakeQuackEngineServer(sharedDir, parityExtensionPath);
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        if (quackServer != null) {
            quackServer.close();
        }
        if (sharedDir != null) {
            deleteRecursively(sharedDir);
        }
    }

    @Test
    void inProcessAndQuackBackendsReturnIdenticalRows() throws Exception
    {
        // Inside the container the file is reachable at /data/cache.db (bind mount).
        // Inside the JVM the file is reachable at sharedDir/cache.db (same inode).
        // The in-process executor reads from the host path; the Quack executor
        // server-side ATTACHes from the container path. Both must produce the same
        // rows.
        Path inProcessPath = dbFile;
        Path quackServerPath = Path.of("/data/cache.db");

        List<DucklakeColumnHandle> projection = List.of(
                new DucklakeColumnHandle(1, "id", INTEGER, false),
                new DucklakeColumnHandle(2, "name", VARCHAR, false));

        DucklakeDuckDbExecutor.ExecutionRequest inProcessReq = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(inProcessPath),
                projection,
                TupleDomain.all());
        DucklakeDuckDbExecutor.ExecutionRequest quackReq = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(quackServerPath),
                projection,
                TupleDomain.all());

        List<List<Object>> inProcessRows;
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new InProcessDuckDbExecutor().execute(inProcessReq)) {
            inProcessRows = drain(ctx.arrowReader(), projection.size());
        }
        List<List<Object>> quackRows;
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new QuackDuckDbExecutor(quackServer.getHost(), quackServer.getMappedPort(), quackServer.getToken())
                        .execute(quackReq)) {
            quackRows = drain(ctx.arrowReader(), projection.size());
        }

        assertThat(inProcessRows)
                .as("in-process executor must return the rows we INSERTed into the .db file")
                .containsExactlyInAnyOrder(
                        List.of(1, "alpha"),
                        List.of(2, "beta"),
                        List.of(3, "gamma"));
        assertThat(quackRows)
                .as("Quack executor (remote DuckDB) must return the same rows for the same file")
                .containsExactlyInAnyOrderElementsOf(inProcessRows);
    }

    @Test
    void bothBackendsHonourEmptyProjection() throws Exception
    {
        // Empty projection (COUNT(*) shape) — both backends must emit row-count-only
        // batches so downstream operators count correctly. We assert position count
        // matches the file's row count.
        DucklakeDuckDbExecutor.ExecutionRequest inProcessReq = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(dbFile),
                List.of(),
                TupleDomain.all());
        DucklakeDuckDbExecutor.ExecutionRequest quackReq = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(Path.of("/data/cache.db")),
                List.of(),
                TupleDomain.all());

        long inProcessRows;
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new InProcessDuckDbExecutor().execute(inProcessReq)) {
            inProcessRows = countRows(ctx.arrowReader());
        }
        long quackRows;
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new QuackDuckDbExecutor(quackServer.getHost(), quackServer.getMappedPort(), quackServer.getToken())
                        .execute(quackReq)) {
            quackRows = countRows(ctx.arrowReader());
        }

        assertThat(inProcessRows).isEqualTo(3L);
        assertThat(quackRows).isEqualTo(inProcessRows);
    }

    @Test
    void sessionTimeZonePropagatesToBothBackends() throws Exception
    {
        // End-to-end proof that ExecutionRequest.duckDbTimeZone causes both
        // executors to issue `SET TimeZone = '<zone>'` server-side after attach.
        //
        // Observation strategy: a pushed CAST(timestamptz_col AS VARCHAR)
        // predicate. The rendering of TIMESTAMPTZ in DuckDB depends on the
        // session TimeZone, so the predicate matches the row iff DuckDB actually
        // set the requested zone. A control case with the wrong rendering must
        // match zero rows — proves the zone is being consulted, not ignored.
        //
        // We don't go through Trino here (no QueryRunner); this is an
        // executor-level integration test that pins the wire-up.

        Path tzDb = sharedDir.resolve("tz.db");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + tzDb.toAbsolutePath());
                Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE " + TABLE_NAME + " (id INTEGER, ts TIMESTAMPTZ)");
            // Same instant written from a UTC session.
            s.execute("SET TimeZone = 'UTC'");
            s.execute("INSERT INTO " + TABLE_NAME
                    + " VALUES (1, TIMESTAMPTZ '2024-06-15 12:00:00+00')");
        }

        Path inProcessPath = tzDb;
        Path quackPath = Path.of("/data/tz.db");
        List<DucklakeColumnHandle> projection = List.of(
                new DucklakeColumnHandle(1, "id", INTEGER, false));

        // --- Positive case: LA-zone request + LA-rendered cast predicate matches.
        DucklakeDuckDbExecutor.ExecutionRequest laMatch = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(inProcessPath),
                projection,
                TupleDomain.all(),
                List.of("CAST(\"ts\" AS VARCHAR) = '2024-06-15 05:00:00-07'"),
                Optional.of("America/Los_Angeles"));
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new InProcessDuckDbExecutor().execute(laMatch)) {
            assertThat(countRows(ctx.arrowReader()))
                    .as("in-process: SET TimeZone='America/Los_Angeles' must take effect so the "
                            + "CAST(ts AS VARCHAR) predicate matches the LA-rendered string")
                    .isEqualTo(1L);
        }
        DucklakeDuckDbExecutor.ExecutionRequest laMatchQuack = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(quackPath),
                projection,
                TupleDomain.all(),
                List.of("CAST(\"ts\" AS VARCHAR) = '2024-06-15 05:00:00-07'"),
                Optional.of("America/Los_Angeles"));
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new QuackDuckDbExecutor(quackServer.getHost(), quackServer.getMappedPort(), quackServer.getToken())
                        .execute(laMatchQuack)) {
            assertThat(countRows(ctx.arrowReader()))
                    .as("Quack: SET TimeZone='America/Los_Angeles' issued via quack_query_by_name "
                            + "must persist for the subsequent SELECT in the same client session")
                    .isEqualTo(1L);
        }

        // --- Singapore-zone request + Singapore-rendered cast predicate matches.
        DucklakeDuckDbExecutor.ExecutionRequest sg = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(inProcessPath),
                projection,
                TupleDomain.all(),
                List.of("CAST(\"ts\" AS VARCHAR) = '2024-06-15 20:00:00+08'"),
                Optional.of("Asia/Singapore"));
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new InProcessDuckDbExecutor().execute(sg)) {
            assertThat(countRows(ctx.arrowReader()))
                    .as("Singapore session zone produces a different rendering of the same instant")
                    .isEqualTo(1L);
        }

        // --- Control: LA zone, but Singapore-rendered predicate — must NOT match.
        // If this matched, the zone wouldn't be consulted; the test would pass for
        // the wrong reason (DuckDB rendering in some other default zone that happens
        // to coincide). This pins "the zone we requested is the zone DuckDB used."
        DucklakeDuckDbExecutor.ExecutionRequest mismatched = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(inProcessPath),
                projection,
                TupleDomain.all(),
                List.of("CAST(\"ts\" AS VARCHAR) = '2024-06-15 20:00:00+08'"),
                Optional.of("America/Los_Angeles"));
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new InProcessDuckDbExecutor().execute(mismatched)) {
            assertThat(countRows(ctx.arrowReader()))
                    .as("LA session zone must NOT render the instant the way Singapore does")
                    .isEqualTo(0L);
        }
    }

    @Test
    void executionRequestWithoutTimeZoneStillWorks() throws Exception
    {
        // Backward-compat: callers that don't supply a zone (every test fixture
        // until this chunk) must still execute correctly. The executor sees an
        // empty Optional and skips SET TimeZone entirely.
        DucklakeDuckDbExecutor.ExecutionRequest noZone = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(dbFile),
                List.of(new DucklakeColumnHandle(1, "id", INTEGER, false)),
                TupleDomain.all());
        try (DucklakeDuckDbExecutor.ExecutionContext ctx =
                new InProcessDuckDbExecutor().execute(noZone)) {
            assertThat(countRows(ctx.arrowReader())).isEqualTo(3L);
        }
    }

    private static List<List<Object>> drain(ArrowReader reader, int columnCount) throws IOException
    {
        List<List<Object>> rows = new ArrayList<>();
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            int batchRowCount = root.getRowCount();
            for (int r = 0; r < batchRowCount; r++) {
                List<Object> row = new ArrayList<>(columnCount);
                for (int c = 0; c < columnCount; c++) {
                    FieldVector v = root.getVector(c);
                    Object value = v.getObject(r);
                    // Arrow VARCHAR vectors return org.apache.arrow.vector.util.Text
                    // (a UTF-8 byte wrapper). Normalise to java.lang.String so equality
                    // comparison against test literals works.
                    if (value instanceof Number || value instanceof Boolean || value == null) {
                        row.add(value);
                    }
                    else {
                        row.add(value.toString());
                    }
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static long countRows(ArrowReader reader) throws IOException
    {
        long total = 0;
        while (reader.loadNextBatch()) {
            total += reader.getVectorSchemaRoot().getRowCount();
        }
        return total;
    }

    private static void deleteRecursively(Path dir) throws IOException
    {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                }
                catch (IOException ignored) {
                }
            });
        }
    }
}
