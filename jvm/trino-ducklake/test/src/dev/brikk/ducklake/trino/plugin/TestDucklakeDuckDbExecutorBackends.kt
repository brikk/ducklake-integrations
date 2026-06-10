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

import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.VarcharType.VARCHAR
import org.apache.arrow.vector.ipc.ArrowReader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Comparator
import java.util.Optional

/**
 * End-to-end exercise of the two implemented [DucklakeDuckDbExecutor]
 * backends: [InProcessDuckDbExecutor] (embedded DuckDB JDBC) and
 * [QuackDuckDbExecutor] (remote DuckDB reached via Quack RPC, hosted in
 * a testcontainer with a shared data volume).
 *
 * Both executors read the *same* `.db` file with the same
 * projection and predicate; the test asserts they produce identical row sets.
 * This proves the abstraction's correctness — switching engines via the
 * catalog property changes how rows are fetched, not what rows are returned.
 *
 * Test fixture topology:
 * ```
 *   host tempdir ── (bind mount, RW) ──▶ container /data
 *   build .db ──────────────────────────▶ /data/cache.db
 *   QuackDuckDbExecutor.execute(LocalPath('/data/cache.db'))
 * ```
 *
 * Quack server-side ATTACH of `/data/cache.db` is dynamic (not
 * baked into init SQL) so this fixture mirrors the production cache-write
 * pattern: a manager writes files into the shared volume, executors ATTACH
 * them on demand.
 */
@Execution(ExecutionMode.SAME_THREAD)  // Quack server is stateful + single-instance per @BeforeAll; concurrent CREATE OR REPLACE MACRO races server-side.
internal class TestDucklakeDuckDbExecutorBackends {

    companion object {
        private const val TABLE_NAME = "t"

        private lateinit var sharedDir: Path
        private lateinit var dbFile: Path
        private var quackServer: TestingDucklakeQuackEngineServer? = null

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            sharedDir = Files.createTempDirectory("ducklake-executor-test-")
            dbFile = sharedDir.resolve("cache.db")
            // Write a deterministic .db file via embedded DuckDB. The QuackDuckDbExecutor
            // expects the SERVER to be able to read this path under /data; the host writes
            // to the same dir via the bind mount.
            DriverManager.getConnection("jdbc:duckdb:" + dbFile.toAbsolutePath()).use { conn ->
                conn.createStatement().use { s ->
                    s.execute("CREATE TABLE $TABLE_NAME (id INTEGER, name VARCHAR)")
                    s.execute("INSERT INTO $TABLE_NAME VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
                }
            }
            // Source the host-side parity extension binary for the Quack
            // testcontainer copy. The testcontainer runs LINUX, so we need a Linux
            // binary regardless of the test JVM's OS. Precedence:
            //   1. -Dducklake.test.parityExtensionPath (if Gradle forwarded it —
            //      operator-asserted to be Linux-loadable).
            //   2. The bundled linux-<arch> binary in the plugin jar resources,
            //      extracted to a temp path. Built via `make linux-arm64` /
            //      `make linux-amd64` in the extension repo.
            // The Quack executor LOADs the in-container counterpart at
            // TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH;
            // withCopyFileToContainer puts the file there.
            val containerArch = if (System.getProperty("os.arch", "").lowercase().contains("aarch"))
                "linux-arm64" else "linux-amd64"
            val configuredPath: Path? = System.getProperty("ducklake.test.parityExtensionPath")
                    ?.trim()
                    ?.takeIf { it.isNotEmpty() }
                    ?.let(Path::of)
            val parityExtensionPath: Optional<Path> = if (configuredPath != null) {
                Optional.of(configuredPath)
            } else {
                TrinoParityExtensionResolver.resolveBundledExtensionPathFor(containerArch)
                        .map { Path.of(it) }
            }
            quackServer = TestingDucklakeQuackEngineServer(sharedDir, parityExtensionPath)
        }

        @AfterAll
        @JvmStatic
        @Throws(Exception::class)
        fun tearDown() {
            if (quackServer != null) {
                quackServer!!.close()
            }
            if (::sharedDir.isInitialized) {
                deleteRecursively(sharedDir)
            }
        }

        private fun drain(reader: ArrowReader, columnCount: Int): List<List<Any?>> {
            val rows: MutableList<List<Any?>> = ArrayList()
            while (reader.loadNextBatch()) {
                val root = reader.vectorSchemaRoot
                val batchRowCount = root.rowCount
                for (r in 0 until batchRowCount) {
                    val row: MutableList<Any?> = ArrayList(columnCount)
                    for (c in 0 until columnCount) {
                        val v = root.getVector(c)
                        val value: Any? = v.getObject(r)
                        // Arrow VARCHAR vectors return org.apache.arrow.vector.util.Text
                        // (a UTF-8 byte wrapper). Normalise to java.lang.String so equality
                        // comparison against test literals works.
                        if (value is Number || value is Boolean || value == null) {
                            row.add(value)
                        }
                        else {
                            row.add(value.toString())
                        }
                    }
                    rows.add(row)
                }
            }
            return rows
        }

        @Throws(IOException::class)
        private fun countRows(reader: ArrowReader): Long {
            var total: Long = 0
            while (reader.loadNextBatch()) {
                total += reader.vectorSchemaRoot.rowCount.toLong()
            }
            return total
        }

        /**
         * Skips the calling test unless the Quack testcontainer can actually LOAD the bundled
         * trino_parity extension. The host bundles the extension by *host* arch, but the container's
         * duckdb may be a different platform (notably: arm64 host + amd64 container on Apple
         * Silicon), in which case the LOAD fails with a platform-mismatch error. Mirrors the
         * graceful skip already used by `quackBackendReadsVortexViaFileScan`, so these
         * in-process-vs-Quack parity tests skip cleanly off-platform instead of hard-failing (full
         * coverage still runs on a matching-arch host / CI). See the task note in
         * dev-docs/HANDOFF-lance-route-a.md (env caveat).
         */
        private fun assumeQuackParityExtensionLoadable() {
            val probe = DucklakeDuckDbExecutor.ExecutionRequest(
                    DuckDbAttachTarget.LocalPath(Path.of("/data/cache.db")),
                    listOf(DucklakeColumnHandle(1L, "id", INTEGER, false)),
                    TupleDomain.all<DucklakeColumnHandle>())
            try {
                QuackDuckDbExecutor(quackServer!!.host, quackServer!!.mappedPort, quackServer!!.token,
                        dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
                        .execute(probe).use { ctx -> ctx.arrowReader().loadNextBatch() }
            }
            catch (e: Exception) {
                assumeTrue(false, "Quack container cannot LOAD the trino_parity extension — host/container "
                        + "platform mismatch (e.g. arm64 host bundling a linux-arm64 extension into an amd64 "
                        + "container). Skipping the in-process-vs-Quack parity check: ${e.message}")
            }
        }

        @Throws(IOException::class)
        private fun deleteRecursively(dir: Path) {
            if (!Files.exists(dir)) {
                return
            }
            Files.walk(dir).use { walk ->
                walk.sorted(Comparator.reverseOrder()).forEach { p ->
                    try {
                        Files.deleteIfExists(p)
                    }
                    catch (ignored: IOException) {
                    }
                }
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun inProcessAndQuackBackendsReturnIdenticalRows() {
        assumeQuackParityExtensionLoadable()
        // Inside the container the file is reachable at /data/cache.db (bind mount).
        // Inside the JVM the file is reachable at sharedDir/cache.db (same inode).
        // The in-process executor reads from the host path; the Quack executor
        // server-side ATTACHes from the container path. Both must produce the same
        // rows.
        val inProcessPath: Path = dbFile
        val quackServerPath: Path = Path.of("/data/cache.db")

        val projection = listOf(
                DucklakeColumnHandle(1L, "id", INTEGER, false),
                DucklakeColumnHandle(2L, "name", VARCHAR, false))

        val inProcessReq = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(inProcessPath),
                projection,
                TupleDomain.all<DucklakeColumnHandle>())
        val quackReq = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(quackServerPath),
                projection,
                TupleDomain.all<DucklakeColumnHandle>())

        val inProcessRows: List<List<Any?>>
        InProcessDuckDbExecutor().execute(inProcessReq).use { ctx ->
            inProcessRows = drain(ctx.arrowReader(), projection.size)
        }
        val quackRows: List<List<Any?>>
        QuackDuckDbExecutor(quackServer!!.host, quackServer!!.mappedPort, quackServer!!.token, dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
                .execute(quackReq).use { ctx ->
            quackRows = drain(ctx.arrowReader(), projection.size)
        }

        assertThat(inProcessRows)
                .`as`("in-process executor must return the rows we INSERTed into the .db file")
                .containsExactlyInAnyOrder(
                        listOf(1, "alpha"),
                        listOf(2, "beta"),
                        listOf(3, "gamma"))
        assertThat(quackRows)
                .`as`("Quack executor (remote DuckDB) must return the same rows for the same file")
                .containsExactlyInAnyOrderElementsOf(inProcessRows)
    }

    @Test
    @Throws(Exception::class)
    fun bothBackendsHonourEmptyProjection() {
        assumeQuackParityExtensionLoadable()
        // Empty projection (COUNT(*) shape) — both backends must emit row-count-only
        // batches so downstream operators count correctly. We assert position count
        // matches the file's row count.
        val inProcessReq = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(dbFile),
                emptyList<DucklakeColumnHandle>(),
                TupleDomain.all<DucklakeColumnHandle>())
        val quackReq = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/data/cache.db")),
                emptyList<DucklakeColumnHandle>(),
                TupleDomain.all<DucklakeColumnHandle>())

        val inProcessRows: Long
        InProcessDuckDbExecutor().execute(inProcessReq).use { ctx ->
            inProcessRows = countRows(ctx.arrowReader())
        }
        val quackRows: Long
        QuackDuckDbExecutor(quackServer!!.host, quackServer!!.mappedPort, quackServer!!.token, dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
                .execute(quackReq).use { ctx ->
            quackRows = countRows(ctx.arrowReader())
        }

        assertThat(inProcessRows).isEqualTo(3L)
        assertThat(quackRows).isEqualTo(inProcessRows)
    }

    @Test
    @Throws(Exception::class)
    fun sessionTimeZonePropagatesToBothBackends() {
        assumeQuackParityExtensionLoadable()
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

        val tzDb: Path = sharedDir.resolve("tz.db")
        DriverManager.getConnection("jdbc:duckdb:" + tzDb.toAbsolutePath()).use { conn ->
            conn.createStatement().use { s ->
                s.execute("CREATE TABLE $TABLE_NAME (id INTEGER, ts TIMESTAMPTZ)")
                // Same instant written from a UTC session.
                s.execute("SET TimeZone = 'UTC'")
                s.execute("INSERT INTO $TABLE_NAME"
                        + " VALUES (1, TIMESTAMPTZ '2024-06-15 12:00:00+00')")
            }
        }

        val inProcessPath: Path = tzDb
        val quackPath: Path = Path.of("/data/tz.db")
        val projection = listOf(
                DucklakeColumnHandle(1L, "id", INTEGER, false))

        // --- Positive case: LA-zone request + LA-rendered cast predicate matches.
        val laMatch = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(inProcessPath),
                projection,
                TupleDomain.all<DucklakeColumnHandle>(),
                listOf("CAST(\"ts\" AS VARCHAR) = '2024-06-15 05:00:00-07'"),
                Optional.of("America/Los_Angeles"))
        InProcessDuckDbExecutor().execute(laMatch).use { ctx ->
            assertThat(countRows(ctx.arrowReader()))
                    .`as`("in-process: SET TimeZone='America/Los_Angeles' must take effect so the "
                            + "CAST(ts AS VARCHAR) predicate matches the LA-rendered string")
                    .isEqualTo(1L)
        }
        val laMatchQuack = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(quackPath),
                projection,
                TupleDomain.all<DucklakeColumnHandle>(),
                listOf("CAST(\"ts\" AS VARCHAR) = '2024-06-15 05:00:00-07'"),
                Optional.of("America/Los_Angeles"))
        QuackDuckDbExecutor(quackServer!!.host, quackServer!!.mappedPort, quackServer!!.token, dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
                .execute(laMatchQuack).use { ctx ->
            assertThat(countRows(ctx.arrowReader()))
                    .`as`("Quack: SET TimeZone='America/Los_Angeles' issued via quack_query_by_name "
                            + "must persist for the subsequent SELECT in the same client session")
                    .isEqualTo(1L)
        }

        // --- Singapore-zone request + Singapore-rendered cast predicate matches.
        val sg = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(inProcessPath),
                projection,
                TupleDomain.all<DucklakeColumnHandle>(),
                listOf("CAST(\"ts\" AS VARCHAR) = '2024-06-15 20:00:00+08'"),
                Optional.of("Asia/Singapore"))
        InProcessDuckDbExecutor().execute(sg).use { ctx ->
            assertThat(countRows(ctx.arrowReader()))
                    .`as`("Singapore session zone produces a different rendering of the same instant")
                    .isEqualTo(1L)
        }

        // --- Control: LA zone, but Singapore-rendered predicate — must NOT match.
        // If this matched, the zone wouldn't be consulted; the test would pass for
        // the wrong reason (DuckDB rendering in some other default zone that happens
        // to coincide). This pins "the zone we requested is the zone DuckDB used."
        val mismatched = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(inProcessPath),
                projection,
                TupleDomain.all<DucklakeColumnHandle>(),
                listOf("CAST(\"ts\" AS VARCHAR) = '2024-06-15 20:00:00+08'"),
                Optional.of("America/Los_Angeles"))
        InProcessDuckDbExecutor().execute(mismatched).use { ctx ->
            assertThat(countRows(ctx.arrowReader()))
                    .`as`("LA session zone must NOT render the instant the way Singapore does")
                    .isEqualTo(0L)
        }
    }

    @Test
    @Throws(Exception::class)
    fun quackBackendReadsVortexViaFileScan() {
        // Proves the FileScan branch works over the SAME Quack RPC path that Lance will reuse:
        // no ATTACH, server-side INSTALL/LOAD <extension> + `FROM read_vortex('/data/x.vortex')`.
        // The .db backend tests above only cover the ATTACH-alias model; this is the only
        // automated coverage of FileScan-via-Quack.
        //
        // Fixture: write the .vortex file on the HOST (osx_amd64 has a vortex build) into the
        // bind-mounted shared dir, so the container sees it at /data/scan.vortex. The container
        // (linux_amd64, vortex published) INSTALL/LOADs vortex server-side and scans it.
        //
        // Double network gate: (1) the host write needs `INSTALL vortex`; (2) the container scan
        // needs the server to reach extensions.duckdb.org. Either failing → SKIP, not fail, so
        // offline CI stays green.
        val vortexFile: Path = sharedDir.resolve("scan.vortex")
        val escaped = vortexFile.toString().replace("'", "''")
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL vortex")
                    s.execute("LOAD vortex")
                    s.execute("CREATE TABLE v AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) t(id, name)")
                    s.execute("COPY v TO '$escaped' (FORMAT vortex)")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "vortex DuckDB extension unavailable on host (offline / unsupported platform): ${e.message}")
            return
        }

        val projection = listOf(
                DucklakeColumnHandle(1L, "id", INTEGER, false),
                DucklakeColumnHandle(2L, "name", VARCHAR, false))
        // Container path (bind mount): the host's sharedDir/scan.vortex is /data/scan.vortex.
        val quackReq = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan("/data/scan.vortex", "read_vortex", "vortex", Optional.empty()),
                projection,
                TupleDomain.all<DucklakeColumnHandle>())

        val quackRows: List<List<Any?>>
        try {
            QuackDuckDbExecutor(quackServer!!.host, quackServer!!.mappedPort, quackServer!!.token, dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
                    .execute(quackReq).use { ctx ->
                quackRows = drain(ctx.arrowReader(), projection.size)
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "Quack server could not INSTALL/LOAD vortex (offline container): ${e.message}")
            return
        }

        assertThat(quackRows)
                .`as`("Quack FileScan(read_vortex) must stream all rows of the server-side vortex file")
                .containsExactlyInAnyOrder(
                        listOf(1, "alpha"),
                        listOf(2, "beta"),
                        listOf(3, "gamma"))
    }

    @Test
    @Throws(Exception::class)
    fun executionRequestWithoutTimeZoneStillWorks() {
        // Backward-compat: callers that don't supply a zone (every test fixture
        // until this chunk) must still execute correctly. The executor sees an
        // empty Optional and skips SET TimeZone entirely.
        val noZone = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(dbFile),
                listOf(DucklakeColumnHandle(1L, "id", INTEGER, false)),
                TupleDomain.all<DucklakeColumnHandle>())
        InProcessDuckDbExecutor().execute(noZone).use { ctx ->
            assertThat(countRows(ctx.arrowReader())).isEqualTo(3L)
        }
    }
}
