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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.MountableFile
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Optional
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Regression test for the Quack-engine server-side init **write-write race**
 * (TODO-pushdown-duckdb "Known open bug"): concurrent queries with s3 targets each ship the
 * shared `ducklake_s3` secret CREATE (and, for `.db` targets, `ATTACH IF NOT EXISTS`) to the
 * SAME long-lived Quack server, and DuckDB 1.5.3 aborts overlapping catalog writes with
 * `Catalog write-write conflict on create/alter with "ducklake_s3"` (observed live 2026-06-10
 * with just two concurrent lance-s3 queries). Pre-fix, the statement was `CREATE OR REPLACE`,
 * whose drop+recreate window additionally left concurrently-binding scans secretless (vortex's
 * object_store then fell back to the EC2 metadata service and timed out). The fix this test
 * pins: `CREATE SECRET IF NOT EXISTS` (steady state = no catalog write at all) plus
 * [DuckDbCatalogWriteRetry] around the init statements for the first-contact storm — exercised
 * here with barrier-aligned concurrent executions of both racy target shapes:
 *
 *  - s3 [DuckDbAttachTarget.FileScan] (vortex) with an EXPLICIT `s3Config` — production vortex
 *    FileScans no longer carry the secret (`read_vortex` is object_store/env-credentialed, see
 *    the fixture comment), so this shape is built directly to keep racing the FileScan-branch
 *    secret+httpfs init statements; the rows coming back also make it the vortex-over-s3 quack
 *    read e2e (env channel);
 *  - [DuckDbAttachTarget.HttpfsS3] (`.db` over s3) — the production secret carrier: secret +
 *    shared-alias ATTACH, and the read itself is httpfs-credentialed, so it proves the created
 *    secret is *usable*.
 *
 * Topology mirrors [TestDucklakeLanceS3QuackRead]: MinIO + Quack on one Testcontainers network.
 * The vortex/httpfs extensions are pre-warmed (downloaded) server-side in the fixture so the
 * barriers race the *catalog writes*, not the extension downloads. Skips when Docker/network or
 * the container-platform trino_parity binary is unavailable.
 *
 * SAME_THREAD: the test manages its own concurrency against one shared Quack server.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeQuackS3InitRace {

    @Test
    fun concurrentVortexS3FileScansDoNotConflictOnSecretCreation() {
        val failures = runConcurrently(DuckDbAttachTarget.FileScan(
                VORTEX_URL, "read_vortex", "vortex", s3Config()))
        assertThat(failures)
                .`as`("concurrent s3 vortex FileScans against one Quack server (each ships "
                        + "the shared-secret CREATE server-side)")
                .isEmpty()
    }

    @Test
    fun concurrentDbAttachesOverS3DoNotConflictOnSecretOrAttach() {
        val failures = runConcurrently(DuckDbAttachTarget.HttpfsS3(DB_URL, s3Config()))
        assertThat(failures)
                .`as`("concurrent s3 .db ATTACHes against one Quack server (each ships "
                        + "the shared-secret CREATE + ATTACH IF NOT EXISTS server-side)")
                .isEmpty()
    }

    /**
     * [THREADS] barrier-aligned executors x [ROUNDS] rounds against the shared Quack server;
     * every execution must stream back the full 3-row fixture. Returns the failures.
     */
    private fun runConcurrently(target: DuckDbAttachTarget): List<Throwable> {
        assumeTrue(available, unavailableReason)
        val executor = QuackDuckDbExecutor(
                quackServer!!.host,
                quackServer!!.mappedPort,
                quackServer!!.token,
                dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                target,
                listOf(
                        DucklakeColumnHandle(1L, "id", INTEGER, false),
                        DucklakeColumnHandle(2L, "name", VARCHAR, false)),
                TupleDomain.all<DucklakeColumnHandle>())

        val barrier = CyclicBarrier(THREADS)
        val failures = ConcurrentLinkedQueue<Throwable>()
        val pool = Executors.newFixedThreadPool(THREADS)
        try {
            val tasks = (1..THREADS).map {
                pool.submit {
                    repeat(ROUNDS) {
                        barrier.await(2, TimeUnit.MINUTES)
                        try {
                            assertThat(countRows(executor, request))
                                    .`as`("rows streamed back by a concurrent execution")
                                    .isEqualTo(3)
                        }
                        catch (t: Throwable) {
                            failures.add(t)
                        }
                    }
                }
            }
            tasks.forEach { it.get(5, TimeUnit.MINUTES) }
        }
        finally {
            pool.shutdownNow()
        }
        return failures.toList()
    }

    private fun countRows(
            executor: QuackDuckDbExecutor,
            request: DucklakeDuckDbExecutor.ExecutionRequest): Int {
        var rows = 0
        executor.execute(request).use { ctx ->
            val reader = ctx.arrowReader()
            while (reader.loadNextBatch()) {
                rows += reader.vectorSchemaRoot.rowCount
            }
        }
        return rows
    }

    companion object {
        private const val THREADS = 8
        private const val ROUNDS = 3

        private const val MINIO_USER = "race-e2e-user"
        private const val MINIO_PASSWORD = "race-e2e-secret"
        private const val BUCKET = "race"
        private const val VORTEX_URL = "s3://$BUCKET/race.vortex"
        private const val DB_URL = "s3://$BUCKET/race.db"
        private const val IN_CONTAINER_ENDPOINT = "http://minio:9000"
        private const val FIXTURE_ROWS =
                "(VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) v(id, name)"

        private var network: Network? = null
        private var minio: GenericContainer<*>? = null
        private var quackServer: TestingDucklakeQuackEngineServer? = null
        private lateinit var sharedDir: Path
        private var available: Boolean = false
        private var unavailableReason: String = "fixture setup did not run"

        @BeforeAll
        @JvmStatic
        fun setUp() {
            sharedDir = Files.createTempDirectory("quack-s3-race-")
            try {
                val net = Network.newNetwork()
                network = net

                val minioContainer = GenericContainer("minio/minio:latest")
                        .withNetwork(net)
                        .withNetworkAliases("minio")
                        .withEnv("MINIO_ROOT_USER", MINIO_USER)
                        .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
                        .withCommand("server", "/data")
                        .withExposedPorts(9000)
                        .waitingFor(Wait.forListeningPort())
                minioContainer.start()
                minio = minioContainer
                val mb = minioContainer.execInContainer("/bin/sh", "-c",
                        "mc alias set local http://localhost:9000 $MINIO_USER $MINIO_PASSWORD && mc mb local/$BUCKET")
                check(mb.exitCode == 0) { "bucket creation failed: ${mb.stdout}\n${mb.stderr}" }

                // AWS_* env on the sidecar (the lance-O1 production topology) — needed because
                // `read_vortex` is object_store-credentialed like lance and NEVER reads the
                // DuckDB httpfs secret (probed 2026-06-11: single-threaded read_vortex over s3
                // with only the secret present fails at bind with the IMDS fallback; the vortex
                // COPY *write* honors the secret, which is what made the secret channel look
                // sufficient). The shipped-anyway secret still exercises the race under test.
                quackServer = TestingDucklakeQuackEngineServer(sharedDir, s3Config().toObjectStoreEnv(), net)
                if (!quackServer!!.installParityExtension()) {
                    unavailableReason = "no trino_parity binary for container platform ${quackServer!!.containerPlatform()}"
                    return
                }

                // Write the vortex fixture to s3 from inside the Quack container (network alias +
                // httpfs secret), which also pre-warms the vortex/httpfs extension downloads into
                // the container's shared extension dir.
                quackServer!!.execDuckDbSql(
                        "INSTALL vortex; LOAD vortex; INSTALL httpfs; LOAD httpfs; "
                                + s3Config().renderCreateSecretSql() + "; "
                                + "CREATE TABLE t AS SELECT * FROM $FIXTURE_ROWS; "
                                + "COPY t TO '$VORTEX_URL' (FORMAT vortex);")

                // The .db fixture: created locally, dropped into MinIO via mc (DuckDB cannot
                // write-attach over s3; the executor ATTACHes it READ_ONLY).
                val dbFile = sharedDir.resolve("race.db")
                DriverManager.getConnection("jdbc:duckdb:" + dbFile.toAbsolutePath()).use { c ->
                    c.createStatement().use { s ->
                        s.execute("CREATE TABLE t AS SELECT * FROM $FIXTURE_ROWS")
                    }
                }
                minioContainer.copyFileToContainer(
                        MountableFile.forHostPath(dbFile), "/tmp/race.db")
                val cp = minioContainer.execInContainer("/bin/sh", "-c",
                        "mc cp /tmp/race.db local/$BUCKET/race.db")
                check(cp.exitCode == 0) { ".db upload failed: ${cp.stdout}\n${cp.stderr}" }

                available = true
            }
            catch (e: Exception) {
                unavailableReason = "quack s3 race fixture unavailable (docker/network/extension): ${e.message}"
            }
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            quackServer?.close()
            minio?.stop()
            network?.close()
        }

        private fun s3Config(): DuckDbS3Config = DuckDbS3Config.fromCatalogConfig(mapOf(
                "s3.endpoint" to IN_CONTAINER_ENDPOINT,
                "s3.region" to "us-east-1",
                "s3.aws-access-key" to MINIO_USER,
                "s3.aws-secret-key" to MINIO_PASSWORD,
                "s3.path-style-access" to "true"))
    }
}
