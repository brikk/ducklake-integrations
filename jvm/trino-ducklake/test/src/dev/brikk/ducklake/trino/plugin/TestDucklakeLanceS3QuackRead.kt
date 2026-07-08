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
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional

/**
 * The O1 fix, automated end-to-end: lance datasets on **s3** read through the **Quack sidecar**
 * whose container env carries the object_store `AWS_*` credential channel
 * ([DuckDbS3Config.toObjectStoreEnv] — the DuckDB lance extension ignores httpfs secrets).
 *
 * Topology: MinIO + the Quack server share a Testcontainers network (`http://minio:9000` from
 * inside); the lance dataset is written to s3 *server-side* (in-container duckdb, inheriting
 * the container's env + network), then read back via [QuackDuckDbExecutor] FileScan —
 * `__lance_scan('s3://…')` and `lance_vector_search('s3://…', …)` — exactly the shapes the
 * read path and the search table functions emit.
 *
 * Requires Docker + network (in-container `INSTALL lance` downloads the linux extension) and
 * the trino_parity binary for the container's platform (selected by
 * [TestingDucklakeQuackEngineServer.installParityExtension] — container arch, not host arch).
 * Skips gracefully when any of that is unavailable.
 *
 * SAME_THREAD: both tests funnel server-side INSTALL/LOAD statements into one shared Quack
 * server; concurrent INSTALLs can race the extension download.
 */
// quack-container: mounts the trino_parity binary into the Quack sidecar CONTAINER. CI excludes
// this tag (-PexcludeTags) because the extension we build on the runner links against the runner's
// glibc (2.39), which the older Quack container can't load ("GLIBC_2.38 not found"). Runs locally
// where the container/binary glibc match. Fix tracked: build a glibc-portable binary via the
// extension's Docker target for a dedicated Quack CI job (dev-docs/TODO-WRITE-MODE.md).
@Tag("quack-container")
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeLanceS3QuackRead {
    @Test
    fun quackReadsLanceDatasetFromS3ViaAwsEnv() {
        val executor = quackExecutor()
        // No s3Config on the FileScan — mirrors resolveDuckDbReadTarget: lance ignores DuckDB
        // secrets; the credentials are the container's AWS_* env (the whole point of this test).
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan(DATASET_URL, "__lance_scan", "lance", null),
                listOf(
                        DucklakeColumnHandle(1L, "id", INTEGER, false),
                        DucklakeColumnHandle(2L, "name", VARCHAR, false)),
                TupleDomain.all<DucklakeColumnHandle>())

        val rows = mutableListOf<Pair<Int, String>>()
        executor.execute(request).use { ctx ->
            val reader = ctx.arrowReader()
            while (reader.loadNextBatch()) {
                val root = reader.vectorSchemaRoot
                for (r in 0 until root.rowCount) {
                    rows.add((root.getVector(0).getObject(r) as Number).toInt()
                            to root.getVector(1).getObject(r).toString())
                }
            }
        }
        assertThat(rows.sortedBy { it.first })
                .`as`("__lance_scan over s3 through the env-credentialed Quack sidecar")
                .containsExactly(1 to "alpha", 2 to "beta", 3 to "gamma")
    }

    @Test
    fun quackRunsLanceVectorSearchOverS3() {
        val executor = quackExecutor()
        // The same FileScan shape LanceSearchSplitProcessor / createLanceSearchPageSource emit
        // for the search table functions — argument tail after the quoted s3 path.
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan(
                        DATASET_URL, "lance_vector_search", "lance", null,
                        ", 'emb', [1.0, 0.0]::DOUBLE[], k := 2, prefilter := false"),
                listOf(
                        DucklakeColumnHandle(1L, "id", INTEGER, false),
                        DucklakeColumnHandle(-1L, "_distance", REAL, true)),
                TupleDomain.all<DucklakeColumnHandle>())

        val ids = mutableListOf<Int>()
        executor.execute(request).use { ctx ->
            val reader = ctx.arrowReader()
            while (reader.loadNextBatch()) {
                val root = reader.vectorSchemaRoot
                for (r in 0 until root.rowCount) {
                    ids.add((root.getVector(0).getObject(r) as Number).toInt())
                }
            }
        }
        assertThat(ids)
                .`as`("vector search over an s3 lance dataset, nearest first")
                .containsExactly(1, 3)
    }

    companion object {
        private const val MINIO_USER = "lance-e2e-user"
        private const val MINIO_PASSWORD = "lance-e2e-secret"
        private const val BUCKET = "lance"
        private const val DATASET_URL = "s3://$BUCKET/t.lance"
        private const val IN_CONTAINER_ENDPOINT = "http://minio:9000"

        private var network: Network? = null
        private var minio: GenericContainer<*>? = null
        private var quackServer: TestingDucklakeQuackEngineServer? = null
        private lateinit var sharedDir: Path
        private var available: Boolean = false
        private var unavailableReason: String = "fixture setup did not run"

        @BeforeAll
        @JvmStatic
        fun setUp() {
            sharedDir = Files.createTempDirectory("lance-s3-quack-")
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
                // The minio/minio image ships `mc`; create the bucket in-container.
                val mb = minioContainer.execInContainer("/bin/sh", "-c",
                        "mc alias set local http://localhost:9000 $MINIO_USER $MINIO_PASSWORD && mc mb local/$BUCKET")
                check(mb.exitCode == 0) { "bucket creation failed: ${mb.stdout}\n${mb.stderr}" }

                // The Quack sidecar gets the O1 credential channel: AWS_* env derived from the
                // SAME DuckDbS3Config mapping production uses, with the in-network endpoint.
                quackServer = TestingDucklakeQuackEngineServer(sharedDir, s3Config().toObjectStoreEnv(), net)
                if (!quackServer!!.installParityExtension()) {
                    unavailableReason = "no trino_parity binary for container platform ${quackServer!!.containerPlatform()}"
                    return
                }

                // Write the lance dataset to s3 server-side: the in-container duckdb inherits
                // the container env (credentials) + network (minio alias). INSTALL lance needs
                // outbound network; failure here -> skip (offline).
                quackServer!!.execDuckDbSql(
                        "INSTALL lance; LOAD lance; "
                                + "CREATE TABLE t AS SELECT * FROM (VALUES "
                                + "(1, 'alpha', [1.0::FLOAT, 0.0::FLOAT]), "
                                + "(2, 'beta',  [0.0::FLOAT, 1.0::FLOAT]), "
                                + "(3, 'gamma', [0.7::FLOAT, 0.3::FLOAT])) v(id, name, emb); "
                                + "COPY t TO '$DATASET_URL' (FORMAT lance);")
                available = true
            }
            catch (e: Exception) {
                unavailableReason = "lance-s3 quack fixture unavailable (docker/network/extension): ${e.message}"
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

        private fun quackExecutor(): QuackDuckDbExecutor {
            assumeTrue(available, unavailableReason)
            return QuackDuckDbExecutor(
                    quackServer!!.host,
                    quackServer!!.mappedPort,
                    quackServer!!.token,
                    dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
        }
    }
}
