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
package dev.brikk.ducklake.catalog

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.MountableFile
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional

/**
 * Testcontainer wrapper for a DuckDB process hosting a Quack RPC listener.
 * Acts as the remote DuckLake-catalog server in cross-engine tests — clients
 * (in-process DuckDB JDBC connections) attach via
 * `ATTACH 'ducklake:quack:host:port' AS lake (...)`.
 *
 * This must remain process-isolated from any JVM-internal DuckDB instance
 * that intends to attach DuckLake-on-Quack: hosting `quack_serve` and the
 * DuckLake-on-Quack client in the same `DatabaseInstance` self-deadlocks
 * on a connection-id RPC loop. See upstream's
 * `scripts/run_quack_tests.py` comment and the in-tree
 * `QuackJdbcProbeTest` for the diagnostic narrative.
 */
class TestingDucklakeDuckDbQuackCatalogServer(parityExtensionPath: Optional<Path>) : AutoCloseable {

    private val container: GenericContainer<*>
    private val token: String = DEFAULT_TOKEN

    init {
        var c: GenericContainer<*> = GenericContainer(buildImage())
            .withExposedPorts(CONTAINER_PORT)
            .withEnv("QUACK_PORT", CONTAINER_PORT.toString())
            .withEnv("QUACK_TOKEN", token)
            .withStartupAttempts(3)
            .waitingFor(Wait.forListeningPort())
        if (parityExtensionPath.isPresent && Files.isRegularFile(parityExtensionPath.get())) {
            c = c.withCopyFileToContainer(
                MountableFile.forHostPath(parityExtensionPath.get()),
                IN_CONTAINER_PARITY_EXTENSION_PATH
            )
        }
        this.container = c
        container.start()
    }

    constructor() : this(Optional.empty())

    fun getHost(): String = container.host

    fun getMappedPort(): Int = container.getMappedPort(CONTAINER_PORT)

    fun getToken(): String = token

    /**
     * URI form a DuckDB client passes inside `ATTACH 'ducklake:...'` —
     * i.e. `quack:host:port`. The `ducklake:` prefix is added by
     * [getDucklakeAttachUri].
     */
    fun getQuackAttachUri(): String = "quack:" + getHost() + ":" + getMappedPort()

    /**
     * Full URI for the DuckLake ATTACH statement:
     * `ducklake:quack:host:port`. Pair with
     * `DATA_PATH '<somewhere shared>'` and a
     * `METADATA_CATALOG '<unique-per-test>'` for isolation.
     */
    fun getDucklakeAttachUri(): String = "ducklake:" + getQuackAttachUri()

    override fun close() {
        container.stop()
    }

    companion object {
        private const val CONTAINER_PORT: Int = 9494
        private const val DEFAULT_TOKEN: String = "ducklake-test-token"

        /**
         * In-container path where the trino_parity DuckDB extension is mounted
         * (when [TestingDucklakeDuckDbQuackCatalogServer] is
         * called with a non-empty path). Clients reach this via the Quack wrapper
         * with `LOAD '<path>'` — server-side allow_unsigned_extensions is
         * enabled by the entrypoint's `duckdb -unsigned` invocation.
         */
        const val IN_CONTAINER_PARITY_EXTENSION_PATH: String =
            "/opt/duckdb-extensions/trino_parity.duckdb_extension"

        private fun buildImage(): ImageFromDockerfile {
            // Reuse the same image hash across test classes — Testcontainers keys
            // ImageFromDockerfile by the (name, deleteOnExit, files) tuple and skips
            // rebuilds when the inputs are unchanged. The image build pulls the
            // DuckDB CLI binary and the core Quack extension; cost amortizes
            // across the full test suite.
            return ImageFromDockerfile("brikk-ducklake-quack-server", false)
                .withFileFromClasspath("Dockerfile", "docker/quack-server/Dockerfile")
                .withFileFromClasspath("entrypoint.sh", "docker/quack-server/entrypoint.sh")
        }
    }
}
