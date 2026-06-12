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

import dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.MountableFile
import java.nio.file.Files
import java.nio.file.Path

/**
 * Out-of-process DuckDB-serving-Quack fixture for exercising
 * [QuackDuckDbExecutor]. Reuses the `brikk-ducklake-quack-server`
 * image (the catalog-path entrypoint also serves arbitrary client SQL just
 * fine for the data-path use case — no DuckLake-attached state needed) and
 * additionally bind-mounts the host's data directory so the server can
 * server-side `ATTACH '/data/<name>.db'` files written from the test.
 *
 * Both the test JVM and the container see the `.db` files at the
 * same path on their respective views. In production this maps to the
 * multi-container pod model where a shared volume is mounted into each
 * Quack-serving sidecar.
 *
 * @param objectStoreEnv extra environment for the container — in production this is where
 *        the operator injects the lance s3 credential channel
 *        ([DuckDbS3Config.toObjectStoreEnv]'s `AWS_*` vars): the DuckDB `lance` extension's
 *        Rust object_store reads process-global env, not DuckDB secrets (HANDOFF O1), and
 *        env set on the container at launch is inherited by the in-container `duckdb`.
 * @param network optional shared Testcontainers network, for tests that pair this server with
 *        other containers (e.g. MinIO) reachable by network alias.
 */
internal class TestingDucklakeQuackEngineServer(
        sharedDataDir: Path,
        objectStoreEnv: Map<String, String> = emptyMap(),
        network: Network? = null,
) : AutoCloseable {
    private val container: GenericContainer<*>
    val token: String = DEFAULT_TOKEN

    init {
        var c: GenericContainer<*> = GenericContainer(buildImage())
                .withExposedPorts(CONTAINER_PORT)
                .withEnv("QUACK_PORT", CONTAINER_PORT.toString())
                .withEnv("QUACK_TOKEN", token)
                .withFileSystemBind(sharedDataDir.toAbsolutePath().toString(), "/data", BindMode.READ_WRITE)
                .withStartupAttempts(3)
                .waitingFor(Wait.forListeningPort())
        for ((key, value) in objectStoreEnv) {
            c = c.withEnv(key, value)
        }
        if (network != null) {
            c = c.withNetwork(network)
        }
        this.container = c
        container.start()
    }

    val host: String
        get() = container.host

    val mappedPort: Int
        get() = container.getMappedPort(CONTAINER_PORT)

    /**
     * The `linux-<arch>` platform string of the *container's* duckdb — NOT the JVM host's
     * `os.arch`. The two genuinely differ in practice: a podman machine on Apple Silicon may
     * run an amd64 VM, so an arm64 host builds and runs an amd64 container. Anything LOADed
     * into the server (the trino_parity extension) must match this, which is why selection by
     * host arch used to mis-bundle and the parity tests skipped on such boxes.
     */
    fun containerPlatform(): String {
        val arch = container.execInContainer("uname", "-m").stdout.trim()
        return when (arch) {
            "x86_64" -> "linux-amd64"
            "aarch64" -> "linux-arm64"
            else -> throw IllegalStateException("Unexpected container arch: '$arch'")
        }
    }

    /**
     * Install the trino_parity extension into the RUNNING container at
     * [TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH], selecting
     * the binary by [containerPlatform]: the `-Dducklake.test.parityExtensionPath` override
     * (operator-asserted container-loadable) wins, else the bundled `linux-<container-arch>`
     * classpath resource. Returns false when no matching binary is available — callers skip
     * their parity-dependent assertions in that case.
     */
    fun installParityExtension(): Boolean {
        val configured: Path? = System.getProperty("ducklake.test.parityExtensionPath")
                ?.trim()
                ?.takeIf { it.isNotEmpty() }
                ?.let(Path::of)
        val binary: Path = configured
            ?: TrinoParityExtensionResolver.resolveBundledExtensionPathFor(containerPlatform())
                    ?.let(Path::of)
            ?: return false
        if (!Files.isRegularFile(binary)) {
            return false
        }
        container.copyFileToContainer(
                MountableFile.forHostPath(binary),
                TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
        return true
    }

    /**
     * Run a SQL script inside the container's own `duckdb` CLI (a second process next to the
     * Quack listener, sharing the container env and network) — for fixture setup that must
     * happen server-side, e.g. writing a lance dataset to MinIO with the container's `AWS_*`
     * credentials. Throws with the combined output on a non-zero exit.
     */
    fun execDuckDbSql(sql: String) {
        val result = container.execInContainer("duckdb", "-unsigned", "-c", sql)
        check(result.exitCode == 0) {
            "in-container duckdb failed (exit ${result.exitCode}):\nstdout: ${result.stdout}\nstderr: ${result.stderr}"
        }
    }

    override fun close() {
        container.stop()
    }

    companion object {
        private const val CONTAINER_PORT = 9494
        private const val DEFAULT_TOKEN = "ducklake-engine-token"

        private fun buildImage(): ImageFromDockerfile {
            // Reuses the catalog-path image — Testcontainers content-hashes the
            // Dockerfile + files, so this builds once per test JVM across all
            // fixtures that point at the same image inputs.
            return ImageFromDockerfile("brikk-ducklake-quack-server", false)
                    .withFileFromClasspath("Dockerfile", "docker/quack-server/Dockerfile")
                    .withFileFromClasspath("entrypoint.sh", "docker/quack-server/entrypoint.sh")
        }
    }
}
