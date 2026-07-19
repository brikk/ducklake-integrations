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
package dev.brikk.duckbridge.trino.plugin

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.MountableFile
import java.nio.file.Files
import java.nio.file.Path

/**
 * A real out-of-process DuckDB hosting a Quack RPC listener — the remote server the T3
 * (quack-jdbc) transport connects to. The test JVM's `quack-jdbc` driver speaks the Quack wire
 * protocol to it via `jdbc:quack://host:port`.
 *
 * Uses a `debian:trixie-slim` (GLIBC 2.41) base rather than the DuckLake fixture's bookworm
 * (2.36): the locally-built `trino_parity.duckdb_extension` links against GLIBC 2.38, so it only
 * LOADs on trixie. [installParityExtension] copies the built binary into the running container so
 * a subsequent server-side `LOAD '<IN_CONTAINER_PARITY_PATH>'` works.
 */
internal class TestingQuackServer : AutoCloseable {
    private val container: GenericContainer<*> =
        GenericContainer(buildImage())
            .withExposedPorts(CONTAINER_PORT)
            .withEnv("QUACK_PORT", CONTAINER_PORT.toString())
            .withEnv("QUACK_TOKEN", TOKEN)
            .withStartupAttempts(3)
            .waitingFor(Wait.forListeningPort())

    val token: String = TOKEN

    init {
        container.start()
    }

    val host: String get() = container.host
    val mappedPort: Int get() = container.getMappedPort(CONTAINER_PORT)

    /** `jdbc:quack://host:mappedPort` — the base-jdbc `connection-url` for the T3 catalog. */
    fun connectionUrl(): String = "jdbc:quack://$host:$mappedPort"

    /**
     * Copy the host-built parity extension into the running container at [IN_CONTAINER_PARITY_PATH],
     * selecting the binary by the container's ACTUAL platform (`uname -m`), not the JVM host arch —
     * the two differ when a podman VM runs a different arch. Returns false when no matching binary is
     * available (callers skip parity-dependent assertions).
     */
    fun installParityExtension(): Boolean {
        val binary =
            TrinoParityExtensionResolver.resolveBundledExtensionPathFor(containerPlatform())?.let(Path::of)
                ?: return false
        if (!Files.isRegularFile(binary)) {
            return false
        }
        container.copyFileToContainer(MountableFile.forHostPath(binary), IN_CONTAINER_PARITY_PATH)
        return true
    }

    private fun containerPlatform(): String =
        when (val arch = container.execInContainer("uname", "-m").stdout.trim()) {
            "x86_64" -> "linux-amd64"
            "aarch64" -> "linux-arm64"
            else -> error("Unexpected container arch: '$arch'")
        }

    override fun close() {
        container.stop()
    }

    companion object {
        private const val CONTAINER_PORT = 9494
        private const val TOKEN = "duckbridge-test-token"

        /** Server-side path where [installParityExtension] drops the binary for a server-side LOAD. */
        const val IN_CONTAINER_PARITY_PATH: String = "/opt/duckdb-extensions/trino_parity.duckdb_extension"

        private fun buildImage(): ImageFromDockerfile =
            // Testcontainers content-hashes the Dockerfile + files, so this builds once per test JVM.
            ImageFromDockerfile("duckbridge-quack-server", false)
                .withFileFromClasspath("Dockerfile", "docker/quack-server/Dockerfile")
                .withFileFromClasspath("entrypoint.sh", "docker/quack-server/entrypoint.sh")
    }
}
