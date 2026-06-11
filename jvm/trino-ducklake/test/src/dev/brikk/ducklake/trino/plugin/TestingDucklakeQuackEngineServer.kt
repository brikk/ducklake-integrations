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
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.MountableFile
import java.nio.file.Files
import java.nio.file.Path
import java.util.Optional

/**
 * Out-of-process DuckDB-serving-Quack fixture for exercising
 * [QuackDuckDbExecutor]. Reuses the `brikk-ducklake-quack-server`
 * image (the catalog-path entrypoint also serves arbitrary client SQL just
 * fine for the data-path use case — no DuckLake-attached state needed) and
 * additionally bind-mounts the host's data directory so the server can
 * server-side `ATTACH '/data/<name>.db'` files written from the test.
 *
 *
 * Both the test JVM and the container see the `.db` files at the
 * same path on their respective views. In production this maps to the
 * multi-container pod model where a shared volume is mounted into each
 * Quack-serving sidecar.
 */
internal class TestingDucklakeQuackEngineServer : AutoCloseable {
    private val container: GenericContainer<*>
    val token: String

    constructor(sharedDataDir: Path) : this(sharedDataDir, Optional.empty())

    constructor(sharedDataDir: Path, parityExtensionPath: Optional<Path>)
            : this(sharedDataDir, parityExtensionPath, emptyMap())

    /**
     * @param objectStoreEnv extra environment for the container — in production this is where
     *        the operator injects the lance s3 credential channel
     *        ([DuckDbS3Config.toObjectStoreEnv]'s `AWS_*` vars): the DuckDB `lance` extension's
     *        Rust object_store reads process-global env, not DuckDB secrets (HANDOFF O1), and
     *        env set on the container at launch is inherited by the in-container `duckdb`.
     */
    constructor(
            sharedDataDir: Path,
            parityExtensionPath: Optional<Path>,
            objectStoreEnv: Map<String, String>) {
        this.token = DEFAULT_TOKEN
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
        if (parityExtensionPath.isPresent && Files.isRegularFile(parityExtensionPath.get())) {
            c = c.withCopyFileToContainer(
                    MountableFile.forHostPath(parityExtensionPath.get()),
                    TestingDucklakeDuckDbQuackCatalogServer.IN_CONTAINER_PARITY_EXTENSION_PATH)
        }
        this.container = c
        container.start()
    }

    val host: String
        get() = container.host

    val mappedPort: Int
        get() = container.getMappedPort(CONTAINER_PORT)

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
