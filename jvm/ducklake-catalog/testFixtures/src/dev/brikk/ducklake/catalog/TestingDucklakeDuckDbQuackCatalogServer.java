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
package dev.brikk.ducklake.catalog;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Testcontainer wrapper for a DuckDB process hosting a Quack RPC listener.
 * Acts as the remote DuckLake-catalog server in cross-engine tests — clients
 * (in-process DuckDB JDBC connections) attach via
 * {@code ATTACH 'ducklake:quack:host:port' AS lake (...)}.
 *
 * <p>This must remain process-isolated from any JVM-internal DuckDB instance
 * that intends to attach DuckLake-on-Quack: hosting {@code quack_serve} and the
 * DuckLake-on-Quack client in the same {@code DatabaseInstance} self-deadlocks
 * on a connection-id RPC loop. See upstream's
 * {@code scripts/run_quack_tests.py} comment and the in-tree
 * {@code QuackJdbcProbeTest} for the diagnostic narrative.
 */
public class TestingDucklakeDuckDbQuackCatalogServer
        implements AutoCloseable
{
    private static final int CONTAINER_PORT = 9494;
    private static final String DEFAULT_TOKEN = "ducklake-test-token";

    private final GenericContainer<?> container;
    private final String token;

    public TestingDucklakeDuckDbQuackCatalogServer()
    {
        this.token = DEFAULT_TOKEN;
        this.container = new GenericContainer<>(buildImage())
                .withExposedPorts(CONTAINER_PORT)
                .withEnv("QUACK_PORT", String.valueOf(CONTAINER_PORT))
                .withEnv("QUACK_TOKEN", token)
                .withStartupAttempts(3)
                .waitingFor(Wait.forListeningPort());
        container.start();
    }

    public String getHost()
    {
        return container.getHost();
    }

    public int getMappedPort()
    {
        return container.getMappedPort(CONTAINER_PORT);
    }

    public String getToken()
    {
        return token;
    }

    /**
     * URI form a DuckDB client passes inside {@code ATTACH 'ducklake:...'} —
     * i.e. {@code quack:host:port}. The {@code ducklake:} prefix is added by
     * {@link #getDucklakeAttachUri()}.
     */
    public String getQuackAttachUri()
    {
        return "quack:" + getHost() + ":" + getMappedPort();
    }

    /**
     * Full URI for the DuckLake ATTACH statement:
     * {@code ducklake:quack:host:port}. Pair with
     * {@code DATA_PATH '<somewhere shared>'} and a
     * {@code METADATA_CATALOG '<unique-per-test>'} for isolation.
     */
    public String getDucklakeAttachUri()
    {
        return "ducklake:" + getQuackAttachUri();
    }

    @Override
    public void close()
    {
        container.stop();
    }

    private static ImageFromDockerfile buildImage()
    {
        // Reuse the same image hash across test classes — Testcontainers keys
        // ImageFromDockerfile by the (name, deleteOnExit, files) tuple and skips
        // rebuilds when the inputs are unchanged. The image build pulls the
        // DuckDB CLI binary and the core Quack extension; cost amortizes
        // across the full test suite.
        return new ImageFromDockerfile("brikk-ducklake-quack-server", false)
                .withFileFromClasspath("Dockerfile", "docker/quack-server/Dockerfile")
                .withFileFromClasspath("entrypoint.sh", "docker/quack-server/entrypoint.sh");
    }
}
