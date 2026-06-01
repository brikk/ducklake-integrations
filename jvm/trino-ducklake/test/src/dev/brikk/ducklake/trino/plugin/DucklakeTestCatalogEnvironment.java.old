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

import com.google.common.collect.ImmutableMap;
import dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer;
import dev.brikk.ducklake.catalog.TestingDucklakeLocalDuckDbCatalogFixture;
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Manages the shared catalog-backend fixtures used by Trino plugin tests. Holds
 * one lazily-created {@link TestingDucklakePostgreSqlCatalogServer} for the
 * default (PG) backend and one {@link TestingDucklakeDuckDbQuackCatalogServer}
 * for the Quack-backed DuckDB backend, with selection driven by
 * {@link DucklakeTestCatalogBackend#fromSystemProperty()}.
 *
 * <p>Backend selection is read once per JVM and pinned for the life of the
 * test process — flipping the system property mid-suite isn't supported.
 */
public final class DucklakeTestCatalogEnvironment
{
    private static final Object LOCK = new Object();

    private static volatile TestingDucklakePostgreSqlCatalogServer pgServer;
    private static volatile TestingDucklakeDuckDbQuackCatalogServer quackServer;
    private static volatile TestingDucklakeLocalDuckDbCatalogFixture localDuckDbFixture;
    private static volatile RuntimeException serverUnavailable;
    private static volatile boolean pgCatalogGenerated;

    private DucklakeTestCatalogEnvironment() {}

    public static DucklakeTestCatalogBackend selectedBackend()
    {
        return DucklakeTestCatalogBackend.fromSystemProperty();
    }

    /**
     * Returns the PG server. Only valid when the selected backend is POSTGRES
     * — callers that need backend-pluggable behaviour should branch on
     * {@link #selectedBackend()} first. Kept as-is for source compatibility
     * with the existing PG-only test code.
     */
    public static TestingDucklakePostgreSqlCatalogServer getServer()
            throws Exception
    {
        return ensurePostgresServer();
    }

    public static TestingDucklakeDuckDbQuackCatalogServer getQuackServer()
            throws Exception
    {
        return ensureQuackServer();
    }

    public static TestingDucklakeLocalDuckDbCatalogFixture getLocalDuckDbFixture()
    {
        return ensureLocalDuckDbFixture();
    }

    public static DucklakeConfig createDucklakeConfig()
            throws Exception
    {
        // Only the shared (non-isolated) PG catalog flow runs through here today;
        // it's the path used by tests that don't call useIsolatedCatalog and
        // expect the pre-bootstrapped 17-table fixture. The Quack backend doesn't
        // ship that bootstrap yet (see DucklakeCatalogGenerator) so any test
        // reaching this code path under DUCKDB_QUACK will need migration to the
        // isolated-catalog form.
        if (selectedBackend() != DucklakeTestCatalogBackend.POSTGRES) {
            throw new IllegalStateException(
                    "createDucklakeConfig (shared catalog) is only supported under POSTGRES backend; "
                            + "selected backend is " + selectedBackend()
                            + ". Use useIsolatedCatalog(...) on the query-runner builder instead.");
        }
        TestingDucklakePostgreSqlCatalogServer server = ensurePostgresServer();
        ensureCatalogGenerated(server);

        return new DucklakeConfig()
                .setMaxCatalogConnections(5)
                .setCatalogDatabaseUrl(server.getJdbcUrl())
                .setCatalogDatabaseUser(server.getUser())
                .setCatalogDatabasePassword(server.getPassword())
                .setDataPath(DucklakeCatalogGenerator.getPostgreSqlCatalogDirectory().resolve("data").toAbsolutePath().toString());
    }

    public static Map<String, String> getConnectorProperties()
            throws Exception
    {
        DucklakeConfig config = createDucklakeConfig();
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ducklake.catalog.database-url", requireNonNull(config.getCatalogDatabaseUrl(), "catalogDatabaseUrl is null"))
                .put("ducklake.data-path", requireNonNull(config.getDataPath(), "dataPath is null"));

        if (config.getCatalogDatabaseUser() != null) {
            properties.put("ducklake.catalog.database-user", config.getCatalogDatabaseUser());
        }
        if (config.getCatalogDatabasePassword() != null) {
            properties.put("ducklake.catalog.database-password", config.getCatalogDatabasePassword());
        }

        return properties.buildOrThrow();
    }

    private static TestingDucklakePostgreSqlCatalogServer ensurePostgresServer()
            throws Exception
    {
        RuntimeException unavailable = serverUnavailable;
        if (unavailable != null) {
            skipTests(unavailable);
        }

        TestingDucklakePostgreSqlCatalogServer result = pgServer;
        if (result == null) {
            synchronized (LOCK) {
                result = pgServer;
                if (result == null) {
                    try {
                        result = new TestingDucklakePostgreSqlCatalogServer();
                        pgServer = result;
                        Runtime.getRuntime().addShutdownHook(new Thread(result::close));
                    }
                    catch (RuntimeException e) {
                        if (isDockerUnavailable(e)) {
                            serverUnavailable = e;
                            skipTests(e);
                        }
                        throw e;
                    }
                }
            }
        }
        return result;
    }

    private static TestingDucklakeDuckDbQuackCatalogServer ensureQuackServer()
            throws Exception
    {
        RuntimeException unavailable = serverUnavailable;
        if (unavailable != null) {
            skipTests(unavailable);
        }

        TestingDucklakeDuckDbQuackCatalogServer result = quackServer;
        if (result == null) {
            synchronized (LOCK) {
                result = quackServer;
                if (result == null) {
                    try {
                        // The Quack catalog container runs Linux; mount the
                        // platform-matched binary (linux-arm64 on macOS arm64
                        // hosts via Docker Desktop, linux-amd64 on Linux amd64
                        // hosts). System-property override wins.
                        String containerArch = System.getProperty("os.arch", "").toLowerCase().contains("aarch")
                                ? "linux-arm64" : "linux-amd64";
                        java.util.Optional<java.nio.file.Path> extensionPath = java.util.Optional
                                .ofNullable(System.getProperty("ducklake.test.parityExtensionPath"))
                                .map(String::strip)
                                .filter(s -> !s.isEmpty())
                                .map(java.nio.file.Path::of)
                                .or(() -> TrinoParityExtensionResolver.resolveBundledExtensionPathFor(containerArch)
                                        .map(java.nio.file.Path::of));
                        result = new TestingDucklakeDuckDbQuackCatalogServer(extensionPath);
                        quackServer = result;
                        Runtime.getRuntime().addShutdownHook(new Thread(result::close));
                    }
                    catch (RuntimeException e) {
                        if (isDockerUnavailable(e)) {
                            serverUnavailable = e;
                            skipTests(e);
                        }
                        throw e;
                    }
                }
            }
        }
        return result;
    }

    private static TestingDucklakeLocalDuckDbCatalogFixture ensureLocalDuckDbFixture()
    {
        TestingDucklakeLocalDuckDbCatalogFixture result = localDuckDbFixture;
        if (result == null) {
            synchronized (LOCK) {
                result = localDuckDbFixture;
                if (result == null) {
                    result = new TestingDucklakeLocalDuckDbCatalogFixture();
                    localDuckDbFixture = result;
                    Runtime.getRuntime().addShutdownHook(new Thread(result::close));
                }
            }
        }
        return result;
    }

    private static void ensureCatalogGenerated(TestingDucklakePostgreSqlCatalogServer server)
            throws Exception
    {
        if (!pgCatalogGenerated) {
            synchronized (LOCK) {
                if (!pgCatalogGenerated) {
                    DucklakeCatalogGenerator.generatePostgreSqlCatalog(server);
                    pgCatalogGenerated = true;
                }
            }
        }
    }

    static boolean isDockerUnavailable(Throwable throwable)
    {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (message.contains("Could not find a valid Docker environment")
                    || message.contains("Previous attempts to find a Docker environment failed"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static void skipTests(Throwable cause)
    {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                false,
                () -> "Ducklake tests require a working Docker environment: " + cause.getMessage());
    }
}
