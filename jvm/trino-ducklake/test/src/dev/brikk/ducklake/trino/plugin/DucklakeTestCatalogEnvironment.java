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

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Manages the shared PostgreSQL test catalog environment.
 * Provides a singleton PostgreSQL Testcontainer and generates the shared
 * test catalog on first use.
 */
public final class DucklakeTestCatalogEnvironment
{
    private static final Object LOCK = new Object();

    private static volatile TestingDucklakePostgreSqlCatalogServer server;
    private static volatile RuntimeException serverUnavailable;
    private static volatile boolean catalogGenerated;

    private DucklakeTestCatalogEnvironment() {}

    public static TestingDucklakePostgreSqlCatalogServer getServer()
            throws Exception
    {
        return ensureServer();
    }

    public static DucklakeConfig createDucklakeConfig()
            throws Exception
    {
        TestingDucklakePostgreSqlCatalogServer pgServer = ensureServer();
        ensureCatalogGenerated(pgServer);

        return new DucklakeConfig()
                .setMaxCatalogConnections(5)
                .setCatalogDatabaseUrl(pgServer.getJdbcUrl())
                .setCatalogDatabaseUser(pgServer.getUser())
                .setCatalogDatabasePassword(pgServer.getPassword())
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

    private static TestingDucklakePostgreSqlCatalogServer ensureServer()
            throws Exception
    {
        RuntimeException unavailable = serverUnavailable;
        if (unavailable != null) {
            skipTests(unavailable);
        }

        TestingDucklakePostgreSqlCatalogServer result = server;
        if (result == null) {
            synchronized (LOCK) {
                result = server;
                if (result == null) {
                    try {
                        result = new TestingDucklakePostgreSqlCatalogServer();
                        server = result;
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

    private static void ensureCatalogGenerated(TestingDucklakePostgreSqlCatalogServer pgServer)
            throws Exception
    {
        if (!catalogGenerated) {
            synchronized (LOCK) {
                if (!catalogGenerated) {
                    DucklakeCatalogGenerator.generatePostgreSqlCatalog(pgServer);
                    catalogGenerated = true;
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
                () -> "Ducklake tests require a working Docker environment for PostgreSQL: " + cause.getMessage());
    }
}
