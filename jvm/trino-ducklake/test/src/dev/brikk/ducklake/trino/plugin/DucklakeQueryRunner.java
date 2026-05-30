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
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class DucklakeQueryRunner
{
    private static final String CATALOG = "ducklake";

    private DucklakeQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        private String isolatedCatalogName;

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema("test_schema")
                    .build());
        }

        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return self();
        }

        /**
         * Use an isolated catalog (fresh PostgreSQL database) instead of the shared test catalog.
         * Each isolated catalog gets its own database in the shared PostgreSQL container,
         * preventing cross-test interference from write operations.
         */
        public Builder useIsolatedCatalog(String testName)
        {
            this.isolatedCatalogName = testName;
            return self();
        }

        private static Map<String, String> buildIsolatedCatalogProperties(String testName)
                throws Exception
        {
            DucklakeTestCatalogBackend backend = DucklakeTestCatalogEnvironment.selectedBackend();
            DucklakeCatalogGenerator.IsolatedCatalog isolated;
            switch (backend) {
                case POSTGRES -> {
                    TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
                    isolated = DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(server, testName);
                }
                case DUCKDB_LOCAL -> {
                    TestingDucklakeLocalDuckDbCatalogFixture fixture = DucklakeTestCatalogEnvironment.getLocalDuckDbFixture();
                    isolated = DucklakeCatalogGenerator.generateIsolatedLocalDuckDbCatalog(fixture, testName);
                }
                case DUCKDB_QUACK -> {
                    TestingDucklakeDuckDbQuackCatalogServer server = DucklakeTestCatalogEnvironment.getQuackServer();
                    isolated = DucklakeCatalogGenerator.generateIsolatedDuckDbQuackCatalog(server, testName);
                }
                default -> throw new IllegalStateException("Unsupported backend: " + backend);
            }
            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("ducklake.catalog.database-url", isolated.jdbcUrl())
                    .put("ducklake.data-path", isolated.dataDir().toAbsolutePath().toString());
            if (isolated.user() != null) {
                properties.put("ducklake.catalog.database-user", isolated.user());
            }
            if (isolated.password() != null) {
                properties.put("ducklake.catalog.database-password", isolated.password());
            }
            return properties.buildOrThrow();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                Map<String, String> baseProperties;
                if (isolatedCatalogName != null) {
                    baseProperties = buildIsolatedCatalogProperties(isolatedCatalogName);
                }
                else {
                    baseProperties = DucklakeTestCatalogEnvironment.getConnectorProperties();
                }

                ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
                        .putAll(baseProperties)
                        .put("fs.hadoop.enabled", "true")
                        .putAll(connectorProperties.buildOrThrow());
                // Test hook: when a built trino_parity.duckdb_extension is available
                // (path passed via -Dducklake.test.parityExtensionPath=...), thread
                // it into the catalog so the in-process executor LOADs the binary
                // instead of replaying the in-tree SQL aliases. Otherwise the
                // executor falls back to TrinoFunctionAliases.applyDirect — same
                // behaviour as before the extension existed.
                String testExtensionPath = System.getProperty("ducklake.test.parityExtensionPath");
                if (testExtensionPath != null && !testExtensionPath.isBlank()
                        && !connectorProperties.buildOrThrow().containsKey("ducklake.duckdb.parity-extension-path")) {
                    propertiesBuilder.put("ducklake.duckdb.parity-extension-path", testExtensionPath);
                }
                Map<String, String> properties = propertiesBuilder.buildOrThrow();

                queryRunner.installPlugin(new DucklakePlugin());
                queryRunner.createCatalog(CATALOG, "ducklake", properties);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    static void main()
            throws Exception
    {
        Logging.initialize();

        @SuppressWarnings("resource")
        DistributedQueryRunner queryRunner = DucklakeQueryRunner.builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();

        Logger log = Logger.get(DucklakeQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
