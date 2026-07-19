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

import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.TestingDucklakeDuckDbQuackCatalogServer
import dev.brikk.ducklake.catalog.TestingDucklakeLocalDuckDbCatalogFixture
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import io.airlift.log.Logger
import io.airlift.log.Logging
import io.airlift.testing.Closeables.closeAllSuppress
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder

object DucklakeQueryRunner {
    private const val CATALOG = "ducklake"

    @JvmStatic
    fun builder(): Builder {
        return Builder()
    }

    class Builder internal constructor() : DistributedQueryRunner.Builder<Builder>(
            testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema("test_schema")
                    .build()) {
        private val connectorProperties: ImmutableMap.Builder<String, String> = ImmutableMap.builder()
        private var isolatedCatalogName: String? = null

        fun addConnectorProperty(key: String, value: String): Builder {
            this.connectorProperties.put(key, value)
            return self()
        }

        /**
         * Use an isolated catalog (fresh PostgreSQL database) instead of the shared test catalog.
         * Each isolated catalog gets its own database in the shared PostgreSQL container,
         * preventing cross-test interference from write operations.
         */
        fun useIsolatedCatalog(testName: String): Builder {
            this.isolatedCatalogName = testName
            return self()
        }

        @Throws(Exception::class)
        override fun build(): DistributedQueryRunner {
            val queryRunner: DistributedQueryRunner = super.build()
            try {
                val baseProperties: Map<String, String> = if (isolatedCatalogName != null) {
                    buildIsolatedCatalogProperties(isolatedCatalogName!!)
                }
                else {
                    DucklakeTestCatalogEnvironment.getConnectorProperties()
                }

                val propertiesBuilder: ImmutableMap.Builder<String, String> = ImmutableMap.builder<String, String>()
                        .putAll(baseProperties)
                        .put("fs.hadoop.enabled", "true")
                        .putAll(connectorProperties.buildOrThrow())
                val properties: Map<String, String> = propertiesBuilder.buildOrThrow()

                queryRunner.installPlugin(DucklakePlugin())
                queryRunner.createCatalog(CATALOG, "ducklake", properties)

                return queryRunner
            }
            catch (e: Throwable) {
                closeAllSuppress(e, queryRunner)
                throw e
            }
        }

        companion object {
            @Throws(Exception::class)
            private fun buildIsolatedCatalogProperties(testName: String): Map<String, String> {
                val backend = DucklakeTestCatalogEnvironment.selectedBackend()
                val isolated: DucklakeCatalogGenerator.IsolatedCatalog = when (backend) {
                    DucklakeTestCatalogBackend.POSTGRES -> {
                        val server: TestingDucklakePostgreSqlCatalogServer = DucklakeTestCatalogEnvironment.getServer()
                        DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(server, testName)
                    }
                    DucklakeTestCatalogBackend.DUCKDB_LOCAL -> {
                        val fixture: TestingDucklakeLocalDuckDbCatalogFixture = DucklakeTestCatalogEnvironment.getLocalDuckDbFixture()
                        DucklakeCatalogGenerator.generateIsolatedLocalDuckDbCatalog(fixture, testName)
                    }
                    DucklakeTestCatalogBackend.DUCKDB_QUACK -> {
                        val server: TestingDucklakeDuckDbQuackCatalogServer = DucklakeTestCatalogEnvironment.getQuackServer()
                        DucklakeCatalogGenerator.generateIsolatedDuckDbQuackCatalog(server, testName)
                    }
                }
                val properties: ImmutableMap.Builder<String, String> = ImmutableMap.builder<String, String>()
                        .put("ducklake.catalog.database-url", isolated.jdbcUrl)
                        .put("ducklake.data-path", isolated.dataDir.toAbsolutePath().toString())
                if (isolated.user != null) {
                    properties.put("ducklake.catalog.database-user", isolated.user)
                }
                if (isolated.password != null) {
                    properties.put("ducklake.catalog.database-password", isolated.password)
                }
                return properties.buildOrThrow()
            }
        }
    }

    @JvmStatic
    @Throws(Exception::class)
    fun main() {
        Logging.initialize()

        @Suppress("UNUSED_VARIABLE")
        val queryRunner: DistributedQueryRunner = DucklakeQueryRunner.builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build()

        val log: Logger = Logger.get(DucklakeQueryRunner::class.java)
        log.info("======== SERVER STARTED ========")
        log.info("\n====\n%s\n====", queryRunner.coordinator.baseUrl)
    }
}
