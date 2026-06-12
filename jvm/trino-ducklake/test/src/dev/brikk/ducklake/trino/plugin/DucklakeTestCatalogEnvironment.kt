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
import org.junit.jupiter.api.Assumptions
import java.nio.file.Path
import java.util.Locale
import java.util.Optional

/**
 * Manages the shared catalog-backend fixtures used by Trino plugin tests. Holds
 * one lazily-created [TestingDucklakePostgreSqlCatalogServer] for the
 * default (PG) backend and one [TestingDucklakeDuckDbQuackCatalogServer]
 * for the Quack-backed DuckDB backend, with selection driven by
 * [DucklakeTestCatalogBackend.fromSystemProperty].
 *
 *
 * Backend selection is read once per JVM and pinned for the life of the
 * test process — flipping the system property mid-suite isn't supported.
 */
object DucklakeTestCatalogEnvironment {
    private val LOCK = Any()

    @Volatile
    private var pgServer: TestingDucklakePostgreSqlCatalogServer? = null

    @Volatile
    private var quackServer: TestingDucklakeDuckDbQuackCatalogServer? = null

    @Volatile
    private var localDuckDbFixture: TestingDucklakeLocalDuckDbCatalogFixture? = null

    @Volatile
    private var serverUnavailable: RuntimeException? = null

    @Volatile
    private var pgCatalogGenerated: Boolean = false

    @JvmStatic
    fun selectedBackend(): DucklakeTestCatalogBackend {
        return DucklakeTestCatalogBackend.fromSystemProperty()
    }

    /**
     * Returns the PG server. Only valid when the selected backend is POSTGRES
     * — callers that need backend-pluggable behaviour should branch on
     * [selectedBackend] first. Kept as-is for source compatibility
     * with the existing PG-only test code.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun getServer(): TestingDucklakePostgreSqlCatalogServer {
        return ensurePostgresServer()
    }

    @JvmStatic
    @Throws(Exception::class)
    fun getQuackServer(): TestingDucklakeDuckDbQuackCatalogServer {
        return ensureQuackServer()
    }

    @JvmStatic
    fun getLocalDuckDbFixture(): TestingDucklakeLocalDuckDbCatalogFixture {
        return ensureLocalDuckDbFixture()
    }

    @JvmStatic
    @Throws(Exception::class)
    fun createDucklakeConfig(): DucklakeConfig {
        // Only the shared (non-isolated) PG catalog flow runs through here today;
        // it's the path used by tests that don't call useIsolatedCatalog and
        // expect the pre-bootstrapped 17-table fixture. The Quack backend doesn't
        // ship that bootstrap yet (see DucklakeCatalogGenerator) so any test
        // reaching this code path under DUCKDB_QUACK will need migration to the
        // isolated-catalog form.
        if (selectedBackend() != DucklakeTestCatalogBackend.POSTGRES) {
            throw IllegalStateException(
                "createDucklakeConfig (shared catalog) is only supported under POSTGRES backend; "
                        + "selected backend is " + selectedBackend()
                        + ". Use useIsolatedCatalog(...) on the query-runner builder instead.")
        }
        val server = ensurePostgresServer()
        ensureCatalogGenerated(server)

        return DucklakeConfig()
            .setMaxCatalogConnections(5)
            .setCatalogDatabaseUrl(server.getJdbcUrl())
            .setCatalogDatabaseUser(server.getUser())
            .setCatalogDatabasePassword(server.getPassword())
            .setDataPath(DucklakeCatalogGenerator.getPostgreSqlCatalogDirectory().resolve("data").toAbsolutePath().toString())
    }

    @JvmStatic
    @Throws(Exception::class)
    fun getConnectorProperties(): Map<String, String> {
        val config = createDucklakeConfig()
        val properties = ImmutableMap.builder<String, String>()
            .put("ducklake.catalog.database-url", requireNotNull(config.getCatalogDatabaseUrl()) { "catalogDatabaseUrl is null" })
            .put("ducklake.data-path", requireNotNull(config.getDataPath()) { "dataPath is null" })

        val catalogDatabaseUser = config.getCatalogDatabaseUser()
        if (catalogDatabaseUser != null) {
            properties.put("ducklake.catalog.database-user", catalogDatabaseUser)
        }
        val catalogDatabasePassword = config.getCatalogDatabasePassword()
        if (catalogDatabasePassword != null) {
            properties.put("ducklake.catalog.database-password", catalogDatabasePassword)
        }

        return properties.buildOrThrow()
    }

    @Throws(Exception::class)
    private fun ensurePostgresServer(): TestingDucklakePostgreSqlCatalogServer {
        val unavailable = serverUnavailable
        if (unavailable != null) {
            skipTests(unavailable)
        }

        var result = pgServer
        if (result == null) {
            synchronized(LOCK) {
                result = pgServer
                if (result == null) {
                    try {
                        result = TestingDucklakePostgreSqlCatalogServer()
                        pgServer = result
                        Runtime.getRuntime().addShutdownHook(Thread { result!!.close() })
                    }
                    catch (e: RuntimeException) {
                        if (isDockerUnavailable(e)) {
                            serverUnavailable = e
                            skipTests(e)
                        }
                        throw e
                    }
                }
            }
        }
        return result!!
    }

    @Throws(Exception::class)
    private fun ensureQuackServer(): TestingDucklakeDuckDbQuackCatalogServer {
        val unavailable = serverUnavailable
        if (unavailable != null) {
            skipTests(unavailable)
        }

        var result = quackServer
        if (result == null) {
            synchronized(LOCK) {
                result = quackServer
                if (result == null) {
                    try {
                        // The Quack catalog container runs Linux; mount the
                        // platform-matched binary (linux-arm64 on macOS arm64
                        // hosts via Docker Desktop, linux-amd64 on Linux amd64
                        // hosts). System-property override wins.
                        val containerArch = if (System.getProperty("os.arch", "").lowercase(Locale.ROOT).contains("aarch"))
                            "linux-arm64" else "linux-amd64"
                        val configuredPath: Path? = System.getProperty("ducklake.test.parityExtensionPath")
                            ?.trim()
                            ?.takeIf { it.isNotEmpty() }
                            ?.let(Path::of)
                        val extensionPath: Optional<Path> = Optional.ofNullable(
                                configuredPath
                                        ?: TrinoParityExtensionResolver.resolveBundledExtensionPathFor(containerArch)
                                                ?.let(Path::of))
                        result = TestingDucklakeDuckDbQuackCatalogServer(extensionPath)
                        quackServer = result
                        Runtime.getRuntime().addShutdownHook(Thread { result!!.close() })
                    }
                    catch (e: RuntimeException) {
                        if (isDockerUnavailable(e)) {
                            serverUnavailable = e
                            skipTests(e)
                        }
                        throw e
                    }
                }
            }
        }
        return result!!
    }

    private fun ensureLocalDuckDbFixture(): TestingDucklakeLocalDuckDbCatalogFixture {
        var result = localDuckDbFixture
        if (result == null) {
            synchronized(LOCK) {
                result = localDuckDbFixture
                if (result == null) {
                    result = TestingDucklakeLocalDuckDbCatalogFixture()
                    localDuckDbFixture = result
                    Runtime.getRuntime().addShutdownHook(Thread { result!!.close() })
                }
            }
        }
        return result!!
    }

    @Throws(Exception::class)
    private fun ensureCatalogGenerated(server: TestingDucklakePostgreSqlCatalogServer) {
        if (!pgCatalogGenerated) {
            synchronized(LOCK) {
                if (!pgCatalogGenerated) {
                    DucklakeCatalogGenerator.generatePostgreSqlCatalog(server)
                    pgCatalogGenerated = true
                }
            }
        }
    }

    @JvmStatic
    internal fun isDockerUnavailable(throwable: Throwable): Boolean {
        var current: Throwable? = throwable
        while (current != null) {
            val message = current.message
            if (message != null && (message.contains("Could not find a valid Docker environment")
                        || message.contains("Previous attempts to find a Docker environment failed"))) {
                return true
            }
            current = current.cause
        }
        return false
    }

    private fun skipTests(cause: Throwable) {
        Assumptions.assumeTrue(
            false
        ) { "Ducklake tests require a working Docker environment: " + cause.message }
    }
}
