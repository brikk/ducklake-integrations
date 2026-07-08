package dev.brikk.ducklake.doris.plugin

import java.sql.DriverManager

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * Verifies [DuckLakeConnector]'s Postgres-driver cleanup on [DuckLakeConnector.close] never
 * disturbs a Postgres driver that some other component owns — the safety half of the
 * plugin-classloader Metaspace-leak fix (see `dev-docs/TODO-read.md` and upstream `34bd8eede75`).
 *
 * The connector only deregisters the registered PG driver when it runs under an ISOLATED plugin
 * classloader (production); under a shared classloader it leaves the driver alone (there's no leak
 * to fix and the driver is shared). This test runs on the flat test classpath — a shared classloader
 * — where the JVM's ServiceLoader already registered the process-wide `org.postgresql.Driver` that
 * other tests' Testcontainers datasources depend on. It asserts that opening and closing a DuckLake
 * connector (even repeatedly) leaves that shared driver resolvable, i.e. `close()`'s cleanup is a
 * no-op here rather than deregistering the shared driver. (The isolated-loader deregistration path is
 * exercised by the live compose smoke, where the plugin runs under a real dedicated classloader.)
 */
internal class DuckLakeConnectorDriverDeregisterTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "driver_dereg")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server.close()
        }
    }

    @Test
    fun openThenCloseLeavesTheSharedPostgresDriverUsable() {
        val url = server.getJdbcUrl()
        // Precondition: the shared org.postgresql.Driver (ServiceLoader-registered) resolves the URL.
        assertThat(DriverManager.getDriver(url)).isNotNull()

        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        DuckLakeConnectorProvider().create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
            // Force catalog construction → registerPostgresDriver() registers OUR own driver instance.
            connector.getMetadata(null).listDatabaseNames(null)
        } // .use → close() deregisters ONLY our instance

        // The shared driver must still be registered and resolve the URL — close() didn't touch it.
        assertThat(DriverManager.getDriver(url)).isNotNull()
    }

    @Test
    fun repeatedOpenCloseCyclesKeepTheSharedDriverUsable() {
        val url = server.getJdbcUrl()
        val properties = DorisTestIdiomKit.isolatedProperties(isolated)
        repeat(3) {
            DuckLakeConnectorProvider().create(properties, FakeConnectorContext("dl", 1L)).use { connector ->
                connector.getMetadata(null).listDatabaseNames(null)
            }
            // Each cycle registers+deregisters our own instance; the shared driver is unaffected.
            assertThat(DriverManager.getDriver(url)).isNotNull()
        }
    }
}
