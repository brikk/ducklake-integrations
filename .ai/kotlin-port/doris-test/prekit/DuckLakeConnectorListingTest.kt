package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext
import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.spi.ConnectorContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * Exercises the read-only metadata path the FE walks for `SHOW DATABASES`
 * and `SHOW TABLES IN dl.<db>`. Goes through the public SPI surface
 * ([DuckLakeConnectorProvider.create]) so any wiring regression between
 * provider, connector, and metadata surfaces here.
 */
internal class DuckLakeConnectorListingTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "listing")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server.close()
        }
    }

    @Test
    fun providerValidatesRequiredProperties() {
        val provider = DuckLakeConnectorProvider()
        assertThatThrownBy { provider.validateProperties(mapOf("type" to "ducklake")) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("metadata.url")
    }

    @Test
    fun providerAcceptsEngineInjectedProperties() {
        // Doris's DDL layer injects engine-level properties like
        // `enable.mapping.varbinary` into every CREATE CATALOG; we must not
        // reject them.
        val provider = DuckLakeConnectorProvider()
        provider.validateProperties(
            mapOf(
                DuckLakeConnectorProperties.METADATA_URL to "jdbc:postgresql://x/y",
                DuckLakeConnectorProperties.METADATA_USER to "u",
                DuckLakeConnectorProperties.STORAGE_WAREHOUSE to "file:///tmp",
                "enable.mapping.varbinary" to "false",
            ),
        )
    }

    @Test
    @Throws(Exception::class)
    fun listsSeededSchemasAndTables() {
        val provider = DuckLakeConnectorProvider()
        assertThat(provider.type).isEqualTo("ducklake")

        val properties = mapOf(
            "type" to "ducklake",
            DuckLakeConnectorProperties.METADATA_URL to isolated.jdbcUrl(),
            DuckLakeConnectorProperties.METADATA_USER to isolated.user(),
            DuckLakeConnectorProperties.METADATA_PASSWORD to isolated.password(),
            DuckLakeConnectorProperties.STORAGE_WAREHOUSE to
                isolated.dataDir().toAbsolutePath().toString(),
        )

        provider.validateProperties(properties)

        val ctx: ConnectorContext = FakeConnectorContext("dl", 1L)
        provider.create(properties, ctx).use { connector ->
            val metadata = connector.getMetadata(null)

            // DuckLake bootstraps an implicit `main` schema; our seed adds two more.
            assertThat(metadata.listDatabaseNames(null))
                .contains("sales", "analytics")

            assertThat(metadata.databaseExists(null, "sales")).isTrue()
            assertThat(metadata.databaseExists(null, "does_not_exist")).isFalse()

            assertThat(metadata.listTableNames(null, "sales"))
                .containsExactlyInAnyOrder(
                    "orders", "customers", "returns_file", "returns_inline",
                )
            assertThat(metadata.listTableNames(null, "analytics"))
                .containsExactly("events")
            assertThat(metadata.listTableNames(null, "does_not_exist"))
                .isEmpty()

            // DESC dl.sales.orders — getTableHandle → getTableSchema / getColumnHandles
            val orders = metadata.getTableHandle(null, "sales", "orders")
                .orElseThrow { AssertionError("expected sales.orders handle") }
            assertThat(metadata.getTableHandle(null, "sales", "nope")).isEmpty()
            assertThat(metadata.getTableHandle(null, "nope", "orders")).isEmpty()

            val ordersSchema = metadata.getTableSchema(null, orders)
            assertThat(ordersSchema.tableName).isEqualTo("orders")
            assertThat(ordersSchema.columns)
                .extracting<String>(ConnectorColumn::getName)
                .containsExactly("id", "total")
            assertThat(ordersSchema.columns)
                .extracting<String> { c -> c.type.typeName }
                .containsExactly("INT", "DOUBLE")

            val ordersHandles = metadata.getColumnHandles(null, orders)
            assertThat(ordersHandles.keys).containsExactly("id", "total")

            // analytics.events tests timestamp + varchar; confirms the schema.events
            // table's typestrings round-trip through DuckLakeTypeMapping.
            val events = metadata.getTableHandle(null, "analytics", "events")
                .orElseThrow { AssertionError("expected analytics.events handle") }
            assertThat(metadata.getTableSchema(null, events).columns)
                .extracting<String> { c -> c.type.typeName }
                .containsExactly("DATETIMEV2", "STRING")
        }
    }
}
