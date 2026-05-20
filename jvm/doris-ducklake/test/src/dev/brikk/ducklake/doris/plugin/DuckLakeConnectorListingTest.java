package dev.brikk.ducklake.doris.plugin;

import java.util.Map;

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.ConnectorContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the read-only metadata path the FE walks for {@code SHOW DATABASES}
 * and {@code SHOW TABLES IN dl.<db>}. Goes through the public SPI surface
 * ({@link DuckLakeConnectorProvider#create}) so any wiring regression between
 * provider, connector, and metadata surfaces here.
 */
class DuckLakeConnectorListingTest {

    private static TestingDucklakePostgreSqlCatalogServer server;
    private static DuckLakeTestCatalogBootstrap.IsolatedCatalog isolated;

    @BeforeAll
    static void setUp() throws Exception {
        server = new TestingDucklakePostgreSqlCatalogServer();
        isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "listing");
    }

    @AfterAll
    static void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void providerValidatesRequiredProperties() {
        DuckLakeConnectorProvider provider = new DuckLakeConnectorProvider();
        assertThatThrownBy(() -> provider.validateProperties(Map.of("type", "ducklake")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("metadata.url");
    }

    @Test
    void providerAcceptsEngineInjectedProperties() {
        // Doris's DDL layer injects engine-level properties like
        // `enable.mapping.varbinary` into every CREATE CATALOG; we must not
        // reject them.
        DuckLakeConnectorProvider provider = new DuckLakeConnectorProvider();
        provider.validateProperties(Map.of(
                DuckLakeConnectorProperties.METADATA_URL, "jdbc:postgresql://x/y",
                DuckLakeConnectorProperties.METADATA_USER, "u",
                DuckLakeConnectorProperties.STORAGE_WAREHOUSE, "file:///tmp",
                "enable.mapping.varbinary", "false"));
    }

    @Test
    void listsSeededSchemasAndTables() throws Exception {
        DuckLakeConnectorProvider provider = new DuckLakeConnectorProvider();
        assertThat(provider.getType()).isEqualTo("ducklake");

        Map<String, String> properties = Map.of(
                "type", "ducklake",
                DuckLakeConnectorProperties.METADATA_URL, isolated.jdbcUrl(),
                DuckLakeConnectorProperties.METADATA_USER, isolated.user(),
                DuckLakeConnectorProperties.METADATA_PASSWORD, isolated.password(),
                DuckLakeConnectorProperties.STORAGE_WAREHOUSE, isolated.dataDir().toAbsolutePath().toString());

        provider.validateProperties(properties);

        ConnectorContext ctx = new FakeConnectorContext("dl", 1L);
        try (Connector connector = provider.create(properties, ctx)) {
            ConnectorMetadata metadata = connector.getMetadata(null);

            // DuckLake bootstraps an implicit `main` schema; our seed adds two more.
            assertThat(metadata.listDatabaseNames(null))
                    .contains("sales", "analytics");

            assertThat(metadata.databaseExists(null, "sales")).isTrue();
            assertThat(metadata.databaseExists(null, "does_not_exist")).isFalse();

            assertThat(metadata.listTableNames(null, "sales"))
                    .containsExactlyInAnyOrder(
                            "orders", "customers", "returns_file", "returns_inline");
            assertThat(metadata.listTableNames(null, "analytics"))
                    .containsExactly("events");
            assertThat(metadata.listTableNames(null, "does_not_exist"))
                    .isEmpty();

            // DESC dl.sales.orders — getTableHandle → getTableSchema / getColumnHandles
            ConnectorTableHandle orders = metadata.getTableHandle(null, "sales", "orders")
                    .orElseThrow(() -> new AssertionError("expected sales.orders handle"));
            assertThat(metadata.getTableHandle(null, "sales", "nope")).isEmpty();
            assertThat(metadata.getTableHandle(null, "nope", "orders")).isEmpty();

            ConnectorTableSchema ordersSchema = metadata.getTableSchema(null, orders);
            assertThat(ordersSchema.getTableName()).isEqualTo("orders");
            assertThat(ordersSchema.getColumns())
                    .extracting(ConnectorColumn::getName)
                    .containsExactly("id", "total");
            assertThat(ordersSchema.getColumns())
                    .extracting(c -> c.getType().getTypeName())
                    .containsExactly("INT", "DOUBLE");

            Map<String, ConnectorColumnHandle> ordersHandles = metadata.getColumnHandles(null, orders);
            assertThat(ordersHandles.keySet()).containsExactly("id", "total");

            // analytics.events tests timestamp + varchar; confirms the schema.events
            // table's typestrings round-trip through DuckLakeTypeMapping.
            ConnectorTableHandle events = metadata.getTableHandle(null, "analytics", "events")
                    .orElseThrow(() -> new AssertionError("expected analytics.events handle"));
            assertThat(metadata.getTableSchema(null, events).getColumns())
                    .extracting(c -> c.getType().getTypeName())
                    .containsExactly("DATETIMEV2", "STRING");
        }
    }
}
