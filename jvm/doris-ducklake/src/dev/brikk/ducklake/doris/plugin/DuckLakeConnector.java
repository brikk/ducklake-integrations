package dev.brikk.ducklake.doris.plugin;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.ConnectorContext;

/**
 * Read-side DuckLake {@link Connector}. Owns a single {@link JdbcDucklakeCatalog}
 * lazily constructed from the catalog properties on first metadata call. Capability
 * set will grow as features land (see {@code ducklake-doris-todo.md}).
 */
public final class DuckLakeConnector implements Connector {

    private final Map<String, String> properties;
    private final ConnectorContext context;

    private volatile JdbcDucklakeCatalog catalog;
    private volatile DuckLakeScanPlanProvider scanPlanProvider;

    DuckLakeConnector(Map<String, String> properties, ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        return new DuckLakeConnectorMetadata(catalog());
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        DuckLakeScanPlanProvider local = scanPlanProvider;
        if (local == null) {
            synchronized (this) {
                local = scanPlanProvider;
                if (local == null) {
                    JdbcDucklakeCatalog cat = catalog();
                    String warehouse = DuckLakeConnectorProperties.requireString(
                            properties, DuckLakeConnectorProperties.STORAGE_WAREHOUSE);
                    local = new DuckLakeScanPlanProvider(
                            cat,
                            new DuckLakePathResolver(cat, warehouse),
                            properties);
                    scanPlanProvider = local;
                }
            }
        }
        return local;
    }

    /**
     * v1 capabilities, sized to the roadmap "Step 2" of
     * {@code ducklake-doris-todo.md}: enough for {@code SELECT *} with
     * snapshot pinning, position deletes, time travel, partition pruning,
     * and statistics. Filter / projection / limit pushdown stay off until
     * the corresponding {@code apply*} methods land on
     * {@link DuckLakeConnectorMetadata} — declaring without implementing
     * crashes the planner.
     */
    @Override
    public Set<ConnectorCapability> getCapabilities() {
        return EnumSet.of(
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
                ConnectorCapability.SUPPORTS_POSITION_DELETE,
                ConnectorCapability.SUPPORTS_TIME_TRAVEL,
                ConnectorCapability.SUPPORTS_PARTITION_PRUNING,
                ConnectorCapability.SUPPORTS_STATISTICS);
    }

    private JdbcDucklakeCatalog catalog() {
        JdbcDucklakeCatalog local = catalog;
        if (local == null) {
            synchronized (this) {
                local = catalog;
                if (local == null) {
                    local = buildCatalog();
                    catalog = local;
                }
            }
        }
        return local;
    }

    private JdbcDucklakeCatalog buildCatalog() {
        // Force Postgres JDBC driver registration on the plugin classloader.
        // DriverManager consults its registry (populated by ServiceLoader at
        // JVM startup with the *system* classloader) — our plugin jar is on a
        // child classloader, so its META-INF/services/java.sql.Driver isn't
        // discovered without explicit Class.forName from inside the plugin.
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "PostgreSQL JDBC driver missing from plugin classpath", e);
        }
        DucklakeCatalogConfig config = new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(
                        DuckLakeConnectorProperties.requireString(
                                properties, DuckLakeConnectorProperties.METADATA_URL))
                .setCatalogDatabaseUser(
                        DuckLakeConnectorProperties.requireString(
                                properties, DuckLakeConnectorProperties.METADATA_USER))
                .setCatalogDatabasePassword(
                        properties.getOrDefault(DuckLakeConnectorProperties.METADATA_PASSWORD, ""))
                .setDataPath(
                        DuckLakeConnectorProperties.requireString(
                                properties, DuckLakeConnectorProperties.STORAGE_WAREHOUSE));
        return new JdbcDucklakeCatalog(config);
    }

    @Override
    public java.util.List<org.apache.doris.connector.api.ConnectorPropertyMetadata<?>> getCatalogProperties() {
        return DuckLakeConnectorProperties.catalogProperties();
    }

    @Override
    public void close() throws IOException {
        JdbcDucklakeCatalog local = catalog;
        if (local != null) {
            local.close();
        }
    }

    Map<String, String> properties() {
        return properties;
    }

    ConnectorContext context() {
        return context;
    }
}
