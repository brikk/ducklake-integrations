package dev.brikk.ducklake.doris.plugin;

import java.util.List;
import java.util.Map;

import org.apache.doris.connector.api.ConnectorPropertyMetadata;

/**
 * Catalog-level property metadata. Doris's DDL layer reads this for
 * {@code SHOW CREATE CATALOG}, completion, validation hints — implementing
 * it correctly removes the need for a separate validation class.
 */
final class DuckLakeConnectorProperties {

    static final String METADATA_URL = "metadata.url";
    static final String METADATA_USER = "metadata.user";
    static final String METADATA_PASSWORD = "metadata.password";
    static final String STORAGE_WAREHOUSE = "storage.warehouse";

    private DuckLakeConnectorProperties() {}

    static List<ConnectorPropertyMetadata<?>> catalogProperties() {
        return List.of(
                ConnectorPropertyMetadata.requiredStringProperty(
                        METADATA_URL,
                        "JDBC URL of the DuckLake metadata database, e.g. "
                                + "jdbc:postgresql://host:5432/ducklake"),
                ConnectorPropertyMetadata.requiredStringProperty(
                        METADATA_USER,
                        "Username for the DuckLake metadata database"),
                ConnectorPropertyMetadata.stringProperty(
                        METADATA_PASSWORD,
                        "Password for the DuckLake metadata database. May be empty for trust auth.",
                        ""),
                ConnectorPropertyMetadata.requiredStringProperty(
                        STORAGE_WAREHOUSE,
                        "Warehouse root, e.g. s3://bucket/path or file:///local/path"));
    }

    static String requireString(Map<String, String> props, String key) {
        String value = props.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(
                    "DuckLake catalog property '" + key + "' is required");
        }
        return value;
    }
}
