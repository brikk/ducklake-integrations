package dev.brikk.ducklake.doris.plugin

import org.apache.doris.connector.api.ConnectorPropertyMetadata

/**
 * Catalog-level property metadata. Doris's DDL layer reads this for
 * `SHOW CREATE CATALOG`, completion, validation hints — implementing
 * it correctly removes the need for a separate validation class.
 */
internal object DuckLakeConnectorProperties {

    const val METADATA_URL = "metadata.url"
    const val METADATA_USER = "metadata.user"
    const val METADATA_PASSWORD = "metadata.password"
    const val STORAGE_WAREHOUSE = "storage.warehouse"

    @JvmStatic
    fun catalogProperties(): List<ConnectorPropertyMetadata<*>> =
        listOf(
            ConnectorPropertyMetadata.requiredStringProperty(
                METADATA_URL,
                "JDBC URL of the DuckLake metadata database, e.g. " +
                    "jdbc:postgresql://host:5432/ducklake",
            ),
            ConnectorPropertyMetadata.requiredStringProperty(
                METADATA_USER,
                "Username for the DuckLake metadata database",
            ),
            ConnectorPropertyMetadata.stringProperty(
                METADATA_PASSWORD,
                "Password for the DuckLake metadata database. May be empty for trust auth.",
                "",
            ),
            ConnectorPropertyMetadata.requiredStringProperty(
                STORAGE_WAREHOUSE,
                "Warehouse root, e.g. s3://bucket/path or file:///local/path",
            ),
        )

    @JvmStatic
    fun requireString(props: Map<String, String>, key: String): String {
        val value = props[key]
        require(!value.isNullOrEmpty()) {
            "DuckLake catalog property '$key' is required"
        }
        return value
    }
}
