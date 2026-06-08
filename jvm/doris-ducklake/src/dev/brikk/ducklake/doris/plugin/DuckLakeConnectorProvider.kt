package dev.brikk.ducklake.doris.plugin

import org.apache.doris.connector.api.Connector
import org.apache.doris.connector.spi.ConnectorContext
import org.apache.doris.connector.spi.ConnectorProvider

/**
 * `ServiceLoader` entry for the DuckLake connector. Registered via
 * `META-INF/services/org.apache.doris.connector.spi.ConnectorProvider`.
 */
class DuckLakeConnectorProvider : ConnectorProvider {

    override fun getType(): String = TYPE

    override fun validateProperties(properties: Map<String, String>) {
        // Required-property check only. Doris's DDL layer injects engine-level
        // properties (e.g. `enable.mapping.varbinary`) into every CREATE CATALOG
        // regardless of plugin, so a strict unknown-property check would reject
        // valid usage.
        for (meta in DuckLakeConnectorProperties.catalogProperties()) {
            if (meta.isRequired && properties[meta.name].isNullOrEmpty()) {
                throw IllegalArgumentException(
                    "DuckLake catalog property '${meta.name}' is required",
                )
            }
        }
    }

    override fun create(properties: Map<String, String>, context: ConnectorContext): Connector =
        DuckLakeConnector(properties, context)

    companion object {
        const val TYPE: String = "ducklake"
    }
}
