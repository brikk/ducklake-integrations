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

    // Mirrors the in-tree iceberg connector's ENABLE_MAPPING_TIMESTAMP_TZ
    // (fe-connector-iceberg IcebergConnectorProperties): default false maps a
    // DuckLake `timestamptz` to naive DATETIMEV2 (correct UTC values, zone-naive
    // typing) for broad BE compatibility; true maps to zone-aware TIMESTAMPTZ,
    // which needs a BE new enough to read TIMESTAMP_MICROS(isAdjustedToUtc) into
    // a TimeStampTz slot (Int64ToTimestampTz, master-only; NOT in 4.0.x/4.1.x releases incl. 4.1.2 — verified).
    const val ENABLE_MAPPING_TIMESTAMP_TZ = "enable.mapping.timestamp_tz"

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
            ConnectorPropertyMetadata.booleanProperty(
                ENABLE_MAPPING_TIMESTAMP_TZ,
                "Map DuckLake timestamptz to zone-aware Doris TIMESTAMPTZ (needs a master/nightly BE — the converter is NOT in any 4.1.x release); " +
                    "default false maps to naive DATETIMEV2 (correct UTC values).",
                false,
            ),
        )

    fun requireString(props: Map<String, String>, key: String): String {
        val value = props[key]
        require(!value.isNullOrEmpty()) {
            "DuckLake catalog property '$key' is required"
        }
        return value
    }
}
