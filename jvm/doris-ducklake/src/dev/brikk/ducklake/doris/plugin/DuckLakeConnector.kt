package dev.brikk.ducklake.doris.plugin

import java.io.IOException
import java.util.EnumSet

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog

import org.apache.doris.connector.api.Connector
import org.apache.doris.connector.api.ConnectorCapability
import org.apache.doris.connector.api.ConnectorMetadata
import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider
import org.apache.doris.connector.spi.ConnectorContext

/**
 * Read-side DuckLake [Connector]. Owns a single [JdbcDucklakeCatalog]
 * lazily constructed from the catalog properties on first metadata call. Capability
 * set will grow as features land (see `ducklake-doris-todo.md`).
 */
class DuckLakeConnector internal constructor(
    private val properties: Map<String, String>,
    private val context: ConnectorContext,
) : Connector {

    @Volatile
    private var catalog: JdbcDucklakeCatalog? = null

    @Volatile
    private var scanPlanProvider: DuckLakeScanPlanProvider? = null

    override fun getMetadata(session: ConnectorSession?): ConnectorMetadata =
        DuckLakeConnectorMetadata(catalog())

    override fun getScanPlanProvider(): ConnectorScanPlanProvider {
        var local = scanPlanProvider
        if (local == null) {
            synchronized(this) {
                local = scanPlanProvider
                if (local == null) {
                    val cat = catalog()
                    val warehouse = DuckLakeConnectorProperties.requireString(
                        properties, DuckLakeConnectorProperties.STORAGE_WAREHOUSE,
                    )
                    local = DuckLakeScanPlanProvider(
                        cat,
                        DuckLakePathResolver(cat, warehouse),
                        properties,
                    )
                    scanPlanProvider = local
                }
            }
        }
        return local!!
    }

    /**
     * v1 capabilities (see `ducklake-doris-todo.md`): `SELECT *` with snapshot
     * pinning (MVCC), time travel, partition pruning, and statistics. The
     * P-series SPI dropped `SUPPORTS_POSITION_DELETE` — position deletes ride
     * the scan range's delete-file list, not a capability flag. Filter /
     * projection / limit pushdown stay off until the corresponding `apply*`
     * methods land on [DuckLakeConnectorMetadata] — declaring without
     * implementing crashes the planner.
     */
    override fun getCapabilities(): Set<ConnectorCapability> =
        EnumSet.of(
            ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
            ConnectorCapability.SUPPORTS_TIME_TRAVEL,
            ConnectorCapability.SUPPORTS_PARTITION_PRUNING,
            ConnectorCapability.SUPPORTS_STATISTICS,
        )

    private fun catalog(): JdbcDucklakeCatalog {
        var local = catalog
        if (local == null) {
            synchronized(this) {
                local = catalog
                if (local == null) {
                    local = buildCatalog()
                    catalog = local
                }
            }
        }
        return local!!
    }

    private fun buildCatalog(): JdbcDucklakeCatalog {
        // Force Postgres JDBC driver registration on the plugin classloader.
        // DriverManager consults its registry (populated by ServiceLoader at
        // JVM startup with the *system* classloader) — our plugin jar is on a
        // child classloader, so its META-INF/services/java.sql.Driver isn't
        // discovered without explicit Class.forName from inside the plugin.
        try {
            Class.forName("org.postgresql.Driver")
        } catch (e: ClassNotFoundException) {
            throw IllegalStateException(
                "PostgreSQL JDBC driver missing from plugin classpath", e,
            )
        }
        val config = DucklakeCatalogConfig().apply {
            catalogDatabaseUrl = DuckLakeConnectorProperties.requireString(
                properties, DuckLakeConnectorProperties.METADATA_URL,
            )
            catalogDatabaseUser = DuckLakeConnectorProperties.requireString(
                properties, DuckLakeConnectorProperties.METADATA_USER,
            )
            catalogDatabasePassword =
                properties.getOrDefault(DuckLakeConnectorProperties.METADATA_PASSWORD, "")
            dataPath = DuckLakeConnectorProperties.requireString(
                properties, DuckLakeConnectorProperties.STORAGE_WAREHOUSE,
            )
        }
        return JdbcDucklakeCatalog(config)
    }

    @Throws(IOException::class)
    override fun close() {
        catalog?.close()
    }

    internal fun properties(): Map<String, String> = properties

    internal fun context(): ConnectorContext = context
}
