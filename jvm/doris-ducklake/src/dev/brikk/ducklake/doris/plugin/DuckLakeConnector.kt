package dev.brikk.ducklake.doris.plugin

import java.io.IOException
import java.sql.Driver
import java.sql.DriverManager
import java.sql.SQLException
import java.util.EnumSet

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog

import org.apache.doris.connector.api.Connector
import org.apache.doris.connector.api.ConnectorCapability
import org.apache.doris.connector.api.ConnectorMetadata
import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider
import org.apache.doris.connector.spi.ConnectorContext

/**
 * Read-side DuckLake [Connector]. Owns a single [JdbcDucklakeCatalog]
 * lazily constructed from the catalog properties on first metadata call. Capability
 * set will grow as features land (see `dev-docs/TODO-read.md`).
 */
class DuckLakeConnector internal constructor(
    private val properties: Map<String, String>,
    private val context: ConnectorContext,
) : Connector {

    @Volatile
    private var catalog: JdbcDucklakeCatalog? = null

    @Volatile
    private var scanPlanProvider: DuckLakeScanPlanProvider? = null

    @Volatile
    private var writePlanProvider: DuckLakeWritePlanProvider? = null

    @Volatile
    private var procedureOps: DuckLakeProcedureOps? = null

    // The Postgres Driver instance THIS connector registered (guarded by `this`, like the catalog).
    // Held so close() can deregister exactly it — never a driver another component registered.
    private var ourPostgresDriver: Driver? = null

    override fun getMetadata(session: ConnectorSession?): ConnectorMetadata =
        DuckLakeConnectorMetadata(
            catalog(),
            properties[DuckLakeConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ].toBoolean(),
        )

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

    override fun getWritePlanProvider(): ConnectorWritePlanProvider {
        var local = writePlanProvider
        if (local == null) {
            synchronized(this) {
                local = writePlanProvider
                if (local == null) {
                    val cat = catalog()
                    val warehouse = DuckLakeConnectorProperties.requireString(
                        properties, DuckLakeConnectorProperties.STORAGE_WAREHOUSE,
                    )
                    local = DuckLakeWritePlanProvider(
                        cat,
                        DuckLakePathResolver(cat, warehouse),
                        properties,
                    )
                    writePlanProvider = local
                }
            }
        }
        return local!!
    }

    /**
     * Table-procedure surface (`ALTER TABLE t EXECUTE <proc>(...)`). Lazily/singleton-built like
     * the scan/write providers so the catalog is only constructed on first real use. Today it
     * exposes `expire_snapshots` — see [DuckLakeProcedureOps] for the catalog-wide-vs-table
     * semantics.
     */
    override fun getProcedureOps(): ConnectorProcedureOps {
        var local = procedureOps
        if (local == null) {
            synchronized(this) {
                local = procedureOps
                if (local == null) {
                    local = DuckLakeProcedureOps(catalog(), properties)
                    procedureOps = local
                }
            }
        }
        return local!!
    }

    /**
     * Capabilities after the P6 iceberg-cutover SPI (doris `branch-catalog-spi`
     * @ 8b391c7, connector-capability-unification): the declarative capability
     * enum was gutted — pushdown/insert/create-table/time-travel/statistics
     * constants were **deleted** because fe-core never consumed them. Pushdown
     * is now attempted unconditionally (`applyFilter`/`applyProjection` on the
     * metadata), INSERT admission comes from
     * `ConnectorWritePlanProvider.supportedOperations()` (default `{INSERT}`),
     * and DDL routes through `ConnectorTableOps`/`ConnectorSchemaOps` directly.
     *
     * `SUPPORTS_MVCC_SNAPSHOT` is the one surviving flag that matters to us —
     * it gates MVCC table creation in `PluginDrivenExternalDatabase` (snapshot
     * pinning + time travel). New opt-ins we deliberately do NOT declare yet:
     * `SUPPORTS_SHOW_CREATE_DDL`, `SUPPORTS_TOPN_LAZY_MATERIALIZE`,
     * `SUPPORTS_NESTED_COLUMN_PRUNE` (needs per-field ids),
     * `SUPPORTS_VIEW`, `SUPPORTS_COLUMN_AUTO_ANALYZE`,
     * `SUPPORTS_METADATA_PRELOAD` — each has behavioral prerequisites tracked
     * in `dev-docs/TODO-read.md`.
     */
    override fun getCapabilities(): Set<ConnectorCapability> =
        EnumSet.of(
            ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
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
        registerPostgresDriver()
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
        deregisterOurPostgresDriver()
    }

    /**
     * Ensure the Postgres [Driver] is registered (loading it on our classloader), and — only when we
     * run under an **isolated plugin classloader** (production) — capture the instance our load
     * registered so [close] can deregister exactly it.
     *
     * `org.postgresql.Driver`'s static initializer self-registers its own instance
     * (`static { register(); }`), so a plain `Class.forName(..., initialize=true)` both loads the
     * class and registers the driver; we don't construct our own (that would create a redundant
     * second registration we'd never fully clean up).
     *
     * ## Why the isolation guard
     * The registered driver's defining classloader is what pins Metaspace. It's ours to clean up
     * ONLY when our classloader is dedicated to this plugin (the real FE: `ConnectorPluginManager`
     * loads the plugin in an isolated child loader). In a **shared** classloader (unit tests on a
     * flat classpath, where `org.postgresql.Driver` is the process-wide ServiceLoader-registered
     * driver on the *same* loader as us), that same instance is used by everyone else — deregistering
     * it on close would break other consumers. And there's no leak to fix there anyway: a shared
     * loader isn't reclaimed on plugin unload. So we only arm cleanup when
     * [runsUnderIsolatedPluginClassLoader] — which is precisely the condition under which the leak
     * exists. Missing driver class ⇒ fail loud (a hard packaging error).
     */
    private fun registerPostgresDriver() {
        if (ourPostgresDriver != null) {
            return // already handled by a prior buildCatalog() on this connector
        }
        try {
            Class.forName(POSTGRES_DRIVER_CLASS, true, javaClass.classLoader)
        } catch (e: ClassNotFoundException) {
            throw IllegalStateException("PostgreSQL JDBC driver missing from plugin classpath", e)
        }
        if (!runsUnderIsolatedPluginClassLoader()) {
            return // shared classloader: the driver is not exclusively ours; nothing to clean up.
        }
        ourPostgresDriver = DriverManager.drivers().toList()
            .firstOrNull { it.javaClass.classLoader === javaClass.classLoader && it.javaClass.name == POSTGRES_DRIVER_CLASS }
    }

    /**
     * Whether our classloader is an isolated plugin loader (production) rather than a shared
     * application/system loader (unit tests). A JDBC driver registered by an isolated plugin loader
     * is exclusively ours to deregister; one registered on a shared loader is not.
     */
    private fun runsUnderIsolatedPluginClassLoader(): Boolean {
        val ourClassLoader = javaClass.classLoader
        return ourClassLoader != null &&
            ourClassLoader !== ClassLoader.getSystemClassLoader() &&
            ourClassLoader !== ClassLoader.getPlatformClassLoader()
    }

    /**
     * Deregister exactly the Postgres [Driver] instance this connector registered (if any), so the
     * static process-lived [DriverManager] no longer strong-references our plugin classloader.
     *
     * Without this, on **plugin reload/unload** fe-core closes the old plugin classloader but the
     * still-registered driver keeps it (and all its classes) pinned → one classloader's worth of
     * Metaspace leaked per reload. (Ordinary CREATE→DROP→CREATE catalog churn does NOT leak — the
     * plugin classloader is built once at FE startup and reused across catalogs, unlike upstream's
     * per-URL `JdbcConnectorClient`; see fix `34bd8eede75` + `dev-docs/TODO-read.md`.)
     * `PluginDrivenExternalCatalog` calls us on catalog `onClose()`/re-init precisely to "release
     * its connection pool and classloader reference".
     *
     * Best-effort: swallow failure (nothing safe to do on a closing connector; throwing would mask
     * the real close reason). Deregistering only OUR held instance means we never disturb a Postgres
     * driver another component registered.
     */
    private fun deregisterOurPostgresDriver() {
        val driver = ourPostgresDriver ?: return
        ourPostgresDriver = null
        try {
            DriverManager.deregisterDriver(driver)
        } catch (e: SQLException) {
            LOG.log(System.Logger.Level.WARNING, "Failed to deregister our PostgreSQL driver on close", e)
        }
    }

    internal fun properties(): Map<String, String> = properties

    internal fun context(): ConnectorContext = context

    private companion object {
        private val LOG: System.Logger = System.getLogger(DuckLakeConnector::class.java.name)
        private const val POSTGRES_DRIVER_CLASS = "org.postgresql.Driver"
    }
}
