package dev.brikk.ducklake.doris.plugin.cache

import org.apache.doris.connector.spi.ConnectorContext

/**
 * Minimal [ConnectorContext] test double — only what our v1 plugin
 * touches. Mirrors the iceberg connector's test harness but trimmed: we
 * don't yet wire cache bindings, credentials, or events, so the defaults
 * on `ConnectorContext` suffice for those methods.
 */
class FakeConnectorContext(
    private val catalogName: String,
    private val catalogId: Long,
) : ConnectorContext {

    override fun getCatalogName(): String = catalogName

    override fun getCatalogId(): Long = catalogId
}
