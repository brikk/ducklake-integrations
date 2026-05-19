package dev.brikk.ducklake.doris.plugin.cache;

import org.apache.doris.connector.spi.ConnectorContext;

/**
 * Minimal {@link ConnectorContext} test double — only what our v1 plugin
 * touches. Mirrors the iceberg connector's test harness but trimmed: we
 * don't yet wire cache bindings, credentials, or events, so the defaults
 * on {@code ConnectorContext} suffice for those methods.
 */
public final class FakeConnectorContext implements ConnectorContext {

    private final String catalogName;
    private final long catalogId;

    public FakeConnectorContext(String catalogName, long catalogId) {
        this.catalogName = catalogName;
        this.catalogId = catalogId;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }
}
