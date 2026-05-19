package dev.brikk.ducklake.doris.plugin;

import java.util.Map;
import java.util.Optional;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorPropertyMetadata;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;

/**
 * {@code ServiceLoader} entry for the DuckLake connector. Registered via
 * {@code META-INF/services/org.apache.doris.connector.spi.ConnectorProvider}.
 */
public final class DuckLakeConnectorProvider implements ConnectorProvider {

    public static final String TYPE = "ducklake";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validateProperties(Map<String, String> properties) {
        // Required-property check only. Doris's DDL layer injects engine-level
        // properties (e.g. `enable.mapping.varbinary`) into every CREATE CATALOG
        // regardless of plugin, so a strict unknown-property check would reject
        // valid usage.
        for (ConnectorPropertyMetadata<?> meta : DuckLakeConnectorProperties.catalogProperties()) {
            if (meta.isRequired() && (properties.get(meta.getName()) == null
                    || properties.get(meta.getName()).isEmpty())) {
                throw new IllegalArgumentException(
                        "DuckLake catalog property '" + meta.getName() + "' is required");
            }
        }
    }

    @Override
    public Connector create(Map<String, String> properties, ConnectorContext context) {
        return new DuckLakeConnector(properties, context);
    }

    /**
     * DuckLake supports MVCC snapshot pinning; the codec round-trips a
     * {@link DuckLakeConnectorMvccSnapshot} via a fixed 24-byte binary frame.
     * The engine uses this codec to ferry the snapshot across FE&rarr;BE.
     */
    @Override
    public Optional<ConnectorMvccSnapshot.Codec> getMvccSnapshotCodec() {
        return Optional.of(new DuckLakeConnectorMvccSnapshot.Codec());
    }
}
