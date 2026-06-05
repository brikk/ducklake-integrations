package dev.brikk.ducklake.doris.plugin;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;

import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the v1 capability declaration on {@link DuckLakeConnector} and the
 * matching codec wiring on {@link DuckLakeConnectorProvider}. Sanity-check §4
 * is the load-bearing reference. Adding or removing entries from the EnumSet
 * is a deliberate roadmap step (see {@code ducklake-doris-todo.md}) — bump
 * the assertion in the same PR that flips a feature on.
 */
class DuckLakeConnectorCapabilitiesTest {

    @Test
    void declaresOnlyV1Capabilities() {
        Connector connector = new DuckLakeConnector(
                Map.of(),
                new FakeConnectorContext("dl", 1L));
        assertThat(connector.getCapabilities())
                .containsExactlyInAnyOrderElementsOf(EnumSet.of(
                        ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
                        ConnectorCapability.SUPPORTS_POSITION_DELETE,
                        ConnectorCapability.SUPPORTS_TIME_TRAVEL,
                        ConnectorCapability.SUPPORTS_PARTITION_PRUNING,
                        ConnectorCapability.SUPPORTS_STATISTICS));
    }

    @Test
    void pushdownCapabilitiesStayOffUntilApplyMethodsLand() {
        // Declaring without implementing crashes the planner — keep these
        // off until Step 6 of the roadmap implements the apply* hooks.
        Connector connector = new DuckLakeConnector(
                Map.of(),
                new FakeConnectorContext("dl", 1L));
        assertThat(connector.getCapabilities())
                .doesNotContain(
                        ConnectorCapability.SUPPORTS_FILTER_PUSHDOWN,
                        ConnectorCapability.SUPPORTS_PROJECTION_PUSHDOWN,
                        ConnectorCapability.SUPPORTS_LIMIT_PUSHDOWN);
    }

    @Test
    void providerSuppliesMvccSnapshotCodec() {
        DuckLakeConnectorProvider provider = new DuckLakeConnectorProvider();
        Optional<ConnectorMvccSnapshot.Codec> codec = provider.getMvccSnapshotCodec();
        assertThat(codec).isPresent();
        assertThat(codec.get()).isInstanceOf(DuckLakeConnectorMvccSnapshot.Codec.class);
    }
}
