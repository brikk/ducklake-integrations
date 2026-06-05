package dev.brikk.ducklake.doris.plugin

import java.util.EnumSet

import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext

import org.apache.doris.connector.api.Connector
import org.apache.doris.connector.api.ConnectorCapability
import org.junit.jupiter.api.Test

import org.assertj.core.api.Assertions.assertThat

/**
 * Pins the v1 capability declaration on [DuckLakeConnector] and the
 * matching codec wiring on [DuckLakeConnectorProvider]. Sanity-check §4
 * is the load-bearing reference. Adding or removing entries from the EnumSet
 * is a deliberate roadmap step (see `ducklake-doris-todo.md`) — bump
 * the assertion in the same PR that flips a feature on.
 */
internal class DuckLakeConnectorCapabilitiesTest {

    @Test
    fun declaresOnlyV1Capabilities() {
        val connector: Connector = DuckLakeConnector(
            emptyMap(),
            FakeConnectorContext("dl", 1L),
        )
        assertThat(connector.capabilities)
            .containsExactlyInAnyOrderElementsOf(
                EnumSet.of(
                    ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
                    ConnectorCapability.SUPPORTS_POSITION_DELETE,
                    ConnectorCapability.SUPPORTS_TIME_TRAVEL,
                    ConnectorCapability.SUPPORTS_PARTITION_PRUNING,
                    ConnectorCapability.SUPPORTS_STATISTICS,
                ),
            )
    }

    @Test
    fun pushdownCapabilitiesStayOffUntilApplyMethodsLand() {
        // Declaring without implementing crashes the planner — keep these
        // off until Step 6 of the roadmap implements the apply* hooks.
        val connector: Connector = DuckLakeConnector(
            emptyMap(),
            FakeConnectorContext("dl", 1L),
        )
        assertThat(connector.capabilities)
            .doesNotContain(
                ConnectorCapability.SUPPORTS_FILTER_PUSHDOWN,
                ConnectorCapability.SUPPORTS_PROJECTION_PUSHDOWN,
                ConnectorCapability.SUPPORTS_LIMIT_PUSHDOWN,
            )
    }

    @Test
    fun providerSuppliesMvccSnapshotCodec() {
        val provider = DuckLakeConnectorProvider()
        val codec = provider.mvccSnapshotCodec
        assertThat(codec).isPresent
        assertThat(codec.get()).isInstanceOf(DuckLakeConnectorMvccSnapshot.Codec::class.java)
    }
}
