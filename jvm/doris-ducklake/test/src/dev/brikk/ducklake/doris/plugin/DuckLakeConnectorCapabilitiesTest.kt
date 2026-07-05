package dev.brikk.ducklake.doris.plugin

import java.util.EnumSet

import dev.brikk.ducklake.doris.plugin.cache.FakeConnectorContext

import org.apache.doris.connector.api.Connector
import org.apache.doris.connector.api.ConnectorCapability
import org.junit.jupiter.api.Test

import org.assertj.core.api.Assertions.assertThat

/**
 * Pins the capability declaration on [DuckLakeConnector] against the P6
 * iceberg-cutover SPI (`branch-catalog-spi` @ 8b391c7), where the
 * capability-unification gutted the declarative enum: pushdown / insert /
 * create-table / time-travel / statistics constants were deleted (fe-core
 * never consumed them — pushdown is attempted unconditionally, INSERT
 * admission is `supportedWriteOperations()`, DDL routes through
 * `ConnectorTableOps`). `SUPPORTS_MVCC_SNAPSHOT` is the surviving flag that
 * matters: it gates MVCC table creation (snapshot pinning + time travel).
 *
 * Adding entries from the new opt-in set (`SUPPORTS_SHOW_CREATE_DDL`,
 * `SUPPORTS_TOPN_LAZY_MATERIALIZE`, `SUPPORTS_NESTED_COLUMN_PRUNE`,
 * `SUPPORTS_VIEW`, `SUPPORTS_COLUMN_AUTO_ANALYZE`,
 * `SUPPORTS_METADATA_PRELOAD`) is a deliberate roadmap step (see
 * `dev-docs/TODO-read.md` + `REPORT-doris-p6-iceberg-spi-cutover.md`) —
 * bump the assertion in the same PR that flips a feature on.
 */
internal class DuckLakeConnectorCapabilitiesTest {

    @Test
    fun declaresOnlyMvccSnapshot() {
        val connector: Connector = DuckLakeConnector(
            emptyMap(),
            FakeConnectorContext("dl", 1L),
        )
        assertThat(connector.capabilities)
            .containsExactlyInAnyOrderElementsOf(
                EnumSet.of(
                    ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT,
                ),
            )
    }

    @Test
    fun newOptInCapabilitiesStayOffUntilTheirPrerequisitesLand() {
        // Each of these has behavioral prerequisites (field-id stamping for
        // nested prune, credential-free properties for SHOW CREATE, ...);
        // declaring one without the matching implementation misplans queries.
        val connector: Connector = DuckLakeConnector(
            emptyMap(),
            FakeConnectorContext("dl", 1L),
        )
        assertThat(connector.capabilities)
            .doesNotContain(
                ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL,
                ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE,
                ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE,
                ConnectorCapability.SUPPORTS_VIEW,
                ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE,
                ConnectorCapability.SUPPORTS_METADATA_PRELOAD,
            )
    }
}
