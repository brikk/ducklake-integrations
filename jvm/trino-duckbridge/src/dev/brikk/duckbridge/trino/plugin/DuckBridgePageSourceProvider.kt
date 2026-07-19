/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.duckbridge.trino.plugin

import com.google.inject.Inject
import io.trino.plugin.jdbc.ForBaseJdbc
import io.trino.plugin.jdbc.JdbcClient
import io.trino.plugin.jdbc.JdbcColumnHandle
import io.trino.plugin.jdbc.JdbcPageSourceProvider
import io.trino.plugin.jdbc.JdbcSplit
import io.trino.plugin.jdbc.JdbcTableHandle
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.ConnectorTableCredentials
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.type.Type
import org.duckdb.DuckDBConnection
import java.util.Optional

/**
 * G1(c) data-plane seam. Delegates to base-jdbc's [JdbcPageSourceProvider] for the default JDBC
 * engine (production path); for the T2 DUCKDB_LOCAL engine it runs the split's SQL through an
 * in-process DuckDB and streams Arrow batches ([DuckBridgeArrowPageSource]).
 *
 * The SQL + bound parameters are produced by `JdbcClient.buildSql` on the executor's own DuckDB
 * connection, so projection/predicate/limit/parity pushdown are exactly what the default path emits
 * — the T2 path only swaps how rows come back (columnar Arrow vs row-by-row record set).
 *
 * QUACK T2 is intentionally NOT wired here: it needs `quack_query_by_name` SQL-wrapping (which
 * `buildSql` can't produce) AND is gated behind Quack 1.5.4's server-side pool-exhaustion issue.
 * Selecting QUACK is rejected at startup by [DuckBridgeConfig.isExecutionEngineOperational]
 * (fail loud, never a config that lies); the [QuackDuckBridgeExecutor] machinery is ported and
 * unit-tested for when the gate lifts. See dev-docs/P3-NOTES.md.
 */
class DuckBridgePageSourceProvider
    @Inject
    constructor(
        @param:ForBaseJdbc private val jdbcClient: JdbcClient,
        private val config: DuckBridgeConfig,
        private val executorFactory: DuckBridgeExecutorFactory,
        private val delegate: JdbcPageSourceProvider,
    ) : io.trino.spi.connector.ConnectorPageSourceProvider {
        @Suppress("LongParameterList", "OVERRIDE_DEPRECATION")
        override fun createPageSource(
            transaction: ConnectorTransactionHandle,
            session: ConnectorSession,
            split: ConnectorSplit,
            table: ConnectorTableHandle,
            credentials: Optional<ConnectorTableCredentials>,
            columns: List<ColumnHandle>,
            dynamicFilter: DynamicFilter,
        ): ConnectorPageSource {
            if (config.executionEngine != DuckBridgeExecutionEngine.DUCKDB_LOCAL) {
                // QUACK is rejected at config time (DuckBridgeConfig.isExecutionEngineOperational),
                // so the only non-Arrow engine reaching here is the default JDBC path.
                return delegate.createPageSource(transaction, session, split, table, credentials, columns, dynamicFilter)
            }
            return createArrowPageSource(session, split as JdbcSplit, table as JdbcTableHandle, columns)
        }

        private fun createArrowPageSource(
            session: ConnectorSession,
            split: JdbcSplit,
            table: JdbcTableHandle,
            columns: List<ColumnHandle>,
        ): ConnectorPageSource {
            val jdbcColumns = columns.map { it as JdbcColumnHandle }
            val columnTypes: List<Type> = jdbcColumns.map { it.columnType }
            val duckDbZone = TrinoTimeZoneNormaliser.normalise(session.timeZoneKey.id)

            val executor = executorFactory.create() as? InProcessDuckBridgeExecutor
                ?: error("DUCKDB_LOCAL page source requires the in-process executor")
            var connection: DuckDBConnection? = null
            try {
                connection = executor.openPreparedConnection(duckDbZone)
                // Reuse base-jdbc's exact SQL + parameter binding on the executor's connection.
                val statement = jdbcClient.buildSql(session, connection, split, table, jdbcColumns)
                val converter = DuckBridgeArrowToPageConverter(columnTypes)
                return DuckBridgeArrowPageSource(connection, statement, converter, jdbcColumns.size)
            } catch (@Suppress("TooGenericExceptionCaught") t: Throwable) {
                runCatching { connection?.close() }
                throw t
            }
        }


    }
