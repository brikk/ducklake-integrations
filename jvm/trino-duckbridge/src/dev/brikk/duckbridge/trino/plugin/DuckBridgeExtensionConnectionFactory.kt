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

import io.trino.plugin.jdbc.ConnectionFactory
import io.trino.spi.connector.ConnectorSession
import java.sql.Connection
import java.sql.SQLException

/**
 * Decorates the base [ConnectionFactory] so that EVERY DuckDB connection the connector opens — the
 * scan/read path AND base-jdbc's own metadata probes (`getTableHandle`, `getColumns`, which call
 * `ConnectionFactory.openConnection` directly, bypassing `DuckBridgeClient.getConnection`) — has the
 * parity + lance/vortex extensions loaded.
 *
 * This is why extension loading lives here rather than only in `DuckBridgeClient.getConnection`: the
 * P5 scan PTFs resolve their output columns via `getTableHandle`, which opens a bare connection and
 * would otherwise see `__lance_scan` / `read_vortex` as unknown functions. Session-specific setup
 * (SET TimeZone) stays in `DuckBridgeClient.getConnection` — metadata probes don't need it.
 */
class DuckBridgeExtensionConnectionFactory(
    private val delegate: ConnectionFactory,
    private val parity: DuckBridgeParity,
    private val scanExtensions: DuckBridgeScanExtensions,
) : ConnectionFactory {
    @Throws(SQLException::class)
    override fun openConnection(session: ConnectorSession): Connection {
        val connection = delegate.openConnection(session)
        try {
            parity.ensureInitialised(connection)
            scanExtensions.ensureLoaded(connection)
        } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
            runCatching { connection.close() }
            throw e
        }
        return connection
    }

    @Throws(Exception::class)
    override fun close() {
        delegate.close()
    }
}
