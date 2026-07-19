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

import io.airlift.log.Logger
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.duckdb.DuckDBConnection
import org.duckdb.DuckDBResultSet
import java.io.IOException
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.ConcurrentHashMap

/**
 * Out-of-process T2 [DuckBridgeExecutor] reached via the Quack RPC protocol. Ported from the
 * DuckLake connector's `QuackDuckDbExecutor` and simplified to the base-jdbc seam.
 *
 * The JDBC client is still `duckdb_jdbc` (NOT gizmo's quack-jdbc): the local DuckDB LOADs the `quack`
 * extension, ATTACHes the remote server, and the query is run server-side via `quack_query_by_name`.
 * We use the DuckDB driver here — not quack-jdbc — precisely because only the DuckDB driver exposes
 * `DuckDBResultSet.arrowExportStream`, the Arrow surface this data plane needs.
 *
 * **Known gate (do not fight it):** Quack 1.5.4's fixed server-side connection pool exhausts under
 * per-split churn, so this is a benchmark channel, not the default, until the pool rework lands.
 */
class QuackDuckBridgeExecutor(
    private val host: String,
    private val port: Int,
    private val token: String,
    private val tuning: DuckDbTuning,
    private val parityExtensionPath: String?,
) : DuckBridgeExecutor {
    init {
        require(token.length >= MIN_TOKEN_LENGTH) {
            "Quack auth token must be at least $MIN_TOKEN_LENGTH characters (Quack server-side requirement)"
        }
    }

    @Throws(SQLException::class)
    override fun execute(request: DuckBridgeExecutor.ExecutionRequest): DuckBridgeExecutor.ExecutionContext {
        val connection = DriverManager.getConnection("jdbc:duckdb:") as DuckDBConnection
        var statement: Statement? = null
        var resultSet: DuckDBResultSet? = null
        var allocator: BufferAllocator? = null
        var arrowReader: ArrowReader? = null
        try {
            connection.createStatement().use { init ->
                init.execute("INSTALL quack")
                init.execute("LOAD quack")
                init.execute("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '" + escapeLiteral(token) + "')")
                // disable_ssl=true: quack_serve binds plain HTTP within the pod.
                init.execute("ATTACH 'quack:$host:$port' AS $ENGINE_CATALOG (disable_ssl true)")
                for (tuningSql in DuckDbTuningSql.statements(tuning)) {
                    drainWrappedQuery(init, tuningSql)
                }
                // Server-side LOAD of trino_parity (registers trino_<name> + trino_meta() into the
                // server's shared catalog). Idempotent-converging; retry catalog write-write conflicts.
                parityExtensionPath?.let { path ->
                    DuckDbCatalogWriteRetry.retryConflicts("server-side LOAD trino_parity") {
                        connection.createStatement().use { s ->
                            drainWrappedQuery(s, "LOAD '" + escapeLiteral(path) + "'")
                        }
                    }
                }
                applyServerSideTimeZone(init, request.duckDbTimeZone)
            }
            statement = connection.createStatement()
            resultSet = statement.executeQuery(wrapAsSelect(request.sql)) as DuckDBResultSet
            allocator = RootAllocator()
            arrowReader = resultSet.arrowExportStream(allocator, ARROW_BATCH_SIZE) as ArrowReader
            return QuackExecutionContext(connection, statement, resultSet, allocator, arrowReader)
        } catch (@Suppress("TooGenericExceptionCaught") e: RuntimeException) {
            closeQuietly(arrowReader, resultSet, statement, allocator, connection)
            throw e
        } catch (e: SQLException) {
            closeQuietly(arrowReader, resultSet, statement, allocator, connection)
            throw e
        }
    }

    private class QuackExecutionContext(
        private val connection: DuckDBConnection,
        private val statement: Statement,
        private val resultSet: DuckDBResultSet,
        private val allocator: BufferAllocator,
        private val arrowReader: ArrowReader,
    ) : DuckBridgeExecutor.ExecutionContext {
        override fun arrowReader(): ArrowReader = arrowReader

        override fun memoryUsage(): Long = allocator.allocatedMemory

        @Throws(IOException::class)
        override fun close() {
            closeQuietly(arrowReader, resultSet, statement, connection, allocator)
        }
    }

    companion object {
        private val log: Logger = Logger.get(QuackDuckBridgeExecutor::class.java)

        private const val ARROW_BATCH_SIZE: Long = 1024
        private const val ENGINE_CATALOG: String = "engine"
        private const val MIN_TOKEN_LENGTH: Int = 4

        private fun escapeLiteral(value: String): String = value.replace("'", "''")

        /** Run inner SQL via `quack_query_by_name` and discard rows (DDL-style statements). */
        @Throws(SQLException::class)
        private fun drainWrappedQuery(stmt: Statement, innerSql: String) {
            stmt.executeQuery(wrapAsSelect(innerSql)).use { rs ->
                while (rs.next()) {
                    // discard
                }
            }
        }

        /** Wrap SQL for server-side execution via the `quack_query_by_name` table function. */
        private fun wrapAsSelect(innerSql: String): String =
            "SELECT * FROM system.main.quack_query_by_name('$ENGINE_CATALOG', '${escapeLiteral(innerSql)}')"

        private val timeZoneFailureWarned = ConcurrentHashMap<String, Boolean>()

        private fun applyServerSideTimeZone(stmt: Statement, zone: String?) {
            val z = zone ?: return
            try {
                drainWrappedQuery(stmt, "SET TimeZone = '${escapeLiteral(z)}'")
            } catch (@Suppress("SwallowedException") e: SQLException) {
                if (timeZoneFailureWarned.putIfAbsent(z, true) == null) {
                    log.warn(
                        "duckbridge: Quack server rejected SET TimeZone for zone '%s': %s. " +
                            "Tier A/B pushdown unaffected; Tier C correctness may diverge.",
                        z,
                        e.message?.lineSequence()?.firstOrNull() ?: e.message ?: "(no message)",
                    )
                }
            }
        }

        private fun closeQuietly(vararg resources: AutoCloseable?) {
            for (r in resources) {
                if (r == null) {
                    continue
                }
                try {
                    r.close()
                } catch (@Suppress("SwallowedException", "TooGenericExceptionCaught") ignored: Exception) {
                    // best-effort teardown
                }
            }
        }
    }
}
