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

import org.apache.arrow.vector.ipc.ArrowReader
import org.duckdb.DuckDBConnection
import java.io.IOException
import java.sql.SQLException

/**
 * T2 execution-engine strategy for the Arrow data plane. Owns the JDBC / RPC client used to run a
 * fully-rendered SQL query and stream rows back as Arrow.
 *
 * Ported from the DuckLake connector's `DucklakeDuckDbExecutor`, but adapted to the base-jdbc seam:
 * the query SQL is already rendered by base-jdbc's `QueryBuilder`/`PreparedQuery` from the
 * `JdbcTableHandle` (see [DuckBridgeArrowPageSourceProvider]), so this interface takes a plain SQL
 * string rather than DuckLake's `ExecutionRequest` + `DuckDbAttachTarget` + `DuckDbSelectSqlBuilder`
 * (all of which encode DuckLake's file-scan / schema-evolution model and are intentionally NOT
 * ported — see dev-docs/P3-NOTES.md "Dropped from the ported executor surface").
 *
 * Two concrete shapes:
 *  - [InProcessDuckBridgeExecutor]: embedded DuckDB via `jdbc:duckdb:`.
 *  - [QuackDuckBridgeExecutor]: remote DuckDB reached via the `quack` DuckDB extension +
 *    `quack_query_by_name` wrapper (the DuckDB JDBC driver keeps `arrowExportStream`, which
 *    gizmo's quack-jdbc — the T3 transport — does not expose).
 */
interface DuckBridgeExecutor {
    /**
     * Open a connection, apply tuning + parity LOAD + SET TimeZone, and execute [request]'s SQL.
     * Returns a context whose [ArrowReader] streams the result and whose `close()` releases the
     * connection / statement / allocator.
     */
    @Throws(SQLException::class)
    fun execute(request: ExecutionRequest): ExecutionContext

    /**
     * Open a DuckDB connection with tuning + parity + SET TimeZone already applied, WITHOUT running a
     * query. Lets the page source reuse base-jdbc's `JdbcClient.buildSql` (correct projection + bound
     * parameters) on this exact connection, then pull `arrowExportStream` off the resulting
     * `DuckDBResultSet`. Only the in-process (embedded) executor supports this — the Quack executor
     * must wrap SQL in `quack_query_by_name`, which base-jdbc's buildSql can't produce, so it uses
     * [execute] with a pre-rendered SQL string instead.
     *
     * @throws UnsupportedOperationException for executors that cannot expose a plain connection.
     */
    @Throws(SQLException::class)
    fun openPreparedConnection(duckDbTimeZone: String?): DuckDBConnection =
        throw UnsupportedOperationException("This executor does not expose a plain prepared connection")

    /**
     * @param sql          the fully-rendered SELECT (produced by base-jdbc's QueryBuilder from the
     *                     split's JdbcTableHandle; predicate/limit/parity pushdown already inlined).
     * @param duckDbTimeZone optional DuckDB-acceptable zone (Trino session zone normalised via
     *                     [TrinoTimeZoneNormaliser]); applied with `SET TimeZone` before the query.
     *                     Failure is logged once and tolerated (Tier C pushdown degrades).
     */
    @JvmRecord
    data class ExecutionRequest(val sql: String, val duckDbTimeZone: String?)

    /** Per-execution lifecycle handle. The page source iterates the [ArrowReader]; close releases all. */
    interface ExecutionContext : AutoCloseable {
        fun arrowReader(): ArrowReader

        fun memoryUsage(): Long

        @Throws(IOException::class)
        override fun close()
    }
}
