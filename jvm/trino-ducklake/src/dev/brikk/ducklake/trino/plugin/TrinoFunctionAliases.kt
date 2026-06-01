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
package dev.brikk.ducklake.trino.plugin

import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement

/**
 * Loader for the {@code trino_parity} DuckDB extension, which registers all the
 * {@code trino_<name>(...)} macros and the {@code trino_meta()} table macro
 * the connector's pushdown translator relies on.
 *
 * <p>Historical note: this class used to read a classpath SQL resource and run
 * {@code CREATE OR REPLACE MACRO} statements per attach as a fallback for
 * environments without the extension binary. That code path was deleted along
 * with the SQL resource — the extension is now a hard dependency. The plugin
 * jar bundles a host-built extension binary by default (see
 * {@link TrinoParityExtensionResolver}); operators can override with the
 * {@code ducklake.duckdb.parity-extension-path} catalog property.
 *
 * <p>The class is two static helpers: one for the in-process JDBC executor
 * (LOAD on a local {@link Connection}) and one for the Quack executor
 * (LOAD over the wrapper, using the same {@link Statement} that runs
 * {@code drainWrappedQuery}). Both throw {@link SQLException} on failure so
 * the executor surfaces a clear error rather than silently degrading.
 */
object TrinoFunctionAliases
{
    /**
     * LOAD the trino_parity extension into the JDBC DuckDB the in-process
     * executor opened. Uses a scratch {@link Statement} so a failure
     * propagates without closing the caller's main statement (DuckDB JDBC
     * auto-closes a Statement on any SQLException). Caller's connection must
     * have been opened with {@code allow_unsigned_extensions=true} as a
     * Properties entry — that setting is immutable once the DB is running.
     */
    @JvmStatic
    @Throws(SQLException::class)
    fun loadInProcess(connection: Connection, path: String)
    {
        connection.createStatement().use { scratch ->
            scratch.execute("LOAD '" + path.replace("'", "''") + "'")
        }
    }

    /**
     * LOAD the trino_parity extension server-side via the Quack wrapper. The
     * caller's {@code init} statement is used to forward the LOAD through
     * {@code QuackDuckDbExecutor.drainWrappedQuery}. The server-side DuckDB
     * must have been started with {@code -unsigned} (the testcontainer
     * entrypoint does this; production Quack deployments need the same).
     */
    @JvmStatic
    @Throws(SQLException::class)
    fun loadServerSide(forwarder: QuackStatementForwarder, path: String)
    {
        forwarder.execute("LOAD '" + path.replace("'", "''") + "'")
    }

    /**
     * Functional handle to {@code QuackDuckDbExecutor.drainWrappedQuery} that
     * lets this helper stay decoupled from QuackDuckDbExecutor.
     */
    fun interface QuackStatementForwarder
    {
        @Throws(SQLException::class)
        fun execute(sql: String)
    }
}
