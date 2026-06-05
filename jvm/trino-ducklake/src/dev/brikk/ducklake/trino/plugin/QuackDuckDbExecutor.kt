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

import dev.brikk.ducklake.trino.plugin.DucklakeDuckDbExecutor.ExecutionContext
import dev.brikk.ducklake.trino.plugin.DucklakeDuckDbExecutor.ExecutionRequest
import io.airlift.log.Logger
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.duckdb.DuckDBConnection
import org.duckdb.DuckDBResultSet
import java.io.IOException
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.HexFormat
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

/**
 * Out-of-process DuckDB executor reached via the Quack RPC protocol. The JDBC
 * client is still {@code duckdb_jdbc}: locally we install + load the quack
 * extension and ATTACH the remote server as a catalog. The {@code .db} file is
 * ATTACHed <em>server-side</em> via {@code quack_query_by_name(engine, …)},
 * because client-side catalog scope only sees the server's default catalog;
 * server-side ATTACHes are sibling catalogs invisible across the RPC seam.
 * Both ATTACH and SELECT therefore go through the wrapper.
 *
 * <p>The same Arrow stream surface as the in-process executor:
 * {@code DuckDBResultSet.arrowExportStream(...)} on the local driver still
 * exposes server-shipped rows as Arrow vectors, because the wrapper SQL is
 * locally one {@code TableFunction} call and the local DuckDB materialises
 * the result through its standard vector path.
 *
 * <p>ATTACH naming: server-side state is shared across all sessions on the
 * Quack server (DuckDB catalogs are instance-scoped). The alias is derived
 * from the absolute path so concurrent clients converge on the same name.
 * {@code IF NOT EXISTS} makes the ATTACH idempotent across both repeat
 * invocations from the same client and competing clients that race to first.
 */
internal class QuackDuckDbExecutor(
        private val host: String,
        private val port: Int,
        private val token: String,
        private val tuning: DuckDbTuning,
        private val parityExtensionPath: String,
) : DucklakeDuckDbExecutor {

    /**
     * Path is the SERVER-SIDE filesystem path inside the Quack DuckDB process,
     * where the trino_parity.duckdb_extension binary has been baked into the
     * container image or mounted via volume. The Quack server must be started
     * with allow_unsigned_extensions enabled (e.g. {@code duckdb -unsigned}
     * in the entrypoint).
     */
    constructor(host: String, port: Int, token: String, parityExtensionPath: String) :
            this(host, port, token, DuckDbTuning.defaults(), parityExtensionPath)

    init {
        if (token.length < 4) {
            throw IllegalArgumentException(
                    "Quack auth token must be at least 4 characters (Quack server-side requirement)")
        }
    }

    @Throws(SQLException::class)
    override fun execute(request: ExecutionRequest): ExecutionContext {
        val connection = DriverManager.getConnection("jdbc:duckdb:") as DuckDBConnection
        var statement: Statement? = null
        var resultSet: DuckDBResultSet? = null
        var allocator: BufferAllocator? = null
        var arrowReader: ArrowReader? = null
        var serverAlias: String?
        try {
            serverAlias = serverAttachAlias(request.target())
            connection.createStatement().use { init ->
                init.execute("INSTALL quack")
                init.execute("LOAD quack")
                init.execute("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '" + escapeLiteral(token) + "')")
                // disable_ssl=true: quack_serve binds plain HTTP; upstream URL parser
                // defaults SSL on for non-localhost hosts. Plain HTTP within the pod.
                init.execute("ATTACH 'quack:$host:$port' AS $ENGINE_CATALOG (disable_ssl true)")
                // Tuning applied server-side via the wrapper — affects the long-lived
                // Quack server's DuckDB, not the ephemeral local client.
                for (tuningSql in DuckDbTuningSql.statements(tuning)) {
                    drainWrappedQuery(init, tuningSql)
                }
                // Server-side LOAD of trino_parity. The extension's LoadInternal
                // registers every trino_<name> and trino_meta() into the server's
                // shared catalog. Required — no SQL fallback. Path must point at
                // a binary the Quack DuckDB process can read; the Quack server
                // must have been started with allow_unsigned_extensions enabled
                // (see TestingDucklakeQuackEngineServer's entrypoint.sh).
                TrinoFunctionAliases.loadServerSide({ sql -> drainWrappedQuery(init, sql) }, parityExtensionPath)
                for (serverInitStatement in serverInitStatementsFor(request.target())) {
                    drainWrappedQuery(init, serverInitStatement)
                }
                val attachServerSide = buildServerSideAttachSql(request.target(), serverAlias!!)
                drainWrappedQuery(init, attachServerSide)
                applyServerSideTimeZone(init, request.duckDbTimeZone())
            }
            val selectSql = wrapAsSelect(buildInnerSelectSql(request, serverAlias!!))
            statement = connection.createStatement()
            resultSet = statement!!.executeQuery(selectSql) as DuckDBResultSet
            allocator = RootAllocator()
            arrowReader = resultSet!!.arrowExportStream(allocator, ARROW_BATCH_SIZE) as ArrowReader
            return QuackExecutionContext(connection, statement!!, resultSet!!, allocator!!, arrowReader!!,
                    describeAttachTarget(request.target()))
        }
        catch (e: SQLException) {
            closeQuietly(arrowReader, resultSet, statement, allocator)
            try {
                connection.close()
            }
            catch (ignored: SQLException) {
            }
            throw e
        }
        catch (e: RuntimeException) {
            closeQuietly(arrowReader, resultSet, statement, allocator)
            try {
                connection.close()
            }
            catch (ignored: SQLException) {
            }
            throw e
        }
    }

    private fun buildServerSideAttachSql(target: DuckDbAttachTarget, serverAlias: String): String = when (target) {
        is DuckDbAttachTarget.LocalPath ->
            "ATTACH IF NOT EXISTS '${target.path.toAbsolutePath().toString().replace("'", "''")}' AS $serverAlias (READ_ONLY)"
        is DuckDbAttachTarget.HttpfsS3 ->
            "ATTACH IF NOT EXISTS '${target.s3Url.replace("'", "''")}' AS $serverAlias (READ_ONLY)"
    }

    /**
     * SQL the Quack server must run BEFORE the ATTACH to make the target
     * reachable: nothing for {@link DuckDbAttachTarget.LocalPath} (the file is
     * accessible via the server's filesystem mount), but for
     * {@link DuckDbAttachTarget.HttpfsS3} we ship the {@code INSTALL httpfs} /
     * {@code LOAD httpfs} / {@code CREATE OR REPLACE SECRET (TYPE S3, ...)}
     * sequence so the server can resolve the {@code s3://} URL itself. Each
     * statement is run via the wrapper.
     */
    private fun serverInitStatementsFor(target: DuckDbAttachTarget): List<String> = when (target) {
        is DuckDbAttachTarget.LocalPath -> emptyList()
        is DuckDbAttachTarget.HttpfsS3 -> listOf(
                "INSTALL httpfs",
                "LOAD httpfs",
                // CREATE OR REPLACE — the secret is server-instance-scoped and
                // shared across concurrent clients on this Quack server. Same
                // credentials from any client (same Trino catalog) → safe race.
                target.s3Config.renderCreateSecretSql())
    }

    private fun buildInnerSelectSql(request: ExecutionRequest, serverAlias: String): String =
        DuckDbSelectSqlBuilder.buildSelectSql(
                "$serverAlias.main.$ATTACHED_TABLE", request)

    private class QuackExecutionContext(
            private val connection: DuckDBConnection,
            private val statement: Statement,
            private val resultSet: DuckDBResultSet,
            private val allocator: BufferAllocator,
            private val arrowReader: ArrowReader,
            private val attachDescription: String,
    ) : ExecutionContext {

        override fun arrowReader(): ArrowReader = arrowReader

        override fun memoryUsage(): Long = allocator.allocatedMemory

        @Throws(IOException::class)
        override fun close() {
            // Close in dependency order, keeping the first failure to log (never rethrow).
            // No client-side DETACH needed — the client-side ATTACH was just the Quack engine
            // catalog itself, lives with the connection. The server-side ATTACH of the .db file
            // persists across this client's lifetime intentionally (next query against the same
            // file is a no-op via IF NOT EXISTS); cache-row invalidation drives explicit DETACH
            // from the cache manager, not from this page source.
            var suppressed: Throwable? = null
            for (resource in listOf(arrowReader, resultSet, statement, connection, allocator)) {
                try {
                    resource.close()
                }
                catch (t: Throwable) {
                    if (suppressed == null) {
                        suppressed = t
                    }
                }
            }
            if (suppressed != null) {
                log.warn(suppressed, "Error while closing Quack DuckDB executor for %s", attachDescription)
            }
        }
    }

    companion object {
        private val log: Logger = Logger.get(QuackDuckDbExecutor::class.java)

        private const val ARROW_BATCH_SIZE: Long = 1024
        private const val ENGINE_CATALOG: String = "engine"
        private const val ATTACHED_TABLE: String = "t"

        /**
         * Execute the given inner SQL via the {@code quack_query_by_name} wrapper
         * and discard the result rows. Used for DDL-style statements (ATTACH,
         * INSTALL, LOAD, CREATE SECRET) that the Quack server still surfaces as
         * a single-row result by its wire-protocol contract.
         */
        @Throws(SQLException::class)
        private fun drainWrappedQuery(stmt: Statement, innerSql: String) {
            stmt.executeQuery(wrapAsSelect(innerSql)).use { rs ->
                while (rs.next()) {
                    // discard
                }
            }
        }

        /**
         * Wrap an arbitrary SQL string for server-side execution via the
         * {@code quack_query_by_name} table function. The FROM-clause form is
         * usable everywhere a result set is consumed (driver-level
         * {@code executeQuery}, {@code arrowExportStream}, etc.) — the {@code CALL}
         * form would also work for fire-and-forget statements but isn't valid
         * inside another query, so the FROM form is consistently used. The first
         * function arg is the local catalog identifier the client uses to address
         * this Quack server; the second is the SQL the server runs against its own
         * default {@code Connection}.
         */
        private fun wrapAsSelect(innerSql: String): String =
            "SELECT * FROM system.main.quack_query_by_name('$ENGINE_CATALOG', '${escapeLiteral(innerSql)}')"

        /**
         * Derive a stable server-side ATTACH alias from the {@code .db} path so
         * concurrent clients agree on the same name and {@code IF NOT EXISTS}
         * makes the ATTACH idempotent. Prefix + truncated SHA-256 keeps the alias
         * SQL-safe and bounded; alphanumeric only because DuckDB unquoted
         * identifiers reject the {@code -} characters base16 produces.
         */
        private fun serverAttachAlias(target: DuckDbAttachTarget): String {
            val key: String = when (target) {
                is DuckDbAttachTarget.LocalPath -> target.path.toAbsolutePath().toString()
                is DuckDbAttachTarget.HttpfsS3 -> target.s3Url
            }
            try {
                val md = MessageDigest.getInstance("SHA-256")
                val digest = md.digest(key.toByteArray(Charsets.UTF_8))
                return "ducklake_cache_" + HexFormat.of().formatHex(digest).substring(0, 16).lowercase(Locale.ROOT)
            }
            catch (e: NoSuchAlgorithmException) {
                throw IllegalStateException("SHA-256 not available", e)
            }
        }

        private fun describeAttachTarget(target: DuckDbAttachTarget): String = when (target) {
            is DuckDbAttachTarget.LocalPath -> target.path.toString()
            is DuckDbAttachTarget.HttpfsS3 -> target.s3Url
        }

        private fun escapeLiteral(value: String): String = value.replace("'", "''")

        private val TIMEZONE_FAILURE_WARNED = ConcurrentHashMap<String, Boolean>()

        /**
         * Best-effort server-side {@code SET TimeZone = '<zone>'} via
         * {@code quack_query_by_name}. The Quack server holds a long-lived DuckDB
         * instance across many client RPCs, but each {@code quack_query_by_name}
         * call lands in the same server-side {@code Connection} for this client
         * session (verified empirically in
         * {@code ProbeDuckDbTimeZoneHandling#probeQ4b_quackContainerParity}), so the
         * setting persists for the subsequent SELECT in the same {@link #execute}
         * call. Failure handling mirrors {@link InProcessDuckDbExecutor}: one-shot
         * WARN per normalised zone, proceed without setting.
         */
        private fun applyServerSideTimeZone(stmt: Statement, zone: java.util.Optional<String>) {
            if (zone.isEmpty) {
                return
            }
            val z = zone.get()
            val innerSql = "SET TimeZone = '${escapeLiteral(z)}'"
            try {
                drainWrappedQuery(stmt, innerSql)
            }
            catch (e: SQLException) {
                if (TIMEZONE_FAILURE_WARNED.putIfAbsent(z, true) == null) {
                    log.warn("Quack server-side DuckDB rejected SET TimeZone for normalised zone '%s': %s. "
                                    + "Subsequent splits with the same zone proceed without an explicit "
                                    + "SET; Tier A/B pushdown unaffected, Tier C correctness may diverge. "
                                    + "See dev-docs/archive/REPORT-datetime-tz-handling.md.",
                            z, firstLineOrFull(e.message ?: "(no message)"))
                }
            }
        }

        private fun firstLineOrFull(message: String): String = message.lineSequence().firstOrNull() ?: message

        private fun closeQuietly(vararg resources: AutoCloseable?) {
            for (r in resources) {
                if (r == null) {
                    continue
                }
                try {
                    r.close()
                }
                catch (ignored: Exception) {
                }
            }
        }
    }
}
