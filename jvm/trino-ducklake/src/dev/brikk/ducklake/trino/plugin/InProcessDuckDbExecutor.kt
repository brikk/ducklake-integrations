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

import io.airlift.log.Logger
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.duckdb.DuckDBConnection
import org.duckdb.DuckDBResultSet
import java.io.IOException
import java.lang.String.format
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

/**
 * In-process [DucklakeDuckDbExecutor]: per-split embedded DuckDB via
 * `jdbc:duckdb:`. The `.db` file is ATTACHed either as a local
 * path (materialize cache) or as an `s3://` URL via the httpfs extension,
 * exactly as the original `DuckDbFilePageSource` did before the executor
 * abstraction was introduced. Behaviour-preserving: nothing about query
 * execution changes vs the pre-refactor code.
 */
class InProcessDuckDbExecutor
internal constructor(private val tuning: DuckDbTuning, private val parityExtensionPath: String) : DucklakeDuckDbExecutor {

    /**
     * Test-only convenience constructor: auto-resolves the bundled trino_parity
     * extension binary from the plugin classpath. Throws if no bundled binary
     * is available for the host platform — production code paths the path
     * through the factory which surfaces a CONFIGURATION_INVALID error instead.
     */
    internal constructor() : this(DuckDbTuning.defaults(), TrinoParityExtensionResolver.resolveBundledExtensionPath()
            ?: throw IllegalStateException(
                    "No bundled trino_parity extension found on classpath. " +
                            "Build the extension first: `(cd duckdb-trino-parity-extension && GEN=ninja make)`, " +
                            "then rebuild the trino-ducklake plugin jar."))

    @Throws(SQLException::class)
    override fun execute(request: DucklakeDuckDbExecutor.ExecutionRequest): DucklakeDuckDbExecutor.ExecutionContext {
        // allow_unsigned_extensions is a database-startup setting; setting it via
        // `SET` after the DB is running fails ("Cannot change while running") AND
        // closes the JDBC Statement on the way out. Pass it as a connection
        // property so the LOAD '<path>' below works.
        val connectionProps = Properties()
        connectionProps.setProperty("allow_unsigned_extensions", "true")
        val connection: DuckDBConnection = DriverManager.getConnection("jdbc:duckdb:", connectionProps) as DuckDBConnection
        var statement: Statement? = null
        var resultSet: DuckDBResultSet? = null
        var allocator: BufferAllocator? = null
        var arrowReader: ArrowReader? = null
        try {
            TrinoFunctionAliases.loadInProcess(connection, parityExtensionPath)
            val sourceRef: String = connection.createStatement().use { attachStmt ->
                DuckDbTuningSql.applyDirect(attachStmt, tuning)
                val ref: String = prepareSource(request.target(), attachStmt)
                applySessionTimeZone(attachStmt, request.duckDbTimeZone())
                ref
            }
            val selectSql = DuckDbSelectSqlBuilder.buildSelectSql(sourceRef, request)
            statement = connection.createStatement()
            resultSet = statement.executeQuery(selectSql) as DuckDBResultSet
            allocator = RootAllocator()
            arrowReader = resultSet.arrowExportStream(allocator, ARROW_BATCH_SIZE) as ArrowReader
            return InProcessExecutionContext(connection, statement, resultSet, allocator, arrowReader,
                    describeAttachTarget(request.target()))
        }
        catch (e: SQLException) {
            // Surface the most-likely-useful error message; close half-built resources.
            closeQuietly(arrowReader, resultSet, statement, allocator)
            try {
                connection.close()
            }
            catch (ignored: SQLException) {
            }
            throw e
        }
        catch (e: RuntimeException) {
            // Surface the most-likely-useful error message; close half-built resources.
            closeQuietly(arrowReader, resultSet, statement, allocator)
            try {
                connection.close()
            }
            catch (ignored: SQLException) {
            }
            throw e
        }
    }

    private class InProcessExecutionContext(
            private val connection: DuckDBConnection?,
            private val statement: Statement?,
            private val resultSet: DuckDBResultSet?,
            private val allocator: BufferAllocator?,
            private val arrowReader: ArrowReader?,
            private val attachDescription: String) : DucklakeDuckDbExecutor.ExecutionContext {
        override fun arrowReader(): ArrowReader = arrowReader!!

        override fun memoryUsage(): Long =
            allocator?.allocatedMemory ?: 0

        @Throws(IOException::class)
        override fun close() {
            var suppressed: Throwable? = null
            try {
                arrowReader?.close()
            }
            catch (t: Throwable) {
                suppressed = t
            }
            try {
                resultSet?.close()
            }
            catch (t: Throwable) {
                if (suppressed == null) {
                    suppressed = t
                }
            }
            try {
                statement?.close()
            }
            catch (t: Throwable) {
                if (suppressed == null) {
                    suppressed = t
                }
            }
            if (connection != null) {
                // No explicit DETACH: this is a fresh per-split in-memory `jdbc:duckdb:` connection
                // that is never reused, so connection.close() tears down the attached DB anyway.
                // DETACH-on-close is meaningful only for engines whose server-side ATTACH state
                // persists across queries (see DucklakeDuckDbExecutor.ExecutionContext) — the
                // in-process path is not such an engine, so the extra round trip (and its spurious
                // close-time WARN on an already-tearing-down connection) was pure overhead.
                try {
                    connection.close()
                }
                catch (t: Throwable) {
                    if (suppressed == null) {
                        suppressed = t
                    }
                }
            }
            try {
                allocator?.close()
            }
            catch (t: Throwable) {
                if (suppressed == null) {
                    suppressed = t
                }
            }
            if (suppressed != null) {
                log.warn(suppressed, "Error while closing in-process DuckDB executor for %s", attachDescription)
            }
        }
    }

    companion object {
        private val log: Logger = Logger.get(InProcessDuckDbExecutor::class.java)

        private const val ARROW_BATCH_SIZE: Long = 1024
        private const val ATTACHED_DB: String = "ducklake_in"
        private const val ATTACHED_TABLE: String = "t"

        /**
         * Prepare the per-split DuckDB instance for [target] (run ATTACH or INSTALL/LOAD) and
         * return the SQL FROM source — a `db.main.t` reference for attach targets, or a
         * `scanFn('path')` table-function call for a [DuckDbAttachTarget.FileScan]. The caller
         * passes the result to [DuckDbSelectSqlBuilder.buildSelectSql].
         */
        @Throws(SQLException::class)
        private fun prepareSource(target: DuckDbAttachTarget, stmt: Statement): String =
            when (target) {
                is DuckDbAttachTarget.LocalPath -> {
                    stmt.execute(format(
                            "ATTACH '%s' AS %s (READ_ONLY)",
                            target.path.toAbsolutePath().toString().replace("'", "''"),
                            ATTACHED_DB))
                    "$ATTACHED_DB.main.$ATTACHED_TABLE"
                }
                is DuckDbAttachTarget.HttpfsS3 -> {
                    // INSTALL is idempotent and cached on disk per-DuckDB-version. LOAD
                    // is required per-instance. CREATE SECRET supplies S3 credentials
                    // for the ATTACH that follows; fixed name means re-creation on a
                    // fresh in-memory instance always succeeds.
                    stmt.execute("INSTALL httpfs")
                    stmt.execute("LOAD httpfs")
                    stmt.execute(target.s3Config.renderCreateSecretSql())
                    stmt.execute(format(
                            "ATTACH '%s' AS %s (READ_ONLY)",
                            target.s3Url.replace("'", "''"),
                            ATTACHED_DB))
                    "$ATTACHED_DB.main.$ATTACHED_TABLE"
                }
                is DuckDbAttachTarget.FileScan -> {
                    // No ATTACH: read the single file via the extension's table function.
                    // extension/scanFunction are connector-controlled constants (not user input).
                    stmt.execute("INSTALL ${target.extension}")
                    stmt.execute("LOAD ${target.extension}")
                    if (target.s3Config.isPresent) {
                        stmt.execute("INSTALL httpfs")
                        stmt.execute("LOAD httpfs")
                        stmt.execute(target.s3Config.get().renderCreateSecretSql())
                    }
                    "${target.scanFunction}('${target.path.replace("'", "''")}'${target.extraArgsSql})"
                }
            }

        private fun describeAttachTarget(target: DuckDbAttachTarget): String =
            when (target) {
                is DuckDbAttachTarget.LocalPath -> target.path.toString()
                is DuckDbAttachTarget.HttpfsS3 -> target.s3Url
                is DuckDbAttachTarget.FileScan -> target.path
            }

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

        private val TIMEZONE_FAILURE_WARNED: ConcurrentHashMap<String, Boolean> =
                ConcurrentHashMap()

        /**
         * Best-effort `SET TimeZone = '<zone>'` after attach. Empty Optional
         * is a no-op (test harness paths without a session). On SQLException we log
         * a one-shot WARN per (already-normalised) zone string and proceed — the
         * connection is still usable for Tier A/B work which doesn't care about the
         * session zone. Tier C correctness is compromised for this split, but
         * Tier C functions stay off the pushdown surface until chunk 3 anyway,
         * so the only visible effect today is that `current_setting('TimeZone')`
         * on the executor side reflects the JVM system default rather than Trino's
         * session zone.
         */
        private fun applySessionTimeZone(stmt: Statement, zone: java.util.Optional<String>) {
            if (zone.isEmpty) {
                return
            }
            val z = zone.get()
            // Single-quote escape: defence in depth — the normaliser only emits values
            // from a fixed grammar (IANA zone names, Etc/GMT±N, the original Trino
            // string for fractional offsets), none of which contain quotes, but the
            // string comes from session config and we treat it as untrusted by habit.
            val sql = "SET TimeZone = '" + z.replace("'", "''") + "'"
            try {
                stmt.execute(sql)
            }
            catch (e: SQLException) {
                if (TIMEZONE_FAILURE_WARNED.putIfAbsent(z, true) == null) {
                    log.warn("DuckDB rejected SET TimeZone for normalised zone '%s' (in-process): %s. "
                                    + "Subsequent splits with the same zone proceed without an explicit "
                                    + "SET; Tier A/B pushdown unaffected, Tier C correctness may diverge. "
                                    + "See dev-docs/archive/REPORT-datetime-tz-handling.md.",
                            z, e.message?.lineSequence()?.firstOrNull() ?: e.message ?: "(no message)")
                }
            }
        }
    }
}
