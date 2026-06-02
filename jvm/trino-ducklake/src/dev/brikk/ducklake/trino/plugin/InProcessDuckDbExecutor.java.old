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
package dev.brikk.ducklake.trino.plugin;

import io.airlift.log.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import static java.lang.String.format;

/**
 * In-process {@link DucklakeDuckDbExecutor}: per-split embedded DuckDB via
 * {@code jdbc:duckdb:}. The {@code .db} file is ATTACHed either as a local
 * path (materialize cache) or as an {@code s3://} URL via the httpfs extension,
 * exactly as the original {@code DuckDbFilePageSource} did before the executor
 * abstraction was introduced. Behaviour-preserving: nothing about query
 * execution changes vs the pre-refactor code.
 */
final class InProcessDuckDbExecutor
        implements DucklakeDuckDbExecutor
{
    private static final Logger log = Logger.get(InProcessDuckDbExecutor.class);

    private static final long ARROW_BATCH_SIZE = 1024;
    private static final String ATTACHED_DB = "ducklake_in";
    private static final String ATTACHED_TABLE = "t";

    private final DuckDbTuning tuning;
    private final String parityExtensionPath;

    /**
     * Test-only convenience constructor: auto-resolves the bundled trino_parity
     * extension binary from the plugin classpath. Throws if no bundled binary
     * is available for the host platform — production code paths the path
     * through the factory which surfaces a CONFIGURATION_INVALID error instead.
     */
    InProcessDuckDbExecutor()
    {
        this(DuckDbTuning.defaults(), TrinoParityExtensionResolver.resolveBundledExtensionPath()
                .orElseThrow(() -> new IllegalStateException(
                        "No bundled trino_parity extension found on classpath. " +
                                "Build the extension first: `(cd duckdb-trino-parity-extension && GEN=ninja make)`, " +
                                "then rebuild the trino-ducklake plugin jar.")));
    }

    InProcessDuckDbExecutor(DuckDbTuning tuning, String parityExtensionPath)
    {
        this.tuning = java.util.Objects.requireNonNull(tuning, "tuning is null");
        this.parityExtensionPath = java.util.Objects.requireNonNull(parityExtensionPath, "parityExtensionPath is null");
    }

    @Override
    public ExecutionContext execute(ExecutionRequest request)
            throws SQLException
    {
        // allow_unsigned_extensions is a database-startup setting; setting it via
        // `SET` after the DB is running fails ("Cannot change while running") AND
        // closes the JDBC Statement on the way out. Pass it as a connection
        // property so the LOAD '<path>' below works.
        Properties connectionProps = new Properties();
        connectionProps.setProperty("allow_unsigned_extensions", "true");
        DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", connectionProps);
        Statement statement = null;
        DuckDBResultSet resultSet = null;
        BufferAllocator allocator = null;
        ArrowReader arrowReader = null;
        try {
            TrinoFunctionAliases.loadInProcess(connection, parityExtensionPath);
            try (Statement attachStmt = connection.createStatement()) {
                DuckDbTuningSql.applyDirect(attachStmt, tuning);
                attachStmt.execute(buildAttachSql(request.target(), attachStmt));
                applySessionTimeZone(attachStmt, request.duckDbTimeZone());
            }
            String selectSql = buildSelectSql(request);
            statement = connection.createStatement();
            resultSet = (DuckDBResultSet) statement.executeQuery(selectSql);
            allocator = new RootAllocator();
            arrowReader = (ArrowReader) resultSet.arrowExportStream(allocator, ARROW_BATCH_SIZE);
            return new InProcessExecutionContext(connection, statement, resultSet, allocator, arrowReader,
                    describeAttachTarget(request.target()));
        }
        catch (SQLException | RuntimeException e) {
            // Surface the most-likely-useful error message; close half-built resources.
            closeQuietly(arrowReader, resultSet, statement, allocator);
            try {
                connection.close();
            }
            catch (SQLException ignored) {
            }
            throw e;
        }
    }

    private static String buildAttachSql(DuckDbAttachTarget target, Statement stmtForSideEffects)
            throws SQLException
    {
        return switch (target) {
            case DuckDbAttachTarget.LocalPath local -> format(
                    "ATTACH '%s' AS %s (READ_ONLY)",
                    local.path().toAbsolutePath().toString().replace("'", "''"),
                    ATTACHED_DB);
            case DuckDbAttachTarget.HttpfsS3 remote -> {
                // INSTALL is idempotent and cached on disk per-DuckDB-version. LOAD
                // is required per-instance. CREATE SECRET supplies S3 credentials
                // for the ATTACH that follows; fixed name means re-creation on a
                // fresh in-memory instance always succeeds.
                stmtForSideEffects.execute("INSTALL httpfs");
                stmtForSideEffects.execute("LOAD httpfs");
                stmtForSideEffects.execute(remote.s3Config().renderCreateSecretSql());
                yield format(
                        "ATTACH '%s' AS %s (READ_ONLY)",
                        remote.s3Url().replace("'", "''"),
                        ATTACHED_DB);
            }
        };
    }

    private static String buildSelectSql(ExecutionRequest request)
    {
        return DuckDbSelectSqlBuilder.buildSelectSql(
                ATTACHED_DB + ".main." + ATTACHED_TABLE, request);
    }

    private static String describeAttachTarget(DuckDbAttachTarget target)
    {
        return switch (target) {
            case DuckDbAttachTarget.LocalPath local -> local.path().toString();
            case DuckDbAttachTarget.HttpfsS3 remote -> remote.s3Url();
        };
    }

    private static void closeQuietly(AutoCloseable... resources)
    {
        for (AutoCloseable r : resources) {
            if (r == null) {
                continue;
            }
            try {
                r.close();
            }
            catch (Exception ignored) {
            }
        }
    }

    private static final java.util.concurrent.ConcurrentHashMap<String, Boolean> TIMEZONE_FAILURE_WARNED =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Best-effort {@code SET TimeZone = '<zone>'} after attach. Empty Optional
     * is a no-op (test harness paths without a session). On SQLException we log
     * a one-shot WARN per (already-normalised) zone string and proceed — the
     * connection is still usable for Tier A/B work which doesn't care about the
     * session zone. Tier C correctness is compromised for this split, but
     * Tier C functions stay off the pushdown surface until chunk 3 anyway,
     * so the only visible effect today is that {@code current_setting('TimeZone')}
     * on the executor side reflects the JVM system default rather than Trino's
     * session zone.
     */
    private static void applySessionTimeZone(Statement stmt, java.util.Optional<String> zone)
    {
        if (zone.isEmpty()) {
            return;
        }
        String z = zone.get();
        // Single-quote escape: defence in depth — the normaliser only emits values
        // from a fixed grammar (IANA zone names, Etc/GMT±N, the original Trino
        // string for fractional offsets), none of which contain quotes, but the
        // string comes from session config and we treat it as untrusted by habit.
        String sql = "SET TimeZone = '" + z.replace("'", "''") + "'";
        try {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            if (TIMEZONE_FAILURE_WARNED.putIfAbsent(z, Boolean.TRUE) == null) {
                log.warn("DuckDB rejected SET TimeZone for normalised zone '%s' (in-process): %s. "
                                + "Subsequent splits with the same zone proceed without an explicit "
                                + "SET; Tier A/B pushdown unaffected, Tier C correctness may diverge. "
                                + "See dev-docs/archive/REPORT-datetime-tz-handling.md.",
                        z, e.getMessage().lines().findFirst().orElse(e.getMessage()));
            }
        }
    }

    private static final class InProcessExecutionContext
            implements ExecutionContext
    {
        private final DuckDBConnection connection;
        private final Statement statement;
        private final DuckDBResultSet resultSet;
        private final BufferAllocator allocator;
        private final ArrowReader arrowReader;
        private final String attachDescription;

        InProcessExecutionContext(
                DuckDBConnection connection,
                Statement statement,
                DuckDBResultSet resultSet,
                BufferAllocator allocator,
                ArrowReader arrowReader,
                String attachDescription)
        {
            this.connection = connection;
            this.statement = statement;
            this.resultSet = resultSet;
            this.allocator = allocator;
            this.arrowReader = arrowReader;
            this.attachDescription = attachDescription;
        }

        @Override
        public ArrowReader arrowReader()
        {
            return arrowReader;
        }

        @Override
        public long memoryUsage()
        {
            return allocator == null ? 0 : allocator.getAllocatedMemory();
        }

        @Override
        public void close() throws IOException
        {
            Throwable suppressed = null;
            try {
                if (arrowReader != null) {
                    arrowReader.close();
                }
            }
            catch (Throwable t) {
                suppressed = t;
            }
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            }
            catch (Throwable t) {
                if (suppressed == null) {
                    suppressed = t;
                }
            }
            try {
                if (statement != null) {
                    statement.close();
                }
            }
            catch (Throwable t) {
                if (suppressed == null) {
                    suppressed = t;
                }
            }
            if (connection != null) {
                try (Statement detach = connection.createStatement()) {
                    detach.execute("DETACH " + ATTACHED_DB);
                }
                catch (Throwable t) {
                    if (suppressed == null) {
                        suppressed = t;
                    }
                }
                try {
                    connection.close();
                }
                catch (Throwable t) {
                    if (suppressed == null) {
                        suppressed = t;
                    }
                }
            }
            try {
                if (allocator != null) {
                    allocator.close();
                }
            }
            catch (Throwable t) {
                if (suppressed == null) {
                    suppressed = t;
                }
            }
            if (suppressed != null) {
                log.warn(suppressed, "Error while closing in-process DuckDB executor for %s", attachDescription);
            }
        }
    }
}
