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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.Locale;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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
final class QuackDuckDbExecutor
        implements DucklakeDuckDbExecutor
{
    private static final Logger log = Logger.get(QuackDuckDbExecutor.class);

    private static final long ARROW_BATCH_SIZE = 1024;
    private static final String ENGINE_CATALOG = "engine";
    private static final String ATTACHED_TABLE = "t";

    private final String host;
    private final int port;
    private final String token;
    private final DuckDbTuning tuning;

    QuackDuckDbExecutor(String host, int port, String token)
    {
        this(host, port, token, DuckDbTuning.defaults());
    }

    QuackDuckDbExecutor(String host, int port, String token, DuckDbTuning tuning)
    {
        this.host = requireNonNull(host, "host is null");
        this.port = port;
        this.token = requireNonNull(token, "token is null");
        this.tuning = requireNonNull(tuning, "tuning is null");
        if (token.length() < 4) {
            throw new IllegalArgumentException(
                    "Quack auth token must be at least 4 characters (Quack server-side requirement)");
        }
    }

    @Override
    public ExecutionContext execute(ExecutionRequest request)
            throws SQLException
    {
        DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        Statement statement = null;
        DuckDBResultSet resultSet = null;
        BufferAllocator allocator = null;
        ArrowReader arrowReader = null;
        String serverAlias = null;
        try {
            serverAlias = serverAttachAlias(request.target());
            try (Statement init = connection.createStatement()) {
                init.execute("INSTALL quack");
                init.execute("LOAD quack");
                init.execute("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '" + escapeLiteral(token) + "')");
                // disable_ssl=true: quack_serve binds plain HTTP; upstream URL parser
                // defaults SSL on for non-localhost hosts. Plain HTTP within the pod.
                init.execute(format(
                        "ATTACH 'quack:%s:%d' AS %s (disable_ssl true)",
                        host, port, ENGINE_CATALOG));
                // Tuning applied server-side via the wrapper — affects the long-lived
                // Quack server's DuckDB, not the ephemeral local client.
                for (String tuningSql : DuckDbTuningSql.statements(tuning)) {
                    drainWrappedQuery(init, tuningSql);
                }
                for (String aliasSql : TrinoFunctionAliases.statements()) {
                    try {
                        drainWrappedQuery(init, aliasSql);
                    }
                    catch (SQLException e) {
                        if (TrinoFunctionAliases.isBestEffort(aliasSql)) {
                            log.warn("trino-function-aliases best-effort statement failed server-side: %s — %s",
                                    aliasSql.lines().findFirst().orElse(aliasSql), e.getMessage());
                            continue;
                        }
                        throw e;
                    }
                }
                for (String serverInitStatement : serverInitStatementsFor(request.target())) {
                    drainWrappedQuery(init, serverInitStatement);
                }
                String attachServerSide = buildServerSideAttachSql(request.target(), serverAlias);
                drainWrappedQuery(init, attachServerSide);
            }
            String selectSql = wrapAsSelect(buildInnerSelectSql(request, serverAlias));
            statement = connection.createStatement();
            resultSet = (DuckDBResultSet) statement.executeQuery(selectSql);
            allocator = new RootAllocator();
            arrowReader = (ArrowReader) resultSet.arrowExportStream(allocator, ARROW_BATCH_SIZE);
            return new QuackExecutionContext(connection, statement, resultSet, allocator, arrowReader,
                    describeAttachTarget(request.target()));
        }
        catch (SQLException | RuntimeException e) {
            closeQuietly(arrowReader, resultSet, statement, allocator);
            try {
                connection.close();
            }
            catch (SQLException ignored) {
            }
            throw e;
        }
    }

    private String buildServerSideAttachSql(DuckDbAttachTarget target, String serverAlias)
    {
        return switch (target) {
            case DuckDbAttachTarget.LocalPath local -> format(
                    "ATTACH IF NOT EXISTS '%s' AS %s (READ_ONLY)",
                    local.path().toAbsolutePath().toString().replace("'", "''"),
                    serverAlias);
            case DuckDbAttachTarget.HttpfsS3 remote -> format(
                    "ATTACH IF NOT EXISTS '%s' AS %s (READ_ONLY)",
                    remote.s3Url().replace("'", "''"),
                    serverAlias);
        };
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
    private java.util.List<String> serverInitStatementsFor(DuckDbAttachTarget target)
    {
        return switch (target) {
            case DuckDbAttachTarget.LocalPath ignored -> java.util.List.of();
            case DuckDbAttachTarget.HttpfsS3 remote -> java.util.List.of(
                    "INSTALL httpfs",
                    "LOAD httpfs",
                    // CREATE OR REPLACE — the secret is server-instance-scoped and
                    // shared across concurrent clients on this Quack server. Same
                    // credentials from any client (same Trino catalog) → safe race.
                    remote.s3Config().renderCreateSecretSql());
        };
    }

    /**
     * Execute the given inner SQL via the {@code quack_query_by_name} wrapper
     * and discard the result rows. Used for DDL-style statements (ATTACH,
     * INSTALL, LOAD, CREATE SECRET) that the Quack server still surfaces as
     * a single-row result by its wire-protocol contract.
     */
    private static void drainWrappedQuery(Statement stmt, String innerSql)
            throws SQLException
    {
        try (var rs = stmt.executeQuery(wrapAsSelect(innerSql))) {
            while (rs.next()) {
                // discard
            }
        }
    }

    private String buildInnerSelectSql(ExecutionRequest request, String serverAlias)
    {
        return DuckDbSelectSqlBuilder.buildSelectSql(
                serverAlias + ".main." + ATTACHED_TABLE, request);
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
    private static String wrapAsSelect(String innerSql)
    {
        return "SELECT * FROM system.main.quack_query_by_name('"
                + ENGINE_CATALOG + "', '"
                + escapeLiteral(innerSql) + "')";
    }

    /**
     * Derive a stable server-side ATTACH alias from the {@code .db} path so
     * concurrent clients agree on the same name and {@code IF NOT EXISTS}
     * makes the ATTACH idempotent. Prefix + truncated SHA-256 keeps the alias
     * SQL-safe and bounded; alphanumeric only because DuckDB unquoted
     * identifiers reject the {@code -} characters base16 produces.
     */
    private static String serverAttachAlias(DuckDbAttachTarget target)
    {
        String key = switch (target) {
            case DuckDbAttachTarget.LocalPath local -> local.path().toAbsolutePath().toString();
            case DuckDbAttachTarget.HttpfsS3 remote -> remote.s3Url();
        };
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return "ducklake_cache_" + HexFormat.of().formatHex(digest).substring(0, 16).toLowerCase(Locale.ROOT);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String describeAttachTarget(DuckDbAttachTarget target)
    {
        return switch (target) {
            case DuckDbAttachTarget.LocalPath local -> local.path().toString();
            case DuckDbAttachTarget.HttpfsS3 remote -> remote.s3Url();
        };
    }

    private static String escapeLiteral(String value)
    {
        return value.replace("'", "''");
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

    private static final class QuackExecutionContext
            implements ExecutionContext
    {
        private final DuckDBConnection connection;
        private final Statement statement;
        private final DuckDBResultSet resultSet;
        private final BufferAllocator allocator;
        private final ArrowReader arrowReader;
        private final String attachDescription;

        QuackExecutionContext(
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
                // No client-side DETACH needed — the client-side ATTACH was just
                // the Quack engine catalog itself, lives with the connection. The
                // server-side ATTACH of the .db file persists across this client's
                // lifetime intentionally (next query against the same file is a
                // no-op via IF NOT EXISTS); cache-row invalidation drives explicit
                // DETACH from the cache manager, not from this page source.
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
                log.warn(suppressed, "Error while closing Quack DuckDB executor for %s", attachDescription);
            }
        }
    }
}
