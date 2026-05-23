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
import java.util.Optional;

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

    @Override
    public ExecutionContext execute(ExecutionRequest request)
            throws SQLException
    {
        DuckDBConnection connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        Statement statement = null;
        DuckDBResultSet resultSet = null;
        BufferAllocator allocator = null;
        ArrowReader arrowReader = null;
        try {
            try (Statement attachStmt = connection.createStatement()) {
                attachStmt.execute(buildAttachSql(request.target(), attachStmt));
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
        StringBuilder sql = new StringBuilder("SELECT ");
        if (request.isEmptyProjection()) {
            // COUNT(*) and similar collapse to no projected columns. We still need a
            // SELECT clause that yields one row per file row so deletes can be
            // applied by row position downstream — emit a constant, ignored by the
            // converter.
            sql.append("1");
        }
        else {
            var columns = request.projectedColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                String name = columns.get(i).columnName().replace("\"", "\"\"");
                sql.append('"').append(name).append('"');
            }
        }
        sql.append(" FROM ").append(ATTACHED_DB).append(".main.").append(ATTACHED_TABLE);
        Optional<String> whereClause = DuckDbWhereClauseTranslator.toWhereClause(request.pushedPredicate());
        whereClause.ifPresent(w -> sql.append(" WHERE ").append(w));
        return sql.toString();
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
