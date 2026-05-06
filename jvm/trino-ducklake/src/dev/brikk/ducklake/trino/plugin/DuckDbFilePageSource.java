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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Reads rows from a single DuckDB-format data file via a per-split DuckDB JDBC
 * connection. The file is reached either as a {@link DuckDbAttachTarget.LocalPath}
 * (materialize cache pulled the {@code .db} to local tmp first) or a
 * {@link DuckDbAttachTarget.HttpfsS3} (the page source loads DuckDB's httpfs
 * extension and ATTACHes the {@code s3://...} URL directly — no local copy). In
 * both cases ATTACH is READ_ONLY into a fresh in-memory DuckDB instance, then
 * results stream out via DuckDB's Arrow C-data export ({@code arrowExportStream})
 * and are converted batch-by-batch by {@link DucklakeArrowToPageConverter}.
 *
 * <p>Predicates flow as the file-stats / dynamic-filter intersection from the split
 * manager plus best-effort {@code WHERE} translation in
 * {@link DuckDbWhereClauseTranslator}. Schema evolution (column rename, new column
 * added after the file was written) is not supported on the duckdb path yet.
 */
final class DuckDbFilePageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(DuckDbFilePageSource.class);

    private static final long ARROW_BATCH_SIZE = 1024;
    private static final String ATTACHED_DB = "ducklake_in";
    private static final String ATTACHED_TABLE = "t";

    private final DuckDbAttachTarget attachTarget;
    private final List<DucklakeColumnHandle> columns;
    private final DucklakeArrowToPageConverter converter;
    private final TupleDomain<DucklakeColumnHandle> effectivePredicate;
    private final boolean emptyProjection;

    private DuckDBConnection connection;
    private Statement statement;
    private DuckDBResultSet resultSet;
    private BufferAllocator allocator;
    private ArrowReader arrowReader;

    private boolean initialized;
    private boolean finished;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    DuckDbFilePageSource(
            DuckDbAttachTarget attachTarget,
            List<DucklakeColumnHandle> columns,
            List<Type> columnTypes,
            TupleDomain<DucklakeColumnHandle> effectivePredicate)
    {
        this.attachTarget = requireNonNull(attachTarget, "attachTarget is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.converter = new DucklakeArrowToPageConverter(columnTypes);
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.emptyProjection = this.columns.isEmpty();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (!initialized) {
                initialize();
                initialized = true;
            }
            if (!arrowReader.loadNextBatch()) {
                finished = true;
                return null;
            }
            Page page;
            if (emptyProjection) {
                // The synthetic SELECT 1 column is ignored; we just need the row count
                // so downstream operators (count/aggregations, delete-file filtering)
                // see the right position count.
                page = new Page(arrowReader.getVectorSchemaRoot().getRowCount());
            }
            else {
                page = converter.convert(arrowReader.getVectorSchemaRoot());
            }
            completedPositions += page.getPositionCount();
            completedBytes += page.getSizeInBytes();
            return SourcePage.create(page);
        }
        catch (IOException | SQLException e) {
            throw new TrinoException(NOT_SUPPORTED, "Failed to read DuckDB file " + describeAttachTarget(), e);
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    private void initialize()
            throws SQLException
    {
        connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        try (Statement stmt = connection.createStatement()) {
            String attachSql = switch (attachTarget) {
                case DuckDbAttachTarget.LocalPath local -> format(
                        "ATTACH '%s' AS %s (READ_ONLY)",
                        local.path().toAbsolutePath().toString().replace("'", "''"),
                        ATTACHED_DB);
                case DuckDbAttachTarget.HttpfsS3 remote -> {
                    // INSTALL is idempotent and cached on disk per-DuckDB-version. LOAD
                    // is required per-instance. CREATE SECRET supplies S3 credentials
                    // for the ATTACH that follows; we use a fixed name so re-creation
                    // on a fresh in-memory instance always succeeds (no DROP needed).
                    stmt.execute("INSTALL httpfs");
                    stmt.execute("LOAD httpfs");
                    stmt.execute(remote.s3Config().renderCreateSecretSql());
                    yield format(
                            "ATTACH '%s' AS %s (READ_ONLY)",
                            remote.s3Url().replace("'", "''"),
                            ATTACHED_DB);
                }
            };
            stmt.execute(attachSql);
        }

        String selectSql = buildSelectSql();
        statement = connection.createStatement();
        resultSet = (DuckDBResultSet) statement.executeQuery(selectSql);
        allocator = new RootAllocator();
        arrowReader = (ArrowReader) resultSet.arrowExportStream(allocator, ARROW_BATCH_SIZE);
    }

    private String describeAttachTarget()
    {
        return switch (attachTarget) {
            case DuckDbAttachTarget.LocalPath local -> local.path().toString();
            case DuckDbAttachTarget.HttpfsS3 remote -> remote.s3Url();
        };
    }

    private String buildSelectSql()
    {
        StringBuilder sql = new StringBuilder("SELECT ");
        if (emptyProjection) {
            // COUNT(*) and similar collapse to no projected columns. We still need a
            // SELECT clause that yields one row per file row (so deletes can be applied
            // by row position downstream), but the value is never read. Emit a constant
            // and skip Arrow conversion for these batches.
            sql.append("1");
        }
        else {
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                String name = columns.get(i).columnName().replace("\"", "\"\"");
                sql.append('"').append(name).append('"');
            }
        }
        sql.append(" FROM ").append(ATTACHED_DB).append(".main.").append(ATTACHED_TABLE);

        // Best-effort predicate pushdown. Anything we can't translate stays in the
        // Trino filter pipeline, so partial pushdown remains correct.
        Optional<String> whereClause = DuckDbWhereClauseTranslator.toWhereClause(effectivePredicate);
        whereClause.ifPresent(w -> sql.append(" WHERE ").append(w));
        return sql.toString();
    }

    @Override
    public long getMemoryUsage()
    {
        return allocator == null ? 0 : allocator.getAllocatedMemory();
    }

    @Override
    public void close()
            throws IOException
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
            log.warn(suppressed, "Error while closing DuckDB page source for %s", describeAttachTarget());
        }
    }
}
