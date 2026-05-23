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
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Reads rows from a single DuckDB-format data file via a configurable
 * {@link DucklakeDuckDbExecutor}. The executor owns the JDBC / RPC connection
 * lifecycle, ATTACHes the file (locally for in-process; server-side via
 * {@code quack_query_by_name} for Quack), and returns an Arrow stream which
 * this page source iterates batch-by-batch and converts via
 * {@link DucklakeArrowToPageConverter}.
 *
 * <p>The {@link DuckDbAttachTarget} (local materialized path vs httpfs S3 URL)
 * is resolved upstream by the page source provider's read-mode logic and
 * passed through to the executor — different executors interpret the target
 * differently (in-process opens the local file directly or loads httpfs;
 * Quack forwards the path to the server-side ATTACH).
 *
 * <p>Predicates flow as the file-stats / dynamic-filter intersection from the
 * split manager plus best-effort {@code WHERE} translation in
 * {@link DuckDbWhereClauseTranslator}. Schema evolution (column rename, new
 * column added after the file was written) is not supported on the duckdb
 * path yet.
 */
final class DuckDbFilePageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(DuckDbFilePageSource.class);

    private final DucklakeDuckDbExecutor executor;
    private final DucklakeDuckDbExecutor.ExecutionRequest request;
    private final DucklakeArrowToPageConverter converter;
    private final boolean emptyProjection;

    private DucklakeDuckDbExecutor.ExecutionContext executionContext;
    private ArrowReader arrowReader;

    private boolean initialized;
    private boolean finished;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    DuckDbFilePageSource(
            DucklakeDuckDbExecutor executor,
            DuckDbAttachTarget attachTarget,
            List<DucklakeColumnHandle> columns,
            List<Type> columnTypes,
            TupleDomain<DucklakeColumnHandle> effectivePredicate)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.request = new DucklakeDuckDbExecutor.ExecutionRequest(
                requireNonNull(attachTarget, "attachTarget is null"),
                List.copyOf(requireNonNull(columns, "columns is null")),
                requireNonNull(effectivePredicate, "effectivePredicate is null"));
        this.converter = new DucklakeArrowToPageConverter(columnTypes);
        this.emptyProjection = this.request.isEmptyProjection();
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
                executionContext = executor.execute(request);
                arrowReader = executionContext.arrowReader();
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

    private String describeAttachTarget()
    {
        return switch (request.target()) {
            case DuckDbAttachTarget.LocalPath local -> local.path().toString();
            case DuckDbAttachTarget.HttpfsS3 remote -> remote.s3Url();
        };
    }

    @Override
    public long getMemoryUsage()
    {
        return executionContext == null ? 0 : executionContext.memoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        if (executionContext != null) {
            try {
                executionContext.close();
            }
            catch (IOException e) {
                log.warn(e, "Error while closing DuckDB executor for %s", describeAttachTarget());
                throw e;
            }
        }
    }
}
