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
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.SourcePage
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.Type
import org.apache.arrow.vector.ipc.ArrowReader
import java.io.IOException
import java.sql.SQLException

/**
 * Reads rows from a single DuckDB-format data file via a configurable
 * [DucklakeDuckDbExecutor]. The executor owns the JDBC / RPC connection
 * lifecycle, ATTACHes the file (locally for in-process; server-side via
 * `quack_query_by_name` for Quack), and returns an Arrow stream which
 * this page source iterates batch-by-batch and converts via
 * [DucklakeArrowToPageConverter].
 *
 * The [DuckDbAttachTarget] (local materialized path vs httpfs S3 URL)
 * is resolved upstream by the page source provider's read-mode logic and
 * passed through to the executor — different executors interpret the target
 * differently (in-process opens the local file directly or loads httpfs;
 * Quack forwards the path to the server-side ATTACH).
 *
 * Predicates flow as the file-stats / dynamic-filter intersection from the
 * split manager plus best-effort `WHERE` translation in
 * [DuckDbWhereClauseTranslator]. Schema evolution (column rename, new
 * column added after the file was written) is not supported on the duckdb
 * path yet.
 */
class DuckDbFilePageSource(
        private val executor: DucklakeDuckDbExecutor,
        attachTarget: DuckDbAttachTarget,
        columns: List<DucklakeColumnHandle>,
        columnTypes: List<Type>,
        effectivePredicate: TupleDomain<DucklakeColumnHandle>,
        pushedExpressions: List<String>,
        duckDbTimeZone: String?) : ConnectorPageSource {
    private val request: DucklakeDuckDbExecutor.ExecutionRequest = DucklakeDuckDbExecutor.ExecutionRequest(
            attachTarget,
            columns.toList(),
            effectivePredicate,
            pushedExpressions.toList(),
            duckDbTimeZone)
    private val converter: DucklakeArrowToPageConverter = DucklakeArrowToPageConverter(columnTypes)
    private val emptyProjection: Boolean = this.request.isEmptyProjection()

    private var executionContext: DucklakeDuckDbExecutor.ExecutionContext? = null
    private var arrowReader: ArrowReader? = null

    private var initialized: Boolean = false
    private var finished: Boolean = false
    private var completedPositions: Long = 0
    private var readTimeNanos: Long = 0

    override fun getCompletedBytes(): Long {
        // SPI contract: return input bytes processed; if unavailable, return 0.
        // The DuckDB executor exposes no bytes-read accessor, so we cannot
        // honestly report input bytes here.
        return 0L
    }

    override fun getReadTimeNanos(): Long = readTimeNanos

    override fun isFinished(): Boolean = finished

    override fun getNextSourcePage(): SourcePage? {
        if (finished) return null
        val start = System.nanoTime()
        try {
            if (!initialized) {
                executionContext = executor.execute(request)
                arrowReader = executionContext!!.arrowReader()
                initialized = true
            }
            if (!arrowReader!!.loadNextBatch()) {
                finished = true
                return null
            }
            val page: Page
            if (emptyProjection) {
                // The synthetic SELECT 1 column is ignored; we just need the row count
                // so downstream operators (count/aggregations, delete-file filtering)
                // see the right position count.
                page = Page(arrowReader!!.vectorSchemaRoot.rowCount)
            }
            else {
                page = converter.convert(arrowReader!!.vectorSchemaRoot)
            }
            completedPositions += page.positionCount.toLong()
            return SourcePage.create(page)
        }
        // A read failure here (S3/httpfs error, dropped JDBC connection, DuckDB engine
        // error, corrupt batch) is a genuine IO/runtime fault, not an unsupported feature.
        // NOT_SUPPORTED is a USER_ERROR reserved for true type/feature gaps — which the
        // Arrow→Page converter raises separately as its own NOT_SUPPORTED exceptions — so
        // classify these as GENERIC_INTERNAL_ERROR to avoid misleading operators and
        // defeating error-type-keyed retry classification.
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read DuckDB file ${describeAttachTarget()}", e)
        }
        catch (e: SQLException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read DuckDB file ${describeAttachTarget()}", e)
        }
        finally {
            readTimeNanos += System.nanoTime() - start
        }
    }

    private fun describeAttachTarget(): String = when (val target = request.target()) {
        is DuckDbAttachTarget.LocalPath -> target.path.toString()
        is DuckDbAttachTarget.HttpfsS3 -> target.s3Url
        is DuckDbAttachTarget.FileScan -> target.path
    }

    override fun getMemoryUsage(): Long = executionContext?.memoryUsage() ?: 0L

    @Throws(IOException::class)
    override fun close() {
        if (executionContext != null) {
            try {
                executionContext!!.close()
            }
            catch (e: IOException) {
                log.warn(e, "Error while closing DuckDB executor for %s", describeAttachTarget())
                throw e
            }
        }
    }

    companion object {
        private val log: Logger = Logger.get(DuckDbFilePageSource::class.java)
    }
}
