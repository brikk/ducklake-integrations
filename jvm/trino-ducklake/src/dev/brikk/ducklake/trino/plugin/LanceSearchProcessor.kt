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

import io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.function.table.ConnectorTableFunctionHandle
import io.trino.spi.function.table.TableFunctionProcessorProvider
import io.trino.spi.function.table.TableFunctionProcessorState
import io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED
import io.trino.spi.function.table.TableFunctionProcessorState.Processed
import io.trino.spi.function.table.TableFunctionSplitProcessor
import io.trino.spi.predicate.TupleDomain
import java.io.IOException
import java.sql.SQLException
import java.util.Optional

/**
 * Executes one [LanceSearchSplit] of any lance search table function: runs the matching DuckDB
 * `lance_*` call (`lance_vector_search` / `lance_fts` / `lance_hybrid_search`, dispatched on the
 * concrete [LanceSearchHandle]) through the configured [DucklakeDuckDbExecutor] — the same engine
 * the lance FileScan read path uses — and streams the Arrow result back as Trino pages via
 * [DucklakeArrowToPageConverter].
 */
class LanceSearchProcessorProvider(
        private val executorFactory: DucklakeDuckDbExecutorFactory) : TableFunctionProcessorProvider {

    override fun getSplitProcessor(
            session: ConnectorSession,
            handle: ConnectorTableFunctionHandle,
            split: ConnectorSplit): TableFunctionSplitProcessor =
        LanceSearchSplitProcessor(
                executorFactory.create(),
                handle as LanceSearchHandle,
                (split as LanceSearchSplit).datasetPath)
}

internal class LanceSearchSplitProcessor(
        private val executor: DucklakeDuckDbExecutor,
        private val handle: LanceSearchHandle,
        private val datasetPath: String) : TableFunctionSplitProcessor {
    private val converter = DucklakeArrowToPageConverter(handle.outputColumns.map { it.columnType })
    private var context: DucklakeDuckDbExecutor.ExecutionContext? = null
    private var finished = false

    override fun process(): TableFunctionProcessorState {
        if (finished) {
            return FINISHED
        }
        try {
            val ctx: DucklakeDuckDbExecutor.ExecutionContext = context ?: executor.execute(buildRequest()).also { context = it }
            if (ctx.arrowReader().loadNextBatch()) {
                return Processed.produced(converter.convert(ctx.arrowReader().vectorSchemaRoot))
            }
            finished = true
            return FINISHED
        }
        catch (e: SQLException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR,
                    "${scanFunctionFor(handle)} failed for dataset $datasetPath: ${e.message}", e)
        }
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR,
                    "${scanFunctionFor(handle)} failed for dataset $datasetPath: ${e.message}", e)
        }
    }

    @Throws(IOException::class)
    override fun close() {
        context?.close()
        context = null
    }

    private fun buildRequest(): DucklakeDuckDbExecutor.ExecutionRequest =
        DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.FileScan(
                        datasetPath,
                        scanFunctionFor(handle),
                        DucklakeSessionProperties.FORMAT_LANCE,
                        Optional.empty(),
                        renderExtraArgsSql(handle)),
                handle.outputColumns,
                TupleDomain.all())

    companion object {
        /** The DuckDB table function backing this handle's search. */
        internal fun scanFunctionFor(handle: LanceSearchHandle): String =
            when (handle) {
                is LanceVectorSearchFunctionHandle -> LanceVectorSearchTableFunction.FUNCTION_NAME
                is LanceFtsFunctionHandle -> LanceFtsTableFunction.FUNCTION_NAME
                is LanceHybridSearchFunctionHandle -> LanceHybridSearchTableFunction.FUNCTION_NAME
                else -> throw IllegalArgumentException("Unknown lance search handle: ${handle.javaClass.name}")
            }

        /**
         * The argument tail appended after the quoted dataset path inside the DuckDB call's
         * parentheses. String arguments (column names, FTS queries) are single-quote escaped;
         * vector elements are finite doubles (validated at analyze time) so `Double.toString`
         * always yields valid SQL literals; `k`/`alpha`/`prefilter` are numeric/boolean.
         */
        internal fun renderExtraArgsSql(handle: LanceSearchHandle): String =
            when (handle) {
                is LanceVectorSearchFunctionHandle ->
                    ", ${sqlString(handle.columnName)}, ${sqlVector(handle.queryVector)}" +
                            ", k := ${handle.k}, prefilter := ${handle.prefilter}"
                is LanceFtsFunctionHandle ->
                    ", ${sqlString(handle.columnName)}, ${sqlString(handle.query)}" +
                            ", k := ${handle.k}, prefilter := ${handle.prefilter}"
                is LanceHybridSearchFunctionHandle ->
                    ", ${sqlString(handle.vectorColumn)}, ${sqlVector(handle.queryVector)}" +
                            ", ${sqlString(handle.textColumn)}, ${sqlString(handle.query)}" +
                            ", k := ${handle.k}" +
                            (handle.alpha?.let { ", alpha := $it" } ?: "") +
                            ", prefilter := ${handle.prefilter}"
                else -> throw IllegalArgumentException("Unknown lance search handle: ${handle.javaClass.name}")
            }

        private fun sqlString(value: String): String = "'${value.replace("'", "''")}'"

        private fun sqlVector(vector: List<Double>): String = "[${vector.joinToString(", ")}]::DOUBLE[]"
    }
}
