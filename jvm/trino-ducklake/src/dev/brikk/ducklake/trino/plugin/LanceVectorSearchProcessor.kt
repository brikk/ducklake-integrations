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
 * Executes one [LanceVectorSearchSplit]: runs DuckDB's
 * `lance_vector_search('<dataset>', '<column>', [...]::DOUBLE[], k := ..., prefilter := ...)`
 * through the configured [DucklakeDuckDbExecutor] (the same engine the lance FileScan read path
 * uses) and streams the Arrow result back as Trino pages via [DucklakeArrowToPageConverter].
 */
class LanceVectorSearchProcessorProvider(
        private val executorFactory: DucklakeDuckDbExecutorFactory) : TableFunctionProcessorProvider {

    override fun getSplitProcessor(
            session: ConnectorSession,
            handle: ConnectorTableFunctionHandle,
            split: ConnectorSplit): TableFunctionSplitProcessor =
        LanceVectorSearchSplitProcessor(
                executorFactory.create(),
                handle as LanceVectorSearchFunctionHandle,
                (split as LanceVectorSearchSplit).datasetPath)
}

internal class LanceVectorSearchSplitProcessor(
        private val executor: DucklakeDuckDbExecutor,
        private val handle: LanceVectorSearchFunctionHandle,
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
                    "lance_vector_search failed for dataset $datasetPath: ${e.message}", e)
        }
        catch (e: IOException) {
            throw TrinoException(GENERIC_INTERNAL_ERROR,
                    "lance_vector_search failed for dataset $datasetPath: ${e.message}", e)
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
                        LanceVectorSearchTableFunction.FUNCTION_NAME,
                        DucklakeSessionProperties.FORMAT_LANCE,
                        Optional.empty(),
                        renderExtraArgsSql(handle)),
                handle.outputColumns,
                TupleDomain.all())

    companion object {
        /**
         * The argument tail appended after the quoted dataset path inside the
         * `lance_vector_search(...)` call. The column name is single-quote escaped; the vector
         * elements are finite doubles (validated at analyze time) so `Double.toString` always
         * yields valid SQL literals; `k`/`prefilter` are a long and a boolean.
         */
        internal fun renderExtraArgsSql(handle: LanceVectorSearchFunctionHandle): String {
            val column: String = handle.columnName.replace("'", "''")
            val vector: String = handle.queryVector.joinToString(", ")
            return ", '$column', [$vector]::DOUBLE[], k := ${handle.k}, prefilter := ${handle.prefilter}"
        }
    }
}
