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
package dev.brikk.duckbridge.trino.plugin

import com.google.common.collect.ImmutableList
import io.airlift.slice.Slice
import io.trino.plugin.jdbc.JdbcColumnHandle
import io.trino.plugin.jdbc.JdbcMetadata
import io.trino.plugin.jdbc.JdbcTableHandle
import io.trino.plugin.jdbc.JdbcTransactionManager
import io.trino.plugin.jdbc.PreparedQuery
import io.trino.plugin.jdbc.ptf.Query
import io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.function.table.AbstractConnectorTableFunction
import io.trino.spi.function.table.Argument
import io.trino.spi.function.table.ArgumentSpecification
import io.trino.spi.function.table.Descriptor
import io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE
import io.trino.spi.function.table.ScalarArgument
import io.trino.spi.function.table.TableFunctionAnalysis
import io.trino.spi.type.DoubleType.DOUBLE
import java.util.Optional

/**
 * Shared machinery for the DuckBridge scan + lance-search table functions
 * (`lance_scan`, `vortex_scan`, `lance_vector_search`, `lance_fts`, `lance_hybrid_search`) under
 * `<catalog>.system.*`.
 *
 * Ported from the DuckLake connector's `AbstractLanceSearchTableFunction`, but **path-based** rather
 * than catalog-resolved: a duckbridge catalog has no DuckLake table→dataset-directory mapping, so
 * these PTFs take the dataset `PATH` directly. The DuckLake v1-scope guards (all-lance-format,
 * no-deletes, s3-only-on-Quack) resolved a *catalog table*'s files and are dropped — there is no
 * table to resolve; the user names a path.
 *
 * Execution reuses base-jdbc's `query` PTF pattern (the cleanest self-contained seam in the JdbcPlugin
 * frame): [analyzeScanQuery] builds the DuckDB scan/search SQL, wraps it in a [PreparedQuery], and
 * calls [JdbcMetadata.getTableHandle] to resolve the output columns — which runs the query's metadata
 * on a connection that has the lance/vortex extension loaded (see [DuckBridgeScanExtensions] wired into
 * `DuckBridgeClient.getConnection`). The returned [Query.QueryFunctionHandle] then flows through
 * base-jdbc's existing `applyTableFunction` → scan → split → page-source path. No custom
 * ConnectorMetadata / split / processor is needed.
 *
 * Consequence (be truthful): domain predicates over the PTF are NOT pushed into the generated scan
 * SQL (the DuckLake `LanceSearchTableHandle` / `applyFilter` / `applyTopN` machinery that did that
 * needs a custom ConnectorMetadata, which the JdbcPlugin frame doesn't expose for PTFs). A `WHERE` /
 * `ORDER BY ... LIMIT` over the PTF is applied by Trino ABOVE the function. `DuckDbWhereClauseTranslator`
 * was therefore NOT ported (see dev-docs/P5-NOTES.md).
 */
abstract class AbstractDuckBridgeScanFunction(
    private val transactionManager: JdbcTransactionManager,
    private val extension: String,
    functionName: String,
    arguments: List<ArgumentSpecification>,
) : AbstractConnectorTableFunction(SYSTEM_SCHEMA, functionName, arguments, GENERIC_TABLE, "") {
    /**
     * Build the analysis for a scan/search whose DuckDB SQL is [scanSql]. Resolves the output layout
     * by probing the query metadata (which loads/uses the extension on the connection) and returns a
     * [Query.QueryFunctionHandle] so base-jdbc's scan path executes it.
     */
    protected fun analyzeScanQuery(
        session: ConnectorSession,
        transaction: ConnectorTransactionHandle,
        scanSql: String,
    ): TableFunctionAnalysis {
        val preparedQuery = PreparedQuery(scanSql, ImmutableList.of())
        val metadata: JdbcMetadata = transactionManager.getMetadata(transaction)
        val tableHandle: JdbcTableHandle = metadata.getTableHandle(session, preparedQuery)
        val columns: List<JdbcColumnHandle> =
            tableHandle.columns.orElseThrow { IllegalStateException("Scan handle has no column info: $scanSql") }
        val returnedType =
            Descriptor(columns.map { Descriptor.Field(it.columnName, Optional.of(it.columnType)) })
        return TableFunctionAnalysis.builder()
            .returnedType(returnedType)
            .handle(Query.QueryFunctionHandle(tableHandle))
            .build()
    }

    /** The DuckDB extension this PTF needs (for the enabled-check message). */
    protected val requiredExtension: String get() = extension

    companion object {
        const val SYSTEM_SCHEMA: String = "system"

        const val ARG_PATH: String = "PATH"
        const val ARG_COLUMN: String = "COLUMN"
        const val ARG_QUERY_VECTOR: String = "QUERY_VECTOR"
        const val ARG_QUERY: String = "QUERY"
        const val ARG_K: String = "K"
        const val ARG_PREFILTER: String = "PREFILTER"
        const val DEFAULT_K: Long = 10L

        fun invalidArgument(message: String): Nothing = throw TrinoException(INVALID_FUNCTION_ARGUMENT, message)

        private fun scalarValue(arguments: Map<String, Argument>, name: String): Any =
            (arguments[name] as ScalarArgument).value ?: invalidArgument("$name must not be null")

        fun stringArgument(arguments: Map<String, Argument>, name: String): String {
            val value: String = (scalarValue(arguments, name) as Slice).toStringUtf8()
            if (value.isEmpty()) {
                invalidArgument("$name must not be empty")
            }
            return value
        }

        fun booleanArgument(arguments: Map<String, Argument>, name: String): Boolean = scalarValue(arguments, name) as Boolean

        /** Extract `K`, validating positivity. */
        fun kArgument(arguments: Map<String, Argument>): Long {
            val k: Long = scalarValue(arguments, ARG_K) as Long
            if (k <= 0) {
                invalidArgument("$ARG_K must be positive, got $k")
            }
            return k
        }

        /**
         * The `ARRAY(DOUBLE)` constant arrives as a [Block] of double elements. Reject NULL elements
         * and non-finite values — the vector is rendered as a SQL list literal for DuckDB.
         */
        fun queryVectorArgument(arguments: Map<String, Argument>, name: String): List<Double> {
            val block: Block = scalarValue(arguments, name) as Block
            if (block.positionCount == 0) {
                invalidArgument("$name must not be empty")
            }
            val vector = ArrayList<Double>(block.positionCount)
            for (i in 0 until block.positionCount) {
                if (block.isNull(i)) {
                    invalidArgument("$name must not contain NULL elements")
                }
                val value: Double = DOUBLE.getDouble(block, i)
                if (!value.isFinite()) {
                    invalidArgument("$name must contain finite values, got $value")
                }
                vector.add(value)
            }
            return vector
        }
    }
}
