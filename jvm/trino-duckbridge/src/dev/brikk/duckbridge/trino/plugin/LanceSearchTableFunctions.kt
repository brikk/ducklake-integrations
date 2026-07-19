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

import io.trino.plugin.jdbc.JdbcTransactionManager
import io.trino.spi.connector.ConnectorAccessControl
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.function.table.Argument
import io.trino.spi.function.table.ScalarArgument
import io.trino.spi.function.table.ScalarArgumentSpecification
import io.trino.spi.function.table.TableFunctionAnalysis
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.VarcharType.VARCHAR

/**
 * `system.lance_vector_search(path => ..., column => ..., query_vector => ARRAY[...], k => 10,
 * prefilter => false)` — approximate-nearest-neighbor search over a lance dataset directory, via
 * DuckDB's `lance_vector_search`. Returns the dataset's columns plus `_distance` (ascending).
 *
 * Ported from the DuckLake connector; path-based (no catalog table resolution). The DuckDB function
 * validates that `column` is an embedding column and appends `_distance`, so the output layout comes
 * back from the metadata probe — no connector-side type check needed.
 */
class LanceVectorSearchTableFunction(
    private val scanExtensions: DuckBridgeScanExtensions,
    transactionManager: JdbcTransactionManager,
) : AbstractDuckBridgeScanFunction(
        transactionManager,
        DuckBridgeScanExtensions.LANCE,
        FUNCTION_NAME,
        listOf(
            ScalarArgumentSpecification.builder().name(ARG_PATH).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_COLUMN).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_QUERY_VECTOR).type(ArrayType(DOUBLE)).build(),
            ScalarArgumentSpecification.builder().name(ARG_K).type(BIGINT).defaultValue(DEFAULT_K).build(),
            ScalarArgumentSpecification.builder().name(ARG_PREFILTER).type(BOOLEAN).defaultValue(false).build(),
        ),
    ) {
    override fun analyze(
        session: ConnectorSession,
        transaction: ConnectorTransactionHandle,
        arguments: Map<String, Argument>,
        accessControl: ConnectorAccessControl,
    ): TableFunctionAnalysis {
        scanExtensions.requireEnabled(requiredExtension)
        val path = stringArgument(arguments, ARG_PATH)
        val column = stringArgument(arguments, ARG_COLUMN)
        val queryVector = queryVectorArgument(arguments, ARG_QUERY_VECTOR)
        val k = kArgument(arguments)
        val prefilter = booleanArgument(arguments, ARG_PREFILTER)
        return analyzeScanQuery(
            session,
            transaction,
            LanceSearchSql.lanceVectorSearch(path, column, queryVector, k, prefilter),
        )
    }

    companion object {
        const val FUNCTION_NAME: String = "lance_vector_search"
    }
}

/**
 * `system.lance_fts(path => ..., column => ..., query => 'text', k => 10, prefilter => false)` —
 * BM25 full-text search over a lance dataset directory via DuckDB's `lance_fts`. Returns matching
 * rows plus `_score` (descending). Works on unindexed datasets (brute-force). Ported from DuckLake.
 */
class LanceFtsTableFunction(
    private val scanExtensions: DuckBridgeScanExtensions,
    transactionManager: JdbcTransactionManager,
) : AbstractDuckBridgeScanFunction(
        transactionManager,
        DuckBridgeScanExtensions.LANCE,
        FUNCTION_NAME,
        listOf(
            ScalarArgumentSpecification.builder().name(ARG_PATH).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_COLUMN).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_QUERY).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_K).type(BIGINT).defaultValue(DEFAULT_K).build(),
            ScalarArgumentSpecification.builder().name(ARG_PREFILTER).type(BOOLEAN).defaultValue(false).build(),
        ),
    ) {
    override fun analyze(
        session: ConnectorSession,
        transaction: ConnectorTransactionHandle,
        arguments: Map<String, Argument>,
        accessControl: ConnectorAccessControl,
    ): TableFunctionAnalysis {
        scanExtensions.requireEnabled(requiredExtension)
        val path = stringArgument(arguments, ARG_PATH)
        val column = stringArgument(arguments, ARG_COLUMN)
        val query = stringArgument(arguments, ARG_QUERY)
        val k = kArgument(arguments)
        val prefilter = booleanArgument(arguments, ARG_PREFILTER)
        return analyzeScanQuery(session, transaction, LanceSearchSql.lanceFts(path, column, query, k, prefilter))
    }

    companion object {
        const val FUNCTION_NAME: String = "lance_fts"
    }
}

/**
 * `system.lance_hybrid_search(path => ..., vector_column => ..., query_vector => ARRAY[...],
 * text_column => ..., query => 'text', k => 10, alpha => NULL, prefilter => false)` — combined
 * vector + full-text search via DuckDB's `lance_hybrid_search`. Returns the dataset's columns plus
 * `_distance`, `_score` (NULL for rows with no text match), `_hybrid_score` (descending). Ported
 * from DuckLake. `alpha` (vector-vs-text weight in [0,1]) is optional — NULL lets the extension pick.
 */
class LanceHybridSearchTableFunction(
    private val scanExtensions: DuckBridgeScanExtensions,
    transactionManager: JdbcTransactionManager,
) : AbstractDuckBridgeScanFunction(
        transactionManager,
        DuckBridgeScanExtensions.LANCE,
        FUNCTION_NAME,
        listOf(
            ScalarArgumentSpecification.builder().name(ARG_PATH).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_VECTOR_COLUMN).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_QUERY_VECTOR).type(ArrayType(DOUBLE)).build(),
            ScalarArgumentSpecification.builder().name(ARG_TEXT_COLUMN).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_QUERY).type(VARCHAR).build(),
            ScalarArgumentSpecification.builder().name(ARG_K).type(BIGINT).defaultValue(DEFAULT_K).build(),
            ScalarArgumentSpecification.builder().name(ARG_ALPHA).type(DOUBLE).defaultValue(null).build(),
            ScalarArgumentSpecification.builder().name(ARG_PREFILTER).type(BOOLEAN).defaultValue(false).build(),
        ),
    ) {
    override fun analyze(
        session: ConnectorSession,
        transaction: ConnectorTransactionHandle,
        arguments: Map<String, Argument>,
        accessControl: ConnectorAccessControl,
    ): TableFunctionAnalysis {
        scanExtensions.requireEnabled(requiredExtension)
        val path = stringArgument(arguments, ARG_PATH)
        val vectorColumn = stringArgument(arguments, ARG_VECTOR_COLUMN)
        val queryVector = queryVectorArgument(arguments, ARG_QUERY_VECTOR)
        val textColumn = stringArgument(arguments, ARG_TEXT_COLUMN)
        val query = stringArgument(arguments, ARG_QUERY)
        val k = kArgument(arguments)
        val alpha = alphaArgument(arguments)
        val prefilter = booleanArgument(arguments, ARG_PREFILTER)
        return analyzeScanQuery(
            session,
            transaction,
            LanceSearchSql.lanceHybridSearch(path, vectorColumn, queryVector, textColumn, query, k, alpha, prefilter),
        )
    }

    companion object {
        const val FUNCTION_NAME: String = "lance_hybrid_search"

        private const val ARG_VECTOR_COLUMN: String = "VECTOR_COLUMN"
        private const val ARG_TEXT_COLUMN: String = "TEXT_COLUMN"
        private const val ARG_ALPHA: String = "ALPHA"

        /** `ALPHA` is optional (null = extension default); when given, must be in [0,1]. */
        private fun alphaArgument(arguments: Map<String, Argument>): Double? {
            val alpha: Double = (arguments[ARG_ALPHA] as ScalarArgument).value as Double? ?: return null
            if (!alpha.isFinite() || alpha < 0.0 || alpha > 1.0) {
                invalidArgument("ALPHA must be between 0 and 1, got $alpha")
            }
            return alpha
        }
    }
}
