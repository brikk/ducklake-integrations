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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.inject.Inject
import dev.brikk.ducklake.catalog.DucklakeCatalog
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
import io.trino.spi.type.VarcharType
import io.trino.spi.type.VarcharType.VARCHAR

/**
 * `SELECT * FROM TABLE(<catalog>.system.lance_hybrid_search(...))` — combined vector + full-text
 * search over a DuckLake table whose data files are lance dataset directories, executed by the
 * DuckDB `lance` extension's `lance_hybrid_search` table function (Route A, Phase A3).
 *
 * <p>Arguments: `SCHEMA_NAME`, `TABLE_NAME`, `VECTOR_COLUMN` + `QUERY_VEC` (the ANN half),
 * `TEXT_COLUMN` + `QUERY` (the FTS half), `K` (default 10), `ALPHA` (optional vector-vs-text
 * weight in [0,1]; omitted → the extension's default blend), `PREFILTER` (default false).
 *
 * <p>Returns the table's columns plus `_distance` REAL, `_score` REAL (NULL for rows with no
 * text match), and `_hybrid_score` REAL, descending by hybrid score. Fragments are searched
 * independently — wrap with `ORDER BY _hybrid_score DESC LIMIT k` for exact global top-k. v1
 * scope (local paths, all-lance, no deletes) is enforced by [AbstractLanceSearchTableFunction].
 */
class LanceHybridSearchTableFunction @Inject constructor(
        catalog: DucklakeCatalog,
        typeConverter: DucklakeTypeConverter,
        pathResolver: DucklakePathResolver,
) : AbstractLanceSearchTableFunction(
        catalog,
        typeConverter,
        pathResolver,
        FUNCTION_NAME,
        listOf(
                ScalarArgumentSpecification.builder().name(ARG_SCHEMA_NAME).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_TABLE_NAME).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_VECTOR_COLUMN).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_QUERY_VEC).type(ArrayType(DOUBLE)).build(),
                ScalarArgumentSpecification.builder().name(ARG_TEXT_COLUMN).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_QUERY).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_K).type(BIGINT).defaultValue(DEFAULT_K).build(),
                ScalarArgumentSpecification.builder().name(ARG_ALPHA).type(DOUBLE).defaultValue(null).build(),
                ScalarArgumentSpecification.builder().name(ARG_PREFILTER).type(BOOLEAN).defaultValue(false).build())) {

    override fun analyze(
            session: ConnectorSession,
            transaction: ConnectorTransactionHandle,
            arguments: Map<String, Argument>,
            accessControl: ConnectorAccessControl): TableFunctionAnalysis {
        val vectorColumnName: String = stringArgument(arguments, ARG_VECTOR_COLUMN)
        val queryVector: List<Double> = queryVectorArgument(arguments, ARG_QUERY_VEC)
        val textColumnName: String = stringArgument(arguments, ARG_TEXT_COLUMN)
        val query: String = stringArgument(arguments, ARG_QUERY)
        val k: Long = kArgument(arguments)
        val alpha: Double? = alphaArgument(arguments)
        val prefilter: Boolean = booleanArgument(arguments, ARG_PREFILTER)

        val resolved: ResolvedLanceTable = resolveSearchTable(arguments, accessControl)
        val vectorColumn: DucklakeColumnHandle = requireColumn(resolved, vectorColumnName)
        if (vectorColumn.columnType !is ArrayType) {
            invalidArgument("Column '${vectorColumn.columnName}' is not an embedding (array) column: "
                    + vectorColumn.columnType)
        }
        val textColumn: DucklakeColumnHandle = requireColumn(resolved, textColumnName)
        if (textColumn.columnType !is VarcharType) {
            invalidArgument("Column '${textColumn.columnName}' is not a text (varchar) column: "
                    + textColumn.columnType)
        }

        val outputColumns: List<DucklakeColumnHandle> =
            resolved.columnHandles + DISTANCE_COLUMN + SCORE_COLUMN + HYBRID_SCORE_COLUMN
        return analysis(outputColumns, LanceHybridSearchFunctionHandle(
                resolved.datasetPaths,
                vectorColumn.columnName,
                queryVector,
                textColumn.columnName,
                query,
                k,
                alpha,
                prefilter,
                outputColumns))
    }

    companion object {
        const val FUNCTION_NAME: String = "lance_hybrid_search"

        private const val ARG_VECTOR_COLUMN: String = "VECTOR_COLUMN"
        private const val ARG_QUERY_VEC: String = "QUERY_VEC"
        private const val ARG_TEXT_COLUMN: String = "TEXT_COLUMN"
        private const val ARG_QUERY: String = "QUERY"
        private const val ARG_ALPHA: String = "ALPHA"

        /** `ALPHA` is optional (null default = let the extension pick); when given, must be in [0,1]. */
        private fun alphaArgument(arguments: Map<String, Argument>): Double? {
            val alpha: Double = (arguments[ARG_ALPHA] as ScalarArgument).value as Double? ?: return null
            if (!alpha.isFinite() || alpha < 0.0 || alpha > 1.0) {
                invalidArgument("$ARG_ALPHA must be between 0 and 1, got $alpha")
            }
            return alpha
        }
    }
}

/**
 * Analysis result of `<catalog>.system.lance_hybrid_search(...)` — see [LanceSearchHandle] for
 * the shared contract. [outputColumns] = table columns + `_distance`, `_score`, `_hybrid_score`
 * (all REAL; `_score` NULL for rows with no text match). [alpha] null means "extension default".
 */
@JvmRecord
data class LanceHybridSearchFunctionHandle @JsonCreator constructor(
        @param:JsonProperty("datasetPaths") override val datasetPaths: List<String>,
        @get:JvmName("vectorColumn")
        @param:JsonProperty("vectorColumn") val vectorColumn: String,
        @get:JvmName("queryVector")
        @param:JsonProperty("queryVector") val queryVector: List<Double>,
        @get:JvmName("textColumn")
        @param:JsonProperty("textColumn") val textColumn: String,
        @get:JvmName("query")
        @param:JsonProperty("query") val query: String,
        @param:JsonProperty("k") override val k: Long,
        @get:JvmName("alpha")
        @param:JsonProperty("alpha") val alpha: Double?,
        @get:JvmName("prefilter")
        @param:JsonProperty("prefilter") override val prefilter: Boolean,
        @param:JsonProperty("outputColumns") override val outputColumns: List<DucklakeColumnHandle>)
        : io.trino.spi.function.table.ConnectorTableFunctionHandle, LanceSearchHandle
{
    override fun withK(newK: Long): LanceHybridSearchFunctionHandle = copy(k = newK)

    override fun scoreOrderColumn(): String = AbstractLanceSearchTableFunction.HYBRID_SCORE_COLUMN.columnName

    override fun scoreOrderAscending(): Boolean = false
}
