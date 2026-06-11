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
import io.trino.spi.function.table.ScalarArgumentSpecification
import io.trino.spi.function.table.TableFunctionAnalysis
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.VarcharType
import io.trino.spi.type.VarcharType.VARCHAR

/**
 * `SELECT * FROM TABLE(<catalog>.system.lance_fts(...))` — BM25 full-text search over a DuckLake
 * table whose data files are lance dataset directories, executed by the DuckDB `lance`
 * extension's `lance_fts` table function (Route A, Phase A3). Works on unindexed datasets
 * (brute-force; an FTS index, when present in the dataset, accelerates it transparently).
 *
 * <p>Arguments: `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAME` (a VARCHAR column), `QUERY` (the
 * match query), `K` (default 10), `PREFILTER` (default false — see TODO-lance O2; note that
 * prefiltering changes BM25 corpus statistics, so scores differ from post-filtering).
 *
 * <p>Returns only matching rows: the table's columns plus a synthetic `_score` REAL column,
 * descending (higher = better match). The shipped extension treats `k` as best-effort — output
 * may exceed `k` — and each dataset fragment is searched independently, so wrap with
 * `ORDER BY _score DESC LIMIT k` for exact top-k semantics. v1 scope (local paths, all-lance,
 * no deletes) is enforced by [AbstractLanceSearchTableFunction].
 */
class LanceFtsTableFunction @Inject constructor(
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
                ScalarArgumentSpecification.builder().name(ARG_COLUMN_NAME).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_QUERY).type(VARCHAR).build(),
                ScalarArgumentSpecification.builder().name(ARG_K).type(BIGINT).defaultValue(DEFAULT_K).build(),
                ScalarArgumentSpecification.builder().name(ARG_PREFILTER).type(BOOLEAN).defaultValue(false).build())) {

    override fun analyze(
            session: ConnectorSession,
            transaction: ConnectorTransactionHandle,
            arguments: Map<String, Argument>,
            accessControl: ConnectorAccessControl): TableFunctionAnalysis {
        val columnName: String = stringArgument(arguments, ARG_COLUMN_NAME)
        val query: String = stringArgument(arguments, ARG_QUERY)
        val k: Long = kArgument(arguments)
        val prefilter: Boolean = booleanArgument(arguments, ARG_PREFILTER)

        val resolved: ResolvedLanceTable = resolveSearchTable(arguments, accessControl)
        val textColumn: DucklakeColumnHandle = requireColumn(resolved, columnName)
        if (textColumn.columnType !is VarcharType) {
            invalidArgument("Column '${textColumn.columnName}' is not a text (varchar) column: "
                    + textColumn.columnType)
        }

        val outputColumns: List<DucklakeColumnHandle> = resolved.columnHandles + SCORE_COLUMN
        return analysis(outputColumns, LanceFtsFunctionHandle(
                resolved.datasetPaths,
                textColumn.columnName,
                query,
                k,
                prefilter,
                outputColumns))
    }

    companion object {
        const val FUNCTION_NAME: String = "lance_fts"

        private const val ARG_COLUMN_NAME: String = "COLUMN_NAME"
        private const val ARG_QUERY: String = "QUERY"
    }
}

/**
 * Analysis result of `<catalog>.system.lance_fts(...)` — see [LanceSearchHandle] for the shared
 * contract. [outputColumns] = table columns + `_score` REAL.
 */
@JvmRecord
data class LanceFtsFunctionHandle @JsonCreator constructor(
        @param:JsonProperty("datasetPaths") override val datasetPaths: List<String>,
        @get:JvmName("columnName")
        @param:JsonProperty("columnName") val columnName: String,
        @get:JvmName("query")
        @param:JsonProperty("query") val query: String,
        @param:JsonProperty("k") override val k: Long,
        @get:JvmName("prefilter")
        @param:JsonProperty("prefilter") override val prefilter: Boolean,
        @param:JsonProperty("outputColumns") override val outputColumns: List<DucklakeColumnHandle>)
        : io.trino.spi.function.table.ConnectorTableFunctionHandle, LanceSearchHandle
{
    override fun withK(newK: Long): LanceFtsFunctionHandle = copy(k = newK)

    override fun scoreOrderColumn(): String = AbstractLanceSearchTableFunction.SCORE_COLUMN.columnName

    override fun scoreOrderAscending(): Boolean = false
}
