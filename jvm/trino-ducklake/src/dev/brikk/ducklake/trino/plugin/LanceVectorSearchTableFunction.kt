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

import com.google.inject.Inject
import dev.brikk.ducklake.catalog.DucklakeCatalog
import io.trino.spi.connector.ConnectorAccessControl
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTransactionHandle
import io.trino.spi.function.table.Argument
import io.trino.spi.function.table.ScalarArgumentSpecification
import io.trino.spi.function.table.TableFunctionAnalysis
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.VarcharType.VARCHAR

/**
 * `SELECT * FROM TABLE(<catalog>.system.lance_vector_search(...))` — approximate-nearest-neighbor
 * search over a DuckLake table whose data files are lance dataset directories, executed by the
 * DuckDB `lance` extension's `lance_vector_search` table function (Route A, Phase A3).
 *
 * <p>Arguments: `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAME` (the embedding column),
 * `QUERY_VEC` (an `ARRAY(DOUBLE)` constant — a bare `ARRAY[0.1, 0.2]` literal coerces),
 * `K` (top-k, default 10), `PREFILTER` (default false; forwarded to lance — only meaningful
 * once predicate pushdown into the function lands, see TODO-lance O2).
 *
 * <p>Returns the table's columns plus a synthetic `_distance` REAL column, ascending. Each lance
 * dataset fragment is searched independently (one split per dataset directory), so a
 * multi-fragment table returns up to `k × fragments` rows — a superset of the global top-k; wrap
 * with `ORDER BY _distance LIMIT k` for exact global semantics. v1 scope (local paths, all-lance,
 * no deletes) is enforced by [AbstractLanceSearchTableFunction].
 */
class LanceVectorSearchTableFunction @Inject constructor(
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
                ScalarArgumentSpecification.builder().name(ARG_QUERY_VEC).type(ArrayType(DOUBLE)).build(),
                ScalarArgumentSpecification.builder().name(ARG_K).type(BIGINT).defaultValue(DEFAULT_K).build(),
                ScalarArgumentSpecification.builder().name(ARG_PREFILTER).type(BOOLEAN).defaultValue(false).build())) {

    override fun analyze(
            session: ConnectorSession,
            transaction: ConnectorTransactionHandle,
            arguments: Map<String, Argument>,
            accessControl: ConnectorAccessControl): TableFunctionAnalysis {
        val columnName: String = stringArgument(arguments, ARG_COLUMN_NAME)
        val queryVector: List<Double> = queryVectorArgument(arguments, ARG_QUERY_VEC)
        val k: Long = kArgument(arguments)
        val prefilter: Boolean = booleanArgument(arguments, ARG_PREFILTER)

        val resolved: ResolvedLanceTable = resolveSearchTable(arguments, accessControl)
        val embeddingColumn: DucklakeColumnHandle = requireColumn(resolved, columnName)
        if (embeddingColumn.columnType !is ArrayType) {
            invalidArgument("Column '${embeddingColumn.columnName}' is not an embedding (array) column: "
                    + embeddingColumn.columnType)
        }

        val outputColumns: List<DucklakeColumnHandle> = resolved.columnHandles + DISTANCE_COLUMN
        return analysis(outputColumns, LanceVectorSearchFunctionHandle(
                resolved.datasetPaths,
                embeddingColumn.columnName,
                queryVector,
                k,
                prefilter,
                outputColumns))
    }

    companion object {
        const val FUNCTION_NAME: String = "lance_vector_search"

        private const val ARG_COLUMN_NAME: String = "COLUMN_NAME"
        private const val ARG_QUERY_VEC: String = "QUERY_VEC"
    }
}
