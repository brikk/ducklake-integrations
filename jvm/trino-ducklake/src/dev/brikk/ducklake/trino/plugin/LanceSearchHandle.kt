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

import io.trino.spi.function.table.ConnectorTableFunctionHandle

/**
 * Common shape of the lance search table-function handles (`lance_vector_search`, `lance_fts`,
 * `lance_hybrid_search`), produced at analyze time by the [AbstractLanceSearchTableFunction]
 * subclasses with everything execution needs fully resolved:
 *
 * - [datasetPaths]: the table's lance dataset *directories* (catalog paths resolved through
 *   [DucklakePathResolver]). [DucklakeSplitManager] emits one [LanceSearchSplit] per entry; each
 *   split computes its own local top-k, so a multi-fragment table yields up to `k * fragments`
 *   rows (a superset of the global top-k — wrap with `ORDER BY <score column> LIMIT k` for exact
 *   global semantics).
 * - [outputColumns]: the produced page layout — every table column followed by the function's
 *   synthetic score column(s), in the exact order of the descriptor returned to the engine.
 *
 * [LanceSearchProcessorProvider] dispatches on the concrete type to render the DuckDB call.
 */
interface LanceSearchHandle : ConnectorTableFunctionHandle {
    val datasetPaths: List<String>
    val outputColumns: List<DucklakeColumnHandle>
}
