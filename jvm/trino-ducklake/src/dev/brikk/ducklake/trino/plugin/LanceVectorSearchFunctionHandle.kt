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
import io.trino.spi.function.table.ConnectorTableFunctionHandle

/**
 * Analysis result of `<catalog>.system.lance_vector_search(...)` — everything execution needs,
 * fully resolved at analyze time by [LanceVectorSearchTableFunction]:
 *
 * - [datasetPaths]: the table's lance dataset *directories* (catalog paths resolved through
 *   [DucklakePathResolver]). One [LanceVectorSearchSplit] is emitted per entry; each computes its
 *   own local top-[k], so a multi-fragment table yields up to `k * fragments` rows (a superset of
 *   the global top-k — wrap with `ORDER BY _distance LIMIT k` for exact global semantics).
 * - [outputColumns]: the produced page layout — every table column followed by the synthetic
 *   `_distance` REAL column, in the exact order of the descriptor returned to the engine.
 */
@JvmRecord
data class LanceVectorSearchFunctionHandle @JsonCreator constructor(
        @get:JvmName("datasetPaths")
        @param:JsonProperty("datasetPaths") val datasetPaths: List<String>,
        @get:JvmName("columnName")
        @param:JsonProperty("columnName") val columnName: String,
        @get:JvmName("queryVector")
        @param:JsonProperty("queryVector") val queryVector: List<Double>,
        @get:JvmName("k")
        @param:JsonProperty("k") val k: Long,
        @get:JvmName("prefilter")
        @param:JsonProperty("prefilter") val prefilter: Boolean,
        @get:JvmName("outputColumns")
        @param:JsonProperty("outputColumns") val outputColumns: List<DucklakeColumnHandle>)
        : ConnectorTableFunctionHandle
