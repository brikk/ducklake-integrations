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
 * Analysis result of `<catalog>.system.lance_vector_search(...)` — see [LanceSearchHandle] for
 * the shared contract. [outputColumns] = table columns + `_distance` REAL.
 */
@JvmRecord
data class LanceVectorSearchFunctionHandle @JsonCreator constructor(
        @param:JsonProperty("datasetPaths") override val datasetPaths: List<String>,
        @get:JvmName("columnName")
        @param:JsonProperty("columnName") val columnName: String,
        @get:JvmName("queryVector")
        @param:JsonProperty("queryVector") val queryVector: List<Double>,
        @param:JsonProperty("k") override val k: Long,
        @get:JvmName("prefilter")
        @param:JsonProperty("prefilter") override val prefilter: Boolean,
        @param:JsonProperty("outputColumns") override val outputColumns: List<DucklakeColumnHandle>)
        : ConnectorTableFunctionHandle, LanceSearchHandle
{
    override fun withK(newK: Long): LanceVectorSearchFunctionHandle = copy(k = newK)

    override fun scoreOrderColumn(): String = AbstractLanceSearchTableFunction.DISTANCE_COLUMN.columnName

    override fun scoreOrderAscending(): Boolean = true
}
