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
 * Common state of an analyzed change-feed table function, shared by the analyze-time
 * [ConnectorTableFunctionHandle] ([ChangeFeedFunctionHandle]) and the rewritten scan
 * [ChangeFeedTableHandle]. All bounds are resolved to concrete snapshot ids at analyze time
 * (timestamp arguments already mapped via `getSnapshotAtOrBefore`), inclusive on both ends.
 *
 * [dataColumns] are the table's columns as of [endSnapshot] (the DuckLake spec reads change-feed
 * rows using the end-snapshot schema); the full output layout adds the prepended metadata columns
 * via [ChangeFeedColumns.outputColumns].
 */
interface ChangeFeedHandle {
    val feedType: ChangeFeedType
    val schemaName: String
    val tableName: String
    val tableId: Long
    val startSnapshot: Long
    val endSnapshot: Long
    val dataColumns: List<DucklakeColumnHandle>

    fun outputColumns(): List<DucklakeColumnHandle> = ChangeFeedColumns.outputColumns(feedType, dataColumns)
}

/**
 * The [ConnectorTableFunctionHandle] produced by a change-feed function's `analyze`. The engine's
 * `RewriteTableFunctionToTableScan` rule turns this into a [ChangeFeedTableHandle] scan
 * ([DucklakeMetadata.applyTableFunction]); the connector never runs it through the table-function
 * *processor* path.
 */
@JvmRecord
data class ChangeFeedFunctionHandle @JsonCreator constructor(
        @get:JvmName("feedType")
        @param:JsonProperty("feedType") override val feedType: ChangeFeedType,
        @get:JvmName("schemaName")
        @param:JsonProperty("schemaName") override val schemaName: String,
        @get:JvmName("tableName")
        @param:JsonProperty("tableName") override val tableName: String,
        @get:JvmName("tableId")
        @param:JsonProperty("tableId") override val tableId: Long,
        @get:JvmName("startSnapshot")
        @param:JsonProperty("startSnapshot") override val startSnapshot: Long,
        @get:JvmName("endSnapshot")
        @param:JsonProperty("endSnapshot") override val endSnapshot: Long,
        @get:JvmName("dataColumns")
        @param:JsonProperty("dataColumns") override val dataColumns: List<DucklakeColumnHandle>)
        : ConnectorTableFunctionHandle, ChangeFeedHandle
