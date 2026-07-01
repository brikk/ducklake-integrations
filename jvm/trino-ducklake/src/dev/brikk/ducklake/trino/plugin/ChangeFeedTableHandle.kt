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
import io.trino.spi.connector.ConnectorTableHandle

/**
 * The *scan-shaped* form of a change-feed function: [DucklakeMetadata.applyTableFunction] maps the
 * analyzed [ChangeFeedFunctionHandle] to this [ConnectorTableHandle] so the engine plans an
 * ordinary table scan over the function's output. Unlike the lance searches this carries no
 * pushdown state — the change feed reads a bounded set of files and its predicates stay entirely
 * in the engine's filter above the scan.
 *
 * All state is carried directly (no nested [io.trino.spi.function.table.ConnectorTableFunctionHandle])
 * so there is no Jackson polymorphic-type-id hazard.
 */
@JvmRecord
data class ChangeFeedTableHandle @JsonCreator constructor(
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
        : ConnectorTableHandle, ChangeFeedHandle
