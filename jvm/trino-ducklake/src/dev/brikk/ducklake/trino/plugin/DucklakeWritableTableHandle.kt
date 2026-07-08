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
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import io.trino.spi.connector.ConnectorInsertTableHandle
import io.trino.spi.connector.ConnectorOutputTableHandle

import java.util.Optional

@JvmRecord
data class DucklakeWritableTableHandle @JsonCreator constructor(
        @get:JvmName("schemaName")
        @param:JsonProperty("schemaName") val schemaName: String,
        @get:JvmName("tableName")
        @param:JsonProperty("tableName") val tableName: String,
        @get:JvmName("tableId")
        @param:JsonProperty("tableId") val tableId: Long,
        @get:JvmName("columns")
        @param:JsonProperty("columns") val columns: List<DucklakeColumnHandle>,
        @get:JvmName("allCatalogColumns")
        @param:JsonProperty("allCatalogColumns") val allCatalogColumns: List<DucklakeColumn>,
        @get:JvmName("tableDataPath")
        @param:JsonProperty("tableDataPath") val tableDataPath: String,
        @get:JvmName("partitionSpec")
        @param:JsonProperty("partitionSpec") val partitionSpec: Optional<DucklakePartitionSpec>,
        @get:JvmName("temporalPartitionEncoding")
        @param:JsonProperty("temporalPartitionEncoding") val temporalPartitionEncoding: DucklakeTemporalPartitionEncoding,
        @get:JvmName("fileFormat")
        @param:JsonProperty("fileFormat") val fileFormat: String,
        @get:JvmName("duckDbWriterMode")
        @param:JsonProperty("duckDbWriterMode") val duckDbWriterMode: String,
        /**
         * The honored write-side sort prefix (see [DucklakeSortColumn]). Non-empty ONLY for the
         * gated case — parquet format, unpartitioned table, a resolvable DuckLake sort spec — in
         * which the page sink physically sorts rows before writing. Empty (the default) means the
         * sink behaves exactly as before: no buffering, no sorting. Resolved in [DucklakeMetadata]
         * so read and write agree on the honored prefix; defaulted so every existing positional
         * constructor call and JSON payload (which omits it) round-trips to "unsorted".
         */
        @get:JvmName("sortColumns")
        @param:JsonProperty("sortColumns") val sortColumns: List<DucklakeSortColumn> = emptyList())
        : ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    override fun toString(): String
    {
        return "$schemaName.$tableName#$tableId"
    }
}
