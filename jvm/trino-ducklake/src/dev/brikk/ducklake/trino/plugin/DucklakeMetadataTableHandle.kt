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
import io.trino.spi.connector.SchemaTableName

@JvmRecord
data class DucklakeMetadataTableHandle @JsonCreator constructor(
        @get:JvmName("schemaName")
        @param:JsonProperty("schemaName") val schemaName: String,
        @get:JvmName("tableName")
        @param:JsonProperty("tableName") val tableName: String,
        @get:JvmName("baseTableName")
        @param:JsonProperty("baseTableName") val baseTableName: String,
        @get:JvmName("baseTableId")
        @param:JsonProperty("baseTableId") val baseTableId: Long,
        @get:JvmName("snapshotId")
        @param:JsonProperty("snapshotId") val snapshotId: Long,
        @get:JvmName("metadataTableType")
        @param:JsonProperty("metadataTableType") val metadataTableType: DucklakeMetadataTableType) : ConnectorTableHandle {
    fun getSchemaTableName(): SchemaTableName {
        return SchemaTableName(schemaName, tableName)
    }
}
