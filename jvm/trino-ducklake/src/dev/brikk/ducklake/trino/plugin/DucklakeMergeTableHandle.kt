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
import io.trino.spi.connector.ConnectorMergeTableHandle
import io.trino.spi.connector.ConnectorTableHandle

@JvmRecord
data class DucklakeMergeTableHandle @JsonCreator constructor(
        @get:JvmName("tableHandle")
        @param:JsonProperty("tableHandle") val tableHandle: DucklakeTableHandle,
        @get:JvmName("insertHandle")
        @param:JsonProperty("insertHandle") val insertHandle: DucklakeWritableTableHandle,
        @get:JvmName("dataFileRanges")
        @param:JsonProperty("dataFileRanges") val dataFileRanges: List<DataFileRange>)
        : ConnectorMergeTableHandle
{
    override fun getTableHandle(): ConnectorTableHandle = tableHandle

    @JvmRecord
    data class DataFileRange @JsonCreator constructor(
            @get:JvmName("dataFileId")
            @param:JsonProperty("dataFileId") val dataFileId: Long,
            @get:JvmName("rowIdStart")
            @param:JsonProperty("rowIdStart") val rowIdStart: Long,
            @get:JvmName("recordCount")
            @param:JsonProperty("recordCount") val recordCount: Long,
            @get:JvmName("existingDeleteFilePaths")
            @param:JsonProperty("existingDeleteFilePaths") val existingDeleteFilePaths: List<String>,
            // Resolved data file path, written verbatim into the `file_path` column of the
            // spec-shaped delete files this merge produces (what upstream's writer records).
            @get:JvmName("dataFilePath")
            @param:JsonProperty("dataFilePath") val dataFilePath: String)
    {
        fun containsRowId(rowId: Long): Boolean = rowId >= rowIdStart && rowId < rowIdStart + recordCount
    }
}
