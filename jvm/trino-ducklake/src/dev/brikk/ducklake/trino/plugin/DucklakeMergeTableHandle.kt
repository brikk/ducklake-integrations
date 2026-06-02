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

import java.util.Objects

class DucklakeMergeTableHandle
        : ConnectorMergeTableHandle
{
    private val tableHandle: DucklakeTableHandle
    private val insertHandle: DucklakeWritableTableHandle
    private val dataFileRanges: List<DataFileRange>

    @JsonCreator
    constructor(
            @JsonProperty("tableHandle") tableHandle: DucklakeTableHandle,
            @JsonProperty("insertHandle") insertHandle: DucklakeWritableTableHandle,
            @JsonProperty("dataFileRanges") dataFileRanges: List<DataFileRange>)
    {
        this.tableHandle = tableHandle
        this.insertHandle = insertHandle
        this.dataFileRanges = dataFileRanges.toList()
    }

    @JsonProperty("tableHandle")
    fun tableHandle(): DucklakeTableHandle = tableHandle

    @JsonProperty("insertHandle")
    fun insertHandle(): DucklakeWritableTableHandle = insertHandle

    @JsonProperty("dataFileRanges")
    fun dataFileRanges(): List<DataFileRange> = dataFileRanges

    override fun getTableHandle(): ConnectorTableHandle
    {
        return tableHandle
    }

    override fun equals(other: Any?): Boolean
    {
        if (this === other) return true
        if (other !is DucklakeMergeTableHandle) return false
        return tableHandle == other.tableHandle
                && insertHandle == other.insertHandle
                && dataFileRanges == other.dataFileRanges
    }

    override fun hashCode(): Int
    {
        return Objects.hash(tableHandle, insertHandle, dataFileRanges)
    }

    override fun toString(): String
    {
        return "DucklakeMergeTableHandle[tableHandle=$tableHandle, insertHandle=$insertHandle, dataFileRanges=$dataFileRanges]"
    }

    class DataFileRange
    {
        private val dataFileId: Long
        private val rowIdStart: Long
        private val recordCount: Long
        private val existingDeleteFilePaths: List<String>

        @JsonCreator
        constructor(
                @JsonProperty("dataFileId") dataFileId: Long,
                @JsonProperty("rowIdStart") rowIdStart: Long,
                @JsonProperty("recordCount") recordCount: Long,
                @JsonProperty("existingDeleteFilePaths") existingDeleteFilePaths: List<String>?)
        {
            this.dataFileId = dataFileId
            this.rowIdStart = rowIdStart
            this.recordCount = recordCount
            this.existingDeleteFilePaths = existingDeleteFilePaths?.toList() ?: emptyList()
        }

        @JsonProperty("dataFileId")
        fun dataFileId(): Long = dataFileId

        @JsonProperty("rowIdStart")
        fun rowIdStart(): Long = rowIdStart

        @JsonProperty("recordCount")
        fun recordCount(): Long = recordCount

        @JsonProperty("existingDeleteFilePaths")
        fun existingDeleteFilePaths(): List<String> = existingDeleteFilePaths

        fun containsRowId(rowId: Long): Boolean
        {
            return rowId >= rowIdStart && rowId < rowIdStart + recordCount
        }

        override fun equals(other: Any?): Boolean
        {
            if (this === other) return true
            if (other !is DataFileRange) return false
            return dataFileId == other.dataFileId
                    && rowIdStart == other.rowIdStart
                    && recordCount == other.recordCount
                    && existingDeleteFilePaths == other.existingDeleteFilePaths
        }

        override fun hashCode(): Int
        {
            return Objects.hash(dataFileId, rowIdStart, recordCount, existingDeleteFilePaths)
        }

        override fun toString(): String
        {
            return "DataFileRange[dataFileId=$dataFileId, rowIdStart=$rowIdStart, recordCount=$recordCount, existingDeleteFilePaths=$existingDeleteFilePaths]"
        }
    }
}
