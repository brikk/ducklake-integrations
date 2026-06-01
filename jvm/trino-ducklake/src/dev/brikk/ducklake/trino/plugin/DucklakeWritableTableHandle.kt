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

import java.util.Objects
import java.util.Optional
import java.util.Objects.requireNonNull

class DucklakeWritableTableHandle
        : ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    private val schemaName: String
    private val tableName: String
    private val tableId: Long
    private val columns: List<DucklakeColumnHandle>
    private val allCatalogColumns: List<DucklakeColumn>
    private val tableDataPath: String
    private val partitionSpec: Optional<DucklakePartitionSpec>
    private val temporalPartitionEncoding: DucklakeTemporalPartitionEncoding
    private val fileFormat: String
    private val duckDbWriterMode: String

    @JsonCreator
    constructor(
            @JsonProperty("schemaName") schemaName: String,
            @JsonProperty("tableName") tableName: String,
            @JsonProperty("tableId") tableId: Long,
            @JsonProperty("columns") columns: List<DucklakeColumnHandle>,
            @JsonProperty("allCatalogColumns") allCatalogColumns: List<DucklakeColumn>,
            @JsonProperty("tableDataPath") tableDataPath: String,
            @JsonProperty("partitionSpec") partitionSpec: Optional<DucklakePartitionSpec>,
            @JsonProperty("temporalPartitionEncoding") temporalPartitionEncoding: DucklakeTemporalPartitionEncoding,
            @JsonProperty("fileFormat") fileFormat: String,
            @JsonProperty("duckDbWriterMode") duckDbWriterMode: String?)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null")
        this.tableName = requireNonNull(tableName, "tableName is null")
        this.tableId = tableId
        this.columns = java.util.List.copyOf(requireNonNull(columns, "columns is null"))
        this.allCatalogColumns = java.util.List.copyOf(requireNonNull(allCatalogColumns, "allCatalogColumns is null"))
        this.tableDataPath = requireNonNull(tableDataPath, "tableDataPath is null")
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null")
        this.temporalPartitionEncoding = requireNonNull(temporalPartitionEncoding, "temporalPartitionEncoding is null")
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null")
        this.duckDbWriterMode = duckDbWriterMode ?: DucklakeSessionProperties.WRITER_MODE_ARROW_STREAM
    }

    @JsonProperty("schemaName")
    fun schemaName(): String = schemaName

    @JsonProperty("tableName")
    fun tableName(): String = tableName

    @JsonProperty("tableId")
    fun tableId(): Long = tableId

    @JsonProperty("columns")
    fun columns(): List<DucklakeColumnHandle> = columns

    @JsonProperty("allCatalogColumns")
    fun allCatalogColumns(): List<DucklakeColumn> = allCatalogColumns

    @JsonProperty("tableDataPath")
    fun tableDataPath(): String = tableDataPath

    @JsonProperty("partitionSpec")
    fun partitionSpec(): Optional<DucklakePartitionSpec> = partitionSpec

    @JsonProperty("temporalPartitionEncoding")
    fun temporalPartitionEncoding(): DucklakeTemporalPartitionEncoding = temporalPartitionEncoding

    @JsonProperty("fileFormat")
    fun fileFormat(): String = fileFormat

    @JsonProperty("duckDbWriterMode")
    fun duckDbWriterMode(): String = duckDbWriterMode

    override fun toString(): String
    {
        return schemaName + "." + tableName + "#" + tableId
    }

    override fun equals(other: Any?): Boolean
    {
        if (this === other) return true
        if (other !is DucklakeWritableTableHandle) return false
        return schemaName == other.schemaName
                && tableName == other.tableName
                && tableId == other.tableId
                && columns == other.columns
                && allCatalogColumns == other.allCatalogColumns
                && tableDataPath == other.tableDataPath
                && partitionSpec == other.partitionSpec
                && temporalPartitionEncoding == other.temporalPartitionEncoding
                && fileFormat == other.fileFormat
                && duckDbWriterMode == other.duckDbWriterMode
    }

    override fun hashCode(): Int
    {
        return Objects.hash(
                schemaName,
                tableName,
                tableId,
                columns,
                allCatalogColumns,
                tableDataPath,
                partitionSpec,
                temporalPartitionEncoding,
                fileFormat,
                duckDbWriterMode)
    }
}
