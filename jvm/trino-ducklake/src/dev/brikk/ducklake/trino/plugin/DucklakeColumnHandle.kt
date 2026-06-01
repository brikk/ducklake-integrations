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
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.type.Type

import io.airlift.slice.SizeOf.estimatedSizeOf
import io.airlift.slice.SizeOf.instanceSize
import io.airlift.slice.SizeOf.sizeOf
import io.trino.spi.type.BigintType.BIGINT
import java.util.Objects
import java.util.Objects.requireNonNull

/**
 * Handle for a Ducklake column.
 */
class DucklakeColumnHandle
        : ColumnHandle
{
    private val columnId: Long
    private val columnName: String
    private val columnType: Type
    private val nullable: Boolean

    @JsonCreator
    constructor(
            @JsonProperty("columnId") columnId: Long,
            @JsonProperty("columnName") columnName: String,
            @JsonProperty("columnType") columnType: Type,
            @JsonProperty("nullable") nullable: Boolean)
    {
        this.columnId = columnId
        this.columnName = requireNonNull(columnName, "columnName is null")
        this.columnType = requireNonNull(columnType, "columnType is null")
        this.nullable = nullable
    }

    @JsonProperty("columnId")
    fun columnId(): Long = columnId

    @JsonProperty("columnName")
    fun columnName(): String = columnName

    @JsonProperty("columnType")
    fun columnType(): Type = columnType

    @JsonProperty("nullable")
    fun nullable(): Boolean = nullable

    fun isRowIdColumn(): Boolean
    {
        return columnId == ROW_ID_COLUMN_ID
    }

    override fun toString(): String
    {
        return columnName + ":" + columnType
    }

    fun getRetainedSizeInBytes(): Long
    {
        // columnType is omitted as Type instances are shared by Trino type registry
        return (INSTANCE_SIZE.toLong()
                + sizeOf(columnId)
                + estimatedSizeOf(columnName)
                + sizeOf(nullable))
    }

    override fun equals(other: Any?): Boolean
    {
        if (this === other) return true
        if (other !is DucklakeColumnHandle) return false
        return columnId == other.columnId
                && columnName == other.columnName
                && columnType == other.columnType
                && nullable == other.nullable
    }

    override fun hashCode(): Int
    {
        return Objects.hash(columnId, columnName, columnType, nullable)
    }

    companion object {
        private val INSTANCE_SIZE: Int = instanceSize(DucklakeColumnHandle::class.java)

        const val ROW_ID_COLUMN_ID: Long = -100
        const val ROW_ID_COLUMN_NAME: String = "\$row_id"

        @JvmStatic
        fun rowIdColumnHandle(): DucklakeColumnHandle
        {
            return DucklakeColumnHandle(ROW_ID_COLUMN_ID, ROW_ID_COLUMN_NAME, BIGINT, false)
        }
    }
}
