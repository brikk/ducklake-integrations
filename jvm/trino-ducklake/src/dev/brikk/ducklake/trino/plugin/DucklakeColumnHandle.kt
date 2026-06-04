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
import io.trino.spi.type.BigintType.BIGINT

/**
 * Handle for a Ducklake column.
 */
@JvmRecord
data class DucklakeColumnHandle @JsonCreator constructor(
        @get:JvmName("columnId")
        @param:JsonProperty("columnId") val columnId: Long,
        @get:JvmName("columnName")
        @param:JsonProperty("columnName") val columnName: String,
        @get:JvmName("columnType")
        @param:JsonProperty("columnType") val columnType: Type,
        @get:JvmName("nullable")
        @param:JsonProperty("nullable") val nullable: Boolean)
        : ColumnHandle
{
    fun isRowIdColumn(): Boolean
    {
        return columnId == ROW_ID_COLUMN_ID
    }

    override fun toString(): String
    {
        return "$columnName:$columnType"
    }

    fun getRetainedSizeInBytes(): Long
    {
        // INSTANCE_SIZE already accounts for primitive fields (columnId, nullable).
        // columnType is omitted as Type instances are shared by Trino type registry.
        return INSTANCE_SIZE.toLong() + estimatedSizeOf(columnName)
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
