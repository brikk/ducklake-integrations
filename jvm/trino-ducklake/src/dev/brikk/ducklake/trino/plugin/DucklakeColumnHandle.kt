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
import io.trino.spi.type.VarcharType.VARCHAR

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
        @param:JsonProperty("nullable") val nullable: Boolean,
        /**
         * `ducklake_column.initial_default` — the value rows written BEFORE the
         * column existed must project (plain value text). Null = no default.
         * NON_NULL keeps the wire format unchanged for the common case.
         */
        @get:JvmName("initialDefault")
        @get:com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        @param:JsonProperty("initialDefault") val initialDefault: String? = null)
        : ColumnHandle
{
    fun isRowIdColumn(): Boolean
    {
        return columnId == ROW_ID_COLUMN_ID
    }

    /**
     * The virtual-column kind this handle represents, or null for a real catalog
     * column (and for the MERGE row-id channel handle, which is NOT a queryable
     * virtual — see [VirtualKind]). Centralizes the reserved-sentinel-id convention
     * so callers never test magic numbers directly.
     */
    fun virtualKind(): VirtualKind? = VirtualKind.fromColumnId(columnId)

    /** Whether this handle is one of the queryable hidden virtual columns. */
    fun isVirtual(): Boolean = virtualKind() != null

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

        fun rowIdColumnHandle(): DucklakeColumnHandle
        {
            return DucklakeColumnHandle(ROW_ID_COLUMN_ID, ROW_ID_COLUMN_NAME, BIGINT, false)
        }
    }
}

/**
 * The DuckLake "virtual" (hidden) columns the connector exposes on the read path,
 * matching Trino's Iceberg/Delta `$`-prefixed convention. This enum is the single
 * source of truth for each virtual's reserved (negative) `columnId`, hidden name, and
 * Trino type — keep the magic numbers here and nowhere else.
 *
 * Real catalog columns always have non-negative ids, so the negative range is
 * collision-free. The MERGE row-id channel keeps its own distinct id
 * ([DucklakeColumnHandle.ROW_ID_COLUMN_ID] = -100) and flows through
 * `getMergeRowIdColumnHandle`, not `getColumnHandles`; these queryable virtuals start
 * at -101. The `$row_id` virtual (-104) shares the name and `rowIdStart + position`
 * encoding with the MERGE channel but is a distinct handle — see
 * dev-docs/DESIGN-virtual-columns.md § 3.4 / § 3.6.
 */
enum class VirtualKind(
        @get:JvmName("columnId") val columnId: Long,
        @get:JvmName("columnName") val columnName: String,
        @get:JvmName("columnType") val columnType: Type,
        // perRow=true: the value varies per row (file position based) and must be injected
        // inside the read pipeline before delete-filtering — see DucklakePageSourceProvider's
        // positional injector. perRow=false: constant per split (RLE block in the outer wrapper).
        @get:JvmName("perRow") val perRow: Boolean)
{
    PATH(-101L, "\$path", VARCHAR, false),
    SNAPSHOT_ID(-102L, "\$snapshot_id", BIGINT, false),
    FILE_ROW_NUMBER(-103L, "\$file_row_number", BIGINT, true),
    ROW_ID(-104L, "\$row_id", BIGINT, true),
    FILE_SIZE_BYTES(-105L, "\$file_size_bytes", BIGINT, false);

    /**
     * The hidden column handle for this virtual. Marked nullable because the
     * file-bound virtuals ($path, $file_row_number, $row_id) are NULL on inlined-data
     * splits; $snapshot_id is never null but marking it nullable is harmless.
     */
    fun columnHandle(): DucklakeColumnHandle =
            DucklakeColumnHandle(columnId, columnName, columnType, true)

    companion object {
        private val BY_COLUMN_ID: Map<Long, VirtualKind> =
                values().associateBy(VirtualKind::columnId)

        fun fromColumnId(columnId: Long): VirtualKind? = BY_COLUMN_ID[columnId]
    }
}
