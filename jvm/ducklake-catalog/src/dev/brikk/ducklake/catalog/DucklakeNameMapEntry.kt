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
package dev.brikk.ducklake.catalog

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * One entry in a [DucklakeNameMap]: maps a parquet column path (by
 * source name) onto a target DuckLake field_id. Recurses through nested
 * types (STRUCT / LIST / MAP) via [children].
 *
 * Mirrors a row in `ducklake_name_mapping`, plus a recursive
 * parent/child relationship persisted via the `parent_column`
 * column on the same table.
 *
 * [isPartition] marks a hive-partition entry — the value
 * comes from the file path rather than a parquet column, so it has no
 * source column in the parquet schema. Upstream stores these the same
 * way (an entry in `ducklake_name_mapping` with
 * `is_partition = true`).
 */
@JacksonSerializedInternalClass
data class DucklakeNameMapEntry(
        val sourceName: String,
        val targetFieldId: Long,
        val isPartition: Boolean,
        val children: List<DucklakeNameMapEntry>)
{
    companion object {
        @JsonCreator
        fun jsonCreate(
                @JsonProperty("sourceName") sourceName: String,
                @JsonProperty("targetFieldId") targetFieldId: Long,
                @JsonProperty("isPartition") isPartition: Boolean,
                @JsonProperty("children") children: List<DucklakeNameMapEntry>?): DucklakeNameMapEntry
        {
            return DucklakeNameMapEntry(
                    sourceName,
                    targetFieldId,
                    isPartition,
                children?.toList() ?: emptyList()
            )
        }
    }

    constructor(sourceName: String, targetFieldId: Long)
            : this(sourceName, targetFieldId, false, emptyList())

    constructor(sourceName: String, targetFieldId: Long, children: List<DucklakeNameMapEntry>)
            : this(sourceName, targetFieldId, false, children.toList())
}
