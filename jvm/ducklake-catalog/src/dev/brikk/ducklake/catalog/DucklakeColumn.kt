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
import java.util.Optional

/**
 * Represents a column from the ducklake_column table.
 */
@JvmRecord
@JacksonSerializedInternalClass
data class DucklakeColumn @JsonCreator constructor(
    @param:JsonProperty("columnId") val columnId: Long,
    @param:JsonProperty("beginSnapshot") val beginSnapshot: Long,
    @param:JsonProperty("endSnapshot") val endSnapshot: Optional<Long>,
    @param:JsonProperty("tableId") val tableId: Long,
    @param:JsonProperty("columnOrder") val columnOrder: Long,
    @param:JsonProperty("columnName") val columnName: String,
    @param:JsonProperty("columnType") val columnType: String,
    @param:JsonProperty("nullsAllowed") val nullsAllowed: Boolean,
    @param:JsonProperty("parentColumn") val parentColumn: Optional<Long>,
)
