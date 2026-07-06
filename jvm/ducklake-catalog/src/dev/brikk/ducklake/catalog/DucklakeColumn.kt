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

import com.fasterxml.jackson.annotation.JsonInclude

/**
 * Represents a column from the ducklake_column table.
 */
@JvmRecord
@JacksonSerializedInternalJavaCompatibleClass
@JsonInclude(JsonInclude.Include.NON_NULL)
data class DucklakeColumn(
    val columnId: Long,
    val beginSnapshot: Long,
    val endSnapshot: Long?,
    val tableId: Long,
    val columnOrder: Long,
    val columnName: String,
    val columnType: String,
    val nullsAllowed: Boolean,
    val parentColumn: Long?,
    /**
     * `ducklake_column.initial_default`: the value rows written BEFORE this
     * column existed must project (plain value text, not SQL-quoted — e.g.
     * `42` or `hi 'there'`). Null = no default (project SQL NULL).
     */
    val initialDefault: String? = null,
)
