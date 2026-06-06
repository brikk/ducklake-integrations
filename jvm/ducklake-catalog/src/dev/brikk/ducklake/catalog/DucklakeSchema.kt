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
import java.util.UUID

/**
 * Represents a schema in the Ducklake catalog.
 * Maps to the ducklake_schema metadata table.
 */
@JvmRecord
@JacksonSerializedInternalJavaCompatibleClass
@JsonInclude(JsonInclude.Include.NON_NULL)
data class DucklakeSchema(
    val schemaId: Long,
    val schemaUuid: UUID,
    val beginSnapshot: Long,
    val endSnapshot: Long?,
    val schemaName: String,
    val path: String?,
    val pathIsRelative: Boolean?,
)
