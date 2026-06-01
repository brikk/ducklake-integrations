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

import java.util.Optional
import java.util.UUID

/**
 * Represents a schema in the Ducklake catalog.
 * Maps to the ducklake_schema metadata table.
 */
@JvmRecord
data class DucklakeSchema(
    @get:JvmName("schemaId") val schemaId: Long,
    @get:JvmName("schemaUuid") val schemaUuid: UUID,
    @get:JvmName("beginSnapshot") val beginSnapshot: Long,
    @get:JvmName("endSnapshot") val endSnapshot: Optional<Long>,
    @get:JvmName("schemaName") val schemaName: String,
    @get:JvmName("path") val path: Optional<String>,
    @get:JvmName("pathIsRelative") val pathIsRelative: Optional<Boolean>,
)
