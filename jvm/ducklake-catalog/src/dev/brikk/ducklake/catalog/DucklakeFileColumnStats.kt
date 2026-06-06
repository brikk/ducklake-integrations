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
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty

@JvmRecord
@JacksonSerializedInternalClass
@JsonInclude(JsonInclude.Include.NON_NULL)
data class DucklakeFileColumnStats @JsonCreator constructor(
    @param:JsonProperty("columnId") val columnId: Long,
    @param:JsonProperty("columnSizeBytes") val columnSizeBytes: Long,
    @param:JsonProperty("valueCount") val valueCount: Long,
    @param:JsonProperty("nullCount") val nullCount: Long,
    @param:JsonProperty("minValue") val minValue: String?,
    @param:JsonProperty("maxValue") val maxValue: String?,
    @param:JsonProperty("containsNan") val containsNan: Boolean,
)
