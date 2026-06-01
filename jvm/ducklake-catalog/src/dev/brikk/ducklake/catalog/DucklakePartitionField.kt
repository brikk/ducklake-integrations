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
import java.util.OptionalInt

@JvmRecord
data class DucklakePartitionField @JsonCreator constructor(
        @get:JvmName("partitionKeyIndex")
        @param:JsonProperty("partitionKeyIndex") val partitionKeyIndex: Int,
        @get:JvmName("columnId")
        @param:JsonProperty("columnId") val columnId: Long,
        @get:JvmName("transform")
        @param:JsonProperty("transform") val transform: DucklakePartitionTransform,
        // Bucket arity (the N in bucket(N)). Present only for BUCKET transforms,
        // empty for IDENTITY / temporal kinds.
        @get:JvmName("arity")
        @param:JsonProperty("arity") val arity: OptionalInt)
{
    init {
        if (transform == DucklakePartitionTransform.BUCKET && arity.isEmpty) {
            throw IllegalArgumentException("BUCKET transform requires an arity")
        }
        if (transform != DucklakePartitionTransform.BUCKET && arity.isPresent) {
            throw IllegalArgumentException("Arity is only valid for BUCKET transform")
        }
    }

    constructor(partitionKeyIndex: Int, columnId: Long, transform: DucklakePartitionTransform)
            : this(partitionKeyIndex, columnId, transform, OptionalInt.empty())
}
