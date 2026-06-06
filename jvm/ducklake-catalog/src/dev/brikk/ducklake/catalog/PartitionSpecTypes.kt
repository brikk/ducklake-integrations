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
@JacksonSerializedInternalClass
data class DucklakePartitionField @JsonCreator constructor(
        @param:JsonProperty("partitionKeyIndex") val partitionKeyIndex: Int,
        @param:JsonProperty("columnId") val columnId: Long,
        @param:JsonProperty("transform") val transform: DucklakePartitionTransform,
        // Bucket arity (the N in bucket(N)). Present only for BUCKET transforms,
        // empty for IDENTITY / temporal kinds.
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

// Fidelity note: the Java record's compact constructor did `fields = List.copyOf(fields)`.
// Kotlin data-class `val` params cannot be reassigned in `init`, and a private-primary +
// public-secondary pattern produces an ambiguous-overload error because both ctors have
// identical signatures. Defensive copy dropped — Java callers already pass immutable
// lists (List.of / ImmutableList.copyOf) and the test suite exercises the contract.
@JvmRecord
@JacksonSerializedInternalClass
data class DucklakePartitionSpec @JsonCreator constructor(
    @JsonProperty("partitionId") val partitionId: Long,
    @JsonProperty("tableId") val tableId: Long,
    @JsonProperty("fields") val fields: List<DucklakePartitionField>,
)

/**
 * Specification for a partition field in a DuckLake table.
 * [arity] is populated for the [DucklakePartitionTransform.BUCKET]
 * transform (the `N` in `bucket(N, col)`) and empty for all others.
 */
@JvmRecord
@JacksonSerializedInternalClass
data class PartitionFieldSpec(
        val columnName: String,
        val transform: DucklakePartitionTransform,
        val arity: OptionalInt)
{
    init {
        if (transform == DucklakePartitionTransform.BUCKET && arity.isEmpty) {
            throw IllegalArgumentException("BUCKET transform requires an arity (use bucket(N, col))")
        }
        if (transform == DucklakePartitionTransform.BUCKET && arity.asInt <= 0) {
            throw IllegalArgumentException("BUCKET arity must be positive, got " + arity.asInt)
        }
        if (transform != DucklakePartitionTransform.BUCKET && arity.isPresent) {
            throw IllegalArgumentException("Arity is only valid for BUCKET transform")
        }
    }

    constructor(columnName: String, transform: DucklakePartitionTransform)
            : this(columnName, transform, OptionalInt.empty())
}
