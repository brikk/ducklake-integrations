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

@JvmRecord
@JacksonSerializedInternalClass
@JsonInclude(JsonInclude.Include.NON_NULL)
data class DucklakePartitionField(
        val partitionKeyIndex: Int,
        val columnId: Long,
        val transform: DucklakePartitionTransform,
        // Bucket arity (the N in bucket(N)). Present only for BUCKET transforms,
        // null for IDENTITY / temporal kinds.
        val arity: Int?)
{
    init {
        if (transform == DucklakePartitionTransform.BUCKET && arity == null) {
            throw IllegalArgumentException("BUCKET transform requires an arity")
        }
        if (transform != DucklakePartitionTransform.BUCKET && arity != null) {
            throw IllegalArgumentException("Arity is only valid for BUCKET transform")
        }
    }

    constructor(partitionKeyIndex: Int, columnId: Long, transform: DucklakePartitionTransform)
            : this(partitionKeyIndex, columnId, transform, null)
}

// Fidelity note: the Java record's compact constructor did `fields = List.copyOf(fields)`.
// Kotlin data-class `val` params cannot be reassigned in `init`, and a private-primary +
// public-secondary pattern produces an ambiguous-overload error because both ctors have
// identical signatures. Defensive copy dropped — Java callers already pass immutable
// lists (List.of / ImmutableList.copyOf) and the test suite exercises the contract.
@JvmRecord
@JacksonSerializedInternalClass
data class DucklakePartitionSpec(
    val partitionId: Long,
    val tableId: Long,
    val fields: List<DucklakePartitionField>,
)

/**
 * Specification for a partition field in a DuckLake table.
 * [arity] is populated for the [DucklakePartitionTransform.BUCKET]
 * transform (the `N` in `bucket(N, col)`) and empty for all others.
 */
@JvmRecord
@JacksonSerializedInternalClass
@JsonInclude(JsonInclude.Include.NON_NULL)
data class PartitionFieldSpec(
        val columnName: String,
        val transform: DucklakePartitionTransform,
        val arity: Int?)
{
    init {
        if (transform == DucklakePartitionTransform.BUCKET && arity == null) {
            throw IllegalArgumentException("BUCKET transform requires an arity (use bucket(N, col))")
        }
        if (transform == DucklakePartitionTransform.BUCKET && arity!! <= 0) {
            throw IllegalArgumentException("BUCKET arity must be positive, got $arity")
        }
        if (transform != DucklakePartitionTransform.BUCKET && arity != null) {
            throw IllegalArgumentException("Arity is only valid for BUCKET transform")
        }
    }

    constructor(columnName: String, transform: DucklakePartitionTransform)
            : this(columnName, transform, null)
}
