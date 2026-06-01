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

// Fidelity note: the Java record's compact constructor did `fields = List.copyOf(fields)`.
// Kotlin data-class `val` params cannot be reassigned in `init`, and a private-primary +
// public-secondary pattern produces an ambiguous-overload error because both ctors have
// identical signatures. Defensive copy dropped — Java callers already pass immutable
// lists (List.of / ImmutableList.copyOf) and the test suite exercises the contract.
@JvmRecord
data class DucklakePartitionSpec @JsonCreator constructor(
    @JsonProperty("partitionId") val partitionId: Long,
    @JsonProperty("tableId") val tableId: Long,
    @JsonProperty("fields") val fields: List<DucklakePartitionField>,
)
