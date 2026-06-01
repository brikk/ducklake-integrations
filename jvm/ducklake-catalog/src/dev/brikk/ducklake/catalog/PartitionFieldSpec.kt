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

import java.util.OptionalInt

/**
 * Specification for a partition field in a DuckLake table.
 * [arity] is populated for the [DucklakePartitionTransform.BUCKET]
 * transform (the `N` in `bucket(N, col)`) and empty for all others.
 */
@JvmRecord
data class PartitionFieldSpec(
        @get:JvmName("columnName") val columnName: String,
        @get:JvmName("transform") val transform: DucklakePartitionTransform,
        @get:JvmName("arity") val arity: OptionalInt)
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
