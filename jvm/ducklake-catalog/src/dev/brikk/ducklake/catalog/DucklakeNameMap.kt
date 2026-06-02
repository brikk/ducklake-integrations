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

/**
 * A column-name → field-id mapping for one or more data files registered
 * by `add_files`. Persisted to the catalog as one
 * `ducklake_column_mapping` row plus one
 * `ducklake_name_mapping` row per leaf-or-branch entry. Reused
 * across files that share the same parquet schema — the catalog dedupes
 * identical maps per call.
 */
@JvmRecord
data class DucklakeNameMap(
    @get:JvmName("entries") val entries: List<DucklakeNameMapEntry>,
) {
    companion object {
        @JvmStatic
        @JsonCreator
        fun jsonCreate(
            @JsonProperty("entries") entries: List<DucklakeNameMapEntry>?,
        ): DucklakeNameMap {
            return DucklakeNameMap(
                if (entries == null) emptyList() else entries.toList(),
            )
        }
    }
}
