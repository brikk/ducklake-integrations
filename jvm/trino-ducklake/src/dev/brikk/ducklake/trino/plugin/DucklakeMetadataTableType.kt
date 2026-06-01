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
package dev.brikk.ducklake.trino.plugin

import java.util.Locale
import java.util.Objects.requireNonNull
import java.util.Optional

enum class DucklakeMetadataTableType(private val suffix: String) {
    FILES("files"),
    SNAPSHOTS("snapshots"),
    CURRENT_SNAPSHOT("current_snapshot"),
    SNAPSHOT_CHANGES("snapshot_changes");

    init {
        requireNonNull(suffix, "suffix is null")
    }

    fun suffix(): String {
        return suffix
    }

    companion object {
        @JvmStatic
        fun fromSuffix(suffix: String?): Optional<DucklakeMetadataTableType> {
            val normalized = requireNonNull(suffix, "suffix is null")!!
                    .lowercase(Locale.ENGLISH)

            for (value in entries) {
                if (value.suffix == normalized) {
                    return Optional.of(value)
                }
            }
            return Optional.empty()
        }
    }
}
