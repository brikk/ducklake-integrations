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

/**
 * Per-table storage path passed at CREATE TABLE time. Lands in
 * `ducklake_table.path` / `ducklake_table.path_is_relative`.
 *
 * `path` is stored verbatim — callers are expected to have applied
 * any trailing-slash normalization and traversal/scheme checks before
 * constructing this record.
 */
@JvmRecord
data class TableLocationSpec(
    @get:JvmName("path") val path: String,
    @get:JvmName("isRelative") val isRelative: Boolean,
) {
    init {
        // Parity with Java record's compact-constructor requireNonNull on platform-typed callers.
        @Suppress("SENSELESS_COMPARISON")
        if (path == null) throw NullPointerException("path is null")
        if (path.isEmpty()) {
            throw IllegalArgumentException("path is empty")
        }
    }
}
