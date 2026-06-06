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

@JvmRecord
data class DucklakeColumnStats(
    val columnId: Long,
    val totalValueCount: Long,
    val totalNullCount: Long,
    val totalSizeBytes: Long,
    val minValue: Optional<String>,
    val maxValue: Optional<String>,
) {
    init {
        // Parity with Java record's compact-constructor requireNonNull on platform-typed callers.
        @Suppress("SENSELESS_COMPARISON")
        if (minValue == null) throw NullPointerException("minValue is null")
        @Suppress("SENSELESS_COMPARISON")
        if (maxValue == null) throw NullPointerException("maxValue is null")
    }
}
