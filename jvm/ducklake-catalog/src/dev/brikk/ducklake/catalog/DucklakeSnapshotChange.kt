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
data class DucklakeSnapshotChange(
    val snapshotId: Long,
    val changesMade: Optional<String>,
    val author: Optional<String>,
    val commitMessage: Optional<String>,
    val commitExtraInfo: Optional<String>,
) {
    init {
        // Parity with Java record's compact-constructor requireNonNull on platform-typed callers.
        @Suppress("SENSELESS_COMPARISON")
        if (changesMade == null) throw NullPointerException("changesMade is null")
        @Suppress("SENSELESS_COMPARISON")
        if (author == null) throw NullPointerException("author is null")
        @Suppress("SENSELESS_COMPARISON")
        if (commitMessage == null) throw NullPointerException("commitMessage is null")
        @Suppress("SENSELESS_COMPARISON")
        if (commitExtraInfo == null) throw NullPointerException("commitExtraInfo is null")
    }
}
