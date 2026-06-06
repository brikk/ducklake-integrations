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

import java.time.Instant

/**
 * Represents a snapshot from the ducklake_snapshot table.
 */
@JvmRecord
data class DucklakeSnapshot(
    val snapshotId: Long,
    val snapshotTime: Instant,
    val schemaVersion: Long,
    val nextCatalogId: Long,
    val nextFileId: Long,
) {
    init {
        // Parity with Java record's compact-constructor requireNonNull on platform-typed callers.
        @Suppress("SENSELESS_COMPARISON")
        if (snapshotTime == null) throw NullPointerException("snapshotTime is null")
    }
}
