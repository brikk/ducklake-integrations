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
 * Metadata about an inlined data table in the DuckLake catalog.
 * DuckLake stores small table data directly in the metadata catalog
 * in tables named ducklake_inlined_data_{tableId}_{schemaVersion}.
 *
 * [hasLiveRows] records, for the snapshot the descriptor was resolved at, whether the
 * physical inlined table has any rows active at that snapshot. It is computed in the
 * same per-table probe that confirms the table exists, so callers can decide between a
 * real inlined split and the empty-table synthetic split without a second round trip.
 */
@JvmRecord
data class DucklakeInlinedDataInfo(
    @get:JvmName("tableId") val tableId: Long,
    @get:JvmName("tableName") val tableName: String,
    @get:JvmName("schemaVersion") val schemaVersion: Long,
    @get:JvmName("hasLiveRows") val hasLiveRows: Boolean,
) {
    init {
        // Parity with Java record's compact-constructor requireNonNull on platform-typed callers.
        @Suppress("SENSELESS_COMPARISON")
        if (tableName == null) throw NullPointerException("tableName is null")
    }
}
