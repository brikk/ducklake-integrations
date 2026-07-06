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
 * One inlined row that changed within a change-feed window — a row in a dynamic
 * `ducklake_inlined_data_<tableId>_<schemaVersion>` table whose `begin_snapshot` (inserted) or
 * `end_snapshot` (deleted) falls in the window. Produced by [DucklakeCatalog.getInlinedChangesBetween].
 *
 * A single row can be BOTH an insert and a delete when it was inserted and deleted within the same
 * window; the caller emits an insert event at [beginSnapshot] and a delete event at [endSnapshot]
 * accordingly. [rowId] is DuckLake's global row id (the inlined table's `row_id`), the same space
 * the file-based feed uses. [values] are projected to the change feed's requested columns as of the
 * end snapshot (null-filled where the column didn't exist in this row's schema version).
 */
@JvmRecord
data class DucklakeInlinedChangeRow(
    val rowId: Long,
    val beginSnapshot: Long,
    val endSnapshot: Long?,
    val values: List<Any?>,
)

/**
 * One inlined DELETE of a DATA-FILE row — a row in the dynamic `ducklake_inlined_delete_<tableId>`
 * table (small DELETEs DuckDB records inline instead of writing a positional delete file). Produced
 * by [DucklakeCatalog.getInlinedFileDeletesBetween]: the file-local [position] of a row in data
 * file [dataFileId] retired at [snapshotId]. The caller reads that row from the data file (like a
 * regular positional delete) to emit the deletion.
 */
@JvmRecord
data class DucklakeInlinedFileDelete(
    val dataFileId: Long,
    val position: Long,
    val snapshotId: Long,
)
