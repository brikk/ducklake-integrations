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
 * One deletion event in the change feed: the rows of a single data file that were retired at
 * [snapshotId], expressed as the difference between the delete state *at* [snapshotId] and the
 * delete state *immediately before* it. Produced by [DucklakeCatalog.getDeletionsBetween].
 *
 * DuckLake writes a fresh, **cumulative** positional delete file per delete operation (the read
 * path only ever applies the single latest active delete file per data file — see
 * `getDataFiles`), so the rows newly deleted at [snapshotId] are:
 *
 * ```
 * positions(currentDelete)  −  positions(previousDelete)
 * ```
 *
 * Two shapes:
 *  - **incremental** ([fullFileDelete] = false): a new delete file with `begin_snapshot =
 *    snapshotId` was written. [currentDelete*] point at it; [previousDelete*] point at the
 *    delete file that was active just before (null when this is the file's first deletion).
 *  - **full-file** ([fullFileDelete] = true): the whole data file was retired at [snapshotId]
 *    (`end_snapshot = snapshotId` — a TRUNCATE/DROP, a DELETE that removed the last live rows, or
 *    compaction). Every position NOT already in [previousDelete*] is deleted; [currentDelete*] are
 *    null. (Compaction moves rows rather than deleting them; per the DuckLake spec, compaction can
 *    therefore limit what the change feed reports — such files still surface here as deletions.)
 *
 * The row identifier of a deleted position `pos` is `rowIdStart + pos` — the same DuckLake `rowid`
 * vocabulary the connector's `$row_id` virtual column uses.
 */
@JvmRecord
data class DucklakeChangeFeedDeletion(
    val snapshotId: Long,
    val dataFileBeginSnapshot: Long,
    val dataFilePath: String,
    val dataFilePathIsRelative: Boolean,
    val dataFileFormat: String,
    val dataFileFooterSize: Long,
    val dataFileSizeBytes: Long,
    val rowIdStart: Long,
    val recordCount: Long,
    val fullFileDelete: Boolean,
    val currentDeletePath: String?,
    val currentDeletePathIsRelative: Boolean?,
    val currentDeleteFormat: String?,
    val currentDeleteFooterSize: Long?,
    val currentDeletePartialMax: Long?,
    val previousDeletePath: String?,
    val previousDeletePathIsRelative: Boolean?,
    val previousDeleteFormat: String?,
    val previousDeleteFooterSize: Long?,
    val previousDeletePartialMax: Long?,
)
