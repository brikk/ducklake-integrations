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

import com.fasterxml.jackson.annotation.JsonInclude

/**
 * Represents a data file from the ducklake_data_file table.
 */
@JvmRecord
@JacksonSerializedInternalJavaCompatibleClass
@JsonInclude(JsonInclude.Include.NON_NULL)
data class DucklakeDataFile(
    val dataFileId: Long,
    val tableId: Long,
    val beginSnapshot: Long,
    val endSnapshot: Long?,
    val fileOrder: Long,
    val path: String,
    val pathIsRelative: Boolean,
    val fileFormat: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long,
    val rowIdStart: Long,
    val partitionId: Long?,
    val deleteFilePath: String?,
    val deleteFilePathIsRelative: Boolean?,
    val deleteFileFooterSize: Long?,
    val deleteFileFormat: String?,
    val mappingId: Long?,
    /**
     * `ducklake_data_file.partial_max` — the MAX `_ducklake_internal_snapshot_id` in a
     * cross-snapshot compacted ("partial") file (`begin_snapshot` is the implicit MIN). NULL for
     * ordinary files. A read at snapshot S must drop the file's rows whose internal snapshot id
     * exceeds S, but only when `partial_max > S`.
     */
    val partialMax: Long? = null,
    /**
     * `ducklake_delete_file.partial_max` for the joined delete file — the MAX
     * `_ducklake_internal_snapshot_id` in a consolidated ("partial") delete file. NULL for
     * ordinary delete files (or none). A read at S applies only the deletions whose internal
     * snapshot id is <= S, but only when this exceeds S.
     */
    val deleteFilePartialMax: Long? = null,
) {
    /**
     * Backwards-compatible constructor for call sites that don't carry a name-map id.
     */
    constructor(
        dataFileId: Long,
        tableId: Long,
        beginSnapshot: Long,
        endSnapshot: Long?,
        fileOrder: Long,
        path: String,
        pathIsRelative: Boolean,
        fileFormat: String,
        recordCount: Long,
        fileSizeBytes: Long,
        footerSize: Long,
        rowIdStart: Long,
        partitionId: Long?,
        deleteFilePath: String?,
        deleteFilePathIsRelative: Boolean?,
        deleteFileFooterSize: Long?,
        deleteFileFormat: String?,
    ) : this(
        dataFileId, tableId, beginSnapshot, endSnapshot, fileOrder, path, pathIsRelative,
        fileFormat, recordCount, fileSizeBytes, footerSize, rowIdStart, partitionId,
        deleteFilePath, deleteFilePathIsRelative, deleteFileFooterSize, deleteFileFormat,
        null,
    )
}

/**
 * A raw storage-path reference held by the catalog (relative or absolute per [pathIsRelative]).
 * Used by orphan-file detection to build the "known set" of paths the catalog owns for a table.
 */
data class DucklakeFilePathRef(
    val path: String,
    val pathIsRelative: Boolean,
)

/**
 * A file row from `ducklake_files_scheduled_for_deletion`. NOTE: per the DuckLake spec the
 * relative paths in this table are relative to the **catalog global `data_path` root** (not a
 * per-table directory — that's why the table carries no `table_id`). [dataFileId] is the id of the
 * data- or delete-file row that was retired (used only to delete the schedule row after the
 * physical file is removed).
 */
data class DucklakeScheduledFile(
    val dataFileId: Long,
    val path: String,
    val pathIsRelative: Boolean,
)

/** Outcome of [DucklakeCatalog.expireSnapshots]. */
data class ExpireSnapshotsResult(
    val expiredSnapshotCount: Int,
    val scheduledFileCount: Int,
)

/**
 * One output file of a partial-emitting compaction ([DucklakeCatalog.rewriteDataFilesPartial]):
 * the registration [fragment] plus the back-dated [beginSnapshot] (= MIN of the source
 * begin_snapshots of the rows it holds) and [partialMax] (= MAX). The merged file physically
 * carries a per-row `_ducklake_internal_snapshot_id` column so a read at S keeps rows whose value
 * is `<= S`; these bounds are persisted onto the `ducklake_data_file` row.
 */
data class PartialMergedFile(
    val fragment: DucklakeWriteFragment,
    val beginSnapshot: Long,
    val partialMax: Long,
)
