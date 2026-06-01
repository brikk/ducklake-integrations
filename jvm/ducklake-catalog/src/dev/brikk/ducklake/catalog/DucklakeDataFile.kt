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

/**
 * Represents a data file from the ducklake_data_file table.
 */
@JvmRecord
data class DucklakeDataFile(
    val dataFileId: Long,
    val tableId: Long,
    val beginSnapshot: Long,
    val endSnapshot: Optional<Long>,
    val fileOrder: Long,
    val path: String,
    val pathIsRelative: Boolean,
    val fileFormat: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long,
    val rowIdStart: Long,
    val partitionId: Optional<Long>,
    val deleteFilePath: Optional<String>,
    val deleteFilePathIsRelative: Optional<Boolean>,
    val deleteFileFooterSize: Optional<Long>,
    val deleteFileFormat: Optional<String>,
    val mappingId: Optional<Long>,
) {
    init {
        // Parity with Java record's compact-constructor requireNonNull on
        // platform-typed callers (e.g. raw Java callers via reflection).
        @Suppress("SENSELESS_COMPARISON")
        if (path == null) throw NullPointerException("path is null")
        @Suppress("SENSELESS_COMPARISON")
        if (fileFormat == null) throw NullPointerException("fileFormat is null")
        @Suppress("SENSELESS_COMPARISON")
        if (endSnapshot == null) throw NullPointerException("endSnapshot is null")
        @Suppress("SENSELESS_COMPARISON")
        if (partitionId == null) throw NullPointerException("partitionId is null")
        @Suppress("SENSELESS_COMPARISON")
        if (deleteFilePath == null) throw NullPointerException("deleteFilePath is null")
        @Suppress("SENSELESS_COMPARISON")
        if (deleteFilePathIsRelative == null) throw NullPointerException("deleteFilePathIsRelative is null")
        @Suppress("SENSELESS_COMPARISON")
        if (deleteFileFooterSize == null) throw NullPointerException("deleteFileFooterSize is null")
        @Suppress("SENSELESS_COMPARISON")
        if (deleteFileFormat == null) throw NullPointerException("deleteFileFormat is null")
        @Suppress("SENSELESS_COMPARISON")
        if (mappingId == null) throw NullPointerException("mappingId is null")
    }

    /**
     * Backwards-compatible constructor for call sites that don't carry a name-map id.
     */
    constructor(
        dataFileId: Long,
        tableId: Long,
        beginSnapshot: Long,
        endSnapshot: Optional<Long>,
        fileOrder: Long,
        path: String,
        pathIsRelative: Boolean,
        fileFormat: String,
        recordCount: Long,
        fileSizeBytes: Long,
        footerSize: Long,
        rowIdStart: Long,
        partitionId: Optional<Long>,
        deleteFilePath: Optional<String>,
        deleteFilePathIsRelative: Optional<Boolean>,
        deleteFileFooterSize: Optional<Long>,
        deleteFileFormat: Optional<String>,
    ) : this(
        dataFileId, tableId, beginSnapshot, endSnapshot, fileOrder, path, pathIsRelative,
        fileFormat, recordCount, fileSizeBytes, footerSize, rowIdStart, partitionId,
        deleteFilePath, deleteFilePathIsRelative, deleteFileFooterSize, deleteFileFormat,
        Optional.empty(),
    )
}
