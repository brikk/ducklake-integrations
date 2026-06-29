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
