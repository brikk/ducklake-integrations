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

@JvmRecord
@JacksonSerializedInternalJavaCompatibleClass
@JsonInclude(JsonInclude.Include.NON_NULL)
data class DucklakeWriteFragment(
    val path: String,
    val pathIsRelative: Boolean,
    val fileFormat: String,
    val fileSizeBytes: Long,
    val footerSize: Long,
    val recordCount: Long,
    val columnStats: List<DucklakeFileColumnStats>,
    val partitionValues: Map<Int, String?>,
    val partitionId: Long?,
    val nameMap: DucklakeNameMap?,
) {
    init {
        // Parity with Java record's compact-constructor requireNonNull on
        // platform-typed callers (e.g. raw Java callers via reflection).
        @Suppress("SENSELESS_COMPARISON")
        if (path == null) throw NullPointerException("path is null")
        @Suppress("SENSELESS_COMPARISON")
        if (columnStats == null) throw NullPointerException("columnStats is null")
    }

    /**
     * Convenience constructor for unpartitioned parquet fragments produced by the
     * connector's own INSERT path. Stores the file under the table's data path
     * (relative) and inherits the default no-name-map convention used by today's
     * INSERT / CTAS — reads project parquet columns by their embedded field_id
     * annotations.
     */
    constructor(
        path: String,
        fileSizeBytes: Long,
        footerSize: Long,
        recordCount: Long,
        columnStats: List<DucklakeFileColumnStats>,
    ) : this(
        path = path,
        pathIsRelative = true,
        fileFormat = "parquet",
        fileSizeBytes = fileSizeBytes,
        footerSize = footerSize,
        recordCount = recordCount,
        columnStats = columnStats.toList(),
        partitionValues = emptyMap(),
        partitionId = null,
        nameMap = null,
    )

    /**
     * Convenience constructor for partitioned parquet fragments produced by the
     * connector's own INSERT path.
     */
    constructor(
        path: String,
        fileSizeBytes: Long,
        footerSize: Long,
        recordCount: Long,
        columnStats: List<DucklakeFileColumnStats>,
        partitionValues: Map<Int, String?>,
        partitionId: Long?,
    ) : this(
        path = path,
        pathIsRelative = true,
        fileFormat = "parquet",
        fileSizeBytes = fileSizeBytes,
        footerSize = footerSize,
        recordCount = recordCount,
        columnStats = columnStats.toList(),
        partitionValues = partitionValues.toMap(),
        partitionId = partitionId,
        nameMap = null,
    )

    /**
     * Six-arg legacy constructor kept for backwards-compatibility with callers
     * that pass an explicit `fileFormat`. New code should pass
     * `pathIsRelative` and `nameMap` explicitly through the canonical
     * constructor.
     */
    constructor(
        path: String,
        fileFormat: String,
        fileSizeBytes: Long,
        footerSize: Long,
        recordCount: Long,
        columnStats: List<DucklakeFileColumnStats>,
        partitionValues: Map<Int, String?>,
        partitionId: Long?,
    ) : this(
        path = path,
        pathIsRelative = true,
        fileFormat = fileFormat,
        fileSizeBytes = fileSizeBytes,
        footerSize = footerSize,
        recordCount = recordCount,
        columnStats = columnStats.toList(),
        partitionValues = partitionValues.toMap(),
        partitionId = partitionId,
        nameMap = null,
    )

}
