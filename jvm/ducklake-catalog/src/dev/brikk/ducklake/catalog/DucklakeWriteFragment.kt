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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty

@JvmRecord
@JacksonSerializedInternalClass
@JsonInclude(JsonInclude.Include.NON_NULL)
data class DucklakeWriteFragment(
    @get:JsonProperty("path") val path: String,
    @get:JsonProperty("pathIsRelative") val pathIsRelative: Boolean,
    @get:JsonProperty("fileFormat") val fileFormat: String,
    @get:JsonProperty("fileSizeBytes") val fileSizeBytes: Long,
    @get:JsonProperty("footerSize") val footerSize: Long,
    @get:JsonProperty("recordCount") val recordCount: Long,
    @get:JsonProperty("columnStats") val columnStats: List<DucklakeFileColumnStats>,
    @get:JsonProperty("partitionValues") val partitionValues: Map<Int, String?>,
    @get:JsonProperty("partitionId") val partitionId: Long?,
    @get:JsonProperty("nameMap") val nameMap: DucklakeNameMap?,
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

    companion object {
        /**
         * Jackson entry point. Mirrors the Java record's compact-constructor
         * normalization (`null` → defaults, defensive copies) so deserialization
         * works the same whether fields are present, missing, or explicitly
         * null in the JSON payload.
         *
         * The canonical primary constructor itself can't perform null-tolerant
         * normalization (Kotlin val parameters are non-null reference types and
         * data-class `init` blocks can't rewrite property values); routing
         * Jackson through this factory preserves the Java side's exact
         * "missing JSON field becomes the documented default" behaviour.
         */
        @JsonCreator
        fun jsonCreate(
            @JsonProperty("path") path: String,
            @JsonProperty("pathIsRelative") pathIsRelative: Boolean,
            @JsonProperty("fileFormat") fileFormat: String?,
            @JsonProperty("fileSizeBytes") fileSizeBytes: Long,
            @JsonProperty("footerSize") footerSize: Long,
            @JsonProperty("recordCount") recordCount: Long,
            @JsonProperty("columnStats") columnStats: List<DucklakeFileColumnStats>,
            @JsonProperty("partitionValues") partitionValues: Map<Int, String?>?,
            @JsonProperty("partitionId") partitionId: Long?,
            @JsonProperty("nameMap") nameMap: DucklakeNameMap?,
        ): DucklakeWriteFragment {
            return DucklakeWriteFragment(
                path = path,
                pathIsRelative = pathIsRelative,
                fileFormat = fileFormat ?: "parquet",
                fileSizeBytes = fileSizeBytes,
                footerSize = footerSize,
                recordCount = recordCount,
                columnStats = columnStats.toList(),
                partitionValues = if (partitionValues == null) {
                    emptyMap()
                }
                else {
                    partitionValues.toMap()
                },
                partitionId = partitionId,
                nameMap = nameMap,
            )
        }
    }
}
