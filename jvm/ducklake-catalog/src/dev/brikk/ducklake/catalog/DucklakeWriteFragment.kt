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
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.Collections
import java.util.HashMap
import java.util.Optional
import java.util.OptionalLong

@JvmRecord
data class DucklakeWriteFragment(
    @get:JsonProperty("path") @get:JvmName("path") val path: String,
    @get:JsonProperty("pathIsRelative") @get:JvmName("pathIsRelative") val pathIsRelative: Boolean,
    @get:JsonProperty("fileFormat") @get:JvmName("fileFormat") val fileFormat: String,
    @get:JsonProperty("fileSizeBytes") @get:JvmName("fileSizeBytes") val fileSizeBytes: Long,
    @get:JsonProperty("footerSize") @get:JvmName("footerSize") val footerSize: Long,
    @get:JsonProperty("recordCount") @get:JvmName("recordCount") val recordCount: Long,
    @get:JsonProperty("columnStats") @get:JvmName("columnStats") val columnStats: List<DucklakeFileColumnStats>,
    @get:JsonProperty("partitionValues") @get:JvmName("partitionValues") val partitionValues: Map<Int, String?>,
    @get:JsonProperty("partitionId") @get:JvmName("partitionId") val partitionId: OptionalLong,
    @get:JsonProperty("nameMap") @get:JvmName("nameMap") val nameMap: Optional<DucklakeNameMap>,
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
        columnStats = java.util.List.copyOf(columnStats),
        partitionValues = emptyMap(),
        partitionId = OptionalLong.empty(),
        nameMap = Optional.empty(),
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
        partitionId: OptionalLong,
    ) : this(
        path = path,
        pathIsRelative = true,
        fileFormat = "parquet",
        fileSizeBytes = fileSizeBytes,
        footerSize = footerSize,
        recordCount = recordCount,
        columnStats = java.util.List.copyOf(columnStats),
        partitionValues = Collections.unmodifiableMap(HashMap(partitionValues)),
        partitionId = partitionId,
        nameMap = Optional.empty(),
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
        partitionId: OptionalLong,
    ) : this(
        path = path,
        pathIsRelative = true,
        fileFormat = fileFormat,
        fileSizeBytes = fileSizeBytes,
        footerSize = footerSize,
        recordCount = recordCount,
        columnStats = java.util.List.copyOf(columnStats),
        partitionValues = Collections.unmodifiableMap(HashMap(partitionValues)),
        partitionId = partitionId,
        nameMap = Optional.empty(),
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
        @JvmStatic
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
            @JsonProperty("partitionId") partitionId: OptionalLong?,
            @JsonProperty("nameMap") nameMap: Optional<DucklakeNameMap>?,
        ): DucklakeWriteFragment {
            return DucklakeWriteFragment(
                path = path,
                pathIsRelative = pathIsRelative,
                fileFormat = fileFormat ?: "parquet",
                fileSizeBytes = fileSizeBytes,
                footerSize = footerSize,
                recordCount = recordCount,
                columnStats = java.util.List.copyOf(columnStats),
                partitionValues = if (partitionValues == null) {
                    emptyMap()
                }
                else {
                    Collections.unmodifiableMap(HashMap(partitionValues))
                },
                partitionId = partitionId ?: OptionalLong.empty(),
                nameMap = nameMap ?: Optional.empty(),
            )
        }
    }
}
