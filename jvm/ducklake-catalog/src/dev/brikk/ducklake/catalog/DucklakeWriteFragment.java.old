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
package dev.brikk.ducklake.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record DucklakeWriteFragment(
        @JsonProperty("path") String path,
        @JsonProperty("pathIsRelative") boolean pathIsRelative,
        @JsonProperty("fileFormat") String fileFormat,
        @JsonProperty("fileSizeBytes") long fileSizeBytes,
        @JsonProperty("footerSize") long footerSize,
        @JsonProperty("recordCount") long recordCount,
        @JsonProperty("columnStats") List<DucklakeFileColumnStats> columnStats,
        @JsonProperty("partitionValues") Map<Integer, String> partitionValues,
        @JsonProperty("partitionId") OptionalLong partitionId,
        @JsonProperty("nameMap") Optional<DucklakeNameMap> nameMap)
{
    @JsonCreator
    public DucklakeWriteFragment
    {
        requireNonNull(path, "path is null");
        if (fileFormat == null) {
            fileFormat = "parquet";
        }
        requireNonNull(columnStats, "columnStats is null");
        columnStats = List.copyOf(columnStats);
        if (partitionValues == null) {
            partitionValues = Map.of();
        }
        else {
            partitionValues = java.util.Collections.unmodifiableMap(new java.util.HashMap<>(partitionValues));
        }
        if (partitionId == null) {
            partitionId = OptionalLong.empty();
        }
        if (nameMap == null) {
            nameMap = Optional.empty();
        }
    }

    /**
     * Convenience constructor for unpartitioned parquet fragments produced by the
     * connector's own INSERT path. Stores the file under the table's data path
     * (relative) and inherits the default no-name-map convention used by today's
     * INSERT / CTAS — reads project parquet columns by their embedded field_id
     * annotations.
     */
    public DucklakeWriteFragment(String path, long fileSizeBytes, long footerSize, long recordCount, List<DucklakeFileColumnStats> columnStats)
    {
        this(path, true, "parquet", fileSizeBytes, footerSize, recordCount, columnStats, Map.of(), OptionalLong.empty(), Optional.empty());
    }

    /**
     * Convenience constructor for partitioned parquet fragments produced by the
     * connector's own INSERT path.
     */
    public DucklakeWriteFragment(
            String path,
            long fileSizeBytes,
            long footerSize,
            long recordCount,
            List<DucklakeFileColumnStats> columnStats,
            Map<Integer, String> partitionValues,
            OptionalLong partitionId)
    {
        this(path, true, "parquet", fileSizeBytes, footerSize, recordCount, columnStats, partitionValues, partitionId, Optional.empty());
    }

    /**
     * Six-arg legacy constructor kept for backwards-compatibility with callers
     * that pass an explicit {@code fileFormat}. New code should pass
     * {@code pathIsRelative} and {@code nameMap} explicitly through the canonical
     * constructor.
     */
    public DucklakeWriteFragment(
            String path,
            String fileFormat,
            long fileSizeBytes,
            long footerSize,
            long recordCount,
            List<DucklakeFileColumnStats> columnStats,
            Map<Integer, String> partitionValues,
            OptionalLong partitionId)
    {
        this(path, true, fileFormat, fileSizeBytes, footerSize, recordCount, columnStats, partitionValues, partitionId, Optional.empty());
    }
}
