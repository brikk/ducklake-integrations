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
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record DucklakeWriteFragment(
        @JsonProperty("path") String path,
        @JsonProperty("fileSizeBytes") long fileSizeBytes,
        @JsonProperty("footerSize") long footerSize,
        @JsonProperty("recordCount") long recordCount,
        @JsonProperty("columnStats") List<DucklakeFileColumnStats> columnStats,
        @JsonProperty("partitionValues") Map<Integer, String> partitionValues,
        @JsonProperty("partitionId") OptionalLong partitionId)
{
    @JsonCreator
    public DucklakeWriteFragment
    {
        requireNonNull(path, "path is null");
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
    }

    /**
     * Convenience constructor for unpartitioned fragments.
     */
    public DucklakeWriteFragment(String path, long fileSizeBytes, long footerSize, long recordCount, List<DucklakeFileColumnStats> columnStats)
    {
        this(path, fileSizeBytes, footerSize, recordCount, columnStats, Map.of(), OptionalLong.empty());
    }
}
