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

import static java.util.Objects.requireNonNull;

/**
 * One entry in a {@link DucklakeNameMap}: maps a parquet column path (by
 * source name) onto a target DuckLake field_id. Recurses through nested
 * types (STRUCT / LIST / MAP) via {@link #children()}.
 *
 * <p>Mirrors a row in {@code ducklake_name_mapping}, plus a recursive
 * parent/child relationship persisted via the {@code parent_column}
 * column on the same table.
 *
 * <p>{@link #isPartition()} marks a hive-partition entry — the value
 * comes from the file path rather than a parquet column, so it has no
 * source column in the parquet schema. Upstream stores these the same
 * way (an entry in {@code ducklake_name_mapping} with
 * {@code is_partition = true}).
 */
public record DucklakeNameMapEntry(
        @JsonProperty("sourceName") String sourceName,
        @JsonProperty("targetFieldId") long targetFieldId,
        @JsonProperty("isPartition") boolean isPartition,
        @JsonProperty("children") List<DucklakeNameMapEntry> children)
{
    @JsonCreator
    public DucklakeNameMapEntry
    {
        requireNonNull(sourceName, "sourceName is null");
        children = children == null ? List.of() : List.copyOf(children);
    }

    public DucklakeNameMapEntry(String sourceName, long targetFieldId)
    {
        this(sourceName, targetFieldId, false, List.of());
    }

    public DucklakeNameMapEntry(String sourceName, long targetFieldId, List<DucklakeNameMapEntry> children)
    {
        this(sourceName, targetFieldId, false, children);
    }
}
