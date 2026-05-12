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

/**
 * A column-name → field-id mapping for one or more data files registered
 * by {@code add_files}. Persisted to the catalog as one
 * {@code ducklake_column_mapping} row plus one
 * {@code ducklake_name_mapping} row per leaf-or-branch entry. Reused
 * across files that share the same parquet schema — the catalog dedupes
 * identical maps per call.
 */
public record DucklakeNameMap(@JsonProperty("entries") List<DucklakeNameMapEntry> entries)
{
    @JsonCreator
    public DucklakeNameMap
    {
        entries = entries == null ? List.of() : List.copyOf(entries);
    }
}
