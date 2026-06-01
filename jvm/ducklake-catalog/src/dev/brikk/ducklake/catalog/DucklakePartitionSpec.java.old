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

public record DucklakePartitionSpec(
        @JsonProperty("partitionId") long partitionId,
        @JsonProperty("tableId") long tableId,
        @JsonProperty("fields") List<DucklakePartitionField> fields)
{
    @JsonCreator
    public DucklakePartitionSpec
    {
        requireNonNull(fields, "fields is null");
        fields = List.copyOf(fields);
    }
}
