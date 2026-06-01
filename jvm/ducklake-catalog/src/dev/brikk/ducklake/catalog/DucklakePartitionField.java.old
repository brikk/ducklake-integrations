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

import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public record DucklakePartitionField(
        @JsonProperty("partitionKeyIndex") int partitionKeyIndex,
        @JsonProperty("columnId") long columnId,
        @JsonProperty("transform") DucklakePartitionTransform transform,
        // Bucket arity (the N in bucket(N)). Present only for BUCKET transforms,
        // empty for IDENTITY / temporal kinds.
        @JsonProperty("arity") OptionalInt arity)
{
    @JsonCreator
    public DucklakePartitionField
    {
        requireNonNull(transform, "transform is null");
        requireNonNull(arity, "arity is null");
        if (transform == DucklakePartitionTransform.BUCKET && arity.isEmpty()) {
            throw new IllegalArgumentException("BUCKET transform requires an arity");
        }
        if (transform != DucklakePartitionTransform.BUCKET && arity.isPresent()) {
            throw new IllegalArgumentException("Arity is only valid for BUCKET transform");
        }
    }

    public DucklakePartitionField(int partitionKeyIndex, long columnId, DucklakePartitionTransform transform)
    {
        this(partitionKeyIndex, columnId, transform, OptionalInt.empty());
    }
}
