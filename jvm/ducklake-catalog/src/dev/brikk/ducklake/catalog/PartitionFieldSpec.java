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

import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

/**
 * Specification for a partition field in a DuckLake table.
 * {@code arity} is populated for the {@link DucklakePartitionTransform#BUCKET}
 * transform (the {@code N} in {@code bucket(N, col)}) and empty for all others.
 */
public record PartitionFieldSpec(
        String columnName,
        DucklakePartitionTransform transform,
        OptionalInt arity)
{
    public PartitionFieldSpec
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(transform, "transform is null");
        requireNonNull(arity, "arity is null");
        if (transform == DucklakePartitionTransform.BUCKET && arity.isEmpty()) {
            throw new IllegalArgumentException("BUCKET transform requires an arity (use bucket(N, col))");
        }
        if (transform == DucklakePartitionTransform.BUCKET && arity.getAsInt() <= 0) {
            throw new IllegalArgumentException("BUCKET arity must be positive, got " + arity.getAsInt());
        }
        if (transform != DucklakePartitionTransform.BUCKET && arity.isPresent()) {
            throw new IllegalArgumentException("Arity is only valid for BUCKET transform");
        }
    }

    public PartitionFieldSpec(String columnName, DucklakePartitionTransform transform)
    {
        this(columnName, transform, OptionalInt.empty());
    }
}
