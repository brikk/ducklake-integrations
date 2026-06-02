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
package dev.brikk.ducklake.trino.plugin;

import io.airlift.slice.Slice;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;

/**
 * Pruning for files partitioned by Iceberg-compatible {@code bucket(N)} transforms.
 *
 * <p>Bucket pruning is only useful for *discrete* predicates: bucketing scrambles
 * value ordering, so a range predicate (e.g. {@code WHERE x > 10}) cannot exclude
 * any bucket. For equality / {@code IN} predicates we hash each constant and keep
 * files whose stored bucket value matches at least one of the resulting buckets.
 *
 * <p>Conservative on every non-discrete shape: returns {@code true} (don't prune)
 * to avoid false negatives.
 */
final class DucklakeBucketPartitionMatcher
{
    private DucklakeBucketPartitionMatcher() {}

    public static boolean partitionValueMatchesDomain(
            Type columnType,
            String partitionValue,
            Domain domain,
            int arity)
    {
        try {
            if (domain.isNone()) {
                return false;
            }
            if (domain.getValues().isAll()) {
                return true;
            }

            int targetBucket = Integer.parseInt(partitionValue);

            // Only discrete predicates (equality / IN) give us useful pruning for bucket.
            ValueSet values = domain.getValues();
            return values.getValuesProcessor().transform(
                    ranges -> {
                        // Single-point ranges (a <= x AND x <= a) are effectively equality —
                        // hash the singleton and match. Anything wider can span any bucket,
                        // so we can't prune.
                        for (var range : ranges.getOrderedRanges()) {
                            if (range.isSingleValue()) {
                                if (hashesToBucket(columnType, range.getSingleValue(), arity, targetBucket)) {
                                    return true;
                                }
                            }
                            else {
                                // Non-point range — bucket can be anywhere, can't prune.
                                return true;
                            }
                        }
                        return false;
                    },
                    discreteValues -> {
                        for (Object value : discreteValues.getValues()) {
                            if (hashesToBucket(columnType, value, arity, targetBucket)) {
                                return true;
                            }
                        }
                        return false;
                    },
                    allOrNone -> true);
        }
        catch (RuntimeException _) {
            return true; // parse / hash failure — don't prune to avoid false negatives
        }
    }

    private static boolean hashesToBucket(Type columnType, Object value, int arity, int targetBucket)
    {
        // Build a single-position block carrying this value so we can reuse the
        // exact hash function used at write time. Going through a block keeps the
        // serialization of strings, varbinary, timestamps, etc. byte-identical to
        // the write path; otherwise we'd duplicate that adapter logic here.
        ValueBlock block = nativeValueBlock(columnType, value);
        int bucket = DucklakePartitionComputer.computeBucket(columnType, block, 0, arity);
        return bucket == targetBucket;
    }

    private static ValueBlock nativeValueBlock(Type columnType, Object value)
    {
        var builder = columnType.createBlockBuilder(null, 1);
        if (value == null) {
            builder.appendNull();
        }
        else if (value instanceof Long longValue) {
            columnType.writeLong(builder, longValue);
        }
        else if (value instanceof Integer intValue) {
            columnType.writeLong(builder, intValue);
        }
        else if (value instanceof Slice slice) {
            columnType.writeSlice(builder, slice);
        }
        else if (value instanceof Boolean boolValue) {
            columnType.writeBoolean(builder, boolValue);
        }
        else {
            columnType.writeObject(builder, value);
        }
        return builder.buildValueBlock();
    }
}
