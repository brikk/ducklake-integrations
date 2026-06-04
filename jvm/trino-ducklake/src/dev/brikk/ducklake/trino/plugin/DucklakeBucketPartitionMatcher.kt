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
package dev.brikk.ducklake.trino.plugin

import io.airlift.slice.Slice
import io.trino.spi.block.ValueBlock
import io.trino.spi.predicate.Domain
import io.trino.spi.type.Type

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
object DucklakeBucketPartitionMatcher {
    fun partitionValueMatchesDomain(
            columnType: Type,
            partitionValue: String,
            domain: Domain,
            arity: Int): Boolean {
        try {
            if (domain.isNone) {
                return false
            }
            if (domain.values.isAll) {
                return true
            }

            val targetBucket = partitionValue.toInt()

            // Only discrete predicates (equality / IN) give us useful pruning for bucket.
            val values = domain.values
            return values.valuesProcessor.transform(
                    { ranges ->
                        // Single-point ranges (a <= x AND x <= a) are effectively equality —
                        // hash the singleton and match. Anything wider can span any bucket,
                        // so we can't prune.
                        var matched = false
                        for (range in ranges.orderedRanges) {
                            if (range.isSingleValue) {
                                if (hashesToBucket(columnType, range.singleValue, arity, targetBucket)) {
                                    matched = true
                                    break
                                }
                            }
                            else {
                                // Non-point range — bucket can be anywhere, can't prune.
                                matched = true
                                break
                            }
                        }
                        matched
                    },
                    { discreteValues ->
                        if (!discreteValues.isInclusive) {
                            // Exclusion set (x != v / NOT IN (...)): getValues() lists the
                            // values to EXCLUDE. The file may hold any non-excluded value,
                            // which can hash to any bucket — so we can never prune. (Latent
                            // today: every bucket-able type is orderable and routes through
                            // rangesFunction; guard it anyway against silent data loss.)
                            true
                        }
                        else {
                            discreteValues.values.any { value ->
                                hashesToBucket(columnType, value, arity, targetBucket)
                            }
                        }
                    },
                    { _ -> true })
        }
        catch (_: RuntimeException) {
            return true // parse / hash failure — don't prune to avoid false negatives
        }
    }

    private fun hashesToBucket(columnType: Type, value: Any?, arity: Int, targetBucket: Int): Boolean {
        // Build a single-position block carrying this value so we can reuse the
        // exact hash function used at write time. Going through a block keeps the
        // serialization of strings, varbinary, timestamps, etc. byte-identical to
        // the write path; otherwise we'd duplicate that adapter logic here.
        val block: ValueBlock = nativeValueBlock(columnType, value)
        val bucket = DucklakePartitionComputer.computeBucket(columnType, block, 0, arity)
        return bucket == targetBucket
    }

    private fun nativeValueBlock(columnType: Type, value: Any?): ValueBlock {
        val builder = columnType.createBlockBuilder(null, 1)
        if (value == null) {
            builder.appendNull()
        }
        else if (value is Long) {
            columnType.writeLong(builder, value)
        }
        else if (value is Int) {
            columnType.writeLong(builder, value.toLong())
        }
        else if (value is Slice) {
            columnType.writeSlice(builder, value)
        }
        else if (value is Boolean) {
            columnType.writeBoolean(builder, value)
        }
        else {
            columnType.writeObject(builder, value)
        }
        return builder.buildValueBlock()
    }
}
