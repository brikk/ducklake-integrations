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

import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.CALENDAR
import dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.EPOCH
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.Range
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.Type
import java.time.LocalDate

object DucklakeTemporalPartitionMatcher {
    @JvmStatic
    fun partitionValueMatchesDomain(
            columnType: Type,
            partitionValue: String,
            domain: Domain,
            transform: DucklakePartitionTransform,
            configuredEncoding: DucklakeTemporalPartitionEncoding,
            readLeniency: Boolean): Boolean {
        try {
            if (domain.isNone) {
                return false
            }
            if (domain.values.isAll) {
                return true
            }

            val transformedValue = partitionValue.toLong()
            if (!readLeniency) {
                return partitionValueMatchesDomainForEncoding(columnType, transformedValue, domain, transform, configuredEncoding)
            }

            if (partitionValueMatchesDomainForEncoding(columnType, transformedValue, domain, transform, configuredEncoding)) {
                return true
            }

            val fallbackEncoding: DucklakeTemporalPartitionEncoding = if (configuredEncoding == CALENDAR) EPOCH else CALENDAR
            return partitionValueMatchesDomainForEncoding(columnType, transformedValue, domain, transform, fallbackEncoding)
        }
        catch (_: RuntimeException) {
            // Never prune on parse/transform failures to avoid false negatives.
            return true
        }
    }

    private fun partitionValueMatchesDomainForEncoding(
            columnType: Type,
            transformedValue: Long,
            domain: Domain,
            transform: DucklakePartitionTransform,
            encoding: DucklakeTemporalPartitionEncoding): Boolean {
        if (!isPlausibleValueForEncoding(transformedValue, transform, encoding)) {
            return false
        }

        return domain.values.valuesProcessor.transform(
                { ranges ->
                    for (range in ranges.orderedRanges) {
                        if (temporalRangeContainsTransformedValue(columnType, range, transformedValue, transform, encoding)) {
                            return@transform true
                        }
                    }
                    false
                },
                { discreteValues ->
                    for (value in discreteValues.values) {
                        val transformed = applyTemporalTransform(columnType, value, transform, encoding)
                        if (transformed == transformedValue) {
                            return@transform true
                        }
                    }
                    false
                },
                { _ -> true })
    }

    private fun temporalRangeContainsTransformedValue(
            columnType: Type,
            range: Range,
            transformedValue: Long,
            transform: DucklakePartitionTransform,
            encoding: DucklakeTemporalPartitionEncoding): Boolean {
        val lowTransformed: Long = range.lowValue
                .map { v -> applyTemporalTransform(columnType, v, transform, encoding) }
                .orElse(Long.MIN_VALUE)
        var highTransformed: Long = range.highValue
                .map { v -> applyTemporalTransform(columnType, v, transform, encoding) }
                .orElse(Long.MAX_VALUE)

        val nonMonotonicCalendar: Boolean = encoding == CALENDAR &&
                (transform == DucklakePartitionTransform.MONTH
                        || transform == DucklakePartitionTransform.DAY
                        || transform == DucklakePartitionTransform.HOUR)

        // CALENDAR sub-year transforms (month/day/hour) are only monotonic within a single
        // parent period (a day for HOUR, a month for DAY, a year for MONTH). If either bound
        // is unbounded, or the two bounds straddle different parent periods, the transformed
        // value wraps and [low, high] range pruning would silently drop matching files.
        // Skip pruning in those cases — reading extra files is correct; dropping them is not.
        if (nonMonotonicCalendar &&
                (range.lowValue.isEmpty
                        || range.highValue.isEmpty
                        || !inSameCalendarParentPeriod(columnType, range.lowValue.get(), range.highValue.get(), transform))) {
            return true
        }

        // For non-DATE types, Trino does not normalize exclusive bounds to inclusive
        // (DATE is discrete so Trino converts e.g. `< DATE '2026-03-08'` to `<= DATE '2026-03-07'`).
        // When an exclusive high bound falls exactly on a partition transform boundary
        // (e.g. `< TIMESTAMP '2026-03-08 00:00:00'` with DAY transform), no data in that
        // partition can satisfy the predicate, so we must adjust the effective bound.
        if (columnType != io.trino.spi.type.DateType.DATE &&
                range.highValue.isPresent && !range.isHighInclusive
        ) {
            val epochMicros = extractEpochMicros(columnType, range.highValue.get())
            if (isAtTransformBoundary(epochMicros, transform)) {
                highTransformed--
            }
        }

        // Defensive fallback: even within one parent period an exclusive-high decrement can
        // invert the bounds (e.g. a range covering only the boundary instant). Skip pruning
        // rather than drop everything.
        if (nonMonotonicCalendar && lowTransformed > highTransformed) {
            return true
        }

        return transformedValue in lowTransformed..highTransformed
    }

    /**
     * Whether two range bounds fall in the same CALENDAR parent period for the given
     * sub-year transform, i.e. the period over which the transform is monotonic:
     * the same day for HOUR, the same calendar month for DAY, the same year for MONTH.
     */
    private fun inSameCalendarParentPeriod(columnType: Type, lowValue: Any, highValue: Any, transform: DucklakePartitionTransform): Boolean {
        val low = TemporalValue.from(columnType, lowValue)
        val high = TemporalValue.from(columnType, highValue)
        return when (transform) {
            DucklakePartitionTransform.HOUR -> low.epochDay == high.epochDay
            DucklakePartitionTransform.DAY -> low.date.year == high.date.year
                    && low.date.monthValue == high.date.monthValue
            DucklakePartitionTransform.MONTH -> low.date.year == high.date.year
            else -> true
        }
    }

    private fun isAtTransformBoundary(epochMicros: Long, transform: DucklakePartitionTransform): Boolean {
        return when (transform) {
            DucklakePartitionTransform.HOUR -> epochMicros % 3_600_000_000L == 0L
            DucklakePartitionTransform.DAY -> epochMicros % 86_400_000_000L == 0L
            DucklakePartitionTransform.MONTH -> {
                if (epochMicros % 86_400_000_000L != 0L) {
                    false
                }
                else {
                    val date = LocalDate.ofEpochDay(epochMicros / 86_400_000_000L)
                    date.dayOfMonth == 1
                }
            }
            DucklakePartitionTransform.YEAR -> {
                if (epochMicros % 86_400_000_000L != 0L) {
                    false
                }
                else {
                    val date = LocalDate.ofEpochDay(epochMicros / 86_400_000_000L)
                    date.monthValue == 1 && date.dayOfMonth == 1
                }
            }
            else -> false
        }
    }

    private fun applyTemporalTransform(columnType: Type, value: Any, transform: DucklakePartitionTransform, encoding: DucklakeTemporalPartitionEncoding): Long {
        val temporalValue = TemporalValue.from(columnType, value)

        if (encoding == CALENDAR) {
            return when (transform) {
                DucklakePartitionTransform.YEAR -> temporalValue.date.year.toLong()
                DucklakePartitionTransform.MONTH -> temporalValue.date.monthValue.toLong()
                DucklakePartitionTransform.DAY -> temporalValue.date.dayOfMonth.toLong()
                DucklakePartitionTransform.HOUR -> temporalValue.hourOfDay.toLong()
                else -> throw IllegalArgumentException("Unsupported transform for calendar encoding: $transform")
            }
        }

        return when (transform) {
            DucklakePartitionTransform.YEAR -> temporalValue.date.year - 1970L
            DucklakePartitionTransform.MONTH -> (temporalValue.date.year - 1970L) * 12L + (temporalValue.date.monthValue - 1L)
            DucklakePartitionTransform.DAY -> temporalValue.epochDay
            DucklakePartitionTransform.HOUR -> temporalValue.epochHour
            else -> throw IllegalArgumentException("Unsupported transform for epoch encoding: $transform")
        }
    }

    private fun isPlausibleValueForEncoding(transformedValue: Long, transform: DucklakePartitionTransform, encoding: DucklakeTemporalPartitionEncoding): Boolean {
        if (encoding == EPOCH) {
            return true
        }

        return when (transform) {
            DucklakePartitionTransform.YEAR -> true
            DucklakePartitionTransform.MONTH -> transformedValue in 1..12
            DucklakePartitionTransform.DAY -> transformedValue in 1..31
            DucklakePartitionTransform.HOUR -> transformedValue in 0..23
            else -> false
        }
    }

    @JvmRecord
    private data class TemporalValue(val date: LocalDate, val epochDay: Long, val epochHour: Long, val hourOfDay: Int) {
        companion object {
            @JvmStatic
            fun from(columnType: Type, value: Any): TemporalValue {
                if (columnType == io.trino.spi.type.DateType.DATE) {
                    val epochDay = (value as Number).toLong()
                    val date = LocalDate.ofEpochDay(epochDay)
                    val epochHour = Math.multiplyExact(epochDay, 24L)
                    return TemporalValue(date, epochDay, epochHour, 0)
                }

                val epochMicros = extractEpochMicros(columnType, value)
                val epochHour = Math.floorDiv(epochMicros, 3_600_000_000L)
                val epochDay = Math.floorDiv(epochMicros, 86_400_000_000L)
                val date = LocalDate.ofEpochDay(epochDay)
                val hourOfDay = Math.floorMod(epochHour, 24L).toInt()
                return TemporalValue(date, epochDay, epochHour, hourOfDay)
            }
        }
    }

    private fun extractEpochMicros(columnType: Type, value: Any): Long {
        if (columnType is TimestampType) {
            if (columnType.isShort) {
                return value as Long
            }
            return (value as LongTimestamp).epochMicros
        }

        if (columnType is TimestampWithTimeZoneType) {
            if (columnType.isShort) {
                return unpackMillisUtc(value as Long) * 1_000L
            }

            val longTimestamp = value as LongTimestampWithTimeZone
            return longTimestamp.epochMillis * 1_000L + longTimestamp.picosOfMilli / 1_000_000
        }

        if (value is Number) {
            return value.toLong()
        }

        throw IllegalArgumentException("Unsupported temporal predicate value type: ${value::class.java.name}")
    }
}
