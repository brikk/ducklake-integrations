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

import dev.brikk.ducklake.catalog.DucklakePartitionTransform;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.LocalDate;

import static dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.CALENDAR;
import static dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.EPOCH;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;

final class DucklakeTemporalPartitionMatcher
{
    private DucklakeTemporalPartitionMatcher() {}

    public static boolean partitionValueMatchesDomain(
            Type columnType,
            String partitionValue,
            Domain domain,
            DucklakePartitionTransform transform,
            DucklakeTemporalPartitionEncoding configuredEncoding,
            boolean readLeniency)
    {
        try {
            if (domain.isNone()) {
                return false;
            }
            if (domain.getValues().isAll()) {
                return true;
            }

            long transformedValue = Long.parseLong(partitionValue);
            if (!readLeniency) {
                return partitionValueMatchesDomainForEncoding(columnType, transformedValue, domain, transform, configuredEncoding);
            }

            if (partitionValueMatchesDomainForEncoding(columnType, transformedValue, domain, transform, configuredEncoding)) {
                return true;
            }

            DucklakeTemporalPartitionEncoding fallbackEncoding = (configuredEncoding == CALENDAR) ? EPOCH : CALENDAR;
            return partitionValueMatchesDomainForEncoding(columnType, transformedValue, domain, transform, fallbackEncoding);
        }
        catch (RuntimeException _) {
            // Never prune on parse/transform failures to avoid false negatives.
            return true;
        }
    }

    private static boolean partitionValueMatchesDomainForEncoding(
            Type columnType,
            long transformedValue,
            Domain domain,
            DucklakePartitionTransform transform,
            DucklakeTemporalPartitionEncoding encoding)
    {
        if (!isPlausibleValueForEncoding(transformedValue, transform, encoding)) {
            return false;
        }

        return domain.getValues().getValuesProcessor().transform(
                ranges -> {
                    for (Range range : ranges.getOrderedRanges()) {
                        if (temporalRangeContainsTransformedValue(columnType, range, transformedValue, transform, encoding)) {
                            return true;
                        }
                    }
                    return false;
                },
                discreteValues -> {
                    for (Object value : discreteValues.getValues()) {
                        long transformed = applyTemporalTransform(columnType, value, transform, encoding);
                        if (transformed == transformedValue) {
                            return true;
                        }
                    }
                    return false;
                },
                allOrNone -> true);
    }

    private static boolean temporalRangeContainsTransformedValue(
            Type columnType,
            Range range,
            long transformedValue,
            DucklakePartitionTransform transform,
            DucklakeTemporalPartitionEncoding encoding)
    {
        long lowTransformed = range.getLowValue()
                .map(v -> applyTemporalTransform(columnType, v, transform, encoding))
                .orElse(Long.MIN_VALUE);
        long highTransformed = range.getHighValue()
                .map(v -> applyTemporalTransform(columnType, v, transform, encoding))
                .orElse(Long.MAX_VALUE);

        // For non-DATE types, Trino does not normalize exclusive bounds to inclusive
        // (DATE is discrete so Trino converts e.g. `< DATE '2026-03-08'` to `<= DATE '2026-03-07'`).
        // When an exclusive high bound falls exactly on a partition transform boundary
        // (e.g. `< TIMESTAMP '2026-03-08 00:00:00'` with DAY transform), no data in that
        // partition can satisfy the predicate, so we must adjust the effective bound.
        if (!columnType.equals(io.trino.spi.type.DateType.DATE) &&
                range.getHighValue().isPresent() && !range.isHighInclusive()) {
            long epochMicros = extractEpochMicros(columnType, range.getHighValue().get());
            if (isAtTransformBoundary(epochMicros, transform)) {
                highTransformed--;
            }
        }

        // Calendar month/day/hour transforms are not monotonic over broad ranges.
        // In wrapping cases, skip pruning to avoid false negatives.
        if (encoding == CALENDAR &&
                (transform == DucklakePartitionTransform.MONTH || transform == DucklakePartitionTransform.DAY || transform == DucklakePartitionTransform.HOUR) &&
                lowTransformed > highTransformed) {
            return true;
        }

        return transformedValue >= lowTransformed && transformedValue <= highTransformed;
    }

    private static boolean isAtTransformBoundary(long epochMicros, DucklakePartitionTransform transform)
    {
        return switch (transform) {
            case HOUR -> epochMicros % 3_600_000_000L == 0;
            case DAY -> epochMicros % 86_400_000_000L == 0;
            case MONTH -> {
                if (epochMicros % 86_400_000_000L != 0) {
                    yield false;
                }
                LocalDate date = LocalDate.ofEpochDay(epochMicros / 86_400_000_000L);
                yield date.getDayOfMonth() == 1;
            }
            case YEAR -> {
                if (epochMicros % 86_400_000_000L != 0) {
                    yield false;
                }
                LocalDate date = LocalDate.ofEpochDay(epochMicros / 86_400_000_000L);
                yield date.getMonthValue() == 1 && date.getDayOfMonth() == 1;
            }
            default -> false;
        };
    }

    private static long applyTemporalTransform(Type columnType, Object value, DucklakePartitionTransform transform, DucklakeTemporalPartitionEncoding encoding)
    {
        TemporalValue temporalValue = TemporalValue.from(columnType, value);

        if (encoding == CALENDAR) {
            return switch (transform) {
                case YEAR -> temporalValue.date().getYear();
                case MONTH -> temporalValue.date().getMonthValue();
                case DAY -> temporalValue.date().getDayOfMonth();
                case HOUR -> temporalValue.hourOfDay();
                default -> throw new IllegalArgumentException("Unsupported transform for calendar encoding: " + transform);
            };
        }

        return switch (transform) {
            case YEAR -> temporalValue.date().getYear() - 1970L;
            case MONTH -> (temporalValue.date().getYear() - 1970L) * 12L + (temporalValue.date().getMonthValue() - 1L);
            case DAY -> temporalValue.epochDay();
            case HOUR -> temporalValue.epochHour();
            default -> throw new IllegalArgumentException("Unsupported transform for epoch encoding: " + transform);
        };
    }

    private static boolean isPlausibleValueForEncoding(long transformedValue, DucklakePartitionTransform transform, DucklakeTemporalPartitionEncoding encoding)
    {
        if (encoding == EPOCH) {
            return true;
        }

        return switch (transform) {
            case YEAR -> true;
            case MONTH -> transformedValue >= 1 && transformedValue <= 12;
            case DAY -> transformedValue >= 1 && transformedValue <= 31;
            case HOUR -> transformedValue >= 0 && transformedValue <= 23;
            default -> false;
        };
    }

    private record TemporalValue(LocalDate date, long epochDay, long epochHour, int hourOfDay)
    {
        private static TemporalValue from(Type columnType, Object value)
        {
            if (columnType.equals(io.trino.spi.type.DateType.DATE)) {
                long epochDay = ((Number) value).longValue();
                LocalDate date = LocalDate.ofEpochDay(epochDay);
                long epochHour = Math.multiplyExact(epochDay, 24L);
                return new TemporalValue(date, epochDay, epochHour, 0);
            }

            long epochMicros = extractEpochMicros(columnType, value);
            long epochHour = Math.floorDiv(epochMicros, 3_600_000_000L);
            long epochDay = Math.floorDiv(epochMicros, 86_400_000_000L);
            LocalDate date = LocalDate.ofEpochDay(epochDay);
            int hourOfDay = (int) Math.floorMod(epochHour, 24L);
            return new TemporalValue(date, epochDay, epochHour, hourOfDay);
        }
    }

    private static long extractEpochMicros(Type columnType, Object value)
    {
        if (columnType instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return (long) value;
            }
            return ((LongTimestamp) value).getEpochMicros();
        }

        if (columnType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.isShort()) {
                return unpackMillisUtc((long) value) * 1_000L;
            }

            LongTimestampWithTimeZone longTimestamp = (LongTimestampWithTimeZone) value;
            return longTimestamp.getEpochMillis() * 1_000L + longTimestamp.getPicosOfMilli() / 1_000_000;
        }

        if (value instanceof Number number) {
            return number.longValue();
        }

        throw new IllegalArgumentException("Unsupported temporal predicate value type: " + value.getClass().getName());
    }
}
