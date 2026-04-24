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
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.LongTimestampWithTimeZone;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.CALENDAR;
import static dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.EPOCH;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeTemporalPartitionMatcher
{
    @Test
    public void testStrictCalendarMatchesCalendarYearValue()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "2023",
                domain,
                DucklakePartitionTransform.YEAR,
                CALENDAR,
                false);

        assertThat(matches).isTrue();
    }

    @Test
    public void testStrictEpochRejectsCalendarYearValue()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "2023",
                domain,
                DucklakePartitionTransform.YEAR,
                EPOCH,
                false);

        assertThat(matches).isFalse();
    }

    @Test
    public void testStrictEpochMatchesEpochYearValue()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "53",
                domain,
                DucklakePartitionTransform.YEAR,
                EPOCH,
                false);

        assertThat(matches).isTrue();
    }

    @Test
    public void testLenientModeKeepsAmbiguousMonthWhenEitherEncodingMatches()
    {
        Domain domain = singleDate(LocalDate.of(2023, 1, 15));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "1",
                domain,
                DucklakePartitionTransform.MONTH,
                EPOCH,
                true);

        assertThat(matches).isTrue();
    }

    @Test
    public void testLenientModeKeepsImpossibleCalendarMonthWhenEpochMatches()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "641",
                domain,
                DucklakePartitionTransform.MONTH,
                CALENDAR,
                true);

        assertThat(matches).isTrue();
    }

    @Test
    public void testLenientModePrunesWhenImpossibleCalendarMonthAndEpochDoesNotMatch()
    {
        Domain domain = singleDate(LocalDate.of(2023, 1, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "641",
                domain,
                DucklakePartitionTransform.MONTH,
                CALENDAR,
                true);

        assertThat(matches).isFalse();
    }

    @Test
    public void testLenientModeKeepsAmbiguousDayWhenCalendarMatches()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 15));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "15",
                domain,
                DucklakePartitionTransform.DAY,
                EPOCH,
                true);

        assertThat(matches).isTrue();
    }

    @Test
    public void testLenientModePrunesWhenImpossibleCalendarDayAndEpochDoesNotMatch()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 16));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "19523",
                domain,
                DucklakePartitionTransform.DAY,
                CALENDAR,
                true);

        assertThat(matches).isFalse();
    }

    @Test
    public void testParseFailureDoesNotPrune()
    {
        Domain domain = singleDate(LocalDate.of(2023, 6, 10));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "not-a-number",
                domain,
                DucklakePartitionTransform.YEAR,
                CALENDAR,
                true);

        assertThat(matches).isTrue();
    }

    @Test
    public void testCalendarWrappingMonthRangeDoesNotPrune()
    {
        Domain wrappedRange = dateRange(LocalDate.of(2023, 11, 1), LocalDate.of(2024, 2, 28));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "3",
                wrappedRange,
                DucklakePartitionTransform.MONTH,
                CALENDAR,
                false);

        assertThat(matches).isTrue();
    }

    @Test
    public void testEpochMonthRangePrunesOutOfRangeValue()
    {
        Domain wrappedRange = dateRange(LocalDate.of(2023, 11, 1), LocalDate.of(2024, 2, 28));

        boolean matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                DATE,
                "650",
                wrappedRange,
                DucklakePartitionTransform.MONTH,
                EPOCH,
                false);

        assertThat(matches).isFalse();
    }

    @Test
    public void testTimestampHourSupportsBothEncodingsInLenientMode()
    {
        long tsMicros = 1_704_900_600_000_000L; // 2024-01-10T15:30:00Z
        long epochHours = Math.floorDiv(tsMicros, 3_600_000_000L);
        Domain domain = Domain.singleValue(TIMESTAMP_MICROS, tsMicros);

        boolean calendarValueMatches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MICROS,
                "15",
                domain,
                DucklakePartitionTransform.HOUR,
                EPOCH,
                true);
        boolean epochValueMatches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MICROS,
                String.valueOf(epochHours),
                domain,
                DucklakePartitionTransform.HOUR,
                CALENDAR,
                true);

        assertThat(calendarValueMatches).isTrue();
        assertThat(epochValueMatches).isTrue();
    }

    @Test
    public void testTimestampTzMicrosCalendarYearMatch()
    {
        // 2026-03-07T00:00:00Z
        long epochMillis = 1_772_841_600_000L;
        LongTimestampWithTimeZone value = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                epochMillis, 0, UTC_KEY);
        Domain domain = Domain.singleValue(TIMESTAMP_TZ_MICROS, value);

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "2026", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "3", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        // Wrong day should not match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testTimestampTzMicrosEpochDayMatch()
    {
        // 2026-03-07T00:00:00Z
        long epochMillis = 1_772_841_600_000L;
        long epochDay = epochMillis / 86_400_000L;
        LongTimestampWithTimeZone value = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                epochMillis, 0, UTC_KEY);
        Domain domain = Domain.singleValue(TIMESTAMP_TZ_MICROS, value);

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, String.valueOf(epochDay), domain, DucklakePartitionTransform.DAY, EPOCH, false))
                .isTrue();
        // Wrong epoch day
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, String.valueOf(epochDay + 1), domain, DucklakePartitionTransform.DAY, EPOCH, false))
                .isFalse();
    }

    @Test
    public void testTimestampTzMillisCalendarMatch()
    {
        // 2026-03-07T12:00:00Z as short timestamp with timezone (millis packed with zone)
        long epochMillis = 1_772_841_600_000L + 43_200_000L; // noon UTC on 2026-03-07
        long packed = packDateTimeWithZone(epochMillis, UTC_KEY);
        Domain domain = Domain.singleValue(TIMESTAMP_TZ_MILLIS, packed);

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MILLIS, "2026", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MILLIS, "3", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MILLIS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MILLIS, "12", domain, DucklakePartitionTransform.HOUR, CALENDAR, false))
                .isTrue();
        // Wrong month
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MILLIS, "4", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testTimestampTzMicrosRangePruning()
    {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z) — exclusive high at midnight boundary
        long startMillis = 1_772_841_600_000L;
        long endMillis = startMillis + 86_400_000L;
        LongTimestampWithTimeZone low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                startMillis, 0, UTC_KEY);
        LongTimestampWithTimeZone high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                endMillis, 0, UTC_KEY);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
                false);

        // Day 7 should match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        // Day 8 should NOT match — exclusive high at midnight means no data in day 8 qualifies
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isFalse();
        // Day 6 should not match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "6", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isFalse();
        // Year 2025 should not match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "2025", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testTimestampTzMicrosExclusiveHighAtMidnightBoundaryEpoch()
    {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z) with epoch encoding
        long startMillis = 1_772_841_600_000L;
        long endMillis = startMillis + 86_400_000L;
        long epochDay7 = startMillis / 86_400_000L;
        long epochDay8 = endMillis / 86_400_000L;
        LongTimestampWithTimeZone low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                startMillis, 0, UTC_KEY);
        LongTimestampWithTimeZone high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                endMillis, 0, UTC_KEY);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
                false);

        // Epoch day 7 should match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, String.valueOf(epochDay7), domain, DucklakePartitionTransform.DAY, EPOCH, false))
                .isTrue();
        // Epoch day 8 should NOT match — exclusive high at midnight boundary
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, String.valueOf(epochDay8), domain, DucklakePartitionTransform.DAY, EPOCH, false))
                .isFalse();
    }

    @Test
    public void testTimestampTzMicrosExclusiveHighNotAtBoundaryKeepsPartition()
    {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T12:00:00Z) — exclusive high at NOON, not a day boundary
        long startMillis = 1_772_841_600_000L;
        long endMillis = startMillis + 86_400_000L + 43_200_000L; // noon on March 8
        LongTimestampWithTimeZone low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                startMillis, 0, UTC_KEY);
        LongTimestampWithTimeZone high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                endMillis, 0, UTC_KEY);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
                false);

        // Day 7 should match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        // Day 8 SHOULD match — exclusive high at noon means day 8 has data before noon
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        // Day 9 should not match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "9", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testTimestampMicrosExclusiveHighAtBoundary()
    {
        // TIMESTAMP_MICROS (no timezone): [2026-03-07T00:00:00, 2026-03-08T00:00:00)
        long lowMicros = 1_772_841_600_000_000L;    // 2026-03-07 00:00:00
        long highMicros = lowMicros + 86_400_000_000L; // 2026-03-08 00:00:00
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_MICROS, lowMicros, true, highMicros, false)),
                false);

        // Day 7 matches
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        // Day 8 should NOT match — exclusive high at midnight
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testTimestampMillisExclusiveHighAtBoundary()
    {
        // TIMESTAMP_MILLIS (no timezone): [2026-03-07T00:00:00, 2026-03-08T00:00:00)
        long lowMicros = 1_772_841_600_000_000L;
        long highMicros = lowMicros + 86_400_000_000L;
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_MILLIS, lowMicros, true, highMicros, false)),
                false);

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MILLIS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MILLIS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testTimestampTzMillisExclusiveHighAtBoundary()
    {
        // TIMESTAMP_TZ_MILLIS: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z)
        long startMillis = 1_772_841_600_000L;
        long endMillis = startMillis + 86_400_000L;
        long packedLow = packDateTimeWithZone(startMillis, UTC_KEY);
        long packedHigh = packDateTimeWithZone(endMillis, UTC_KEY);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MILLIS, packedLow, true, packedHigh, false)),
                false);

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MILLIS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MILLIS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testExclusiveHighAtHourBoundary()
    {
        // [2026-03-07T14:00:00Z, 2026-03-07T15:00:00Z) — exclusive high at hour boundary
        long startMillis = 1_772_841_600_000L + 14 * 3_600_000L;
        long endMillis = startMillis + 3_600_000L;
        LongTimestampWithTimeZone low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                startMillis, 0, UTC_KEY);
        LongTimestampWithTimeZone high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                endMillis, 0, UTC_KEY);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
                false);

        // Hour 14 matches
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "14", domain, DucklakePartitionTransform.HOUR, CALENDAR, false))
                .isTrue();
        // Hour 15 should NOT match — exclusive high at hour boundary
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "15", domain, DucklakePartitionTransform.HOUR, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testExclusiveHighAtMonthBoundary()
    {
        // [2026-03-01T00:00:00Z, 2026-04-01T00:00:00Z) — exclusive high at month boundary
        long marchFirst = 1_772_323_200_000L; // 2026-03-01 00:00:00 UTC
        long aprilFirst = marchFirst + 31L * 86_400_000L; // 2026-04-01 00:00:00 UTC
        LongTimestampWithTimeZone low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                marchFirst, 0, UTC_KEY);
        LongTimestampWithTimeZone high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                aprilFirst, 0, UTC_KEY);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
                false);

        // Month 3 (March) matches
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "3", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
                .isTrue();
        // Month 4 (April) should NOT match — exclusive high at month boundary
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "4", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
                .isFalse();
    }

    @Test
    public void testInclusiveHighAtBoundaryKeepsPartition()
    {
        // [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z] — INCLUSIVE high at midnight
        long startMillis = 1_772_841_600_000L;
        long endMillis = startMillis + 86_400_000L;
        LongTimestampWithTimeZone low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                startMillis, 0, UTC_KEY);
        LongTimestampWithTimeZone high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                endMillis, 0, UTC_KEY);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, true)),
                false);

        // Day 7 matches
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
        // Day 8 SHOULD match — inclusive high means midnight of March 8 is included
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_TZ_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
                .isTrue();
    }

    @Test
    public void testTimestampMillisCalendarMatch()
    {
        // TIMESTAMP_MILLIS (no timezone) - 2023-06-10T14:00:00Z in micros
        long tsMicros = 1_686_405_600_000_000L;
        Domain domain = Domain.singleValue(TIMESTAMP_MILLIS, tsMicros);

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MILLIS, "2023", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MILLIS, "6", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
                .isTrue();
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
                TIMESTAMP_MILLIS, "2024", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
                .isFalse();
    }

    private static Domain singleDate(LocalDate date)
    {
        return Domain.singleValue(DATE, date.toEpochDay());
    }

    private static Domain dateRange(LocalDate low, LocalDate high)
    {
        return Domain.create(
                ValueSet.ofRanges(Range.range(DATE, low.toEpochDay(), true, high.toEpochDay(), true)),
                false);
    }
}
