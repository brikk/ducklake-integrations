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
import io.trino.spi.predicate.ValueSet
import io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.LocalDate

class TestDucklakeTemporalPartitionMatcher {
    @Test
    fun testStrictCalendarMatchesCalendarYearValue() {
        val domain = singleDate(LocalDate.of(2023, 6, 10))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "2023",
            domain,
            DucklakePartitionTransform.YEAR,
            CALENDAR,
            false)

        assertThat(matches).isTrue()
    }

    @Test
    fun testStrictEpochRejectsCalendarYearValue() {
        val domain = singleDate(LocalDate.of(2023, 6, 10))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "2023",
            domain,
            DucklakePartitionTransform.YEAR,
            EPOCH,
            false)

        assertThat(matches).isFalse()
    }

    @Test
    fun testStrictEpochMatchesEpochYearValue() {
        val domain = singleDate(LocalDate.of(2023, 6, 10))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "53",
            domain,
            DucklakePartitionTransform.YEAR,
            EPOCH,
            false)

        assertThat(matches).isTrue()
    }

    @Test
    fun testLenientModeKeepsAmbiguousMonthWhenEitherEncodingMatches() {
        val domain = singleDate(LocalDate.of(2023, 1, 15))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "1",
            domain,
            DucklakePartitionTransform.MONTH,
            EPOCH,
            true)

        assertThat(matches).isTrue()
    }

    @Test
    fun testLenientModeKeepsImpossibleCalendarMonthWhenEpochMatches() {
        val domain = singleDate(LocalDate.of(2023, 6, 10))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "641",
            domain,
            DucklakePartitionTransform.MONTH,
            CALENDAR,
            true)

        assertThat(matches).isTrue()
    }

    @Test
    fun testLenientModePrunesWhenImpossibleCalendarMonthAndEpochDoesNotMatch() {
        val domain = singleDate(LocalDate.of(2023, 1, 10))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "641",
            domain,
            DucklakePartitionTransform.MONTH,
            CALENDAR,
            true)

        assertThat(matches).isFalse()
    }

    @Test
    fun testLenientModeKeepsAmbiguousDayWhenCalendarMatches() {
        val domain = singleDate(LocalDate.of(2023, 6, 15))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "15",
            domain,
            DucklakePartitionTransform.DAY,
            EPOCH,
            true)

        assertThat(matches).isTrue()
    }

    @Test
    fun testLenientModePrunesWhenImpossibleCalendarDayAndEpochDoesNotMatch() {
        val domain = singleDate(LocalDate.of(2023, 6, 16))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "19523",
            domain,
            DucklakePartitionTransform.DAY,
            CALENDAR,
            true)

        assertThat(matches).isFalse()
    }

    @Test
    fun testParseFailureDoesNotPrune() {
        val domain = singleDate(LocalDate.of(2023, 6, 10))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "not-a-number",
            domain,
            DucklakePartitionTransform.YEAR,
            CALENDAR,
            true)

        assertThat(matches).isTrue()
    }

    @Test
    fun testCalendarWrappingMonthRangeDoesNotPrune() {
        val wrappedRange = dateRange(LocalDate.of(2023, 11, 1), LocalDate.of(2024, 2, 28))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "3",
            wrappedRange,
            DucklakePartitionTransform.MONTH,
            CALENDAR,
            false)

        assertThat(matches).isTrue()
    }

    @Test
    fun testEpochMonthRangePrunesOutOfRangeValue() {
        val wrappedRange = dateRange(LocalDate.of(2023, 11, 1), LocalDate.of(2024, 2, 28))

        val matches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            DATE,
            "650",
            wrappedRange,
            DucklakePartitionTransform.MONTH,
            EPOCH,
            false)

        assertThat(matches).isFalse()
    }

    @Test
    fun testTimestampHourSupportsBothEncodingsInLenientMode() {
        val tsMicros = 1_704_900_600_000_000L // 2024-01-10T15:30:00Z
        val epochHours = Math.floorDiv(tsMicros, 3_600_000_000L)
        val domain = Domain.singleValue(TIMESTAMP_MICROS, tsMicros)

        val calendarValueMatches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MICROS,
            "15",
            domain,
            DucklakePartitionTransform.HOUR,
            EPOCH,
            true)
        val epochValueMatches = DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MICROS,
            epochHours.toString(),
            domain,
            DucklakePartitionTransform.HOUR,
            CALENDAR,
            true)

        assertThat(calendarValueMatches).isTrue()
        assertThat(epochValueMatches).isTrue()
    }

    @Test
    fun testTimestampTzMicrosCalendarYearMatch() {
        // 2026-03-07T00:00:00Z
        val epochMillis = 1_772_841_600_000L
        val value = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            epochMillis, 0, UTC_KEY)
        val domain = Domain.singleValue(TIMESTAMP_TZ_MICROS, value)

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "2026", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "3", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        // Wrong day should not match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testTimestampTzMicrosEpochDayMatch() {
        // 2026-03-07T00:00:00Z
        val epochMillis = 1_772_841_600_000L
        val epochDay = epochMillis / 86_400_000L
        val value = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            epochMillis, 0, UTC_KEY)
        val domain = Domain.singleValue(TIMESTAMP_TZ_MICROS, value)

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, epochDay.toString(), domain, DucklakePartitionTransform.DAY, EPOCH, false))
            .isTrue()
        // Wrong epoch day
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, (epochDay + 1).toString(), domain, DucklakePartitionTransform.DAY, EPOCH, false))
            .isFalse()
    }

    @Test
    fun testTimestampTzMillisCalendarMatch() {
        // 2026-03-07T12:00:00Z as short timestamp with timezone (millis packed with zone)
        val epochMillis = 1_772_841_600_000L + 43_200_000L // noon UTC on 2026-03-07
        val packed = packDateTimeWithZone(epochMillis, UTC_KEY)
        val domain = Domain.singleValue(TIMESTAMP_TZ_MILLIS, packed)

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MILLIS, "2026", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MILLIS, "3", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MILLIS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MILLIS, "12", domain, DucklakePartitionTransform.HOUR, CALENDAR, false))
            .isTrue()
        // Wrong month
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MILLIS, "4", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testTimestampTzMicrosRangePruning() {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z) — exclusive high at midnight boundary
        val startMillis = 1_772_841_600_000L
        val endMillis = startMillis + 86_400_000L
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            startMillis, 0, UTC_KEY)
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            endMillis, 0, UTC_KEY)
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
            false)

        // Day 7 should match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        // Day 8 should NOT match — exclusive high at midnight means no data in day 8 qualifies
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isFalse()
        // Day 6 should not match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "6", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isFalse()
        // Year 2025 should not match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "2025", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testTimestampTzMicrosExclusiveHighAtMidnightBoundaryEpoch() {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z) with epoch encoding
        val startMillis = 1_772_841_600_000L
        val endMillis = startMillis + 86_400_000L
        val epochDay7 = startMillis / 86_400_000L
        val epochDay8 = endMillis / 86_400_000L
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            startMillis, 0, UTC_KEY)
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            endMillis, 0, UTC_KEY)
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
            false)

        // Epoch day 7 should match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, epochDay7.toString(), domain, DucklakePartitionTransform.DAY, EPOCH, false))
            .isTrue()
        // Epoch day 8 should NOT match — exclusive high at midnight boundary
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, epochDay8.toString(), domain, DucklakePartitionTransform.DAY, EPOCH, false))
            .isFalse()
    }

    @Test
    fun testTimestampTzMicrosExclusiveHighNotAtBoundaryKeepsPartition() {
        // Range: [2026-03-07T00:00:00Z, 2026-03-08T12:00:00Z) — exclusive high at NOON, not a day boundary
        val startMillis = 1_772_841_600_000L
        val endMillis = startMillis + 86_400_000L + 43_200_000L // noon on March 8
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            startMillis, 0, UTC_KEY)
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            endMillis, 0, UTC_KEY)
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
            false)

        // Day 7 should match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        // Day 8 SHOULD match — exclusive high at noon means day 8 has data before noon
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        // Day 9 should not match
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "9", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testTimestampMicrosExclusiveHighAtBoundary() {
        // TIMESTAMP_MICROS (no timezone): [2026-03-07T00:00:00, 2026-03-08T00:00:00)
        val lowMicros = 1_772_841_600_000_000L    // 2026-03-07 00:00:00
        val highMicros = lowMicros + 86_400_000_000L // 2026-03-08 00:00:00
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_MICROS, lowMicros, true, highMicros, false)),
            false)

        // Day 7 matches
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        // Day 8 should NOT match — exclusive high at midnight
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testTimestampMillisExclusiveHighAtBoundary() {
        // TIMESTAMP_MILLIS (no timezone): [2026-03-07T00:00:00, 2026-03-08T00:00:00)
        val lowMicros = 1_772_841_600_000_000L
        val highMicros = lowMicros + 86_400_000_000L
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_MILLIS, lowMicros, true, highMicros, false)),
            false)

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MILLIS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MILLIS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testTimestampTzMillisExclusiveHighAtBoundary() {
        // TIMESTAMP_TZ_MILLIS: [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z)
        val startMillis = 1_772_841_600_000L
        val endMillis = startMillis + 86_400_000L
        val packedLow = packDateTimeWithZone(startMillis, UTC_KEY)
        val packedHigh = packDateTimeWithZone(endMillis, UTC_KEY)
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MILLIS, packedLow, true, packedHigh, false)),
            false)

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MILLIS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MILLIS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testExclusiveHighAtHourBoundary() {
        // [2026-03-07T14:00:00Z, 2026-03-07T15:00:00Z) — exclusive high at hour boundary
        val startMillis = 1_772_841_600_000L + 14 * 3_600_000L
        val endMillis = startMillis + 3_600_000L
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            startMillis, 0, UTC_KEY)
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            endMillis, 0, UTC_KEY)
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
            false)

        // Hour 14 matches
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "14", domain, DucklakePartitionTransform.HOUR, CALENDAR, false))
            .isTrue()
        // Hour 15 should NOT match — exclusive high at hour boundary
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "15", domain, DucklakePartitionTransform.HOUR, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testExclusiveHighAtMonthBoundary() {
        // [2026-03-01T00:00:00Z, 2026-04-01T00:00:00Z) — exclusive high at month boundary
        val marchFirst = 1_772_323_200_000L // 2026-03-01 00:00:00 UTC
        val aprilFirst = marchFirst + 31L * 86_400_000L // 2026-04-01 00:00:00 UTC
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            marchFirst, 0, UTC_KEY)
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            aprilFirst, 0, UTC_KEY)
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, false)),
            false)

        // Month 3 (March) matches
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "3", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
            .isTrue()
        // Month 4 (April) should NOT match — exclusive high at month boundary
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "4", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
            .isFalse()
    }

    @Test
    fun testInclusiveHighAtBoundaryKeepsPartition() {
        // [2026-03-07T00:00:00Z, 2026-03-08T00:00:00Z] — INCLUSIVE high at midnight
        val startMillis = 1_772_841_600_000L
        val endMillis = startMillis + 86_400_000L
        val low = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            startMillis, 0, UTC_KEY)
        val high = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
            endMillis, 0, UTC_KEY)
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(TIMESTAMP_TZ_MICROS, low, true, high, true)),
            false)

        // Day 7 matches
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "7", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
        // Day 8 SHOULD match — inclusive high means midnight of March 8 is included
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_TZ_MICROS, "8", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
    }

    @Test
    fun testTimestampMillisCalendarMatch() {
        // TIMESTAMP_MILLIS (no timezone) - 2023-06-10T14:00:00Z in micros
        val tsMicros = 1_686_405_600_000_000L
        val domain = Domain.singleValue(TIMESTAMP_MILLIS, tsMicros)

        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MILLIS, "2023", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MILLIS, "6", domain, DucklakePartitionTransform.MONTH, CALENDAR, false))
            .isTrue()
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MILLIS, "2024", domain, DucklakePartitionTransform.YEAR, CALENDAR, false))
            .isFalse()
    }

    // ==================== CALENDAR non-monotonic / open-range pruning (B4) ====================
    // Under CALENDAR encoding the sub-year transforms (MONTH/DAY/HOUR) are only monotonic
    // within one parent period. An open-ended or multi-period datetime range wraps the
    // transformed value, so range pruning must be skipped to avoid silently dropping files.

    @Test
    fun testCalendarHourUnboundedLowAtMidnightDoesNotPruneAll() {
        // `ts < 2026-03-08 00:00:00` with HOUR transform: the exclusive-high boundary
        // adjustment decremented the transformed hour to -1, so every partition hour
        // (0..23) was wrongly pruned. Hour 10 of an earlier day satisfies the predicate.
        val domain = Domain.create(
            ValueSet.ofRanges(Range.lessThan(TIMESTAMP_MICROS, tsMicros(2026, 3, 8, 0))),
            false)
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MICROS, "10", domain, DucklakePartitionTransform.HOUR, CALENDAR, false))
            .isTrue()
    }

    @Test
    fun testCalendarDayUnboundedLowAtMonthStartDoesNotPruneAll() {
        // `ts < 2026-03-01 00:00:00` with DAY transform: exclusive high decremented the
        // transformed day to 0, so every partition day (1..31) was wrongly pruned.
        val domain = Domain.create(
            ValueSet.ofRanges(Range.lessThan(TIMESTAMP_MICROS, tsMicros(2026, 3, 1, 0))),
            false)
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MICROS, "15", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
    }

    @Test
    fun testCalendarDayUnboundedHighDoesNotPrune() {
        // `ts >= 2026-03-08` with DAY transform: high is unbounded so day-of-month wraps
        // across months; day 5 (e.g. 2026-04-05) must not be pruned.
        val domain = Domain.create(
            ValueSet.ofRanges(Range.greaterThanOrEqual(TIMESTAMP_MICROS, tsMicros(2026, 3, 8, 0))),
            false)
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MICROS, "5", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
    }

    @Test
    fun testCalendarDayMultiMonthRangeDoesNotPrune() {
        // Bounded but spanning Feb..Apr: day-of-month is non-monotonic across the span,
        // so day 25 (e.g. 2026-02-25 or 2026-03-25) must not be pruned.
        val domain = Domain.create(
            ValueSet.ofRanges(Range.range(
                TIMESTAMP_MICROS, tsMicros(2026, 2, 8, 0), true, tsMicros(2026, 4, 20, 0), false)),
            false)
        assertThat(DucklakeTemporalPartitionMatcher.partitionValueMatchesDomain(
            TIMESTAMP_MICROS, "25", domain, DucklakePartitionTransform.DAY, CALENDAR, false))
            .isTrue()
    }

    companion object {
        private fun tsMicros(year: Int, month: Int, day: Int, hour: Int): Long {
            return LocalDate.of(year, month, day).toEpochDay() * 86_400_000_000L + hour * 3_600_000_000L
        }

        private fun singleDate(date: LocalDate): Domain {
            return Domain.singleValue(DATE, date.toEpochDay())
        }

        private fun dateRange(low: LocalDate, high: LocalDate): Domain {
            return Domain.create(
                ValueSet.ofRanges(Range.range(DATE, low.toEpochDay(), true, high.toEpochDay(), true)),
                false)
        }
    }
}
