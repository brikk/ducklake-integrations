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
package dev.brikk.duckbridge.trino.plugin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pins the [TrinoTimeZoneNormaliser] three-rule contract documented in
 * `dev-docs/archive/REPORT-datetime-tz-handling.md` (Q3 table). The 14 cases
 * here are the same set the `ProbeDuckDbTimeZoneHandling#probeQ3b…`
 * probe validated against `java.time` as ground truth — every
 * MATCH/DUCK-FAIL row in that table corresponds to an assertion below.
 */
class TestTrinoTimeZoneNormaliser {
    // --- Named IANA zones — pass through unchanged ---------------------------

    @Test
    fun testNamedIanaPassesThrough() {
        assertThat(TrinoTimeZoneNormaliser.normalise("America/Los_Angeles"))
                .isEqualTo("America/Los_Angeles")
        assertThat(TrinoTimeZoneNormaliser.normalise("Europe/Berlin"))
                .isEqualTo("Europe/Berlin")
        assertThat(TrinoTimeZoneNormaliser.normalise("Asia/Singapore"))
                .isEqualTo("Asia/Singapore")
    }

    @Test
    fun testNamedIanaWithFractionalOffsetPassesThrough() {
        // Named zones that are fractional-offset internally (UTC+05:30, UTC+12:45)
        // still resolve via DuckDB's bundled tzdb; the normaliser doesn't have to
        // know they're fractional, the name is enough.
        assertThat(TrinoTimeZoneNormaliser.normalise("Asia/Kolkata"))
                .isEqualTo("Asia/Kolkata")
        assertThat(TrinoTimeZoneNormaliser.normalise("Pacific/Chatham"))
                .isEqualTo("Pacific/Chatham")
    }

    @Test
    fun testUtcAndGmtPassThrough() {
        assertThat(TrinoTimeZoneNormaliser.normalise("UTC")).isEqualTo("UTC")
        assertThat(TrinoTimeZoneNormaliser.normalise("GMT")).isEqualTo("GMT")
    }

    // --- `Z` → `UTC` ---------------------------------------------------------

    @Test
    fun testZAliasesToUtc() {
        // java.time accepts `Z` as a synonym for UTC; DuckDB doesn't, but `UTC`
        // produces the same semantics.
        assertThat(TrinoTimeZoneNormaliser.normalise("Z")).isEqualTo("UTC")
    }

    // --- Integer-hour fixed offsets → Etc/GMT∓N (POSIX sign inversion) ------

    @Test
    fun testPositiveOffsetGetsInvertedToNegativeEtcGmt() {
        // +05:00 means UTC+5 → Etc/GMT-5 in POSIX-style zone names.
        assertThat(TrinoTimeZoneNormaliser.normalise("+05:00")).isEqualTo("Etc/GMT-5")
        assertThat(TrinoTimeZoneNormaliser.normalise("+14:00")).isEqualTo("Etc/GMT-14")
    }

    @Test
    fun testNegativeOffsetGetsInvertedToPositiveEtcGmt() {
        // -08:00 means UTC-8 → Etc/GMT+8.
        assertThat(TrinoTimeZoneNormaliser.normalise("-08:00")).isEqualTo("Etc/GMT+8")
        assertThat(TrinoTimeZoneNormaliser.normalise("-12:00")).isEqualTo("Etc/GMT+12")
    }

    @Test
    fun testZeroOffsetTranslatesToEtcGmtZero() {
        // +00:00 normalises to Etc/GMT-0. DuckDB accepts both Etc/GMT-0 and
        // Etc/GMT0 as UTC; either result is correct.
        assertThat(TrinoTimeZoneNormaliser.normalise("+00:00")).isEqualTo("Etc/GMT-0")
    }

    // --- Fractional bare offsets — pass through (DuckDB will reject) --------

    @Test
    fun testFractionalPositiveOffsetPassesThroughForDuckDbToReject() {
        // +05:30 (India) cannot be expressed as Etc/GMT-N (integer hours only).
        // The normaliser passes it through; DuckDB will respond with a clean
        // "Unknown TimeZone" error and the caller decides what to do. Trino's
        // actual TimeZoneKey for India arrives as `Asia/Kolkata` in practice,
        // so this rejection path is rarely hit.
        assertThat(TrinoTimeZoneNormaliser.normalise("+05:30")).isEqualTo("+05:30")
    }

    @Test
    fun testFractionalNegativeOffsetPassesThrough() {
        // -03:30 (Newfoundland) — same reasoning.
        assertThat(TrinoTimeZoneNormaliser.normalise("-03:30")).isEqualTo("-03:30")
    }

    // --- Defensive cases — null, malformed, off-by-one shapes ---------------

    @Test
    fun testNullStaysNull() {
        // The session might not provide a zone (no Tier C correctness needed);
        // null in, null out, callers branch on it.
        assertThat(TrinoTimeZoneNormaliser.normalise(null)).isNull()
    }

    @Test
    fun testWrongLengthOffsetShapeDoesNotMatch() {
        // 5 chars (`+5:00`), 7 chars (`+05:000`) — neither matches the strict
        // 6-char `±HH:MM` shape; pass through and let DuckDB reject.
        assertThat(TrinoTimeZoneNormaliser.normalise("+5:00")).isEqualTo("+5:00")
        assertThat(TrinoTimeZoneNormaliser.normalise("+05:000")).isEqualTo("+05:000")
    }

    @Test
    fun testNonDigitInOffsetShapeDoesNotMatch() {
        // Length 6 and signs/colon right, but the digit positions hold non-digits.
        // Don't false-positive — pass through unchanged.
        assertThat(TrinoTimeZoneNormaliser.normalise("+0x:00")).isEqualTo("+0x:00")
        assertThat(TrinoTimeZoneNormaliser.normalise("+05:0x")).isEqualTo("+05:0x")
    }

    @Test
    fun testEmptyStringPassesThrough() {
        // No special-case for empty; DuckDB will reject it with a clear error.
        assertThat(TrinoTimeZoneNormaliser.normalise("")).isEqualTo("")
    }
}
