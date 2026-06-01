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

/**
 * Translates a Trino {@code TimeZoneKey} string into a string DuckDB's
 * {@code SET TimeZone = '...'} will accept. Three rules, derived empirically
 * in {@code dev-docs/archive/REPORT-datetime-tz-handling.md} (Q3 table) and validated
 * across 14 representative shapes — every named IANA and integer-hour offset
 * matched {@code java.time}'s ground truth exactly.
 *
 * <ol>
 *   <li>{@code Z} → {@code UTC}. {@code java.time} accepts {@code Z};
 *       DuckDB doesn't, but they mean the same thing.</li>
 *   <li>{@code ±HH:MM} with {@code MM == 00} → {@code Etc/GMT∓HH}. POSIX sign
 *       inversion: {@code +05:00} (UTC+5) becomes {@code Etc/GMT-5}.</li>
 *   <li>Everything else — named IANA ({@code America/Los_Angeles},
 *       {@code Asia/Kolkata}), bare {@code UTC}/{@code GMT}, AND fractional
 *       bare offsets ({@code +05:30}) — passes through unchanged. DuckDB
 *       accepts the named cases and cleanly rejects fractional bare offsets
 *       (probed: {@code Unknown TimeZone '+05:30'!}). The caller decides
 *       what to do on rejection — typically log a one-shot WARN and proceed
 *       without a {@code SET TimeZone}, which compromises Tier C pushdown
 *       correctness for that attach but leaves Tier A/B untouched.</li>
 * </ol>
 *
 * <p>In practice Trino delivers fractional-offset zones via their named IANA
 * counterparts ({@code Asia/Kolkata}, {@code America/St_Johns}), so the
 * fractional-bare-offset rejection path is rarely hit.
 */
final class TrinoTimeZoneNormaliser
{
    private TrinoTimeZoneNormaliser() {}

    /**
     * Normalise a Trino zone identifier to a string DuckDB's
     * {@code SET TimeZone} is most likely to accept. Returns the original
     * string for any case the rules can't translate; the caller handles
     * DuckDB's actual response.
     */
    static String normalise(String trinoZoneId)
    {
        if (trinoZoneId == null) {
            return null;
        }
        if ("Z".equals(trinoZoneId)) {
            return "UTC";
        }
        // ±HH:MM with integer-hour offset → Etc/GMT∓HH (POSIX sign inversion).
        // The string is exactly six characters: sign, two digits, ':', two digits.
        if (trinoZoneId.length() == 6
                && (trinoZoneId.charAt(0) == '+' || trinoZoneId.charAt(0) == '-')
                && trinoZoneId.charAt(3) == ':'
                && isAsciiDigit(trinoZoneId.charAt(1))
                && isAsciiDigit(trinoZoneId.charAt(2))
                && isAsciiDigit(trinoZoneId.charAt(4))
                && isAsciiDigit(trinoZoneId.charAt(5))) {
            int hours = (trinoZoneId.charAt(1) - '0') * 10 + (trinoZoneId.charAt(2) - '0');
            int mins = (trinoZoneId.charAt(4) - '0') * 10 + (trinoZoneId.charAt(5) - '0');
            if (mins == 0) {
                char invertedSign = (trinoZoneId.charAt(0) == '+') ? '-' : '+';
                return "Etc/GMT" + invertedSign + hours;
            }
        }
        return trinoZoneId;
    }

    private static boolean isAsciiDigit(char c)
    {
        return c >= '0' && c <= '9';
    }
}
