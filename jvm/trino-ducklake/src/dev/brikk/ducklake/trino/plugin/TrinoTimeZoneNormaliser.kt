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

/**
 * Translates a Trino `TimeZoneKey` string into a string DuckDB's
 * `SET TimeZone = '...'` will accept. Three rules, derived empirically
 * in `dev-docs/archive/REPORT-datetime-tz-handling.md` (Q3 table) and validated
 * across 14 representative shapes — every named IANA and integer-hour offset
 * matched `java.time`'s ground truth exactly.
 *
 *   - `Z` → `UTC`. `java.time` accepts `Z`;
 *       DuckDB doesn't, but they mean the same thing.
 *   - `±HH:MM` with `MM == 00` → `Etc/GMT∓HH`. POSIX sign
 *       inversion: `+05:00` (UTC+5) becomes `Etc/GMT-5`.
 *   - Everything else — named IANA (`America/Los_Angeles`,
 *       `Asia/Kolkata`), bare `UTC`/`GMT`, AND fractional
 *       bare offsets (`+05:30`) — passes through unchanged. DuckDB
 *       accepts the named cases and cleanly rejects fractional bare offsets
 *       (probed: `Unknown TimeZone '+05:30'!`). The caller decides
 *       what to do on rejection — typically log a one-shot WARN and proceed
 *       without a `SET TimeZone`, which compromises Tier C pushdown
 *       correctness for that attach but leaves Tier A/B untouched.
 *
 * In practice Trino delivers fractional-offset zones via their named IANA
 * counterparts (`Asia/Kolkata`, `America/St_Johns`), so the
 * fractional-bare-offset rejection path is rarely hit.
 */
object TrinoTimeZoneNormaliser {
    /**
     * Normalise a Trino zone identifier to a string DuckDB's
     * `SET TimeZone` is most likely to accept. Returns the original
     * string for any case the rules can't translate; the caller handles
     * DuckDB's actual response.
     */
    fun normalise(trinoZoneId: String?): String? {
        if (trinoZoneId == null) {
            return null
        }
        if ("Z" == trinoZoneId) {
            return "UTC"
        }
        // ±HH:MM with integer-hour offset → Etc/GMT∓HH (POSIX sign inversion).
        // The string is exactly six characters: sign, two digits, ':', two digits.
        if (trinoZoneId.length == 6
                && (trinoZoneId[0] == '+' || trinoZoneId[0] == '-')
                && trinoZoneId[3] == ':'
                && isAsciiDigit(trinoZoneId[1])
                && isAsciiDigit(trinoZoneId[2])
                && isAsciiDigit(trinoZoneId[4])
                && isAsciiDigit(trinoZoneId[5])) {
            val hours = (trinoZoneId[1].code - '0'.code) * 10 + (trinoZoneId[2].code - '0'.code)
            val mins = (trinoZoneId[4].code - '0'.code) * 10 + (trinoZoneId[5].code - '0'.code)
            if (mins == 0) {
                val invertedSign = if (trinoZoneId[0] == '+') '-' else '+'
                return "Etc/GMT$invertedSign$hours"
            }
        }
        return trinoZoneId
    }

    private fun isAsciiDigit(c: Char): Boolean = c in '0'..'9'
}
