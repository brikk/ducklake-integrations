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
package dev.brikk.ducklake.catalog

import java.math.BigDecimal
import java.util.Locale

/**
 * Canonical DuckLake type vocabulary and the single type-aware comparator/parser
 * for the textual min/max statistics stored in `ducklake_file_column_stats`.
 *
 * Stats are stored as strings (the canonical DuckLake textual form). Comparing
 * them lexically is correct for every canonical type *except numbers*:
 * ISO-8601 dates/times/timestamps sort chronologically as text, `"false"`
 * sorts before `"true"`, and `varchar`/`blob`/`uuid` are already textual.
 * Numbers are the sole exception — lexically `"12" < "8"` and `"-3" > "-10"`
 * — so numeric stats must be compared as [BigDecimal].
 *
 * This class is the one place that decision lives. Three call sites route
 * through it so the type vocabulary can never drift apart again:
 *  - `JdbcDucklakeCatalog.typedMin/typedMax` — cross-file min/max merge.
 *  - `JdbcDucklakeCatalog.parseStatValue` — range-prune bound parsing.
 *  - `DucklakeStatsExtractor.stringMin/stringMax` — per-row-group merge.
 *
 * The numeric vocabulary covers the full DuckLake spec — including `int128`
 * and `uint128` (HUGEINT / UHUGEINT), which Trino never emits but which appear
 * in catalogs written by DuckDB directly or via `add_files`. [BigDecimal]
 * handles their full ±3.4e38 range with no overflow.
 */
object DucklakeStatTypes {
    /**
     * Whether the given canonical DuckLake type is numeric (integer, unsigned
     * integer, floating point, or decimal) and therefore must be compared
     * numerically rather than lexically.
     */
    fun isNumericType(canonicalType: String?): Boolean {
        if (canonicalType == null) {
            return false
        }
        return when (normalize(canonicalType)) {
            "int8", "int16", "int32", "int64", "int128",
            "uint8", "uint16", "uint32", "uint64", "uint128",
            "float32", "float64" -> true
            else -> normalize(canonicalType).startsWith("decimal")
        }
    }

    /**
     * Parse a stored stat string into a [Comparable] for range comparison,
     * per its canonical type. Numbers parse to [BigDecimal], booleans to
     * [Boolean]; everything else stays a [String] (ISO temporal forms
     * and text all order correctly lexically). Returns `null` for a
     * `null` input or any value that fails to parse — callers treat
     * `null` as "unknown, do not prune" to avoid false negatives.
     */
    fun parseStat(canonicalType: String?, value: String?): Comparable<*>? {
        if (value == null) {
            return null
        }
        return try {
            if (isNumericType(canonicalType)) {
                BigDecimal(value)
            }
            else if ("boolean" == normalize(canonicalType!!)) {
                parseBoolean(value)
            }
            else {
                value
            }
        }
        catch (e: RuntimeException) {
            // Unparseable: signal "unknown" so the caller skips pruning on this value.
            null
        }
    }

    /** Type-aware comparison of two stored stat strings. Lexical for everything but numbers. */
    fun compare(a: String, b: String, canonicalType: String?): Int =
        compare(a, b, isNumericType(canonicalType))

    /**
     * Core comparison: numeric stats compare as [BigDecimal]; all others
     * (and any numeric value that fails to parse) fall back to lexical order.
     */
    fun compare(a: String, b: String, numeric: Boolean): Int {
        if (numeric) {
            try {
                return BigDecimal(a).compareTo(BigDecimal(b))
            }
            catch (ignored: NumberFormatException) {
                // Non-finite or malformed (e.g. "Infinity", "NaN"): conservative lexical fallback.
            }
        }
        return a.compareTo(b)
    }

    @JvmStatic
    fun min(a: String, b: String, canonicalType: String?): String =
        if (compare(a, b, canonicalType) <= 0) a else b

    @JvmStatic
    fun max(a: String, b: String, canonicalType: String?): String =
        if (compare(a, b, canonicalType) >= 0) a else b

    @JvmStatic
    fun min(a: String, b: String, numeric: Boolean): String =
        if (compare(a, b, numeric) <= 0) a else b

    @JvmStatic
    fun max(a: String, b: String, numeric: Boolean): String =
        if (compare(a, b, numeric) >= 0) a else b

    private fun parseBoolean(value: String): Boolean {
        if (value.equals("true", ignoreCase = true) || value == "1") {
            return true
        }
        if (value.equals("false", ignoreCase = true) || value == "0") {
            return false
        }
        throw IllegalArgumentException("Invalid boolean value: $value")
    }

    private fun normalize(type: String): String {
        return type.trim().lowercase(Locale.ROOT)
    }
}
