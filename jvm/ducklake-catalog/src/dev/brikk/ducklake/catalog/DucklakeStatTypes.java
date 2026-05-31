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
package dev.brikk.ducklake.catalog;

import java.math.BigDecimal;

/**
 * Canonical DuckLake type vocabulary and the single type-aware comparator/parser
 * for the textual min/max statistics stored in {@code ducklake_file_column_stats}.
 *
 * <p>Stats are stored as strings (the canonical DuckLake textual form). Comparing
 * them lexically is correct for every canonical type <em>except numbers</em>:
 * ISO-8601 dates/times/timestamps sort chronologically as text, {@code "false"}
 * sorts before {@code "true"}, and {@code varchar}/{@code blob}/{@code uuid} are
 * already textual. Numbers are the sole exception — lexically {@code "12" < "8"}
 * and {@code "-3" > "-10"} — so numeric stats must be compared as {@link BigDecimal}.
 *
 * <p>This class is the one place that decision lives. Three call sites route
 * through it so the type vocabulary can never drift apart again:
 * <ul>
 *   <li>{@code JdbcDucklakeCatalog.typedMin/typedMax} — cross-file min/max merge.</li>
 *   <li>{@code JdbcDucklakeCatalog.parseStatValue} — range-prune bound parsing.</li>
 *   <li>{@code DucklakeStatsExtractor.stringMin/stringMax} — per-row-group merge.</li>
 * </ul>
 *
 * <p>The numeric vocabulary covers the full DuckLake spec — including {@code int128}
 * and {@code uint128} (HUGEINT / UHUGEINT), which Trino never emits but which appear
 * in catalogs written by DuckDB directly or via {@code add_files}. {@link BigDecimal}
 * handles their full ±3.4e38 range with no overflow.
 */
public final class DucklakeStatTypes
{
    private DucklakeStatTypes() {}

    /**
     * Whether the given canonical DuckLake type is numeric (integer, unsigned
     * integer, floating point, or decimal) and therefore must be compared
     * numerically rather than lexically.
     */
    public static boolean isNumericType(String canonicalType)
    {
        if (canonicalType == null) {
            return false;
        }
        return switch (normalize(canonicalType)) {
            case "int8", "int16", "int32", "int64", "int128",
                 "uint8", "uint16", "uint32", "uint64", "uint128",
                 "float32", "float64" -> true;
            default -> normalize(canonicalType).startsWith("decimal");
        };
    }

    /**
     * Parse a stored stat string into a {@link Comparable} for range comparison,
     * per its canonical type. Numbers parse to {@link BigDecimal}, booleans to
     * {@link Boolean}; everything else stays a {@link String} (ISO temporal forms
     * and text all order correctly lexically). Returns {@code null} for a
     * {@code null} input or any value that fails to parse — callers treat
     * {@code null} as "unknown, do not prune" to avoid false negatives.
     */
    public static Comparable<?> parseStat(String canonicalType, String value)
    {
        if (value == null) {
            return null;
        }
        try {
            if (isNumericType(canonicalType)) {
                return new BigDecimal(value);
            }
            if ("boolean".equals(normalize(canonicalType))) {
                return parseBoolean(value);
            }
            return value;
        }
        catch (RuntimeException e) {
            // Unparseable: signal "unknown" so the caller skips pruning on this value.
            return null;
        }
    }

    /** Type-aware comparison of two stored stat strings. Lexical for everything but numbers. */
    public static int compare(String a, String b, String canonicalType)
    {
        return compare(a, b, isNumericType(canonicalType));
    }

    /**
     * Core comparison: numeric stats compare as {@link BigDecimal}; all others
     * (and any numeric value that fails to parse) fall back to lexical order.
     */
    public static int compare(String a, String b, boolean numeric)
    {
        if (numeric) {
            try {
                return new BigDecimal(a).compareTo(new BigDecimal(b));
            }
            catch (NumberFormatException ignored) {
                // Non-finite or malformed (e.g. "Infinity", "NaN"): conservative lexical fallback.
            }
        }
        return a.compareTo(b);
    }

    public static String min(String a, String b, String canonicalType)
    {
        return compare(a, b, canonicalType) <= 0 ? a : b;
    }

    public static String max(String a, String b, String canonicalType)
    {
        return compare(a, b, canonicalType) >= 0 ? a : b;
    }

    public static String min(String a, String b, boolean numeric)
    {
        return compare(a, b, numeric) <= 0 ? a : b;
    }

    public static String max(String a, String b, boolean numeric)
    {
        return compare(a, b, numeric) >= 0 ? a : b;
    }

    private static Boolean parseBoolean(String value)
    {
        if (value.equalsIgnoreCase("true") || value.equals("1")) {
            return Boolean.TRUE;
        }
        if (value.equalsIgnoreCase("false") || value.equals("0")) {
            return Boolean.FALSE;
        }
        throw new IllegalArgumentException("Invalid boolean value: " + value);
    }

    private static String normalize(String type)
    {
        return type.trim().toLowerCase(java.util.Locale.ROOT);
    }
}
