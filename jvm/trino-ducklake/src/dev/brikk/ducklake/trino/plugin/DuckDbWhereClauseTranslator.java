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

import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;

/**
 * Best-effort translation of a Trino {@link TupleDomain} into a DuckDB SQL {@code WHERE}
 * clause for predicate pushdown into the {@code .db} reader. Anything we don't know how
 * to translate safely is simply omitted — Trino's own filter operator runs on top, so
 * partial pushdown is always correct (just less selective).
 *
 * <p>Phase 1 type coverage matches the reader/writer scalar set: BOOLEAN, integer
 * family, REAL, DOUBLE, DECIMAL, DATE, short-precision TIMESTAMP, VARCHAR. Other types
 * (TIMESTAMPTZ, VARBINARY, UUID, nested) are intentionally skipped here even when
 * supported on the read path; they fall back to Trino-side filtering.
 */
final class DuckDbWhereClauseTranslator
{
    private static final DateTimeFormatter TIMESTAMP_MICROS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT);

    private DuckDbWhereClauseTranslator() {}

    /**
     * Translate a tuple domain into a DuckDB SQL boolean expression. Returns
     * {@link Optional#empty()} when the domain is "all" (no predicate to push) or when
     * nothing is translatable.
     */
    static Optional<String> toWhereClause(TupleDomain<DucklakeColumnHandle> predicate)
    {
        if (predicate.isAll()) {
            return Optional.empty();
        }
        if (predicate.isNone()) {
            return Optional.of("FALSE");
        }
        Optional<Map<DucklakeColumnHandle, Domain>> domainsOpt = predicate.getDomains();
        if (domainsOpt.isEmpty()) {
            return Optional.empty();
        }
        List<String> conjuncts = new ArrayList<>();
        for (Map.Entry<DucklakeColumnHandle, Domain> entry : domainsOpt.get().entrySet()) {
            translateDomain(entry.getKey(), entry.getValue()).ifPresent(conjuncts::add);
        }
        if (conjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.join(" AND ", conjuncts));
    }

    private static Optional<String> translateDomain(DucklakeColumnHandle column, Domain domain)
    {
        String name = quoteIdentifier(column.columnName());
        Type type = column.columnType();

        if (domain.isAll()) {
            return Optional.empty();
        }
        if (domain.isNone()) {
            return Optional.of("FALSE");
        }

        ValueSet values = domain.getValues();
        boolean nullAllowed = domain.isNullAllowed();

        if (values.isNone()) {
            // Only NULL or nothing.
            return Optional.of(nullAllowed ? name + " IS NULL" : "FALSE");
        }
        if (values.isAll()) {
            // Any non-null value is fine; honor null disposition.
            return Optional.of(nullAllowed ? "TRUE" : name + " IS NOT NULL");
        }

        Optional<String> valuesTerm = translateValues(name, type, values);
        if (valuesTerm.isEmpty()) {
            return Optional.empty();
        }
        if (nullAllowed) {
            return Optional.of("(" + valuesTerm.get() + " OR " + name + " IS NULL)");
        }
        return valuesTerm;
    }

    private static Optional<String> translateValues(String name, Type type, ValueSet values)
    {
        if (values.isDiscreteSet()) {
            List<Object> discrete = values.getDiscreteSet();
            List<String> literals = new ArrayList<>(discrete.size());
            for (Object v : discrete) {
                Optional<String> lit = formatLiteral(type, v);
                if (lit.isEmpty()) {
                    return Optional.empty();
                }
                literals.add(lit.get());
            }
            if (literals.size() == 1) {
                return Optional.of(name + " = " + literals.getFirst());
            }
            return Optional.of(name + " IN (" + String.join(", ", literals) + ")");
        }

        // Range-based value sets — sortedRangeSet exposes ordered ranges.
        try {
            List<Range> ranges = values.getRanges().getOrderedRanges();
            List<String> terms = new ArrayList<>(ranges.size());
            for (Range r : ranges) {
                Optional<String> rangeTerm = formatRange(name, type, r);
                if (rangeTerm.isEmpty()) {
                    return Optional.empty();
                }
                terms.add(rangeTerm.get());
            }
            if (terms.isEmpty()) {
                return Optional.empty();
            }
            if (terms.size() == 1) {
                return Optional.of(terms.getFirst());
            }
            return Optional.of("(" + String.join(" OR ", terms) + ")");
        }
        catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
    }

    private static Optional<String> formatRange(String name, Type type, Range r)
    {
        if (r.isSingleValue()) {
            Optional<String> lit = formatLiteral(type, r.getSingleValue());
            return lit.map(l -> name + " = " + l);
        }

        StringBuilder sb = new StringBuilder();
        if (!r.isLowUnbounded()) {
            Optional<String> lit = formatLiteral(type, r.getLowBoundedValue());
            if (lit.isEmpty()) {
                return Optional.empty();
            }
            sb.append(name).append(r.isLowInclusive() ? " >= " : " > ").append(lit.get());
        }
        if (!r.isHighUnbounded()) {
            if (!sb.isEmpty()) {
                sb.append(" AND ");
            }
            Optional<String> lit = formatLiteral(type, r.getHighBoundedValue());
            if (lit.isEmpty()) {
                return Optional.empty();
            }
            sb.append(name).append(r.isHighInclusive() ? " <= " : " < ").append(lit.get());
        }
        if (sb.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of("(" + sb + ")");
    }

    private static Optional<String> formatLiteral(Type type, Object value)
    {
        if (value == null) {
            return Optional.empty();
        }
        if (type.equals(BOOLEAN)) {
            return Optional.of(((Boolean) value) ? "TRUE" : "FALSE");
        }
        if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            // Trino stores all integer-family values as long.
            return Optional.of(((Long) value).toString());
        }
        if (type.equals(REAL)) {
            // Trino REAL is encoded as the float bits in a long.
            float f = Float.intBitsToFloat(((Long) value).intValue());
            if (Float.isFinite(f)) {
                return Optional.of(Float.toString(f));
            }
            return Optional.empty();
        }
        if (type.equals(DOUBLE)) {
            double d = (Double) value;
            if (Double.isFinite(d)) {
                return Optional.of(Double.toString(d));
            }
            return Optional.empty();
        }
        if (type instanceof DecimalType decimalType) {
            BigDecimal bd;
            if (decimalType.isShort()) {
                bd = BigDecimal.valueOf((Long) value, decimalType.getScale());
            }
            else {
                Int128 i128 = (Int128) value;
                bd = new BigDecimal(new BigInteger(i128.toBigEndianBytes()), decimalType.getScale());
            }
            return Optional.of(bd.toPlainString());
        }
        if (type.equals(DATE)) {
            long days = (Long) value;
            return Optional.of("DATE '" + LocalDate.ofEpochDay(days) + "'");
        }
        if (type instanceof TimestampType timestampType && timestampType.isShort()) {
            long micros = (Long) value;
            long epochSeconds = floorDiv(micros, 1_000_000L);
            int nanoOfSecond = (int) (floorMod(micros, 1_000_000L) * 1_000L);
            LocalDateTime ldt = LocalDateTime.ofEpochSecond(epochSeconds, nanoOfSecond, ZoneOffset.UTC);
            return Optional.of("TIMESTAMP '" + TIMESTAMP_MICROS.format(ldt) + "'");
        }
        if (type instanceof VarcharType) {
            Slice slice = (Slice) value;
            return Optional.of("'" + slice.toStringUtf8().replace("'", "''") + "'");
        }
        return Optional.empty();
    }

    private static String quoteIdentifier(String name)
    {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    static String formatLiteralOrThrow(Type type, Object value)
    {
        return formatLiteral(type, value)
                .orElseThrow(() -> new IllegalArgumentException(format("Cannot format %s value as DuckDB literal", type)));
    }
}
