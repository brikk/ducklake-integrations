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

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Converts JDBC values from SQLite inlined data tables to Trino-native values
 * compatible with InMemoryRecordSet.
 */
public final class DucklakeInlinedValueConverter
{
    private DucklakeInlinedValueConverter() {}

    /**
     * Convert a JDBC value to the representation expected by InMemoryRecordSet for the given Trino type.
     * Returns null for null input.
     */
    public static Object convertJdbcValue(Object jdbcValue, Type trinoType)
    {
        if (jdbcValue == null) {
            return null;
        }

        if (trinoType instanceof ArrayType arrayType) {
            return convertArray(jdbcValue, arrayType);
        }
        if (trinoType.equals(BOOLEAN)) {
            return toBoolean(jdbcValue);
        }
        if (trinoType.equals(TINYINT) || trinoType.equals(SMALLINT) || trinoType.equals(INTEGER) || trinoType.equals(BIGINT)) {
            return toLong(jdbcValue);
        }
        if (trinoType instanceof RealType) {
            return (long) Float.floatToIntBits(toFloat(jdbcValue));
        }
        if (trinoType.equals(DOUBLE)) {
            return toDouble(jdbcValue);
        }
        if (trinoType instanceof DateType) {
            return toEpochDays(jdbcValue);
        }
        if (trinoType instanceof TimestampType timestampType) {
            return toTimestamp(jdbcValue, timestampType);
        }
        if (trinoType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return toTimestampWithTimeZone(jdbcValue, timestampWithTimeZoneType);
        }
        if (trinoType instanceof VarcharType) {
            return Slices.utf8Slice(toStringValue(jdbcValue));
        }
        if (trinoType instanceof VarbinaryType) {
            if (jdbcValue instanceof byte[] bytes) {
                return Slices.wrappedBuffer(bytes);
            }
            return Slices.wrappedBuffer(toStringValue(jdbcValue).getBytes(StandardCharsets.UTF_8));
        }
        if (trinoType instanceof DecimalType decimalType) {
            return toDecimal(jdbcValue, decimalType);
        }

        // Fallback: try as string for any remaining types
        return Slices.utf8Slice(toStringValue(jdbcValue));
    }

    private static Block convertArray(Object jdbcValue, ArrayType arrayType)
    {
        Type elementType = arrayType.getElementType();
        if (elementType instanceof ArrayType || elementType instanceof MapType || elementType instanceof RowType) {
            throw new UnsupportedOperationException(
                    "Inlined data reads for nested list/struct/map element types are not yet supported " +
                            "(see TODO-compatibility.md B2); element type: " + elementType.getDisplayName());
        }

        Object[] elements = extractArrayElements(jdbcValue);
        BlockBuilder builder = elementType.createBlockBuilder(null, elements.length);
        for (Object element : elements) {
            TypeUtils.writeNativeValue(elementType, builder, convertJdbcValue(element, elementType));
        }
        return builder.build();
    }

    // Upstream serializes inlined `list<T>` as VARCHAR text `[a, b, NULL, ...]`
    // (ducklake_util.cpp::ToSQLString LIST branch); `java.sql.Array` is accepted too.
    private static Object[] extractArrayElements(Object jdbcValue)
    {
        if (jdbcValue instanceof java.sql.Array array) {
            try {
                Object raw = array.getArray();
                if (raw instanceof Object[] objects) {
                    return objects;
                }
                throw new IllegalArgumentException("Unexpected raw JDBC array contents: " + raw.getClass().getName());
            }
            catch (SQLException e) {
                throw new IllegalArgumentException("Failed to read JDBC array value", e);
            }
        }
        if (jdbcValue instanceof Object[] objects) {
            return objects;
        }
        if (jdbcValue instanceof String text) {
            return parseDucklakeListText(text);
        }
        if (jdbcValue instanceof byte[] bytes) {
            return parseDucklakeListText(new String(bytes, StandardCharsets.UTF_8));
        }
        throw new IllegalArgumentException("Unexpected array JDBC value: " + jdbcValue.getClass().getName());
    }

    private static Object[] parseDucklakeListText(String raw)
    {
        String text = raw.trim();
        if (!text.startsWith("[") || !text.endsWith("]")) {
            throw new IllegalArgumentException("Invalid inlined list text: " + raw);
        }
        String inner = text.substring(1, text.length() - 1).trim();
        if (inner.isEmpty()) {
            return new Object[0];
        }

        java.util.List<Object> elements = new java.util.ArrayList<>();
        int len = inner.length();
        int i = 0;
        while (i < len) {
            while (i < len && Character.isWhitespace(inner.charAt(i))) {
                i++;
            }
            if (i >= len) {
                break;
            }
            if (inner.charAt(i) == '\'') {
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < len) {
                    char cc = inner.charAt(i);
                    if (cc == '\\' && i + 1 < len) {
                        sb.append(inner.charAt(i + 1));
                        i += 2;
                        continue;
                    }
                    if (cc == '\'') {
                        if (i + 1 < len && inner.charAt(i + 1) == '\'') {
                            sb.append('\'');
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    sb.append(cc);
                    i++;
                }
                elements.add(sb.toString());
            }
            else {
                int start = i;
                while (i < len && inner.charAt(i) != ',') {
                    i++;
                }
                String token = inner.substring(start, i).trim();
                elements.add(token.equals("NULL") ? null : token);
            }
            while (i < len && Character.isWhitespace(inner.charAt(i))) {
                i++;
            }
            if (i < len && inner.charAt(i) == ',') {
                i++;
            }
        }
        return elements.toArray();
    }

    private static Boolean toBoolean(Object value)
    {
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof Number n) {
            return n.intValue() != 0;
        }
        return Boolean.parseBoolean(toStringValue(value));
    }

    private static long toLong(Object value)
    {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(toStringValue(value));
    }

    private static float toFloat(Object value)
    {
        if (value instanceof Number n) {
            return n.floatValue();
        }
        return Float.parseFloat(toStringValue(value));
    }

    private static double toDouble(Object value)
    {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return Double.parseDouble(toStringValue(value));
    }

    private static long toEpochDays(Object value)
    {
        if (value instanceof Number n) {
            return n.longValue();
        }
        // SQLite may store dates as ISO strings
        return LocalDate.parse(toStringValue(value)).toEpochDay();
    }

    private static Object toTimestamp(Object value, TimestampType timestampType)
    {
        long epochMicros;
        int picosOfMicro = 0;

        if (value instanceof Number n) {
            epochMicros = n.longValue();
        }
        else {
            LocalDateTime timestamp = parseLocalDateTimeValue(value);
            long epochSecond = timestamp.toEpochSecond(ZoneOffset.UTC);
            int nanosOfSecond = timestamp.getNano();
            epochMicros = epochSecond * 1_000_000 + nanosOfSecond / 1_000;
            picosOfMicro = (nanosOfSecond % 1_000) * 1_000;
        }

        if (timestampType.isShort()) {
            return epochMicros;
        }
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    private static Object toTimestampWithTimeZone(Object value, TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        long epochMicros;
        int picosOfMicro = 0;

        if (value instanceof Number n) {
            epochMicros = n.longValue();
        }
        else {
            OffsetDateTime timestampWithZone = parseOffsetDateTimeValue(value);
            long epochSecond = timestampWithZone.toEpochSecond();
            int nanosOfSecond = timestampWithZone.getNano();
            epochMicros = epochSecond * 1_000_000 + nanosOfSecond / 1_000;
            picosOfMicro = (nanosOfSecond % 1_000) * 1_000;
        }

        long epochMillis = Math.floorDiv(epochMicros, 1_000);
        int microsOfMilli = (int) Math.floorMod(epochMicros, 1_000);
        int picosOfMilli = microsOfMilli * 1_000_000 + picosOfMicro;

        if (timestampWithTimeZoneType.isShort()) {
            return packDateTimeWithZone(epochMillis, UTC_KEY);
        }
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY);
    }

    private static LocalDateTime parseLocalDateTimeValue(Object value)
    {
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime;
        }

        String normalized = normalizeTimestampText(toStringValue(value));
        try {
            return LocalDateTime.parse(normalized);
        }
        catch (DateTimeParseException e) {
            // Some drivers can return offset-bearing text even for timestamp without time zone.
            try {
                return OffsetDateTime.parse(normalized)
                        .atZoneSameInstant(ZoneOffset.UTC)
                        .toLocalDateTime();
            }
            catch (DateTimeParseException _) {
                throw new IllegalArgumentException("Invalid timestamp value: " + value, e);
            }
        }
    }

    private static OffsetDateTime parseOffsetDateTimeValue(Object value)
    {
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime;
        }

        String normalized = normalizeTimestampText(toStringValue(value));
        try {
            return OffsetDateTime.parse(normalized);
        }
        catch (DateTimeParseException e) {
            // If no explicit zone is provided, default to UTC for internal normalization.
            try {
                return LocalDateTime.parse(normalized).atOffset(ZoneOffset.UTC);
            }
            catch (DateTimeParseException _) {
                throw new IllegalArgumentException("Invalid timestamp with time zone value: " + value, e);
            }
        }
    }

    private static String normalizeTimestampText(String value)
    {
        String normalized = value.trim().replace(' ', 'T');
        if (normalized.matches(".*[+-][0-9]{2}$")) {
            normalized = normalized + ":00";
        }
        if (normalized.matches(".*[+-][0-9]{4}$")) {
            normalized = normalized.substring(0, normalized.length() - 5)
                    + normalized.substring(normalized.length() - 5, normalized.length() - 2)
                    + ":"
                    + normalized.substring(normalized.length() - 2);
        }
        return normalized;
    }

    private static Object toDecimal(Object value, DecimalType decimalType)
    {
        BigDecimal decimal;
        if (value instanceof BigDecimal bd) {
            decimal = bd;
        }
        else {
            decimal = new BigDecimal(toStringValue(value));
        }
        decimal = decimal.setScale(decimalType.getScale(), java.math.RoundingMode.HALF_UP);

        if (decimalType.isShort()) {
            return decimal.unscaledValue().longValueExact();
        }
        return Int128.valueOf(decimal.unscaledValue());
    }

    private static String toStringValue(Object value)
    {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }
}
