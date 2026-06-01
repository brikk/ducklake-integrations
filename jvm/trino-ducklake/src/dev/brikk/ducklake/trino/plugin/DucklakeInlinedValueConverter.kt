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

import io.airlift.slice.Slices
import io.trino.spi.block.Block
import io.trino.spi.block.BlockBuilder
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.TypeUtils
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeParseException

/**
 * Converts JDBC values from SQLite inlined data tables to Trino-native values
 * compatible with InMemoryRecordSet.
 */
public object DucklakeInlinedValueConverter {

    /**
     * Convert a JDBC value to the representation expected by InMemoryRecordSet for the given Trino type.
     * Returns null for null input.
     */
    @JvmStatic
    fun convertJdbcValue(jdbcValue: Any?, trinoType: Type): Any? {
        if (jdbcValue == null) {
            return null
        }

        if (trinoType is ArrayType) {
            return convertArray(jdbcValue, trinoType)
        }
        if (trinoType.equals(BOOLEAN)) {
            return toBoolean(jdbcValue)
        }
        if (trinoType.equals(TINYINT) || trinoType.equals(SMALLINT) || trinoType.equals(INTEGER) || trinoType.equals(BIGINT)) {
            return toLong(jdbcValue)
        }
        if (trinoType is RealType) {
            return java.lang.Float.floatToIntBits(toFloat(jdbcValue)).toLong()
        }
        if (trinoType.equals(DOUBLE)) {
            return toDouble(jdbcValue)
        }
        if (trinoType is DateType) {
            return toEpochDays(jdbcValue)
        }
        if (trinoType is TimestampType) {
            return toTimestamp(jdbcValue, trinoType)
        }
        if (trinoType is TimestampWithTimeZoneType) {
            return toTimestampWithTimeZone(jdbcValue, trinoType)
        }
        if (trinoType is UuidType) {
            return toUuidSlice(jdbcValue)
        }
        if (trinoType is VarcharType) {
            return Slices.utf8Slice(toStringValue(jdbcValue))
        }
        if (trinoType is VarbinaryType) {
            if (jdbcValue is ByteArray) {
                return Slices.wrappedBuffer(*jdbcValue)
            }
            // Text form (typical for list<blob> elements): DuckDB's Blob::ToString emits printable
            // ASCII (except '\\', '\'', '"') as-is and everything else as `\xNN`. Decode back to bytes.
            return Slices.wrappedBuffer(*decodeBlobText(toStringValue(jdbcValue)))
        }
        if (trinoType is DecimalType) {
            return toDecimal(jdbcValue, trinoType)
        }

        // Fallback: try as string for any remaining types
        return Slices.utf8Slice(toStringValue(jdbcValue))
    }

    private fun convertArray(jdbcValue: Any, arrayType: ArrayType): Block {
        val elementType = arrayType.getElementType()
        if (elementType is ArrayType || elementType is MapType || elementType is RowType) {
            throw UnsupportedOperationException(
                    "Inlined data reads for nested list/struct/map element types are not yet supported " +
                            "(see dev-docs/COMPARE-pg_ducklake.md B2); element type: " + elementType.getDisplayName())
        }

        val elements = extractArrayElements(jdbcValue)
        val builder: BlockBuilder = elementType.createBlockBuilder(null, elements.size)
        for (element in elements) {
            TypeUtils.writeNativeValue(elementType, builder, convertJdbcValue(element, elementType))
        }
        return builder.build()
    }

    // Upstream serializes inlined `list<T>` as VARCHAR text `[a, b, NULL, ...]`
    // (ducklake_util.cpp::ToSQLString LIST branch); `java.sql.Array` is accepted too.
    private fun extractArrayElements(jdbcValue: Any): Array<Any?> {
        if (jdbcValue is java.sql.Array) {
            try {
                val raw = jdbcValue.array
                if (raw is Array<*>) {
                    @Suppress("UNCHECKED_CAST")
                    return raw as Array<Any?>
                }
                throw IllegalArgumentException("Unexpected raw JDBC array contents: " + raw.javaClass.getName())
            }
            catch (e: SQLException) {
                throw IllegalArgumentException("Failed to read JDBC array value", e)
            }
        }
        if (jdbcValue is Array<*>) {
            @Suppress("UNCHECKED_CAST")
            return jdbcValue as Array<Any?>
        }
        if (jdbcValue is String) {
            return parseDucklakeListText(jdbcValue)
        }
        if (jdbcValue is ByteArray) {
            return parseDucklakeListText(String(jdbcValue, StandardCharsets.UTF_8))
        }
        throw IllegalArgumentException("Unexpected array JDBC value: " + jdbcValue.javaClass.getName())
    }

    private fun parseDucklakeListText(raw: String): Array<Any?> {
        val text = raw.trim()
        if (!text.startsWith("[") || !text.endsWith("]")) {
            throw IllegalArgumentException("Invalid inlined list text: " + raw)
        }
        val inner = text.substring(1, text.length - 1).trim()
        if (inner.isEmpty()) {
            return arrayOfNulls(0)
        }

        val elements: MutableList<Any?> = java.util.ArrayList()
        val len = inner.length
        var i = 0
        while (i < len) {
            while (i < len && Character.isWhitespace(inner.get(i))) {
                i++
            }
            if (i >= len) {
                break
            }
            if (inner.get(i) == '\'') {
                val sb = StringBuilder()
                i++
                while (i < len) {
                    val cc = inner.get(i)
                    if (cc == '\\' && i + 1 < len) {
                        sb.append(inner.get(i + 1))
                        i += 2
                        continue
                    }
                    if (cc == '\'') {
                        if (i + 1 < len && inner.get(i + 1) == '\'') {
                            sb.append('\'')
                            i += 2
                            continue
                        }
                        i++
                        break
                    }
                    sb.append(cc)
                    i++
                }
                elements.add(sb.toString())
            }
            else {
                val start = i
                while (i < len && inner.get(i) != ',') {
                    i++
                }
                val token = inner.substring(start, i).trim()
                elements.add(if (token == "NULL") null else token)
            }
            while (i < len && Character.isWhitespace(inner.get(i))) {
                i++
            }
            if (i < len && inner.get(i) == ',') {
                i++
            }
        }
        return elements.toTypedArray()
    }

    /**
     * Decode DuckDB's blob-to-text representation back to raw bytes. Mirrors
     * {@code Blob::ToBlob} in {@code duckdb/src/common/types/blob.cpp}: each
     * {@code \xNN} sequence (uppercase or lowercase hex) becomes one byte; any
     * other ASCII character is taken as a literal byte value. The text form is
     * unambiguous because {@code Blob::ToString} emits {@code \\}, {@code '},
     * {@code "}, and every non-printable byte as {@code \xNN}, so a literal
     * {@code \\} in the text always introduces a hex escape.
     *
     * <p>Visible for testing.
     */
    @JvmStatic
    internal fun decodeBlobText(text: String): ByteArray {
        val len = text.length
        val out = ByteArray(len)
        var j = 0
        var i = 0
        while (i < len) {
            val c = text.get(i)
            if (c == '\\') {
                // DuckDB never emits a bare backslash — 0x5C is always escaped as \x5C — so we
                // require a complete \xNN here. Anything else is malformed input.
                if (i + 3 >= len || text.get(i + 1) != 'x') {
                    throw IllegalArgumentException("Truncated or invalid \\xNN escape in blob text: " + text)
                }
                val hi = hexDigit(text.get(i + 2))
                val lo = hexDigit(text.get(i + 3))
                if (hi < 0 || lo < 0) {
                    throw IllegalArgumentException("Invalid hex digits in \\xNN escape: " + text)
                }
                out[j++] = ((hi shl 4) or lo).toByte()
                i += 4
                continue
            }
            if (c.code > 0x7F) {
                throw IllegalArgumentException("Non-ASCII char 0x" + Integer.toHexString(c.code)
                        + " in blob text — DuckDB escapes these as \\xNN: " + text)
            }
            out[j++] = c.code.toByte()
            i++
        }
        if (j == out.size) {
            return out
        }
        val trimmed = ByteArray(j)
        System.arraycopy(out, 0, trimmed, 0, j)
        return trimmed
    }

    private fun hexDigit(c: Char): Int {
        if (c >= '0' && c <= '9') {
            return c - '0'
        }
        if (c >= 'a' && c <= 'f') {
            return 10 + (c - 'a')
        }
        if (c >= 'A' && c <= 'F') {
            return 10 + (c - 'A')
        }
        return -1
    }

    // DuckDB serializes UUID values as the canonical 36-character text form
    // (e.g. "550e8400-e29b-41d4-a716-446655440000") in inlined data tables.
    // Trino's UuidType expects a 16-byte little/big-endian-packed Slice; build it
    // through the SPI-provided helper so endian handling stays consistent with
    // Trino's other UUID write paths.
    private fun toUuidSlice(value: Any): io.airlift.slice.Slice {
        val parsed = java.util.UUID.fromString(toStringValue(value).trim())
        return UuidType.javaUuidToTrinoUuid(parsed)
    }

    private fun toBoolean(value: Any): Boolean {
        if (value is Boolean) {
            return value
        }
        if (value is Number) {
            return value.toInt() != 0
        }
        return java.lang.Boolean.parseBoolean(toStringValue(value))
    }

    private fun toLong(value: Any): Long {
        if (value is Number) {
            return value.toLong()
        }
        return java.lang.Long.parseLong(toStringValue(value))
    }

    private fun toFloat(value: Any): Float {
        if (value is Number) {
            return value.toFloat()
        }
        return java.lang.Float.parseFloat(toStringValue(value))
    }

    private fun toDouble(value: Any): Double {
        if (value is Number) {
            return value.toDouble()
        }
        return java.lang.Double.parseDouble(toStringValue(value))
    }

    private fun toEpochDays(value: Any): Long {
        if (value is Number) {
            return value.toLong()
        }
        // SQLite may store dates as ISO strings
        return LocalDate.parse(toStringValue(value)).toEpochDay()
    }

    private fun toTimestamp(value: Any, timestampType: TimestampType): Any {
        var epochMicros: Long
        var picosOfMicro = 0

        if (value is Number) {
            epochMicros = value.toLong()
        }
        else {
            val timestamp = parseLocalDateTimeValue(value)
            val epochSecond = timestamp.toEpochSecond(ZoneOffset.UTC)
            val nanosOfSecond = timestamp.getNano()
            epochMicros = epochSecond * 1_000_000 + nanosOfSecond / 1_000
            picosOfMicro = (nanosOfSecond % 1_000) * 1_000
        }

        if (timestampType.isShort()) {
            return epochMicros
        }
        return LongTimestamp(epochMicros, picosOfMicro)
    }

    private fun toTimestampWithTimeZone(value: Any, timestampWithTimeZoneType: TimestampWithTimeZoneType): Any {
        var epochMicros: Long
        var picosOfMicro = 0

        if (value is Number) {
            epochMicros = value.toLong()
        }
        else {
            val timestampWithZone = parseOffsetDateTimeValue(value)
            val epochSecond = timestampWithZone.toEpochSecond()
            val nanosOfSecond = timestampWithZone.getNano()
            epochMicros = epochSecond * 1_000_000 + nanosOfSecond / 1_000
            picosOfMicro = (nanosOfSecond % 1_000) * 1_000
        }

        val epochMillis = Math.floorDiv(epochMicros, 1_000)
        val microsOfMilli = Math.floorMod(epochMicros, 1_000).toInt()
        val picosOfMilli = microsOfMilli * 1_000_000 + picosOfMicro

        if (timestampWithTimeZoneType.isShort()) {
            return packDateTimeWithZone(epochMillis, UTC_KEY)
        }
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY)
    }

    private fun parseLocalDateTimeValue(value: Any): LocalDateTime {
        if (value is LocalDateTime) {
            return value
        }

        val normalized = normalizeTimestampText(toStringValue(value))
        try {
            return LocalDateTime.parse(normalized)
        }
        catch (e: DateTimeParseException) {
            // Some drivers can return offset-bearing text even for timestamp without time zone.
            try {
                return OffsetDateTime.parse(normalized)
                        .atZoneSameInstant(ZoneOffset.UTC)
                        .toLocalDateTime()
            }
            catch (_: DateTimeParseException) {
                throw IllegalArgumentException("Invalid timestamp value: " + value, e)
            }
        }
    }

    private fun parseOffsetDateTimeValue(value: Any): OffsetDateTime {
        if (value is OffsetDateTime) {
            return value
        }

        val normalized = normalizeTimestampText(toStringValue(value))
        try {
            return OffsetDateTime.parse(normalized)
        }
        catch (e: DateTimeParseException) {
            // If no explicit zone is provided, default to UTC for internal normalization.
            try {
                return LocalDateTime.parse(normalized).atOffset(ZoneOffset.UTC)
            }
            catch (_: DateTimeParseException) {
                throw IllegalArgumentException("Invalid timestamp with time zone value: " + value, e)
            }
        }
    }

    private fun normalizeTimestampText(value: String): String {
        var normalized = value.trim().replace(' ', 'T')
        if (normalized.matches(Regex(".*[+-][0-9]{2}$"))) {
            normalized = normalized + ":00"
        }
        if (normalized.matches(Regex(".*[+-][0-9]{4}$"))) {
            normalized = normalized.substring(0, normalized.length - 5) +
                    normalized.substring(normalized.length - 5, normalized.length - 2) +
                    ":" +
                    normalized.substring(normalized.length - 2)
        }
        return normalized
    }

    private fun toDecimal(value: Any, decimalType: DecimalType): Any {
        var decimal: BigDecimal
        if (value is BigDecimal) {
            decimal = value
        }
        else {
            decimal = BigDecimal(toStringValue(value))
        }
        decimal = decimal.setScale(decimalType.getScale(), java.math.RoundingMode.HALF_UP)

        if (decimalType.isShort()) {
            return decimal.unscaledValue().longValueExact()
        }
        return Int128.valueOf(decimal.unscaledValue())
    }

    private fun toStringValue(value: Any): String {
        if (value is ByteArray) {
            return String(value, StandardCharsets.UTF_8)
        }
        return value.toString()
    }
}
