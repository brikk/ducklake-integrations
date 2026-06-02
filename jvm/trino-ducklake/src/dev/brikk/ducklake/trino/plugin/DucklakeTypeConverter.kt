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

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType
import io.trino.spi.type.StandardTypes
import io.trino.spi.type.TimeType
import io.trino.spi.type.TimeWithTimeZoneType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType
import io.trino.spi.type.Type
import io.trino.spi.type.TypeManager
import io.trino.spi.type.TypeParameter
import io.trino.spi.type.TypeSignature
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType
import java.lang.String.format
import java.util.ArrayList
import java.util.Locale
import java.util.Optional
import java.util.regex.Pattern

/**
 * Converts Ducklake type strings to Trino types.
 * Handles all Ducklake primitive, nested, and special types.
 */
public open class DucklakeTypeConverter @Inject constructor(typeManager: TypeManager) {
    private val typeManager: TypeManager = typeManager

    /**
     * Convert Ducklake type string to Trino Type
     */
    public open fun toTrinoType(ducklakeType: String): Type {
        val normalizedType: String = ducklakeType.trim().lowercase(Locale.ROOT)

        if (normalizedType.startsWith("list<") && normalizedType.endsWith(">")) {
            val elementType = extractTypeArguments(normalizedType, "list").trim()
            return ArrayType(toTrinoType(elementType))
        }

        if (normalizedType.startsWith("struct<") && normalizedType.endsWith(">")) {
            val fieldsStr = extractTypeArguments(normalizedType, "struct")
            val fieldParts: List<String> = splitTopLevelCommas(fieldsStr)
            val fields: ImmutableList.Builder<RowType.Field> = ImmutableList.builder()
            for (fieldPart in fieldParts) {
                val colonIndex = fieldPart.indexOf(':')
                if (colonIndex < 0) {
                    throw TrinoException(NOT_SUPPORTED, format("Invalid struct field (missing colon): %s", fieldPart))
                }
                val fieldName = fieldPart.substring(0, colonIndex).trim()
                val fieldType = fieldPart.substring(colonIndex + 1).trim()
                fields.add(RowType.Field(Optional.of(fieldName), toTrinoType(fieldType)))
            }
            return RowType.from(fields.build())
        }

        if (normalizedType.startsWith("map<") && normalizedType.endsWith(">")) {
            val argsStr = extractTypeArguments(normalizedType, "map")
            val parts: List<String> = splitTopLevelCommas(argsStr)
            if (parts.size != 2) {
                throw TrinoException(NOT_SUPPORTED, format("Invalid map type (expected 2 type arguments, got %d): %s", parts.size, ducklakeType))
            }
            val keySignature: TypeSignature = toTrinoType(parts.get(0).trim()).getTypeSignature()
            val valueSignature: TypeSignature = toTrinoType(parts.get(1).trim()).getTypeSignature()
            return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                    TypeParameter.typeParameter(keySignature),
                    TypeParameter.typeParameter(valueSignature)))
        }

        return when (normalizedType) {
            // Boolean
            "boolean" -> BooleanType.BOOLEAN

            // Signed integers
            "int8" -> TinyintType.TINYINT
            "int16" -> SmallintType.SMALLINT
            "int32" -> IntegerType.INTEGER
            "int64" -> BigintType.BIGINT

            // Unsigned integers - map to next larger signed type
            // TODO: Add validation to ensure values don't exceed signed range
            "uint8" -> SmallintType.SMALLINT  // 0..255 -> -32768..32767
            "uint16" -> IntegerType.INTEGER    // 0..65535 -> -2^31..2^31-1
            "uint32" -> BigintType.BIGINT      // 0..2^32-1 -> -2^63..2^63-1
            "uint64" -> DecimalType.createDecimalType(20, 0) // 0..2^64-1 -> decimal(20,0)

            // 128-bit integers. DuckLake stores these as "int128" / "uint128" (see upstream
            // ducklake_types.cpp: int128 <-> HUGEINT, uint128 <-> UHUGEINT). int128 fits in
            // DECIMAL(38, 0) for all but the extreme edges of its ±1.7e38 range; uint128's
            // 0..3.4e38 range exceeds any Trino decimal, so we degrade it to VARCHAR (data
            // preserved as text, no numeric ops). See dev-docs/COMPARE-pg_ducklake.md B3.
            "int128" -> DecimalType.createDecimalType(38, 0)
            "uint128" -> VarcharType.VARCHAR

            // Floating point
            "float32" -> RealType.REAL
            "float64" -> DoubleType.DOUBLE

            // Temporal types
            "date" -> DateType.DATE
            "time" -> TimeType.TIME_MICROS  // Ducklake uses microsecond precision
            "timetz" -> TimeWithTimeZoneType.TIME_TZ_MICROS
            "timestamp" -> TimestampType.TIMESTAMP_MICROS
            "timestamptz" -> TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
            "timestamp_s" -> TimestampType.createTimestampType(0)   // second precision
            "timestamp_ms" -> TimestampType.createTimestampType(3)  // millisecond precision
            "timestamp_ns" -> TimestampType.createTimestampType(9)  // nanosecond precision

            // String types
            "varchar" -> VarcharType.VARCHAR
            "blob" -> VarbinaryType.VARBINARY
            "uuid" -> UuidType.UUID

            // ⚠️ DEGRADED SEMANTICS - MVP PLACEHOLDERS ONLY
            // These mappings allow basic data access but DO NOT provide full type support:
            // - No type-specific operators or functions
            // - No validation or type safety
            // - Data is accessible but with reduced functionality
            "json" -> VarcharType.VARCHAR  // DEGRADED: No JSON operators/functions, stored as string
            "variant" -> VarcharType.VARCHAR  // DEGRADED: No variant support, needs shredding implementation
            "interval" -> VarcharType.VARCHAR  // DEGRADED: Interval not supported in Trino, stored as string

            // DEGRADED: Geometry types stored as raw bytes, no spatial functions.
            // DuckLake 1.0 renamed "linestring z" to "linestring_z"; we accept both
            // so catalogs written against either spec version read correctly.
            "geometry", "point", "linestring", "polygon",
            "multipoint", "multilinestring", "multipolygon",
            "linestring_z", "linestring z", "geometrycollection" -> VarbinaryType.VARBINARY

            else -> {
                // Check for decimal(P,S)
                val decimalMatcher = DECIMAL_PATTERN.matcher(normalizedType)
                if (decimalMatcher.matches()) {
                    val precision = Integer.parseInt(decimalMatcher.group(1))
                    val scale = Integer.parseInt(decimalMatcher.group(2))
                    DecimalType.createDecimalType(precision, scale)
                }
                else {
                    throw TrinoException(NOT_SUPPORTED, format("Unsupported Ducklake type: %s", ducklakeType))
                }
            }
        }
    }

    /**
     * Convert Trino type to Ducklake type string (for write operations)
     */
    public open fun toDucklakeType(trinoType: Type): String {
        if (trinoType.equals(BooleanType.BOOLEAN)) {
            return "boolean"
        }
        if (trinoType.equals(TinyintType.TINYINT)) {
            return "int8"
        }
        if (trinoType.equals(SmallintType.SMALLINT)) {
            return "int16"
        }
        if (trinoType.equals(IntegerType.INTEGER)) {
            return "int32"
        }
        if (trinoType.equals(BigintType.BIGINT)) {
            return "int64"
        }
        if (trinoType.equals(RealType.REAL)) {
            return "float32"
        }
        if (trinoType.equals(DoubleType.DOUBLE)) {
            return "float64"
        }
        if (trinoType is DecimalType) {
            val decimalType: DecimalType = trinoType
            return format("decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale())
        }
        if (trinoType.equals(DateType.DATE)) {
            return "date"
        }
        if (trinoType is TimestampType) {
            val timestampType: TimestampType = trinoType
            return when (timestampType.getPrecision()) {
                0 -> "timestamp_s"
                3 -> "timestamp_ms"
                6 -> "timestamp"
                9 -> "timestamp_ns"
                else -> "timestamp"
            }
        }
        if (trinoType is TimestampWithTimeZoneType) {
            return "timestamptz"
        }
        if (trinoType is TimeType) {
            return "time"
        }
        if (trinoType is TimeWithTimeZoneType) {
            return "timetz"
        }
        if (trinoType.equals(VarcharType.VARCHAR) || trinoType is VarcharType) {
            return "varchar"
        }
        if (trinoType.equals(VarbinaryType.VARBINARY)) {
            return "blob"
        }
        if (trinoType.equals(UuidType.UUID)) {
            return "uuid"
        }

        // Nested types: DuckLake stores these as parent column type strings;
        // child columns are separate rows linked via parent_column.
        if (trinoType is ArrayType) {
            return "list"
        }
        if (trinoType is RowType) {
            return "struct"
        }
        if (trinoType is MapType) {
            return "map"
        }

        throw TrinoException(NOT_SUPPORTED, format("Unsupported Trino type: %s", trinoType))
    }

    public companion object {
        private val DECIMAL_PATTERN: Pattern = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)")

        // Extracts the inner type arguments from a parameterized type string (e.g. the content between the outermost angle brackets)
        private fun extractTypeArguments(typeString: String, prefix: String): String {
            // prefix< ... >
            return typeString.substring(prefix.length + 1, typeString.length - 1)
        }

        // Splits a string at top-level commas, respecting nested angle brackets
        private fun splitTopLevelCommas(input: String): List<String> {
            val parts: MutableList<String> = ArrayList()
            var depth = 0
            var start = 0
            var i = 0
            while (i < input.length) {
                val c = input.get(i)
                if (c == '<') {
                    depth++
                }
                else if (c == '>') {
                    depth--
                }
                else if (c == ',' && depth == 0) {
                    parts.add(input.substring(start, i).trim())
                    start = i + 1
                }
                i++
            }
            parts.add(input.substring(start).trim())
            return parts
        }
    }
}
