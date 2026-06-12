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
import java.util.Locale
import java.util.Optional
import java.util.regex.Pattern

/**
 * Converts Ducklake type strings to Trino types.
 * Handles all Ducklake primitive, nested, and special types.
 */
open class DucklakeTypeConverter @Inject constructor(private val typeManager: TypeManager) {

    /**
     * Convert Ducklake type string to Trino Type
     */
    open fun toTrinoType(ducklakeType: String): Type {
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
                    throw TrinoException(NOT_SUPPORTED, "Invalid struct field (missing colon): $fieldPart")
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
                throw TrinoException(NOT_SUPPORTED, "Invalid map type (expected 2 type arguments, got ${parts.size}): $ducklakeType")
            }
            val keySignature: TypeSignature = toTrinoType(parts[0].trim()).typeSignature
            val valueSignature: TypeSignature = toTrinoType(parts[1].trim()).typeSignature
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
                    val precision = decimalMatcher.group(1).toInt()
                    val scale = decimalMatcher.group(2).toInt()
                    DecimalType.createDecimalType(precision, scale)
                }
                else {
                    throw TrinoException(NOT_SUPPORTED, "Unsupported Ducklake type: $ducklakeType")
                }
            }
        }
    }

    /**
     * Convert Trino type to Ducklake type string (for write operations)
     */
    open fun toDucklakeType(trinoType: Type): String {
        when (trinoType) {
            BooleanType.BOOLEAN -> return "boolean"
            TinyintType.TINYINT -> return "int8"
            SmallintType.SMALLINT -> return "int16"
            IntegerType.INTEGER -> return "int32"
            BigintType.BIGINT -> return "int64"
            RealType.REAL -> return "float32"
            DoubleType.DOUBLE -> return "float64"
            is DecimalType -> return "decimal(${trinoType.precision},${trinoType.scale})"
            DateType.DATE -> return "date"
            is TimestampType -> {
                return when (trinoType.precision) {
                    0 -> "timestamp_s"
                    3 -> "timestamp_ms"
                    6 -> "timestamp"
                    9 -> "timestamp_ns"
                    // DuckLake only encodes second/milli/micro/nano timestamps; any other Trino
                    // precision (1, 2, 4, 5, 7, 8) has no representable DuckLake type. Fail loudly at
                    // DDL time instead of silently collapsing to micros and reading back as TIMESTAMP(6).
                    else -> throw TrinoException(NOT_SUPPORTED,
                            "Unsupported timestamp precision ${trinoType.precision} for DuckLake write; supported precisions are 0, 3, 6, 9")
                }
            }
            is TimestampWithTimeZoneType -> return "timestamptz"
        }
        // DuckLake time / timetz are microsecond-only. Reject any other declared precision rather
        // than silently coercing to micros (which would read back as TIME(6) / a changed precision).
        if (trinoType is TimeType) {
            if (trinoType.precision != 6) {
                throw TrinoException(NOT_SUPPORTED,
                        "Unsupported time precision ${trinoType.precision} for DuckLake write; only precision 6 (microseconds) is supported")
            }
            return "time"
        }
        if (trinoType is TimeWithTimeZoneType) {
            if (trinoType.precision != 6) {
                throw TrinoException(NOT_SUPPORTED,
                        "Unsupported time with time zone precision ${trinoType.precision} for DuckLake write; only precision 6 (microseconds) is supported")
            }
            return "timetz"
        }
        if (trinoType == VarcharType.VARCHAR || trinoType is VarcharType) {
            return "varchar"
        }
        if (trinoType == VarbinaryType.VARBINARY) {
            return "blob"
        }
        if (trinoType == UuidType.UUID) {
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

        throw TrinoException(NOT_SUPPORTED, "Unsupported Trino type: $trinoType")
    }

    companion object {
        private val DECIMAL_PATTERN: Pattern = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)")

        // Extracts the inner type arguments from a parameterized type string (e.g. the content between the outermost angle brackets)
        private fun extractTypeArguments(typeString: String, prefix: String): String {
            // prefix< ... >
            return typeString.substring(prefix.length + 1, typeString.length - 1)
        }

        // Splits a string at top-level commas, respecting nested angle brackets
        private fun splitTopLevelCommas(input: String): List<String> {
            val parts = mutableListOf<String>()
            var depth = 0
            var start = 0
            var i = 0
            while (i < input.length) {
                val c = input[i]
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
