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

import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType
import java.lang.String.format
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.util.Locale
import java.util.Optional

/**
 * Shared mapping helpers for the two DuckDB writers (file-based and Arrow-stream). These were
 * previously duplicated verbatim in each writer and had begun to drift cosmetically; hoisting
 * them here keeps a single source of truth. Behavior is identical to the prior copies — the only
 * per-writer difference (the unsupported-type error prefix) is passed in as [writerLabel].
 */
internal object DuckDbWriterSupport {

    /** Map a Trino [Type] to its DuckDB SQL type keyword for `CREATE TABLE`. */
    fun toDuckDbSqlType(type: Type, writerLabel: String): String {
        when (type) {
            BOOLEAN -> return "BOOLEAN"
            TINYINT -> return "TINYINT"
            SMALLINT -> return "SMALLINT"
            INTEGER -> return "INTEGER"
            BIGINT -> return "BIGINT"
            REAL -> return "REAL"
            DOUBLE -> return "DOUBLE"
            DATE -> return "DATE"
            VARBINARY -> return "BLOB"
            is DecimalType -> return format(Locale.ROOT, "DECIMAL(%d,%d)", type.precision, type.scale)
            is TimestampType -> {
                return when (type.precision) {
                    0 -> "TIMESTAMP_S"
                    3 -> "TIMESTAMP_MS"
                    6 -> "TIMESTAMP"
                    9 -> "TIMESTAMP_NS"
                    else -> "TIMESTAMP"
                }
            }
            is TimestampWithTimeZoneType -> return "TIMESTAMPTZ"
            is VarcharType -> return "VARCHAR"
            UuidType.UUID -> return "UUID"
        }
        if (DucklakeJsonSupport.isJson(type)) {
            return "JSON"
        }
        return toDuckDbComplexSqlType(type, writerLabel)
    }

    /** The complex (ARRAY/ROW/MAP) tail of [toDuckDbSqlType] — recursion shares one source of
     * truth with the scalar mapping. */
    private fun toDuckDbComplexSqlType(type: Type, writerLabel: String): String {
        if (type is ArrayType) {
            return toDuckDbSqlType(type.elementType, writerLabel) + "[]"
        }
        if (type is RowType) {
            return type.fields.mapIndexed { idx, f ->
                '"' + f.name.orElse("f$idx").replace("\"", "\"\"") + "\" " + toDuckDbSqlType(f.type, writerLabel)
            }.joinToString(", ", "STRUCT(", ")")
        }
        if (type is MapType) {
            return "MAP(${toDuckDbSqlType(type.keyType, writerLabel)}, ${toDuckDbSqlType(type.valueType, writerLabel)})"
        }
        throw TrinoException(NOT_SUPPORTED, "$writerLabel does not yet support type: $type")
    }

    /** Format a column min/max stat value for persistence, or null when not representable. */
    fun formatStatValue(type: Type, value: Any?): String? {
        if (value == null) {
            return null
        }
        when (type) {
            BOOLEAN -> return if (value as Boolean) "true" else "false"
            TINYINT, SMALLINT, INTEGER, BIGINT -> return (value as Number).toLong().toString()
            REAL -> {
                val f: Float = (value as Number).toFloat()
                return if (f.isNaN()) null else f.toString()
            }
            DOUBLE -> {
                val d: Double = (value as Number).toDouble()
                return if (d.isNaN()) null else d.toString()
            }
            is DecimalType -> {
                val bd: BigDecimal = value as? BigDecimal ?: BigDecimal(value.toString())
                return bd.toPlainString()
            }
            DATE -> {
                if (value is java.sql.Date) {
                    return value.toLocalDate().toString()
                }
                if (value is LocalDate) {
                    return value.toString()
                }
                return value.toString()
            }
            is TimestampType -> {
                if (value is LocalDateTime) {
                    return value.toString()
                }
                if (value is java.sql.Timestamp) {
                    return value.toLocalDateTime().toString()
                }
                return value.toString()
            }
            is TimestampWithTimeZoneType -> {
                if (value is OffsetDateTime) {
                    return value.toInstant().toString()
                }
                if (value is java.sql.Timestamp) {
                    return value.toInstant().toString()
                }
                return value.toString()
            }
            is VarcharType -> return value.toString()
        }
        return value.toString()
    }
}
