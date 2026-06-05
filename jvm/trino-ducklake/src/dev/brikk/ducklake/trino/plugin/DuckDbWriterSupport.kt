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
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
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
        if (type == BOOLEAN) {
            return "BOOLEAN"
        }
        if (type == TINYINT) {
            return "TINYINT"
        }
        if (type == SMALLINT) {
            return "SMALLINT"
        }
        if (type == INTEGER) {
            return "INTEGER"
        }
        if (type == BIGINT) {
            return "BIGINT"
        }
        if (type == REAL) {
            return "REAL"
        }
        if (type == DOUBLE) {
            return "DOUBLE"
        }
        if (type == DATE) {
            return "DATE"
        }
        if (type == VARBINARY) {
            return "BLOB"
        }
        if (type is DecimalType) {
            return format(Locale.ROOT, "DECIMAL(%d,%d)", type.precision, type.scale)
        }
        if (type is TimestampType) {
            return when (type.precision) {
                0 -> "TIMESTAMP_S"
                3 -> "TIMESTAMP_MS"
                6 -> "TIMESTAMP"
                9 -> "TIMESTAMP_NS"
                else -> "TIMESTAMP"
            }
        }
        if (type is TimestampWithTimeZoneType) {
            return "TIMESTAMPTZ"
        }
        if (type is VarcharType) {
            return "VARCHAR"
        }
        if (type == UuidType.UUID) {
            return "UUID"
        }
        throw TrinoException(NOT_SUPPORTED, "$writerLabel does not yet support type: $type")
    }

    /** Format a column min/max stat value for persistence, or empty when not representable. */
    fun formatStatValue(type: Type, value: Any?): Optional<String> {
        if (value == null) {
            return Optional.empty()
        }
        if (type == BOOLEAN) {
            return Optional.of(if (value as Boolean) "true" else "false")
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return Optional.of((value as Number).toLong().toString())
        }
        if (type == REAL) {
            val f: Float = (value as Number).toFloat()
            return if (f.isNaN()) Optional.empty() else Optional.of(f.toString())
        }
        if (type == DOUBLE) {
            val d: Double = (value as Number).toDouble()
            return if (d.isNaN()) Optional.empty() else Optional.of(d.toString())
        }
        if (type is DecimalType) {
            val bd: BigDecimal = value as? BigDecimal ?: BigDecimal(value.toString())
            return Optional.of(bd.toPlainString())
        }
        if (type == DATE) {
            if (value is java.sql.Date) {
                return Optional.of(value.toLocalDate().toString())
            }
            if (value is LocalDate) {
                return Optional.of(value.toString())
            }
            return Optional.of(value.toString())
        }
        if (type is TimestampType) {
            if (value is LocalDateTime) {
                return Optional.of(value.toString())
            }
            if (value is java.sql.Timestamp) {
                return Optional.of(value.toLocalDateTime().toString())
            }
            return Optional.of(value.toString())
        }
        if (type is TimestampWithTimeZoneType) {
            if (value is OffsetDateTime) {
                return Optional.of(value.toInstant().toString())
            }
            if (value is java.sql.Timestamp) {
                return Optional.of(value.toInstant().toString())
            }
            return Optional.of(value.toString())
        }
        if (type is VarcharType) {
            return Optional.of(value.toString())
        }
        return Optional.of(value.toString())
    }
}
