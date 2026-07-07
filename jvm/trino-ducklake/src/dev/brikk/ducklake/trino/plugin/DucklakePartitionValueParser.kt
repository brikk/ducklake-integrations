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
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType

import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.type.VarcharType.VARCHAR

/**
 * Parses the string-encoded partition values stored in
 * `ducklake_file_partition_value.partition_value` into Trino native values.
 * Shared by the split manager's partition-pruning path (compares the parsed value
 * against the query's pushdown domain) and the page source provider's
 * partition-column projection (constants the parsed value across the file).
 *
 * Encoding contract is the DuckLake 1.0 calendar form for temporals
 * (handled by `DucklakeTemporalPartitionMatcher`); this helper covers
 * identity-transform value parsing only.
 */
object DucklakePartitionValueParser
{
    fun parseIdentity(type: Type, value: String): Any
    {
        return when {
            type == VARCHAR || type is VarcharType -> Slices.utf8Slice(value)
            type == BIGINT -> java.lang.Long.parseLong(value)
            type == INTEGER -> Integer.parseInt(value).toLong()
            type == SMALLINT -> java.lang.Short.parseShort(value).toLong()
            type == TINYINT -> java.lang.Byte.parseByte(value).toLong()
            type == DOUBLE -> java.lang.Double.parseDouble(value)
            type == REAL -> java.lang.Float.floatToIntBits(java.lang.Float.parseFloat(value)).toLong()
            type == DATE -> LocalDate.parse(value).toEpochDay()
            type == BOOLEAN -> parseBoolean(value)
            else -> parseWideType(type, value)
        }
    }

    // DECIMAL and the temporal types, split out to keep parseIdentity's dispatch under the
    // cyclomatic-complexity gate.
    private fun parseWideType(type: Type, value: String): Any =
        when (type) {
            is DecimalType -> parseDecimal(type, value)
            is TimestampType -> parseTimestamp(type, value)
            is TimestampWithTimeZoneType -> parseTimestampWithTimeZone(type, value)
            else -> throw IllegalArgumentException("Unsupported partition value type: $type")
        }

    // DECIMAL: parse to the column's scale (UNNECESSARY rounding — a scale mismatch throws,
    // matching Trino's Decimals SPI convention, so the caller's catch falls back to NULL rather
    // than silently rounding). Short decimals project as the unscaled long; long decimals as Int128.
    private fun parseDecimal(type: DecimalType, value: String): Any {
        val decimal: BigDecimal = BigDecimal(value.trim()).setScale(type.scale, RoundingMode.UNNECESSARY)
        return if (type.isShort) {
            decimal.unscaledValue().longValueExact()
        } else {
            Int128.valueOf(decimal.unscaledValue())
        }
    }

    // TIMESTAMP (without time zone): a hive path stores the value as `2024-01-15 10:30:00[.ffffff]`.
    // Short precision (<= 6) projects as epoch micros; long precision as LongTimestamp(micros, picos).
    private fun parseTimestamp(type: TimestampType, value: String): Any {
        val ldt: LocalDateTime = LocalDateTime.parse(value.trim().replace(' ', 'T'))
        val epochMicros: Long = ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000 + ldt.nano / 1_000
        if (type.isShort) {
            return epochMicros
        }
        val picosOfMicro: Int = (ldt.nano % 1_000) * 1_000
        return LongTimestamp(epochMicros, picosOfMicro)
    }

    // TIMESTAMP WITH TIME ZONE: parse an offset-bearing (or UTC-defaulted) literal to the packed
    // short form or LongTimestampWithTimeZone, normalized to UTC (the connector's storage zone).
    private fun parseTimestampWithTimeZone(type: TimestampWithTimeZoneType, value: String): Any {
        val text: String = value.trim().replace(' ', 'T')
        val odt: OffsetDateTime = try {
            OffsetDateTime.parse(text)
        } catch (_: java.time.format.DateTimeParseException) {
            LocalDateTime.parse(text).atOffset(ZoneOffset.UTC)
        }
        val epochMicros: Long = odt.toEpochSecond() * 1_000_000 + odt.nano / 1_000
        val epochMillis: Long = Math.floorDiv(epochMicros, 1_000)
        if (type.isShort) {
            return packDateTimeWithZone(epochMillis, UTC_KEY)
        }
        val picosOfMilli: Int = Math.floorMod(epochMicros, 1_000).toInt() * 1_000_000 + (odt.nano % 1_000) * 1_000
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY)
    }

    // Boolean.parseBoolean silently maps everything that isn't "true" to false, so an
    // externally-written catalog that encodes boolean partitions as "1"/"0" (the form
    // DuckLake's C++ extension may store, and the form JdbcDucklakeCatalog/DucklakeStatTypes
    // already accept) would parse "1" as false — wrong pruning and a wrong projected
    // constant. Mirror the catalog's encoding and throw on anything else so the callers'
    // catch(RuntimeException) blocks fall back safely (don't-prune / NULL) instead of
    // silently producing the wrong value.
    private fun parseBoolean(value: String): Boolean {
        return when {
            value.equals("true", ignoreCase = true) || value == "1" -> true
            value.equals("false", ignoreCase = true) || value == "0" -> false
            else -> throw IllegalArgumentException("Invalid boolean partition value: $value")
        }
    }
}
