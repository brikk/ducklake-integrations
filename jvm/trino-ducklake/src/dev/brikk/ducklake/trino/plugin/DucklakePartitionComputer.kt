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

import com.google.common.hash.Hashing.murmur3_32_fixed
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import io.airlift.slice.Slice
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.DateType
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
import io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.OptionalInt

/**
 * Computes partition values from Trino block data for DuckLake partitioned writes.
 * Supports CALENDAR / EPOCH encoding for temporal transforms and Iceberg-compatible
 * Murmur3 hashing for `bucket(N)` transforms.
 */
object DucklakePartitionComputer {
    /**
     * Compute the partition value string for a given row position.
     *
     * @return the partition value as a string, or null if the value is null
     */
    /**
     * Convenience overload for IDENTITY / temporal transforms (no arity needed).
     */
    fun computePartitionValue(
            columnType: Type,
            block: Block,
            position: Int,
            transform: DucklakePartitionTransform,
            encoding: DucklakeTemporalPartitionEncoding): String? =
            computePartitionValue(columnType, block, position, transform, null, encoding)

    fun computePartitionValue(
            columnType: Type,
            block: Block,
            position: Int,
            transform: DucklakePartitionTransform,
            arity: Int?,
            encoding: DucklakeTemporalPartitionEncoding): String? {
        if (block.isNull(position)) {
            return null
        }

        if (transform.isIdentity()) {
            return computeIdentityValue(columnType, block, position)
        }

        if (transform.isBucket()) {
            val n = arity ?: throw IllegalArgumentException("BUCKET transform requires an arity")
            return computeBucket(columnType, block, position, n).toString()
        }

        return computeTemporalValue(columnType, block, position, transform, encoding)
    }

    /**
     * True iff an IDENTITY partition over [type] can be canonically encoded to (and parsed back
     * from) a DuckLake partition-value string — i.e. exactly the set [computeIdentityValue] handles
     * and [DucklakePartitionValueParser] round-trips. Callers reject the rest at CREATE TABLE time
     * (the runtime encoder's `else` throw is the belt-and-braces guard). NOT partitionable:
     * VARBINARY, UUID, CHAR, TIME, and the complex types.
     */
    fun isIdentityPartitionable(type: Type): Boolean = when (type) {
        BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE -> true
        is DateType, is DecimalType, is TimestampType, is TimestampWithTimeZoneType, is VarcharType -> true
        else -> false
    }

    // `yyyy-MM-dd HH:mm:ss` with an optional fractional part (0-9 digits, dot only when non-zero).
    // Matches the form DucklakePartitionValueParser accepts and DuckDB's hive-path convention.
    private val IDENTITY_TIMESTAMP_FORMAT = DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter()

    private fun computeIdentityValue(type: Type, block: Block, position: Int): String {
        when (type) {
            is DateType -> {
                val days = DATE.getInt(block, position)
                return LocalDate.ofEpochDay(days.toLong()).toString()
            }
            BOOLEAN -> return BOOLEAN.getBoolean(block, position).toString()
            TINYINT -> return TINYINT.getLong(block, position).toString()
            SMALLINT -> return SMALLINT.getLong(block, position).toString()
            INTEGER -> return INTEGER.getInt(block, position).toString()
            BIGINT -> return BIGINT.getLong(block, position).toString()
            REAL -> return REAL.getFloat(block, position).toString()
            DOUBLE -> return DOUBLE.getDouble(block, position).toString()
        }
        // Wide identity types whose canonical text mirrors DucklakePartitionValueParser (the read
        // side): DECIMAL and the timestamp family. These are long/Int128/object-backed, so the old
        // `getSlice()` fallback threw UnsupportedOperationException at INSERT time — encode them
        // properly instead.
        return when (type) {
            is DecimalType -> encodeDecimal(type, block, position)
            is TimestampType, is TimestampWithTimeZoneType ->
                // extractLocalDateTime normalizes all four short/long timestamp[tz] forms to a UTC
                // LocalDateTime; render `yyyy-MM-dd HH:mm:ss[.fraction]`, which the parser accepts
                // (it swaps the space for 'T' before LocalDateTime/OffsetDateTime.parse).
                extractLocalDateTime(type, block, position).format(IDENTITY_TIMESTAMP_FORMAT)
            // VARCHAR round-trips as its raw UTF-8 text.
            is VarcharType -> type.getSlice(block, position).toStringUtf8()
            // Everything else (VARBINARY, UUID, CHAR, TIME, …) has no canonical identity-partition
            // text the read side can parse back — fail loud rather than write a value that reads
            // wrong (raw bytes as invalid UTF-8) or crash opaquely in getSlice().
            else -> throw TrinoException(
                    NOT_SUPPORTED, "Identity partitioning is not supported for column type $type")
        }
    }

    private fun encodeDecimal(type: DecimalType, block: Block, position: Int): String {
        val decimal: BigDecimal = if (type.isShort) {
            BigDecimal.valueOf(type.getLong(block, position), type.scale)
        }
        else {
            BigDecimal((type.getObject(block, position) as Int128).toBigInteger(), type.scale)
        }
        // Plain (non-scientific) so the parser's BigDecimal(value).setScale(scale) round-trips.
        return decimal.toPlainString()
    }

    private fun computeTemporalValue(
            columnType: Type,
            block: Block,
            position: Int,
            transform: DucklakePartitionTransform,
            encoding: DucklakeTemporalPartitionEncoding): String {
        val dateTime = extractLocalDateTime(columnType, block, position)

        return when (encoding) {
            DucklakeTemporalPartitionEncoding.CALENDAR -> computeCalendarValue(dateTime, transform)
            DucklakeTemporalPartitionEncoding.EPOCH -> computeEpochValue(columnType, block, position, dateTime, transform)
        }
    }

    private fun extractLocalDateTime(columnType: Type, block: Block, position: Int): LocalDateTime {
        if (columnType is DateType) {
            val days = DATE.getInt(block, position)
            return LocalDate.ofEpochDay(days.toLong()).atStartOfDay()
        }
        if (columnType is TimestampType) {
            if (columnType.isShort) {
                val epochMicros = columnType.getLong(block, position)
                val epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND)
                val nanoAdjustment = (floorMod(epochMicros, MICROSECONDS_PER_SECOND).toInt()) * 1000
                return LocalDateTime.ofEpochSecond(epochSeconds, nanoAdjustment, ZoneOffset.UTC)
            }
            // Long timestamp (precision > 6)
            val longTs: LongTimestamp = columnType.getObject(block, position) as LongTimestamp
            val epochMicros = longTs.epochMicros
            val epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND)
            val nanoAdjustment = (floorMod(epochMicros, MICROSECONDS_PER_SECOND) * 1000 + longTs.picosOfMicro / 1000).toInt()
            return LocalDateTime.ofEpochSecond(epochSeconds, nanoAdjustment, ZoneOffset.UTC)
        }
        if (columnType is TimestampWithTimeZoneType) {
            if (columnType.isShort) {
                val packedValue = columnType.getLong(block, position)
                val epochMillis = unpackMillisUtc(packedValue)
                return Instant.ofEpochMilli(epochMillis).atOffset(ZoneOffset.UTC).toLocalDateTime()
            }
            val longTz: LongTimestampWithTimeZone =
                    columnType.getObject(block, position) as LongTimestampWithTimeZone
            return Instant.ofEpochMilli(longTz.epochMillis)
                    .atOffset(ZoneOffset.UTC)
                    .toLocalDateTime()
        }
        throw IllegalArgumentException("Temporal partition not supported for type: $columnType")
    }

    /**
     * Calendar encoding: literal human-readable values matching DuckDB's behavior.
     */
    private fun computeCalendarValue(dateTime: LocalDateTime, transform: DucklakePartitionTransform): String {
        return when (transform) {
            DucklakePartitionTransform.YEAR -> dateTime.year.toString()
            DucklakePartitionTransform.MONTH -> dateTime.monthValue.toString()
            DucklakePartitionTransform.DAY -> dateTime.dayOfMonth.toString()
            DucklakePartitionTransform.HOUR -> dateTime.hour.toString()
            DucklakePartitionTransform.IDENTITY, DucklakePartitionTransform.BUCKET -> throw IllegalArgumentException("$transform is not a temporal transform")
        }
    }

    /**
     * Epoch encoding: values relative to 1970-01-01 epoch (Iceberg-style).
     */
    private fun computeEpochValue(
            columnType: Type,
            block: Block,
            position: Int,
            dateTime: LocalDateTime,
            transform: DucklakePartitionTransform): String {
        return when (transform) {
            DucklakePartitionTransform.YEAR -> (dateTime.year - 1970).toString()
            DucklakePartitionTransform.MONTH -> ((dateTime.year - 1970) * 12 + (dateTime.monthValue - 1)).toString()
            DucklakePartitionTransform.DAY -> {
                if (columnType is DateType) {
                    DATE.getInt(block, position).toString()
                }
                else {
                    dateTime.toLocalDate().toEpochDay().toString()
                }
            }
            DucklakePartitionTransform.HOUR -> {
                val epochDay = dateTime.toLocalDate().toEpochDay()
                (epochDay * 24 + dateTime.hour).toString()
            }
            DucklakePartitionTransform.IDENTITY, DucklakePartitionTransform.BUCKET -> throw IllegalArgumentException("$transform is not a temporal transform")
        }
    }

    /**
     * Iceberg-compatible bucket hash: `(murmur3_32(serialized_v) & INT_MAX) % N`.
     * Iceberg widens int/date to long before hashing; UTF-8 bytes for VARCHAR; raw bytes
     * for VARBINARY; UUID hashed as two 8-byte halves (msb then lsb).
     *
     * FLOAT / DOUBLE / BOOLEAN are intentionally unsupported, matching Iceberg's spec.
     * Caller should reject these at table-property validation time; this is a defensive
     * runtime guard.
     */
    fun computeBucket(type: Type, block: Block, position: Int, arity: Int): Int {
        val hash = murmur3Hash(type, block, position)
        return (hash and Integer.MAX_VALUE) % arity
    }

    private fun murmur3Hash(type: Type, block: Block, position: Int): Int {
        if (type == TINYINT) {
            return murmur3_32_fixed().hashLong(TINYINT.getLong(block, position)).asInt()
        }
        if (type == SMALLINT) {
            return murmur3_32_fixed().hashLong(SMALLINT.getLong(block, position)).asInt()
        }
        if (type == INTEGER) {
            return murmur3_32_fixed().hashLong(INTEGER.getInt(block, position).toLong()).asInt()
        }
        if (type == BIGINT) {
            return murmur3_32_fixed().hashLong(BIGINT.getLong(block, position)).asInt()
        }
        if (type == DATE) {
            return murmur3_32_fixed().hashLong(DATE.getInt(block, position).toLong()).asInt()
        }
        if (type is TimestampType) {
            val epochMicros: Long
            if (type.isShort) {
                epochMicros = type.getLong(block, position)
            }
            else {
                epochMicros = (type.getObject(block, position) as LongTimestamp).epochMicros
            }
            return murmur3_32_fixed().hashLong(epochMicros).asInt()
        }
        if (type is TimestampWithTimeZoneType) {
            val epochMicros: Long
            if (type.isShort) {
                epochMicros = unpackMillisUtc(type.getLong(block, position)) * 1_000L
            }
            else {
                val tz: LongTimestampWithTimeZone =
                        type.getObject(block, position) as LongTimestampWithTimeZone
                epochMicros = tz.epochMillis * 1_000L + tz.picosOfMilli / 1_000_000L
            }
            return murmur3_32_fixed().hashLong(epochMicros).asInt()
        }
        if (type is VarcharType) {
            val slice: Slice = type.getSlice(block, position)
            return murmur3_32_fixed().hashString(slice.toStringUtf8(), StandardCharsets.UTF_8).asInt()
        }
        if (type is VarbinaryType) {
            val slice: Slice = type.getSlice(block, position)
            return murmur3_32_fixed().hashBytes(slice.byteArray(), slice.byteArrayOffset(), slice.length()).asInt()
        }
        if (type is UuidType) {
            // Iceberg hashes UUID as 16 bytes big-endian (msb, lsb) — Trino stores the same
            // 16 bytes packed in a slice via UuidType.javaUuidToTrinoUuid.
            val slice: Slice = UuidType.UUID.getSlice(block, position)
            return murmur3_32_fixed().hashBytes(slice.byteArray(), slice.byteArrayOffset(), slice.length()).asInt()
        }
        if (type == REAL || type == DOUBLE || type == BOOLEAN) {
            throw IllegalArgumentException("bucket(N) is not defined for type $type (Iceberg spec)")
        }
        throw IllegalArgumentException("bucket(N) does not yet support type: $type")
    }
}
