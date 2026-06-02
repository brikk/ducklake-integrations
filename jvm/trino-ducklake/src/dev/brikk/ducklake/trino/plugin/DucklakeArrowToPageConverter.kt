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

import io.airlift.log.Logger
import io.airlift.slice.Slices
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.block.BlockBuilder
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimeZoneKey
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimeZoneNotSupportedException
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.DateDayVector
import org.apache.arrow.vector.DecimalVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.SmallIntVector
import org.apache.arrow.vector.TimeStampMicroTZVector
import org.apache.arrow.vector.TimeStampMicroVector
import org.apache.arrow.vector.TimeStampMilliTZVector
import org.apache.arrow.vector.TimeStampMilliVector
import org.apache.arrow.vector.TimeStampNanoTZVector
import org.apache.arrow.vector.TimeStampNanoVector
import org.apache.arrow.vector.TimeStampSecTZVector
import org.apache.arrow.vector.TimeStampSecVector
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import java.lang.String.format

/**
 * Materializes one {@link VectorSchemaRoot} batch from DuckDB's Arrow export stream
 * into a Trino {@link Page}, dispatching per-column on the resolved Trino {@link Type}.
 *
 * <p>Supported types track the Phase 1 writer surface (scalars only). Nested types,
 * unsupported timestamp precisions, and unknown vector classes raise
 * {@link io.trino.spi.StandardErrorCode#NOT_SUPPORTED}.
 */
internal class DucklakeArrowToPageConverter(columnTypes: List<Type>) {
    private val columnTypes: List<Type> = columnTypes.toList()

    fun convert(root: VectorSchemaRoot): Page {
        val rowCount = root.getRowCount()
        val columnCount = columnTypes.size
        if (root.getFieldVectors().size != columnCount) {
            throw TrinoException(
                    NOT_SUPPORTED,
                    format("DuckDB Arrow batch has %d columns but %d projected",
                            root.getFieldVectors().size, columnCount))
        }
        val blocks: Array<Block> = Array(columnCount) { i ->
            convertColumn(columnTypes.get(i), root.getVector(i), rowCount)
        }
        return Page(rowCount, *blocks)
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeArrowToPageConverter::class.java)

        private fun convertColumn(type: Type, vector: FieldVector, rowCount: Int): Block {
            val builder: BlockBuilder = type.createBlockBuilder(null, rowCount)
            if (type.equals(BOOLEAN)) {
                val v = vector as BitVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        BOOLEAN.writeBoolean(builder, v.get(i) != 0)
                    }
                }
            }
            else if (type.equals(TINYINT)) {
                val v = vector as TinyIntVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        TINYINT.writeLong(builder, v.get(i).toLong())
                    }
                }
            }
            else if (type.equals(SMALLINT)) {
                val v = vector as SmallIntVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        SMALLINT.writeLong(builder, v.get(i).toLong())
                    }
                }
            }
            else if (type.equals(INTEGER)) {
                val v = vector as IntVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        INTEGER.writeLong(builder, v.get(i).toLong())
                    }
                }
            }
            else if (type.equals(BIGINT)) {
                val v = vector as BigIntVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        BIGINT.writeLong(builder, v.get(i))
                    }
                }
            }
            else if (type.equals(REAL)) {
                val v = vector as Float4Vector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        REAL.writeLong(builder, java.lang.Float.floatToRawIntBits(v.get(i)).toLong())
                    }
                }
            }
            else if (type.equals(DOUBLE)) {
                val v = vector as Float8Vector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        DOUBLE.writeDouble(builder, v.get(i))
                    }
                }
            }
            else if (type.equals(DATE)) {
                val v = vector as DateDayVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        DATE.writeLong(builder, v.get(i).toLong())
                    }
                }
            }
            else if (type.equals(VARBINARY)) {
                val v = vector as VarBinaryVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        VARBINARY.writeSlice(builder, Slices.wrappedBuffer(*v.get(i)))
                    }
                }
            }
            else if (type is VarcharType) {
                val v = vector as VarCharVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        type.writeSlice(builder, Slices.wrappedBuffer(*v.get(i)))
                    }
                }
            }
            else if (type is DecimalType) {
                writeDecimalColumn(type, vector as DecimalVector, builder, rowCount)
            }
            else if (type is TimestampType) {
                writeTimestampColumn(type, vector, builder, rowCount)
            }
            else if (type is TimestampWithTimeZoneType) {
                writeTimestampTzColumn(type, vector, builder, rowCount)
            }
            else if (type.equals(UuidType.UUID)) {
                // DuckDB exports UUID columns as Utf8 (the printed hex form), not
                // FixedSizeBinary(16) — verified empirically against the in-process
                // DuckDB JDBC driver. Parse the string and convert to Trino's
                // 16-byte big-endian UUID Slice.
                val v = vector as VarCharVector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        val s = String(v.get(i), java.nio.charset.StandardCharsets.UTF_8)
                        UuidType.UUID.writeSlice(builder, UuidType.javaUuidToTrinoUuid(java.util.UUID.fromString(s)))
                    }
                }
            }
            else {
                throw TrinoException(
                        NOT_SUPPORTED,
                        "DuckDB-format reader does not yet support type: " + type)
            }
            return builder.build()
        }

        private fun writeDecimalColumn(decimalType: DecimalType, vector: DecimalVector, builder: BlockBuilder, rowCount: Int) {
            for (i in 0 until rowCount) {
                if (vector.isNull(i)) {
                    builder.appendNull()
                    continue
                }
                val value = vector.getObject(i)
                if (decimalType.isShort()) {
                    decimalType.writeLong(builder, value.unscaledValue().longValueExact())
                }
                else {
                    val unscaled = Int128.valueOf(value.unscaledValue())
                    decimalType.writeObject(builder, unscaled)
                }
            }
        }

        private fun writeTimestampColumn(type: TimestampType, vector: FieldVector, builder: BlockBuilder, rowCount: Int) {
            // Trino stores TIMESTAMP at micros-since-epoch in a long for precision <= 6,
            // and as LongTimestamp(micros, picosOfMicro) for precision <= 9.
            if (vector is TimeStampSecVector) {
                for (i in 0 until rowCount) {
                    if (vector.isNull(i)) {
                        builder.appendNull()
                        continue
                    }
                    val micros = Math.multiplyExact(vector.get(i), 1_000_000L)
                    writeTimestampMicrosWithRemainder(type, builder, micros, 0)
                }
            }
            else if (vector is TimeStampMilliVector) {
                for (i in 0 until rowCount) {
                    if (vector.isNull(i)) {
                        builder.appendNull()
                        continue
                    }
                    val micros = Math.multiplyExact(vector.get(i), 1_000L)
                    writeTimestampMicrosWithRemainder(type, builder, micros, 0)
                }
            }
            else if (vector is TimeStampMicroVector) {
                for (i in 0 until rowCount) {
                    if (vector.isNull(i)) {
                        builder.appendNull()
                        continue
                    }
                    writeTimestampMicrosWithRemainder(type, builder, vector.get(i), 0)
                }
            }
            else if (vector is TimeStampNanoVector) {
                for (i in 0 until rowCount) {
                    if (vector.isNull(i)) {
                        builder.appendNull()
                        continue
                    }
                    val nanos = vector.get(i)
                    val micros = Math.floorDiv(nanos, 1_000L)
                    val picosOfMicro = (Math.floorMod(nanos, 1_000L) * 1_000L).toInt()
                    writeTimestampMicrosWithRemainder(type, builder, micros, picosOfMicro)
                }
            }
            else {
                throw TrinoException(
                        NOT_SUPPORTED,
                        "Unsupported Arrow timestamp vector for Trino type " + type + ": " + vector.javaClass.getSimpleName())
            }
        }

        private fun writeTimestampMicrosWithRemainder(type: TimestampType, builder: BlockBuilder, micros: Long, picosOfMicro: Int) {
            if (type.isShort()) {
                type.writeLong(builder, micros)
            }
            else {
                type.writeObject(builder, LongTimestamp(micros, picosOfMicro))
            }
        }

        private fun writeTimestampTzColumn(type: TimestampWithTimeZoneType, vector: FieldVector, builder: BlockBuilder, rowCount: Int) {
            // Resolve the time zone ONCE per column from the Arrow schema. DuckDB
            // sets the Arrow timestamp vector's TZ field to the session TimeZone at
            // export time (probed across IANA / Etc/GMT± / fractional-offset-via-named
            // zones — every shape comes through). Chunk 2 set DuckDB's session
            // TimeZone to match Trino's session zone on attach, so the schema TZ
            // arriving here IS the session zone the user wants. Pre-3.5 the code
            // hardcoded UTC_KEY, which made Trino's above-scan year() always UTC-
            // aligned regardless of session — that was an outlier vs. other Trino
            // connectors (Iceberg, Hive, Parquet `isAdjustedToUtc=true` all return
            // WTZ in session zone) and blocked Tier C pushdown over WTZ columns
            // because DuckDB-side eval (session zone) and Trino above-scan eval
            // (UTC) disagreed.
            //
            // Fallback to UTC_KEY when the schema TZ is missing or unparseable
            // (one-shot WARN per zone string). Should not happen on the read path
            // we control, but the converter stays robust for any future Arrow
            // producer that didn't set the field.
            val zone = resolveTimeZoneKey(vector)
            if (vector is TimeStampSecTZVector) {
                for (i in 0 until rowCount) {
                    if (vector.isNull(i)) {
                        builder.appendNull()
                        continue
                    }
                    writeTimestampTz(type, builder, Math.multiplyExact(vector.get(i), 1_000L), 0, zone)
                }
            }
            else if (vector is TimeStampMilliTZVector) {
                for (i in 0 until rowCount) {
                    if (vector.isNull(i)) {
                        builder.appendNull()
                        continue
                    }
                    writeTimestampTz(type, builder, vector.get(i), 0, zone)
                }
            }
            else if (vector is TimeStampMicroTZVector) {
                for (i in 0 until rowCount) {
                    if (vector.isNull(i)) {
                        builder.appendNull()
                        continue
                    }
                    val micros = vector.get(i)
                    val millis = Math.floorDiv(micros, 1_000L)
                    val picosOfMilli = (Math.floorMod(micros, 1_000L) * 1_000_000L).toInt()
                    writeTimestampTz(type, builder, millis, picosOfMilli, zone)
                }
            }
            else if (vector is TimeStampNanoTZVector) {
                for (i in 0 until rowCount) {
                    if (vector.isNull(i)) {
                        builder.appendNull()
                        continue
                    }
                    val nanos = vector.get(i)
                    val millis = Math.floorDiv(nanos, 1_000_000L)
                    val picosOfMilli = (Math.floorMod(nanos, 1_000_000L) * 1_000L).toInt()
                    writeTimestampTz(type, builder, millis, picosOfMilli, zone)
                }
            }
            else {
                throw TrinoException(
                        NOT_SUPPORTED,
                        "Unsupported Arrow timestamp_tz vector for Trino type " + type + ": " + vector.javaClass.getSimpleName())
            }
        }

        private val UNPARSEABLE_TZ_WARNED: java.util.concurrent.ConcurrentHashMap<String, Boolean> =
                java.util.concurrent.ConcurrentHashMap()

        private fun resolveTimeZoneKey(vector: FieldVector): TimeZoneKey {
            val arrowType = vector.getField().getType()
            if (arrowType !is ArrowType.Timestamp) {
                return UTC_KEY
            }
            val tzString = arrowType.getTimezone()
            if (tzString == null) {
                return UTC_KEY
            }
            try {
                return TimeZoneKey.getTimeZoneKey(tzString)
            }
            catch (e: TimeZoneNotSupportedException) {
                if (UNPARSEABLE_TZ_WARNED.putIfAbsent(tzString, java.lang.Boolean.TRUE) == null) {
                    log.warn("Arrow schema TimeZone '%s' is not a recognised Trino TimeZoneKey; "
                                    + "falling back to UTC for incoming TIMESTAMP WITH TIME ZONE values "
                                    + "of this column. Subsequent occurrences of the same zone string "
                                    + "use UTC without re-warning. See dev-docs/archive/REPORT-datetime-tz-handling.md.",
                            tzString)
                }
                return UTC_KEY
            }
            catch (e: IllegalArgumentException) {
                if (UNPARSEABLE_TZ_WARNED.putIfAbsent(tzString, java.lang.Boolean.TRUE) == null) {
                    log.warn("Arrow schema TimeZone '%s' is not a recognised Trino TimeZoneKey; "
                                    + "falling back to UTC for incoming TIMESTAMP WITH TIME ZONE values "
                                    + "of this column. Subsequent occurrences of the same zone string "
                                    + "use UTC without re-warning. See dev-docs/archive/REPORT-datetime-tz-handling.md.",
                            tzString)
                }
                return UTC_KEY
            }
        }

        private fun writeTimestampTz(type: TimestampWithTimeZoneType, builder: BlockBuilder, epochMillis: Long, picosOfMilli: Int, zone: TimeZoneKey) {
            if (type.isShort()) {
                type.writeLong(builder, packDateTimeWithZone(epochMillis, zone))
            }
            else {
                type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, zone))
            }
        }
    }
}
