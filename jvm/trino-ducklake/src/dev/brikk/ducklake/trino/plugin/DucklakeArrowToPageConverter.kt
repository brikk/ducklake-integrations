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
import io.trino.spi.block.ArrayBlockBuilder
import io.trino.spi.block.Block
import io.trino.spi.block.BlockBuilder
import io.trino.spi.type.ArrayType
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
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.types.pojo.ArrowType

/**
 * Materializes one {@link VectorSchemaRoot} batch from DuckDB's Arrow export stream
 * into a Trino {@link Page}, dispatching per-column on the resolved Trino {@link Type}.
 *
 * <p>Supported types are the scalar writer surface plus {@code ARRAY} of scalar (or nested
 * {@code ARRAY}) elements — the latter added for Lance embedding columns, which arrive as Arrow
 * {@code FixedSizeList<float>} and map to {@code ARRAY(REAL)}. {@code ROW}/{@code MAP}, timestamp/
 * uuid array elements, unsupported timestamp precisions, and unknown vector classes raise
 * {@link io.trino.spi.StandardErrorCode#NOT_SUPPORTED}.
 */
internal class DucklakeArrowToPageConverter(columnTypes: List<Type>) {
    private val columnTypes: List<Type> = columnTypes.toList()

    fun convert(root: VectorSchemaRoot): Page {
        val rowCount = root.rowCount
        val columnCount = columnTypes.size
        if (root.fieldVectors.size != columnCount) {
            throw TrinoException(
                    NOT_SUPPORTED,
                    "DuckDB Arrow batch has ${root.fieldVectors.size} columns but $columnCount projected")
        }
        val blocks: Array<Block> = Array(columnCount) { i ->
            convertColumn(columnTypes[i], root.getVector(i), rowCount)
        }
        return Page(rowCount, *blocks)
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeArrowToPageConverter::class.java)

        private fun convertColumn(type: Type, vector: FieldVector, rowCount: Int): Block {
            val builder: BlockBuilder = type.createBlockBuilder(null, rowCount)
            if (type == BOOLEAN) {
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
            else if (type == TINYINT) {
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
            else if (type == SMALLINT) {
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
            else if (type == INTEGER) {
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
            else if (type == BIGINT) {
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
            else if (type == REAL) {
                val v = vector as Float4Vector
                for (i in 0 until rowCount) {
                    if (v.isNull(i)) {
                        builder.appendNull()
                    }
                    else {
                        REAL.writeLong(builder, v.get(i).toRawBits().toLong())
                    }
                }
            }
            else if (type == DOUBLE) {
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
            else if (type == DATE) {
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
            else if (type == VARBINARY) {
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
            else if (type == UuidType.UUID) {
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
                        val s = String(v.get(i), Charsets.UTF_8)
                        UuidType.UUID.writeSlice(builder, UuidType.javaUuidToTrinoUuid(java.util.UUID.fromString(s)))
                    }
                }
            }
            else if (type is ArrayType) {
                // Lance embedding columns arrive as Arrow FixedSizeList<float> (e.g. FLOAT[3]) and
                // map to ARRAY(REAL); plain DuckDB/vortex lists arrive as variable-width ListVector.
                // Both are handled here. ROW/MAP and timestamp/uuid element types are still
                // NOT_SUPPORTED (see appendArrayElement).
                writeArrayColumn(type, vector, builder as ArrayBlockBuilder, rowCount)
            }
            else {
                throw TrinoException(
                        NOT_SUPPORTED,
                        "DuckDB-format reader does not yet support type: $type")
            }
            return builder.build()
        }

        /** Writes a whole ARRAY column: one Trino array entry per row (null-aware at the list level). */
        private fun writeArrayColumn(type: ArrayType, vector: FieldVector, builder: ArrayBlockBuilder, rowCount: Int) {
            for (row in 0 until rowCount) {
                if (vector.isNull(row)) {
                    builder.appendNull()
                }
                else {
                    appendArrayEntry(type, vector, row, builder)
                }
            }
        }

        /**
         * Appends the (non-null) ARRAY value at [row] of [vector] as one entry of [builder].
         * Handles both Arrow [FixedSizeListVector] (lance embeddings, `FLOAT[N]`) and variable-width
         * [ListVector]. Element values are written via [appendArrayElement], which also drives
         * recursion for nested ARRAY-of-ARRAY.
         */
        private fun appendArrayEntry(type: ArrayType, vector: FieldVector, row: Int, builder: ArrayBlockBuilder) {
            val elementType: Type = type.elementType
            when (vector) {
                is FixedSizeListVector -> {
                    val width: Int = vector.listSize
                    val data: FieldVector = vector.dataVector as FieldVector
                    val base: Int = row * width
                    builder.buildEntry<RuntimeException> { elementBuilder ->
                        for (j in 0 until width) {
                            appendArrayElement(elementType, data, base + j, elementBuilder)
                        }
                    }
                }
                is ListVector -> {
                    val data: FieldVector = vector.dataVector
                    val start: Int = vector.getElementStartIndex(row)
                    val end: Int = vector.getElementEndIndex(row)
                    builder.buildEntry<RuntimeException> { elementBuilder ->
                        for (idx in start until end) {
                            appendArrayElement(elementType, data, idx, elementBuilder)
                        }
                    }
                }
                else -> throw TrinoException(
                        NOT_SUPPORTED,
                        "DuckDB-format reader: ARRAY backed by unsupported Arrow vector ${vector.javaClass.simpleName}")
            }
        }

        /**
         * Appends a single element value of Trino [type] read from [vector] at [i] into [builder].
         * Null-aware (uses the element vector's own validity). Covers the scalar element types lance
         * arrays carry (REAL for embeddings, plus the other primitives) and recurses for nested
         * ARRAY; ROW/MAP and timestamp/uuid elements remain NOT_SUPPORTED.
         */
        private fun appendArrayElement(type: Type, vector: FieldVector, i: Int, builder: BlockBuilder) {
            if (vector.isNull(i)) {
                builder.appendNull()
                return
            }
            when {
                type is DecimalType -> appendDecimalElement(type, vector as DecimalVector, i, builder)
                type is ArrayType -> appendArrayEntry(type, vector, i, builder as ArrayBlockBuilder)
                else -> appendScalarElement(type, vector, i, builder)
            }
        }

        /** Appends one (non-null) scalar element value. Split from [appendArrayElement] to keep each
         * dispatch's cyclomatic complexity in check. */
        private fun appendScalarElement(type: Type, vector: FieldVector, i: Int, builder: BlockBuilder) {
            when {
                type == BOOLEAN -> BOOLEAN.writeBoolean(builder, (vector as BitVector).get(i) != 0)
                type == TINYINT -> TINYINT.writeLong(builder, (vector as TinyIntVector).get(i).toLong())
                type == SMALLINT -> SMALLINT.writeLong(builder, (vector as SmallIntVector).get(i).toLong())
                type == INTEGER -> INTEGER.writeLong(builder, (vector as IntVector).get(i).toLong())
                type == BIGINT -> BIGINT.writeLong(builder, (vector as BigIntVector).get(i))
                type == REAL -> REAL.writeLong(builder, (vector as Float4Vector).get(i).toRawBits().toLong())
                type == DOUBLE -> DOUBLE.writeDouble(builder, (vector as Float8Vector).get(i))
                type == DATE -> DATE.writeLong(builder, (vector as DateDayVector).get(i).toLong())
                type == VARBINARY -> VARBINARY.writeSlice(builder, Slices.wrappedBuffer(*(vector as VarBinaryVector).get(i)))
                type is VarcharType -> type.writeSlice(builder, Slices.wrappedBuffer(*(vector as VarCharVector).get(i)))
                else -> throw TrinoException(
                        NOT_SUPPORTED,
                        "DuckDB-format reader does not yet support ARRAY element type: $type")
            }
        }

        /** Appends one (non-null) decimal element, short or long form. */
        private fun appendDecimalElement(type: DecimalType, vector: DecimalVector, i: Int, builder: BlockBuilder) {
            val value = vector.getObject(i)
            if (type.isShort) {
                type.writeLong(builder, value.unscaledValue().longValueExact())
            }
            else {
                type.writeObject(builder, Int128.valueOf(value.unscaledValue()))
            }
        }

        private fun writeDecimalColumn(decimalType: DecimalType, vector: DecimalVector, builder: BlockBuilder, rowCount: Int) {
            for (i in 0 until rowCount) {
                if (vector.isNull(i)) {
                    builder.appendNull()
                    continue
                }
                val value = vector.getObject(i)
                if (decimalType.isShort) {
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
                        "Unsupported Arrow timestamp vector for Trino type $type: ${vector.javaClass.simpleName}")
            }
        }

        private fun writeTimestampMicrosWithRemainder(type: TimestampType, builder: BlockBuilder, micros: Long, picosOfMicro: Int) {
            if (type.isShort) {
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
                        "Unsupported Arrow timestamp_tz vector for Trino type $type: ${vector.javaClass.simpleName}")
            }
        }

        private val UNPARSEABLE_TZ_WARNED: java.util.concurrent.ConcurrentHashMap<String, Boolean> =
                java.util.concurrent.ConcurrentHashMap()

        private fun resolveTimeZoneKey(vector: FieldVector): TimeZoneKey {
            val arrowType = vector.field.type
            if (arrowType !is ArrowType.Timestamp) {
                return UTC_KEY
            }
            val tzString = arrowType.timezone ?: return UTC_KEY
            try {
                return TimeZoneKey.getTimeZoneKey(tzString)
            }
            catch (e: TimeZoneNotSupportedException) {
                if (UNPARSEABLE_TZ_WARNED.putIfAbsent(tzString, true) == null) {
                    log.warn("Arrow schema TimeZone '%s' is not a recognised Trino TimeZoneKey; "
                                    + "falling back to UTC for incoming TIMESTAMP WITH TIME ZONE values "
                                    + "of this column. Subsequent occurrences of the same zone string "
                                    + "use UTC without re-warning. See dev-docs/archive/REPORT-datetime-tz-handling.md.",
                            tzString)
                }
                return UTC_KEY
            }
            catch (e: IllegalArgumentException) {
                if (UNPARSEABLE_TZ_WARNED.putIfAbsent(tzString, true) == null) {
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
            if (type.isShort) {
                type.writeLong(builder, packDateTimeWithZone(epochMillis, zone))
            }
            else {
                type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, zone))
            }
        }
    }
}
