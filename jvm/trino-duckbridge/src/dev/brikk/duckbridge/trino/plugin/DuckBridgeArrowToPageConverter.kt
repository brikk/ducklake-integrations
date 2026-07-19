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
package dev.brikk.duckbridge.trino.plugin

import io.airlift.log.Logger
import io.airlift.slice.Slices
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.ArrayBlockBuilder
import io.trino.spi.block.Block
import io.trino.spi.block.BlockBuilder
import io.trino.spi.block.MapBlockBuilder
import io.trino.spi.block.RowBlockBuilder
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
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimeZoneKey
import io.trino.spi.type.TimeZoneKey.UTC_KEY
import io.trino.spi.type.TimeZoneNotSupportedException
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.StandardTypes
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
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.ArrowType

/**
 * Materializes one [VectorSchemaRoot] batch from DuckDB's Arrow export stream
 * into a Trino [Page], dispatching per-column on the resolved Trino [Type].
 *
 * Supported types are the full scalar surface plus the complex types `ARRAY`
 * (Arrow `List` / `FixedSizeList` — the latter is how Lance materializes embedding
 * columns), `ROW` (Arrow `Struct`, fields matched positionally), and `MAP`
 * (Arrow `Map`); complex types nest arbitrarily, and nested values cover every scalar the
 * column level does (timestamps, timestamptz, uuid, decimal included) via [appendNestedValue].
 * Scalar columns keep dedicated per-column loops (one type dispatch per column); complex columns
 * dispatch per value, which is unavoidable for nested data. Unsupported timestamp precisions and
 * unknown vector classes raise [io.trino.spi.StandardErrorCode.NOT_SUPPORTED].
 */
class DuckBridgeArrowToPageConverter(columnTypes: List<Type>) {
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
        private val log: Logger = Logger.get(DuckBridgeArrowToPageConverter::class.java)

        // DuckDB exports JSON columns as Utf8 (the JSON text) over Arrow. Inlined from the DuckLake
        // connector's DucklakeJsonSupport.isJson to avoid porting that whole helper.
        private fun isJsonType(type: Type): Boolean = type.baseName == StandardTypes.JSON

        // Faithful port of the DuckLake converter: one dedicated per-column loop per Trino type,
        // kept as a single dispatch so each type's Arrow-vector handling stays in one place.
        @Suppress("CyclomaticComplexMethod", "LongMethod")
        private fun convertColumn(type: Type, vector: FieldVector, rowCount: Int): Block {
            val builder: BlockBuilder = type.createBlockBuilder(null, rowCount)
            when (type) {
                BOOLEAN -> {
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
                TINYINT -> {
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
                SMALLINT -> {
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
                INTEGER -> {
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
                BIGINT -> {
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
                REAL -> {
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
                DOUBLE -> {
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
                DATE -> {
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
                VARBINARY -> {
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
                is VarcharType -> {
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
                is DecimalType -> {
                    writeDecimalColumn(type, vector as DecimalVector, builder, rowCount)
                }
                is TimestampType -> {
                    writeTimestampColumn(type, vector, builder, rowCount)
                }
                is TimestampWithTimeZoneType -> {
                    writeTimestampTzColumn(type, vector, builder, rowCount)
                }
                UuidType.UUID -> {
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
                is ArrayType, is RowType, is MapType -> {
                    // ARRAY arrives as Arrow List (or FixedSizeList — how lance materializes
                    // embeddings), ROW as Struct (fields positional), MAP as Map (list of key/value
                    // entry structs). Contents go through the recursive appendNestedValue machinery.
                    writeComplexColumn(type, vector, builder, rowCount)
                }
                else -> {
                    // JSON: DuckDB exports JSON columns as Utf8 (the JSON text) over Arrow, same
                    // as VARCHAR. Wrap the bytes into the JSON Slice via the SPI writeSlice.
                    if (isJsonType(type)) {
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
                    else {
                        throw TrinoException(
                                NOT_SUPPORTED,
                                "DuckBridge Arrow reader does not yet support type: $type")
                    }
                }
            }
            return builder.build()
        }

        /** Writes a whole complex (ARRAY/ROW/MAP) column: one entry per row, null-aware at the
         * top level; entry contents go through the recursive [appendNestedValue] machinery. */
        private fun writeComplexColumn(type: Type, vector: FieldVector, builder: BlockBuilder, rowCount: Int) {
            for (row in 0 until rowCount) {
                appendNestedValue(type, vector, row, builder)
            }
        }

        /**
         * Appends the (non-null) ARRAY value at [row] of [vector] as one entry of [builder].
         * Handles both Arrow [FixedSizeListVector] (lance embeddings, `FLOAT[N]`) and variable-width
         * [ListVector]. Element values are written via [appendNestedValue], which also drives
         * recursion for nested complex elements.
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
                            appendNestedValue(elementType, data, base + j, elementBuilder)
                        }
                    }
                }
                is ListVector -> {
                    val data: FieldVector = vector.dataVector
                    val start: Int = vector.getElementStartIndex(row)
                    val end: Int = vector.getElementEndIndex(row)
                    builder.buildEntry<RuntimeException> { elementBuilder ->
                        for (idx in start until end) {
                            appendNestedValue(elementType, data, idx, elementBuilder)
                        }
                    }
                }
                else -> throw TrinoException(
                        NOT_SUPPORTED,
                        "DuckBridge Arrow reader: ARRAY backed by unsupported Arrow vector ${vector.javaClass.simpleName}")
            }
        }

        /**
         * Appends the (non-null) ROW value at [row] of [vector] as one entry of [builder].
         * Arrow struct children are matched to Trino row fields POSITIONALLY — both sides carry
         * the catalog's declared field order (DuckDB exports struct entries in declaration order).
         */
        private fun appendRowEntry(type: RowType, vector: StructVector, row: Int, builder: RowBlockBuilder) {
            val fields: List<RowType.Field> = type.fields
            if (vector.size() < fields.size) {
                throw TrinoException(
                        NOT_SUPPORTED,
                        "DuckBridge Arrow reader: Arrow struct has ${vector.size()} children but Trino ROW declares ${fields.size} fields")
            }
            builder.buildEntry<RuntimeException> { fieldBuilders ->
                for (f in fields.indices) {
                    appendNestedValue(fields[f].type, vector.getChildByOrdinal(f) as FieldVector, row, fieldBuilders[f])
                }
            }
        }

        /**
         * Appends the (non-null) MAP value at [row] of [vector] as one entry of [builder]. An
         * Arrow Map is list-shaped: per-row offsets into an entries struct whose two children are
         * the flattened keys and values. Keys are non-null by both Arrow and Trino contract;
         * values are null-aware via [appendNestedValue].
         */
        private fun appendMapEntry(type: MapType, vector: MapVector, row: Int, builder: MapBlockBuilder) {
            val entries: StructVector = vector.dataVector as StructVector
            val keyChild: FieldVector = entries.getChildByOrdinal(0) as FieldVector
            val valueChild: FieldVector = entries.getChildByOrdinal(1) as FieldVector
            val start: Int = vector.getElementStartIndex(row)
            val end: Int = vector.getElementEndIndex(row)
            builder.buildEntry<RuntimeException> { keyBuilder, valueBuilder ->
                for (idx in start until end) {
                    appendNestedValue(type.keyType, keyChild, idx, keyBuilder)
                    appendNestedValue(type.valueType, valueChild, idx, valueBuilder)
                }
            }
        }

        /**
         * Appends a single value of Trino [type] read from [vector] at [i] into [builder].
         * Null-aware (uses the vector's own validity). This is the recursive workhorse for all
         * complex-type contents — it covers the FULL supported type surface (every scalar
         * including timestamps/timestamptz/uuid/decimal, plus nested ARRAY/ROW/MAP), so any
         * supported type can appear at any nesting depth.
         */
        private fun appendNestedValue(type: Type, vector: FieldVector, i: Int, builder: BlockBuilder) {
            if (vector.isNull(i)) {
                builder.appendNull()
                return
            }
            when {
                type is DecimalType -> appendDecimalElement(type, vector as DecimalVector, i, builder)
                type is ArrayType -> appendArrayEntry(type, vector, i, builder as ArrayBlockBuilder)
                type is RowType -> appendRowEntry(type, vector as StructVector, i, builder as RowBlockBuilder)
                type is MapType -> appendMapEntry(type, vector as MapVector, i, builder as MapBlockBuilder)
                type is TimestampType -> appendTimestampValue(type, vector, i, builder)
                type is TimestampWithTimeZoneType -> appendTimestampTzValue(type, vector, i, builder)
                type == UuidType.UUID -> {
                    // Utf8-encoded over the Arrow exchange, like the column-level path.
                    val s = String((vector as VarCharVector).get(i), Charsets.UTF_8)
                    UuidType.UUID.writeSlice(builder, UuidType.javaUuidToTrinoUuid(java.util.UUID.fromString(s)))
                }
                else -> appendScalarElement(type, vector, i, builder)
            }
        }

        /** Appends one (non-null) primitive value. Split from [appendNestedValue] to keep each
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
                        "DuckBridge Arrow reader does not yet support nested value type: $type")
            }
        }

        /** Per-value TIMESTAMP write for nested positions; same unit math as [writeTimestampColumn]. */
        private fun appendTimestampValue(type: TimestampType, vector: FieldVector, i: Int, builder: BlockBuilder) {
            when (vector) {
                is TimeStampSecVector ->
                    writeTimestampMicrosWithRemainder(type, builder, Math.multiplyExact(vector.get(i), 1_000_000L), 0)
                is TimeStampMilliVector ->
                    writeTimestampMicrosWithRemainder(type, builder, Math.multiplyExact(vector.get(i), 1_000L), 0)
                is TimeStampMicroVector ->
                    writeTimestampMicrosWithRemainder(type, builder, vector.get(i), 0)
                is TimeStampNanoVector -> {
                    val nanos = vector.get(i)
                    writeTimestampMicrosWithRemainder(type, builder,
                            Math.floorDiv(nanos, 1_000L), (Math.floorMod(nanos, 1_000L) * 1_000L).toInt())
                }
                else -> throw TrinoException(
                        NOT_SUPPORTED,
                        "Unsupported Arrow timestamp vector for nested Trino type $type: ${vector.javaClass.simpleName}")
            }
        }

        /** Per-value TIMESTAMP WITH TIME ZONE write for nested positions; same unit math and
         * schema-TZ resolution as [writeTimestampTzColumn] (the zone lookup is a map hit). */
        private fun appendTimestampTzValue(type: TimestampWithTimeZoneType, vector: FieldVector, i: Int, builder: BlockBuilder) {
            val zone = resolveTimeZoneKey(vector)
            when (vector) {
                is TimeStampSecTZVector ->
                    writeTimestampTz(type, builder, Math.multiplyExact(vector.get(i), 1_000L), 0, zone)
                is TimeStampMilliTZVector ->
                    writeTimestampTz(type, builder, vector.get(i), 0, zone)
                is TimeStampMicroTZVector -> {
                    val micros = vector.get(i)
                    writeTimestampTz(type, builder,
                            Math.floorDiv(micros, 1_000L), (Math.floorMod(micros, 1_000L) * 1_000_000L).toInt(), zone)
                }
                is TimeStampNanoTZVector -> {
                    val nanos = vector.get(i)
                    writeTimestampTz(type, builder,
                            Math.floorDiv(nanos, 1_000_000L), (Math.floorMod(nanos, 1_000_000L) * 1_000L).toInt(), zone)
                }
                else -> throw TrinoException(
                        NOT_SUPPORTED,
                        "Unsupported Arrow timestamp_tz vector for nested Trino type $type: ${vector.javaClass.simpleName}")
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

        @Suppress("CyclomaticComplexMethod") // dispatch over Arrow timestamp precision variants
        private fun writeTimestampColumn(type: TimestampType, vector: FieldVector, builder: BlockBuilder, rowCount: Int) {
            // Trino stores TIMESTAMP at micros-since-epoch in a long for precision <= 6,
            // and as LongTimestamp(micros, picosOfMicro) for precision <= 9.
            when (vector) {
                is TimeStampSecVector -> {
                    for (i in 0 until rowCount) {
                        if (vector.isNull(i)) {
                            builder.appendNull()
                            continue
                        }
                        val micros = Math.multiplyExact(vector.get(i), 1_000_000L)
                        writeTimestampMicrosWithRemainder(type, builder, micros, 0)
                    }
                }
                is TimeStampMilliVector -> {
                    for (i in 0 until rowCount) {
                        if (vector.isNull(i)) {
                            builder.appendNull()
                            continue
                        }
                        val micros = Math.multiplyExact(vector.get(i), 1_000L)
                        writeTimestampMicrosWithRemainder(type, builder, micros, 0)
                    }
                }
                is TimeStampMicroVector -> {
                    for (i in 0 until rowCount) {
                        if (vector.isNull(i)) {
                            builder.appendNull()
                            continue
                        }
                        writeTimestampMicrosWithRemainder(type, builder, vector.get(i), 0)
                    }
                }
                is TimeStampNanoVector -> {
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
                else -> {
                    throw TrinoException(
                            NOT_SUPPORTED,
                            "Unsupported Arrow timestamp vector for Trino type $type: ${vector.javaClass.simpleName}")
                }
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

        @Suppress("CyclomaticComplexMethod") // dispatch over Arrow timestamptz precision variants
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
            when (vector) {
                is TimeStampSecTZVector -> {
                    for (i in 0 until rowCount) {
                        if (vector.isNull(i)) {
                            builder.appendNull()
                            continue
                        }
                        writeTimestampTz(type, builder, Math.multiplyExact(vector.get(i), 1_000L), 0, zone)
                    }
                }
                is TimeStampMilliTZVector -> {
                    for (i in 0 until rowCount) {
                        if (vector.isNull(i)) {
                            builder.appendNull()
                            continue
                        }
                        writeTimestampTz(type, builder, vector.get(i), 0, zone)
                    }
                }
                is TimeStampMicroTZVector -> {
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
                is TimeStampNanoTZVector -> {
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
                else -> {
                    throw TrinoException(
                            NOT_SUPPORTED,
                            "Unsupported Arrow timestamp_tz vector for Trino type $type: ${vector.javaClass.simpleName}")
                }
            }
        }

        private val UNPARSEABLE_TZ_WARNED: java.util.concurrent.ConcurrentHashMap<String, Boolean> =
                java.util.concurrent.ConcurrentHashMap()

        // Intentional log-and-fallback-to-UTC for an unparseable Arrow zone string; the exception
        // carries no extra actionable info beyond the zone string we already log.
        @Suppress("SwallowedException")
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
