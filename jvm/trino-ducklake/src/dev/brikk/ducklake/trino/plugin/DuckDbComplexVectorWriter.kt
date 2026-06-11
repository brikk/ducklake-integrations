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
import io.trino.spi.block.Block
import io.trino.spi.block.SqlMap
import io.trino.spi.block.SqlRow
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
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
import org.apache.arrow.vector.TimeStampMicroVector
import org.apache.arrow.vector.TimeStampMilliTZVector
import org.apache.arrow.vector.TimeStampMilliVector
import org.apache.arrow.vector.TimeStampNanoTZVector
import org.apache.arrow.vector.TimeStampNanoVector
import org.apache.arrow.vector.TimeStampSecVector
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.complex.impl.UnionListWriter
import java.lang.Float.intBitsToFloat
import java.lang.Math.floorDiv
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets

/**
 * Trino-block → Arrow-vector population for the complex types the Arrow-stream writer supports
 * (ARRAY / ROW / MAP), split out of [DuckDbArrowStreamFileWriter]'s companion to keep that class
 * navigable. Column-level entry points are [populateListVector], [populateStructVector] and
 * [populateMapVector]; their contents go through [writeNestedValue], the per-value random-access
 * counterpart of the writer's per-column loops, which is what lets complex types nest (ROW in
 * ROW, ARRAY in ROW, ROW in MAP values, ...). MAP nested inside another complex type stays
 * fail-fast — per-value map offset bookkeeping is not worth it until a use case shows up.
 */
internal object DuckDbComplexVectorWriter {

    /** Whether [type] is or contains a MAP at any nesting depth. */
    fun containsMapType(type: Type): Boolean = when (type) {
        is MapType -> true
        is ArrayType -> containsMapType(type.elementType)
        is RowType -> type.fields.any { containsMapType(it.type) }
        else -> false
    }

    /** Whether [type] is or contains a ROW at any nesting depth. */
    fun containsRowType(type: Type): Boolean = when (type) {
        is RowType -> true
        is ArrayType -> containsRowType(type.elementType)
        is MapType -> containsRowType(type.keyType) || containsRowType(type.valueType)
        else -> false
    }

    /**
     * ARRAY(scalar) → Arrow List. NULL *rows* are fine (unwritten slots become null —
     * [ListVector.startNewValue] back-fills skipped offsets, and the caller's
     * `vector.valueCount = rowCount` finalizes the tail). NULL *elements* are rejected:
     * the list writer's per-type sub-writers have no positional null append, and the only
     * driving use case (embedding vectors) never carries null components.
     */
    fun populateListVector(vector: ListVector, type: ArrayType, block: Block, rowCount: Int) {
        vector.allocateNew()
        val writer: UnionListWriter = vector.writer
        val elementType: Type = type.elementType
        for (i in 0 until rowCount) {
            if (block.isNull(i)) {
                continue
            }
            writer.setPosition(i)
            writer.startList()
            val elements: Block = type.getObject(block, i) as Block
            for (j in 0 until elements.positionCount) {
                if (elements.isNull(j)) {
                    throw TrinoException(NOT_SUPPORTED,
                            "DuckDB Arrow-stream writer does not support NULL elements in array columns")
                }
                appendListElement(writer, elementType, elements, j)
            }
            writer.endList()
        }
    }

    /**
     * ROW → Arrow Struct. NULL rows set struct validity null (children left unwritten at that
     * index); field values are written per-value into the positional children via
     * [writeNestedValue], so any ROW-supported type can appear as a field — including nested
     * ROW (recursion) and ARRAY of scalar.
     */
    fun populateStructVector(vector: StructVector, type: RowType, block: Block, rowCount: Int) {
        vector.allocateNew()
        for (i in 0 until rowCount) {
            if (block.isNull(i)) {
                vector.setNull(i)
                continue
            }
            writeStructValue(vector, type, type.getObject(block, i) as SqlRow, i)
        }
    }

    /**
     * MAP → Arrow Map (list-shaped offsets over a non-null entries struct with key/value
     * children). NULL rows are skipped like [populateListVector] (unwritten slots become null;
     * `startNewValue` back-fills offsets and the final valueCount finalizes the tail). Keys
     * are non-null by Trino contract; values go through the null-aware [writeNestedValue].
     */
    fun populateMapVector(vector: MapVector, type: MapType, block: Block, rowCount: Int) {
        vector.allocateNew()
        val entries: StructVector = vector.dataVector as StructVector
        val keyChild: FieldVector = entries.getChildByOrdinal(0) as FieldVector
        val valueChild: FieldVector = entries.getChildByOrdinal(1) as FieldVector
        var entryIndex = 0
        for (i in 0 until rowCount) {
            if (block.isNull(i)) {
                continue
            }
            vector.startNewValue(i)
            val map: SqlMap = type.getObject(block, i) as SqlMap
            val rawKeys: Block = map.rawKeyBlock
            val rawValues: Block = map.rawValueBlock
            val offset: Int = map.rawOffset
            for (e in 0 until map.size) {
                entries.setIndexDefined(entryIndex)
                writeNestedValue(keyChild, type.keyType, rawKeys, offset + e, entryIndex)
                writeNestedValue(valueChild, type.valueType, rawValues, offset + e, entryIndex)
                entryIndex++
            }
            vector.endValue(i, map.size)
        }
    }

    /** Writes one (non-null) ROW value into [vector] at [target]. */
    private fun writeStructValue(vector: StructVector, type: RowType, row: SqlRow, target: Int) {
        vector.setIndexDefined(target)
        val fields: List<RowType.Field> = type.fields
        for (f in fields.indices) {
            writeNestedValue(vector.getChildByOrdinal(f) as FieldVector, fields[f].type,
                    row.getRawFieldBlock(f), row.rawIndex, target)
        }
    }

    /**
     * Writes one value of [type] from [source] at [pos] into [vector] at [target] — the
     * per-value (random-access) counterpart of the writer's per-column loops, used for the
     * contents of STRUCT fields and MAP keys/values. Null-aware. Covers every scalar the column
     * level does plus nested ROW (recursion) and ARRAY of scalar elements.
     */
    @Suppress("CyclomaticComplexMethod", "LongMethod")
    private fun writeNestedValue(vector: FieldVector, type: Type, source: Block, pos: Int, target: Int) {
        when {
            type == BOOLEAN -> {
                val v = vector as BitVector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, if (BOOLEAN.getBoolean(source, pos)) 1 else 0)
            }
            type == TINYINT -> {
                val v = vector as TinyIntVector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, TINYINT.getByte(source, pos).toInt())
            }
            type == SMALLINT -> {
                val v = vector as SmallIntVector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, SMALLINT.getShort(source, pos).toInt())
            }
            type == INTEGER -> {
                val v = vector as IntVector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, INTEGER.getInt(source, pos))
            }
            type == BIGINT -> {
                val v = vector as BigIntVector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, BIGINT.getLong(source, pos))
            }
            type == REAL -> {
                val v = vector as Float4Vector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, intBitsToFloat(REAL.getInt(source, pos)))
            }
            type == DOUBLE -> {
                val v = vector as Float8Vector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, DOUBLE.getDouble(source, pos))
            }
            type == DATE -> {
                val v = vector as DateDayVector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, DATE.getInt(source, pos))
            }
            type == VARBINARY -> {
                val v = vector as VarBinaryVector
                if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, VARBINARY.getSlice(source, pos).bytes)
            }
            type is VarcharType -> {
                val v = vector as VarCharVector
                if (source.isNull(pos)) v.setNull(target)
                else v.setSafe(target, type.getSlice(source, pos).toStringUtf8().toByteArray(StandardCharsets.UTF_8))
            }
            type == UuidType.UUID -> {
                // Utf8 over the Arrow exchange, like the column-level path.
                val v = vector as VarCharVector
                if (source.isNull(pos)) v.setNull(target)
                else v.setSafe(target, UuidType.trinoUuidToJavaUuid(UuidType.UUID.getSlice(source, pos))
                        .toString().toByteArray(StandardCharsets.UTF_8))
            }
            type is DecimalType -> writeNestedDecimal(vector as DecimalVector, type, source, pos, target)
            type is TimestampType -> writeNestedTimestamp(vector, type, source, pos, target)
            type is TimestampWithTimeZoneType -> writeNestedTimestampTz(vector, type, source, pos, target)
            type is RowType -> {
                val v = vector as StructVector
                if (source.isNull(pos)) v.setNull(target)
                else writeStructValue(v, type, type.getObject(source, pos) as SqlRow, target)
            }
            type is ArrayType -> writeNestedList(vector as ListVector, type, source, pos, target)
            else -> throw TrinoException(NOT_SUPPORTED,
                    "DuckDB Arrow-stream writer does not yet support type nested in STRUCT/MAP: $type")
        }
    }

    /** One ARRAY(scalar) value at a random [target] position, via the list writer's
     * positional API; same element surface + NULL-element rejection as [populateListVector].
     * A null value leaves the slot unwritten (becomes null when offsets are finalized). */
    private fun writeNestedList(vector: ListVector, type: ArrayType, source: Block, pos: Int, target: Int) {
        if (source.isNull(pos)) {
            return
        }
        val writer: UnionListWriter = vector.writer
        writer.setPosition(target)
        writer.startList()
        val elements: Block = type.getObject(source, pos) as Block
        for (j in 0 until elements.positionCount) {
            if (elements.isNull(j)) {
                throw TrinoException(NOT_SUPPORTED,
                        "DuckDB Arrow-stream writer does not support NULL elements in array columns")
            }
            appendListElement(writer, type.elementType, elements, j)
        }
        writer.endList()
    }

    private fun appendListElement(writer: UnionListWriter, elementType: Type, elements: Block, j: Int) {
        when {
            elementType == REAL -> writer.float4().writeFloat4(intBitsToFloat(REAL.getInt(elements, j)))
            elementType == DOUBLE -> writer.float8().writeFloat8(DOUBLE.getDouble(elements, j))
            elementType == INTEGER -> writer.integer().writeInt(INTEGER.getInt(elements, j))
            elementType == BIGINT -> writer.bigInt().writeBigInt(BIGINT.getLong(elements, j))
            elementType == SMALLINT -> writer.smallInt().writeSmallInt(SMALLINT.getShort(elements, j))
            elementType == TINYINT -> writer.tinyInt().writeTinyInt(TINYINT.getByte(elements, j))
            elementType == BOOLEAN -> writer.bit().writeBit(if (BOOLEAN.getBoolean(elements, j)) 1 else 0)
            elementType is VarcharType -> writer.varChar().writeVarChar(
                    org.apache.arrow.vector.util.Text(elementType.getSlice(elements, j).toStringUtf8()))
            else -> throw TrinoException(NOT_SUPPORTED,
                    "DuckDB Arrow-stream writer does not yet support array element type: $elementType")
        }
    }

    /** One DECIMAL value at a random position; same short/long extraction as the column path. */
    private fun writeNestedDecimal(vector: DecimalVector, type: DecimalType, source: Block, pos: Int, target: Int) {
        if (source.isNull(pos)) {
            vector.setNull(target)
            return
        }
        val value: BigDecimal = if (type.isShort) {
            BigDecimal.valueOf(type.getLong(source, pos), type.scale)
        }
        else {
            BigDecimal(BigInteger((type.getObject(source, pos) as Int128).toBigEndianBytes()), type.scale)
        }
        vector.setSafe(target, value)
    }

    /** One TIMESTAMP value at a random position; same per-precision math as the column path. */
    private fun writeNestedTimestamp(vector: FieldVector, type: TimestampType, source: Block, pos: Int, target: Int) {
        if (source.isNull(pos)) {
            when (vector) {
                is TimeStampSecVector -> vector.setNull(target)
                is TimeStampMilliVector -> vector.setNull(target)
                is TimeStampMicroVector -> vector.setNull(target)
                is TimeStampNanoVector -> vector.setNull(target)
                else -> throw TrinoException(NOT_SUPPORTED,
                        "Unsupported TIMESTAMP precision nested in STRUCT/MAP: ${type.precision}")
            }
            return
        }
        when (type.precision) {
            0 -> (vector as TimeStampSecVector).setSafe(target, floorDiv(type.getLong(source, pos), 1_000_000L))
            3 -> (vector as TimeStampMilliVector).setSafe(target, floorDiv(type.getLong(source, pos), 1_000L))
            6 -> (vector as TimeStampMicroVector).setSafe(target, type.getLong(source, pos))
            9 -> {
                val v = vector as TimeStampNanoVector
                if (type.isShort) {
                    v.setSafe(target, Math.multiplyExact(type.getLong(source, pos), 1_000L))
                }
                else {
                    val ts: LongTimestamp = type.getObject(source, pos) as LongTimestamp
                    v.setSafe(target, Math.multiplyExact(ts.epochMicros, 1_000L) + ts.picosOfMicro / 1_000)
                }
            }
            else -> throw TrinoException(NOT_SUPPORTED,
                    "Unsupported TIMESTAMP precision nested in STRUCT/MAP: ${type.precision}")
        }
    }

    /** One TIMESTAMPTZ value at a random position; same packing as the column path. */
    private fun writeNestedTimestampTz(vector: FieldVector, type: TimestampWithTimeZoneType, source: Block, pos: Int, target: Int) {
        if (type.isShort) {
            val v = vector as TimeStampMilliTZVector
            if (source.isNull(pos)) v.setNull(target) else v.setSafe(target, unpackMillisUtc(type.getLong(source, pos)))
            return
        }
        val v = vector as TimeStampNanoTZVector
        if (source.isNull(pos)) {
            v.setNull(target)
            return
        }
        val ts: LongTimestampWithTimeZone = type.getObject(source, pos) as LongTimestampWithTimeZone
        v.setSafe(target, Math.multiplyExact(ts.epochMillis, 1_000_000L) + ts.picosOfMilli / 1_000)
    }
}
