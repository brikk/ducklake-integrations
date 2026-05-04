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
package dev.brikk.ducklake.trino.plugin;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.String.format;

/**
 * Materializes one {@link VectorSchemaRoot} batch from DuckDB's Arrow export stream
 * into a Trino {@link Page}, dispatching per-column on the resolved Trino {@link Type}.
 *
 * <p>Supported types track the Phase 1 writer surface (scalars only). Nested types,
 * unsupported timestamp precisions, and unknown vector classes raise
 * {@link io.trino.spi.StandardErrorCode#NOT_SUPPORTED}.
 */
final class DucklakeArrowToPageConverter
{
    private final List<Type> columnTypes;

    DucklakeArrowToPageConverter(List<Type> columnTypes)
    {
        this.columnTypes = List.copyOf(columnTypes);
    }

    Page convert(VectorSchemaRoot root)
    {
        int rowCount = root.getRowCount();
        int columnCount = columnTypes.size();
        if (root.getFieldVectors().size() != columnCount) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    format("DuckDB Arrow batch has %d columns but %d projected",
                            root.getFieldVectors().size(), columnCount));
        }
        Block[] blocks = new Block[columnCount];
        for (int i = 0; i < columnCount; i++) {
            blocks[i] = convertColumn(columnTypes.get(i), root.getVector(i), rowCount);
        }
        return new Page(rowCount, blocks);
    }

    private static Block convertColumn(Type type, FieldVector vector, int rowCount)
    {
        BlockBuilder builder = type.createBlockBuilder(null, rowCount);
        if (type.equals(BOOLEAN)) {
            BitVector v = (BitVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    BOOLEAN.writeBoolean(builder, v.get(i) != 0);
                }
            }
        }
        else if (type.equals(TINYINT)) {
            TinyIntVector v = (TinyIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    TINYINT.writeLong(builder, v.get(i));
                }
            }
        }
        else if (type.equals(SMALLINT)) {
            SmallIntVector v = (SmallIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    SMALLINT.writeLong(builder, v.get(i));
                }
            }
        }
        else if (type.equals(INTEGER)) {
            IntVector v = (IntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    INTEGER.writeLong(builder, v.get(i));
                }
            }
        }
        else if (type.equals(BIGINT)) {
            BigIntVector v = (BigIntVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    BIGINT.writeLong(builder, v.get(i));
                }
            }
        }
        else if (type.equals(REAL)) {
            Float4Vector v = (Float4Vector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    REAL.writeLong(builder, Float.floatToRawIntBits(v.get(i)));
                }
            }
        }
        else if (type.equals(DOUBLE)) {
            Float8Vector v = (Float8Vector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    DOUBLE.writeDouble(builder, v.get(i));
                }
            }
        }
        else if (type.equals(DATE)) {
            DateDayVector v = (DateDayVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    DATE.writeLong(builder, v.get(i));
                }
            }
        }
        else if (type.equals(VARBINARY)) {
            VarBinaryVector v = (VarBinaryVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    VARBINARY.writeSlice(builder, Slices.wrappedBuffer(v.get(i)));
                }
            }
        }
        else if (type instanceof VarcharType varcharType) {
            VarCharVector v = (VarCharVector) vector;
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    varcharType.writeSlice(builder, Slices.wrappedBuffer(v.get(i)));
                }
            }
        }
        else if (type instanceof DecimalType decimalType) {
            writeDecimalColumn(decimalType, (DecimalVector) vector, builder, rowCount);
        }
        else if (type instanceof TimestampType timestampType) {
            writeTimestampColumn(timestampType, vector, builder, rowCount);
        }
        else if (type instanceof TimestampWithTimeZoneType tzType) {
            writeTimestampTzColumn(tzType, vector, builder, rowCount);
        }
        else {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "DuckDB-format reader does not yet support type: " + type);
        }
        return builder.build();
    }

    private static void writeDecimalColumn(DecimalType decimalType, DecimalVector vector, BlockBuilder builder, int rowCount)
    {
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
                continue;
            }
            BigDecimal value = vector.getObject(i);
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, value.unscaledValue().longValueExact());
            }
            else {
                Int128 unscaled = Int128.valueOf(value.unscaledValue());
                decimalType.writeObject(builder, unscaled);
            }
        }
    }

    private static void writeTimestampColumn(TimestampType type, FieldVector vector, BlockBuilder builder, int rowCount)
    {
        // Trino stores TIMESTAMP at micros-since-epoch in a long for precision <= 6,
        // and as LongTimestamp(micros, picosOfMicro) for precision <= 9.
        if (vector instanceof TimeStampSecVector v) {
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                long micros = Math.multiplyExact(v.get(i), 1_000_000L);
                writeTimestampMicrosWithRemainder(type, builder, micros, 0);
            }
        }
        else if (vector instanceof TimeStampMilliVector v) {
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                long micros = Math.multiplyExact(v.get(i), 1_000L);
                writeTimestampMicrosWithRemainder(type, builder, micros, 0);
            }
        }
        else if (vector instanceof TimeStampMicroVector v) {
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                writeTimestampMicrosWithRemainder(type, builder, v.get(i), 0);
            }
        }
        else if (vector instanceof TimeStampNanoVector v) {
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                long nanos = v.get(i);
                long micros = Math.floorDiv(nanos, 1_000L);
                int picosOfMicro = (int) (Math.floorMod(nanos, 1_000L) * 1_000L);
                writeTimestampMicrosWithRemainder(type, builder, micros, picosOfMicro);
            }
        }
        else {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Unsupported Arrow timestamp vector for Trino type " + type + ": " + vector.getClass().getSimpleName());
        }
    }

    private static void writeTimestampMicrosWithRemainder(TimestampType type, BlockBuilder builder, long micros, int picosOfMicro)
    {
        if (type.isShort()) {
            type.writeLong(builder, micros);
        }
        else {
            type.writeObject(builder, new LongTimestamp(micros, picosOfMicro));
        }
    }

    private static void writeTimestampTzColumn(TimestampWithTimeZoneType type, FieldVector vector, BlockBuilder builder, int rowCount)
    {
        if (vector instanceof TimeStampSecTZVector v) {
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                writeTimestampTz(type, builder, Math.multiplyExact(v.get(i), 1_000L), 0);
            }
        }
        else if (vector instanceof TimeStampMilliTZVector v) {
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                writeTimestampTz(type, builder, v.get(i), 0);
            }
        }
        else if (vector instanceof TimeStampMicroTZVector v) {
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                long micros = v.get(i);
                long millis = Math.floorDiv(micros, 1_000L);
                int picosOfMilli = (int) (Math.floorMod(micros, 1_000L) * 1_000_000L);
                writeTimestampTz(type, builder, millis, picosOfMilli);
            }
        }
        else if (vector instanceof TimeStampNanoTZVector v) {
            for (int i = 0; i < rowCount; i++) {
                if (v.isNull(i)) {
                    builder.appendNull();
                    continue;
                }
                long nanos = v.get(i);
                long millis = Math.floorDiv(nanos, 1_000_000L);
                int picosOfMilli = (int) (Math.floorMod(nanos, 1_000_000L) * 1_000L);
                writeTimestampTz(type, builder, millis, picosOfMilli);
            }
        }
        else {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "Unsupported Arrow timestamp_tz vector for Trino type " + type + ": " + vector.getClass().getSimpleName());
        }
    }

    private static void writeTimestampTz(TimestampWithTimeZoneType type, BlockBuilder builder, long epochMillis, int picosOfMilli)
    {
        if (type.isShort()) {
            type.writeLong(builder, packDateTimeWithZone(epochMillis, UTC_KEY));
        }
        else {
            type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY));
        }
    }
}
