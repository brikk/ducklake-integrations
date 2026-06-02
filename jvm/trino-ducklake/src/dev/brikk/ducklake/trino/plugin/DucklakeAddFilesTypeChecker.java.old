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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

/**
 * Verifies that a parquet column's logical type is compatible with the target
 * DuckLake table column type. Implements the same widening rules as upstream's
 * {@code DuckLakeParquetTypeChecker} (signed-int / unsigned-int widening,
 * float→double widening, timestamp-precision compatibility, decimal
 * precision/scale fit, exact-match otherwise).
 *
 * <p>Operates on Trino's {@link Type} hierarchy on both sides: the source side
 * is derived from the parquet schema by Trino's existing
 * {@link io.trino.parquet.ParquetTypeUtils} machinery, and the target side
 * comes from the DuckLake catalog's stored column type.
 */
final class DucklakeAddFilesTypeChecker
{
    private DucklakeAddFilesTypeChecker() {}

    /**
     * @throws DucklakeAddFilesException when source is not assignable to target.
     */
    static void checkCompatible(Type target, Type source, String columnPath, String fileName, String tableName)
    {
        if (isCompatible(target, source)) {
            return;
        }
        throw new DucklakeAddFilesException(String.format(
                "Failed to map column \"%s\" from file \"%s\" to table \"%s\": expected %s, found %s",
                columnPath, fileName, tableName, describe(target), describe(source)));
    }

    private static boolean isCompatible(Type target, Type source)
    {
        if (target.equals(source)) {
            return true;
        }
        if (target instanceof TinyintType
                || target instanceof SmallintType
                || target instanceof IntegerType
                || target instanceof BigintType) {
            return isSignedIntCompatible(target, source);
        }
        if (target instanceof RealType) {
            return source instanceof RealType;
        }
        if (target instanceof DoubleType) {
            return source instanceof RealType || source instanceof DoubleType;
        }
        if (target instanceof TimestampType targetTs && source instanceof TimestampType sourceTs) {
            // Upstream accepts widening between timestamp precisions; require equal precision
            // for strict round-trip, but accept smaller-precision source.
            return sourceTs.getPrecision() <= targetTs.getPrecision();
        }
        if (target instanceof TimestampWithTimeZoneType targetTsTz && source instanceof TimestampWithTimeZoneType sourceTsTz) {
            return sourceTsTz.getPrecision() <= targetTsTz.getPrecision();
        }
        if (target instanceof TimeType targetT && source instanceof TimeType sourceT) {
            return sourceT.getPrecision() <= targetT.getPrecision();
        }
        if (target instanceof TimeWithTimeZoneType targetT && source instanceof TimeWithTimeZoneType sourceT) {
            return sourceT.getPrecision() <= targetT.getPrecision();
        }
        if (target instanceof DecimalType targetD && source instanceof DecimalType sourceD) {
            return sourceD.getPrecision() <= targetD.getPrecision()
                    && sourceD.getScale() <= targetD.getScale();
        }
        if (target instanceof VarcharType targetVc && source instanceof VarcharType sourceVc) {
            // Trino VARCHAR(N) — unbounded target accepts any bounded source.
            return targetVc.isUnbounded() || (!sourceVc.isUnbounded() && sourceVc.getBoundedLength() <= targetVc.getBoundedLength());
        }
        if (target instanceof CharType targetC && source instanceof CharType sourceC) {
            return sourceC.getLength() <= targetC.getLength();
        }
        // BLOB / UUID / BOOLEAN / DATE / nested types: exact-match handled by .equals() at top.
        return false;
    }

    private static boolean isSignedIntCompatible(Type target, Type source)
    {
        int targetWidth = signedIntWidth(target);
        int sourceWidth = signedIntWidth(source);
        return sourceWidth > 0 && sourceWidth <= targetWidth;
    }

    private static int signedIntWidth(Type type)
    {
        if (type instanceof TinyintType) {
            return 1;
        }
        if (type instanceof SmallintType) {
            return 2;
        }
        if (type instanceof IntegerType) {
            return 4;
        }
        if (type instanceof BigintType) {
            return 8;
        }
        return 0;
    }

    private static String describe(Type type)
    {
        if (type instanceof ArrayType arr) {
            return "array(" + describe(arr.getElementType()) + ")";
        }
        if (type instanceof MapType map) {
            return "map(" + describe(map.getKeyType()) + "," + describe(map.getValueType()) + ")";
        }
        if (type instanceof RowType row) {
            StringBuilder sb = new StringBuilder("row(");
            boolean first = true;
            for (RowType.Field f : row.getFields()) {
                if (!first) {
                    sb.append(",");
                }
                first = false;
                f.getName().ifPresent(n -> sb.append(n).append(" "));
                sb.append(describe(f.getType()));
            }
            sb.append(")");
            return sb.toString();
        }
        if (type instanceof BooleanType) {
            return "boolean";
        }
        if (type instanceof VarbinaryType) {
            return "varbinary";
        }
        if (type instanceof UuidType) {
            return "uuid";
        }
        if (type instanceof DateType) {
            return "date";
        }
        return type.getDisplayName();
    }
}
