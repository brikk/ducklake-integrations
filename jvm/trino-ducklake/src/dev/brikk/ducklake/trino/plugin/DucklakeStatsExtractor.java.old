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

import com.google.common.collect.ImmutableList;
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats;
import dev.brikk.ducklake.catalog.DucklakeStatTypes;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Statistics;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

/**
 * Aggregates per-row-group parquet statistics into one {@link DucklakeFileColumnStats}
 * row per parquet leaf, keyed by the DuckLake catalog field_id at that leaf
 * (top-level column_id for flat columns; child column_id for nested struct /
 * array / map leaves).
 *
 * <p>Caller is responsible for projecting the leaf list — see
 * {@link DucklakeStatsLeafProjector} (write path) and
 * {@link DucklakeAddFilesNameMapper} (add_files path). Both produce a
 * {@link LeafStatsTarget} list in the same order parquet writes leaves into
 * {@code RowGroup.columns}.
 */
public final class DucklakeStatsExtractor
{
    private DucklakeStatsExtractor() {}

    public static List<DucklakeFileColumnStats> extractStats(
            FileMetaData fileMetaData,
            List<LeafStatsTarget> leafTargets)
    {
        ImmutableList.Builder<DucklakeFileColumnStats> result = ImmutableList.builder();

        for (LeafStatsTarget target : leafTargets) {
            int parquetColumnIndex = target.parquetColumnIndex();
            Type type = target.leafType();
            boolean numeric = isNumericTrinoType(type);

            long totalCompressedSize = 0;
            long totalValueCount = 0;
            long totalNullCount = 0;
            boolean containsNan = false;
            Optional<String> minValue = Optional.empty();
            Optional<String> maxValue = Optional.empty();
            boolean hasStats = false;

            for (RowGroup rowGroup : fileMetaData.getRow_groups()) {
                if (parquetColumnIndex >= rowGroup.getColumns().size()) {
                    continue;
                }
                ColumnMetaData columnMeta = rowGroup.getColumns().get(parquetColumnIndex).getMeta_data();
                totalCompressedSize += columnMeta.getTotal_compressed_size();
                totalValueCount += columnMeta.getNum_values();

                if (columnMeta.isSetStatistics()) {
                    Statistics stats = columnMeta.getStatistics();
                    if (stats.isSetNull_count()) {
                        totalNullCount += stats.getNull_count();
                    }

                    if (stats.isSetMin_value() && stats.isSetMax_value()) {
                        hasStats = true;
                        Optional<String> groupMin = convertStatValue(stats.getMin_value(), type, columnMeta.getType());
                        Optional<String> groupMax = convertStatValue(stats.getMax_value(), type, columnMeta.getType());

                        if (groupMin.isPresent()) {
                            minValue = minValue.isEmpty() ? groupMin : Optional.of(DucklakeStatTypes.min(minValue.get(), groupMin.get(), numeric));
                        }
                        if (groupMax.isPresent()) {
                            maxValue = maxValue.isEmpty() ? groupMax : Optional.of(DucklakeStatTypes.max(maxValue.get(), groupMax.get(), numeric));
                        }
                    }
                }
            }

            result.add(new DucklakeFileColumnStats(
                    target.fieldId(),
                    totalCompressedSize,
                    totalValueCount,
                    totalNullCount,
                    hasStats ? minValue : Optional.empty(),
                    hasStats ? maxValue : Optional.empty(),
                    containsNan));
        }

        return result.build();
    }

    static Optional<String> convertStatValue(byte[] value, Type type)
    {
        return convertStatValue(value, type, null);
    }

    static Optional<String> convertStatValue(byte[] value, Type type, org.apache.parquet.format.Type physicalType)
    {
        if (value == null || value.length == 0) {
            return Optional.empty();
        }

        try {
            if (type instanceof BooleanType) {
                return Optional.of(value[0] != 0 ? "true" : "false");
            }
            if (type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
                int intVal = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
                return Optional.of(String.valueOf(intVal));
            }
            if (type instanceof BigintType) {
                long longVal = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
                return Optional.of(String.valueOf(longVal));
            }
            if (type instanceof RealType) {
                float floatVal = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getFloat();
                if (Float.isNaN(floatVal)) {
                    return Optional.empty();
                }
                return Optional.of(String.valueOf(floatVal));
            }
            if (type instanceof DoubleType) {
                double doubleVal = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getDouble();
                if (Double.isNaN(doubleVal)) {
                    return Optional.empty();
                }
                return Optional.of(String.valueOf(doubleVal));
            }
            if (type instanceof VarcharType) {
                return Optional.of(new String(value, java.nio.charset.StandardCharsets.UTF_8));
            }
            if (type instanceof VarbinaryType || type instanceof UuidType) {
                return Optional.empty();
            }
            if (type instanceof DateType) {
                int daysSinceEpoch = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
                return Optional.of(LocalDate.ofEpochDay(daysSinceEpoch).toString());
            }
            if (type instanceof TimestampType timestampType) {
                long micros = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
                LocalDateTime dateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L),
                        ZoneOffset.UTC);
                return Optional.of(dateTime.toString());
            }
            if (type instanceof TimestampWithTimeZoneType) {
                long micros = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
                Instant instant = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L);
                return Optional.of(instant.toString());
            }
            if (type instanceof DecimalType decimalType) {
                BigInteger unscaled = decodeDecimalUnscaled(value, physicalType);
                BigDecimal decimal = new BigDecimal(unscaled, decimalType.getScale());
                return Optional.of(decimal.toPlainString());
            }

            return Optional.empty();
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Parquet stores DECIMAL statistics in different byte orders depending on the
     * physical type: INT32/INT64-backed decimals are little-endian two's-complement
     * (matching the primitive layout), while FIXED_LEN_BYTE_ARRAY / BINARY decimals
     * are big-endian two's-complement. Decoding the short INT32/INT64 forms as
     * big-endian ({@code new BigInteger(byte[])}) silently corrupts min/max for
     * low-precision decimals.
     */
    private static BigInteger decodeDecimalUnscaled(byte[] value, org.apache.parquet.format.Type physicalType)
    {
        if (physicalType == org.apache.parquet.format.Type.INT32) {
            return BigInteger.valueOf(ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt());
        }
        if (physicalType == org.apache.parquet.format.Type.INT64) {
            return BigInteger.valueOf(ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong());
        }
        // FIXED_LEN_BYTE_ARRAY / BINARY (and the unknown/test path): big-endian two's complement.
        return new BigInteger(value);
    }

    private static boolean isNumericTrinoType(Type type)
    {
        return type instanceof TinyintType
                || type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType
                || type instanceof RealType
                || type instanceof DoubleType
                || type instanceof DecimalType;
    }
}
