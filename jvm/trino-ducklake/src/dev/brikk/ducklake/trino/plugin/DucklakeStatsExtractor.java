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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
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

public final class DucklakeStatsExtractor
{
    private DucklakeStatsExtractor() {}

    public static List<DucklakeFileColumnStats> extractStats(
            FileMetaData fileMetaData,
            List<DucklakeColumnHandle> columns)
    {
        ImmutableList.Builder<DucklakeFileColumnStats> result = ImmutableList.builder();

        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            DucklakeColumnHandle column = columns.get(columnIndex);
            Type type = column.columnType();

            // Skip complex types — no meaningful min/max stats
            if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
                continue;
            }

            long totalCompressedSize = 0;
            long totalValueCount = 0;
            long totalNullCount = 0;
            boolean containsNan = false;
            Optional<String> minValue = Optional.empty();
            Optional<String> maxValue = Optional.empty();
            boolean hasStats = false;

            for (RowGroup rowGroup : fileMetaData.getRow_groups()) {
                if (columnIndex >= rowGroup.getColumns().size()) {
                    continue;
                }
                ColumnMetaData columnMeta = rowGroup.getColumns().get(columnIndex).getMeta_data();
                totalCompressedSize += columnMeta.getTotal_compressed_size();
                totalValueCount += columnMeta.getNum_values();

                if (columnMeta.isSetStatistics()) {
                    Statistics stats = columnMeta.getStatistics();
                    if (stats.isSetNull_count()) {
                        totalNullCount += stats.getNull_count();
                    }

                    if (stats.isSetMin_value() && stats.isSetMax_value()) {
                        hasStats = true;
                        Optional<String> groupMin = convertStatValue(stats.getMin_value(), type);
                        Optional<String> groupMax = convertStatValue(stats.getMax_value(), type);

                        if (groupMin.isPresent()) {
                            minValue = minValue.isEmpty() ? groupMin : Optional.of(stringMin(minValue.get(), groupMin.get()));
                        }
                        if (groupMax.isPresent()) {
                            maxValue = maxValue.isEmpty() ? groupMax : Optional.of(stringMax(maxValue.get(), groupMax.get()));
                        }
                    }
                }
            }

            result.add(new DucklakeFileColumnStats(
                    column.columnId(),
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
                BigInteger unscaled = new BigInteger(value);
                BigDecimal decimal = new BigDecimal(unscaled, decimalType.getScale());
                return Optional.of(decimal.toPlainString());
            }

            return Optional.empty();
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static String stringMin(String a, String b)
    {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static String stringMax(String a, String b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }
}
