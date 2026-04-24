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

import dev.brikk.ducklake.catalog.DucklakePartitionTransform;
import io.trino.spi.block.Block;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

/**
 * Computes partition values from Trino block data for DuckLake partitioned writes.
 * Supports both CALENDAR (DuckDB-compatible) and EPOCH encoding for temporal transforms.
 */
public final class DucklakePartitionComputer
{
    private DucklakePartitionComputer() {}

    /**
     * Compute the partition value string for a given row position.
     *
     * @return the partition value as a string, or null if the value is null
     */
    public static String computePartitionValue(
            Type columnType,
            Block block,
            int position,
            DucklakePartitionTransform transform,
            DucklakeTemporalPartitionEncoding encoding)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (transform.isIdentity()) {
            return computeIdentityValue(columnType, block, position);
        }

        return computeTemporalValue(columnType, block, position, transform, encoding);
    }

    private static String computeIdentityValue(Type type, Block block, int position)
    {
        if (type instanceof DateType) {
            int days = DATE.getInt(block, position);
            return LocalDate.ofEpochDay(days).toString();
        }
        if (type.equals(BOOLEAN)) {
            return String.valueOf(BOOLEAN.getBoolean(block, position));
        }
        if (type.equals(TINYINT)) {
            return String.valueOf(TINYINT.getLong(block, position));
        }
        if (type.equals(SMALLINT)) {
            return String.valueOf(SMALLINT.getLong(block, position));
        }
        if (type.equals(INTEGER)) {
            return String.valueOf(INTEGER.getInt(block, position));
        }
        if (type.equals(BIGINT)) {
            return String.valueOf(BIGINT.getLong(block, position));
        }
        if (type.equals(REAL)) {
            return String.valueOf(REAL.getFloat(block, position));
        }
        if (type.equals(DOUBLE)) {
            return String.valueOf(DOUBLE.getDouble(block, position));
        }
        // VARCHAR and other string-like types
        return type.getSlice(block, position).toStringUtf8();
    }

    private static String computeTemporalValue(
            Type columnType,
            Block block,
            int position,
            DucklakePartitionTransform transform,
            DucklakeTemporalPartitionEncoding encoding)
    {
        LocalDateTime dateTime = extractLocalDateTime(columnType, block, position);

        return switch (encoding) {
            case CALENDAR -> computeCalendarValue(dateTime, transform);
            case EPOCH -> computeEpochValue(columnType, block, position, dateTime, transform);
        };
    }

    private static LocalDateTime extractLocalDateTime(Type columnType, Block block, int position)
    {
        if (columnType instanceof DateType) {
            int days = DATE.getInt(block, position);
            return LocalDate.ofEpochDay(days).atStartOfDay();
        }
        if (columnType instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                long epochMicros = timestampType.getLong(block, position);
                long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
                int nanoAdjustment = (int) floorMod(epochMicros, MICROSECONDS_PER_SECOND) * 1000;
                return LocalDateTime.ofEpochSecond(epochSeconds, nanoAdjustment, ZoneOffset.UTC);
            }
            // Long timestamp (precision > 6)
            io.trino.spi.type.LongTimestamp longTs = (io.trino.spi.type.LongTimestamp) timestampType.getObject(block, position);
            long epochMicros = longTs.getEpochMicros();
            long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanoAdjustment = (int) (floorMod(epochMicros, MICROSECONDS_PER_SECOND) * 1000 + longTs.getPicosOfMicro() / 1000);
            return LocalDateTime.ofEpochSecond(epochSeconds, nanoAdjustment, ZoneOffset.UTC);
        }
        if (columnType instanceof TimestampWithTimeZoneType tzType) {
            if (tzType.isShort()) {
                long packedValue = tzType.getLong(block, position);
                long epochMillis = unpackMillisUtc(packedValue);
                return Instant.ofEpochMilli(epochMillis).atOffset(ZoneOffset.UTC).toLocalDateTime();
            }
            io.trino.spi.type.LongTimestampWithTimeZone longTz =
                    (io.trino.spi.type.LongTimestampWithTimeZone) tzType.getObject(block, position);
            return Instant.ofEpochMilli(longTz.getEpochMillis())
                    .atOffset(ZoneOffset.UTC)
                    .toLocalDateTime();
        }
        throw new IllegalArgumentException("Temporal partition not supported for type: " + columnType);
    }

    /**
     * Calendar encoding: literal human-readable values matching DuckDB's behavior.
     */
    private static String computeCalendarValue(LocalDateTime dateTime, DucklakePartitionTransform transform)
    {
        return switch (transform) {
            case YEAR -> String.valueOf(dateTime.getYear());
            case MONTH -> String.valueOf(dateTime.getMonthValue());
            case DAY -> String.valueOf(dateTime.getDayOfMonth());
            case HOUR -> String.valueOf(dateTime.getHour());
            case IDENTITY -> throw new IllegalArgumentException("IDENTITY is not a temporal transform");
        };
    }

    /**
     * Epoch encoding: values relative to 1970-01-01 epoch (Iceberg-style).
     */
    private static String computeEpochValue(
            Type columnType,
            Block block,
            int position,
            LocalDateTime dateTime,
            DucklakePartitionTransform transform)
    {
        return switch (transform) {
            case YEAR -> String.valueOf(dateTime.getYear() - 1970);
            case MONTH -> String.valueOf((dateTime.getYear() - 1970) * 12 + (dateTime.getMonthValue() - 1));
            case DAY -> {
                if (columnType instanceof DateType) {
                    yield String.valueOf(DATE.getInt(block, position));
                }
                yield String.valueOf(dateTime.toLocalDate().toEpochDay());
            }
            case HOUR -> {
                long epochDay = dateTime.toLocalDate().toEpochDay();
                yield String.valueOf(epochDay * 24 + dateTime.getHour());
            }
            case IDENTITY -> throw new IllegalArgumentException("IDENTITY is not a temporal transform");
        };
    }
}
