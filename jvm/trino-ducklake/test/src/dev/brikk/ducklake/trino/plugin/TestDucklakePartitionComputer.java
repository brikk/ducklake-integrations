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
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestDucklakePartitionComputer
{
    // ===== Identity transforms =====

    @Test
    void testIdentityVarchar()
    {
        Block block = buildVarcharBlock("hello");
        String value = DucklakePartitionComputer.computePartitionValue(
                VARCHAR, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("hello");
    }

    @Test
    void testIdentityInteger()
    {
        BlockBuilder builder = INTEGER.createBlockBuilder(null, 1);
        INTEGER.writeInt(builder, 42);
        Block block = builder.build();

        String value = DucklakePartitionComputer.computePartitionValue(
                INTEGER, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("42");
    }

    @Test
    void testIdentityDate()
    {
        Block block = buildDateBlock(LocalDate.of(2023, 6, 15));
        String value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("2023-06-15");
    }

    @Test
    void testIdentityNull()
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1);
        builder.appendNull();
        Block block = builder.build();

        String value = DucklakePartitionComputer.computePartitionValue(
                VARCHAR, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isNull();
    }

    // ===== Calendar encoding (DuckDB-compatible) =====

    @Test
    void testCalendarYearFromDate()
    {
        Block block = buildDateBlock(LocalDate.of(2023, 6, 15));
        String value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("2023");
    }

    @Test
    void testCalendarMonthFromDate()
    {
        Block block = buildDateBlock(LocalDate.of(2023, 6, 15));
        String value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.MONTH, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("6");
    }

    @Test
    void testCalendarDayFromDate()
    {
        Block block = buildDateBlock(LocalDate.of(2023, 6, 15));
        String value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.DAY, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("15");
    }

    @Test
    void testCalendarYearFromTimestamp()
    {
        // 2023-06-15 14:30:00 UTC in microseconds since epoch
        long epochMicros = LocalDate.of(2023, 6, 15).atTime(14, 30).toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L;
        Block block = buildTimestampMicrosBlock(epochMicros);
        String value = DucklakePartitionComputer.computePartitionValue(
                TIMESTAMP_MICROS, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("2023");
    }

    @Test
    void testCalendarHourFromTimestamp()
    {
        long epochMicros = LocalDate.of(2023, 6, 15).atTime(14, 30).toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L;
        Block block = buildTimestampMicrosBlock(epochMicros);
        String value = DucklakePartitionComputer.computePartitionValue(
                TIMESTAMP_MICROS, block, 0, DucklakePartitionTransform.HOUR, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("14");
    }

    // ===== Epoch encoding =====

    @Test
    void testEpochYearFromDate()
    {
        Block block = buildDateBlock(LocalDate.of(2023, 6, 15));
        String value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.EPOCH);
        assertThat(value).isEqualTo("53"); // 2023 - 1970
    }

    @Test
    void testEpochMonthFromDate()
    {
        Block block = buildDateBlock(LocalDate.of(2023, 6, 15));
        String value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.MONTH, DucklakeTemporalPartitionEncoding.EPOCH);
        assertThat(value).isEqualTo("641"); // (2023-1970)*12 + (6-1) = 53*12 + 5 = 641
    }

    @Test
    void testEpochDayFromDate()
    {
        Block block = buildDateBlock(LocalDate.of(2023, 6, 15));
        String value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.DAY, DucklakeTemporalPartitionEncoding.EPOCH);
        assertThat(value).isEqualTo(String.valueOf(LocalDate.of(2023, 6, 15).toEpochDay()));
    }

    @Test
    void testEpochHourFromTimestamp()
    {
        long epochMicros = LocalDate.of(2023, 6, 15).atTime(14, 30).toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L;
        Block block = buildTimestampMicrosBlock(epochMicros);
        String value = DucklakePartitionComputer.computePartitionValue(
                TIMESTAMP_MICROS, block, 0, DucklakePartitionTransform.HOUR, DucklakeTemporalPartitionEncoding.EPOCH);
        long expectedHours = LocalDate.of(2023, 6, 15).toEpochDay() * 24 + 14;
        assertThat(value).isEqualTo(String.valueOf(expectedHours));
    }

    // ===== Edge cases =====

    @Test
    void testEpochBoundaryDate()
    {
        Block block = buildDateBlock(LocalDate.of(1970, 1, 1));
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.EPOCH))
                .isEqualTo("0");
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.MONTH, DucklakeTemporalPartitionEncoding.EPOCH))
                .isEqualTo("0");
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.DAY, DucklakeTemporalPartitionEncoding.EPOCH))
                .isEqualTo("0");
    }

    @Test
    void testCalendarDecemberDate()
    {
        Block block = buildDateBlock(LocalDate.of(2023, 12, 31));
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.MONTH, DucklakeTemporalPartitionEncoding.CALENDAR))
                .isEqualTo("12");
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.DAY, DucklakeTemporalPartitionEncoding.CALENDAR))
                .isEqualTo("31");
    }

    @Test
    void testPreEpochDate()
    {
        Block block = buildDateBlock(LocalDate.of(1969, 12, 31));
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.EPOCH))
                .isEqualTo("-1");
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.CALENDAR))
                .isEqualTo("1969");
    }

    @Test
    void testIdentityBigint()
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(builder, 9999999999L);
        Block block = builder.build();

        String value = DucklakePartitionComputer.computePartitionValue(
                BIGINT, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("9999999999");
    }

    @Test
    void testIdentityDouble()
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, 1);
        DOUBLE.writeDouble(builder, 3.14);
        Block block = builder.build();

        String value = DucklakePartitionComputer.computePartitionValue(
                DOUBLE, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR);
        assertThat(value).isEqualTo("3.14");
    }

    // ===== Helpers =====

    private static Block buildDateBlock(LocalDate date)
    {
        BlockBuilder builder = DATE.createBlockBuilder(null, 1);
        DATE.writeInt(builder, (int) date.toEpochDay());
        return builder.build();
    }

    private static Block buildTimestampMicrosBlock(long epochMicros)
    {
        BlockBuilder builder = TIMESTAMP_MICROS.createBlockBuilder(null, 1);
        TIMESTAMP_MICROS.writeLong(builder, epochMicros);
        return builder.build();
    }

    private static Block buildVarcharBlock(String value)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(value));
        return builder.build();
    }
}
