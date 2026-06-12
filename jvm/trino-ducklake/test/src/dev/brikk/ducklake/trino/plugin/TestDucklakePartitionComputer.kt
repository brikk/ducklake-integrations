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

import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import io.trino.spi.block.Block
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.LocalDate

internal class TestDucklakePartitionComputer {
    // ===== Identity transforms =====

    @Test
    fun testIdentityVarchar() {
        val block = buildVarcharBlock("hello")
        val value = DucklakePartitionComputer.computePartitionValue(
                VARCHAR, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("hello")
    }

    @Test
    fun testIdentityInteger() {
        val builder = INTEGER.createBlockBuilder(null, 1)
        INTEGER.writeInt(builder, 42)
        val block = builder.build()

        val value = DucklakePartitionComputer.computePartitionValue(
                INTEGER, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("42")
    }

    @Test
    fun testIdentityDate() {
        val block = buildDateBlock(LocalDate.of(2023, 6, 15))
        val value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("2023-06-15")
    }

    @Test
    fun testIdentityNull() {
        val builder = VARCHAR.createBlockBuilder(null, 1)
        builder.appendNull()
        val block = builder.build()

        val value = DucklakePartitionComputer.computePartitionValue(
                VARCHAR, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isNull()
    }

    // ===== Calendar encoding (DuckDB-compatible) =====

    @Test
    fun testCalendarYearFromDate() {
        val block = buildDateBlock(LocalDate.of(2023, 6, 15))
        val value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("2023")
    }

    @Test
    fun testCalendarMonthFromDate() {
        val block = buildDateBlock(LocalDate.of(2023, 6, 15))
        val value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.MONTH, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("6")
    }

    @Test
    fun testCalendarDayFromDate() {
        val block = buildDateBlock(LocalDate.of(2023, 6, 15))
        val value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.DAY, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("15")
    }

    @Test
    fun testCalendarYearFromTimestamp() {
        // 2023-06-15 14:30:00 UTC in microseconds since epoch
        val epochMicros = LocalDate.of(2023, 6, 15).atTime(14, 30).toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L
        val block = buildTimestampMicrosBlock(epochMicros)
        val value = DucklakePartitionComputer.computePartitionValue(
                TIMESTAMP_MICROS, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("2023")
    }

    @Test
    fun testCalendarHourFromTimestamp() {
        val epochMicros = LocalDate.of(2023, 6, 15).atTime(14, 30).toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L
        val block = buildTimestampMicrosBlock(epochMicros)
        val value = DucklakePartitionComputer.computePartitionValue(
                TIMESTAMP_MICROS, block, 0, DucklakePartitionTransform.HOUR, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("14")
    }

    // ===== Epoch encoding =====

    @Test
    fun testEpochYearFromDate() {
        val block = buildDateBlock(LocalDate.of(2023, 6, 15))
        val value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.EPOCH)
        assertThat(value).isEqualTo("53") // 2023 - 1970
    }

    @Test
    fun testEpochMonthFromDate() {
        val block = buildDateBlock(LocalDate.of(2023, 6, 15))
        val value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.MONTH, DucklakeTemporalPartitionEncoding.EPOCH)
        assertThat(value).isEqualTo("641") // (2023-1970)*12 + (6-1) = 53*12 + 5 = 641
    }

    @Test
    fun testEpochDayFromDate() {
        val block = buildDateBlock(LocalDate.of(2023, 6, 15))
        val value = DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.DAY, DucklakeTemporalPartitionEncoding.EPOCH)
        assertThat(value).isEqualTo(LocalDate.of(2023, 6, 15).toEpochDay().toString())
    }

    @Test
    fun testEpochHourFromTimestamp() {
        val epochMicros = LocalDate.of(2023, 6, 15).atTime(14, 30).toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L
        val block = buildTimestampMicrosBlock(epochMicros)
        val value = DucklakePartitionComputer.computePartitionValue(
                TIMESTAMP_MICROS, block, 0, DucklakePartitionTransform.HOUR, DucklakeTemporalPartitionEncoding.EPOCH)
        val expectedHours = LocalDate.of(2023, 6, 15).toEpochDay() * 24 + 14
        assertThat(value).isEqualTo(expectedHours.toString())
    }

    // ===== Edge cases =====

    @Test
    fun testEpochBoundaryDate() {
        val block = buildDateBlock(LocalDate.of(1970, 1, 1))
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.EPOCH))
                .isEqualTo("0")
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.MONTH, DucklakeTemporalPartitionEncoding.EPOCH))
                .isEqualTo("0")
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.DAY, DucklakeTemporalPartitionEncoding.EPOCH))
                .isEqualTo("0")
    }

    @Test
    fun testCalendarDecemberDate() {
        val block = buildDateBlock(LocalDate.of(2023, 12, 31))
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.MONTH, DucklakeTemporalPartitionEncoding.CALENDAR))
                .isEqualTo("12")
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.DAY, DucklakeTemporalPartitionEncoding.CALENDAR))
                .isEqualTo("31")
    }

    @Test
    fun testPreEpochDate() {
        val block = buildDateBlock(LocalDate.of(1969, 12, 31))
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.EPOCH))
                .isEqualTo("-1")
        assertThat(DucklakePartitionComputer.computePartitionValue(
                DATE, block, 0, DucklakePartitionTransform.YEAR, DucklakeTemporalPartitionEncoding.CALENDAR))
                .isEqualTo("1969")
    }

    @Test
    fun testIdentityBigint() {
        val builder = BIGINT.createBlockBuilder(null, 1)
        BIGINT.writeLong(builder, 9999999999L)
        val block = builder.build()

        val value = DucklakePartitionComputer.computePartitionValue(
                BIGINT, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("9999999999")
    }

    @Test
    fun testIdentityDouble() {
        val builder = DOUBLE.createBlockBuilder(null, 1)
        DOUBLE.writeDouble(builder, 3.14)
        val block = builder.build()

        val value = DucklakePartitionComputer.computePartitionValue(
                DOUBLE, block, 0, DucklakePartitionTransform.IDENTITY, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("3.14")
    }

    // ===== Helpers =====

    private fun buildDateBlock(date: LocalDate): Block {
        val builder = DATE.createBlockBuilder(null, 1)
        DATE.writeInt(builder, date.toEpochDay().toInt())
        return builder.build()
    }

    private fun buildTimestampMicrosBlock(epochMicros: Long): Block {
        val builder = TIMESTAMP_MICROS.createBlockBuilder(null, 1)
        TIMESTAMP_MICROS.writeLong(builder, epochMicros)
        return builder.build()
    }

    private fun buildVarcharBlock(value: String): Block {
        val builder = VARCHAR.createBlockBuilder(null, 1)
        VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(value))
        return builder.build()
    }

    // ===== Bucket transform (Iceberg-compatible Murmur3) =====

    // Reference values from Iceberg's spec Appendix B "Bucketing":
    //   bucket[100] over int 34 → bucket 79
    //   bucket[100] over long 34 → bucket 79 (int widened to long before hashing)
    //   bucket[100] over CharSequence "iceberg" → bucket 89
    //   bucket[100] over date 2017-11-16 → bucket 26

    @Test
    fun testBucketInteger34MatchesIceberg() {
        val builder = INTEGER.createBlockBuilder(null, 1)
        INTEGER.writeInt(builder, 34)
        val block = builder.build()
        assertThat(DucklakePartitionComputer.computeBucket(INTEGER, block, 0, 100)).isEqualTo(79)
    }

    @Test
    fun testBucketBigint34MatchesIcebergIntCompat() {
        // long 34 hashes identically to int 34 because Iceberg widens int to long before hashing.
        val builder = BIGINT.createBlockBuilder(null, 1)
        BIGINT.writeLong(builder, 34L)
        val block = builder.build()
        assertThat(DucklakePartitionComputer.computeBucket(BIGINT, block, 0, 100)).isEqualTo(79)
    }

    @Test
    fun testBucketStringMatchesIceberg() {
        val block = buildVarcharBlock("iceberg")
        assertThat(DucklakePartitionComputer.computeBucket(VARCHAR, block, 0, 100)).isEqualTo(89)
    }

    @Test
    fun testBucketDateMatchesIceberg() {
        val block = buildDateBlock(LocalDate.of(2017, 11, 16))
        assertThat(DucklakePartitionComputer.computeBucket(DATE, block, 0, 100)).isEqualTo(26)
    }

    @Test
    fun testBucketValueIsNonNegative() {
        // (hash & INT_MAX) ensures positive bucket value even for negative hash outputs.
        for (v in longArrayOf(-1L, Long.MIN_VALUE, Long.MAX_VALUE, 0L, 999_999L)) {
            val builder = BIGINT.createBlockBuilder(null, 1)
            BIGINT.writeLong(builder, v)
            val block = builder.build()
            val bucket = DucklakePartitionComputer.computeBucket(BIGINT, block, 0, 16)
            assertThat(bucket).isBetween(0, 15)
        }
    }

    @Test
    fun testBucketIsStableAcrossInvocations() {
        // Determinism check — the hash function must be pure so two calls on the
        // same input always produce the same bucket.
        val block = buildVarcharBlock("alice")
        val first = DucklakePartitionComputer.computeBucket(VARCHAR, block, 0, 16)
        val second = DucklakePartitionComputer.computeBucket(VARCHAR, block, 0, 16)
        assertThat(first).isEqualTo(second)
    }

    @Test
    fun testBucketRejectsUnsupportedTypes() {
        val builder = DOUBLE.createBlockBuilder(null, 1)
        DOUBLE.writeDouble(builder, 3.14)
        val block = builder.build()
        org.assertj.core.api.Assertions.assertThatThrownBy {
            DucklakePartitionComputer.computeBucket(DOUBLE, block, 0, 16)
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("not defined for type")
    }

    @Test
    fun testBucketPartitionValueIsBucketIndex() {
        val builder = INTEGER.createBlockBuilder(null, 1)
        INTEGER.writeInt(builder, 34)
        val block = builder.build()
        val value = DucklakePartitionComputer.computePartitionValue(
                INTEGER, block, 0, DucklakePartitionTransform.BUCKET,
                100, DucklakeTemporalPartitionEncoding.CALENDAR)
        assertThat(value).isEqualTo("79")
    }

    @Test
    fun testBucketWithoutArityThrows() {
        val builder = INTEGER.createBlockBuilder(null, 1)
        INTEGER.writeInt(builder, 1)
        val block = builder.build()
        org.assertj.core.api.Assertions.assertThatThrownBy {
            DucklakePartitionComputer.computePartitionValue(
                    INTEGER, block, 0, DucklakePartitionTransform.BUCKET,
                    null, DucklakeTemporalPartitionEncoding.CALENDAR)
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("BUCKET")
    }
}
