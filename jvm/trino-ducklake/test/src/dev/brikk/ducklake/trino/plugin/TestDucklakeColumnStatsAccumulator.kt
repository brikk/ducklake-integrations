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

import io.airlift.slice.Slices
import io.trino.spi.Page
import io.trino.spi.block.Block
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit tests for [DucklakeColumnStatsAccumulator] — the reusable single-pass stats accumulator.
 * Focused on the correctness-critical cases: null vs value counts, NaN exclusion from min/max,
 * and (the silent-bug-prone one) VARCHAR min/max under UTF-8 byte order, not Java UTF-16 order.
 */
class TestDucklakeColumnStatsAccumulator {
    @Test
    fun accumulatesCountsMinMaxAndExcludesNaN() {
        val columns = listOf(
                DucklakeColumnHandle(1L, "id", INTEGER, true),
                DucklakeColumnHandle(2L, "name", VARCHAR, true),
                DucklakeColumnHandle(3L, "amt", DOUBLE, true))
        val acc = DucklakeColumnStatsAccumulator(columns)

        // 4 rows; one null per column. amt has a NaN (non-null → counted, but excluded from min/max).
        acc.add(Page(
                intBlock(3, 1, null, 2),
                varcharBlock("banana", "apple", null, "cherry"),
                doubleBlock(1.0, Double.NaN, null, 2.0)))

        val stats = acc.build()
        assertThat(stats).hasSize(3)

        assertThat(stats[0].columnId).isEqualTo(1L)
        assertThat(stats[0].valueCount).isEqualTo(3L)
        assertThat(stats[0].nullCount).isEqualTo(1L)
        assertThat(stats[0].minValue).isEqualTo("1")
        assertThat(stats[0].maxValue).isEqualTo("3")
        assertThat(stats[0].containsNan).isFalse()

        assertThat(stats[1].valueCount).isEqualTo(3L)
        assertThat(stats[1].nullCount).isEqualTo(1L)
        assertThat(stats[1].minValue).isEqualTo("apple")
        assertThat(stats[1].maxValue).isEqualTo("cherry")
        assertThat(stats[1].containsNan).isFalse()

        // NaN is a non-null value (counted) but must not enter min/max — AND contains_nan must be
        // set so DuckLake does not prune this file by min/max (NaN sorts above finite values there).
        assertThat(stats[2].valueCount).isEqualTo(3L)
        assertThat(stats[2].nullCount).isEqualTo(1L)
        assertThat(stats[2].minValue).isEqualTo("1.0")
        assertThat(stats[2].maxValue).isEqualTo("2.0")
        assertThat(stats[2].containsNan).isTrue()
    }

    @Test
    fun floatColumnWithoutNaNReportsContainsNanFalse() {
        val columns = listOf(DucklakeColumnHandle(1L, "amt", DOUBLE, true))
        val acc = DucklakeColumnStatsAccumulator(columns)
        acc.add(Page(doubleBlock(1.0, 2.0, null)))
        assertThat(acc.build()[0].containsNan).isFalse()
    }

    @Test
    fun varcharMinMaxUsesUtf8ByteOrderNotUtf16() {
        // U+FFFF (BMP, UTF-8 EF BF BF) vs U+1F600 😀 (UTF-8 F0 9F 98 80).
        // UTF-8 byte order: 😀 > U+FFFF.  Java String.compareTo (UTF-16): the 😀 high surrogate
        // (0xD83D) < 0xFFFF, so it would WRONGLY rank 😀 as the min. The accumulator must use
        // UTF-8 (Slice) order, matching DuckDB's binary collation and the read-side pruning.
        val highBmp = "￿"
        val emoji = "😀"
        val columns = listOf(DucklakeColumnHandle(1L, "s", VARCHAR, true))
        val acc = DucklakeColumnStatsAccumulator(columns)
        acc.add(Page(varcharBlock(highBmp, emoji)))

        val s = acc.build()[0]
        assertThat(s.minValue).isEqualTo(highBmp)
        assertThat(s.maxValue).isEqualTo(emoji)
    }

    @Test
    fun allNullColumnHasNoMinMax() {
        val columns = listOf(DucklakeColumnHandle(1L, "id", INTEGER, true))
        val acc = DucklakeColumnStatsAccumulator(columns)
        acc.add(Page(intBlock(null, null)))
        val s = acc.build()[0]
        assertThat(s.valueCount).isEqualTo(0L)
        assertThat(s.nullCount).isEqualTo(2L)
        assertThat(s.minValue).isNull()
        assertThat(s.maxValue).isNull()
    }

    private fun intBlock(vararg values: Int?): Block {
        val b = INTEGER.createBlockBuilder(null, values.size)
        for (v in values) if (v == null) b.appendNull() else INTEGER.writeLong(b, v.toLong())
        return b.build()
    }

    private fun varcharBlock(vararg values: String?): Block {
        val b = VARCHAR.createBlockBuilder(null, values.size)
        for (v in values) if (v == null) b.appendNull() else VARCHAR.writeSlice(b, Slices.utf8Slice(v))
        return b.build()
    }

    private fun doubleBlock(vararg values: Double?): Block {
        val b = DOUBLE.createBlockBuilder(null, values.size)
        for (v in values) if (v == null) b.appendNull() else DOUBLE.writeDouble(b, v)
        return b.build()
    }
}
