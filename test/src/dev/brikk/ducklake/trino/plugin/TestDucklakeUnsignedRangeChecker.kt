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

import com.google.common.collect.ImmutableList
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DecimalType
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.SmallintType.SMALLINT
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.math.BigInteger

class TestDucklakeUnsignedRangeChecker {
    // ---------- No-op when no unsigned columns ----------

    @Test
    fun testNoUnsignedColumnsProducesNoOpChecker() {
        val checker = DucklakeUnsignedRangeChecker.build(
            ImmutableList.of(
                DucklakeColumnHandle(1, "a", BIGINT, true),
                DucklakeColumnHandle(2, "b", INTEGER, true)),
            mapOf(1L to "int64", 2L to "int32"))
        assertThat(checker.isNoOp()).isTrue()
        // No-op validate must not throw on anything.
        checker.validate(Page(1, smallintBlock((-1).toShort())))
    }

    // ---------- uint8 ----------

    @Test
    fun testUint8InRangeAccepted() {
        val checker = uint8Checker()
        val block = smallintBlock(0.toShort(), 100.toShort(), 255.toShort())
        checker.validate(Page(block.positionCount, block))
    }

    @Test
    fun testUint8NegativeRejected() {
        val checker = uint8Checker()
        val block = smallintBlock((-1).toShort())
        assertOverflow(checker, block, "uint8", "-1", "255")
    }

    @Test
    fun testUint8AboveMaxRejected() {
        val checker = uint8Checker()
        val block = smallintBlock(256.toShort())
        assertOverflow(checker, block, "uint8", "256", "255")
    }

    @Test
    fun testUint8WrapValue300Rejected() {
        // 300 as SMALLINT survives Trino intact; the bug (pre-fix) was Parquet encoding
        // the low 8 bits (300 & 0xFF = 44) into the uint8 column and silently losing the
        // high bit on read. This test pins the symptom at its first crossing.
        val checker = uint8Checker()
        val block = smallintBlock(300.toShort())
        assertOverflow(checker, block, "uint8", "300", "255")
    }

    @Test
    fun testUint8NullsSkipped() {
        val checker = uint8Checker()
        val builder = SMALLINT.createBlockBuilder(null, 3)
        builder.appendNull()
        SMALLINT.writeLong(builder, 42)
        builder.appendNull()
        checker.validate(Page(3, builder.build()))
    }

    // ---------- uint16 ----------

    @Test
    fun testUint16InRangeAccepted() {
        val checker = uint16Checker()
        val block = intBlock(0, 65_535, 12_345)
        checker.validate(Page(block.positionCount, block))
    }

    @Test
    fun testUint16NegativeRejected() {
        val checker = uint16Checker()
        assertOverflow(checker, intBlock(-1), "uint16", "-1", "65535")
    }

    @Test
    fun testUint16AboveMaxRejected() {
        val checker = uint16Checker()
        assertOverflow(checker, intBlock(65_536), "uint16", "65536", "65535")
    }

    // ---------- uint32 ----------

    @Test
    fun testUint32InRangeAccepted() {
        val checker = uint32Checker()
        val block = bigintBlock(0L, (1L shl 32) - 1, 1_000_000_000L)
        checker.validate(Page(block.positionCount, block))
    }

    @Test
    fun testUint32NegativeRejected() {
        val checker = uint32Checker()
        assertOverflow(checker, bigintBlock(-1L), "uint32", "-1", "4294967295")
    }

    @Test
    fun testUint32AboveMaxRejected() {
        val checker = uint32Checker()
        assertOverflow(checker, bigintBlock(1L shl 32), "uint32", "4294967296", "4294967295")
    }

    // ---------- uint64 ----------

    @Test
    fun testUint64ZeroAccepted() {
        val checker = uint64Checker()
        val block = longDecimalBlock(BigInteger.ZERO)
        checker.validate(Page(block.positionCount, block))
    }

    @Test
    fun testUint64MaxAccepted() {
        val checker = uint64Checker()
        val block = longDecimalBlock(BigInteger("18446744073709551615"))
        checker.validate(Page(block.positionCount, block))
    }

    @Test
    fun testUint64NegativeRejected() {
        val checker = uint64Checker()
        val block = longDecimalBlock(BigInteger.ONE.negate())
        assertOverflow(checker, block, "uint64", "-1", "18446744073709551615")
    }

    @Test
    fun testUint64AboveMaxRejected() {
        val checker = uint64Checker()
        val block = longDecimalBlock(BigInteger("18446744073709551616"))
        assertOverflow(checker, block, "uint64", "18446744073709551616", "18446744073709551615")
    }

    // ---------- helpers ----------

    companion object {
        // Trino read-side widenings for DuckLake unsigned types (see DucklakeTypeConverter).
        private val UINT64_DECIMAL: DecimalType = DecimalType.createDecimalType(20, 0)

        private fun uint8Checker(): DucklakeUnsignedRangeChecker {
            return DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(DucklakeColumnHandle(1, "u8col", SMALLINT, true)),
                mapOf(1L to "uint8"))
        }

        private fun uint16Checker(): DucklakeUnsignedRangeChecker {
            return DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(DucklakeColumnHandle(1, "u16col", INTEGER, true)),
                mapOf(1L to "uint16"))
        }

        private fun uint32Checker(): DucklakeUnsignedRangeChecker {
            return DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(DucklakeColumnHandle(1, "u32col", BIGINT, true)),
                mapOf(1L to "uint32"))
        }

        private fun uint64Checker(): DucklakeUnsignedRangeChecker {
            return DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(DucklakeColumnHandle(1, "u64col", UINT64_DECIMAL, true)),
                mapOf(1L to "uint64"))
        }

        private fun smallintBlock(vararg values: Short): Block {
            val builder = SMALLINT.createBlockBuilder(null, values.size)
            for (value in values) {
                SMALLINT.writeLong(builder, value.toLong())
            }
            return builder.build()
        }

        private fun intBlock(vararg values: Int): Block {
            val builder = INTEGER.createBlockBuilder(null, values.size)
            for (value in values) {
                INTEGER.writeLong(builder, value.toLong())
            }
            return builder.build()
        }

        private fun bigintBlock(vararg values: Long): Block {
            val builder = BIGINT.createBlockBuilder(null, values.size)
            for (value in values) {
                BIGINT.writeLong(builder, value)
            }
            return builder.build()
        }

        private fun longDecimalBlock(vararg values: BigInteger): Block {
            val builder = UINT64_DECIMAL.createBlockBuilder(null, values.size)
            for (value in values) {
                UINT64_DECIMAL.writeObject(builder, Int128.valueOf(value))
            }
            return builder.build()
        }

        private fun assertOverflow(checker: DucklakeUnsignedRangeChecker, block: Block, type: String, value: String, max: String) {
            assertThatThrownBy { checker.validate(Page(block.positionCount, block)) }
                .isInstanceOfSatisfying(TrinoException::class.java) { ex ->
                    assertThat(ex.errorCode).isEqualTo(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode())
                }
                .hasMessageContaining(type)
                .hasMessageContaining("0..$max")
                .hasMessageContaining("Value $value")
        }
    }
}
