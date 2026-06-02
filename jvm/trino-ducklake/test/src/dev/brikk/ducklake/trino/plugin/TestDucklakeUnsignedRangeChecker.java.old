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
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDucklakeUnsignedRangeChecker
{
    // Trino read-side widenings for DuckLake unsigned types (see DucklakeTypeConverter).
    private static final DecimalType UINT64_DECIMAL = DecimalType.createDecimalType(20, 0);

    // ---------- No-op when no unsigned columns ----------

    @Test
    public void testNoUnsignedColumnsProducesNoOpChecker()
    {
        DucklakeUnsignedRangeChecker checker = DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(
                        new DucklakeColumnHandle(1, "a", BIGINT, true),
                        new DucklakeColumnHandle(2, "b", INTEGER, true)),
                Map.of(1L, "int64", 2L, "int32"));
        assertThat(checker.isNoOp()).isTrue();
        // No-op validate must not throw on anything.
        checker.validate(new Page(1, smallintBlock((short) -1)));
    }

    // ---------- uint8 ----------

    @Test
    public void testUint8InRangeAccepted()
    {
        DucklakeUnsignedRangeChecker checker = uint8Checker();
        Block block = smallintBlock((short) 0, (short) 100, (short) 255);
        checker.validate(new Page(block.getPositionCount(), block));
    }

    @Test
    public void testUint8NegativeRejected()
    {
        DucklakeUnsignedRangeChecker checker = uint8Checker();
        Block block = smallintBlock((short) -1);
        assertOverflow(checker, block, "uint8", "-1", "255");
    }

    @Test
    public void testUint8AboveMaxRejected()
    {
        DucklakeUnsignedRangeChecker checker = uint8Checker();
        Block block = smallintBlock((short) 256);
        assertOverflow(checker, block, "uint8", "256", "255");
    }

    @Test
    public void testUint8WrapValue300Rejected()
    {
        // 300 as SMALLINT survives Trino intact; the bug (pre-fix) was Parquet encoding
        // the low 8 bits (300 & 0xFF = 44) into the uint8 column and silently losing the
        // high bit on read. This test pins the symptom at its first crossing.
        DucklakeUnsignedRangeChecker checker = uint8Checker();
        Block block = smallintBlock((short) 300);
        assertOverflow(checker, block, "uint8", "300", "255");
    }

    @Test
    public void testUint8NullsSkipped()
    {
        DucklakeUnsignedRangeChecker checker = uint8Checker();
        BlockBuilder builder = SMALLINT.createBlockBuilder(null, 3);
        builder.appendNull();
        SMALLINT.writeLong(builder, 42);
        builder.appendNull();
        checker.validate(new Page(3, builder.build()));
    }

    // ---------- uint16 ----------

    @Test
    public void testUint16InRangeAccepted()
    {
        DucklakeUnsignedRangeChecker checker = uint16Checker();
        Block block = intBlock(0, 65_535, 12_345);
        checker.validate(new Page(block.getPositionCount(), block));
    }

    @Test
    public void testUint16NegativeRejected()
    {
        DucklakeUnsignedRangeChecker checker = uint16Checker();
        assertOverflow(checker, intBlock(-1), "uint16", "-1", "65535");
    }

    @Test
    public void testUint16AboveMaxRejected()
    {
        DucklakeUnsignedRangeChecker checker = uint16Checker();
        assertOverflow(checker, intBlock(65_536), "uint16", "65536", "65535");
    }

    // ---------- uint32 ----------

    @Test
    public void testUint32InRangeAccepted()
    {
        DucklakeUnsignedRangeChecker checker = uint32Checker();
        Block block = bigintBlock(0L, (1L << 32) - 1, 1_000_000_000L);
        checker.validate(new Page(block.getPositionCount(), block));
    }

    @Test
    public void testUint32NegativeRejected()
    {
        DucklakeUnsignedRangeChecker checker = uint32Checker();
        assertOverflow(checker, bigintBlock(-1L), "uint32", "-1", "4294967295");
    }

    @Test
    public void testUint32AboveMaxRejected()
    {
        DucklakeUnsignedRangeChecker checker = uint32Checker();
        assertOverflow(checker, bigintBlock(1L << 32), "uint32", "4294967296", "4294967295");
    }

    // ---------- uint64 ----------

    @Test
    public void testUint64ZeroAccepted()
    {
        DucklakeUnsignedRangeChecker checker = uint64Checker();
        Block block = longDecimalBlock(BigInteger.ZERO);
        checker.validate(new Page(block.getPositionCount(), block));
    }

    @Test
    public void testUint64MaxAccepted()
    {
        DucklakeUnsignedRangeChecker checker = uint64Checker();
        Block block = longDecimalBlock(new BigInteger("18446744073709551615"));
        checker.validate(new Page(block.getPositionCount(), block));
    }

    @Test
    public void testUint64NegativeRejected()
    {
        DucklakeUnsignedRangeChecker checker = uint64Checker();
        Block block = longDecimalBlock(BigInteger.ONE.negate());
        assertOverflow(checker, block, "uint64", "-1", "18446744073709551615");
    }

    @Test
    public void testUint64AboveMaxRejected()
    {
        DucklakeUnsignedRangeChecker checker = uint64Checker();
        Block block = longDecimalBlock(new BigInteger("18446744073709551616"));
        assertOverflow(checker, block, "uint64", "18446744073709551616", "18446744073709551615");
    }

    // ---------- helpers ----------

    private static DucklakeUnsignedRangeChecker uint8Checker()
    {
        return DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(new DucklakeColumnHandle(1, "u8col", SMALLINT, true)),
                Map.of(1L, "uint8"));
    }

    private static DucklakeUnsignedRangeChecker uint16Checker()
    {
        return DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(new DucklakeColumnHandle(1, "u16col", INTEGER, true)),
                Map.of(1L, "uint16"));
    }

    private static DucklakeUnsignedRangeChecker uint32Checker()
    {
        return DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(new DucklakeColumnHandle(1, "u32col", BIGINT, true)),
                Map.of(1L, "uint32"));
    }

    private static DucklakeUnsignedRangeChecker uint64Checker()
    {
        return DucklakeUnsignedRangeChecker.build(
                ImmutableList.of(new DucklakeColumnHandle(1, "u64col", UINT64_DECIMAL, true)),
                Map.of(1L, "uint64"));
    }

    private static Block smallintBlock(short... values)
    {
        BlockBuilder builder = SMALLINT.createBlockBuilder(null, values.length);
        for (short value : values) {
            SMALLINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block intBlock(int... values)
    {
        BlockBuilder builder = INTEGER.createBlockBuilder(null, values.length);
        for (int value : values) {
            INTEGER.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block bigintBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static Block longDecimalBlock(BigInteger... values)
    {
        BlockBuilder builder = UINT64_DECIMAL.createBlockBuilder(null, values.length);
        for (BigInteger value : values) {
            UINT64_DECIMAL.writeObject(builder, Int128.valueOf(value));
        }
        return builder.build();
    }

    private static void assertOverflow(DucklakeUnsignedRangeChecker checker, Block block, String type, String value, String max)
    {
        assertThatThrownBy(() -> checker.validate(new Page(block.getPositionCount(), block)))
                .isInstanceOfSatisfying(TrinoException.class, ex ->
                        assertThat(ex.getErrorCode()).isEqualTo(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode()))
                .hasMessageContaining(type)
                .hasMessageContaining("0.." + max)
                .hasMessageContaining("Value " + value);
    }

    // Silence unused import warning — retained for future readability of the
    // Int128 ctor used indirectly through UINT64_DECIMAL.writeObject.
    @SuppressWarnings("unused")
    private static final Class<?> KEEP_LIST_IMPORT = List.class;
}
