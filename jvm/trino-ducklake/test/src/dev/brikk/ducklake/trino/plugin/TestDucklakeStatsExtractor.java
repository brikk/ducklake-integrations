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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestDucklakeStatsExtractor
{
    @Test
    void testConvertIntegerStats()
    {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(42).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, INTEGER)).isEqualTo(Optional.of("42"));
    }

    @Test
    void testConvertNegativeInteger()
    {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(-17).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, INTEGER)).isEqualTo(Optional.of("-17"));
    }

    @Test
    void testConvertBigintStats()
    {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(9_000_000_000L).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BIGINT)).isEqualTo(Optional.of("9000000000"));
    }

    @Test
    void testConvertDoubleStats()
    {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(3.14).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DOUBLE)).isEqualTo(Optional.of("3.14"));
    }

    @Test
    void testConvertDoubleNan()
    {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(Double.NaN).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DOUBLE)).isEmpty();
    }

    @Test
    void testConvertRealStats()
    {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(2.5f).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, REAL)).isEqualTo(Optional.of("2.5"));
    }

    @Test
    void testConvertVarcharStats()
    {
        byte[] bytes = "hello world".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, VARCHAR)).isEqualTo(Optional.of("hello world"));
    }

    @Test
    void testConvertBooleanTrue()
    {
        byte[] bytes = new byte[] {1};
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BOOLEAN)).isEqualTo(Optional.of("true"));
    }

    @Test
    void testConvertBooleanFalse()
    {
        byte[] bytes = new byte[] {0};
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BOOLEAN)).isEqualTo(Optional.of("false"));
    }

    @Test
    void testConvertDateStats()
    {
        // 2024-01-15 = epoch day 19737
        int epochDay = (int) java.time.LocalDate.of(2024, 1, 15).toEpochDay();
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(epochDay).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DATE)).isEqualTo(Optional.of("2024-01-15"));
    }

    @Test
    void testConvertEmptyValue()
    {
        assertThat(DucklakeStatsExtractor.convertStatValue(new byte[0], INTEGER)).isEmpty();
    }

    @Test
    void testConvertNullValue()
    {
        assertThat(DucklakeStatsExtractor.convertStatValue(null, INTEGER)).isEmpty();
    }
}
