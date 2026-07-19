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

import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DecimalType
import io.trino.spi.type.Int128
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.TimestampType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Identity partition-value parsing. Boolean parsing must accept the "1"/"0" encoding an
 * externally-written (C++ DuckLake) catalog may store — {@code Boolean.parseBoolean} maps
 * "1" to false, which would silently prune the wrong files and project the wrong constant.
 * Anything unrecognized must throw so the callers' catch(RuntimeException) fall back safely.
 */
class TestDucklakePartitionValueParser {

    @Test
    fun booleanAcceptsOneAndZeroEncoding() {
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "1")).isEqualTo(true)
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "0")).isEqualTo(false)
    }

    @Test
    fun booleanAcceptsTrueFalseCaseInsensitive() {
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "true")).isEqualTo(true)
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "TRUE")).isEqualTo(true)
        assertThat(DucklakePartitionValueParser.parseIdentity(BOOLEAN, "False")).isEqualTo(false)
    }

    @Test
    fun booleanRejectsUnrecognizedEncoding() {
        assertThatThrownBy { DucklakePartitionValueParser.parseIdentity(BOOLEAN, "yes") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shortDecimalParsesToUnscaledLong() {
        // DECIMAL(10,2) "99.99" → unscaled 9999 (short decimal native rep). Previously unsupported,
        // so a DECIMAL hive/identity partition value fell back to NULL.
        assertThat(DucklakePartitionValueParser.parseIdentity(DecimalType.createDecimalType(10, 2), "99.99"))
            .isEqualTo(9999L)
    }

    @Test
    fun longDecimalParsesToInt128() {
        // Precision > 18 → Int128-backed long decimal.
        val value = DucklakePartitionValueParser.parseIdentity(DecimalType.createDecimalType(38, 2), "123.45")
        assertThat(value).isEqualTo(Int128.valueOf(12345L))
    }

    @Test
    fun decimalScaleMismatchThrows() {
        // UNNECESSARY rounding: a value with more scale than the column throws → caller falls back to NULL.
        assertThatThrownBy {
            DucklakePartitionValueParser.parseIdentity(DecimalType.createDecimalType(10, 2), "1.234")
        }.isInstanceOf(ArithmeticException::class.java)
    }

    @Test
    fun shortTimestampParsesToEpochMicros() {
        // TIMESTAMP(6) "2024-01-15 10:30:00" → epoch micros (short timestamp). Previously unsupported.
        val expectedMicros = java.time.LocalDateTime.parse("2024-01-15T10:30:00")
            .toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000
        assertThat(DucklakePartitionValueParser.parseIdentity(TimestampType.createTimestampType(6), "2024-01-15 10:30:00"))
            .isEqualTo(expectedMicros)
    }

    @Test
    fun longTimestampParsesToLongTimestamp() {
        // Precision > 6 → LongTimestamp(micros, picosOfMicro).
        val value = DucklakePartitionValueParser.parseIdentity(
            TimestampType.createTimestampType(9), "2024-01-15 10:30:00.000000123")
        assertThat(value).isInstanceOf(LongTimestamp::class.java)
        val ts = value as LongTimestamp
        val expectedMicros = java.time.LocalDateTime.parse("2024-01-15T10:30:00.000000123")
            .toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000
        assertThat(ts.epochMicros).isEqualTo(expectedMicros)
        assertThat(ts.picosOfMicro).isEqualTo(123_000)
    }
}
