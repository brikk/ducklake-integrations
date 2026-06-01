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
package dev.brikk.ducklake.catalog

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal

/**
 * Type-coverage matrix for the canonical stat comparator. The bug this guards
 * against is lexical comparison of numeric stats: lexically `"12" < "8"`
 * and `"-3" > "-10"`, which corrupts cross-file min/max merges and silently
 * mis-prunes data files. Every numeric canonical type must compare numerically;
 * every non-numeric type keeps (correct) lexical order.
 */
internal class TestDucklakeStatTypes {
    @Test
    fun numericTypesAreRecognized() {
        for (type in NUMERIC_TYPES) {
            assertThat(DucklakeStatTypes.isNumericType(type)).`as`(type).isTrue()
        }
    }

    @Test
    fun nonNumericTypesAreNotNumeric() {
        for (type in NON_NUMERIC_TYPES) {
            assertThat(DucklakeStatTypes.isNumericType(type)).`as`(type).isFalse()
        }
    }

    @Test
    fun int128AndUint128AreNumeric() {
        // Regression: the old isNumericType omitted these, so 128-bit columns
        // were parsed as raw strings and hard-pruned by lexical comparison.
        assertThat(DucklakeStatTypes.isNumericType("int128")).isTrue()
        assertThat(DucklakeStatTypes.isNumericType("uint128")).isTrue()
    }

    @Test
    fun numericCrossFileMergeIsNumeric() {
        // The canonical bug case: 12 > 8 numerically, but "12" < "8" lexically.
        for (type in NUMERIC_TYPES) {
            assertThat(DucklakeStatTypes.max("8", "12", type)).`as`(type).isEqualTo("12")
            assertThat(DucklakeStatTypes.min("8", "12", type)).`as`(type).isEqualTo("8")
        }
    }

    @Test
    fun negativeNumericMergeIsNumeric() {
        // -10 < -3 numerically, but "-10" > "-3" lexically.
        for (type in INTEGER_TYPES) {
            assertThat(DucklakeStatTypes.min("-3", "-10", type)).`as`(type).isEqualTo("-10")
            assertThat(DucklakeStatTypes.max("-3", "-10", type)).`as`(type).isEqualTo("-3")
        }
    }

    @Test
    fun floatMergeIsNumeric() {
        for (type in FLOAT_TYPES) {
            assertThat(DucklakeStatTypes.max("9.5", "12.25", type)).`as`(type).isEqualTo("12.25")
            assertThat(DucklakeStatTypes.min("9.5", "12.25", type)).`as`(type).isEqualTo("9.5")
        }
    }

    @Test
    fun int128FullRangeDoesNotOverflow() {
        val big = "170141183460469231731687303715884105727" // 2^127 - 1
        val bigger = "170141183460469231731687303715884105728" // would overflow long
        assertThat(DucklakeStatTypes.max(big, bigger, "int128")).isEqualTo(bigger)
    }

    @Test
    fun booleanComparesLexicallyButCorrectly() {
        // "false" < "true" lexically == false < true semantically.
        assertThat(DucklakeStatTypes.min("true", "false", "boolean")).isEqualTo("false")
        assertThat(DucklakeStatTypes.max("true", "false", "boolean")).isEqualTo("true")
    }

    @Test
    fun isoDatesAndTimestampsSortLexically() {
        assertThat(DucklakeStatTypes.max("2024-01-15", "2024-12-01", "date")).isEqualTo("2024-12-01")
        assertThat(DucklakeStatTypes.max("2024-01-15T08:00", "2024-01-15T17:30", "timestamp"))
                .isEqualTo("2024-01-15T17:30")
    }

    @Test
    fun varcharSortsLexically() {
        assertThat(DucklakeStatTypes.min("alpha", "delta", "varchar")).isEqualTo("alpha")
        assertThat(DucklakeStatTypes.max("alpha", "delta", "varchar")).isEqualTo("delta")
    }

    @Test
    fun parseStatRoundTrip() {
        assertThat(DucklakeStatTypes.parseStat("int64", "42")).isEqualTo(BigDecimal("42"))
        assertThat(DucklakeStatTypes.parseStat("int128", "170141183460469231731687303715884105727"))
                .isEqualTo(BigDecimal("170141183460469231731687303715884105727"))
        assertThat(DucklakeStatTypes.parseStat("decimal(10,2)", "3.14")).isEqualTo(BigDecimal("3.14"))
        assertThat(DucklakeStatTypes.parseStat("boolean", "true")).isEqualTo(java.lang.Boolean.TRUE)
        assertThat(DucklakeStatTypes.parseStat("varchar", "hello")).isEqualTo("hello")
        assertThat(DucklakeStatTypes.parseStat("int64", null)).isNull()
        assertThat(DucklakeStatTypes.parseStat("int64", "not-a-number")).isNull()
    }

    @Test
    fun typeNamesNormalizeCaseAndWhitespace() {
        assertThat(DucklakeStatTypes.isNumericType("  INT64 ")).isTrue()
        assertThat(DucklakeStatTypes.isNumericType("DECIMAL(10,2)")).isTrue()
    }

    companion object {
        private val INTEGER_TYPES = arrayOf(
                "int8", "int16", "int32", "int64", "int128",
                "uint8", "uint16", "uint32", "uint64", "uint128",
        )
        private val FLOAT_TYPES = arrayOf("float32", "float64")
        private val NUMERIC_TYPES = arrayOf(
                "int8", "int16", "int32", "int64", "int128",
                "uint8", "uint16", "uint32", "uint64", "uint128",
                "float32", "float64", "decimal(10,2)", "decimal(38,0)", "decimal(18,4)",
        )
        private val NON_NUMERIC_TYPES = arrayOf(
                "boolean", "date", "time", "timestamp", "timestamptz", "varchar", "blob", "uuid",
        )
    }
}
