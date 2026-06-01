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
package dev.brikk.ducklake.catalog;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Type-coverage matrix for the canonical stat comparator. The bug this guards
 * against is lexical comparison of numeric stats: lexically {@code "12" < "8"}
 * and {@code "-3" > "-10"}, which corrupts cross-file min/max merges and silently
 * mis-prunes data files. Every numeric canonical type must compare numerically;
 * every non-numeric type keeps (correct) lexical order.
 */
class TestDucklakeStatTypes
{
    private static final String[] INTEGER_TYPES = {
            "int8", "int16", "int32", "int64", "int128",
            "uint8", "uint16", "uint32", "uint64", "uint128"
    };
    private static final String[] FLOAT_TYPES = {"float32", "float64"};
    private static final String[] NUMERIC_TYPES = {
            "int8", "int16", "int32", "int64", "int128",
            "uint8", "uint16", "uint32", "uint64", "uint128",
            "float32", "float64", "decimal(10,2)", "decimal(38,0)", "decimal(18,4)"
    };
    private static final String[] NON_NUMERIC_TYPES = {
            "boolean", "date", "time", "timestamp", "timestamptz", "varchar", "blob", "uuid"
    };

    @Test
    void numericTypesAreRecognized()
    {
        for (String type : NUMERIC_TYPES) {
            assertThat(DucklakeStatTypes.isNumericType(type)).as(type).isTrue();
        }
    }

    @Test
    void nonNumericTypesAreNotNumeric()
    {
        for (String type : NON_NUMERIC_TYPES) {
            assertThat(DucklakeStatTypes.isNumericType(type)).as(type).isFalse();
        }
    }

    @Test
    void int128AndUint128AreNumeric()
    {
        // Regression: the old isNumericType omitted these, so 128-bit columns
        // were parsed as raw strings and hard-pruned by lexical comparison.
        assertThat(DucklakeStatTypes.isNumericType("int128")).isTrue();
        assertThat(DucklakeStatTypes.isNumericType("uint128")).isTrue();
    }

    @Test
    void numericCrossFileMergeIsNumeric()
    {
        // The canonical bug case: 12 > 8 numerically, but "12" < "8" lexically.
        for (String type : NUMERIC_TYPES) {
            assertThat(DucklakeStatTypes.max("8", "12", type)).as(type).isEqualTo("12");
            assertThat(DucklakeStatTypes.min("8", "12", type)).as(type).isEqualTo("8");
        }
    }

    @Test
    void negativeNumericMergeIsNumeric()
    {
        // -10 < -3 numerically, but "-10" > "-3" lexically.
        for (String type : INTEGER_TYPES) {
            assertThat(DucklakeStatTypes.min("-3", "-10", type)).as(type).isEqualTo("-10");
            assertThat(DucklakeStatTypes.max("-3", "-10", type)).as(type).isEqualTo("-3");
        }
    }

    @Test
    void floatMergeIsNumeric()
    {
        for (String type : FLOAT_TYPES) {
            assertThat(DucklakeStatTypes.max("9.5", "12.25", type)).as(type).isEqualTo("12.25");
            assertThat(DucklakeStatTypes.min("9.5", "12.25", type)).as(type).isEqualTo("9.5");
        }
    }

    @Test
    void int128FullRangeDoesNotOverflow()
    {
        String big = "170141183460469231731687303715884105727"; // 2^127 - 1
        String bigger = "170141183460469231731687303715884105728"; // would overflow long
        assertThat(DucklakeStatTypes.max(big, bigger, "int128")).isEqualTo(bigger);
    }

    @Test
    void booleanComparesLexicallyButCorrectly()
    {
        // "false" < "true" lexically == false < true semantically.
        assertThat(DucklakeStatTypes.min("true", "false", "boolean")).isEqualTo("false");
        assertThat(DucklakeStatTypes.max("true", "false", "boolean")).isEqualTo("true");
    }

    @Test
    void isoDatesAndTimestampsSortLexically()
    {
        assertThat(DucklakeStatTypes.max("2024-01-15", "2024-12-01", "date")).isEqualTo("2024-12-01");
        assertThat(DucklakeStatTypes.max("2024-01-15T08:00", "2024-01-15T17:30", "timestamp"))
                .isEqualTo("2024-01-15T17:30");
    }

    @Test
    void varcharSortsLexically()
    {
        assertThat(DucklakeStatTypes.min("alpha", "delta", "varchar")).isEqualTo("alpha");
        assertThat(DucklakeStatTypes.max("alpha", "delta", "varchar")).isEqualTo("delta");
    }

    @Test
    void parseStatRoundTrip()
    {
        assertThat(DucklakeStatTypes.parseStat("int64", "42")).isEqualTo(new BigDecimal("42"));
        assertThat(DucklakeStatTypes.parseStat("int128", "170141183460469231731687303715884105727"))
                .isEqualTo(new BigDecimal("170141183460469231731687303715884105727"));
        assertThat(DucklakeStatTypes.parseStat("decimal(10,2)", "3.14")).isEqualTo(new BigDecimal("3.14"));
        assertThat(DucklakeStatTypes.parseStat("boolean", "true")).isEqualTo(Boolean.TRUE);
        assertThat(DucklakeStatTypes.parseStat("varchar", "hello")).isEqualTo("hello");
        assertThat(DucklakeStatTypes.parseStat("int64", null)).isNull();
        assertThat(DucklakeStatTypes.parseStat("int64", "not-a-number")).isNull();
    }

    @Test
    void typeNamesNormalizeCaseAndWhitespace()
    {
        assertThat(DucklakeStatTypes.isNumericType("  INT64 ")).isTrue();
        assertThat(DucklakeStatTypes.isNumericType("DECIMAL(10,2)")).isTrue();
    }
}
