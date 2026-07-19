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
package dev.brikk.duckbridge.trino.plugin

import com.google.common.collect.ImmutableMap
import io.airlift.slice.Slices
import io.trino.plugin.jdbc.JdbcColumnHandle
import io.trino.plugin.jdbc.JdbcTypeHandle
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.expression.Call
import io.trino.spi.expression.ConnectorExpression
import io.trino.spi.expression.Constant
import io.trino.spi.expression.FunctionName
import io.trino.spi.expression.StandardFunctions
import io.trino.spi.expression.Variable
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.testing.TestingConnectorSession
import io.trino.type.LikePattern
import io.trino.type.LikePatternType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.Types
import java.util.Optional

/**
 * Unit tests for [DuckBridgeExpressionTranslator], ported from the DuckLake connector's
 * `TestDuckDbExpressionTranslator`. Column handles are base-jdbc [JdbcColumnHandle]s (the P2 change
 * from DuckLake's own handle); every SQL-shape assertion is preserved verbatim.
 */
class TestDuckBridgeExpressionTranslator {
    @Test
    fun testEqualOnPushableFunction() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("length"), BIGINT, Variable("name", VARCHAR)),
                Constant(5L, BIGINT),
            )
        val conjuncts = DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)")
    }

    @Test
    fun testAndOfPushableAndNonPushable() {
        val pushable: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("length"), BIGINT, Variable("name", VARCHAR)),
                Constant(5L, BIGINT),
            )
        val notPushable: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("some_unmapped_function"), VARCHAR, Variable("name", VARCHAR)),
                varcharConst("x"),
            )
        val and: ConnectorExpression = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, pushable, notPushable)
        val conjuncts = DuckBridgeExpressionTranslator.translateConjuncts(and, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)")
    }

    @Test
    fun testIntegerLiteralAndComparison() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                Variable("id", BIGINT),
                Constant(42L, BIGINT),
            )
        val conjuncts = DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(\"id\" >= 42)")
    }

    @Test
    fun testIsNullOnPushableFunction() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.IS_NULL_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("trim"), VARCHAR, Variable("name", VARCHAR)),
            )
        val conjuncts = DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_trim(\"name\") IS NULL)")
    }

    @Test
    fun testTopLevelAndIsDecomposed() {
        val pushable1: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("length"), BIGINT, Variable("name", VARCHAR)),
                Constant(5L, BIGINT),
            )
        val pushable2: ConnectorExpression =
            call(
                StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                Variable("id", BIGINT),
                Constant(10L, BIGINT),
            )
        val and: ConnectorExpression = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, pushable1, pushable2)
        val conjuncts = DuckBridgeExpressionTranslator.translateConjuncts(and, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)", "(\"id\" < 10)")
    }

    @Test
    fun testUnknownFunctionNotPushed() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("some_unmapped_function"), VARCHAR, Variable("name", VARCHAR)),
                varcharConst("a"),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).isEmpty()
    }

    @Test
    fun testPlaceholderFunctionIsPushed() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("lower"), VARCHAR, Variable("name", VARCHAR)),
                varcharConst("apple"),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_lower(\"name\") = 'apple')")
    }

    @Test
    fun testWrongArityNotPushed() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("length"), BIGINT, Variable("name", VARCHAR), varcharConst("extra")),
                Constant(5L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).isEmpty()
    }

    @Test
    fun testConstantTrueIsSkipped() {
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(Constant.TRUE, mapOf())).isEmpty()
    }

    @Test
    fun testStringEscaping() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                Variable("name", VARCHAR),
                varcharConst("o'reilly"),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(\"name\" = 'o''reilly')")
    }

    @Test
    fun testColumnNameEscaping() {
        val weird = jdbcColumn("we\"ird", VARCHAR)
        val assignments: Map<String, ColumnHandle> = ImmutableMap.of("v", weird)
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, Variable("v", VARCHAR), varcharConst("a"))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, assignments))
            .containsExactly("(\"we\"\"ird\" = 'a')")
    }

    @Test
    fun testIntegerColumnConstantIsLong() {
        val yearCol = jdbcColumn("y", INTEGER)
        val assignments: Map<String, ColumnHandle> = ImmutableMap.of("y", yearCol)
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, Variable("y", INTEGER), Constant(2026L, INTEGER))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, assignments))
            .containsExactly("(\"y\" = 2026)")
    }

    @Test
    fun testTranslateNeverThrows() {
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, Variable("not_present", VARCHAR), varcharConst("x"))
        assertThat(DuckBridgeExpressionTranslator.translate(expression, ASSIGNMENTS)).isNull()
    }

    @Test
    fun testArithmeticAdd() {
        val add: ConnectorExpression =
            call(StandardFunctions.ADD_FUNCTION_NAME, BIGINT, Variable("id", BIGINT), Constant(1L, BIGINT))
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, add, Constant(10L, BIGINT))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("((\"id\" + 1) = 10)")
    }

    @Test
    fun testArithmeticPushedOperators() {
        val cases: List<Pair<FunctionName, String>> =
            listOf(
                StandardFunctions.SUBTRACT_FUNCTION_NAME to "((\"id\" - 1) = 10)",
                StandardFunctions.MULTIPLY_FUNCTION_NAME to "((\"id\" * 1) = 10)",
            )
        for ((op, expectedSql) in cases) {
            val arith: ConnectorExpression = call(op, BIGINT, Variable("id", BIGINT), Constant(1L, BIGINT))
            val expression: ConnectorExpression =
                call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, arith, Constant(10L, BIGINT))
            assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).containsExactly(expectedSql)
        }
    }

    @Test
    fun testDivideAndModuloAreNotPushed() {
        val notPushed = arrayOf(StandardFunctions.DIVIDE_FUNCTION_NAME, StandardFunctions.MODULO_FUNCTION_NAME)
        for (op in notPushed) {
            val arith: ConnectorExpression = call(op, BIGINT, Variable("id", BIGINT), Constant(1L, BIGINT))
            val expression: ConnectorExpression =
                call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, arith, Constant(10L, BIGINT))
            assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).isEmpty()
        }
    }

    @Test
    fun testCoalesceTwoArg() {
        val coalesce: ConnectorExpression =
            call(StandardFunctions.COALESCE_FUNCTION_NAME, VARCHAR, Variable("name", VARCHAR), varcharConst("unknown"))
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, coalesce, varcharConst("apple"))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(coalesce(\"name\", 'unknown') = 'apple')")
    }

    @Test
    fun testCoalesceVariadic() {
        val coalesce: ConnectorExpression =
            call(
                StandardFunctions.COALESCE_FUNCTION_NAME,
                VARCHAR,
                Variable("name", VARCHAR),
                Variable("name", VARCHAR),
                varcharConst("x"),
                varcharConst("y"),
            )
        val expression: ConnectorExpression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, coalesce)
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(coalesce(\"name\", \"name\", 'x', 'y') IS NULL)")
    }

    @Test
    fun testNullif() {
        val nullif: ConnectorExpression =
            call(StandardFunctions.NULLIF_FUNCTION_NAME, VARCHAR, Variable("name", VARCHAR), varcharConst(""))
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, nullif, varcharConst("apple"))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(nullif(\"name\", '') = 'apple')")
    }

    @Test
    fun testIdenticalNullSafeEquality() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                Variable("name", VARCHAR),
                Constant(null, VARCHAR),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(\"name\" IS NOT DISTINCT FROM NULL)")
    }

    @Test
    fun testNegateUnaryMinus() {
        val negate: ConnectorExpression = call(StandardFunctions.NEGATE_FUNCTION_NAME, BIGINT, Variable("id", BIGINT))
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, negate, Constant(-5L, BIGINT))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("((-\"id\") = -5)")
    }

    @Test
    fun testCastToBigint() {
        val cast: ConnectorExpression =
            Call(BIGINT, StandardFunctions.CAST_FUNCTION_NAME, listOf(Variable("name", VARCHAR)))
        val expression: ConnectorExpression =
            call(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, BOOLEAN, cast, Constant(0L, BIGINT))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(CAST(\"name\" AS BIGINT) > 0)")
    }

    @Test
    fun testTryCastToInteger() {
        val tryCast: ConnectorExpression =
            Call(IntegerType.INTEGER, StandardFunctions.TRY_CAST_FUNCTION_NAME, listOf(Variable("name", VARCHAR)))
        val expression: ConnectorExpression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, tryCast)
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(TRY_CAST(\"name\" AS INTEGER) IS NULL)")
    }

    @Test
    fun testCastToUnsupportedTypeIsSkipped() {
        val cast: ConnectorExpression =
            Call(TimestampType.TIMESTAMP_MICROS, StandardFunctions.CAST_FUNCTION_NAME, listOf(Variable("name", VARCHAR)))
        val expression: ConnectorExpression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, cast)
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).isEmpty()
    }

    @Test
    fun testLikeNoEscape() {
        val expression: ConnectorExpression =
            call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN, Variable("name", VARCHAR), likeConst("App%", Optional.empty()))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(\"name\" LIKE 'App%')")
    }

    @Test
    fun testLikeWithEscape() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.LIKE_FUNCTION_NAME,
                BOOLEAN,
                Variable("name", VARCHAR),
                likeConst("A\\%", Optional.of('\\')),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(\"name\" LIKE 'A\\%' ESCAPE '\\')")
    }

    @Test
    fun testLikePatternEscapesSingleQuote() {
        val expression: ConnectorExpression =
            call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN, Variable("name", VARCHAR), likeConst("o'%", Optional.empty()))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(\"name\" LIKE 'o''%')")
    }

    @Test
    fun testLikeEscapeCharIsSingleQuote() {
        val expression: ConnectorExpression =
            call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN, Variable("name", VARCHAR), likeConst("a'%", Optional.of('\'')))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(\"name\" LIKE 'a''%' ESCAPE '''')")
    }

    @Test
    fun testLikeOnPushableFunctionExpression() {
        val lowerCall: ConnectorExpression = call(FunctionName("lower"), VARCHAR, Variable("name", VARCHAR))
        val expression: ConnectorExpression =
            call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN, lowerCall, likeConst("app%", Optional.empty()))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_lower(\"name\") LIKE 'app%')")
    }

    @Test
    fun testNotLikeRecursesThroughNotBranch() {
        val like: ConnectorExpression =
            call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN, Variable("name", VARCHAR), likeConst("App%", Optional.empty()))
        val notLike: ConnectorExpression = call(StandardFunctions.NOT_FUNCTION_NAME, BOOLEAN, like)
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(notLike, ASSIGNMENTS))
            .containsExactly("(NOT (\"name\" LIKE 'App%'))")
    }

    @Test
    fun testLikeWithNonConstantPatternNotPushed() {
        val expression: ConnectorExpression =
            call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN, Variable("name", VARCHAR), Variable("name", VARCHAR))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).isEmpty()
    }

    @Test
    fun testConcatTwoArg() {
        val concat: ConnectorExpression =
            call(FunctionName("concat"), VARCHAR, Variable("name", VARCHAR), varcharConst("X"))
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, concat, varcharConst("AppleX"))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("((\"name\" || 'X') = 'AppleX')")
    }

    @Test
    fun testConcatVariadic() {
        val concat: ConnectorExpression =
            call(
                FunctionName("concat"),
                VARCHAR,
                Variable("name", VARCHAR),
                varcharConst("-"),
                Variable("name", VARCHAR),
                varcharConst("!"),
            )
        val expression: ConnectorExpression =
            call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN, concat, varcharConst("Apple-Apple!"))
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("((\"name\" || '-' || \"name\" || '!') = 'Apple-Apple!')")
    }

    @Test
    fun testConcatArrayOverloadNotPushed() {
        val arrayVarchar = ArrayType(VARCHAR)
        val concat: ConnectorExpression =
            Call(arrayVarchar, FunctionName("concat"), listOf(Variable("name", VARCHAR), Variable("name", VARCHAR)))
        val expression: ConnectorExpression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, concat)
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).isEmpty()
    }

    // --- date/time type-gate tests -----------------------------------------

    @Test
    fun testYearOnDatePushes() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("year"), BIGINT, Variable("d", DATE)),
                Constant(2024L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_year(\"d\") = 2024)")
    }

    @Test
    fun testYearOnTimestampPushes() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("year"), BIGINT, Variable("ts", TIMESTAMP_MILLIS)),
                Constant(2024L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_year(\"ts\") = 2024)")
    }

    @Test
    fun testYearOnTimestampWithTimeZoneDoesNotPushByDefault() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("year"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                Constant(2024L, BIGINT),
            )
        // Session-less overload reads pushdown_timestamp_with_timezone as OFF → not pushed.
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).isEmpty()
    }

    @Test
    fun testDayOfWeekOnDatePushes() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("day_of_week"), BIGINT, Variable("d", DATE)),
                Constant(7L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_day_of_week(\"d\") = 7)")
    }

    @Test
    fun testDayOfWeekOnTimestampDoesNotPush() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("day_of_week"), BIGINT, Variable("ts", TIMESTAMP_MILLIS)),
                Constant(7L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)).isEmpty()
    }

    @Test
    fun testDateTruncOnDatePushes() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("date_trunc"), DATE, varcharConst("month"), Variable("d", DATE)),
                Variable("d", DATE),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_date_trunc('month', \"d\") = \"d\")")
    }

    @Test
    fun testDateDiffOnDatesPushes() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("date_diff"), BIGINT, varcharConst("day"), Variable("d", DATE), Variable("d", DATE)),
                Constant(0L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_date_diff('day', \"d\", \"d\") = 0)")
    }

    @Test
    fun testToUnixtimeOnTimestampPushes() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("to_unixtime"), DOUBLE, Variable("ts", TIMESTAMP_MILLIS)),
                Constant(0.0, DOUBLE),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_to_unixtime(\"ts\") = 0.0)")
    }

    // --- TIMESTAMP WITH TIME ZONE gated by session property -----------------

    @Test
    fun testYearOnTimestampWithTimeZonePushesWhenPropertyOn() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("year"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                Constant(2024L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS, sessionWithTierC(true)))
            .containsExactly("(trino_year(\"tstz\") = 2024)")
    }

    @Test
    fun testYearOnTimestampWithTimeZoneBlockedWhenPropertyOff() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("year"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                Constant(2024L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS, sessionWithTierC(false)))
            .isEmpty()
    }

    @Test
    fun testToUnixtimeOnTimestampWithTimeZonePushesWhenPropertyOn() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("to_unixtime"), DOUBLE, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                Constant(0.0, DOUBLE),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS, sessionWithTierC(true)))
            .containsExactly("(trino_to_unixtime(\"tstz\") = 0.0)")
    }

    @Test
    fun testDayOfWeekOnTimestampWithTimeZoneNeverPushes() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("day_of_week"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                Constant(7L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS, sessionWithTierC(true)))
            .isEmpty()
    }

    @Test
    fun testWithTimezoneOnTimestampPushes() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.IS_NULL_FUNCTION_NAME,
                BOOLEAN,
                call(
                    FunctionName("with_timezone"),
                    TIMESTAMP_TZ_MILLIS,
                    Variable("ts", TIMESTAMP_MILLIS),
                    varcharConst("America/Los_Angeles"),
                ),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_with_timezone(\"ts\", 'America/Los_Angeles') IS NULL)")
    }

    @Test
    fun testLengthOnVarcharStillPushesUngated() {
        val expression: ConnectorExpression =
            call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                BOOLEAN,
                call(FunctionName("length"), BIGINT, Variable("name", VARCHAR)),
                Constant(5L, BIGINT),
            )
        assertThat(DuckBridgeExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
            .containsExactly("(trino_length(\"name\") = 5)")
    }

    @Test
    fun testPushableSetContainsRepresentativeEntries() {
        assertThat(DuckBridgeExpressionTranslator.PUSHABLE_FUNCTIONS)
            .contains(
                DuckBridgeExpressionTranslator.NameArity("length", 1),
                DuckBridgeExpressionTranslator.NameArity("substring", 2),
                DuckBridgeExpressionTranslator.NameArity("substring", 3),
                DuckBridgeExpressionTranslator.NameArity("starts_with", 2),
                DuckBridgeExpressionTranslator.NameArity("abs", 1),
                DuckBridgeExpressionTranslator.NameArity("power", 2),
                DuckBridgeExpressionTranslator.NameArity("regexp_like", 2),
                DuckBridgeExpressionTranslator.NameArity("lower", 1),
                DuckBridgeExpressionTranslator.NameArity("upper", 1),
                DuckBridgeExpressionTranslator.NameArity("reverse", 1),
                DuckBridgeExpressionTranslator.NameArity("chr", 1),
                DuckBridgeExpressionTranslator.NameArity("levenshtein_distance", 2),
            )
    }

    companion object {
        private val NAME_COLUMN = jdbcColumn("name", VARCHAR)
        private val ID_COLUMN = jdbcColumn("id", BIGINT)
        private val DATE_COLUMN = jdbcColumn("d", DATE)
        private val TS_COLUMN = jdbcColumn("ts", TIMESTAMP_MILLIS)
        private val TSTZ_COLUMN = jdbcColumn("tstz", TIMESTAMP_TZ_MILLIS)
        private val ASSIGNMENTS: Map<String, ColumnHandle> =
            ImmutableMap.of(
                "name",
                NAME_COLUMN,
                "id",
                ID_COLUMN,
                "d",
                DATE_COLUMN,
                "ts",
                TS_COLUMN,
                "tstz",
                TSTZ_COLUMN,
            )

        private fun jdbcColumn(name: String, type: Type): JdbcColumnHandle {
            val typeHandle =
                JdbcTypeHandle(Types.OTHER, Optional.of(type.displayName), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
            return JdbcColumnHandle(name, typeHandle, type)
        }

        private fun sessionWithTierC(value: Boolean): ConnectorSession =
            TestingConnectorSession.builder()
                .setPropertyMetadata(DuckBridgeSessionProperties().getSessionProperties())
                .setPropertyValues(
                    java.util.Map.of<String, Any>(DuckBridgeSessionProperties.PUSHDOWN_TIMESTAMP_WITH_TIME_ZONE, value),
                )
                .build()

        private fun call(name: FunctionName, returnType: Type, vararg args: ConnectorExpression): ConnectorExpression =
            Call(returnType, name, listOf(*args))

        private fun varcharConst(s: String): ConnectorExpression = Constant(Slices.utf8Slice(s), VARCHAR)

        private fun likeConst(pattern: String, escape: Optional<Char>): ConnectorExpression =
            Constant(LikePattern.compile(pattern, escape), LikePatternType.LIKE_PATTERN)
    }
}
