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

import com.google.common.collect.ImmutableMap
import io.airlift.slice.Slices
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
import java.util.Optional

/**
 * Unit tests for [DuckDbExpressionTranslator]. These build synthetic
 * [ConnectorExpression] trees that mimic what Trino would hand to
 * `applyFilter`, then assert the SQL fragments the translator emits.
 */
class TestDuckDbExpressionTranslator {

    @Test
    fun testEqualOnPushableFunction() {
        // WHERE length(name) = 5
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("length"), BIGINT, Variable("name", VARCHAR)),
            Constant(5L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)")
    }

    @Test
    fun testAndOfPushableAndNonPushable() {
        // WHERE length(name) = 5 AND some_unmapped_function(name) = 'x'
        // length/1 is in PUSHABLE_FUNCTIONS; some_unmapped_function is not in the
        // catalog at all, so the translator must skip it.
        val pushable: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("length"), BIGINT, Variable("name", VARCHAR)),
            Constant(5L, BIGINT)
        )
        val notPushable: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("some_unmapped_function"), VARCHAR, Variable("name", VARCHAR)),
            varcharConst("x")
        )
        val and: ConnectorExpression = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, pushable, notPushable)

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(and, ASSIGNMENTS)
        // The pushable conjunct survives; the unrecognised conjunct is silently dropped
        // and Trino re-evaluates it above the scan.
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)")
    }

    @Test
    fun testIntegerLiteralAndComparison() {
        // WHERE id >= 42
        val expression: ConnectorExpression = call(
            StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            Variable("id", BIGINT),
            Constant(42L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(\"id\" >= 42)")
    }

    @Test
    fun testIsNullOnPushableFunction() {
        // WHERE trim(name) IS NULL
        val expression: ConnectorExpression = call(
            StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("trim"), VARCHAR, Variable("name", VARCHAR))
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_trim(\"name\") IS NULL)")
    }

    @Test
    fun testTopLevelAndIsDecomposed() {
        // WHERE length(name) = 5 AND id < 10
        val pushable1: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("length"), BIGINT, Variable("name", VARCHAR)),
            Constant(5L, BIGINT)
        )
        val pushable2: ConnectorExpression = call(
            StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, BOOLEAN,
            Variable("id", BIGINT),
            Constant(10L, BIGINT)
        )
        val and: ConnectorExpression = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, pushable1, pushable2)

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(and, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly(
            "(trino_length(\"name\") = 5)",
            "(\"id\" < 10)"
        )
    }

    @Test
    fun testUnknownFunctionNotPushed() {
        // Function we haven't added to trino_meta → not pushed.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("some_unmapped_function"), VARCHAR, Variable("name", VARCHAR)),
            varcharConst("a")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testPlaceholderFunctionIsPushedWithWarning() {
        // lower/1 IS in PUSHABLE_FUNCTIONS as a placeholder — pushed for performance
        // characterization, but the translator logs a one-shot WARN per name. The
        // emit-side warning is not asserted by this test (avoiding a log-capture
        // dependency); we just confirm the SQL is emitted.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("lower"), VARCHAR, Variable("name", VARCHAR)),
            varcharConst("apple")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_lower(\"name\") = 'apple')")
    }

    @Test
    fun testWrongArityNotPushed() {
        // length/2 is not in trino_meta — only length/1 is.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("length"), BIGINT,
                Variable("name", VARCHAR),
                varcharConst("extra")
            ),
            Constant(5L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testConstantTrueIsSkipped() {
        // `Constraint.alwaysTrue()` shows up as a top-level Constant(TRUE). Don't push it.
        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(Constant.TRUE, mapOf())
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testStringEscaping() {
        // Single quote in literal must double up.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            Variable("name", VARCHAR),
            varcharConst("o'reilly")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(\"name\" = 'o''reilly')")
    }

    @Test
    fun testColumnNameEscaping() {
        // Embedded double-quote in column name must double up.
        val weird = DucklakeColumnHandle(999L, "we\"ird", VARCHAR, true)
        val assignments: Map<String, ColumnHandle> = ImmutableMap.of("v", weird)

        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            Variable("v", VARCHAR),
            varcharConst("a")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, assignments)
        assertThat(conjuncts).containsExactly("(\"we\"\"ird\" = 'a')")
    }

    @Test
    fun testIntegerColumnConstantIsLong() {
        // INTEGER column constant arrives as Long on the stack.
        val yearCol = DucklakeColumnHandle(200L, "y", INTEGER, true)
        val assignments: Map<String, ColumnHandle> = ImmutableMap.of("y", yearCol)

        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            Variable("y", INTEGER),
            Constant(2026L, INTEGER)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, assignments)
        assertThat(conjuncts).containsExactly("(\"y\" = 2026)")
    }

    @Test
    fun testTranslateNeverThrows() {
        // Variable with no assignment → assignment lookup returns null → translation fails for that subterm.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            Variable("not_present", VARCHAR),
            varcharConst("x")
        )

        val result: Optional<String> = DuckDbExpressionTranslator.translate(expression, ASSIGNMENTS)
        assertThat(result).isEmpty
    }

    @Test
    fun testArithmeticAdd() {
        // WHERE id + 1 = 10
        val add: ConnectorExpression = call(
            StandardFunctions.ADD_FUNCTION_NAME, BIGINT,
            Variable("id", BIGINT),
            Constant(1L, BIGINT)
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            add,
            Constant(10L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("((\"id\" + 1) = 10)")
    }

    @Test
    fun testArithmeticPushedOperators() {
        // $subtract and $multiply still push as bare DuckDB operators — both engines
        // align on integer overflow throws and float NaN/Inf propagation, so the wire
        // shape matches Trino-native end-to-end. $add is covered separately above.
        val cases: Array<Array<Any>> = arrayOf(
            arrayOf(StandardFunctions.SUBTRACT_FUNCTION_NAME, "-", "((\"id\" - 1) = 10)"),
            arrayOf(StandardFunctions.MULTIPLY_FUNCTION_NAME, "*", "((\"id\" * 1) = 10)")
        )
        for (c in cases) {
            val op = c[0] as FunctionName
            val expectedSql = c[2] as String
            val arith: ConnectorExpression = call(
                op, BIGINT,
                Variable("id", BIGINT),
                Constant(1L, BIGINT)
            )
            val expression: ConnectorExpression = call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                arith,
                Constant(10L, BIGINT)
            )
            assertThat(DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
                .`as`("operator %s", c[1])
                .containsExactly(expectedSql)
        }
    }

    @Test
    fun testDivideAndModuloAreNotPushed() {
        // B2: $divide and $modulo are INTENTIONALLY not pushed — DuckDB diverges
        // from Trino on integer division semantics (5/2=2.5 vs 2) AND on
        // divide-by-zero handling (DuckDB returns Infinity/NULL silently, Trino
        // throws). A bare-operator push on the .db read path returns wrong rows
        // AND suppresses Trino's expected exceptions, because Trino's above-scan
        // re-evaluation can't restore rows DuckDB stripped at the source.
        // See TestDucklakeArithmeticPushdownParity for the end-to-end RED test
        // that drove this change. The performant alternative is a `trino_divide`
        // / `trino_modulo` parity macro in duckdb-trino-parity-extension — captured
        // in PLAN.md / BEFORE-RESUME B2 as a follow-up.
        val notPushed: Array<FunctionName> = arrayOf(
            StandardFunctions.DIVIDE_FUNCTION_NAME,
            StandardFunctions.MODULO_FUNCTION_NAME
        )
        for (op in notPushed) {
            val arith: ConnectorExpression = call(
                op, BIGINT,
                Variable("id", BIGINT),
                Constant(1L, BIGINT)
            )
            val expression: ConnectorExpression = call(
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                arith,
                Constant(10L, BIGINT)
            )
            assertThat(DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
                .`as`("operator %s must NOT push — semantic divergence from Trino on the .db read path", op)
                .isEmpty()
        }
    }

    @Test
    fun testCoalesceTwoArg() {
        // WHERE COALESCE(name, 'unknown') = 'apple'
        val coalesce: ConnectorExpression = call(
            StandardFunctions.COALESCE_FUNCTION_NAME, VARCHAR,
            Variable("name", VARCHAR),
            varcharConst("unknown")
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            coalesce,
            varcharConst("apple")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(coalesce(\"name\", 'unknown') = 'apple')")
    }

    @Test
    fun testCoalesceVariadic() {
        // WHERE COALESCE(name, name, 'x', 'y') IS NOT NULL — exercise variadic >2
        val coalesce: ConnectorExpression = call(
            StandardFunctions.COALESCE_FUNCTION_NAME, VARCHAR,
            Variable("name", VARCHAR),
            Variable("name", VARCHAR),
            varcharConst("x"),
            varcharConst("y")
        )
        val expression: ConnectorExpression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, coalesce)

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(coalesce(\"name\", \"name\", 'x', 'y') IS NULL)")
    }

    @Test
    fun testNullif() {
        // WHERE NULLIF(name, '') = 'apple'
        val nullif: ConnectorExpression = call(
            StandardFunctions.NULLIF_FUNCTION_NAME, VARCHAR,
            Variable("name", VARCHAR),
            varcharConst("")
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            nullif,
            varcharConst("apple")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(nullif(\"name\", '') = 'apple')")
    }

    @Test
    fun testIdenticalNullSafeEquality() {
        // WHERE name IS NOT DISTINCT FROM NULL — Trino encodes as $identical
        val expression: ConnectorExpression = call(
            StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            Variable("name", VARCHAR),
            Constant(null, VARCHAR)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(\"name\" IS NOT DISTINCT FROM NULL)")
    }

    @Test
    fun testCombinedArithmeticAndCoalesce() {
        // WHERE (id + 1) > COALESCE(id, 0) — mixes the new branches with existing ones.
        val add: ConnectorExpression = call(
            StandardFunctions.ADD_FUNCTION_NAME, BIGINT,
            Variable("id", BIGINT),
            Constant(1L, BIGINT)
        )
        val coalesce: ConnectorExpression = call(
            StandardFunctions.COALESCE_FUNCTION_NAME, BIGINT,
            Variable("id", BIGINT),
            Constant(0L, BIGINT)
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, BOOLEAN,
            add, coalesce
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("((\"id\" + 1) > coalesce(\"id\", 0))")
    }

    @Test
    fun testNegateUnaryMinus() {
        // WHERE -id = -5  →  ((-"id") = -5)
        val negate: ConnectorExpression = call(
            StandardFunctions.NEGATE_FUNCTION_NAME, BIGINT,
            Variable("id", BIGINT)
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            negate,
            Constant(-5L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("((-\"id\") = -5)")
    }

    @Test
    fun testCastToBigint() {
        // WHERE CAST(name AS BIGINT) > 0  — Trino encodes CAST as $cast with result type
        val cast: ConnectorExpression = Call(
            BIGINT, StandardFunctions.CAST_FUNCTION_NAME,
            listOf(Variable("name", VARCHAR))
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, BOOLEAN,
            cast,
            Constant(0L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(CAST(\"name\" AS BIGINT) > 0)")
    }

    @Test
    fun testTryCastToInteger() {
        // WHERE TRY_CAST(name AS INTEGER) IS NULL
        val tryCast: ConnectorExpression = Call(
            IntegerType.INTEGER,
            StandardFunctions.TRY_CAST_FUNCTION_NAME,
            listOf(Variable("name", VARCHAR))
        )
        val expression: ConnectorExpression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, tryCast)

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(TRY_CAST(\"name\" AS INTEGER) IS NULL)")
    }

    @Test
    fun testCastToUnsupportedTypeIsSkipped() {
        // CAST to a type we don't map (e.g. an unsupported nested type) → translation fails.
        // Using TIMESTAMP_MICROS as a proxy for "not in duckdbTypeName()" — currently unsupported.
        val cast: ConnectorExpression = Call(
            TimestampType.TIMESTAMP_MICROS,
            StandardFunctions.CAST_FUNCTION_NAME,
            listOf(Variable("name", VARCHAR))
        )
        val expression: ConnectorExpression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, cast)

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        // Whole conjunct dropped because the inner $cast failed.
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testLikeNoEscape() {
        // WHERE name LIKE 'App%'
        val expression: ConnectorExpression = call(
            StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
            Variable("name", VARCHAR),
            likeConst("App%", Optional.empty())
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(\"name\" LIKE 'App%')")
    }

    @Test
    fun testLikeWithEscape() {
        // WHERE name LIKE 'A\%' ESCAPE '\'
        val expression: ConnectorExpression = call(
            StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
            Variable("name", VARCHAR),
            likeConst("A\\%", Optional.of('\\'))
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(\"name\" LIKE 'A\\%' ESCAPE '\\')")
    }

    @Test
    fun testLikePatternEscapesSingleQuote() {
        // Pattern containing a single quote must be doubled in emitted SQL.
        val expression: ConnectorExpression = call(
            StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
            Variable("name", VARCHAR),
            likeConst("o'%", Optional.empty())
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(\"name\" LIKE 'o''%')")
    }

    @Test
    fun testLikeEscapeCharIsSingleQuote() {
        // The escape character itself is a single quote — must be doubled inside the ESCAPE clause.
        val expression: ConnectorExpression = call(
            StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
            Variable("name", VARCHAR),
            likeConst("a'%", Optional.of('\''))
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(\"name\" LIKE 'a''%' ESCAPE '''')")
    }

    @Test
    fun testLikeOnPushableFunctionExpression() {
        // WHERE lower(name) LIKE 'app%'  — value side is itself a translated call.
        val lowerCall: ConnectorExpression = call(
            FunctionName("lower"), VARCHAR,
            Variable("name", VARCHAR)
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
            lowerCall,
            likeConst("app%", Optional.empty())
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_lower(\"name\") LIKE 'app%')")
    }

    @Test
    fun testNotLikeRecursesThroughNotBranch() {
        // NOT LIKE arrives as Call($not, [Call($like, ...)]). The existing $not handler
        // recurses into our new LIKE branch automatically — no separate code path needed.
        val like: ConnectorExpression = call(
            StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
            Variable("name", VARCHAR),
            likeConst("App%", Optional.empty())
        )
        val notLike: ConnectorExpression = call(StandardFunctions.NOT_FUNCTION_NAME, BOOLEAN, like)

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(notLike, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(NOT (\"name\" LIKE 'App%'))")
    }

    @Test
    fun testLikeWithNonConstantPatternNotPushed() {
        // Dynamic pattern (a column) → not pushable; whole conjunct dropped.
        val expression: ConnectorExpression = call(
            StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
            Variable("name", VARCHAR),
            Variable("name", VARCHAR)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testLikeWithUntranslatableValueNotPushed() {
        // Value side references an unmapped variable → translation fails for that subterm.
        val expression: ConnectorExpression = call(
            StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
            Variable("not_present", VARCHAR),
            likeConst("App%", Optional.empty())
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testConcatTwoArg() {
        // WHERE concat(name, 'X') = 'AppleX'  — translator rewrites to `||` chain,
        // not a trino_concat macro, because DuckDB's concat skips NULL while Trino's
        // NULL-propagates. `||` propagates in both engines (archive/REPORT-hash-null-handling.md).
        val concat: ConnectorExpression = call(
            FunctionName("concat"), VARCHAR,
            Variable("name", VARCHAR),
            varcharConst("X")
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            concat,
            varcharConst("AppleX")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("((\"name\" || 'X') = 'AppleX')")
    }

    @Test
    fun testConcatVariadic() {
        // WHERE concat(name, '-', name, '!') = 'Apple-Apple!'  — exercise arity > 2.
        val concat: ConnectorExpression = call(
            FunctionName("concat"), VARCHAR,
            Variable("name", VARCHAR),
            varcharConst("-"),
            Variable("name", VARCHAR),
            varcharConst("!")
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            concat,
            varcharConst("Apple-Apple!")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("((\"name\" || '-' || \"name\" || '!') = 'Apple-Apple!')")
    }

    @Test
    fun testConcatWithUntranslatableArgNotPushed() {
        // One arg references an unmapped variable → whole concat fails translation,
        // and the surrounding conjunct is dropped.
        val concat: ConnectorExpression = call(
            FunctionName("concat"), VARCHAR,
            Variable("name", VARCHAR),
            Variable("not_present", VARCHAR)
        )
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            concat,
            varcharConst("x")
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testConcatArrayOverloadNotPushed() {
        // Trino's `concat(array, array)` is a separate overload with its own NULL
        // semantics; we only rewrite when the return type is VARCHAR. An ARRAY-typed
        // concat falls through to the macro check, where it's not registered → drop.
        val arrayVarchar = ArrayType(VARCHAR)
        val concat: ConnectorExpression = Call(
            arrayVarchar, FunctionName("concat"),
            listOf(Variable("name", VARCHAR), Variable("name", VARCHAR))
        )
        val expression: ConnectorExpression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, concat)

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    // --- Step 4 chunk 1: date/time type-gate tests --------------------------
    //
    // The translator's TYPE_GATES map restricts certain (name, arity) entries
    // to specific argument types. These tests pin both sides of the gate:
    // positive (correct type → push) and negative (TIMESTAMP WITH TIME ZONE
    // → don't push, because the session-TZ plumbing for Tier C hasn't shipped).

    @Test
    fun testYearOnDatePushes() {
        // WHERE year(d) = 2024  (Tier B-allowed shape: DATE)
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("year"), BIGINT, Variable("d", DATE)),
            Constant(2024L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_year(\"d\") = 2024)")
    }

    @Test
    fun testYearOnTimestampPushes() {
        // WHERE year(ts) = 2024  (Tier B-allowed shape: TIMESTAMP no-TZ)
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("year"), BIGINT, Variable("ts", TIMESTAMP_MILLIS)),
            Constant(2024L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_year(\"ts\") = 2024)")
    }

    @Test
    fun testYearOnTimestampWithTimeZoneDoesNotPush() {
        // The whole point of the type gate. year(timestamp_with_time_zone) silently
        // diverges because the result depends on the session TimeZone, which the
        // connector does not yet set on attach. Tier C ships behind a session flag
        // in chunk 3; until then this conjunct must stay unpushed and Trino
        // re-evaluates it above the scan.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("year"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(2024L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testHourOnTimestampPushes() {
        // WHERE hour(ts) = 22 — Tier B function on a TIMESTAMP no-TZ column.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("hour"), BIGINT, Variable("ts", TIMESTAMP_MILLIS)),
            Constant(22L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_hour(\"ts\") = 22)")
    }

    @Test
    fun testHourOnTimestampWithTimeZoneDoesNotPush() {
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("hour"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(22L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testDayOfWeekOnDatePushes() {
        // WHERE day_of_week(d) = 7  (ISO Sunday — Tier A, DATE-only)
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("day_of_week"), BIGINT, Variable("d", DATE)),
            Constant(7L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_day_of_week(\"d\") = 7)")
    }

    @Test
    fun testDayOfWeekOnTimestampDoesNotPush() {
        // Tier A is gated strictly to DATE. Trino's day_of_week ACCEPTS timestamp
        // input, but DuckDB's bare isodow may handle it differently across versions;
        // until probed, we stay conservative and don't push on TIMESTAMP inputs.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("day_of_week"), BIGINT, Variable("ts", TIMESTAMP_MILLIS)),
            Constant(7L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testYearOfWeekSmokingGunDoesNotPushOnTimestampWithTimeZone() {
        // The ISO-year ≠ calendar-year case ('2024-12-30' → year_of_week 2025) is
        // the pressure-point fixture. On a TIMESTAMP WITH TIME ZONE column we
        // additionally have the year-boundary cross-zone hazard (an instant near
        // midnight may render in a different calendar year under different session
        // TZs). Both reasons → not pushable yet.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("year_of_week"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(2025L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testDateTruncOnDatePushes() {
        // WHERE date_trunc('month', d) = ... — second arg is DATE → push.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("date_trunc"), DATE,
                varcharConst("month"),
                Variable("d", DATE)
            ),
            Variable("d", DATE)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_date_trunc('month', \"d\") = \"d\")")
    }

    @Test
    fun testDateTruncOnTimestampWithTimeZoneDoesNotPush() {
        // Pre-chunk-3 latent bug fix: date_trunc previously pushed for ANY arg type.
        val expression: ConnectorExpression = call(
            StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("date_trunc"), TIMESTAMP_TZ_MILLIS,
                varcharConst("month"),
                Variable("tstz", TIMESTAMP_TZ_MILLIS)
            )
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testDateDiffOnDatesPushes() {
        // WHERE date_diff('day', d1, d2) = ... — both date args are DATE → push.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("date_diff"), BIGINT,
                varcharConst("day"),
                Variable("d", DATE),
                Variable("d", DATE)
            ),
            Constant(0L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly(
            "(trino_date_diff('day', \"d\", \"d\") = 0)"
        )
    }

    @Test
    fun testDateDiffMixedTimestampWithTimeZoneDoesNotPush() {
        // Even one TIMESTAMP WTZ arg blocks pushdown — date_diff with mixed
        // wall-clock vs. instant semantics is exactly the case the type gate exists
        // to prevent silent divergence on.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("date_diff"), BIGINT,
                varcharConst("day"),
                Variable("ts", TIMESTAMP_MILLIS),
                Variable("tstz", TIMESTAMP_TZ_MILLIS)
            ),
            Constant(0L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testToUnixtimeOnTimestampPushes() {
        // WHERE to_unixtime(ts) = 0 — Tier B, DOUBLE return.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("to_unixtime"), DOUBLE, Variable("ts", TIMESTAMP_MILLIS)),
            Constant(0.0, DOUBLE)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_to_unixtime(\"ts\") = 0.0)")
    }

    // --- Chunk 3: TIMESTAMP WITH TIME ZONE gated by session property ---------

    @Test
    fun testToUnixtimeOnTimestampWithTimeZonePushesWhenPropertyOn() {
        // The one Tier C entry that's safe to push: to_unixtime returns the
        // absolute UTC epoch regardless of the value's stored zone or DuckDB's
        // session zone, so Trino's above-scan eval and DuckDB-side eval agree
        // byte-for-byte. Property on → pushes.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("to_unixtime"), DOUBLE, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(0.0, DOUBLE)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).containsExactly("(trino_to_unixtime(\"tstz\") = 0.0)")
    }

    @Test
    fun testToUnixtimeOnTimestampWithTimeZoneDoesNotPushWhenPropertyOff() {
        // Same expression, property explicitly off — to_unixtime falls back to
        // the chunk-1 strict gate (DATE or TIMESTAMP no-TZ only). Predicate
        // stays unpushed; Trino evaluates above the scan.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("to_unixtime"), DOUBLE, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(0.0, DOUBLE)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(false)
        )
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testYearOnTimestampWithTimeZonePushesWhenPropertyOn() {
        // Chunk 3.5: zone-dependent extracts over WTZ are now safe to push when
        // the property is on. The chunk-3.5 converter fix constructs incoming
        // WTZ values with the Arrow schema TZ (= session zone), so Trino's
        // above-scan year() and DuckDB-side year() interpret the same instant in
        // the same zone — results agree.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("year"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(2024L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).containsExactly("(trino_year(\"tstz\") = 2024)")
    }

    @Test
    fun testYearOnTimestampWithTimeZoneStillBlockedWhenPropertyOff() {
        // Property off is the default and the safe baseline — never push WTZ.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("year"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(2024L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(false)
        )
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testHourOnTimestampWithTimeZonePushesWhenPropertyOn() {
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("hour"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(22L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).containsExactly("(trino_hour(\"tstz\") = 22)")
    }

    @Test
    fun testDateTruncOnTimestampWithTimeZonePushesWhenPropertyOn() {
        val expression: ConnectorExpression = call(
            StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("date_trunc"), TIMESTAMP_TZ_MILLIS,
                varcharConst("month"),
                Variable("tstz", TIMESTAMP_TZ_MILLIS)
            )
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).containsExactly("(trino_date_trunc('month', \"tstz\") IS NULL)")
    }

    @Test
    fun testDateDiffWithBothTimestampWithTimeZonePushesWhenPropertyOn() {
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("date_diff"), BIGINT,
                varcharConst("day"),
                Variable("tstz", TIMESTAMP_TZ_MILLIS),
                Variable("tstz", TIMESTAMP_TZ_MILLIS)
            ),
            Constant(0L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).containsExactly(
            "(trino_date_diff('day', \"tstz\", \"tstz\") = 0)"
        )
    }

    @Test
    fun testDateDiffMixedDateAndTimestampWithTimeZonePushesWhenPropertyOn() {
        // Mixed-kind args: DATE + TIMESTAMP WTZ. Each arg clears its own per-arg
        // gate independently; the connector trusts both engines to handle the
        // type promotion identically.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("date_diff"), BIGINT,
                varcharConst("day"),
                Variable("d", DATE),
                Variable("tstz", TIMESTAMP_TZ_MILLIS)
            ),
            Constant(0L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).containsExactly(
            "(trino_date_diff('day', \"d\", \"tstz\") = 0)"
        )
    }

    @Test
    fun testDayOfWeekOnTimestampWithTimeZoneNeverPushes() {
        // Tier A (DATE-only) — Trino's day_of_week doesn't accept WTZ inputs, so
        // even with the Tier C property on, the gate refuses. This pins that the
        // Tier A entries don't accidentally get widened.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("day_of_week"), BIGINT, Variable("tstz", TIMESTAMP_TZ_MILLIS)),
            Constant(7L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testFromUnixtimePushesUnconditionally() {
        // from_unixtime(DOUBLE) → WTZ. Input is DOUBLE — no gate, no Tier C
        // property needed. The output WTZ value will be tagged with the session
        // zone by the chunk-3.5 converter when read back.
        val epochCol = DucklakeColumnHandle(106L, "epoch", DOUBLE, true)
        val assignments: Map<String, ColumnHandle> = ImmutableMap.of("epoch", epochCol)
        val expression: ConnectorExpression = call(
            StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("from_unixtime"), TIMESTAMP_TZ_MILLIS,
                Variable("epoch", DOUBLE)
            )
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, assignments
        )
        // Property off — still pushes (no gate).
        assertThat(conjuncts).containsExactly("(trino_from_unixtime(\"epoch\") IS NULL)")
    }

    @Test
    fun testWithTimezoneOnTimestampPushes() {
        // with_timezone(TIMESTAMP no-TZ, varchar) → WTZ. Gated to TIMESTAMP-only.
        val expression: ConnectorExpression = call(
            StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("with_timezone"), TIMESTAMP_TZ_MILLIS,
                Variable("ts", TIMESTAMP_MILLIS),
                varcharConst("America/Los_Angeles")
            )
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS
        )
        assertThat(conjuncts).containsExactly(
            "(trino_with_timezone(\"ts\", 'America/Los_Angeles') IS NULL)"
        )
    }

    @Test
    fun testWithTimezoneOnDateDoesNotPush() {
        // Trino's with_timezone signature is (timestamp(p), varchar) — DATE input
        // wouldn't match in Trino's planner either, but the gate refuses defensively.
        val expression: ConnectorExpression = call(
            StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("with_timezone"), TIMESTAMP_TZ_MILLIS,
                Variable("d", DATE),
                varcharConst("America/Los_Angeles")
            )
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS
        )
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testWithTimezoneOnTimestampWithTimeZoneDoesNotPush() {
        // Re-zoning a WTZ value is `at_timezone` in Trino, not `with_timezone`.
        // `at_timezone` stays unpushable through this connector because DuckDB's
        // AT TIME ZONE on a TIMESTAMPTZ returns TIMESTAMP no-TZ — type mismatch.
        // with_timezone gated strictly to TIMESTAMP no-TZ input.
        val expression: ConnectorExpression = call(
            StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
            call(
                FunctionName("with_timezone"), TIMESTAMP_TZ_MILLIS,
                Variable("tstz", TIMESTAMP_TZ_MILLIS),
                varcharConst("America/Los_Angeles")
            )
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).isEmpty()
    }

    @Test
    fun testYearOnDateStillPushesWhenPropertyOn() {
        // Regression guard: turning the property on must NOT regress the Tier B
        // shape. DATE input still pushes regardless of property value.
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("year"), BIGINT, Variable("d", DATE)),
            Constant(2024L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(
            expression, ASSIGNMENTS, sessionWithTierC(true)
        )
        assertThat(conjuncts).containsExactly("(trino_year(\"d\") = 2024)")
    }

    @Test
    fun testLengthOnVarcharStillPushesAfterTypeGateLanded() {
        // Regression guard: the sparse TYPE_GATES registry must NOT affect entries
        // that have no gate. length/1 has no gate → must still push for any arg
        // type (in practice always VARCHAR-typed at the call site, but the
        // translator must not gain a new "ungated entries are now restricted" bug).
        val expression: ConnectorExpression = call(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
            call(FunctionName("length"), BIGINT, Variable("name", VARCHAR)),
            Constant(5L, BIGINT)
        )

        val conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS)
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)")
    }

    @Test
    fun testTrinoMetaJavaSetContainsRepresentativeEntries() {
        // Tripwire: the Java-side PUSHABLE_FUNCTIONS must match the trino_meta() rows
        // in trino-function-aliases.sql. The full parity-with-DuckDB check is in
        // TestTrinoFunctionAliases#testJavaPushableSetMatchesDuckDbMeta — this is a
        // cheaper smoke check that a few well-known entries are present.
        // lower/upper/reverse are PUSHABLE PLACEHOLDERS: pushed for perf with warn-on-emit.
        assertThat(DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS)
            .contains(
                DuckDbExpressionTranslator.NameArity("length", 1),
                DuckDbExpressionTranslator.NameArity("substring", 2),
                DuckDbExpressionTranslator.NameArity("substring", 3),
                DuckDbExpressionTranslator.NameArity("starts_with", 2),
                DuckDbExpressionTranslator.NameArity("abs", 1),
                DuckDbExpressionTranslator.NameArity("power", 2),
                DuckDbExpressionTranslator.NameArity("regexp_like", 2),
                DuckDbExpressionTranslator.NameArity("lower", 1),
                DuckDbExpressionTranslator.NameArity("upper", 1),
                DuckDbExpressionTranslator.NameArity("reverse", 1),
                DuckDbExpressionTranslator.NameArity("chr", 1),
                DuckDbExpressionTranslator.NameArity("levenshtein_distance", 2)
            )
    }

    // --- Helpers ------------------------------------------------------------

    companion object {
        private val NAME_COLUMN = DucklakeColumnHandle(101L, "name", VARCHAR, true)
        private val ID_COLUMN = DucklakeColumnHandle(102L, "id", BIGINT, true)
        private val DATE_COLUMN = DucklakeColumnHandle(103L, "d", DATE, true)
        private val TS_COLUMN = DucklakeColumnHandle(104L, "ts", TIMESTAMP_MILLIS, true)
        private val TSTZ_COLUMN = DucklakeColumnHandle(105L, "tstz", TIMESTAMP_TZ_MILLIS, true)
        private val ASSIGNMENTS: Map<String, ColumnHandle> = ImmutableMap.of(
            "name", NAME_COLUMN,
            "id", ID_COLUMN,
            "d", DATE_COLUMN,
            "ts", TS_COLUMN,
            "tstz", TSTZ_COLUMN
        )

        /** Builds a session with `pushdown_timestamp_with_timezone` = `value`. */
        private fun sessionWithTierC(value: Boolean): ConnectorSession {
            return TestingConnectorSession.builder()
                .setPropertyMetadata(DucklakeSessionProperties().getSessionProperties())
                .setPropertyValues(
                    java.util.Map.of<String, Any>(
                        DucklakeSessionProperties.PUSHDOWN_TIMESTAMP_WITH_TIMEZONE, value
                    )
                )
                .build()
        }

        private fun call(name: FunctionName, returnType: Type, vararg args: ConnectorExpression): ConnectorExpression {
            return Call(returnType, name, listOf(*args))
        }

        private fun varcharConst(s: String): ConnectorExpression {
            return Constant(Slices.utf8Slice(s), VARCHAR)
        }

        @Suppress("unused")
        private fun boolConst(b: Boolean): ConnectorExpression {
            return Constant(b, BooleanType.BOOLEAN)
        }

        private fun likeConst(pattern: String, escape: Optional<Char>): ConnectorExpression {
            // Trino delivers LIKE patterns as a Constant of LikePatternType whose value
            // is a compiled io.trino.type.LikePattern. The translator only reads the
            // pattern string and optional escape character — RE2 matcher state is
            // irrelevant for pushdown emission.
            return Constant(LikePattern.compile(pattern, escape), LikePatternType.LIKE_PATTERN)
        }
    }
}
