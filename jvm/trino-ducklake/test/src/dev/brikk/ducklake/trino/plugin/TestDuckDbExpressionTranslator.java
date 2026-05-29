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

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BooleanType;
import io.trino.testing.TestingConnectorSession;
import io.trino.type.LikePattern;
import io.trino.type.LikePatternType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DuckDbExpressionTranslator}. These build synthetic
 * {@link ConnectorExpression} trees that mimic what Trino would hand to
 * {@code applyFilter}, then assert the SQL fragments the translator emits.
 */
public class TestDuckDbExpressionTranslator
{
    private static final DucklakeColumnHandle NAME_COLUMN =
            new DucklakeColumnHandle(101L, "name", VARCHAR, true);
    private static final DucklakeColumnHandle ID_COLUMN =
            new DucklakeColumnHandle(102L, "id", BIGINT, true);
    private static final DucklakeColumnHandle DATE_COLUMN =
            new DucklakeColumnHandle(103L, "d", DATE, true);
    private static final DucklakeColumnHandle TS_COLUMN =
            new DucklakeColumnHandle(104L, "ts", TIMESTAMP_MILLIS, true);
    private static final DucklakeColumnHandle TSTZ_COLUMN =
            new DucklakeColumnHandle(105L, "tstz", TIMESTAMP_TZ_MILLIS, true);
    private static final Map<String, ColumnHandle> ASSIGNMENTS = ImmutableMap.of(
            "name", NAME_COLUMN,
            "id", ID_COLUMN,
            "d", DATE_COLUMN,
            "ts", TS_COLUMN,
            "tstz", TSTZ_COLUMN);

    @Test
    public void testEqualOnPushableFunction()
    {
        // WHERE length(name) = 5
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("length"), BIGINT,
                        new Variable("name", VARCHAR)),
                new Constant(5L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)");
    }

    @Test
    public void testAndOfPushableAndNonPushable()
    {
        // WHERE length(name) = 5 AND some_unmapped_function(name) = 'x'
        // length/1 is in PUSHABLE_FUNCTIONS; some_unmapped_function is not in the
        // catalog at all, so the translator must skip it.
        ConnectorExpression pushable = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("length"), BIGINT,
                        new Variable("name", VARCHAR)),
                new Constant(5L, BIGINT));
        ConnectorExpression notPushable = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("some_unmapped_function"), VARCHAR,
                        new Variable("name", VARCHAR)),
                varcharConst("x"));
        ConnectorExpression and = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, pushable, notPushable);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(and, ASSIGNMENTS);
        // The pushable conjunct survives; the unrecognised conjunct is silently dropped
        // and Trino re-evaluates it above the scan.
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)");
    }

    @Test
    public void testIntegerLiteralAndComparison()
    {
        // WHERE id >= 42
        ConnectorExpression expression = call(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                new Variable("id", BIGINT),
                new Constant(42L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(\"id\" >= 42)");
    }

    @Test
    public void testIsNullOnPushableFunction()
    {
        // WHERE trim(name) IS NULL
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("trim"), VARCHAR,
                        new Variable("name", VARCHAR)));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_trim(\"name\") IS NULL)");
    }

    @Test
    public void testTopLevelAndIsDecomposed()
    {
        // WHERE length(name) = 5 AND id < 10
        ConnectorExpression pushable1 = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("length"), BIGINT,
                        new Variable("name", VARCHAR)),
                new Constant(5L, BIGINT));
        ConnectorExpression pushable2 = call(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, BOOLEAN,
                new Variable("id", BIGINT),
                new Constant(10L, BIGINT));
        ConnectorExpression and = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, pushable1, pushable2);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(and, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly(
                "(trino_length(\"name\") = 5)",
                "(\"id\" < 10)");
    }

    @Test
    public void testUnknownFunctionNotPushed()
    {
        // Function we haven't added to trino_meta → not pushed.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("some_unmapped_function"), VARCHAR,
                        new Variable("name", VARCHAR)),
                varcharConst("a"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testPlaceholderFunctionIsPushedWithWarning()
    {
        // lower/1 IS in PUSHABLE_FUNCTIONS as a placeholder — pushed for performance
        // characterization, but the translator logs a one-shot WARN per name. The
        // emit-side warning is not asserted by this test (avoiding a log-capture
        // dependency); we just confirm the SQL is emitted.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("lower"), VARCHAR,
                        new Variable("name", VARCHAR)),
                varcharConst("apple"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_lower(\"name\") = 'apple')");
    }

    @Test
    public void testWrongArityNotPushed()
    {
        // length/2 is not in trino_meta — only length/1 is.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("length"), BIGINT,
                        new Variable("name", VARCHAR),
                        varcharConst("extra")),
                new Constant(5L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testConstantTrueIsSkipped()
    {
        // `Constraint.alwaysTrue()` shows up as a top-level Constant(TRUE). Don't push it.
        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(Constant.TRUE, Map.of());
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testStringEscaping()
    {
        // Single quote in literal must double up.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                new Variable("name", VARCHAR),
                varcharConst("o'reilly"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(\"name\" = 'o''reilly')");
    }

    @Test
    public void testColumnNameEscaping()
    {
        // Embedded double-quote in column name must double up.
        DucklakeColumnHandle weird = new DucklakeColumnHandle(999L, "we\"ird", VARCHAR, true);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("v", weird);

        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                new Variable("v", VARCHAR),
                varcharConst("a"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, assignments);
        assertThat(conjuncts).containsExactly("(\"we\"\"ird\" = 'a')");
    }

    @Test
    public void testIntegerColumnConstantIsLong()
    {
        // INTEGER column constant arrives as Long on the stack.
        DucklakeColumnHandle yearCol = new DucklakeColumnHandle(200L, "y", INTEGER, true);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("y", yearCol);

        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                new Variable("y", INTEGER),
                new Constant(2026L, INTEGER));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, assignments);
        assertThat(conjuncts).containsExactly("(\"y\" = 2026)");
    }

    @Test
    public void testTranslateNeverThrows()
    {
        // Variable with no assignment → assignment lookup returns null → translation fails for that subterm.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                new Variable("not_present", VARCHAR),
                varcharConst("x"));

        Optional<String> result = DuckDbExpressionTranslator.translate(expression, ASSIGNMENTS);
        assertThat(result).isEmpty();
    }

    @Test
    public void testArithmeticAdd()
    {
        // WHERE id + 1 = 10
        ConnectorExpression add = call(StandardFunctions.ADD_FUNCTION_NAME, BIGINT,
                new Variable("id", BIGINT),
                new Constant(1L, BIGINT));
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                add,
                new Constant(10L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("((\"id\" + 1) = 10)");
    }

    @Test
    public void testArithmeticAllOperators()
    {
        // Each of $subtract / $multiply / $divide / $modulo as a single conjunct
        // pinned against the expected DuckDB SQL.
        Object[][] cases = {
                {StandardFunctions.SUBTRACT_FUNCTION_NAME, "-", "((\"id\" - 1) = 10)"},
                {StandardFunctions.MULTIPLY_FUNCTION_NAME, "*", "((\"id\" * 1) = 10)"},
                {StandardFunctions.DIVIDE_FUNCTION_NAME, "/", "((\"id\" / 1) = 10)"},
                {StandardFunctions.MODULO_FUNCTION_NAME, "%", "((\"id\" % 1) = 10)"},
        };
        for (Object[] c : cases) {
            FunctionName op = (FunctionName) c[0];
            String expectedSql = (String) c[2];
            ConnectorExpression arith = call(op, BIGINT,
                    new Variable("id", BIGINT),
                    new Constant(1L, BIGINT));
            ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                    arith,
                    new Constant(10L, BIGINT));
            assertThat(DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS))
                    .as("operator %s", c[1])
                    .containsExactly(expectedSql);
        }
    }

    @Test
    public void testCoalesceTwoArg()
    {
        // WHERE COALESCE(name, 'unknown') = 'apple'
        ConnectorExpression coalesce = call(StandardFunctions.COALESCE_FUNCTION_NAME, VARCHAR,
                new Variable("name", VARCHAR),
                varcharConst("unknown"));
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                coalesce,
                varcharConst("apple"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(coalesce(\"name\", 'unknown') = 'apple')");
    }

    @Test
    public void testCoalesceVariadic()
    {
        // WHERE COALESCE(name, name, 'x', 'y') IS NOT NULL — exercise variadic >2
        ConnectorExpression coalesce = call(StandardFunctions.COALESCE_FUNCTION_NAME, VARCHAR,
                new Variable("name", VARCHAR),
                new Variable("name", VARCHAR),
                varcharConst("x"),
                varcharConst("y"));
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, coalesce);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(coalesce(\"name\", \"name\", 'x', 'y') IS NULL)");
    }

    @Test
    public void testNullif()
    {
        // WHERE NULLIF(name, '') = 'apple'
        ConnectorExpression nullif = call(StandardFunctions.NULLIF_FUNCTION_NAME, VARCHAR,
                new Variable("name", VARCHAR),
                varcharConst(""));
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                nullif,
                varcharConst("apple"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(nullif(\"name\", '') = 'apple')");
    }

    @Test
    public void testIdenticalNullSafeEquality()
    {
        // WHERE name IS NOT DISTINCT FROM NULL — Trino encodes as $identical
        ConnectorExpression expression = call(StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                new Variable("name", VARCHAR),
                new Constant(null, VARCHAR));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(\"name\" IS NOT DISTINCT FROM NULL)");
    }

    @Test
    public void testCombinedArithmeticAndCoalesce()
    {
        // WHERE (id + 1) > COALESCE(id, 0) — mixes the new branches with existing ones.
        ConnectorExpression add = call(StandardFunctions.ADD_FUNCTION_NAME, BIGINT,
                new Variable("id", BIGINT),
                new Constant(1L, BIGINT));
        ConnectorExpression coalesce = call(StandardFunctions.COALESCE_FUNCTION_NAME, BIGINT,
                new Variable("id", BIGINT),
                new Constant(0L, BIGINT));
        ConnectorExpression expression = call(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, BOOLEAN,
                add, coalesce);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("((\"id\" + 1) > coalesce(\"id\", 0))");
    }

    @Test
    public void testNegateUnaryMinus()
    {
        // WHERE -id = -5  →  ((-"id") = -5)
        ConnectorExpression negate = call(StandardFunctions.NEGATE_FUNCTION_NAME, BIGINT,
                new Variable("id", BIGINT));
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                negate,
                new Constant(-5L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("((-\"id\") = -5)");
    }

    @Test
    public void testCastToBigint()
    {
        // WHERE CAST(name AS BIGINT) > 0  — Trino encodes CAST as $cast with result type
        ConnectorExpression cast = new Call(BIGINT, StandardFunctions.CAST_FUNCTION_NAME,
                List.of(new Variable("name", VARCHAR)));
        ConnectorExpression expression = call(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, BOOLEAN,
                cast,
                new Constant(0L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(CAST(\"name\" AS BIGINT) > 0)");
    }

    @Test
    public void testTryCastToInteger()
    {
        // WHERE TRY_CAST(name AS INTEGER) IS NULL
        ConnectorExpression tryCast = new Call(io.trino.spi.type.IntegerType.INTEGER,
                StandardFunctions.TRY_CAST_FUNCTION_NAME,
                List.of(new Variable("name", VARCHAR)));
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, tryCast);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(TRY_CAST(\"name\" AS INTEGER) IS NULL)");
    }

    @Test
    public void testCastToUnsupportedTypeIsSkipped()
    {
        // CAST to a type we don't map (e.g. an unsupported nested type) → translation fails.
        // Using TIMESTAMP_MICROS as a proxy for "not in duckdbTypeName()" — currently unsupported.
        ConnectorExpression cast = new Call(io.trino.spi.type.TimestampType.TIMESTAMP_MICROS,
                StandardFunctions.CAST_FUNCTION_NAME,
                List.of(new Variable("name", VARCHAR)));
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, cast);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        // Whole conjunct dropped because the inner $cast failed.
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testLikeNoEscape()
    {
        // WHERE name LIKE 'App%'
        ConnectorExpression expression = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                new Variable("name", VARCHAR),
                likeConst("App%", Optional.empty()));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(\"name\" LIKE 'App%')");
    }

    @Test
    public void testLikeWithEscape()
    {
        // WHERE name LIKE 'A\%' ESCAPE '\'
        ConnectorExpression expression = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                new Variable("name", VARCHAR),
                likeConst("A\\%", Optional.of('\\')));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(\"name\" LIKE 'A\\%' ESCAPE '\\')");
    }

    @Test
    public void testLikePatternEscapesSingleQuote()
    {
        // Pattern containing a single quote must be doubled in emitted SQL.
        ConnectorExpression expression = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                new Variable("name", VARCHAR),
                likeConst("o'%", Optional.empty()));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(\"name\" LIKE 'o''%')");
    }

    @Test
    public void testLikeEscapeCharIsSingleQuote()
    {
        // The escape character itself is a single quote — must be doubled inside the ESCAPE clause.
        ConnectorExpression expression = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                new Variable("name", VARCHAR),
                likeConst("a'%", Optional.of('\'')));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(\"name\" LIKE 'a''%' ESCAPE '''')");
    }

    @Test
    public void testLikeOnPushableFunctionExpression()
    {
        // WHERE lower(name) LIKE 'app%'  — value side is itself a translated call.
        ConnectorExpression lowerCall = call(new FunctionName("lower"), VARCHAR,
                new Variable("name", VARCHAR));
        ConnectorExpression expression = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                lowerCall,
                likeConst("app%", Optional.empty()));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_lower(\"name\") LIKE 'app%')");
    }

    @Test
    public void testNotLikeRecursesThroughNotBranch()
    {
        // NOT LIKE arrives as Call($not, [Call($like, ...)]). The existing $not handler
        // recurses into our new LIKE branch automatically — no separate code path needed.
        ConnectorExpression like = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                new Variable("name", VARCHAR),
                likeConst("App%", Optional.empty()));
        ConnectorExpression notLike = call(StandardFunctions.NOT_FUNCTION_NAME, BOOLEAN, like);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(notLike, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(NOT (\"name\" LIKE 'App%'))");
    }

    @Test
    public void testLikeWithNonConstantPatternNotPushed()
    {
        // Dynamic pattern (a column) → not pushable; whole conjunct dropped.
        ConnectorExpression expression = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                new Variable("name", VARCHAR),
                new Variable("name", VARCHAR));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testLikeWithUntranslatableValueNotPushed()
    {
        // Value side references an unmapped variable → translation fails for that subterm.
        ConnectorExpression expression = call(StandardFunctions.LIKE_FUNCTION_NAME, BOOLEAN,
                new Variable("not_present", VARCHAR),
                likeConst("App%", Optional.empty()));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testConcatTwoArg()
    {
        // WHERE concat(name, 'X') = 'AppleX'  — translator rewrites to `||` chain,
        // not a trino_concat macro, because DuckDB's concat skips NULL while Trino's
        // NULL-propagates. `||` propagates in both engines (REPORT-hash-null-handling.md).
        ConnectorExpression concat = call(new FunctionName("concat"), VARCHAR,
                new Variable("name", VARCHAR),
                varcharConst("X"));
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                concat,
                varcharConst("AppleX"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("((\"name\" || 'X') = 'AppleX')");
    }

    @Test
    public void testConcatVariadic()
    {
        // WHERE concat(name, '-', name, '!') = 'Apple-Apple!'  — exercise arity > 2.
        ConnectorExpression concat = call(new FunctionName("concat"), VARCHAR,
                new Variable("name", VARCHAR),
                varcharConst("-"),
                new Variable("name", VARCHAR),
                varcharConst("!"));
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                concat,
                varcharConst("Apple-Apple!"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("((\"name\" || '-' || \"name\" || '!') = 'Apple-Apple!')");
    }

    @Test
    public void testConcatWithUntranslatableArgNotPushed()
    {
        // One arg references an unmapped variable → whole concat fails translation,
        // and the surrounding conjunct is dropped.
        ConnectorExpression concat = call(new FunctionName("concat"), VARCHAR,
                new Variable("name", VARCHAR),
                new Variable("not_present", VARCHAR));
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                concat,
                varcharConst("x"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testConcatArrayOverloadNotPushed()
    {
        // Trino's `concat(array, array)` is a separate overload with its own NULL
        // semantics; we only rewrite when the return type is VARCHAR. An ARRAY-typed
        // concat falls through to the macro check, where it's not registered → drop.
        io.trino.spi.type.ArrayType arrayVarchar = new io.trino.spi.type.ArrayType(VARCHAR);
        ConnectorExpression concat = new Call(arrayVarchar, new FunctionName("concat"),
                List.of(new Variable("name", VARCHAR), new Variable("name", VARCHAR)));
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN, concat);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    // --- Step 4 chunk 1: date/time type-gate tests --------------------------
    //
    // The translator's TYPE_GATES map restricts certain (name, arity) entries
    // to specific argument types. These tests pin both sides of the gate:
    // positive (correct type → push) and negative (TIMESTAMP WITH TIME ZONE
    // → don't push, because the session-TZ plumbing for Tier C hasn't shipped).

    @Test
    public void testYearOnDatePushes()
    {
        // WHERE year(d) = 2024  (Tier B-allowed shape: DATE)
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("year"), BIGINT, new Variable("d", DATE)),
                new Constant(2024L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_year(\"d\") = 2024)");
    }

    @Test
    public void testYearOnTimestampPushes()
    {
        // WHERE year(ts) = 2024  (Tier B-allowed shape: TIMESTAMP no-TZ)
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("year"), BIGINT, new Variable("ts", TIMESTAMP_MILLIS)),
                new Constant(2024L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_year(\"ts\") = 2024)");
    }

    @Test
    public void testYearOnTimestampWithTimeZoneDoesNotPush()
    {
        // The whole point of the type gate. year(timestamp_with_time_zone) silently
        // diverges because the result depends on the session TimeZone, which the
        // connector does not yet set on attach. Tier C ships behind a session flag
        // in chunk 3; until then this conjunct must stay unpushed and Trino
        // re-evaluates it above the scan.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("year"), BIGINT, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(2024L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testHourOnTimestampPushes()
    {
        // WHERE hour(ts) = 22 — Tier B function on a TIMESTAMP no-TZ column.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("hour"), BIGINT, new Variable("ts", TIMESTAMP_MILLIS)),
                new Constant(22L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_hour(\"ts\") = 22)");
    }

    @Test
    public void testHourOnTimestampWithTimeZoneDoesNotPush()
    {
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("hour"), BIGINT, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(22L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testDayOfWeekOnDatePushes()
    {
        // WHERE day_of_week(d) = 7  (ISO Sunday — Tier A, DATE-only)
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("day_of_week"), BIGINT, new Variable("d", DATE)),
                new Constant(7L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_day_of_week(\"d\") = 7)");
    }

    @Test
    public void testDayOfWeekOnTimestampDoesNotPush()
    {
        // Tier A is gated strictly to DATE. Trino's day_of_week ACCEPTS timestamp
        // input, but DuckDB's bare isodow may handle it differently across versions;
        // until probed, we stay conservative and don't push on TIMESTAMP inputs.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("day_of_week"), BIGINT, new Variable("ts", TIMESTAMP_MILLIS)),
                new Constant(7L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testYearOfWeekSmokingGunDoesNotPushOnTimestampWithTimeZone()
    {
        // The ISO-year ≠ calendar-year case ('2024-12-30' → year_of_week 2025) is
        // the pressure-point fixture. On a TIMESTAMP WITH TIME ZONE column we
        // additionally have the year-boundary cross-zone hazard (an instant near
        // midnight may render in a different calendar year under different session
        // TZs). Both reasons → not pushable yet.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("year_of_week"), BIGINT, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(2025L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testDateTruncOnDatePushes()
    {
        // WHERE date_trunc('month', d) = ... — second arg is DATE → push.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("date_trunc"), DATE,
                        varcharConst("month"),
                        new Variable("d", DATE)),
                new Variable("d", DATE));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_date_trunc('month', \"d\") = \"d\")");
    }

    @Test
    public void testDateTruncOnTimestampWithTimeZoneDoesNotPush()
    {
        // Pre-chunk-3 latent bug fix: date_trunc previously pushed for ANY arg type.
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("date_trunc"), TIMESTAMP_TZ_MILLIS,
                        varcharConst("month"),
                        new Variable("tstz", TIMESTAMP_TZ_MILLIS)));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testDateDiffOnDatesPushes()
    {
        // WHERE date_diff('day', d1, d2) = ... — both date args are DATE → push.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("date_diff"), BIGINT,
                        varcharConst("day"),
                        new Variable("d", DATE),
                        new Variable("d", DATE)),
                new Constant(0L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly(
                "(trino_date_diff('day', \"d\", \"d\") = 0)");
    }

    @Test
    public void testDateDiffMixedTimestampWithTimeZoneDoesNotPush()
    {
        // Even one TIMESTAMP WTZ arg blocks pushdown — date_diff with mixed
        // wall-clock vs. instant semantics is exactly the case the type gate exists
        // to prevent silent divergence on.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("date_diff"), BIGINT,
                        varcharConst("day"),
                        new Variable("ts", TIMESTAMP_MILLIS),
                        new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(0L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testToUnixtimeOnTimestampPushes()
    {
        // WHERE to_unixtime(ts) = 0 — Tier B, DOUBLE return.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("to_unixtime"), DOUBLE, new Variable("ts", TIMESTAMP_MILLIS)),
                new Constant(0.0, DOUBLE));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_to_unixtime(\"ts\") = 0.0)");
    }

    // --- Chunk 3: TIMESTAMP WITH TIME ZONE gated by session property ---------

    /** Builds a session with {@code pushdown_timestamp_with_timezone} = {@code value}. */
    private static ConnectorSession sessionWithTierC(boolean value)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new DucklakeSessionProperties().getSessionProperties())
                .setPropertyValues(java.util.Map.of(
                        DucklakeSessionProperties.PUSHDOWN_TIMESTAMP_WITH_TIMEZONE, value))
                .build();
    }

    @Test
    public void testToUnixtimeOnTimestampWithTimeZonePushesWhenPropertyOn()
    {
        // The one Tier C entry that's safe to push: to_unixtime returns the
        // absolute UTC epoch regardless of the value's stored zone or DuckDB's
        // session zone, so Trino's above-scan eval and DuckDB-side eval agree
        // byte-for-byte. Property on → pushes.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("to_unixtime"), DOUBLE, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(0.0, DOUBLE));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).containsExactly("(trino_to_unixtime(\"tstz\") = 0.0)");
    }

    @Test
    public void testToUnixtimeOnTimestampWithTimeZoneDoesNotPushWhenPropertyOff()
    {
        // Same expression, property explicitly off — to_unixtime falls back to
        // the chunk-1 strict gate (DATE or TIMESTAMP no-TZ only). Predicate
        // stays unpushed; Trino evaluates above the scan.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("to_unixtime"), DOUBLE, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(0.0, DOUBLE));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(false));
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testYearOnTimestampWithTimeZonePushesWhenPropertyOn()
    {
        // Chunk 3.5: zone-dependent extracts over WTZ are now safe to push when
        // the property is on. The chunk-3.5 converter fix constructs incoming
        // WTZ values with the Arrow schema TZ (= session zone), so Trino's
        // above-scan year() and DuckDB-side year() interpret the same instant in
        // the same zone — results agree.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("year"), BIGINT, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(2024L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).containsExactly("(trino_year(\"tstz\") = 2024)");
    }

    @Test
    public void testYearOnTimestampWithTimeZoneStillBlockedWhenPropertyOff()
    {
        // Property off is the default and the safe baseline — never push WTZ.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("year"), BIGINT, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(2024L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(false));
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testHourOnTimestampWithTimeZonePushesWhenPropertyOn()
    {
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("hour"), BIGINT, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(22L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).containsExactly("(trino_hour(\"tstz\") = 22)");
    }

    @Test
    public void testDateTruncOnTimestampWithTimeZonePushesWhenPropertyOn()
    {
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("date_trunc"), TIMESTAMP_TZ_MILLIS,
                        varcharConst("month"),
                        new Variable("tstz", TIMESTAMP_TZ_MILLIS)));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).containsExactly("(trino_date_trunc('month', \"tstz\") IS NULL)");
    }

    @Test
    public void testDateDiffWithBothTimestampWithTimeZonePushesWhenPropertyOn()
    {
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("date_diff"), BIGINT,
                        varcharConst("day"),
                        new Variable("tstz", TIMESTAMP_TZ_MILLIS),
                        new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(0L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).containsExactly(
                "(trino_date_diff('day', \"tstz\", \"tstz\") = 0)");
    }

    @Test
    public void testDateDiffMixedDateAndTimestampWithTimeZonePushesWhenPropertyOn()
    {
        // Mixed-kind args: DATE + TIMESTAMP WTZ. Each arg clears its own per-arg
        // gate independently; the connector trusts both engines to handle the
        // type promotion identically.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("date_diff"), BIGINT,
                        varcharConst("day"),
                        new Variable("d", DATE),
                        new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(0L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).containsExactly(
                "(trino_date_diff('day', \"d\", \"tstz\") = 0)");
    }

    @Test
    public void testDayOfWeekOnTimestampWithTimeZoneNeverPushes()
    {
        // Tier A (DATE-only) — Trino's day_of_week doesn't accept WTZ inputs, so
        // even with the Tier C property on, the gate refuses. This pins that the
        // Tier A entries don't accidentally get widened.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("day_of_week"), BIGINT, new Variable("tstz", TIMESTAMP_TZ_MILLIS)),
                new Constant(7L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testFromUnixtimePushesUnconditionally()
    {
        // from_unixtime(DOUBLE) → WTZ. Input is DOUBLE — no gate, no Tier C
        // property needed. The output WTZ value will be tagged with the session
        // zone by the chunk-3.5 converter when read back.
        DucklakeColumnHandle epochCol = new DucklakeColumnHandle(106L, "epoch", DOUBLE, true);
        Map<String, ColumnHandle> assignments = ImmutableMap.of("epoch", epochCol);
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("from_unixtime"), TIMESTAMP_TZ_MILLIS,
                        new Variable("epoch", DOUBLE)));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, assignments);
        // Property off — still pushes (no gate).
        assertThat(conjuncts).containsExactly("(trino_from_unixtime(\"epoch\") IS NULL)");
    }

    @Test
    public void testWithTimezoneOnTimestampPushes()
    {
        // with_timezone(TIMESTAMP no-TZ, varchar) → WTZ. Gated to TIMESTAMP-only.
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("with_timezone"), TIMESTAMP_TZ_MILLIS,
                        new Variable("ts", TIMESTAMP_MILLIS),
                        varcharConst("America/Los_Angeles")));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly(
                "(trino_with_timezone(\"ts\", 'America/Los_Angeles') IS NULL)");
    }

    @Test
    public void testWithTimezoneOnDateDoesNotPush()
    {
        // Trino's with_timezone signature is (timestamp(p), varchar) — DATE input
        // wouldn't match in Trino's planner either, but the gate refuses defensively.
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("with_timezone"), TIMESTAMP_TZ_MILLIS,
                        new Variable("d", DATE),
                        varcharConst("America/Los_Angeles")));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testWithTimezoneOnTimestampWithTimeZoneDoesNotPush()
    {
        // Re-zoning a WTZ value is `at_timezone` in Trino, not `with_timezone`.
        // `at_timezone` stays unpushable through this connector because DuckDB's
        // AT TIME ZONE on a TIMESTAMPTZ returns TIMESTAMP no-TZ — type mismatch.
        // with_timezone gated strictly to TIMESTAMP no-TZ input.
        ConnectorExpression expression = call(StandardFunctions.IS_NULL_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("with_timezone"), TIMESTAMP_TZ_MILLIS,
                        new Variable("tstz", TIMESTAMP_TZ_MILLIS),
                        varcharConst("America/Los_Angeles")));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testYearOnDateStillPushesWhenPropertyOn()
    {
        // Regression guard: turning the property on must NOT regress the Tier B
        // shape. DATE input still pushes regardless of property value.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("year"), BIGINT, new Variable("d", DATE)),
                new Constant(2024L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(
                expression, ASSIGNMENTS, sessionWithTierC(true));
        assertThat(conjuncts).containsExactly("(trino_year(\"d\") = 2024)");
    }

    @Test
    public void testLengthOnVarcharStillPushesAfterTypeGateLanded()
    {
        // Regression guard: the sparse TYPE_GATES registry must NOT affect entries
        // that have no gate. length/1 has no gate → must still push for any arg
        // type (in practice always VARCHAR-typed at the call site, but the
        // translator must not gain a new "ungated entries are now restricted" bug).
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("length"), BIGINT, new Variable("name", VARCHAR)),
                new Constant(5L, BIGINT));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_length(\"name\") = 5)");
    }

    @Test
    public void testTrinoMetaJavaSetContainsRepresentativeEntries()
    {
        // Tripwire: the Java-side PUSHABLE_FUNCTIONS must match the trino_meta() rows
        // in trino-function-aliases.sql. The full parity-with-DuckDB check is in
        // TestTrinoFunctionAliases#testJavaPushableSetMatchesDuckDbMeta — this is a
        // cheaper smoke check that a few well-known entries are present.
        // lower/upper/reverse are PUSHABLE PLACEHOLDERS: pushed for perf with warn-on-emit.
        assertThat(DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS)
                .contains(
                        new DuckDbExpressionTranslator.NameArity("length", 1),
                        new DuckDbExpressionTranslator.NameArity("substring", 2),
                        new DuckDbExpressionTranslator.NameArity("substring", 3),
                        new DuckDbExpressionTranslator.NameArity("starts_with", 2),
                        new DuckDbExpressionTranslator.NameArity("abs", 1),
                        new DuckDbExpressionTranslator.NameArity("power", 2),
                        new DuckDbExpressionTranslator.NameArity("regexp_like", 2),
                        new DuckDbExpressionTranslator.NameArity("lower", 1),
                        new DuckDbExpressionTranslator.NameArity("upper", 1),
                        new DuckDbExpressionTranslator.NameArity("reverse", 1),
                        new DuckDbExpressionTranslator.NameArity("chr", 1),
                        new DuckDbExpressionTranslator.NameArity("levenshtein_distance", 2));
    }

    // --- Helpers ------------------------------------------------------------

    private static ConnectorExpression call(FunctionName name, io.trino.spi.type.Type returnType, ConnectorExpression... args)
    {
        return new Call(returnType, name, List.of(args));
    }

    private static ConnectorExpression varcharConst(String s)
    {
        return new Constant(Slices.utf8Slice(s), VARCHAR);
    }

    @SuppressWarnings("unused")
    private static ConnectorExpression boolConst(boolean b)
    {
        return new Constant(b, BooleanType.BOOLEAN);
    }

    private static ConnectorExpression likeConst(String pattern, Optional<Character> escape)
    {
        // Trino delivers LIKE patterns as a Constant of LikePatternType whose value
        // is a compiled io.trino.type.LikePattern. The translator only reads the
        // pattern string and optional escape character — RE2 matcher state is
        // irrelevant for pushdown emission.
        return new Constant(LikePattern.compile(pattern, escape), LikePatternType.LIKE_PATTERN);
    }
}
