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
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BooleanType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
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
    private static final Map<String, ColumnHandle> ASSIGNMENTS = ImmutableMap.of(
            "name", NAME_COLUMN,
            "id", ID_COLUMN);

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
}
