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
    public void testEqualOnLowerFunction()
    {
        // WHERE lower(name) = 'apple'
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("lower"), VARCHAR,
                        new Variable("name", VARCHAR)),
                varcharConst("apple"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly("(trino_lower(\"name\") = 'apple')");
    }

    @Test
    public void testAndOfPushableAndNonPushable()
    {
        // WHERE lower(name) = 'apple' AND someUnknownFunc(name) = 'x'
        ConnectorExpression pushable = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("lower"), VARCHAR,
                        new Variable("name", VARCHAR)),
                varcharConst("apple"));
        ConnectorExpression notPushable = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("trino_secret_function"), VARCHAR,
                        new Variable("name", VARCHAR)),
                varcharConst("x"));
        ConnectorExpression and = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, pushable, notPushable);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(and, ASSIGNMENTS);
        // The pushable conjunct survives; the non-pushable one is silently dropped.
        assertThat(conjuncts).containsExactly("(trino_lower(\"name\") = 'apple')");
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
        // WHERE lower(name) = 'a' AND id < 10
        ConnectorExpression pushable1 = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("lower"), VARCHAR,
                        new Variable("name", VARCHAR)),
                varcharConst("a"));
        ConnectorExpression pushable2 = call(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, BOOLEAN,
                new Variable("id", BIGINT),
                new Constant(10L, BIGINT));
        ConnectorExpression and = call(StandardFunctions.AND_FUNCTION_NAME, BOOLEAN, pushable1, pushable2);

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(and, ASSIGNMENTS);
        assertThat(conjuncts).containsExactly(
                "(trino_lower(\"name\") = 'a')",
                "(\"id\" < 10)");
    }

    @Test
    public void testUnknownFunctionNotPushed()
    {
        // Function with arity matching a real macro but a different name → not pushed.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("regexp_extract"), VARCHAR,
                        new Variable("name", VARCHAR)),
                varcharConst("a"));

        List<String> conjuncts = DuckDbExpressionTranslator.translateConjuncts(expression, ASSIGNMENTS);
        assertThat(conjuncts).isEmpty();
    }

    @Test
    public void testWrongArityNotPushed()
    {
        // lower/2 is not in trino_meta — only lower/1 is.
        ConnectorExpression expression = call(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, BOOLEAN,
                call(new FunctionName("lower"), VARCHAR,
                        new Variable("name", VARCHAR),
                        varcharConst("extra")),
                varcharConst("a"));

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
    public void testTrinoMetaJavaSetMatchesDocumentedRoundOneAndTwo()
    {
        // Tripwire: the Java-side PUSHABLE_FUNCTIONS must match the trino_meta() rows
        // in trino-function-aliases.sql. The full parity-with-DuckDB check is in
        // TestTrinoFunctionAliases#testJavaPushableSetMatchesDuckDbMeta — this is a
        // cheaper smoke check that a few well-known entries are present.
        assertThat(DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS)
                .contains(
                        new DuckDbExpressionTranslator.NameArity("lower", 1),
                        new DuckDbExpressionTranslator.NameArity("substring", 2),
                        new DuckDbExpressionTranslator.NameArity("substring", 3),
                        new DuckDbExpressionTranslator.NameArity("starts_with", 2),
                        new DuckDbExpressionTranslator.NameArity("abs", 1),
                        new DuckDbExpressionTranslator.NameArity("power", 2));
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
