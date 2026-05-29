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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Translates a Trino {@link ConnectorExpression} predicate into DuckDB SQL
 * fragments that the executor AND-s into its WHERE clause.
 *
 * <p>This is the "brain" of pushdown: the translator consults
 * {@link #PUSHABLE_FUNCTIONS} (mirrored from {@code trino-function-aliases.sql})
 * and only emits {@code trino_<name>(...)} for {@code (name, arity)} pairs
 * present there. Anything unrecognized — unknown function, NULL constant,
 * unsupported type, etc. — fails the translation for that conjunct so the
 * caller can either skip it (don't push) or leave it in {@code remainingExpression}
 * for Trino to evaluate above the scan. The translator never throws.
 *
 * <p>Top-level conjuncts (the children of a top-level {@code $and}) are
 * translated independently so partial pushdown is possible: if one conjunct
 * is untranslatable, the others can still be pushed.
 */
final class DuckDbExpressionTranslator
{
    private static final Logger log = Logger.get(DuckDbExpressionTranslator.class);

    /**
     * The set of {@code (trino_name, arg_count)} pairs we have macros for in
     * {@code trino-function-aliases.sql}. Mirrored here so the translator does
     * not need a DuckDB session at plan time. The
     * {@code TestTrinoFunctionAliases} class has a parity check that fails if
     * this set drifts from {@code SELECT * FROM trino_meta()}.
     */
    static final Set<NameArity> PUSHABLE_FUNCTIONS = Set.of(
            // Round 1 — string
            //   lower/1, upper/1, reverse/1 are placeholders: DuckDB's built-in
            //   diverges from Trino on Unicode inputs. Kept here so we can push
            //   them for performance testing; warn-on-emit (see PLACEHOLDER_TRINO_NAMES
            //   below) fires a one-shot WARN per name when the translator emits one.
            //   Native extension is the real fix. REPORT-string-unicode-audit.md
            //   has the divergence catalog.
            new NameArity("lower", 1),
            new NameArity("upper", 1),
            new NameArity("length", 1),
            new NameArity("reverse", 1),
            new NameArity("trim", 1),
            new NameArity("ltrim", 1),
            new NameArity("rtrim", 1),
            new NameArity("substring", 2),
            new NameArity("substring", 3),
            new NameArity("replace", 3),
            new NameArity("strpos", 2),
            new NameArity("starts_with", 2),
            // Round 2 — string
            new NameArity("lpad", 3),
            new NameArity("rpad", 3),
            new NameArity("concat_ws", 2),
            new NameArity("concat_ws", 3),
            new NameArity("concat_ws", 4),
            new NameArity("concat_ws", 5),
            // Round 2 — numeric
            new NameArity("abs", 1),
            new NameArity("ceil", 1),
            new NameArity("floor", 1),
            new NameArity("mod", 2),
            new NameArity("power", 2),
            // Round 3 — numeric (math)
            new NameArity("sqrt", 1),
            new NameArity("exp", 1),
            new NameArity("ln", 1),
            new NameArity("log2", 1),
            new NameArity("log10", 1),
            // Round 3 — string
            new NameArity("translate", 3),
            // Round 3 — regex (RE2 on both sides)
            new NameArity("regexp_like", 2),
            new NameArity("regexp_extract", 2),
            new NameArity("regexp_extract", 3),
            // Round 4 — encoding / distance / char-from-code
            new NameArity("chr", 1),
            new NameArity("url_encode", 1),
            new NameArity("url_decode", 1),
            new NameArity("to_hex", 1),
            new NameArity("from_hex", 1),
            new NameArity("to_base64", 1),
            new NameArity("from_base64", 1),
            new NameArity("levenshtein_distance", 2),
            new NameArity("hamming_distance", 2),
            // Round 5 — trig / hyperbolic / angle / cube root / truncate
            new NameArity("sin", 1),
            new NameArity("cos", 1),
            new NameArity("tan", 1),
            new NameArity("asin", 1),
            new NameArity("acos", 1),
            new NameArity("atan", 1),
            new NameArity("atan2", 2),
            new NameArity("sinh", 1),
            new NameArity("cosh", 1),
            new NameArity("tanh", 1),
            new NameArity("degrees", 1),
            new NameArity("radians", 1),
            new NameArity("cbrt", 1),
            new NameArity("truncate", 1),
            // Round 6b-core — crypto hashes (no extension)
            new NameArity("md5", 1),
            new NameArity("sha1", 1),
            new NameArity("sha256", 1),
            // Round 6a — core DuckDB easy wins
            new NameArity("sign", 1),
            new NameArity("bit_length", 1),
            new NameArity("pi", 0),
            new NameArity("bitwise_xor", 2),
            new NameArity("regexp_replace", 2),
            new NameArity("regexp_replace", 3));

    /**
     * Bare Trino names of placeholder macros (those marked {@code -- @placeholder}
     * in {@code trino-function-aliases.sql}). When the translator emits a SQL
     * fragment for one of these, it logs a one-shot WARN per name so callers see
     * which divergent placeholder fired — useful during perf characterization
     * and as a loud signal for production deployments before the native
     * DuckDB extension lands.
     */
    private static final Set<String> PLACEHOLDER_TRINO_NAMES = derivePlaceholderTrinoNames();

    private static final java.util.concurrent.ConcurrentHashMap<String, Boolean> PLACEHOLDER_EMISSION_WARNED =
            new java.util.concurrent.ConcurrentHashMap<>();

    private static Set<String> derivePlaceholderTrinoNames()
    {
        java.util.Set<String> out = new java.util.HashSet<>();
        for (String macroName : TrinoFunctionAliases.placeholderMacros()) {
            if (macroName.startsWith("trino_")) {
                out.add(macroName.substring("trino_".length()));
            }
        }
        return java.util.Set.copyOf(out);
    }

    private DuckDbExpressionTranslator() {}

    /**
     * Decompose {@code expression} into top-level AND-conjuncts and translate
     * each independently. Returns the SQL fragments for conjuncts the
     * translator could handle; the rest are silently dropped (the caller is
     * expected to leave them in {@code remainingExpression}).
     */
    static List<String> translateConjuncts(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        List<String> out = new ArrayList<>();
        for (ConnectorExpression conjunct : conjuncts(expression)) {
            if (isTautologyTrue(conjunct)) {
                // `Constraint.alwaysTrue()` and any `WHERE TRUE` reduce to a Constant(TRUE)
                // conjunct that adds no information. Pushing it would just clutter the
                // WHERE clause and cause applyFilter to report progress when there is none.
                continue;
            }
            Optional<String> translated = translate(conjunct, assignments);
            translated.ifPresent(out::add);
        }
        return List.copyOf(out);
    }

    private static boolean isTautologyTrue(ConnectorExpression expression)
    {
        return expression instanceof Constant constant
                && constant.getType() instanceof BooleanType
                && Boolean.TRUE.equals(constant.getValue());
    }

    private static List<ConnectorExpression> conjuncts(ConnectorExpression expression)
    {
        if (expression instanceof Call call
                && call.getFunctionName().equals(StandardFunctions.AND_FUNCTION_NAME)) {
            List<ConnectorExpression> out = new ArrayList<>();
            for (ConnectorExpression child : call.getArguments()) {
                out.addAll(conjuncts(child));
            }
            return out;
        }
        return List.of(expression);
    }

    /**
     * Translate a single expression to DuckDB SQL. Returns {@link Optional#empty()}
     * when any subterm is unrecognised. Never throws.
     */
    static Optional<String> translate(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        try {
            return Optional.ofNullable(translateOrNull(expression, assignments));
        }
        catch (RuntimeException ignored) {
            // Defensive: any unexpected RuntimeException from a sub-translator => fail safe.
            return Optional.empty();
        }
    }

    private static String translateOrNull(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return switch (expression) {
            case Variable variable -> translateVariable(variable, assignments);
            case Constant constant -> translateConstant(constant);
            case Call call -> translateCall(call, assignments);
            default -> null;
        };
    }

    private static String translateVariable(Variable variable, Map<String, ColumnHandle> assignments)
    {
        ColumnHandle column = assignments.get(variable.getName());
        if (!(column instanceof DucklakeColumnHandle ducklakeColumn)) {
            return null;
        }
        if (ducklakeColumn.isRowIdColumn()) {
            return null;
        }
        String escaped = ducklakeColumn.columnName().replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    private static String translateConstant(Constant constant)
    {
        Object value = constant.getValue();
        Type type = constant.getType();
        if (value == null) {
            return "NULL";
        }
        if (type instanceof BooleanType) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        if (type instanceof BigintType
                || type instanceof IntegerType
                || type instanceof SmallintType
                || type instanceof TinyintType) {
            // All represented as long on the stack.
            return Long.toString((long) (Long) value);
        }
        if (type instanceof DoubleType) {
            double d = (double) (Double) value;
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                return null;
            }
            return Double.toString(d);
        }
        if (type instanceof VarcharType) {
            // VARCHAR is stack-represented as a Slice.
            if (!(value instanceof Slice slice)) {
                return null;
            }
            String s = slice.toStringUtf8();
            return "'" + s.replace("'", "''") + "'";
        }
        if (type instanceof DateType) {
            // DATE is stack-represented as days-since-epoch.
            long days = (long) (Long) value;
            return "DATE '" + LocalDate.ofEpochDay(days) + "'";
        }
        return null;
    }

    private static String translateCall(Call call, Map<String, ColumnHandle> assignments)
    {
        FunctionName name = call.getFunctionName();
        List<ConnectorExpression> args = call.getArguments();

        // Standard operators: emit infix / prefix SQL.
        if (name.equals(StandardFunctions.AND_FUNCTION_NAME)) {
            return joinBinary(args, " AND ", assignments);
        }
        if (name.equals(StandardFunctions.OR_FUNCTION_NAME)) {
            return joinBinary(args, " OR ", assignments);
        }
        if (name.equals(StandardFunctions.NOT_FUNCTION_NAME) && args.size() == 1) {
            String inner = translateOrNull(args.get(0), assignments);
            return inner == null ? null : "(NOT " + inner + ")";
        }
        if (name.equals(StandardFunctions.IS_NULL_FUNCTION_NAME) && args.size() == 1) {
            String inner = translateOrNull(args.get(0), assignments);
            return inner == null ? null : "(" + inner + " IS NULL)";
        }
        String operator = comparisonOperator(name);
        if (operator != null && args.size() == 2) {
            String left = translateOrNull(args.get(0), assignments);
            String right = translateOrNull(args.get(1), assignments);
            if (left == null || right == null) {
                return null;
            }
            return "(" + left + " " + operator + " " + right + ")";
        }

        // Trino built-in functions: catalogSchema empty (per StandardFunctions.FunctionName usage).
        // Only push if (name, arity) is in our brain.
        if (name.getCatalogSchema().isEmpty()
                && PUSHABLE_FUNCTIONS.contains(new NameArity(name.getName(), args.size()))) {
            return translateMacroCall(name.getName(), args, assignments);
        }
        return null;
    }

    private static String comparisonOperator(FunctionName name)
    {
        if (name.equals(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME)) return "=";
        if (name.equals(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME)) return "<>";
        if (name.equals(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME)) return "<";
        if (name.equals(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) return "<=";
        if (name.equals(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME)) return ">";
        if (name.equals(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) return ">=";
        return null;
    }

    private static String translateMacroCall(
            String trinoName,
            List<ConnectorExpression> args,
            Map<String, ColumnHandle> assignments)
    {
        StringBuilder sql = new StringBuilder("trino_").append(trinoName).append('(');
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String arg = translateOrNull(args.get(i), assignments);
            if (arg == null) {
                return null;
            }
            sql.append(arg);
        }
        sql.append(')');
        warnOnPlaceholderEmissionOnce(trinoName);
        return sql.toString();
    }

    private static void warnOnPlaceholderEmissionOnce(String trinoName)
    {
        if (!PLACEHOLDER_TRINO_NAMES.contains(trinoName)) {
            return;
        }
        if (PLACEHOLDER_EMISSION_WARNED.putIfAbsent(trinoName, Boolean.TRUE) == null) {
            log.warn("Pushdown emitted placeholder macro trino_%s — DuckDB result diverges from "
                            + "Trino on specific non-ASCII inputs; OK for ASCII data, NOT a "
                            + "correctness guarantee for arbitrary inputs. Replace with native "
                            + "extension before production. See dev-docs/REPORT-string-unicode-audit.md.",
                    trinoName);
        }
    }

    private static String joinBinary(
            List<ConnectorExpression> args,
            String separator,
            Map<String, ColumnHandle> assignments)
    {
        if (args.isEmpty()) {
            return null;
        }
        StringBuilder out = new StringBuilder("(");
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                out.append(separator);
            }
            String inner = translateOrNull(args.get(i), assignments);
            if (inner == null) {
                return null;
            }
            out.append(inner);
        }
        out.append(')');
        return out.toString();
    }

    record NameArity(String name, int arity) {}
}
