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
import io.trino.spi.connector.ConnectorSession;
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
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
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
            //   lower/1, upper/1, reverse/1 used to be macro placeholders that diverged
            //   from Trino on non-ASCII input. They are now native C++ scalar functions
            //   in the trino_parity extension (ICU-backed full case folding + code-point
            //   reverse), fully aligned with Trino's semantics. They stay in PUSHABLE_FUNCTIONS
            //   because they are still pushable — just implemented by the extension now.
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
            // Native trino_normalize/1 (NFC). The 2-arg form is NOT pushable —
            // the extension's vendored ICU only bakes in NFC data; Trino
            // evaluates normalize(s, form) above the scan for NFD/NFKC/NFKD.
            new NameArity("normalize", 1),
            new NameArity("pi", 0),
            new NameArity("bitwise_xor", 2),
            new NameArity("regexp_replace", 2),
            new NameArity("regexp_replace", 3),
            // Round 6g — bitwise function-form
            new NameArity("bitwise_and", 2),
            new NameArity("bitwise_or", 2),
            new NameArity("bitwise_not", 1),
            new NameArity("bitwise_left_shift", 2),
            new NameArity("bitwise_right_shift", 2),
            // Round 6g — date convenience
            new NameArity("year", 1),
            new NameArity("month", 1),
            new NameArity("day", 1),
            new NameArity("quarter", 1),
            // Round 6i — conditional + date arithmetic
            new NameArity("if", 2),
            new NameArity("if", 3),
            new NameArity("date_trunc", 2),
            new NameArity("date_diff", 3),
            // Round 6j (step 4 chunk 1) — Tier A: DATE-only date functions. Type gate
            // restricts the arg to DATE; non-DATE inputs fall through to "don't push"
            // (see TYPE_GATES below). All seven map to DuckDB's ISO-aligned functions
            // (isodow/isoweek/isoyear/...) so the Trino semantics carry through.
            new NameArity("day_of_week", 1),
            new NameArity("day_of_year", 1),
            new NameArity("last_day_of_month", 1),
            new NameArity("week", 1),
            new NameArity("week_of_year", 1),
            new NameArity("year_of_week", 1),
            new NameArity("yow", 1),
            // Round 6j — Tier B: DATE or TIMESTAMP (no TZ). Wall-clock components are
            // TZ-invariant in both engines, so these are safe without session-TZ
            // plumbing. TIMESTAMP WITH TIME ZONE inputs are excluded by the type gate;
            // they'll join in chunk 3 behind a session-property flag.
            new NameArity("hour", 1),
            new NameArity("minute", 1),
            new NameArity("second", 1),
            new NameArity("millisecond", 1),
            new NameArity("to_unixtime", 1),
            // Step 4 chunk 4 — Tier C extras.
            // `from_unixtime(double)` is input-type-trivial (DOUBLE always pushable)
            // and produces a WTZ output; chunk-3.5's converter handles the WTZ
            // construction. Safe regardless of session property.
            // `with_timezone(timestamp, varchar)` attaches a zone to a wall-clock;
            // input is TIMESTAMP no-TZ in both engines, no Tier C gate needed.
            new NameArity("from_unixtime", 1),
            new NameArity("with_timezone", 2));

    /**
     * Sparse map of per-entry argument-type gates. {@link #PUSHABLE_FUNCTIONS}
     * remains the binary "is this (name, arity) pushable at all" set; this
     * registry adds finer-grained "and only when the argument types are these"
     * conditions on the subset of entries that need it. Entries without a row
     * here implicitly accept any argument types (preserving the behaviour of
     * the string/numeric/regex catalog that shipped without type gating).
     *
     * <p>Originally added in step 4 chunk 1 for the date/time tier rollout —
     * date functions must reject {@code TIMESTAMP WITH TIME ZONE} arguments
     * until the session-TZ plumbing (chunk 2) lands and Tier C ships (chunk 3).
     * See {@code dev-docs/archive/TODO-pushdown-datetime.md} and
     * {@code dev-docs/archive/PLAN-pushdown-datetime.md}.
     */
    private static final Map<NameArity, ArgTypeGate> TYPE_GATES = buildTypeGates();

    private static Map<NameArity, ArgTypeGate> buildTypeGates()
    {
        Map<NameArity, ArgTypeGate> gates = new java.util.HashMap<>();
        // Tier B always accepted (DATE or TIMESTAMP no-TZ); Tier C (TIMESTAMP WITH
        // TIME ZONE) conditionally accepted when the session sets
        // `pushdown_timestamp_with_timezone` = true. Chunk 2 set DuckDB's session
        // TimeZone to match Trino's on attach; chunk 3.5 fixed the connector's
        // Arrow→Page converter to construct WTZ values with the Arrow schema TZ
        // (which IS the session zone). So Trino's above-scan year() and DuckDB's
        // pushed year() now both interpret the value in Trino's session zone —
        // results agree, pushdown is byte-equivalent. See
        // dev-docs/archive/REPORT-datetime-tz-handling.md and
        // dev-docs/archive/TODO-pushdown-datetime.md "Chunk 3.5 — shipped notes".
        ArgTypeGate arg0Tier = argTier(0);
        for (String name : List.of("year", "month", "day", "quarter",
                "hour", "minute", "second", "millisecond", "to_unixtime")) {
            gates.put(new NameArity(name, 1), arg0Tier);
        }
        // date_trunc(unit, x): gate the second arg.
        gates.put(new NameArity("date_trunc", 2), argTier(1));
        // date_diff(unit, t1, t2): both date-shape args must clear the same gate.
        gates.put(new NameArity("date_diff", 3), (args, session) -> {
            ArgTypeGate inner = argTier(0);
            return inner.accepts(List.of(args.get(1)), session)
                    && inner.accepts(List.of(args.get(2)), session);
        });

        // Tier A — DATE-only, no WTZ extension. Trino's `day_of_week`, `week`,
        // `year_of_week` etc. don't accept WTZ inputs either, so the strict
        // DATE-only gate matches Trino's own signature.
        ArgTypeGate arg0DateStrict = arg(0, DateType.class);
        for (String name : List.of("day_of_week", "day_of_year", "last_day_of_month",
                "week", "week_of_year", "year_of_week", "yow")) {
            gates.put(new NameArity(name, 1), arg0DateStrict);
        }

        // with_timezone(TIMESTAMP no-TZ, varchar) → WTZ. Trino's signature excludes
        // WTZ as the first arg (re-zoning a WTZ value is `at_timezone`, which we
        // can't push — see SQL macros for the reason). Gate strictly to TIMESTAMP.
        gates.put(new NameArity("with_timezone", 2), arg(0, TimestampType.class));
        return Map.copyOf(gates);
    }

    /**
     * Sparse-registry helper: matches when the argument at {@code index} has a
     * runtime {@link Type} that is an instance of one of {@code allowed}. List
     * sizing is the caller's responsibility — entries with arity gates already
     * gate by the {@link NameArity} key, so this only fires when the right number
     * of args is present. Ignores the session.
     */
    private static ArgTypeGate arg(int index, Class<?>... allowed)
    {
        return (args, session) -> {
            if (index >= args.size()) {
                return false;
            }
            Type t = args.get(index).getType();
            for (Class<?> cls : allowed) {
                if (cls.isInstance(t)) {
                    return true;
                }
            }
            return false;
        };
    }

    /**
     * Gate for entries whose Tier B shape is {@code DATE | TIMESTAMP no-TZ} and
     * whose Tier C extension to {@code TIMESTAMP WITH TIME ZONE} is conditional
     * on the {@code pushdown_timestamp_with_timezone} session property.
     */
    private static ArgTypeGate argTier(int index)
    {
        return (args, session) -> {
            if (index >= args.size()) {
                return false;
            }
            Type t = args.get(index).getType();
            if (t instanceof DateType || t instanceof TimestampType) {
                return true;
            }
            if (t instanceof TimestampWithTimeZoneType
                    && DucklakeSessionProperties.isPushdownTimestampWithTimeZone(session)) {
                return true;
            }
            return false;
        };
    }

    @FunctionalInterface
    interface ArgTypeGate
    {
        /**
         * @param args     the actual {@link ConnectorExpression} arguments
         *                 of the call being considered for pushdown
         * @param session  the connector session; may be {@code null} for the
         *                 test overload of {@code translateConjuncts}. Gates
         *                 that consult session properties must tolerate
         *                 {@code null} as "default-off semantics" via
         *                 {@link DucklakeSessionProperties} helpers.
         */
        boolean accepts(List<ConnectorExpression> args, ConnectorSession session);
    }

    private DuckDbExpressionTranslator() {}

    /**
     * Decompose {@code expression} into top-level AND-conjuncts and translate
     * each independently. Returns the SQL fragments for conjuncts the
     * translator could handle; the rest are silently dropped (the caller is
     * expected to leave them in {@code remainingExpression}).
     *
     * <p>The {@code session}-less overload reads as "no session properties
     * available" — Tier C and any other session-property-gated entry stays
     * unpushed. Convenient for unit tests that synthesize a single call without
     * needing a {@link ConnectorSession}.
     */
    static List<String> translateConjuncts(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        return translateConjuncts(expression, assignments, null);
    }

    static List<String> translateConjuncts(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            ConnectorSession session)
    {
        List<String> out = new ArrayList<>();
        for (ConnectorExpression conjunct : conjuncts(expression)) {
            if (isTautologyTrue(conjunct)) {
                // `Constraint.alwaysTrue()` and any `WHERE TRUE` reduce to a Constant(TRUE)
                // conjunct that adds no information. Pushing it would just clutter the
                // WHERE clause and cause applyFilter to report progress when there is none.
                continue;
            }
            Optional<String> translated = translate(conjunct, assignments, session);
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
        return translate(expression, assignments, null);
    }

    static Optional<String> translate(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            ConnectorSession session)
    {
        try {
            return Optional.ofNullable(translateOrNull(expression, assignments, session));
        }
        catch (RuntimeException ignored) {
            // Defensive: any unexpected RuntimeException from a sub-translator => fail safe.
            return Optional.empty();
        }
    }

    private static String translateOrNull(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            ConnectorSession session)
    {
        return switch (expression) {
            case Variable variable -> translateVariable(variable, assignments);
            case Constant constant -> translateConstant(constant);
            case Call call -> translateCall(call, assignments, session);
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

    private static String translateCall(Call call, Map<String, ColumnHandle> assignments, ConnectorSession session)
    {
        FunctionName name = call.getFunctionName();
        List<ConnectorExpression> args = call.getArguments();

        // Standard operators: emit infix / prefix SQL.
        if (name.equals(StandardFunctions.AND_FUNCTION_NAME)) {
            return joinBinary(args, " AND ", assignments, session);
        }
        if (name.equals(StandardFunctions.OR_FUNCTION_NAME)) {
            return joinBinary(args, " OR ", assignments, session);
        }
        if (name.equals(StandardFunctions.NOT_FUNCTION_NAME) && args.size() == 1) {
            String inner = translateOrNull(args.get(0), assignments, session);
            return inner == null ? null : "(NOT " + inner + ")";
        }
        if (name.equals(StandardFunctions.IS_NULL_FUNCTION_NAME) && args.size() == 1) {
            String inner = translateOrNull(args.get(0), assignments, session);
            return inner == null ? null : "(" + inner + " IS NULL)";
        }
        if (name.equals(StandardFunctions.LIKE_FUNCTION_NAME) && args.size() == 2) {
            return translateLike(args.get(0), args.get(1), assignments, session);
        }
        String operator = comparisonOperator(name);
        if (operator != null && args.size() == 2) {
            String left = translateOrNull(args.get(0), assignments, session);
            String right = translateOrNull(args.get(1), assignments, session);
            if (left == null || right == null) {
                return null;
            }
            return "(" + left + " " + operator + " " + right + ")";
        }
        String arithmetic = arithmeticOperator(name);
        if (arithmetic != null && args.size() == 2) {
            String left = translateOrNull(args.get(0), assignments, session);
            String right = translateOrNull(args.get(1), assignments, session);
            if (left == null || right == null) {
                return null;
            }
            return "(" + left + " " + arithmetic + " " + right + ")";
        }
        if (name.equals(StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME) && args.size() == 2) {
            // SQL "IS NOT DISTINCT FROM" — NULL-safe equality. DuckDB supports the
            // grammar directly. Useful for predicates over nullable columns.
            String left = translateOrNull(args.get(0), assignments, session);
            String right = translateOrNull(args.get(1), assignments, session);
            if (left == null || right == null) {
                return null;
            }
            return "(" + left + " IS NOT DISTINCT FROM " + right + ")";
        }
        if (name.equals(StandardFunctions.COALESCE_FUNCTION_NAME) && !args.isEmpty()) {
            // Variadic — both engines align: returns first non-NULL, or NULL if all NULL.
            StringBuilder sql = new StringBuilder("coalesce(");
            for (int i = 0; i < args.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                String arg = translateOrNull(args.get(i), assignments, session);
                if (arg == null) {
                    return null;
                }
                sql.append(arg);
            }
            sql.append(')');
            return sql.toString();
        }
        if (name.equals(StandardFunctions.NULLIF_FUNCTION_NAME) && args.size() == 2) {
            String left = translateOrNull(args.get(0), assignments, session);
            String right = translateOrNull(args.get(1), assignments, session);
            if (left == null || right == null) {
                return null;
            }
            return "nullif(" + left + ", " + right + ")";
        }
        if (name.equals(StandardFunctions.NEGATE_FUNCTION_NAME) && args.size() == 1) {
            // Arithmetic unary minus. Trino encodes `-x` as $negate.
            String inner = translateOrNull(args.get(0), assignments, session);
            return inner == null ? null : "(-" + inner + ")";
        }
        if (name.equals(StandardFunctions.CAST_FUNCTION_NAME) && args.size() == 1) {
            return translateCast(call, args.get(0), "CAST", assignments, session);
        }
        if (name.equals(StandardFunctions.TRY_CAST_FUNCTION_NAME) && args.size() == 1) {
            return translateCast(call, args.get(0), "TRY_CAST", assignments, session);
        }

        // String concat is a translator rewrite (NOT a macro): Trino's `concat(a, b, c)`
        // NULL-propagates, DuckDB's built-in `concat` silently skips NULLs. The `||`
        // operator NULL-propagates in BOTH engines (verified by ProbeConcatNullHandling —
        // see archive/REPORT-hash-null-handling.md), so rewriting to `(a || b || c)` emits
        // Trino-aligned semantics without a macro. Gated on VARCHAR return type to
        // avoid Trino's array overload (`concat(array, array)`), which has different
        // NULL semantics and a different operator shape in DuckDB.
        if (name.getCatalogSchema().isEmpty()
                && "concat".equals(name.getName())
                && args.size() >= 2
                && call.getType() instanceof VarcharType) {
            return translateStringConcat(args, assignments, session);
        }

        // Trino built-in functions: catalogSchema empty (per StandardFunctions.FunctionName usage).
        // Only push if (name, arity) is in our brain, AND the optional argument-type
        // gate (TYPE_GATES) accepts the actual call's argument types.
        if (name.getCatalogSchema().isEmpty()) {
            NameArity key = new NameArity(name.getName(), args.size());
            if (PUSHABLE_FUNCTIONS.contains(key)) {
                ArgTypeGate gate = TYPE_GATES.get(key);
                if (gate != null && !gate.accepts(args, session)) {
                    return null;
                }
                return translateMacroCall(name.getName(), args, assignments, session);
            }
        }
        return null;
    }

    private static String translateStringConcat(
            List<ConnectorExpression> args,
            Map<String, ColumnHandle> assignments,
            ConnectorSession session)
    {
        StringBuilder out = new StringBuilder("(");
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                out.append(" || ");
            }
            String inner = translateOrNull(args.get(i), assignments, session);
            if (inner == null) {
                return null;
            }
            out.append(inner);
        }
        out.append(')');
        return out.toString();
    }

    /**
     * Trino delivers LIKE as {@code Call($like, [value, Constant(LikePattern)])}.
     * {@code io.trino.type.LikePattern} lives in {@code trino-main}, not
     * {@code trino-spi}, so we can't import it on the production classpath —
     * accessed reflectively via {@link LikePatternAccessor}. NOT LIKE arrives as
     * {@code Call($not, [Call($like, ...)])} and is handled by the existing
     * {@code $not} branch recursing into us. Wildcards ({@code %}, {@code _}),
     * ESCAPE semantics, and NULL handling are aligned between DuckDB and Trino
     * (see {@code dev-docs/RESEARCH-function-mapping.md}), so a direct emit is
     * lossless. Returns null when value/pattern is not translatable (including
     * NULL pattern, dynamic pattern expression, etc.).
     */
    private static String translateLike(
            ConnectorExpression value,
            ConnectorExpression patternArg,
            Map<String, ColumnHandle> assignments,
            ConnectorSession session)
    {
        if (!(patternArg instanceof Constant constant)) {
            return null;
        }
        Object patternValue = constant.getValue();
        if (patternValue == null) {
            return null;
        }
        LikePatternAccessor.Extracted extracted = LikePatternAccessor.extract(patternValue);
        if (extracted == null) {
            return null;
        }
        String translatedValue = translateOrNull(value, assignments, session);
        if (translatedValue == null) {
            return null;
        }
        StringBuilder out = new StringBuilder("(")
                .append(translatedValue)
                .append(" LIKE '")
                .append(extracted.pattern().replace("'", "''"))
                .append('\'');
        if (extracted.escape() != null) {
            char escape = extracted.escape();
            out.append(" ESCAPE '");
            if (escape == '\'') {
                out.append("''");
            }
            else {
                out.append(escape);
            }
            out.append('\'');
        }
        out.append(')');
        return out.toString();
    }

    private static String translateCast(
            Call call,
            ConnectorExpression operand,
            String castKeyword,
            Map<String, ColumnHandle> assignments,
            ConnectorSession session)
    {
        String targetType = duckdbTypeName(call.getType());
        if (targetType == null) {
            return null;
        }
        String inner = translateOrNull(operand, assignments, session);
        return inner == null ? null : castKeyword + "(" + inner + " AS " + targetType + ")";
    }

    /**
     * Map a Trino {@link Type} to the DuckDB type name to use inside a CAST.
     * Conservative: only primitive numeric / boolean / varchar / date are handled;
     * timestamp precision + decimal scale + nested types are unsupported here so
     * the translator fails the cast cleanly and stays unpushed.
     */
    private static String duckdbTypeName(Type type)
    {
        if (type instanceof BooleanType) return "BOOLEAN";
        if (type instanceof TinyintType) return "TINYINT";
        if (type instanceof SmallintType) return "SMALLINT";
        if (type instanceof IntegerType) return "INTEGER";
        if (type instanceof BigintType) return "BIGINT";
        if (type instanceof DoubleType) return "DOUBLE";
        if (type instanceof VarcharType) return "VARCHAR";
        if (type instanceof DateType) return "DATE";
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

    private static String arithmeticOperator(FunctionName name)
    {
        // Trino's $add/$subtract/$multiply map to identical SQL operators in DuckDB.
        // Both engines align on integer and float arithmetic semantics (including
        // overflow throws for integers, NaN / Inf propagation for floats). Date/
        // interval arithmetic uses different shapes in the two engines — but if a
        // Constant of those types reaches translateConstant() it returns null, so
        // the whole arithmetic translation fails cleanly and stays unpushed. No
        // type-aware gating needed for the three operators below.
        if (name.equals(StandardFunctions.ADD_FUNCTION_NAME)) return "+";
        if (name.equals(StandardFunctions.SUBTRACT_FUNCTION_NAME)) return "-";
        if (name.equals(StandardFunctions.MULTIPLY_FUNCTION_NAME)) return "*";
        // B2: $divide and $modulo are INTENTIONALLY not pushed. Two independent
        // divergences make a bare-operator push silently wrong on the .db read path
        // (TestDucklakeArithmeticPushdownParity exercises both):
        //   1. Integer DIVISION SEMANTICS — Trino's `/` on integers truncates toward
        //      zero (Java-style: 5/2=2). DuckDB's `/` on integers performs TRUE
        //      division and returns a fractional DOUBLE (5/2=2.5). A pushed
        //      `WHERE id / 2 = 2` therefore returns a DIFFERENT row set from
        //      DuckDB than Trino-native would produce. Trino re-evaluates the
        //      predicate above the scan via remainingExpression, BUT it can only
        //      re-evaluate rows DuckDB returned — rows DuckDB stripped at the
        //      source cannot be restored.
        //   2. DIVIDE/MODULO BY ZERO — Trino throws DIVISION_BY_ZERO. DuckDB
        //      silently returns Infinity (div) / NULL (mod) for the offending row,
        //      and `Infinity = N` / `NULL = N` evaluates to UNKNOWN, so DuckDB
        //      strips the row from its output. Trino never sees the row, never
        //      re-evaluates, never throws — the query silently succeeds with a
        //      hidden row instead of failing. The same suppression applies to
        //      float div-by-zero (Trino throws, DuckDB returns NULL).
        // Returning null here routes both operators through Trino-native evaluation
        // above the scan. Cost: lose source-side filtering for divide / modulo
        // predicates on .db files; parquet path was unaffected either way
        // (pushedExpressions is only consumed by DuckDbFilePageSource). The
        // performant alternative — a `trino_divide` / `trino_modulo` parity macro
        // in duckdb-trino-parity-extension that emulates Trino's truncation and
        // throws on zero — is captured for later in PLAN.md / BEFORE-RESUME B2.
        return null;
    }

    private static String translateMacroCall(
            String trinoName,
            List<ConnectorExpression> args,
            Map<String, ColumnHandle> assignments,
            ConnectorSession session)
    {
        StringBuilder sql = new StringBuilder("trino_").append(trinoName).append('(');
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String arg = translateOrNull(args.get(i), assignments, session);
            if (arg == null) {
                return null;
            }
            sql.append(arg);
        }
        sql.append(')');
        return sql.toString();
    }

    private static String joinBinary(
            List<ConnectorExpression> args,
            String separator,
            Map<String, ColumnHandle> assignments,
            ConnectorSession session)
    {
        if (args.isEmpty()) {
            return null;
        }
        StringBuilder out = new StringBuilder("(");
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                out.append(separator);
            }
            String inner = translateOrNull(args.get(i), assignments, session);
            if (inner == null) {
                return null;
            }
            out.append(inner);
        }
        out.append(')');
        return out.toString();
    }

    record NameArity(String name, int arity) {}

    /**
     * Reflective bridge to {@code io.trino.type.LikePattern}. That class lives in
     * {@code trino-main}, not {@code trino-spi}, so it is not importable on the
     * plugin's compile classpath. The runtime instance arrives via the SPI as
     * the value of a {@code Constant} of {@code LikePatternType}; we reflect on
     * the instance's own class to read its pattern string and optional escape
     * character. Methods are cached per class (single class in practice, but
     * defensive against classloader topology in Trino plugin isolation). If the
     * upstream class shape changes, {@link #extract} returns null and the LIKE
     * conjunct stays unpushed.
     */
    private static final class LikePatternAccessor
    {
        private static final java.util.concurrent.ConcurrentHashMap<Class<?>, MethodPair> CACHE =
                new java.util.concurrent.ConcurrentHashMap<>();
        private static final MethodPair MISSING = new MethodPair(null, null);

        static Extracted extract(Object likePattern)
        {
            MethodPair methods = CACHE.computeIfAbsent(likePattern.getClass(), LikePatternAccessor::resolve);
            if (methods.getPattern() == null || methods.getEscape() == null) {
                return null;
            }
            try {
                String pattern = (String) methods.getPattern().invoke(likePattern);
                if (pattern == null) {
                    return null;
                }
                Object escapeOpt = methods.getEscape().invoke(likePattern);
                Character escape = null;
                if (escapeOpt instanceof Optional<?> opt && opt.isPresent()) {
                    Object inner = opt.get();
                    if (inner instanceof Character c) {
                        escape = c;
                    }
                    else {
                        return null;
                    }
                }
                return new Extracted(pattern, escape);
            }
            catch (ReflectiveOperationException ignored) {
                return null;
            }
        }

        private static MethodPair resolve(Class<?> clazz)
        {
            if (!"io.trino.type.LikePattern".equals(clazz.getName())) {
                return MISSING;
            }
            try {
                return new MethodPair(clazz.getMethod("getPattern"), clazz.getMethod("getEscape"));
            }
            catch (NoSuchMethodException ignored) {
                return MISSING;
            }
        }

        private record MethodPair(java.lang.reflect.Method getPattern, java.lang.reflect.Method getEscape) {}

        record Extracted(String pattern, Character escape) {}
    }
}
