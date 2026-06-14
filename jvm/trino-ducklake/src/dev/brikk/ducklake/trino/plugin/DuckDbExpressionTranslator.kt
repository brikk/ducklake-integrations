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

import io.airlift.log.Logger
import io.airlift.slice.Slice
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.expression.Call
import io.trino.spi.expression.ConnectorExpression
import io.trino.spi.expression.Constant
import io.trino.spi.expression.FunctionName
import io.trino.spi.expression.StandardFunctions
import io.trino.spi.expression.Variable
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.DateType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.SmallintType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType
import java.time.LocalDate
import java.util.Optional

/**
 * Translates a Trino [ConnectorExpression] predicate into DuckDB SQL
 * fragments that the executor AND-s into its WHERE clause.
 *
 * This is the "brain" of pushdown: the translator consults
 * [PUSHABLE_FUNCTIONS] (mirrored from `trino-function-aliases.sql`)
 * and only emits `trino_<name>(...)` for `(name, arity)` pairs
 * present there. Anything unrecognized — unknown function, NULL constant,
 * unsupported type, etc. — fails the translation for that conjunct so the
 * caller can either skip it (don't push) or leave it in `remainingExpression`
 * for Trino to evaluate above the scan. The translator never throws.
 *
 * Top-level conjuncts (the children of a top-level `$and`) are
 * translated independently so partial pushdown is possible: if one conjunct
 * is untranslatable, the others can still be pushed.
 */
// NOTE: Java original was `final class DuckDbExpressionTranslator` (package-private
// utility with private ctor + all-static methods). Kotlin port exposes a public class
// so the JVM name remains `DuckDbExpressionTranslator` (no `internal` mangling),
// matching what Java/Kotlin callers in this module already use.
class DuckDbExpressionTranslator private constructor() {

    companion object {
        private val log: Logger = Logger.get(DuckDbExpressionTranslator::class.java)

        /**
         * The set of `(trino_name, arg_count)` pairs we have macros for in
         * `trino-function-aliases.sql`. Mirrored here so the translator does
         * not need a DuckDB session at plan time. The
         * `TestTrinoFunctionAliases` class has a parity check that fails if
         * this set drifts from `SELECT * FROM trino_meta()`.
         */
        val PUSHABLE_FUNCTIONS: Set<NameArity> = setOf(
                // Round 1 — string
                //   lower/1, upper/1, reverse/1 used to be macro placeholders that diverged
                //   from Trino on non-ASCII input. They are now native C++ scalar functions
                //   in the trino_parity extension (ICU-backed full case folding + code-point
                //   reverse), fully aligned with Trino's semantics. They stay in PUSHABLE_FUNCTIONS
                //   because they are still pushable — just implemented by the extension now.
                NameArity("lower", 1),
                NameArity("upper", 1),
                NameArity("length", 1),
                NameArity("reverse", 1),
                NameArity("trim", 1),
                NameArity("ltrim", 1),
                NameArity("rtrim", 1),
                NameArity("substring", 2),
                NameArity("substring", 3),
                NameArity("replace", 3),
                NameArity("strpos", 2),
                NameArity("starts_with", 2),
                // Round 2 — string
                NameArity("lpad", 3),
                NameArity("rpad", 3),
                NameArity("concat_ws", 2),
                NameArity("concat_ws", 3),
                NameArity("concat_ws", 4),
                NameArity("concat_ws", 5),
                // Round 2 — numeric
                NameArity("abs", 1),
                NameArity("ceil", 1),
                NameArity("floor", 1),
                NameArity("mod", 2),
                NameArity("power", 2),
                // Round 3 — numeric (math)
                NameArity("sqrt", 1),
                NameArity("exp", 1),
                NameArity("ln", 1),
                NameArity("log2", 1),
                NameArity("log10", 1),
                // Round 3 — string
                NameArity("translate", 3),
                // Round 3 — regex (RE2 on both sides)
                NameArity("regexp_like", 2),
                NameArity("regexp_extract", 2),
                NameArity("regexp_extract", 3),
                // Round 4 — encoding / distance / char-from-code
                NameArity("chr", 1),
                NameArity("url_encode", 1),
                NameArity("url_decode", 1),
                NameArity("to_hex", 1),
                NameArity("from_hex", 1),
                NameArity("to_base64", 1),
                NameArity("from_base64", 1),
                NameArity("levenshtein_distance", 2),
                NameArity("hamming_distance", 2),
                // Round 5 — trig / hyperbolic / angle / cube root / truncate
                NameArity("sin", 1),
                NameArity("cos", 1),
                NameArity("tan", 1),
                NameArity("asin", 1),
                NameArity("acos", 1),
                NameArity("atan", 1),
                NameArity("atan2", 2),
                NameArity("sinh", 1),
                NameArity("cosh", 1),
                NameArity("tanh", 1),
                NameArity("degrees", 1),
                NameArity("radians", 1),
                NameArity("cbrt", 1),
                NameArity("truncate", 1),
                // Round 6b-core — crypto hashes (no extension)
                NameArity("md5", 1),
                NameArity("sha1", 1),
                NameArity("sha256", 1),
                // Native hash functions in the trino_parity extension
                // (src/hash_functions.cpp), self-contained over vendored xxHash +
                // WjCryptLib SHA — no community-extension dependency. No type gate:
                // Trino's signatures only accept VARBINARY, so the planner never emits
                // these over another type (same as md5/sha1/sha256 above).
                //   sha512(varbinary)      → SHA-512
                //   xxhash64(varbinary)    → xxHash64, big-endian (matches Trino)
                //   hmac_sha256(data, key) → HMAC-SHA256 over raw bytes. Pushable
                //     because the native function hashes the real VARBINARY bytes; the
                //     earlier crypto_hmac macro route was VARCHAR-only and silently
                //     mangled non-UTF-8 input, so it was rejected.
                NameArity("sha512", 1),
                NameArity("xxhash64", 1),
                NameArity("hmac_sha256", 2),
                // Round 6a — core DuckDB easy wins
                NameArity("sign", 1),
                NameArity("bit_length", 1),
                // Native trino_normalize/1 (NFC). The 2-arg form is NOT pushable —
                // the extension's vendored ICU only bakes in NFC data; Trino
                // evaluates normalize(s, form) above the scan for NFD/NFKC/NFKD.
                NameArity("normalize", 1),
                NameArity("pi", 0),
                NameArity("bitwise_xor", 2),
                NameArity("regexp_replace", 2),
                NameArity("regexp_replace", 3),
                // Round 6g — bitwise function-form
                NameArity("bitwise_and", 2),
                NameArity("bitwise_or", 2),
                NameArity("bitwise_not", 1),
                NameArity("bitwise_left_shift", 2),
                NameArity("bitwise_right_shift", 2),
                // Round 6g — date convenience
                NameArity("year", 1),
                NameArity("month", 1),
                NameArity("day", 1),
                NameArity("quarter", 1),
                // Round 6i — conditional + date arithmetic
                NameArity("if", 2),
                NameArity("if", 3),
                NameArity("date_trunc", 2),
                NameArity("date_diff", 3),
                // Round 6j (step 4 chunk 1) — Tier A: DATE-only date functions. Type gate
                // restricts the arg to DATE; non-DATE inputs fall through to "don't push"
                // (see TYPE_GATES below). All seven map to DuckDB's ISO-aligned functions
                // (isodow/isoweek/isoyear/...) so the Trino semantics carry through.
                NameArity("day_of_week", 1),
                NameArity("day_of_year", 1),
                NameArity("last_day_of_month", 1),
                NameArity("week", 1),
                NameArity("week_of_year", 1),
                NameArity("year_of_week", 1),
                NameArity("yow", 1),
                // Round 6j — Tier B: DATE or TIMESTAMP (no TZ). Wall-clock components are
                // TZ-invariant in both engines, so these are safe without session-TZ
                // plumbing. TIMESTAMP WITH TIME ZONE inputs are excluded by the type gate;
                // they'll join in chunk 3 behind a session-property flag.
                NameArity("hour", 1),
                NameArity("minute", 1),
                NameArity("second", 1),
                NameArity("millisecond", 1),
                NameArity("to_unixtime", 1),
                // Step 4 chunk 4 — Tier C extras.
                // `from_unixtime(double)` is input-type-trivial (DOUBLE always pushable)
                // and produces a WTZ output; chunk-3.5's converter handles the WTZ
                // construction. Safe regardless of session property.
                // `with_timezone(timestamp, varchar)` attaches a zone to a wall-clock;
                // input is TIMESTAMP no-TZ in both engines, no Tier C gate needed.
                NameArity("from_unixtime", 1),
                NameArity("with_timezone", 2))

        /**
         * Sparse map of per-entry argument-type gates. [PUSHABLE_FUNCTIONS]
         * remains the binary "is this (name, arity) pushable at all" set; this
         * registry adds finer-grained "and only when the argument types are these"
         * conditions on the subset of entries that need it. Entries without a row
         * here implicitly accept any argument types (preserving the behaviour of
         * the string/numeric/regex catalog that shipped without type gating).
         *
         * Originally added in step 4 chunk 1 for the date/time tier rollout —
         * date functions must reject `TIMESTAMP WITH TIME ZONE` arguments
         * until the session-TZ plumbing (chunk 2) lands and Tier C ships (chunk 3).
         * See `dev-docs/archive/TODO-pushdown-datetime.md` and
         * `dev-docs/archive/PLAN-pushdown-datetime.md`.
         */
        private val TYPE_GATES: Map<NameArity, ArgTypeGate> = buildTypeGates()

        private fun buildTypeGates(): Map<NameArity, ArgTypeGate> {
            val gates: MutableMap<NameArity, ArgTypeGate> = mutableMapOf()
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
            val arg0Tier = argTier(0)
            for (name in listOf("year", "month", "day", "quarter",
                    "hour", "minute", "second", "millisecond", "to_unixtime")) {
                gates[NameArity(name, 1)] = arg0Tier
            }
            // date_trunc(unit, x): gate the second arg.
            gates[NameArity("date_trunc", 2)] = argTier(1)
            // date_diff(unit, t1, t2): both date-shape args must clear the same gate.
            gates[NameArity("date_diff", 3)] = ArgTypeGate { args, session ->
                val inner = argTier(0)
                inner.accepts(listOf(args[1]), session) &&
                        inner.accepts(listOf(args[2]), session)
            }

            // Tier A — DATE-only, no WTZ extension. Trino's `day_of_week`, `week`,
            // `year_of_week` etc. don't accept WTZ inputs either, so the strict
            // DATE-only gate matches Trino's own signature.
            val arg0DateStrict = arg(0, DateType::class.java)
            for (name in listOf("day_of_week", "day_of_year", "last_day_of_month",
                    "week", "week_of_year", "year_of_week", "yow")) {
                gates[NameArity(name, 1)] = arg0DateStrict
            }

            // with_timezone(TIMESTAMP no-TZ, varchar) → WTZ. Trino's signature excludes
            // WTZ as the first arg (re-zoning a WTZ value is `at_timezone`, which we
            // can't push — see SQL macros for the reason). Gate strictly to TIMESTAMP.
            gates[NameArity("with_timezone", 2)] = arg(0, TimestampType::class.java)
            return gates.toMap()
        }

        /**
         * Sparse-registry helper: matches when the argument at `index` has a
         * runtime [Type] that is an instance of one of `allowed`. List
         * sizing is the caller's responsibility — entries with arity gates already
         * gate by the [NameArity] key, so this only fires when the right number
         * of args is present. Ignores the session.
         */
        private fun arg(index: Int, vararg allowed: Class<*>): ArgTypeGate {
            return ArgTypeGate { args, session ->
                if (index >= args.size) {
                    return@ArgTypeGate false
                }
                val t: Type = args[index].type
                for (cls in allowed) {
                    if (cls.isInstance(t)) {
                        return@ArgTypeGate true
                    }
                }
                false
            }
        }

        /**
         * Gate for entries whose Tier B shape is `DATE | TIMESTAMP no-TZ` and
         * whose Tier C extension to `TIMESTAMP WITH TIME ZONE` is conditional
         * on the `pushdown_timestamp_with_timezone` session property.
         */
        private fun argTier(index: Int): ArgTypeGate {
            return ArgTypeGate { args, session ->
                if (index >= args.size) {
                    return@ArgTypeGate false
                }
                val t: Type = args[index].type
                if (t is DateType || t is TimestampType) {
                    return@ArgTypeGate true
                }
                if (t is TimestampWithTimeZoneType &&
                        DucklakeSessionProperties.isPushdownTimestampWithTimeZone(session)) {
                    return@ArgTypeGate true
                }
                false
            }
        }

        /**
         * Decompose `expression` into top-level AND-conjuncts and translate
         * each independently. Returns the SQL fragments for conjuncts the
         * translator could handle; the rest are silently dropped (the caller is
         * expected to leave them in `remainingExpression`).
         *
         * The `session`-less overload reads as "no session properties
         * available" — Tier C and any other session-property-gated entry stays
         * unpushed. Convenient for unit tests that synthesize a single call without
         * needing a [ConnectorSession].
         */
        fun translateConjuncts(
                expression: ConnectorExpression,
                assignments: Map<String, ColumnHandle>): List<String> =
                translateConjuncts(expression, assignments, null)

        fun translateConjuncts(
                expression: ConnectorExpression,
                assignments: Map<String, ColumnHandle>,
                session: ConnectorSession?): List<String> {
            val out: MutableList<String> = mutableListOf()
            for (conjunct in conjuncts(expression)) {
                if (isTautologyTrue(conjunct)) {
                    // `Constraint.alwaysTrue()` and any `WHERE TRUE` reduce to a Constant(TRUE)
                    // conjunct that adds no information. Pushing it would just clutter the
                    // WHERE clause and cause applyFilter to report progress when there is none.
                    continue
                }
                translate(conjunct, assignments, session)?.let(out::add)
            }
            return out.toList()
        }

        private fun isTautologyTrue(expression: ConnectorExpression): Boolean =
                expression is Constant &&
                    expression.type is BooleanType &&
                    expression.value == true

        private fun conjuncts(expression: ConnectorExpression): List<ConnectorExpression> {
            if (expression is Call &&
                    expression.functionName.equals(StandardFunctions.AND_FUNCTION_NAME)) {
                val out: MutableList<ConnectorExpression> = mutableListOf()
                for (child in expression.arguments) {
                    out.addAll(conjuncts(child))
                }
                return out
            }
            return listOf(expression)
        }

        /**
         * Translate a single expression to DuckDB SQL. Returns [Optional.empty]
         * when any subterm is unrecognised. Never throws.
         */
        fun translate(
                expression: ConnectorExpression,
                assignments: Map<String, ColumnHandle>): String? =
                translate(expression, assignments, null)

        fun translate(
                expression: ConnectorExpression,
                assignments: Map<String, ColumnHandle>,
                session: ConnectorSession?): String? {
            return try {
                translateOrNull(expression, assignments, session)
            }
            catch (ignored: RuntimeException) {
                // Defensive: any unexpected RuntimeException from a sub-translator => fail safe.
                null
            }
        }

        private fun translateOrNull(
                expression: ConnectorExpression,
                assignments: Map<String, ColumnHandle>,
                session: ConnectorSession?): String? {
            return when (expression) {
                is Variable -> translateVariable(expression, assignments)
                is Constant -> translateConstant(expression)
                is Call -> translateCall(expression, assignments, session)
                else -> null
            }
        }

        private fun translateVariable(variable: Variable, assignments: Map<String, ColumnHandle>): String? {
            val column = assignments[variable.name]
            if (column !is DucklakeColumnHandle) {
                return null
            }
            if (column.isRowIdColumn()) {
                return null
            }
            val escaped = column.columnName.replace("\"", "\"\"")
            return "\"$escaped\""
        }

        private fun translateConstant(constant: Constant): String? {
            val value: Any? = constant.value
            val type: Type = constant.type
            if (value == null) {
                return "NULL"
            }
            if (type is BooleanType) {
                return if (value as Boolean) "TRUE" else "FALSE"
            }
            if (type is BigintType ||
                    type is IntegerType ||
                    type is SmallintType ||
                    type is TinyintType) {
                // All represented as long on the stack.
                return (value as Long).toString()
            }
            if (type is DoubleType) {
                val d: Double = (value as Double)
                if (d.isNaN() || d.isInfinite()) {
                    return null
                }
                return d.toString()
            }
            if (type is VarcharType) {
                // VARCHAR is stack-represented as a Slice.
                if (value !is Slice) {
                    return null
                }
                val s = value.toStringUtf8()
                return "'" + s.replace("'", "''") + "'"
            }
            if (type is DateType) {
                // DATE is stack-represented as days-since-epoch.
                val days = (value as Long)
                val date = LocalDate.ofEpochDay(days)
                // LocalDate.toString() renders years <1 (BC) with a '-' and years >9999 with a
                // leading '+' (e.g. "+10000-01-01"), neither of which DuckDB's DATE literal parser
                // accepts — pushing such a fragment would fail the whole query rather than the
                // conjunct cleanly. Leave out-of-range constants unpushed for Trino-side eval.
                if (date.year !in 1..9999) {
                    return null
                }
                return "DATE '$date'"
            }
            return null
        }

        private fun translateCall(call: Call, assignments: Map<String, ColumnHandle>, session: ConnectorSession?): String? {
            val name: FunctionName = call.functionName
            val args: List<ConnectorExpression> = call.arguments

            // Standard operators: emit infix / prefix SQL.
            if (name == StandardFunctions.AND_FUNCTION_NAME) {
                return joinBinary(args, " AND ", assignments, session)
            }
            if (name == StandardFunctions.OR_FUNCTION_NAME) {
                return joinBinary(args, " OR ", assignments, session)
            }
            if (name == StandardFunctions.NOT_FUNCTION_NAME && args.size == 1) {
                val inner = translateOrNull(args[0], assignments, session)
                return if (inner == null) null else "(NOT $inner)"
            }
            if (name == StandardFunctions.IS_NULL_FUNCTION_NAME && args.size == 1) {
                val inner = translateOrNull(args[0], assignments, session)
                return if (inner == null) null else "($inner IS NULL)"
            }
            if (name == StandardFunctions.LIKE_FUNCTION_NAME && args.size == 2) {
                return translateLike(args[0], args[1], assignments, session)
            }
            val operator = comparisonOperator(name)
            if (operator != null && args.size == 2) {
                val left = translateOrNull(args[0], assignments, session)
                val right = translateOrNull(args[1], assignments, session)
                if (left == null || right == null) {
                    return null
                }
                return "($left $operator $right)"
            }
            val arithmetic = arithmeticOperator(name)
            if (arithmetic != null && args.size == 2) {
                val left = translateOrNull(args[0], assignments, session)
                val right = translateOrNull(args[1], assignments, session)
                if (left == null || right == null) {
                    return null
                }
                return "($left $arithmetic $right)"
            }
            if (name == StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME && args.size == 2) {
                // SQL "IS NOT DISTINCT FROM" — NULL-safe equality. DuckDB supports the
                // grammar directly. Useful for predicates over nullable columns.
                val left = translateOrNull(args[0], assignments, session)
                val right = translateOrNull(args[1], assignments, session)
                if (left == null || right == null) {
                    return null
                }
                return "($left IS NOT DISTINCT FROM $right)"
            }
            if (name == StandardFunctions.COALESCE_FUNCTION_NAME && !args.isEmpty()) {
                // Variadic — both engines align: returns first non-NULL, or NULL if all NULL.
                val sql = StringBuilder("coalesce(")
                for (i in args.indices) {
                    if (i > 0) {
                        sql.append(", ")
                    }
                    val arg = translateOrNull(args[i], assignments, session) ?: return null
                    sql.append(arg)
                }
                sql.append(')')
                return sql.toString()
            }
            if (name == StandardFunctions.NULLIF_FUNCTION_NAME && args.size == 2) {
                val left = translateOrNull(args[0], assignments, session)
                val right = translateOrNull(args[1], assignments, session)
                if (left == null || right == null) {
                    return null
                }
                return "nullif($left, $right)"
            }
            if (name == StandardFunctions.NEGATE_FUNCTION_NAME && args.size == 1) {
                // Arithmetic unary minus. Trino encodes `-x` as $negate.
                val inner = translateOrNull(args[0], assignments, session)
                return if (inner == null) null else "(-$inner)"
            }
            if (name == StandardFunctions.CAST_FUNCTION_NAME && args.size == 1) {
                return translateCast(call, args[0], "CAST", assignments, session)
            }
            if (name == StandardFunctions.TRY_CAST_FUNCTION_NAME && args.size == 1) {
                return translateCast(call, args[0], "TRY_CAST", assignments, session)
            }

            // String concat is a translator rewrite (NOT a macro): Trino's `concat(a, b, c)`
            // NULL-propagates, DuckDB's built-in `concat` silently skips NULLs. The `||`
            // operator NULL-propagates in BOTH engines (verified by ProbeConcatNullHandling —
            // see archive/REPORT-hash-null-handling.md), so rewriting to `(a || b || c)` emits
            // Trino-aligned semantics without a macro. Gated on VARCHAR return type to
            // avoid Trino's array overload (`concat(array, array)`), which has different
            // NULL semantics and a different operator shape in DuckDB.
            if (name.catalogSchema.isEmpty &&
                    "concat" == name.name &&
                    args.size >= 2 &&
                    call.type is VarcharType) {
                return translateStringConcat(args, assignments, session)
            }

            // Trino built-in functions: catalogSchema empty (per StandardFunctions.FunctionName usage).
            // Only push if (name, arity) is in our brain, AND the optional argument-type
            // gate (TYPE_GATES) accepts the actual call's argument types.
            if (name.catalogSchema.isEmpty) {
                val key = NameArity(name.name, args.size)
                if (PUSHABLE_FUNCTIONS.contains(key)) {
                    val gate = TYPE_GATES[key]
                    if (gate != null && !gate.accepts(args, session)) {
                        return null
                    }
                    return translateMacroCall(name.name, args, assignments, session)
                }
            }
            return null
        }

        private fun translateStringConcat(
                args: List<ConnectorExpression>,
                assignments: Map<String, ColumnHandle>,
                session: ConnectorSession?): String? {
            val out = StringBuilder("(")
            for (i in args.indices) {
                if (i > 0) {
                    out.append(" || ")
                }
                val inner = translateOrNull(args[i], assignments, session) ?: return null
                out.append(inner)
            }
            out.append(')')
            return out.toString()
        }

        /**
         * Trino delivers LIKE as `Call($like, [value, Constant(LikePattern)])`.
         * `io.trino.type.LikePattern` lives in `trino-main`, not
         * `trino-spi`, so we can't import it on the production classpath —
         * accessed reflectively via [LikePatternAccessor]. NOT LIKE arrives as
         * `Call($not, [Call($like, ...)])` and is handled by the existing
         * `$not` branch recursing into us. Wildcards (`%`, `_`),
         * ESCAPE semantics, and NULL handling are aligned between DuckDB and Trino
         * (see `dev-docs/archive/RESEARCH-function-mapping.md`), so a direct emit is
         * lossless. Returns null when value/pattern is not translatable (including
         * NULL pattern, dynamic pattern expression, etc.).
         */
        private fun translateLike(
                value: ConnectorExpression,
                patternArg: ConnectorExpression,
                assignments: Map<String, ColumnHandle>,
                session: ConnectorSession?): String? {
            if (patternArg !is Constant) {
                return null
            }
            val patternValue: Any = patternArg.value ?: return null
            val extracted = LikePatternAccessor.extract(patternValue) ?: return null
            val translatedValue = translateOrNull(value, assignments, session) ?: return null
            val out = StringBuilder("(")
                    .append(translatedValue)
                    .append(" LIKE '")
                    .append(extracted.pattern.replace("'", "''"))
                    .append('\'')
            if (extracted.escape != null) {
                val escape: Char = extracted.escape
                out.append(" ESCAPE '")
                if (escape == '\'') {
                    out.append("''")
                }
                else {
                    out.append(escape)
                }
                out.append('\'')
            }
            out.append(')')
            return out.toString()
        }

        private fun translateCast(
                call: Call,
                operand: ConnectorExpression,
                castKeyword: String,
                assignments: Map<String, ColumnHandle>,
                session: ConnectorSession?): String? {
            val targetType = duckdbTypeName(call.type) ?: return null
            val inner = translateOrNull(operand, assignments, session)
            return if (inner == null) null else "$castKeyword($inner AS $targetType)"
        }

        /**
         * Map a Trino [Type] to the DuckDB type name to use inside a CAST.
         * Conservative: only primitive numeric / boolean / varchar / date are handled;
         * timestamp precision + decimal scale + nested types are unsupported here so
         * the translator fails the cast cleanly and stays unpushed.
         */
        private fun duckdbTypeName(type: Type): String? {
            if (type is BooleanType) return "BOOLEAN"
            if (type is TinyintType) return "TINYINT"
            if (type is SmallintType) return "SMALLINT"
            if (type is IntegerType) return "INTEGER"
            if (type is BigintType) return "BIGINT"
            if (type is DoubleType) return "DOUBLE"
            if (type is VarcharType) return "VARCHAR"
            if (type is DateType) return "DATE"
            return null
        }

        private fun comparisonOperator(name: FunctionName): String? {
            if (name == StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME) return "="
            if (name == StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME) return "<>"
            if (name == StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME) return "<"
            if (name == StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME) return "<="
            if (name == StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME) return ">"
            if (name == StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME) return ">="
            return null
        }

        private fun arithmeticOperator(name: FunctionName): String? {
            // Trino's $add/$subtract/$multiply map to identical SQL operators in DuckDB.
            // Both engines align on integer and float arithmetic semantics (including
            // overflow throws for integers, NaN / Inf propagation for floats). Date/
            // interval arithmetic uses different shapes in the two engines — but if a
            // Constant of those types reaches translateConstant() it returns null, so
            // the whole arithmetic translation fails cleanly and stays unpushed. No
            // type-aware gating needed for the three operators below.
            if (name == StandardFunctions.ADD_FUNCTION_NAME) return "+"
            if (name == StandardFunctions.SUBTRACT_FUNCTION_NAME) return "-"
            if (name == StandardFunctions.MULTIPLY_FUNCTION_NAME) return "*"
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
            return null
        }

        private fun translateMacroCall(
                trinoName: String,
                args: List<ConnectorExpression>,
                assignments: Map<String, ColumnHandle>,
                session: ConnectorSession?): String? {
            val sql = StringBuilder("trino_").append(trinoName).append('(')
            for (i in args.indices) {
                if (i > 0) {
                    sql.append(", ")
                }
                val arg = translateOrNull(args[i], assignments, session) ?: return null
                sql.append(arg)
            }
            sql.append(')')
            return sql.toString()
        }

        private fun joinBinary(
                args: List<ConnectorExpression>,
                separator: String,
                assignments: Map<String, ColumnHandle>,
                session: ConnectorSession?): String? {
            if (args.isEmpty()) {
                return null
            }
            val out = StringBuilder("(")
            for (i in args.indices) {
                if (i > 0) {
                    out.append(separator)
                }
                val inner = translateOrNull(args[i], assignments, session) ?: return null
                out.append(inner)
            }
            out.append(')')
            return out.toString()
        }
    }

    fun interface ArgTypeGate {
        /**
         * @param args     the actual [ConnectorExpression] arguments
         *                 of the call being considered for pushdown
         * @param session  the connector session; may be `null` for the
         *                 test overload of `translateConjuncts`. Gates
         *                 that consult session properties must tolerate
         *                 `null` as "default-off semantics" via
         *                 [DucklakeSessionProperties] helpers.
         */
        fun accepts(args: List<ConnectorExpression>, session: ConnectorSession?): Boolean
    }

    data class NameArity(val name: String, val arity: Int)

    /**
     * Reflective bridge to `io.trino.type.LikePattern`. That class lives in
     * `trino-main`, not `trino-spi`, so it is not importable on the
     * plugin's compile classpath. The runtime instance arrives via the SPI as
     * the value of a `Constant` of `LikePatternType`; we reflect on
     * the instance's own class to read its pattern string and optional escape
     * character. Methods are cached per class (single class in practice, but
     * defensive against classloader topology in Trino plugin isolation). If the
     * upstream class shape changes, [extract] returns null and the LIKE
     * conjunct stays unpushed.
     */
    private object LikePatternAccessor {
        private val CACHE: java.util.concurrent.ConcurrentHashMap<Class<*>, MethodPair> =
                java.util.concurrent.ConcurrentHashMap()
        private val MISSING: MethodPair = MethodPair(null, null)

        fun extract(likePattern: Any): Extracted? {
            val methods = CACHE.computeIfAbsent(likePattern.javaClass) { resolve(it) }
            if (methods.getPattern == null || methods.getEscape == null) {
                return null
            }
            try {
                val pattern = methods.getPattern.invoke(likePattern) as String? ?: return null
                val escapeOpt = methods.getEscape.invoke(likePattern)
                var escape: Char? = null
                if (escapeOpt is Optional<*> && escapeOpt.isPresent) {
                    val inner = escapeOpt.get()
                    if (inner is Char) {
                        escape = inner
                    }
                    else {
                        return null
                    }
                }
                return Extracted(pattern, escape)
            }
            catch (ignored: ReflectiveOperationException) {
                return null
            }
        }

        private fun resolve(clazz: Class<*>): MethodPair {
            if ("io.trino.type.LikePattern" != clazz.name) {
                return MISSING
            }
            return try {
                MethodPair(clazz.getMethod("getPattern"), clazz.getMethod("getEscape"))
            }
            catch (ignored: NoSuchMethodException) {
                MISSING
            }
        }

        private data class MethodPair(val getPattern: java.lang.reflect.Method?, val getEscape: java.lang.reflect.Method?)

        data class Extracted(val pattern: String, val escape: Char?)
    }
}
