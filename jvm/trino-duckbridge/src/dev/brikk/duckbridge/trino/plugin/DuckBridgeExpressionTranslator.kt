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

import io.airlift.slice.Slice
import io.trino.plugin.jdbc.JdbcColumnHandle
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
import java.util.concurrent.ConcurrentHashMap
import java.lang.reflect.Method

/**
 * Translates a Trino [ConnectorExpression] predicate into DuckDB SQL fragments (`trino_*(...)`
 * calls plus native operators) that the connector pushes into the remote DuckDB WHERE clause via
 * the base-jdbc `convertPredicate` seam.
 *
 * This is the "brain" of function-shape pushdown, ported verbatim (semantics preserved) from the
 * DuckLake connector's `DuckDbExpressionTranslator`. It consults [PUSHABLE_FUNCTIONS] (mirrored
 * from the extension's `trino_meta()` catalog) and only emits `trino_<name>(...)` for `(name,
 * arity)` pairs present there. Anything unrecognized — unknown function, NULL constant,
 * unsupported type — fails the translation for that conjunct so the caller leaves it in the
 * remaining expression for Trino to evaluate above the scan. The translator never throws.
 *
 * Top-level conjuncts (the children of a top-level `$and`) are translated independently so partial
 * pushdown is possible. (base-jdbc additionally splits conjuncts before calling `convertPredicate`,
 * so this decomposition is belt-and-suspenders.)
 *
 * Difference from the DuckLake port: variables resolve against [JdbcColumnHandle] (base-jdbc's
 * remote column handle) instead of DuckLake's own handle, and there is no row-id column concept.
 */
object DuckBridgeExpressionTranslator {
    /**
     * The set of `(trino_name, arg_count)` pairs the extension has functions/macros for, mirrored
     * from `trino_meta()`. Mirrored here so the translator does not need a DuckDB session at plan
     * time. `TestTrinoFunctionAliases.testJavaPushableSetMatchesDuckDbMeta` fails if this set drifts
     * from `SELECT * FROM trino_meta()`.
     */
    val PUSHABLE_FUNCTIONS: Set<NameArity> =
        setOf(
            // Round 1 — string (lower/upper/reverse are native C++ scalars in the extension now)
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
            // Round 6b-core — crypto hashes
            NameArity("md5", 1),
            NameArity("sha1", 1),
            NameArity("sha256", 1),
            NameArity("sha512", 1),
            NameArity("xxhash64", 1),
            NameArity("hmac_sha256", 2),
            // Round 6a — core DuckDB easy wins
            NameArity("sign", 1),
            NameArity("bit_length", 1),
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
            // Round 6j — Tier A: DATE-only date functions
            NameArity("day_of_week", 1),
            NameArity("day_of_year", 1),
            NameArity("last_day_of_month", 1),
            NameArity("week", 1),
            NameArity("week_of_year", 1),
            NameArity("year_of_week", 1),
            NameArity("yow", 1),
            // Round 6j — Tier B: DATE or TIMESTAMP (no TZ)
            NameArity("hour", 1),
            NameArity("minute", 1),
            NameArity("second", 1),
            NameArity("millisecond", 1),
            NameArity("to_unixtime", 1),
            // Tier C extras
            NameArity("from_unixtime", 1),
            NameArity("with_timezone", 2),
        )

    /**
     * Sparse map of per-entry argument-type gates. [PUSHABLE_FUNCTIONS] remains the binary "is this
     * (name, arity) pushable at all" set; this registry adds finer-grained "and only when the
     * argument types are these" conditions. Entries without a row here accept any argument types.
     */
    private val TYPE_GATES: Map<NameArity, ArgTypeGate> = buildTypeGates()

    private fun buildTypeGates(): Map<NameArity, ArgTypeGate> {
        val gates: MutableMap<NameArity, ArgTypeGate> = mutableMapOf()
        // Tier B always accepted (DATE or TIMESTAMP no-TZ); Tier C (TIMESTAMP WITH TIME ZONE)
        // conditionally accepted when the session sets pushdown_timestamp_with_timezone = true.
        val arg0Tier = argTier(0)
        for (name in listOf("year", "month", "day", "quarter", "hour", "minute", "second", "millisecond", "to_unixtime")) {
            gates[NameArity(name, 1)] = arg0Tier
        }
        // date_trunc(unit, x): gate the second arg.
        gates[NameArity("date_trunc", 2)] = argTier(1)
        // date_diff(unit, t1, t2): both date-shape args must clear the same gate.
        gates[NameArity("date_diff", 3)] =
            ArgTypeGate { args, session ->
                val inner = argTier(0)
                inner.accepts(listOf(args[1]), session) && inner.accepts(listOf(args[2]), session)
            }
        // Tier A — DATE-only.
        val arg0DateStrict = arg(0, DateType::class.java)
        for (name in listOf("day_of_week", "day_of_year", "last_day_of_month", "week", "week_of_year", "year_of_week", "yow")) {
            gates[NameArity(name, 1)] = arg0DateStrict
        }
        // with_timezone(TIMESTAMP no-TZ, varchar) → WTZ. Gate strictly to TIMESTAMP.
        gates[NameArity("with_timezone", 2)] = arg(0, TimestampType::class.java)
        return gates.toMap()
    }

    private fun arg(index: Int, vararg allowed: Class<*>): ArgTypeGate =
        ArgTypeGate { args, _ ->
            if (index >= args.size) {
                false
            } else {
                val t: Type = args[index].type
                allowed.any { it.isInstance(t) }
            }
        }

    private fun argTier(index: Int): ArgTypeGate =
        ArgTypeGate { args, session ->
            if (index >= args.size) {
                false
            } else {
                val t: Type = args[index].type
                when {
                    t is DateType || t is TimestampType -> true
                    t is TimestampWithTimeZoneType &&
                        DuckBridgeSessionProperties.isPushdownTimestampWithTimeZone(session) -> true
                    else -> false
                }
            }
        }

    /**
     * Decompose `expression` into top-level AND-conjuncts and translate each independently.
     * Returns the SQL fragments for conjuncts the translator could handle. The session-less overload
     * reads as "no session properties available" — Tier C and any other session-property-gated entry
     * stays unpushed.
     */
    fun translateConjuncts(expression: ConnectorExpression, assignments: Map<String, ColumnHandle>): List<String> =
        translateConjuncts(expression, assignments, null)

    fun translateConjuncts(
        expression: ConnectorExpression,
        assignments: Map<String, ColumnHandle>,
        session: ConnectorSession?,
    ): List<String> {
        val out: MutableList<String> = mutableListOf()
        for (conjunct in conjuncts(expression)) {
            if (isTautologyTrue(conjunct)) {
                continue
            }
            translate(conjunct, assignments, session)?.let(out::add)
        }
        return out.toList()
    }

    private fun isTautologyTrue(expression: ConnectorExpression): Boolean =
        expression is Constant && expression.type is BooleanType && expression.value == true

    private fun conjuncts(expression: ConnectorExpression): List<ConnectorExpression> {
        if (expression is Call && expression.functionName == StandardFunctions.AND_FUNCTION_NAME) {
            val out: MutableList<ConnectorExpression> = mutableListOf()
            for (child in expression.arguments) {
                out.addAll(conjuncts(child))
            }
            return out
        }
        return listOf(expression)
    }

    /** Translate a single expression to DuckDB SQL. Returns null when any subterm is unrecognised. Never throws. */
    fun translate(expression: ConnectorExpression, assignments: Map<String, ColumnHandle>): String? =
        translate(expression, assignments, null)

    fun translate(
        expression: ConnectorExpression,
        assignments: Map<String, ColumnHandle>,
        session: ConnectorSession?,
    ): String? =
        try {
            translateOrNull(expression, assignments, session)
        } catch (@Suppress("TooGenericExceptionCaught") ignored: RuntimeException) {
            // Defensive: any unexpected RuntimeException from a sub-translator => fail safe.
            null
        }

    private fun translateOrNull(
        expression: ConnectorExpression,
        assignments: Map<String, ColumnHandle>,
        session: ConnectorSession?,
    ): String? =
        when (expression) {
            is Variable -> translateVariable(expression, assignments)
            is Constant -> translateConstant(expression)
            is Call -> translateCall(expression, assignments, session)
            else -> null
        }

    private fun translateVariable(variable: Variable, assignments: Map<String, ColumnHandle>): String? {
        val column = assignments[variable.name]
        if (column !is JdbcColumnHandle) {
            return null
        }
        val escaped = column.columnName.replace("\"", "\"\"")
        return "\"$escaped\""
    }

    @Suppress("CyclomaticComplexMethod") // Faithful port: one branch per SPI constant type; splitting it would obscure the type dispatch.
    private fun translateConstant(constant: Constant): String? {
        val value: Any? = constant.value
        val type: Type = constant.type
        if (value == null) {
            return "NULL"
        }
        if (type is BooleanType) {
            return if (value as Boolean) "TRUE" else "FALSE"
        }
        if (isIntegerFamily(type)) {
            return (value as Long).toString()
        }
        if (type is DoubleType) {
            val d: Double = value as Double
            if (d.isNaN() || d.isInfinite()) {
                return null
            }
            return d.toString()
        }
        if (type is VarcharType) {
            if (value !is Slice) {
                return null
            }
            val s = value.toStringUtf8()
            return "'" + s.replace("'", "''") + "'"
        }
        if (type is DateType) {
            val days = value as Long
            val date = LocalDate.ofEpochDay(days)
            // DuckDB's DATE literal parser rejects the signed/extended forms LocalDate emits for
            // years <1 (BC, '-') or >9999 ('+'); leave such constants unpushed for Trino-side eval.
            if (date.year !in 1..9999) {
                return null
            }
            return "DATE '$date'"
        }
        return null
    }

    // Faithful port of the operator/function dispatch table; each branch encodes a verified semantic
    // edge case (see class doc). Intentionally kept as one dispatch rather than re-derived.
    @Suppress("CyclomaticComplexMethod", "LongMethod")
    private fun translateCall(call: Call, assignments: Map<String, ColumnHandle>, session: ConnectorSession?): String? {
        val name: FunctionName = call.functionName
        val args: List<ConnectorExpression> = call.arguments

        when {
            name == StandardFunctions.AND_FUNCTION_NAME -> return joinBinary(args, " AND ", assignments, session)
            name == StandardFunctions.OR_FUNCTION_NAME -> return joinBinary(args, " OR ", assignments, session)
            name == StandardFunctions.NOT_FUNCTION_NAME && args.size == 1 -> {
                val inner = translateOrNull(args[0], assignments, session)
                return if (inner == null) null else "(NOT $inner)"
            }
            name == StandardFunctions.IS_NULL_FUNCTION_NAME && args.size == 1 -> {
                val inner = translateOrNull(args[0], assignments, session)
                return if (inner == null) null else "($inner IS NULL)"
            }
            name == StandardFunctions.LIKE_FUNCTION_NAME && args.size == 2 ->
                return translateLike(args[0], args[1], assignments, session)
        }

        comparisonOperator(name)?.let { operator ->
            if (args.size == 2) {
                val left = translateOrNull(args[0], assignments, session)
                val right = translateOrNull(args[1], assignments, session)
                return if (left == null || right == null) null else "($left $operator $right)"
            }
        }
        arithmeticOperator(name)?.let { arithmetic ->
            if (args.size == 2) {
                val left = translateOrNull(args[0], assignments, session)
                val right = translateOrNull(args[1], assignments, session)
                return if (left == null || right == null) null else "($left $arithmetic $right)"
            }
        }

        when {
            name == StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME && args.size == 2 -> {
                val left = translateOrNull(args[0], assignments, session)
                val right = translateOrNull(args[1], assignments, session)
                return if (left == null || right == null) null else "($left IS NOT DISTINCT FROM $right)"
            }
            name == StandardFunctions.COALESCE_FUNCTION_NAME && args.isNotEmpty() ->
                return translateVariadic("coalesce", args, assignments, session)
            name == StandardFunctions.NULLIF_FUNCTION_NAME && args.size == 2 -> {
                val left = translateOrNull(args[0], assignments, session)
                val right = translateOrNull(args[1], assignments, session)
                return if (left == null || right == null) null else "nullif($left, $right)"
            }
            name == StandardFunctions.NEGATE_FUNCTION_NAME && args.size == 1 -> {
                val inner = translateOrNull(args[0], assignments, session)
                return if (inner == null) null else "(-$inner)"
            }
            name == StandardFunctions.CAST_FUNCTION_NAME && args.size == 1 ->
                return translateCast(call, args[0], "CAST", assignments, session)
            name == StandardFunctions.TRY_CAST_FUNCTION_NAME && args.size == 1 ->
                return translateCast(call, args[0], "TRY_CAST", assignments, session)
        }

        // String concat is a translator rewrite (NOT a macro): Trino's concat(a,b,c) NULL-propagates,
        // DuckDB's built-in concat silently skips NULLs. The `||` operator NULL-propagates in BOTH
        // engines, so rewrite to (a || b || c). Gated on VARCHAR return type to avoid Trino's array
        // overload (different NULL semantics).
        if (isVarcharConcat(name, args, call)) {
            return translateStringConcat(args, assignments, session)
        }

        // Trino built-in functions: only push if (name, arity) is in our brain AND the optional
        // argument-type gate accepts the actual call's argument types.
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

    private fun isIntegerFamily(type: Type): Boolean =
        type is BigintType || type is IntegerType || type is SmallintType || type is TinyintType

    private fun isVarcharConcat(name: FunctionName, args: List<ConnectorExpression>, call: Call): Boolean =
        name.catalogSchema.isEmpty && "concat" == name.name && args.size >= 2 && call.type is VarcharType

    private fun translateVariadic(
        sqlName: String,
        args: List<ConnectorExpression>,
        assignments: Map<String, ColumnHandle>,
        session: ConnectorSession?,
    ): String? {
        val sql = StringBuilder(sqlName).append('(')
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

    private fun translateStringConcat(
        args: List<ConnectorExpression>,
        assignments: Map<String, ColumnHandle>,
        session: ConnectorSession?,
    ): String? {
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
     * Trino delivers LIKE as `Call($like, [value, Constant(LikePattern)])`. `io.trino.type.LikePattern`
     * lives in `trino-main`, not `trino-spi`, so it isn't on the production classpath — accessed
     * reflectively via [LikePatternAccessor]. NOT LIKE arrives as `Call($not, [Call($like, ...)])` and
     * is handled by the `$not` branch recursing into us. Returns null when value/pattern is not
     * translatable (including NULL pattern, dynamic pattern expression, etc.).
     */
    private fun translateLike(
        value: ConnectorExpression,
        patternArg: ConnectorExpression,
        assignments: Map<String, ColumnHandle>,
        session: ConnectorSession?,
    ): String? {
        if (patternArg !is Constant) {
            return null
        }
        val patternValue: Any = patternArg.value ?: return null
        val extracted = LikePatternAccessor.extract(patternValue) ?: return null
        val translatedValue = translateOrNull(value, assignments, session) ?: return null
        val out =
            StringBuilder("(")
                .append(translatedValue)
                .append(" LIKE '")
                .append(extracted.pattern.replace("'", "''"))
                .append('\'')
        if (extracted.escape != null) {
            val escape: Char = extracted.escape
            out.append(" ESCAPE '")
            if (escape == '\'') {
                out.append("''")
            } else {
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
        session: ConnectorSession?,
    ): String? {
        val targetType = duckdbTypeName(call.type) ?: return null
        val inner = translateOrNull(operand, assignments, session)
        return if (inner == null) null else "$castKeyword($inner AS $targetType)"
    }

    /**
     * Map a Trino [Type] to the DuckDB type name to use inside a CAST. Conservative: only primitive
     * numeric / boolean / varchar / date are handled; timestamp precision + decimal scale + nested
     * types are unsupported so the translator fails the cast cleanly and stays unpushed.
     */
    private fun duckdbTypeName(type: Type): String? =
        when (type) {
            is BooleanType -> "BOOLEAN"
            is TinyintType -> "TINYINT"
            is SmallintType -> "SMALLINT"
            is IntegerType -> "INTEGER"
            is BigintType -> "BIGINT"
            is DoubleType -> "DOUBLE"
            is VarcharType -> "VARCHAR"
            is DateType -> "DATE"
            else -> null
        }

    private fun comparisonOperator(name: FunctionName): String? =
        when (name) {
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME -> "="
            StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME -> "<>"
            StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME -> "<"
            StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME -> "<="
            StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME -> ">"
            StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME -> ">="
            else -> null
        }

    private fun arithmeticOperator(name: FunctionName): String? =
        when (name) {
            // Trino's $add/$subtract/$multiply map to identical SQL operators in DuckDB; both engines
            // align on integer overflow throws and float NaN/Inf propagation.
            StandardFunctions.ADD_FUNCTION_NAME -> "+"
            StandardFunctions.SUBTRACT_FUNCTION_NAME -> "-"
            StandardFunctions.MULTIPLY_FUNCTION_NAME -> "*"
            // $divide and $modulo are INTENTIONALLY not pushed: Trino integer `/` truncates toward
            // zero (5/2=2) but DuckDB does true division (5/2=2.5), and divide/modulo-by-zero throws
            // in Trino but silently yields Infinity/NULL in DuckDB (stripping the row before Trino's
            // above-scan re-eval can throw). A future trino_divide/trino_modulo parity function would
            // let these push safely. See TestDuckBridgeArithmeticPushdownParity.
            else -> null
        }

    private fun translateMacroCall(
        trinoName: String,
        args: List<ConnectorExpression>,
        assignments: Map<String, ColumnHandle>,
        session: ConnectorSession?,
    ): String? {
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
        session: ConnectorSession?,
    ): String? {
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

    fun interface ArgTypeGate {
        fun accepts(args: List<ConnectorExpression>, session: ConnectorSession?): Boolean
    }

    data class NameArity(val name: String, val arity: Int)

    /**
     * Reflective bridge to `io.trino.type.LikePattern` (which lives in `trino-main`, not `trino-spi`,
     * so it isn't importable on the plugin's compile classpath). The runtime instance arrives via the
     * SPI as a `Constant` of `LikePatternType`; we reflect on the instance's own class to read its
     * pattern string and optional escape character. If the upstream class shape changes, [extract]
     * returns null and the LIKE conjunct stays unpushed.
     */
    private object LikePatternAccessor {
        private val CACHE: ConcurrentHashMap<Class<*>, MethodPair> = ConcurrentHashMap()
        private val MISSING: MethodPair = MethodPair(null, null)

        fun extract(likePattern: Any): Extracted? {
            val methods = CACHE.computeIfAbsent(likePattern.javaClass) { resolve(it) }
            if (methods.getPattern == null || methods.getEscape == null) {
                return null
            }
            return try {
                val pattern = methods.getPattern.invoke(likePattern) as String? ?: return null
                val escapeOpt = methods.getEscape.invoke(likePattern)
                var escape: Char? = null
                if (escapeOpt is Optional<*> && escapeOpt.isPresent) {
                    val inner = escapeOpt.get()
                    if (inner is Char) {
                        escape = inner
                    } else {
                        return null
                    }
                }
                Extracted(pattern, escape)
            } catch (@Suppress("SwallowedException") ignored: ReflectiveOperationException) {
                null
            }
        }

        private fun resolve(clazz: Class<*>): MethodPair {
            if ("io.trino.type.LikePattern" != clazz.name) {
                return MISSING
            }
            return try {
                MethodPair(clazz.getMethod("getPattern"), clazz.getMethod("getEscape"))
            } catch (@Suppress("SwallowedException") ignored: NoSuchMethodException) {
                MISSING
            }
        }

        private data class MethodPair(val getPattern: Method?, val getEscape: Method?)

        data class Extracted(val pattern: String, val escape: Char?)
    }
}
