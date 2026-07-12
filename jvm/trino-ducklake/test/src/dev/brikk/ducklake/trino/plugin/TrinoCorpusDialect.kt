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

import dev.brikk.house.sql.shape.SqlFragment
import dev.brikk.house.sql.verify.TrinoVerifier

/**
 * Transpile-first corpus dialect gate for the DuckLake corpus-replay adapter
 * (TEST-ONLY — the production connector never sees raw SQL; Trino parses it).
 *
 * Replaces the former hand-maintained DuckDB-ism token deny-list (`::`, `INTERVAL \d`,
 * `ROWID`, `PRAGMA`, `DUCKDB_`, …) + hand-rolled `executeQuery` rewrites with `brikk-sql`:
 * every corpus query is transpiled `duckdb -> trino` and the gate keys off the transpiler's
 * own signals rather than a token list. A query is [Run]nable iff it transpiles AND none of
 * these fire (else [Skip], an engine-skip — never a failure):
 *  - `unmappableFunctions("trino")` non-empty — functions absent from Trino's function catalog
 *    (`read_parquet`, `duckdb_tables`, `ducklake_snapshots`, …). The engine's own registry, not
 *    a blocklist.
 *  - `isRawPassthroughStatement` — `PRAGMA` / raw Command with no Trino form.
 *  - `unsupportedMessages` non-empty — constructs the transpiler can't translate to Trino and
 *    still emits at WARN. NB Trino keeps native `FILTER (WHERE)` and `UNNEST`, so brikk-sql's
 *    Trino renderer emits FEWER of these than the Doris one — that's expected.
 *  - the transpiled SQL fails to re-parse under Trino's real grammar ([TrinoVerifier]) — catches
 *    brikk-sql emission bugs and turns them into clean skips (report to the brikk-sql agent)
 *    instead of runtime failures. (Doris couldn't do this — no fe-doris grammar on classpath.)
 *  - explicit residuals the transpiler can't self-detect (tuned per engine):
 *    - `information_schema` — catalog CONTENT differs DuckDB-vs-Trino (not a syntax problem).
 *    - DuckDB virtual columns (`rowid`, `filename`, `file_row_number`, `file_index`) — valid
 *      identifiers with no Trino column. (Trino's are `$`-prefixed with different semantics;
 *      mapping `rowid`→`$row_id` is a candidate upgrade, but the values aren't guaranteed equal,
 *      so skip rather than risk a false divergence.)
 *
 * Two corpus-specific rewrites brikk-sql doesn't own are handled here:
 *  - `ORDER BY ALL` (a DuckDB stable-sort idiom brikk-sql mis-parses as a column named `ALL`):
 *    dropped when trailing (the mirror compares sorted rows); skipped when it governs a `LIMIT`
 *    (top-N ordering can't be dropped).
 *  - inline time travel `AT (VERSION => n)`: brikk-sql passes it through verbatim, so the literal
 *    form is rewritten to Trino `FOR VERSION AS OF n` on the transpiled output; a non-literal
 *    `AT (…)` (timestamp / expression) has no 1:1 rewrite here and is skipped.
 *
 * The alias→catalog rewrite stays the caller's job (post-transpile, in [TrinoReplayEngine]) —
 * brikk-sql passes table identifiers through unquoted.
 */
object TrinoCorpusDialect {

    private const val DUCKDB = "duckdb"
    private const val TRINO = "trino"

    private val verifier = TrinoVerifier()

    /** Gate outcome. */
    sealed interface Gate

    /** Runnable: [trinoSql] is the transpiled Trino query (still alias-qualified). */
    data class Run(val trinoSql: String) : Gate

    /** Engine-skip with a specific reason (never a failure). */
    data class Skip(val reason: String) : Gate

    @Suppress("ReturnCount")
    fun gate(duckdbSql: String): Gate {
        // Statement-shape check, ORDER BY ALL handling, and the explicit-residual skips (which the
        // transpiler can't self-detect) are done up front; a non-null Skip short-circuits here.
        val (pre, preSkip) = preprocess(duckdbSql)
        if (preSkip != null) return preSkip
        checkNotNull(pre)

        val fragment = try {
            SqlFragment(pre, DUCKDB)
        } catch (e: Exception) {
            return Skip("parse under duckdb failed: ${firstLine(e)}")
        }

        val unmappable = fragment.unmappableFunctions(TRINO)
        if (unmappable.isNotEmpty()) {
            return Skip("functions with no Trino mapping: $unmappable")
        }

        val result = try {
            fragment.transpileTo(TRINO)
        } catch (e: Exception) {
            return Skip("transpile to trino failed: ${firstLine(e)}")
        }
        if (result.isRawPassthroughStatement) {
            return Skip("raw passthrough statement (${result.rootKind}) — no Trino form")
        }
        if (result.unsupportedMessages.isNotEmpty()) {
            return Skip("unsupported by trino: ${result.unsupportedMessages}")
        }

        val sql = rewriteInlineTimeTravel(result.sql)
        if (AT_PAREN.containsMatchIn(sql)) {
            return Skip("non-literal inline time travel has no Trino form")
        }

        // Extra signal: the transpiled SQL must re-parse under Trino's real grammar. A failure is
        // a brikk-sql emission bug — skip it (and report upstream) rather than fail at execute time.
        val verify = verifier.verify(sql)
        if (!verify.accepted) {
            return Skip("transpiled SQL not parseable by Trino grammar: ${verify.error?.let { firstLineOf(it) }}")
        }
        return Run(sql)
    }

    /**
     * Up-front preprocessing on the ORIGINAL DuckDB SQL: reject non-reads, handle `ORDER BY ALL`
     * (drop when trailing — the mirror sorts rows; skip when it governs a LIMIT), and apply the
     * explicit residual skips. Returns `(pre-body, null)` when ready to parse+transpile, or
     * `(null, Skip)` to short-circuit.
     */
    @Suppress("ReturnCount")
    private fun preprocess(duckdbSql: String): Pair<String?, Skip?> {
        val trimmed = duckdbSql.trim().removeSuffix(";").trim()
        val body = stripLeadingCommentsAndWhitespace(trimmed)
        if (!startsWithSelectOrParenSelect(body) && !startsWithWithCte(body)) {
            return null to Skip("not a read (SELECT/WITH) statement")
        }
        val orderByAll = ORDER_BY_ALL.find(body)
        val pre = when {
            orderByAll == null -> body
            LIMIT_OR_OFFSET.containsMatchIn(body.substring(orderByAll.range.last + 1)) ->
                return null to Skip("ORDER BY ALL governs a LIMIT/OFFSET (top-N ordering can't be dropped)")
            else -> body.removeRange(orderByAll.range).trim()
        }
        explicitResidualSkip(pre)?.let { return null to it }
        return pre to null
    }

    /**
     * Explicit residuals the transpiler can't self-detect — checked on the ORIGINAL DuckDB SQL
     * (pre-transpile). Each is a genuine DuckDB-vs-Trino divergence with a TRUE, specific reason,
     * not a token blocklist:
     *  - `information_schema` — catalog CONTENT differs (not syntax).
     *  - DuckDB virtual columns (`rowid`/`filename`/`file_row_number`/`file_index`) — no Trino column.
     *  - `typeof()` — Trino has it, but returns engine-specific type spellings (`integer` vs
     *    `INTEGER`, `real` vs `FLOAT`, …) so the frozen DuckDB golden diverges.
     *  - unquoted DuckDB INTERVAL literal — date/timestamp+interval arithmetic promotes to TIMESTAMP
     *    in DuckDB but stays DATE in Trino, so the rendered result diverges.
     */
    private fun explicitResidualSkip(pre: String): Skip? = when {
        INFORMATION_SCHEMA.containsMatchIn(pre) ->
            Skip("information_schema: catalog content differs DuckDB-vs-Trino")
        DUCKDB_VIRTUAL_COLUMN.find(pre) != null ->
            Skip("DuckDB virtual column '${DUCKDB_VIRTUAL_COLUMN.find(pre)!!.value}' has no Trino equivalent")
        TYPEOF.containsMatchIn(pre) ->
            Skip("typeof(): engine-specific type-name spelling diverges DuckDB-vs-Trino")
        UNQUOTED_INTERVAL.containsMatchIn(pre) ->
            Skip("unquoted DuckDB INTERVAL literal: date/timestamp arithmetic promotion diverges (DuckDB→TIMESTAMP vs Trino→DATE)")
        else -> null
    }

    /**
     * `t AT (VERSION => 3)` → Trino `t FOR VERSION AS OF 3` (DuckLake version == snapshot id, so
     * semantics are identical). Literal form only; idempotent.
     */
    fun rewriteInlineTimeTravel(sql: String): String =
        AT_VERSION_LITERAL.replace(sql) { m -> "FOR VERSION AS OF ${m.groupValues[1]}" }

    private fun stripLeadingCommentsAndWhitespace(sql: String): String {
        var s = sql
        while (true) {
            val t = s.trimStart()
            s = when {
                t.startsWith("--") -> t.substringAfter('\n', missingDelimiterValue = "")
                t.startsWith("/*") -> t.substringAfter("*/", missingDelimiterValue = "")
                else -> return t
            }
        }
    }

    private fun startsWithSelectOrParenSelect(body: String): Boolean {
        val b = body.trimStart('(', ' ', '\t', '\n', '\r')
        return b.regionMatches(0, "SELECT", 0, "SELECT".length, ignoreCase = true)
    }

    private fun startsWithWithCte(body: String): Boolean =
        body.regionMatches(0, "WITH", 0, "WITH".length, ignoreCase = true) &&
            !body.getOrElse(4) { ' ' }.isJavaIdentifierPart()

    private fun firstLine(e: Exception): String = firstLineOf(e.message ?: e.toString())

    private fun firstLineOf(s: String): String = s.lineSequence().firstOrNull().orEmpty()

    private val AT_VERSION_LITERAL =
        Regex("\\bAT\\s*\\(\\s*VERSION\\s*=>\\s*(\\d+)\\s*\\)", RegexOption.IGNORE_CASE)
    private val AT_PAREN = Regex("\\bAT\\s*\\(", RegexOption.IGNORE_CASE)
    private val ORDER_BY_ALL =
        Regex("\\bORDER\\s+BY\\s+ALL\\b(\\s+(ASC|DESC))?", RegexOption.IGNORE_CASE)
    private val LIMIT_OR_OFFSET = Regex("\\b(LIMIT|OFFSET)\\b", RegexOption.IGNORE_CASE)
    private val INFORMATION_SCHEMA = Regex("\\binformation_schema\\b", RegexOption.IGNORE_CASE)
    private val DUCKDB_VIRTUAL_COLUMN =
        Regex("\\b(rowid|filename|file_row_number|file_index)\\b", RegexOption.IGNORE_CASE)
    private val TYPEOF = Regex("\\btypeof\\s*\\(", RegexOption.IGNORE_CASE)
    private val UNQUOTED_INTERVAL = Regex("\\bINTERVAL\\s+\\d", RegexOption.IGNORE_CASE)
}
