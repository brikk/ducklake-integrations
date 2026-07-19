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

import dev.brikk.house.sql.shape.Finding
import dev.brikk.house.sql.shape.FindingKind
import dev.brikk.house.sql.shape.Severity
import dev.brikk.house.sql.shape.SqlFragment
import dev.brikk.house.sql.shape.certify
import dev.brikk.house.sql.verify.TrinoVerifier

/**
 * Transpile-first corpus dialect gate for the DuckLake corpus-replay adapter
 * (TEST-ONLY — the production connector never sees raw SQL; Trino parses it).
 *
 * Replaces the former hand-maintained DuckDB-ism token deny-list (`::`, `INTERVAL \d`,
 * `ROWID`, `PRAGMA`, `DUCKDB_`, …) + hand-rolled `executeQuery` rewrites with `brikk-sql`'s
 * single `certify()` gate (brikk-sql 0.3.0). `certify("trino")` in ONE call covers what we used
 * to assemble by hand PLUS a new class we couldn't check before:
 *  - UNMAPPABLE_FUNCTION — functions absent from Trino's catalog (`read_parquet`, `duckdb_tables`, …).
 *  - RAW_PASSTHROUGH_STATEMENT — `PRAGMA` / raw Command with no Trino form.
 *  - UNSUPPORTED_TRANSLATION — no faithful Trino rendering (e.g. scalar `UNNEST`/`EXPLODE`). NB
 *    Trino keeps native `FILTER (WHERE)` and `UNNEST`, so this fires less than for Doris.
 *  - NO_TARGET_CATALOG — uncertifiable target.
 *  - SEMANTIC_HAZARD — probe-verified divergence even where syntax maps: `typeof()` (engine-specific
 *    type-name spellings), `lower()`/`upper()` (Turkish-İ / ß Unicode case-folding), and the
 *    DATE+INTERVAL type-promotion hazard (DuckDB promotes DATE±INTERVAL to TIMESTAMP; Trino keeps
 *    DATE) — none of which the old deny-list could reason about.
 *
 * Gate policy — this is an EXACT-OUTPUT replay, so "might diverge" is as bad as "does diverge":
 *  1. [Run]nable iff the transpile is certified AND no non-tolerable hazard remains. We use
 *     `report.okAccepting { it.isUnicodeScoped }` rather than plain `report.ok`: `lower()`/`upper()`
 *     REFUSE with areas `[string, unicode]`, but their ONLY divergence is Turkish-İ / ß case
 *     folding, which our ASCII corpus never exercises — so we accept those and recover the coverage
 *     WITHOUT the library lying about the verdict (`typeof`, areas `[string]`, stays refused).
 *  2. Then skip any remaining non-unicode `SEMANTIC_HAZARD` of ANY severity. brikk-sql 0.3.0 tiers
 *     DATE+INTERVAL: a provably-DATE operand REFUSES (caught by step 1) but a bare column only
 *     WARNS (certify can't prove the column type without schema, so `ok` stays true). Our corpus
 *     DOES have DATE columns (e.g. `add_files_hive_partition_cast`), so we must skip the WARNING
 *     too — this replaces the former `INTERVAL \d` regex residual with a precise, provenance-backed
 *     skip that no longer over-skips `CAST(x AS TIMESTAMP) + INTERVAL …` (certify proves that safe).
 * `certify` never throws and always produces `result.sql`; then the transpiled SQL is re-parsed
 * under Trino's real grammar ([TrinoVerifier], which `certify` does NOT run) as a belt-and-braces
 * signal — a parse failure is a brikk-sql emission bug, skipped + reportable rather than a runtime
 * failure. Finally, explicit residuals `certify` can't self-detect (see [explicitResidualSkip]):
 * `information_schema` and DuckDB virtual columns.
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

        // One call replaces the former unmappable-functions + transpile + raw-passthrough +
        // unsupported-messages assembly, and ADDS probe-verified SEMANTIC_HAZARD detection
        // (typeof() type-name spellings, lower()/upper() case-folding, DATE+INTERVAL promotion)
        // that we couldn't check before. certify never throws and always produces `result.sql`.
        // (SqlFragment construction can still throw on a hard parse failure, so guard the whole thing.)
        val report = try {
            SqlFragment(pre, DUCKDB).certify(TRINO, desugarPipes = true)
        } catch (e: Exception) {
            return Skip("certify duckdb->trino threw: ${firstLine(e)}")
        }
        // Step 1: certified, accepting unicode-only REFUSALs (lower()/upper() — ASCII corpus). This
        // is coverage recovery, NOT a semantics lie: the library's verdict stays "divergent"; we
        // just own the context that our data is ASCII (see gate-policy note above).
        if (!report.okAccepting { it.isUnicodeScoped }) {
            val refusals = report.findings.filter { it.severity == Severity.REFUSAL && !it.isUnicodeScoped }
            return Skip("not trino-certified: " + refusals.joinToString("; ") { "${it.kind}: ${it.detail}" })
        }
        // Step 2: exact-output replay can't tolerate a WARNING-level hazard either. Skip any
        // remaining non-unicode SEMANTIC_HAZARD (e.g. the bare-column DATE+INTERVAL promotion that
        // certify WARNs on because it can't prove the operand type without schema).
        report.findings.firstOrNull { it.kind == FindingKind.SEMANTIC_HAZARD && !it.isUnicodeScoped }?.let {
            return Skip("semantic hazard (${it.subject}): ${it.detail}")
        }

        val sql = rewriteInlineTimeTravel(report.result.sql)
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
     * Explicit residuals `certify()` can't self-detect — checked on the ORIGINAL DuckDB SQL
     * (pre-transpile). Each is a genuine DuckDB-vs-Trino divergence with a TRUE, specific reason:
     *  - `information_schema` — valid syntax, certify says ok=true; but the catalog CONTENT differs
     *    DuckDB-vs-Trino, which the transpiler can't know.
     *  - DuckDB virtual columns (`rowid`/`filename`/`file_row_number`/`file_index`) — valid
     *    identifiers, certify ok=true; no Trino column, so they'd resolve differently.
     *
     * The former unquoted-INTERVAL residual is RETIRED as of brikk-sql 0.3.0: certify now tiers the
     * DATE+INTERVAL type-promotion hazard (provably-DATE → REFUSAL, bare column → WARNING), which
     * the two-tier certify check in [gate] handles precisely (and without over-skipping a proven
     * `CAST(x AS TIMESTAMP) + INTERVAL …`).
     */
    private fun explicitResidualSkip(pre: String): Skip? = when {
        INFORMATION_SCHEMA.containsMatchIn(pre) ->
            Skip("information_schema: catalog content differs DuckDB-vs-Trino")
        DUCKDB_VIRTUAL_COLUMN.find(pre) != null ->
            Skip("DuckDB virtual column '${DUCKDB_VIRTUAL_COLUMN.find(pre)!!.value}' has no Trino equivalent")
        else -> null
    }

    /**
     * A finding whose divergence requires non-ASCII input — its [Finding.areas] include `unicode`
     * (today: `lower()`/`upper()` Turkish-İ / ß case folding). Safe to accept for our ASCII-only
     * corpus. `typeof()` (areas `[string]`) and DATE+INTERVAL (areas `[datetime]`) are NOT unicode-
     * scoped and stay blocking.
     */
    private val Finding.isUnicodeScoped: Boolean get() = UNICODE_AREA in areas

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

    /** `Finding.areas` tag marking a divergence that only manifests on non-ASCII input. */
    private const val UNICODE_AREA = "unicode"
}
