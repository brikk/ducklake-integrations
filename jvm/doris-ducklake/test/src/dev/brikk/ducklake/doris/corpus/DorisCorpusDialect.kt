package dev.brikk.ducklake.doris.corpus

import dev.brikk.house.sql.shape.FindingKind
import dev.brikk.house.sql.shape.Severity
import dev.brikk.house.sql.shape.SqlFragment
import dev.brikk.house.sql.shape.certify
import dev.brikk.house.sql.verify.DorisVerifier
import dev.brikk.house.sql.verify.SqlVerifier

/**
 * Transpile-first corpus dialect gate for the DuckLake corpus-replay adapter
 * (TEST-ONLY — the production connector never sees raw SQL; Doris parses it).
 *
 * Replaces the old hand-maintained DuckDB-ism deny-list with brikk-sql's
 * `certify("doris")` (0.3.0): every corpus query is transpiled `duckdb -> doris`
 * and certified in ONE call. `certify` subsumes everything the gate used to
 * assemble by hand — catalog capability (`UNMAPPABLE_FUNCTION`), generator flags
 * (`UNSUPPORTED_TRANSLATION`), `Command`/`Pragma` roots (`RAW_PASSTHROUGH_STATEMENT`),
 * uncertifiable targets (`NO_TARGET_CATALOG`) — plus `SEMANTIC_HAZARD` (probe-
 * verified divergence even where syntax maps).
 *
 * A query is [Run]nable iff it passes a TWO-TIER gate (proven on Trino, 0.3.0)
 * AND none of the explicit residuals below fire (else [Skip], never a failure).
 * `report.ok` alone is NOT sufficient for an exact-output replay:
 *  - Tier 1 (`okAccepting { "unicode" in areas }`): accept ONLY unicode-scoped
 *    REFUSALs (lower()/upper() Turkish-İ / ß case folding) — an ASCII-dominant
 *    corpus never hits them and the verdict stays honestly divergent. Any other
 *    REFUSAL still blocks.
 *  - Tier 2: skip any remaining non-unicode `SEMANTIC_HAZARD` of ANY severity —
 *    certify only WARNs (doesn't block `ok`) when it can't prove an operand type
 *    without schema (e.g. bare-column `DATE + INTERVAL 1 DAY`, which DuckDB
 *    promotes to TIMESTAMP but Doris keeps DATE), and an exact diff can't tolerate
 *    that. This is MORE precise than a token residual — `CAST(x AS TIMESTAMP) +
 *    INTERVAL …` is proven safe and runs.
 * Doris's divergence surface is wider than Trino's (no native `FILTER (WHERE)` /
 * `UNNEST`), so expect more `UNSUPPORTED_TRANSLATION` REFUSALs — correctly blocking.
 *
 * Explicit residuals `certify` can't detect (kept):
 *  - `information_schema` — the catalog CONTENT differs DuckDB-vs-Doris (not a
 *    syntax/function problem, so no finding).
 *  - DuckDB virtual/pseudo columns (`rowid`, `filename`, `file_row_number`,
 *    `file_index`) — valid identifiers, not functions, so no finding; they have
 *    no Doris column.
 *
 * Two corpus-specific rewrites brikk-sql doesn't own are handled here:
 *  - `ORDER BY ALL` (a DuckDB stable-sort idiom brikk-sql mis-parses as a
 *    column named `ALL`): dropped when trailing (the mirror compares sorted
 *    rows), skipped when it governs a `LIMIT` (top-N ordering can't be dropped).
 *  - inline time travel `AT (VERSION => n)`: brikk-sql passes it through
 *    verbatim, so the literal form is rewritten to Doris `FOR VERSION AS OF n`
 *    on the transpiled output; a non-literal `AT (…)` has no Doris equivalent.
 */
object DorisCorpusDialect {

    private const val DUCKDB = "duckdb"
    private const val DORIS = "doris"

    /**
     * Native Doris-grammar verifier, or null when the Doris parser isn't on the
     * test classpath (`DorisVerifier.createOrNull()` reflects into it). Inert
     * today — see build.gradle.kts; the live-FE corpus execution is the grammar
     * check. Constructed once.
     */
    private val VERIFIER: SqlVerifier? = runCatching { DorisVerifier.createOrNull() }.getOrNull()

    /** Gate outcome. */
    sealed interface Gate

    /** Runnable: [dorisSql] is the transpiled Doris query (still alias-qualified). */
    data class Run(val dorisSql: String) : Gate

    /** Engine-skip with a specific reason (never a failure). */
    data class Skip(val reason: String) : Gate

    /**
     * Decide whether [duckdbSql] can be mirrored on Doris and, if so, produce
     * the Doris-dialect SQL to run (alias→catalog rewrite is the caller's job).
     */
    @Suppress("ReturnCount")
    fun gate(duckdbSql: String): Gate {
        val trimmed = duckdbSql.trim().removeSuffix(";").trim()
        val body = stripLeadingCommentsAndWhitespace(trimmed)
        if (!startsWithSelectOrParenSelect(body) && !startsWithWithCte(body)) {
            return Skip("not a read (SELECT/WITH) statement")
        }

        // ORDER BY ALL: DuckDB stable-sort idiom brikk-sql mis-parses as a column
        // named ALL. Drop it when trailing (mirror sorts rows); skip when a LIMIT
        // depends on the ordering (dropping would change which rows survive).
        val orderByAll = ORDER_BY_ALL.find(body)
        val pre = when {
            orderByAll == null -> body
            LIMIT_OR_OFFSET.containsMatchIn(body.substring(orderByAll.range.last + 1)) ->
                return Skip("ORDER BY ALL governs a LIMIT/OFFSET (top-N ordering can't be dropped)")
            else -> body.removeRange(orderByAll.range).trim()
        }

        // information_schema transpiles structurally but the catalog CONTENT
        // differs DuckDB-vs-Doris — a genuine, unfixable divergence.
        if (INFORMATION_SCHEMA.containsMatchIn(pre)) {
            return Skip("information_schema: catalog content differs DuckDB-vs-Doris")
        }
        // DuckDB virtual/pseudo columns (rowid, filename, file_row_number,
        // file_index): valid identifiers that transpile clean but have no Doris
        // column — the transpiler can't flag them (not functions), so an explicit
        // residual, mirroring the old deny-list. (NOT a blanket "Unknown column"
        // classify — that would mask real column-resolution bugs.)
        DUCKDB_VIRTUAL_COLUMN.find(pre)?.let {
            return Skip("DuckDB virtual column '${it.value}' has no Doris equivalent")
        }

        val fragment = try {
            SqlFragment(pre, DUCKDB)
        } catch (e: Exception) {
            return Skip("parse under duckdb failed: ${firstLine(e)}")
        }

        // ONE call covers the whole capability/hazard predicate: UNMAPPABLE_FUNCTION,
        // UNSUPPORTED_TRANSLATION, RAW_PASSTHROUGH_STATEMENT, NO_TARGET_CATALOG,
        // SEMANTIC_HAZARD. desugarPipes=true so pipe fragments auto-desugar. Never
        // throws — the SQL is always produced.
        val report = try {
            fragment.certify(DORIS, desugarPipes = true)
        } catch (e: Exception) {
            return Skip("certify to doris failed: ${firstLine(e)}")
        }

        certifyRefusalReason(report)?.let { return Skip(it) }

        // Inline time travel: brikk-sql passes `AT (VERSION => n)` through verbatim.
        // Rewrite the literal form to Doris `FOR VERSION AS OF n`; a residual
        // `AT (…)` (timestamp / non-literal) has no Doris equivalent.
        val sql = rewriteInlineTimeTravel(report.result.sql)
        if (AT_PAREN.containsMatchIn(sql)) {
            return Skip("non-literal inline time travel has no Doris form")
        }

        // Belt-and-braces (trino agent's endorsed pattern): the transpiled SQL
        // must re-parse under Doris's real grammar. A rejection is a brikk-sql
        // emission bug → skip + report upstream, not a runtime failure. No-op when
        // [VERIFIER] is null (grammar not on the classpath).
        VERIFIER?.verify(sql)?.let { v ->
            if (!v.accepted) {
                return Skip(
                    "transpiled SQL not parseable by Doris grammar: " +
                        v.error?.lineSequence()?.firstOrNull().orEmpty(),
                )
            }
        }
        return Run(sql)
    }

    /**
     * The two-tier certify verdict (proven on the Trino side, 0.3.0). `report.ok`
     * alone is NOT sufficient for an exact-output replay. Returns a skip reason,
     * or null when the query is safe to run.
     *
     * Tier 1 — accept ONLY unicode-scoped REFUSALs (lower()/upper() Turkish-İ / ß
     * case folding, areas contain "unicode"): an ASCII-dominant corpus never hits
     * them and the verdict stays honestly divergent. Any OTHER REFUSAL (typeof,
     * unmappable, unsupported translation, …) still blocks.
     *
     * Tier 2 — skip any remaining non-unicode SEMANTIC_HAZARD of ANY severity:
     * certify only WARNs (doesn't block `ok`) when it can't prove an operand type
     * without schema (e.g. bare-column `DATE + INTERVAL 1 DAY`, which DuckDB
     * promotes to TIMESTAMP but Doris keeps DATE), and an exact diff can't tolerate
     * that. More precise than a token residual — `CAST(x AS TIMESTAMP) + INTERVAL …`
     * is proven safe and runs.
     */
    private fun certifyRefusalReason(report: dev.brikk.house.sql.shape.TranspileReport): String? {
        if (!report.okAccepting { "unicode" in it.areas }) {
            val blocking = report.findings
                .filter { it.severity == Severity.REFUSAL && "unicode" !in it.areas }
                .joinToString("; ") { "${it.kind}(${it.subject}): ${it.detail}" }
            return "certify refused: $blocking"
        }
        return report.findings
            .firstOrNull { it.kind == FindingKind.SEMANTIC_HAZARD && "unicode" !in it.areas }
            ?.let { "semantic hazard (${it.subject}): ${it.detail}" }
    }

    /**
     * Rewrite DuckDB inline time travel with a LITERAL version — `t AT (VERSION
     * => 3)` — into Doris `t FOR VERSION AS OF 3` (DuckLake version == snapshot
     * id, so semantics are identical). Idempotent.
     */
    fun rewriteInlineTimeTravel(sql: String): String =
        AT_VERSION_LITERAL.replace(sql) { m -> "FOR VERSION AS OF ${m.groupValues[1]}" }

    /** Leading `-- …` line comments, `/* … */` block comments, whitespace. */
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
            !body.getOrElse(4) { ' ' }.isJavaIdentifierPart() // "WITH" keyword, not "WITHIN…"

    private fun firstLine(e: Exception): String =
        (e.message ?: e.toString()).lineSequence().firstOrNull().orEmpty()

    private val AT_VERSION_LITERAL =
        Regex("\\bAT\\s*\\(\\s*VERSION\\s*=>\\s*(\\d+)\\s*\\)", RegexOption.IGNORE_CASE)
    private val AT_PAREN = Regex("\\bAT\\s*\\(", RegexOption.IGNORE_CASE)
    private val ORDER_BY_ALL =
        Regex("\\bORDER\\s+BY\\s+ALL\\b(\\s+(ASC|DESC))?", RegexOption.IGNORE_CASE)
    private val LIMIT_OR_OFFSET = Regex("\\b(LIMIT|OFFSET)\\b", RegexOption.IGNORE_CASE)
    private val INFORMATION_SCHEMA = Regex("\\binformation_schema\\b", RegexOption.IGNORE_CASE)
    private val DUCKDB_VIRTUAL_COLUMN =
        Regex("\\b(rowid|filename|file_row_number|file_index)\\b", RegexOption.IGNORE_CASE)
}
