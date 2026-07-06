package dev.brikk.ducklake.doris.corpus

/**
 * v1 dialect gate for the corpus replay adapter — the `accepts(sql)` seam of
 * `ReplayReadEngine` (branch `ducklake-corpus-test`). Rejected queries count
 * as engine-skips in the runner's report, never failures, so this predicate
 * is deliberately CONSERVATIVE: it admits only plain `SELECT`s free of
 * DuckDB-isms we know Doris cannot parse or renders incomparably. Widening it
 * (and watching the corpus green-count climb) is the read-path bring-up
 * lever — grow the allow surface deliberately, one construct at a time, with
 * live-cluster evidence.
 *
 * Rejection tiers:
 *  1. Non-SELECT statements (the oracle owns all DDL/DML/directives; we only
 *     mirror reads).
 *  2. DuckDB-only syntax Doris's parser refuses (`::` casts, `//`-comments
 *     are fine but `$$`-strings, lambdas, `LIST[…]` literals are not…).
 *  3. Constructs that parse but read DuckDB-internal state (catalog
 *     functions, pragmas, table functions over files) — meaningless against
 *     the FE.
 *  4. Function families our type surface degrades (TIME/INTERVAL returning,
 *     nested constructors) — comparable values can't come back yet.
 */
object DorisCorpusDialect {

    fun accepts(sql: String): Boolean {
        val body = stripLeadingCommentsAndWhitespace(sql)
        if (!startsWithSelectOrParenSelect(body)) {
            return false
        }
        val upper = body.uppercase()
        if (DENIED_SUBSTRINGS.any { it in body }) {
            return false
        }
        if (DENIED_REGEXES.any { it.containsMatchIn(upper) }) {
            return false
        }
        if (DENIED_WORDS.any { containsWord(upper, it) }) {
            return false
        }
        return true
    }

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

    /** Word-boundary match so e.g. denied `RANGE` doesn't reject `ORANGES`. */
    private fun containsWord(upperSql: String, upperWord: String): Boolean {
        var from = 0
        while (true) {
            val i = upperSql.indexOf(upperWord, from)
            if (i < 0) {
                return false
            }
            if (isBoundary(upperSql.getOrNull(i - 1)) &&
                isBoundary(upperSql.getOrNull(i + upperWord.length))
            ) {
                return true
            }
            from = i + upperWord.length
        }
    }

    private fun isBoundary(c: Char?): Boolean = c == null || !c.isJavaIdentifierPart()

    /** Raw-substring denials (syntax tokens, not identifiers). */
    private val DENIED_SUBSTRINGS = listOf(
        "::", // DuckDB cast shorthand — Doris uses CAST(x AS t)
        "$$", // dollar-quoted strings
        "->", // lambda / json arrows
        "[", // LIST/ARRAY literals + indexing (also blocks list slicing)
        "{", // STRUCT literals
    )

    /**
     * Pattern denials over the uppercased body. DuckDB's inline time travel
     * (`FROM t AT (VERSION => 3)` / `AT (TIMESTAMP => …)`) has no mechanical
     * rewrite to Doris's `FOR VERSION/TIME AS OF`; first contact showed it
     * reaching the FE parser as an empty-detail error. Bare-word `AT` is far
     * too common to deny — the paren form is the actual syntax.
     */
    private val DENIED_REGEXES = listOf(
        Regex("\\bAT\\s*\\("),
    )

    /**
     * Word-level denials (case-insensitive): DuckDB catalog/table functions,
     * pragmas, and function families Doris lacks or renders incomparably.
     * Seeded from a first read of the upstream corpus; grow/shrink with
     * live-run evidence.
     */
    private val DENIED_WORDS = listOf(
        // DuckDB-internal state readers
        "PRAGMA", "DUCKDB_TABLES", "DUCKDB_COLUMNS", "DUCKDB_SCHEMAS",
        "DUCKDB_CONSTRAINTS", "DUCKDB_SETTINGS", "SQLITE_MASTER",
        "INFORMATION_SCHEMA", "CURRENT_SETTING", "GLOB",
        // table functions over files / generators
        "READ_PARQUET", "READ_CSV", "READ_JSON", "PARQUET_SCAN",
        "PARQUET_METADATA", "PARQUET_FILE_METADATA", "PARQUET_KV_METADATA",
        "PARQUET_SCHEMA", "RANGE", "GENERATE_SERIES", "UNNEST",
        // nested-type constructors / accessors (v1 type surface can't compare)
        "STRUCT_PACK", "LIST_VALUE", "MAP_FROM_ENTRIES", "STRUCT_EXTRACT",
        "LIST_EXTRACT", "MAP_EXTRACT", "ARRAY_AGG", "LIST_AGG",
        // temporal families our surface degrades to STRING
        "INTERVAL", "MAKE_TIME", "MAKE_INTERVAL",
        // sampling / non-deterministic
        "TABLESAMPLE", "USING SAMPLE", "RANDOM", "UUID",
        // DuckLake metadata functions (oracle-side concern)
        "DUCKLAKE_SNAPSHOTS", "DUCKLAKE_TABLE_INFO", "DUCKLAKE_TABLE_CHANGES",
    )
}
