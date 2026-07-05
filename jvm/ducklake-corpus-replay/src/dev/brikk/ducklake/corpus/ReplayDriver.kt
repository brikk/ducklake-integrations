package dev.brikk.ducklake.corpus

import java.sql.SQLException

/**
 * Executes one parsed corpus file against a fresh [DuckDbOracle], producing a
 * [FileResult]. Optionally mirrors lake reads through a [ReplayReadEngine]
 * (live-vs-live comparison); with no engine this is the identity-control mode
 * where the oracle's results are validated against the golden text — which is
 * what proves the parser, templating, and comparator are faithful.
 *
 * Skip policy (never fail on the un-runnable):
 *  - unsatisfiable `require` → whole-file skip with reason
 *  - [SltUnsupported] anywhere → whole-file skip (v1 keeps semantics honest
 *    rather than running around unknown constructs)
 *  - hash-format expected results / conditional records → record-level skip
 */
class ReplayDriver(
    private val engine: ReplayReadEngine? = null,
    private val repoRoot: java.nio.file.Path? = null,
) {

    fun replay(file: SltFile): FileResult {
        val unsupported = findUnsupported(file.records)
        if (unsupported != null) {
            return FileResult(
                file.path,
                "unsupported construct '${unsupported.directive}' at line ${unsupported.line}",
                emptyList(),
            )
        }
        return DuckDbOracle(repoRoot = repoRoot).use { oracle ->
            val outcomes = mutableListOf<RecordOutcome>()
            labelResults.clear()
            val halted = executeAll(file.records, oracle, emptyMap(), outcomes)
            FileResult(file.path, fileSkipReason = halted, outcomes = outcomes)
        }
    }

    /** Result-sharing store for labeled queries, per replayed file. */
    private val labelResults = mutableMapOf<String, List<List<String>>>()

    private fun findUnsupported(records: List<SltRecord>): SltUnsupported? {
        for (r in records) {
            when (r) {
                is SltUnsupported -> return r
                is SltLoop -> findUnsupported(r.body)?.let { return it }
                is SltConditional -> findUnsupported(listOf(r.record))?.let { return it }
                else -> {}
            }
        }
        return null
    }

    /**
     * Executes records in order. Returns a file-skip reason when a `require`
     * is unsatisfiable (upstream semantics: unmet require skips the rest of
     * the file), null otherwise.
     */
    private fun executeAll(
        records: List<SltRecord>,
        oracle: DuckDbOracle,
        bindings: Map<String, String>,
        outcomes: MutableList<RecordOutcome>,
    ): String? {
        for (record in records) {
            when (record) {
                is SltRequire -> {
                    val miss = oracle.require(record.requirement)
                    if (miss != null) return "require ${record.requirement}: $miss"
                }
                is SltTestEnv -> oracle.defineEnv(record.name, bind(record.value, bindings))
                is SltStatement -> outcomes += runStatement(record, oracle, bindings)
                is SltQuery -> outcomes += runQuery(record, oracle, bindings)
                is SltLoop -> {
                    for (v in record.values) {
                        val halted = executeAll(record.body, oracle, bindings + (record.variable to v), outcomes)
                        if (halted != null) return halted
                    }
                }
                is SltConditional -> {
                    // Two condition dialects: `skipif/onlyif <engine>` (oracle is
                    // duckdb) and `skipif/onlyif var=value` over loop bindings.
                    val cond = evalCondition(record.engine, bindings)
                    val run = if (record.skipIf) !cond else cond
                    if (run) {
                        val halted = executeAll(listOf(record.record), oracle, bindings, outcomes)
                        if (halted != null) return halted
                    } else {
                        outcomes += RecordOutcome.Skip(record.record, "${if (record.skipIf) "skipif" else "onlyif"} ${record.engine}")
                    }
                }
                is SltUnsupported -> error("unsupported records are filtered before execution")
            }
        }
        return null
    }

    /** True when the guarded record applies. */
    private fun evalCondition(expr: String, bindings: Map<String, String>): Boolean {
        val eq = expr.indexOf('=')
        if (eq > 0) {
            val variable = expr.substring(0, eq)
            val value = expr.substring(eq + 1)
            val bound = bindings[variable]
            if (bound != null) return bound == value
        }
        return expr.equals("duckdb", ignoreCase = true)
    }

    private fun bind(text: String, bindings: Map<String, String>): String {
        var s = text
        for ((k, v) in bindings) {
            s = s.replace("\${$k}", v).replace("{$k}", v)
        }
        return s
    }

    private fun runStatement(
        record: SltStatement,
        oracle: DuckDbOracle,
        bindings: Map<String, String>,
    ): RecordOutcome {
        val sql = oracle.substitute(bind(record.sql, bindings))
        val conn = oracle.connection(record.connection)
        return try {
            conn.createStatement().use { it.execute(sql) }
            if (record.expectError && record.expectedError != null) {
                RecordOutcome.Fail(record, "expected error containing '${record.expectedError}' but statement succeeded")
            } else if (record.expectError) {
                // `statement maybe` (expectedError == null): success is acceptable.
                RecordOutcome.Pass(record)
            } else {
                RecordOutcome.Pass(record)
            }
        } catch (e: SQLException) {
            if (!record.expectError) {
                RecordOutcome.Fail(record, "unexpected error: ${firstLine(e)}")
            } else if (errorMatches(record.expectedError, e)) {
                RecordOutcome.Pass(record)
            } else {
                RecordOutcome.Fail(record, "error mismatch: expected '${record.expectedError}', got: ${firstLine(e)}")
            }
        }
    }

    private fun errorMatches(expected: String?, e: SQLException): Boolean {
        if (expected == null) return true
        val message = e.message ?: return false
        val regexPrefix = "<REGEX>:"
        return if (expected.startsWith(regexPrefix)) {
            Regex(expected.removePrefix(regexPrefix), RegexOption.DOT_MATCHES_ALL).containsMatchIn(message)
        } else {
            message.contains(expected)
        }
    }

    private fun runQuery(
        record: SltQuery,
        oracle: DuckDbOracle,
        bindings: Map<String, String>,
    ): RecordOutcome {
        val sql = oracle.substitute(bind(record.sql, bindings))
        val conn = oracle.connection(record.connection)
        val actual =
            try {
                conn.createStatement().use { st ->
                    // DML under a `query` directive expects the changed-row count
                    // as the single-cell result — the DuckDB JDBC driver only
                    // reports it via executeUpdate (execute() leaves it at -1).
                    if (isDml(sql)) {
                        listOf(listOf(st.executeUpdate(sql).toString()))
                    } else {
                        // Generic execute: some corpus `query` records wrap
                        // CALL/PRAGMA shapes the driver refuses via executeQuery.
                        val hasResult = st.execute(sql)
                        when {
                            hasResult -> st.resultSet.use { rs -> GoldenComparator.readRows(rs) }
                            st.updateCount >= 0 -> listOf(listOf(st.updateCount.toString()))
                            else -> emptyList()
                        }
                    }
                }
            } catch (e: Exception) {
                // Unchecked escapes happen too (e.g. the JDBC driver throws
                // DateTimeException on TIME '24:00:00'); a record failure must
                // never abort the corpus.
                return RecordOutcome.Fail(record, "query errored: ${firstLine(e)}")
            }
        // Result-sharing labels: first occurrence stores, later ones must match.
        val label = record.label
        if (label != null && record.expected.isEmpty()) {
            val cells = actual.map { row -> row.map(GoldenComparator::toGoldenCell) }
            val stored = labelResults[label]
            return if (stored == null) {
                labelResults[label] = cells
                RecordOutcome.Pass(record)
            } else if (stored == cells) {
                RecordOutcome.Pass(record)
            } else {
                RecordOutcome.Fail(record, "labeled result '$label' diverged from first occurrence")
            }
        }
        // Golden text may reference loop variables and template vars (e.g. an
        // expected count that differs per foreach iteration, or DATA_PATH in
        // path-returning metadata queries) — substitute before comparing.
        val effective =
            if (record.expected.isEmpty()) {
                record
            } else {
                record.copy(expected = record.expected.map { oracle.substitute(bind(it, bindings)) })
            }
        val cmp = GoldenComparator.compare(effective, actual)
        val goldenOutcome =
            when (cmp) {
                is GoldenComparator.Comparison.Match -> RecordOutcome.Pass(record)
                is GoldenComparator.Comparison.Mismatch -> RecordOutcome.Fail(record, cmp.detail)
                is GoldenComparator.Comparison.Unsupported -> RecordOutcome.Skip(record, cmp.reason)
            }
        if (goldenOutcome !is RecordOutcome.Pass || engine == null || !engine.accepts(sql)) {
            return goldenOutcome
        }
        // Live-vs-live mirror through the engine.
        return try {
            val engineRows = engine.executeQuery(sql).map { row -> row.map(GoldenComparator::toGoldenCell) }
            val oracleRows = actual.map { row -> row.map(GoldenComparator::toGoldenCell) }
            if (engineRows.toSortedComparable() == oracleRows.toSortedComparable()) {
                RecordOutcome.Pass(record)
            } else {
                RecordOutcome.Fail(record, "engine '${engine.name}' diverged from oracle")
            }
        } catch (e: Exception) {
            RecordOutcome.Fail(record, "engine '${engine.name}' errored: ${e.message?.lineSequence()?.firstOrNull()}")
        }
    }

    private fun List<List<String>>.toSortedComparable(): List<String> =
        map { it.joinToString("\u0001") }.sorted()

    private val DML = Regex("^\\s*(insert|update|delete|merge|truncate)\\b", RegexOption.IGNORE_CASE)

    private fun isDml(sql: String): Boolean = DML.containsMatchIn(sql)

    private fun firstLine(e: Exception): String = e.message?.lineSequence()?.firstOrNull() ?: e.toString()
}
