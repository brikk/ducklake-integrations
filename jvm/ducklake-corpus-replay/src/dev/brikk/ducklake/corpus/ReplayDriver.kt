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
    /**
     * Metadata-backend axis: maps the DuckLake connection remainder of an
     * ATTACH (the part after `ducklake:`, post template-substitution) to a
     * replacement remainder — e.g. a local `.db` path to
     * `postgres:dbname=corpus_1 host=...` so an external engine can reach the
     * same catalog. Called once per distinct original within a file
     * (memoized: DETACH/re-ATTACH cycles hit the same backend). Null = attach
     * as written (duckdb-local).
     */
    private val metadataRewriter: ((original: String) -> String)? = null,
    /** Max engine mirrors per file (loop-heavy files repeat queries; see [mirrorsThisFile]). */
    private val mirrorCapPerFile: Int = DEFAULT_MIRROR_CAP,
) {

    companion object {
        const val DEFAULT_MIRROR_CAP: Int = 40
    }

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
            rewrittenAttachments.clear()
            attachDataPaths.clear()
            openTransactions.clear()
            mirrorsThisFile = 0
            engineConnected = false
            val halted = executeAll(file.records, oracle, emptyMap(), outcomes)
            FileResult(file.path, fileSkipReason = halted, outcomes = outcomes)
        }
    }

    /** Result-sharing store for labeled queries, per replayed file. */
    private val labelResults = mutableMapOf<String, List<List<String>>>()

    /** Per-file memo of original → rewritten DuckLake connection remainders. */
    private val rewrittenAttachments = mutableMapOf<String, String>()

    /** Per-file memo of original → DATA_PATH (re-attaches often omit options). */
    private val attachDataPaths = mutableMapOf<String, String>()
    private var engineConnected = false

    /**
     * Connections (by label, null = root) with an open oracle transaction.
     * Queries inside one see the oracle's UNCOMMITTED state; the engine reads
     * committed state — divergence there is correct isolation, so the mirror
     * is gated off until COMMIT/ROLLBACK.
     */
    private val openTransactions = mutableSetOf<String?>()

    /**
     * Engine mirrors executed for the current file. Loop-heavy corpus files
     * repeat the same query hundreds of times; the oracle validates every
     * iteration against golden text, but mirroring each one through an
     * external engine multiplies runtime for no additional signal. The cap
     * samples the first N mirrors per file.
     */
    private var mirrorsThisFile = 0

    private val ATTACH_PATTERN =
        Regex(
            "ATTACH\\s+(?:OR\\s+REPLACE\\s+)?(?:IF\\s+NOT\\s+EXISTS\\s+)?'ducklake:([^']+)'(?:\\s+AS\\s+(\\w+))?" +
                "(?:\\s*\\(([^)]*)\\))?",
            RegexOption.IGNORE_CASE,
        )
    private val DATA_PATH_OPTION = Regex("DATA_PATH\\s+'([^']*)'", RegexOption.IGNORE_CASE)

    /**
     * Detects a DuckLake ATTACH, applies the metadata rewrite, and returns the
     * SQL to execute plus the attachment to announce on success (null when the
     * statement is not a ducklake ATTACH or the engine is already connected).
     */
    private val SNAPSHOT_PIN = Regex("SNAPSHOT_(VERSION|TIME)", RegexOption.IGNORE_CASE)

    private fun interceptAttach(sql: String): Pair<String, OracleAttachment?> {
        val m = ATTACH_PATTERN.find(sql) ?: return sql to null
        val options = m.groupValues[3]
        val dataPath = DATA_PATH_OPTION.find(options)?.groupValues?.get(1)
        val original = m.groupValues[1]
        val known = rewrittenAttachments[original]
        if (known == null && dataPath == null) {
            // FIRST attach of a lake without DATA_PATH: DuckLake derives a
            // default from a LOCAL metadata path — a rewritten backend has no
            // equivalent. Attach as written; the file replays oracle-only.
            // (RE-attaches without options must keep hitting the rewritten
            // backend — the memo check above handles them.)
            return sql to null
        }
        val rewritten =
            known ?: rewrittenAttachments.getOrPut(original) { metadataRewriter?.invoke(original) ?: original }
        if (dataPath != null) attachDataPaths[original] = dataPath
        val effectiveSql =
            if (rewritten == original) sql else sql.replace("'ducklake:$original'", "'ducklake:$rewritten'")
        if (SNAPSHOT_PIN.containsMatchIn(options)) {
            // Pinned (time-travel) attach: the oracle now reads an old
            // snapshot; the engine would read latest. Stop mirroring for the
            // rest of the file — divergence is expected, not a bug.
            engineConnected = false
            return effectiveSql to null
        }
        if (engine == null || engineConnected) return effectiveSql to null
        val alias =
            m.groupValues[2].ifEmpty {
                original.substringAfterLast('/').substringBefore('.') // duckdb derives from filename
            }
        val effectiveDataPath = dataPath ?: attachDataPaths[original] ?: ""
        return effectiveSql to OracleAttachment(rewritten, effectiveDataPath, alias)
    }

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
        val substituted = oracle.substitute(bind(record.sql, bindings))
        val (sql, pendingAttachment) = interceptAttach(substituted)
        val conn = oracle.connection(record.connection)
        return try {
            conn.createStatement().use { it.execute(sql) }
            if (pendingAttachment != null && !engineConnected) {
                engine?.connect(pendingAttachment)
                engineConnected = true
            }
            trackTransaction(sql, record.connection)
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
        // Mirror gate: engineConnected is per-file — never mirror against a
        // previous file's catalog, a pinned attach, an un-rewritten lake, or
        // inside an open oracle transaction (uncommitted state is invisible to
        // the engine by design).
        if (goldenOutcome !is RecordOutcome.Pass ||
            engine == null ||
            !engineConnected ||
            record.connection in openTransactions ||
            mirrorsThisFile >= mirrorCapPerFile ||
            !engine.accepts(sql)
        ) {
            return goldenOutcome
        }
        mirrorsThisFile++
        // Live-vs-live mirror through the engine.
        return try {
            val engineRows = engine.executeQuery(sql).map { row -> row.map(GoldenComparator::toGoldenCell) }
            val oracleRows = actual.map { row -> row.map(GoldenComparator::toGoldenCell) }
            if (engineRows.toSortedComparable() == oracleRows.toSortedComparable()) {
                RecordOutcome.Pass(record)
            } else {
                RecordOutcome.Fail(
                    record,
                    "engine '${engine.name}' diverged from oracle\n" +
                        "  oracle (head): ${oracleRows.take(3)}\n" +
                        "  engine (head): ${engineRows.take(3)}",
                )
            }
        } catch (e: ReplayEngineSkip) {
            RecordOutcome.Skip(record, "engine '${engine.name}': ${e.reason}")
        } catch (e: Exception) {
            RecordOutcome.Fail(record, "engine '${engine.name}' errored: ${e.message?.lineSequence()?.firstOrNull()}")
        }
    }

    private fun List<List<String>>.toSortedComparable(): List<String> =
        map { it.joinToString("\u0001") }.sorted()

    private val TXN_BEGIN = Regex("^\\s*BEGIN\\b", RegexOption.IGNORE_CASE)
    private val TXN_END = Regex("^\\s*(COMMIT|ROLLBACK|ABORT)\\b", RegexOption.IGNORE_CASE)

    private fun trackTransaction(sql: String, connection: String?) {
        when {
            TXN_BEGIN.containsMatchIn(sql) -> openTransactions.add(connection)
            TXN_END.containsMatchIn(sql) -> openTransactions.remove(connection)
        }
    }

    private val DML = Regex("^\\s*(insert|update|delete|merge|truncate)\\b", RegexOption.IGNORE_CASE)

    private fun isDml(sql: String): Boolean = DML.containsMatchIn(sql)

    private fun firstLine(e: Exception): String = e.message?.lineSequence()?.firstOrNull() ?: e.toString()
}
