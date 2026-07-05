package dev.brikk.ducklake.corpus

/**
 * Model for DuckDB-dialect sqllogictest files as used by the DuckLake corpus
 * (the .test files under ducklake/test/sql).
 *
 * The dialect (superset of classic sqllogictest):
 *  - `# comment`, blank-line record separation
 *  - `require <extension>` / `require-env NAME [value]`
 *  - `test-env NAME value` (value may reference template vars)
 *  - `statement ok|error [conN]` + SQL lines [+ `----` + expected-error substring]
 *  - `query <types> [rowsort|nosort|label] [conN]` + SQL + `----` + expected rows
 *    (tab-separated columns, one row per line; empty block = empty result)
 *  - `loop var start end` … `endloop` (end exclusive), vars referenced `${var}`
 *  - `foreach var v1 v2 …` … `endloop`
 *  - `skipif <engine>` / `onlyif <engine>` guarding the next record
 *  - template vars: `{NAME}`, `${NAME}`, and `__TEST_DIR__`
 *
 * Constructs deliberately not modeled in v1 (parser emits [Unsupported], the
 * driver skips the whole file with a reason): `concurrentloop`, `restart`,
 * `sleep`, `mode`, `load`, `hash-threshold`, `set`, unknown directives.
 */
sealed interface SltRecord {
    val line: Int
}

/** `statement ok` / `statement error` with optional connection label. */
data class SltStatement(
    override val line: Int,
    val sql: String,
    val expectError: Boolean,
    /** Substring (or `<REGEX>:`-prefixed regex) the error message must match; null = any error. */
    val expectedError: String?,
    val connection: String?,
) : SltRecord

enum class SortMode { NOSORT, ROWSORT, VALUESORT }

/** `query <types>` with expected golden result block. */
data class SltQuery(
    override val line: Int,
    val sql: String,
    /** Column type string, e.g. "III" (I=int, T=text, R=real). */
    val types: String,
    val sortMode: SortMode,
    val connection: String?,
    /**
     * Result-sharing label: the first occurrence stores its result, later
     * queries with the same label must produce the same result.
     */
    val label: String?,
    /** Raw expected lines between `----` and the blank terminator. */
    val expected: List<String>,
) : SltRecord

data class SltRequire(override val line: Int, val requirement: String) : SltRecord

data class SltTestEnv(override val line: Int, val name: String, val value: String) : SltRecord

/** `loop i 0 10` (end exclusive) or `foreach x a b c` — [values] pre-expanded for foreach. */
data class SltLoop(
    override val line: Int,
    val variable: String,
    val values: List<String>,
    val body: List<SltRecord>,
) : SltRecord

/** `skipif engine` / `onlyif engine` applied to the record that follows. */
data class SltConditional(
    override val line: Int,
    val skipIf: Boolean,
    val engine: String,
    val record: SltRecord,
) : SltRecord

/** A construct v1 does not execute; the driver skips the file. */
data class SltUnsupported(override val line: Int, val directive: String, val raw: String) : SltRecord

data class SltFile(
    val path: String,
    val records: List<SltRecord>,
)

// ---------------------------------------------------------------------------
// Replay outcomes
// ---------------------------------------------------------------------------

sealed interface RecordOutcome {
    val record: SltRecord?

    data class Pass(override val record: SltRecord) : RecordOutcome

    data class Fail(override val record: SltRecord, val reason: String) : RecordOutcome

    data class Skip(override val record: SltRecord?, val reason: String) : RecordOutcome
}

data class FileResult(
    val path: String,
    /** Non-null when the whole file was skipped before/without execution. */
    val fileSkipReason: String?,
    val outcomes: List<RecordOutcome>,
) {
    val passed: Int get() = outcomes.count { it is RecordOutcome.Pass }
    val failed: List<RecordOutcome.Fail> get() = outcomes.filterIsInstance<RecordOutcome.Fail>()
    val skipped: List<RecordOutcome.Skip> get() = outcomes.filterIsInstance<RecordOutcome.Skip>()
}
