package dev.brikk.ducklake.corpus

/**
 * A query engine that can read the DuckLake catalog state built by the DuckDB
 * oracle during a corpus replay. Implementations live ENGINE-SIDE (Trino
 * adapter in `trino-ducklake`, Doris adapter in `doris-ducklake`); this module
 * only defines the seam plus the DuckDB identity-control implementation.
 *
 * Contract:
 *  - [executeQuery] returns rows of canonical cell strings: SQL NULL is Kotlin
 *    `null`; everything else is the engine's string rendering. The driver
 *    canonicalizes further (see [GoldenComparator]) before comparing against
 *    the oracle's live results — comparisons are live-vs-live typed values,
 *    never sqllogictest golden text (only the oracle is held to golden text).
 *  - [accepts] is the dialect gate: return false for SQL the engine cannot or
 *    should not attempt (engine-specific syntax, non-SELECT, unsupported
 *    functions). Rejected queries count as engine-skips in the report, never
 *    failures.
 *  - The engine reads the SAME catalog + data path the oracle writes. How it
 *    attaches (JDBC URL, Trino catalog config, Doris FE catalog) is the
 *    adapter's business; [connect] receives the oracle's catalog handle so the
 *    adapter can wire itself per replayed file.
 */
interface ReplayReadEngine : AutoCloseable {
    val name: String

    /**
     * Called when the oracle attaches a DuckLake catalog (first attach per
     * corpus file). Carries everything an adapter needs to point its engine
     * at the same lake: the metadata location (post-rewrite — see
     * [ReplayDriver]'s `metadataRewriter`), the data path, and the alias the
     * corpus SQL references the catalog by (adapters rewrite
     * `<alias>.<table>` into their own qualified form).
     */
    fun connect(attachment: OracleAttachment)

    /** Dialect gate — see contract above. */
    fun accepts(sql: String): Boolean = true

    /** Execute a read; rows of cells, null cell = SQL NULL. */
    fun executeQuery(sql: String): List<List<String?>>
}

/**
 * What the oracle attached. [metadataUri] is the DuckLake connection remainder
 * (the part after `ducklake:` — e.g. `/tmp/x/meta.db` or
 * `postgres:dbname=corpus_1 host=localhost port=5433 user=u password=p`).
 * Extensible without breaking adapter signatures.
 */
data class OracleAttachment(
    val metadataUri: String,
    val dataPath: String,
    val catalogAlias: String,
)

/**
 * Thrown by [ReplayReadEngine.executeQuery] when the engine recognizes a KNOWN,
 * documented gap mid-execution (e.g. Trino skips DuckDB-dialect views, so the
 * relation doesn't exist on its side). The driver records an engine-skip with
 * [reason] instead of a failure. Use sparingly — anything not a documented gap
 * should fail loudly.
 */
class ReplayEngineSkip(val reason: String) : RuntimeException(reason)
