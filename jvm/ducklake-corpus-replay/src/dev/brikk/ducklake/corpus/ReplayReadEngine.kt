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
     * Called once per corpus file before any query is mirrored, with the
     * DuckLake catalog location the oracle attached (e.g. the metadata `.db`
     * path or a Postgres JDBC URL) and the data path.
     */
    fun connect(catalogUri: String, dataPath: String)

    /** Dialect gate — see contract above. */
    fun accepts(sql: String): Boolean = true

    /** Execute a read; rows of cells, null cell = SQL NULL. */
    fun executeQuery(sql: String): List<List<String?>>
}
