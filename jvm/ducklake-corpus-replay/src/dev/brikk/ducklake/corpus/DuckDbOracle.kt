package dev.brikk.ducklake.corpus

import org.duckdb.DuckDBConnection
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.UUID

/**
 * The oracle: an embedded DuckDB session that executes corpus files verbatim.
 *
 * One oracle per corpus file. Owns:
 *  - the root JDBC connection plus named connections (`con1`, `con2`, …) via
 *    [DuckDBConnection.duplicate] — same database instance, separate sessions
 *    (what the upstream harness does for its concurrent-connection tests);
 *  - template-variable state: `{TEST_DIR}` / `${TEST_DIR}` / `__TEST_DIR__`
 *    map to a per-file temp dir, `{UUID}` is fresh per occurrence, and
 *    `test-env` definitions accumulate;
 *  - extension requirements (INSTALL/LOAD, first run needs network, cached in
 *    `~/.duckdb` afterwards).
 */
class DuckDbOracle(
    testDirRoot: Path? = null,
    /** The pinned ducklake repo root; corpus SQL references test data as repo-relative `data/...` paths. */
    private val repoRoot: Path? = null,
) : AutoCloseable {

    val testDir: Path =
        (testDirRoot ?: Files.createTempDirectory("corpus-replay")).toAbsolutePath()

    private val root: DuckDBConnection =
        DriverManager.getConnection("jdbc:duckdb:").unwrap(DuckDBConnection::class.java)

    private val connections = mutableMapOf<String, Connection>()
    private val env = mutableMapOf<String, String>()
    private val loaded = mutableSetOf<String>()

    init {
        root.createStatement().use { it.execute("SET timezone='UTC'") }
    }

    fun connection(name: String?): Connection =
        if (name == null) {
            root
        } else {
            connections.getOrPut(name) {
                root.duplicate().also { c -> c.createStatement().use { it.execute("SET timezone='UTC'") } }
            }
        }

    /**
     * Satisfies a `require` directive. Returns null on success or a skip
     * reason when the requirement can't be met in this harness.
     */
    fun require(requirement: String): String? {
        val req = requirement.substringBefore(' ').trim()
        return when (req) {
            "notwindows", "core_functions" -> null // always true here
            "ducklake", "parquet", "icu", "json", "httpfs" -> installAndLoad(req)
            "no_extension_autoloading", "no_alternative_verify" -> "requires harness flag '$req'"
            else -> "unsupported extension requirement '$req'"
        }
    }

    private fun installAndLoad(extension: String): String? {
        if (extension in loaded) return null
        return try {
            root.createStatement().use {
                it.execute("INSTALL $extension")
                it.execute("LOAD $extension")
            }
            loaded += extension
            null
        } catch (e: SQLException) {
            "cannot INSTALL/LOAD '$extension': ${e.message?.lineSequence()?.firstOrNull()}"
        }
    }

    fun defineEnv(name: String, value: String) {
        env[name] = substitute(value)
    }

    /** Applies template substitution: env vars, TEST_DIR forms, per-occurrence UUID. */
    fun substitute(text: String): String {
        var s = text.replace("__TEST_DIR__", testDir.toString())
        // Repo-relative test-data references (read_parquet('data/...'),
        // ducklake_add_data_files(..., 'data/...')): upstream's harness runs
        // from the repo root; we resolve against the pinned submodule.
        if (repoRoot != null) {
            s = s.replace("'data/", "'${repoRoot.resolve("data")}/")
        }
        // ${NAME} and {NAME} forms; UUID is fresh per occurrence.
        val pattern = Regex("\\$?\\{([A-Za-z_][A-Za-z0-9_]*)}")
        s = pattern.replace(s) { m ->
            when (val name = m.groupValues[1]) {
                "TEST_DIR" -> testDir.toString()
                "UUID" -> UUID.randomUUID().toString()
                else -> env[name] ?: m.value // unknown vars stay literal
            }
        }
        return s
    }

    /**
     * Best-effort discovery of the DuckLake catalog URI + data path from the
     * session (for wiring a [ReplayReadEngine]). Returns nulls when nothing is
     * attached; refined when the engine axis lands.
     */
    fun attachedDucklake(): Pair<String?, String?> = null to null

    override fun close() {
        connections.values.forEach { runCatching { it.close() } }
        runCatching { root.close() }
        runCatching {
            testDir.toFile().deleteRecursively()
        }
    }
}
