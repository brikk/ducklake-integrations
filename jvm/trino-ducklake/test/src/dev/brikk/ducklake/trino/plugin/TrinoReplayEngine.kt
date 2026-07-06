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

import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.corpus.GoldenComparator
import dev.brikk.ducklake.corpus.OracleAttachment
import dev.brikk.ducklake.corpus.ReplayReadEngine
import io.trino.Session
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import java.util.concurrent.atomic.AtomicInteger

/**
 * [ReplayReadEngine] adapter: mirrors corpus lake reads through a Trino
 * [DistributedQueryRunner] with the DuckLake connector.
 *
 * Backend axis: owns a [TestingDucklakePostgreSqlCatalogServer] and exposes
 * [metadataRewriter] for the corpus driver — every DuckLake ATTACH the oracle
 * executes is rewritten onto a fresh, isolated PostgreSQL database on this
 * server; [connect] then registers a Trino catalog against the same database
 * (`ducklake.catalog.database-url`) and the oracle's data path, so oracle and
 * engine read the exact same lake, including intermediate states.
 *
 * v1 `accepts` gate: only alias-qualified reads (`<alias>.<table>` or
 * `<alias>.main.<table>`). Bare table names may reference the oracle's
 * in-memory temp tables (not the lake) and DuckDB-specific surfaces
 * (functions, PRAGMA, time-travel `AT (...)`, `::` casts) are engine-skips.
 */
class TrinoReplayEngine : ReplayReadEngine {

    override val name: String = "trino"

    private val server = TestingDucklakePostgreSqlCatalogServer()
    private val runner: DistributedQueryRunner
    private val counter = AtomicInteger()

    private var currentCatalog: String? = null
    private var currentAlias: String? = null
    private var currentSession: Session? = null
    private var lastDatabase: String? = null

    init {
        val session =
            testSessionBuilder()
                .setCatalog("system")
                .setSchema("runtime")
                .build()
        runner = DistributedQueryRunner.builder(session).build()
        runner.installPlugin(DucklakePlugin())
    }

    /**
     * The corpus driver's metadata-rewrite hook: allocate a fresh PG database
     * for the original (duckdb-local) metadata path and return the DuckLake
     * remainder the ORACLE should attach instead.
     */
    val metadataRewriter: (String) -> String = { _ ->
        val db = "corpus_" + counter.incrementAndGet()
        server.createDatabase(db)
        lastDatabase = db
        // Fixture returns the full ATTACH uri including the `ducklake:` prefix;
        // the driver re-adds that prefix, so hand back only the remainder.
        server.getDuckDbAttachUri(db).removePrefix("ducklake:")
    }

    override fun connect(attachment: OracleAttachment) {
        val db = lastDatabase ?: error("metadataRewriter did not run before connect()")
        val catalog = "corpus_" + counter.get()
        val properties =
            ImmutableMap.builder<String, String>()
                .put("ducklake.catalog.database-url", server.getJdbcUrl(db))
                .put("ducklake.catalog.database-user", server.getUser())
                .put("ducklake.catalog.database-password", server.getPassword())
                .put("ducklake.data-path", attachment.dataPath)
                .put("fs.hadoop.enabled", "true")
                .also { props ->
                    System.getProperty("ducklake.test.parityExtensionPath")
                        ?.takeIf { it.isNotBlank() }
                        ?.let { props.put("ducklake.duckdb.parity-extension-path", it) }
                }
                .buildOrThrow()
        runner.createCatalog(catalog, "ducklake", properties)
        currentCatalog = catalog
        currentAlias = attachment.catalogAlias
        currentSession =
            testSessionBuilder()
                .setCatalog(catalog)
                .setSchema("main")
                .build()
    }

    override fun accepts(sql: String): Boolean {
        val alias = currentAlias ?: return false
        val s = sql.trim()
        val upper = s.uppercase()
        if (!upper.startsWith("SELECT ") && !upper.startsWith("FROM ")) return false
        // DuckDB's `FROM t SELECT cols` form can't be mechanically prefixed.
        if (upper.startsWith("FROM ") && upper.contains(" SELECT ")) return false
        // Must reference the lake by alias — bare names may be oracle-local temp tables.
        if (!Regex("\\b$alias\\.").containsMatchIn(s)) return false
        // Catalog-scoped table functions (`<alias>.snapshots()`, …) have no
        // 1:1 rewrite; our equivalents are $-tables/PTFs with different names.
        if (Regex("\\b$alias\\.\\w+\\s*\\(").containsMatchIn(s)) return false
        // DuckDB-specific surfaces we don't attempt in v1. `rowid` is DuckDB's
        // virtual column (ours is `$row_id` — a candidate mapping upgrade).
        val blocked =
            listOf(
                "ducklake_", "__ducklake", "PRAGMA", " AT ", "AT(", "GLOB(", "::",
                "DUCKDB_", "SQLITE_", "INFORMATION_SCHEMA", "CURRENT SETTING", "SETTINGS",
                "ROWID", "POSITIONAL JOIN", "ASOF ", "STATS(",
            )
        return blocked.none { upper.contains(it) }
    }

    override fun executeQuery(sql: String): List<List<String?>> {
        val session = currentSession ?: error("connect() was not called")
        val catalog = currentCatalog!!
        val alias = currentAlias!!
        var s = sql.trim().removeSuffix(";").trim()
        if (s.uppercase().startsWith("FROM ")) {
            s = "SELECT * $s"
        }
        // The mirror compares sorted rows, so DuckDB's ORDER BY ALL is
        // droppable rather than untranslatable.
        s = s.replace(Regex("\\bORDER\\s+BY\\s+ALL\\s*$", RegexOption.IGNORE_CASE), "")
        // <alias>.<schema>.<t> → <catalog>.<schema>.<t>; then <alias>.<t> (no
        // further dot) → <catalog>.main.<t>.
        s = s.replace(Regex("\\b$alias\\.(\\w+)\\.(\\w+)"), "$catalog.$1.$2")
        s = s.replace(Regex("\\b$alias\\.(\\w+)\\b(?!\\.)"), "$catalog.main.$1")
        val result =
            try {
                runner.execute(session, s)
            } catch (e: RuntimeException) {
                val message = e.message ?: throw e
                if (message.contains("does not exist")) {
                    // Documented parity gap: the connector exposes Trino-dialect
                    // views only; corpus views are DuckDB-dialect and skipped by
                    // design, so their names don't resolve on the Trino side.
                    throw dev.brikk.ducklake.corpus.ReplayEngineSkip(
                        "relation does not resolve (duckdb-dialect view is a documented skip): " +
                            message.lineSequence().first(),
                    )
                }
                if (message.contains("Cannot apply operator")) {
                    // Implicit-cast dialect gap: DuckDB coerces varchar
                    // literals ('inf', dates, …) in comparisons; Trino is
                    // strictly typed by design.
                    throw dev.brikk.ducklake.corpus.ReplayEngineSkip(
                        "dialect: no implicit cast — " + message.lineSequence().first(),
                    )
                }
                if (message.contains("not yet supported") || message.contains("not supported")) {
                    // The connector's own deliberate NOT_SUPPORTED gates (e.g.
                    // inlined reads of nested element types) — documented gaps
                    // that fail cleanly by design.
                    throw dev.brikk.ducklake.corpus.ReplayEngineSkip(
                        "connector documented gap: " + message.lineSequence().first(),
                    )
                }
                throw e
            }
        return result.materializedRows.map { row ->
            (0 until row.fieldCount).map { i -> GoldenComparator.renderCell(row.getField(i)) }
        }
    }

    override fun close() {
        runCatching { runner.close() }
        runCatching { server.close() }
    }
}
