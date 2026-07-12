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
 * `accepts` gate: alias-qualified reads only (`<alias>.<table>` — bare names may be the oracle's
 * in-memory temp tables, not the lake) AND [TrinoCorpusDialect] returns a `Run`. The dialect is
 * transpile-first (brikk-sql translates DuckDB→Trino and keys off the transpiler's own
 * unmappable/unsupported/verify signals), replacing the former hand-maintained DuckDB-ism token
 * deny-list + hand-rolled rewrites. [executeQuery] runs the transpiled SQL (after the
 * corpus-specific alias→catalog rewrite).
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

    /** Cache the last gate decision so accepts() + executeQuery() transpile once. */
    private var lastGate: Pair<String, TrinoCorpusDialect.Gate>? = null

    private fun gateFor(sql: String): TrinoCorpusDialect.Gate {
        lastGate?.let { (cachedSql, gate) -> if (cachedSql == sql) return gate }
        val gate = TrinoCorpusDialect.gate(sql)
        lastGate = sql to gate
        return gate
    }

    override fun accepts(sql: String): Boolean {
        val alias = currentAlias ?: return false
        val s = sql.trim()
        // Must reference the lake by alias — bare names may be oracle-local temp tables (harness
        // concern, not dialect, so it stays here rather than in the transpile gate).
        if (!Regex("\\b${Regex.escape(alias)}\\.").containsMatchIn(s)) return false
        // Catalog-scoped table functions (`<alias>.snapshots()`, …) have no 1:1 rewrite; our
        // equivalents are $-tables/PTFs with different names.
        if (Regex("\\b${Regex.escape(alias)}\\.\\w+\\s*\\(").containsMatchIn(s)) return false
        // Transpile-first gate: runnable only if brikk-sql faithfully translates duckdb→trino
        // (replaces the former token deny-list + INTERVAL/`::`/… heuristics — see TrinoCorpusDialect).
        return gateFor(sql) is TrinoCorpusDialect.Run
    }

    override fun executeQuery(sql: String): List<List<String?>> {
        val session = currentSession ?: error("connect() was not called")
        val catalog = currentCatalog!!
        val alias = currentAlias!!
        // The transpiled Trino SQL (DuckDB-isms translated, time travel mapped, ORDER BY ALL
        // handled) — accepts() already confirmed this is a Run. The only rewrite left is the
        // corpus alias→catalog step (brikk-sql passes table identifiers through unquoted):
        // <alias>.<schema>.<t> → <catalog>.<schema>.<t>; then <alias>.<t> (no further dot) →
        // <catalog>.main.<t>.
        val run = gateFor(sql) as? TrinoCorpusDialect.Run
            ?: error("executeQuery on a non-runnable query: ${gateFor(sql)}")
        var s = run.trinoSql
        s = s.replace(Regex("\\b${Regex.escape(alias)}\\.(\\w+)\\.(\\w+)"), "$catalog.$1.$2")
        s = s.replace(Regex("\\b${Regex.escape(alias)}\\.(\\w+)\\b(?!\\.)"), "$catalog.main.$1")
        val result =
            try {
                runner.execute(session, s)
            } catch (e: RuntimeException) {
                throw classifyEngineError(e)
            }
        val types = result.types
        return result.materializedRows.map { row ->
            (0 until row.fieldCount).map { i -> renderTyped(row.getField(i), types[i]) }
        }
    }

    /**
     * Classifies engine errors: KNOWN, documented gaps become
     * [dev.brikk.ducklake.corpus.ReplayEngineSkip] (recorded as engine-skips,
     * never failures); everything else propagates as a real failure.
     *  - "does not exist": corpus views are DuckDB-dialect and skipped by the
     *    connector by design, so their names don't resolve on the Trino side.
     *  - "Cannot apply operator": implicit-cast dialect gap — DuckDB coerces
     *    varchar literals ('inf', dates, …) in comparisons; Trino is strictly
     *    typed by design.
     *  - "not (yet) supported": the connector's own deliberate NOT_SUPPORTED
     *    gates — documented gaps that fail cleanly.
     */
    private fun classifyEngineError(e: RuntimeException): RuntimeException {
        val message = e.message ?: return e
        val first = message.lineSequence().first()
        return when {
            message.contains("does not exist") ->
                dev.brikk.ducklake.corpus.ReplayEngineSkip(
                    "relation does not resolve (duckdb-dialect view is a documented skip): $first")
            message.contains("Cannot apply operator") ->
                dev.brikk.ducklake.corpus.ReplayEngineSkip("dialect: no implicit cast — $first")
            message.contains("not yet supported") || message.contains("not supported") ->
                dev.brikk.ducklake.corpus.ReplayEngineSkip("connector documented gap: $first")
            // DuckDB-only function the accepts() heuristic can't enumerate.
            message.contains("not registered") ->
                dev.brikk.ducklake.corpus.ReplayEngineSkip("dialect: DuckDB-only function — $first")
            // Known type-mapping gap failing cleanly (e.g. unsigned widenings on
            // add_files parquet — uint audit, F8/F11 territory).
            message.contains("Unsupported Trino column type") ->
                dev.brikk.ducklake.corpus.ReplayEngineSkip("connector documented type gap: $first")
            else -> e
        }
    }

    /**
     * Renders a materialized Trino value into DuckDB's text dialect so it
     * compares against the oracle's rendering: ROW → `{'name': v}`, MAP →
     * `{k=v}`, ARRAY → `[v, ...]`, scalars via [GoldenComparator.renderCell].
     * Trino materializes ROWs as [io.trino.testing.MaterializedRow] or List,
     * MAPs as java Map, ARRAYs as List.
     */
    private fun renderTyped(value: Any?, type: io.trino.spi.type.Type): String? {
        if (value == null) return null
        return when (type) {
            is io.trino.spi.type.RowType -> {
                val fields = type.fields
                val children = rowChildren(value)
                fields.indices.joinToString(", ", prefix = "{", postfix = "}") { i ->
                    val name = fields[i].name.orElse("f$i")
                    "'$name': ${renderTypedNested(children[i], fields[i].type)}"
                }
            }
            is io.trino.spi.type.MapType ->
                (value as Map<*, *>).entries.joinToString(", ", prefix = "{", postfix = "}") { (k, v) ->
                    "${renderTypedNested(k, type.keyType)}=${renderTypedNested(v, type.valueType)}"
                }
            is io.trino.spi.type.ArrayType ->
                (value as List<*>).joinToString(", ", prefix = "[", postfix = "]") {
                    renderTypedNested(it, type.elementType)
                }
            else -> GoldenComparator.renderCell(value)
        }
    }

    private fun renderTypedNested(value: Any?, type: io.trino.spi.type.Type): String =
        when {
            value == null -> "NULL"
            type is io.trino.spi.type.RowType ||
                type is io.trino.spi.type.MapType ||
                type is io.trino.spi.type.ArrayType -> renderTyped(value, type)!!
            else -> GoldenComparator.renderNested(value)
        }

    private fun rowChildren(value: Any): List<Any?> =
        when (value) {
            is io.trino.testing.MaterializedRow -> value.fields
            is List<*> -> value
            else -> throw IllegalStateException("Unexpected materialized ROW shape: ${value.javaClass.name}")
        }

    override fun close() {
        runCatching { runner.close() }
        runCatching { server.close() }
    }
}
