package dev.brikk.ducklake.doris.corpus

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.util.concurrent.atomic.AtomicInteger

import dev.brikk.ducklake.corpus.OracleAttachment
import dev.brikk.ducklake.corpus.ReplayEngineSkip
import dev.brikk.ducklake.corpus.ReplayReadEngine

/**
 * [ReplayReadEngine] adapter: mirrors corpus lake reads through a LIVE compose
 * Doris FE+BE (`compose/smoke.sh --up-only`) over the mysql protocol.
 * Reference model: `TrinoReplayEngine` (trino-ducklake) — same
 * metadata-rewrite / alias-rewrite / error-classification shape, different
 * transport.
 *
 * Backend axis wiring (unlike Trino, our engine is out-of-process and half of
 * it is containerized — two views of everything):
 *  - METADATA: [metadataRewriter] allocates a fresh database per corpus file
 *    on the compose substrate PostgreSQL. The ORACLE attaches via the
 *    host-mapped port (`localhost:9432`); the FE catalog gets the
 *    container-DNS form (`trino-ducklake-postgres:5432`) of the SAME database.
 *  - DATA: the oracle writes parquet under `java.io.tmpdir`, which the
 *    `corpusReplayTest` gradle task pins to a host dir the compose bind-mounts
 *    into FE+BE at the SAME absolute path — host-side oracle and
 *    containerized BE read identical file paths.
 *
 * v1 `accepts` gate: [DorisCorpusDialect] (SELECT-only + DuckDB-ism deny
 * tiers) AND alias-qualified reads only — bare table names may reference the
 * oracle's in-memory temp tables, not the lake.
 */
internal class DorisReplayEngine(
    private val feJdbcUrl: String =
        System.getProperty("doris.corpus.fe.url", "jdbc:mysql://127.0.0.1:9030/?user=root"),
    private val pgHostUrl: String =
        System.getProperty("doris.corpus.pg.host.url", "jdbc:postgresql://localhost:9432/ducklake"),
    private val pgUser: String = System.getProperty("doris.corpus.pg.user", "ducklake"),
    private val pgPassword: String = System.getProperty("doris.corpus.pg.password", "ducklake"),
    /** DuckLake ATTACH remainder host/port the ORACLE uses for the rewritten metadata. */
    private val pgOracleHostPort: String =
        System.getProperty("doris.corpus.pg.oracle.hostport", "host=localhost port=9432"),
    /** JDBC host:port the FE (inside the compose network) reaches the same PG by. */
    private val pgFeHostPort: String =
        System.getProperty("doris.corpus.pg.fe.hostport", "trino-ducklake-postgres:5432"),
) : ReplayReadEngine {

    override val name: String = "doris"

    private val counter = AtomicInteger()

    /**
     * Globally-unique naming: the corpus test chunks files across engine
     * instances, and a failed (best-effort) drop in a previous instance must
     * never collide with this one — first full-corpus contact lost 112 files
     * to exactly that ("database corpus_doris_1 already exists").
     */
    private val runId: String = java.lang.Long.toHexString(System.nanoTime())
    private val createdDatabases = mutableListOf<String>()
    private val createdCatalogs = mutableListOf<String>()

    private val fe: Connection = DriverManager.getConnection(feJdbcUrl)
    private val pg: Connection = DriverManager.getConnection(pgHostUrl, pgUser, pgPassword)

    private var currentCatalog: String? = null
    private var currentAlias: String? = null
    private var lastDatabase: String? = null

    /**
     * The corpus driver's metadata-rewrite hook: fresh PG database per
     * original metadata path; the oracle re-attaches on the host-mapped port.
     */
    val metadataRewriter: (String) -> String = { _ ->
        val db = "corpus_doris_${runId}_" + counter.incrementAndGet()
        pg.createStatement().use { it.execute("CREATE DATABASE $db") }
        createdDatabases += db
        lastDatabase = db
        "postgres:dbname=$db $pgOracleHostPort user=$pgUser password=$pgPassword"
    }

    override fun connect(attachment: OracleAttachment) {
        val db = lastDatabase ?: error("metadataRewriter did not run before connect()")
        currentCatalog = null
        currentAlias = null
        if (attachment.dataPath.isBlank()) {
            // No DATA_PATH on the attach: DuckLake derived a default from a
            // local metadata path that has no engine-side equivalent. Stay
            // disconnected — accepts() rejects everything for this file.
            return
        }
        val catalog = "corpus_${runId}_" + counter.get()
        fe.createStatement().use { st ->
            st.execute("DROP CATALOG IF EXISTS $catalog")
            st.execute(
                """
                CREATE CATALOG $catalog PROPERTIES (
                    'type' = 'ducklake',
                    'metadata.url' = 'jdbc:postgresql://$pgFeHostPort/$db',
                    'metadata.user' = '$pgUser',
                    'metadata.password' = '$pgPassword',
                    'storage.warehouse' = '${attachment.dataPath}'
                )
                """.trimIndent(),
            )
        }
        createdCatalogs += catalog
        currentCatalog = catalog
        currentAlias = attachment.catalogAlias
    }

    override fun accepts(sql: String): Boolean {
        val alias = currentAlias ?: return false
        val s = sql.trim()
        // Must reference the lake by alias — bare names may be oracle-local
        // temp tables (invisible to the FE by design).
        if (!Regex("\\b${Regex.escape(alias)}\\.").containsMatchIn(s)) {
            return false
        }
        // Catalog-scoped table functions (`<alias>.snapshots()`, …) have no
        // Doris-side rewrite.
        if (Regex("\\b${Regex.escape(alias)}\\.\\w+\\s*\\(").containsMatchIn(s)) {
            return false
        }
        return DorisCorpusDialect.accepts(s)
    }

    override fun executeQuery(sql: String): List<List<String?>> {
        val catalog = currentCatalog ?: error("connect() was not called")
        val alias = currentAlias ?: error("connect() was not called")
        var s = sql.trim().removeSuffix(";").trim()
        // DuckDB inline time travel `t AT (VERSION => n)` → Doris
        // `t FOR VERSION AS OF n` (accepts() already gated to the literal
        // form). Done BEFORE the alias rewrite so the clause stays attached to
        // its (still-alias-qualified) table reference.
        s = DorisCorpusDialect.rewriteInlineTimeTravel(s)
        // The mirror compares sorted rows, so DuckDB's ORDER BY ALL is
        // droppable rather than untranslatable.
        s = s.replace(Regex("\\bORDER\\s+BY\\s+ALL\\s*$", RegexOption.IGNORE_CASE), "")
        // <alias>.<schema>.<t> → <catalog>.<schema>.<t>; then <alias>.<t>
        // (no further dot) → <catalog>.main.<t> (DuckLake's default schema).
        s = s.replace(Regex("\\b${Regex.escape(alias)}\\.(\\w+)\\.(\\w+)"), "$catalog.$1.$2")
        s = s.replace(Regex("\\b${Regex.escape(alias)}\\.(\\w+)\\b(?!\\.)"), "$catalog.main.$1")
        return try {
            fe.createStatement().use { st ->
                // The oracle writes to the lake BETWEEN our reads; Doris's
                // external-catalog objects (db/table lists, snapshot state)
                // are FE-cached by design and REFRESH is the documented
                // freshness contract for externally-written catalogs. Without
                // it, first contact showed schemas created after CREATE
                // CATALOG missing and drop/recreate cycles serving stale rows.
                st.execute("REFRESH CATALOG $catalog")
                st.executeQuery(s).use { rs -> readRows(rs) }
            }
        } catch (e: SQLException) {
            throw classify(e)
        }
    }

    /**
     * Error classification, mirroring the Trino adapter's tiers: documented
     * gaps become [ReplayEngineSkip]; everything else fails loudly.
     */
    private fun classify(e: SQLException): Exception {
        val message = e.message ?: return e
        return when {
            // Our connector doesn't surface DuckLake VIEWs (nor DuckDB-dialect
            // views); their names don't resolve FE-side by design.
            message.contains("Unknown table") ||
                message.contains("Unknown database") ||
                message.contains("does not exist") ->
                ReplayEngineSkip("relation does not resolve (views/unsupported surface): " + firstLine(message))
            // The connector's own deliberate rejection gates (type mapper,
            // nested v1 limits, inlined-state guard, …) fail cleanly by design.
            message.contains("not supported") || message.contains("Unsupported") ->
                ReplayEngineSkip("connector documented gap: " + firstLine(message))
            // DuckDB functions with no Doris equivalent (typeof,
            // uuid_extract_version, …): dialect gap, not a correctness signal —
            // the generic classification beats an ever-growing deny-list.
            message.contains("Can not found function") ->
                ReplayEngineSkip("dialect: function missing in doris — " + firstLine(message))
            else -> e
        }
    }

    private fun readRows(rs: ResultSet): List<List<String?>> {
        val md = rs.metaData
        val cols = md.columnCount
        // Doris BOOLEAN is TINYINT(1) on the wire; the driver exposes it as
        // BIT/BOOLEAN jdbc types when it detects width 1 — thread that to the
        // normalizer so 1/0 renders true/false like the oracle's booleans.
        val logicalBoolean = (1..cols).map { i ->
            md.getColumnType(i) == Types.BIT || md.getColumnType(i) == Types.BOOLEAN
        }
        val rows = mutableListOf<List<String?>>()
        while (rs.next()) {
            rows += (1..cols).map { i ->
                DorisValueNormalizer.normalize(rs.getObject(i), logicalBoolean[i - 1])
            }
        }
        return rows
    }

    private fun firstLine(message: String): String = message.lineSequence().first()

    override fun close() {
        for (catalog in createdCatalogs) {
            runCatching { fe.createStatement().use { it.execute("DROP CATALOG IF EXISTS $catalog") } }
                .onFailure { System.err.println("corpus cleanup: DROP CATALOG $catalog failed: ${it.message}") }
        }
        runCatching { fe.close() }
        // FORCE: the FE plugin may hold pooled connections to the corpus DBs.
        // Failures are non-fatal (names are run-unique) but must be VISIBLE —
        // silent drop failures cost 112 files on first full-corpus contact.
        for (db in createdDatabases) {
            runCatching { pg.createStatement().use { it.execute("DROP DATABASE IF EXISTS $db WITH (FORCE)") } }
                .onFailure { System.err.println("corpus cleanup: DROP DATABASE $db failed: ${it.message}") }
        }
        runCatching { pg.close() }
    }
}
