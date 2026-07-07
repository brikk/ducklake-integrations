package dev.brikk.ducklake.doris.corpus

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

import dev.brikk.ducklake.corpus.CorpusReport
import dev.brikk.ducklake.corpus.CorpusRunner
import dev.brikk.ducklake.corpus.FileResult

import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

import org.assertj.core.api.Assertions.assertThat

/**
 * Corpus replay with the Doris engine mirroring lake reads (live-vs-live
 * against the DuckDB oracle) on the PostgreSQL backend axis. Model:
 * `TestTrinoCorpusReplay` (trino-ducklake).
 *
 * PRECONDITIONS (all assume-gated, so plain `:doris-ducklake:test` runs never
 * fail on them — this class is driven by the `corpusReplayTest` gradle task):
 *  1. live compose cluster: `compose/smoke.sh --up-only` (FE on
 *     127.0.0.1:9030, substrate PG host-mapped on 9432),
 *  2. `java.io.tmpdir` inside the compose corpus bind-mount (the gradle task
 *     pins it to /tmp/ducklake-corpus) so the containerized BE sees the
 *     oracle's data files at identical absolute paths,
 *  3. duckdb `ducklake`/`postgres` extensions installable (network on first
 *     run), corpus submodule initialized.
 *
 * A failure here means Doris read the same committed catalog state
 * differently than DuckDB — exactly the class of bug this harness exists for.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class DorisCorpusReplayTest {

    private val corpusRoot: Path =
        Path.of(
            System.getProperty("ducklake.corpus.root")
                ?: "../ducklake-corpus-replay/ducklake/test/sql",
        )

    /** Per-file skips (path → reason) specific to the Doris mirror axis. */
    private val skipList: Map<String, String> =
        mapOf(
            "general/metadata_parameters.test" to
                "custom METADATA_SCHEMA/METADATA_PARAMETERS not threaded to the FE catalog properties",
            "metadata/ducklake_settings.test" to
                "asserts metadata backend type 'duckdb'; the PG backend-axis rewrite makes it 'postgres' by design",
            "general/metadata_cache.test" to
                "KNOWN BE GAP: DuckLake position-delete parquet uses OPTIONAL columns; BE iceberg reader " +
                "requires REQUIRED (friction log 2026-05-19; REPORT-*delete*-nullability.md)",
            // ---- data_inlining: served now (Stage 1 scalar); these hit known gaps ----
            "data_inlining/data_inlining_encryption.test" to
                "ENCRYPTED DuckLake attach (DuckDB-only parquet encryption); not a Doris read path",
            "data_inlining/data_inlining_per_schema_alter.test" to
                "column DEFAULT values over inlined data (same gap as issue_1135; TODO-read)",
            "data_inlining/data_inlining_update_inline_verification.test" to
                "inlined DELETEs (Stage 2): UPDATE produces inlined file-deletions the connector can't apply",
            "data_inlining/data_inlining_transaction_local_delete.test" to
                "inlined transaction-local deletes (Stage 2 inlined-delete territory)",
            "data_inlining/basic_data_inlining.test" to
                "ORACLE-side: SELECT COUNT(*) FROM GLOB(...) physical-file-count assertions depend on exact " +
                "inlining/flush/checkpoint behavior that differs from the golden text in this harness " +
                "(oracle vs golden, not a Doris read gap) — the lake reads themselves mirror clean",
            "data_inlining/data_inlining_option.test" to
                "ORACLE-side: GLOB(...) physical-file-count assertion vs golden (harness inlining variance)",
            "data_inlining/data_inlining_delete.test" to
                "ORACLE-side: GLOB(...) physical-file-count assertion vs golden (harness inlining variance)",
            // ---- issues/ regressions: map to already-known gaps or non-mirrorable harness tests ----
            "issues/issue_865_update_wrong_result.test" to
                "inlined file-deletes (DATA_INLINING_ROW_LIMIT 10): UPDATE mixing a committed delete file " +
                "with inlined file-deletions — the known inlined-delete gap (friction 2026-05-19)",
            "issues/issue_1135.test" to
                "column DEFAULT values not surfaced: ADD COLUMN ... DEFAULT 42 should backfill old rows; " +
                "DuckLake stores the default but our read path returns NULL (TODO-read: column defaults)",
            "issues/view_tosql_performance.test" to
                "DuckLake view + duckdb_views(): views are not surfaced by the connector (dialect skip)",
            "issues/corrupted_catalog_fault_isolation.test" to
                "fault-injection harness test (deliberately corrupts the catalog / expects DuckDB-specific " +
                "error text) — not mirrorable against an external engine",
            // ---- REAL GAPS the mirror found on full-corpus contact (TODO-read items; un-skip when fixed) ----
            "add_files/add_files_hive.test" to GAP_HIVE_PARTITION_FILL,
            "add_files/add_files_hive_many_columns.test" to GAP_HIVE_PARTITION_FILL,
            "add_files/add_files_hive_partition_cast.test" to GAP_HIVE_PARTITION_FILL,
            // Renamed columns + simple add_files legacy files read correctly via the
            // field-id schema dictionary (DuckLakeSchemaDictionary). These two remain
            // UNSOLVABLE with a table-level dictionary: they register id-less parquet
            // files whose physical column names collide across a DROP+re-ADD field-id
            // boundary (a file's `col2` means the DROPPED field-id 2; the live table's
            // `col2` is a NEW field-id 4). Disambiguating requires PER-FILE schema info
            // (which mapping_id applies to which file) — but the SPI schema dictionary
            // is scan-node-level (one for all files) and the BE applies the table
            // name-fallback uniformly. Conflict-aware union already prevents the SILENT
            // wrong-rows mis-bind; the BE then errors loudly ("name_mapping must be
            // set") rather than returning garbage. Real fix needs per-range schema info
            // in the SPI or the BE consulting DuckLake's per-file mapping_id — tracked
            // in the friction log + TODO-read.
            "add_files/add_files.test" to
                "per-file mapping unsolvable table-level: DROP+re-ADD reuses physical name `col2` across " +
                "field-id boundary over id-less files; needs per-range schema info (see friction log)",
            "delete/delete_legacy_missing_mapping_after_rename_add_files.test" to
                "per-file mapping unsolvable table-level: legacy id-less file after rename+add_files (same " +
                "root cause as add_files.test)",
            "stats/filter_stress.test" to
                "oracle-local cross-check: compares the lake against a non-lake DuckDB reference table " +
                "(events_ref) via EXCEPT ALL; the reference table only exists oracle-side, so it can't be " +
                "mirrored (not a Doris read gap)",
            // ---- runner-side (not ours) ----
            "add_files/add_files_type_check_timestamp.test" to
                "ORACLE golden gap: GoldenComparator renders micros only, golden expects timestamp_ns nanos " +
                "(runner-side; reported to the runner owner)",
        )

    @BeforeAll
    fun setUp() {
        val ducklakeOk =
            runCatching {
                DriverManager.getConnection("jdbc:duckdb:").use { c ->
                    c.createStatement().use {
                        it.execute("INSTALL ducklake")
                        it.execute("LOAD ducklake")
                        it.execute("INSTALL postgres")
                        it.execute("LOAD postgres")
                    }
                }
            }.isSuccess
        assumeTrue(ducklakeOk, "ducklake/postgres duckdb extensions not installable (network?)")
        assumeTrue(Files.isDirectory(corpusRoot), "corpus submodule not initialized at $corpusRoot")
        assumeTrue(feAlive(), "no live Doris FE at 127.0.0.1:9030 — run compose/smoke.sh --up-only")
        assumeTrue(pgAlive(), "substrate PostgreSQL not host-mapped at localhost:9432 — is the compose up?")
        sweepLeftoverCorpusDatabases()
    }

    /** Best-effort cleanup of corpus_doris_* leftovers from prior aborted runs (PG bloat control). */
    private fun sweepLeftoverCorpusDatabases() {
        runCatching {
            DriverManager.getConnection(
                System.getProperty("doris.corpus.pg.host.url", "jdbc:postgresql://localhost:9432/ducklake"),
                System.getProperty("doris.corpus.pg.user", "ducklake"),
                System.getProperty("doris.corpus.pg.password", "ducklake"),
            ).use { c ->
                val names = mutableListOf<String>()
                c.createStatement().use { st ->
                    st.executeQuery("SELECT datname FROM pg_database WHERE datname LIKE 'corpus_doris_%'").use { rs ->
                        while (rs.next()) names += rs.getString(1)
                    }
                }
                for (db in names) {
                    runCatching { c.createStatement().use { it.execute("DROP DATABASE IF EXISTS $db WITH (FORCE)") } }
                }
            }
        }
    }

    private fun feAlive(): Boolean =
        runCatching {
            DriverManager.getConnection(
                System.getProperty("doris.corpus.fe.url", "jdbc:mysql://127.0.0.1:9030/?user=root"),
            ).use { c -> c.createStatement().use { it.executeQuery("SELECT 1").next() } }
        }.getOrDefault(false)

    private fun pgAlive(): Boolean =
        runCatching {
            DriverManager.getConnection(
                System.getProperty("doris.corpus.pg.host.url", "jdbc:postgresql://localhost:9432/ducklake"),
                System.getProperty("doris.corpus.pg.user", "ducklake"),
                System.getProperty("doris.corpus.pg.password", "ducklake"),
            ).use { c -> c.createStatement().use { it.executeQuery("SELECT 1").next() } }
        }.getOrDefault(false)

    @Test
    fun corpusLakeReadsMirrorThroughDoris() {
        val discovery = CorpusRunner(corpusRoot, skipList)
        val dirsProp = System.getProperty("ducklake.corpus.dirs", "general,catalog,time_travel,view")
        val files =
            if (dirsProp == "all") {
                discovery.discover()
            } else {
                dirsProp.split(',').flatMap { discovery.discover(it.trim()) }
            }
        assertThat(files).isNotEmpty()

        // Fresh engine per chunk: each per-file FE catalog carries a HikariCP
        // pool inside the plugin; chunking bounds PG connection usage and lets
        // close() reap catalogs + corpus databases incrementally.
        val results = mutableListOf<FileResult>()
        for (chunk in files.chunked(CHUNK_SIZE)) {
            DorisReplayEngine().use { engine ->
                val runner =
                    CorpusRunner(
                        corpusRoot,
                        skipList,
                        engine = engine,
                        metadataRewriter = engine.metadataRewriter,
                    )
                results += runner.run(chunk).files
            }
        }
        val report = CorpusReport(results)
        println(report.summary(maxFailures = MAX_FAILURES_SHOWN))

        assertThat(report.failures)
            .withFailMessage { "doris corpus-mirror failures:\n" + report.summary(maxFailures = MAX_FAILURES_SHOWN) }
            .isEmpty()
        assertThat(report.crashes)
            .withFailMessage { "harness crashes:\n" + report.summary(maxFailures = MAX_FAILURES_SHOWN) }
            .isEmpty()
    }

    companion object {
        private const val CHUNK_SIZE = 30
        private const val MAX_FAILURES_SHOWN = 40
        private const val GAP_HIVE_PARTITION_FILL =
            "KNOWN GAP: hive-layout add_files partition columns are not constant-filled from partition " +
                "values on the Doris scan (parquet body lacks them; TODO-read item)"
        private const val GAP_NAME_MAPPING =
            "KNOWN GAP: ducklake_name_mapping (add_files field-id mapping / renamed columns over legacy " +
                "files) not applied on the Doris scan (TODO-read item)"
    }
}
