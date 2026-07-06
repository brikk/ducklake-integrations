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
    }
}
