package dev.brikk.ducklake.corpus

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Identity control: replays corpus files through the DuckDB oracle ONLY and
 * validates live results against the files' golden text. Green here means the
 * parser, templating, connection routing, and comparator are faithful — the
 * precondition for trusting any engine adapter comparison.
 *
 * Requires the `ducklake` extension to be installable (network on first run,
 * cached in ~/.duckdb afterwards); the whole class is assumption-skipped when
 * it isn't, mirroring the lance/vortex network-gating convention.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestIdentityControl {

    private val corpusRoot: Path =
        Path.of(
            System.getProperty("ducklake.corpus.root")
                ?: "ducklake/test/sql",
        )

    /**
     * Per-file curated skips with reasons — the long-tail curation surface.
     * Keep reasons precise; every entry is a candidate future fix.
     */
    private val skipList: Map<String, String> =
        mapOf(
            "add_files/add_files_type_check_timestamp.test" to
                "duckdb-jdbc truncates TIMESTAMP_NS to micros (1 golden row expects nanos)",
            "geo/ducklake_geometry.test" to GEO_REASON,
            "geo/ducklake_geometry_add_files.test" to GEO_REASON,
            "geo/ducklake_geometry_inlining.test" to GEO_REASON,
            "geo/ducklake_geometry_merge.test" to GEO_REASON,
        )

    companion object {
        private const val GEO_REASON =
            "GEOMETRY over duckdb-jdbc surfaces raw WKB; golden expects WKT rendering"

        @JvmStatic
        @BeforeAll
        fun requireDucklakeExtension() {
            val ok =
                runCatching {
                    DriverManager.getConnection("jdbc:duckdb:").use { c ->
                        c.createStatement().use {
                            it.execute("INSTALL ducklake")
                            it.execute("LOAD ducklake")
                        }
                    }
                }.isSuccess
            assumeTrue(ok, "ducklake extension not installable (network?) — skipping identity control")
        }
    }

    @Test
    fun `corpus replays clean against golden text`() {
        assumeTrue(Files.isDirectory(corpusRoot), "corpus submodule not initialized at $corpusRoot")
        val runner = CorpusRunner(corpusRoot, skipList)
        // Default: the proven starter set. `-Dducklake.corpus.dirs=all` runs everything;
        // a comma list selects specific corpus subdirectories.
        val dirsProp = System.getProperty("ducklake.corpus.dirs", "general,catalog,comments,time_travel,view,metadata")
        val files =
            if (dirsProp == "all") {
                runner.discover()
            } else {
                dirsProp.split(',').flatMap { runner.discover(it.trim()) }
            }
        assertThat(files).isNotEmpty()

        val report = runner.run(files)
        println(report.summary(maxFailures = 60))

        assertThat(report.failures)
            .withFailMessage { "corpus identity failures:\n" + report.summary(maxFailures = 60) }
            .isEmpty()
        assertThat(report.crashes)
            .withFailMessage { "corpus harness crashes:\n" + report.summary(maxFailures = 60) }
            .isEmpty()
        // Guard against silently skipping everything.
        assertThat(report.totalPassed).isGreaterThan(100)
    }
}
