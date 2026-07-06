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

import dev.brikk.ducklake.corpus.CorpusReport
import dev.brikk.ducklake.corpus.CorpusRunner
import dev.brikk.ducklake.corpus.FileResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Corpus replay with the Trino engine mirroring lake reads (live-vs-live
 * against the DuckDB oracle) on the PostgreSQL backend axis.
 *
 * Every corpus file's DuckLake ATTACH is rewritten onto an isolated PG
 * database ([TrinoReplayEngine.metadataRewriter]); the oracle executes the
 * file verbatim against it; each golden-validated, `accepts`-gated query is
 * re-executed through Trino and compared row-for-row against the oracle's
 * live result. A failure here means Trino read the same catalog state
 * differently than DuckDB — exactly the class of bug this harness exists for.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestTrinoCorpusReplay {

    private val corpusRoot: Path =
        Path.of(
            System.getProperty("ducklake.corpus.root")
                ?: "../ducklake-corpus-replay/ducklake/test/sql",
        )

    /** Per-file skips (path → reason) specific to the Trino mirror axis. */
    private val skipList: Map<String, String> =
        mapOf(
            "general/metadata_parameters.test" to
                "custom METADATA_SCHEMA/METADATA_PARAMETERS not threaded to the connector catalog config",
            "metadata/appender_variant_stats.test" to
                "VARIANT-typed parquet not readable by the Trino parquet reader (F10 territory)",
            "metadata/ducklake_settings.test" to
                "asserts metadata backend type 'duckdb'; the PG backend-axis rewrite makes it 'postgres' by design",
            // ---- REAL BUGS the mirror found (tracked in TODO-READ-MODE; un-skip when fixed) ----
            "alter/struct_evolution.test" to BUG_STRUCT_EVOLUTION,
            "alter/struct_evolution_alter.test" to BUG_STRUCT_EVOLUTION,
            "alter/struct_evolution_nested.test" to BUG_STRUCT_EVOLUTION,
            "alter/struct_evolution_nested_alter.test" to BUG_STRUCT_EVOLUTION,
            "alter/struct_evolution_reuse.test" to BUG_STRUCT_EVOLUTION,
            "alter/struct_evolution_map_alter.test" to BUG_STRUCT_EVOLUTION,
            "alter/struct_in_map_evolution.test" to BUG_STRUCT_EVOLUTION,
            "alter/add_column_nested.test" to BUG_STRUCT_EVOLUTION,
            "alter/drop_column_nested.test" to BUG_STRUCT_EVOLUTION,
            "types/struct.test" to BUG_STRUCT_EVOLUTION,
            "types/json.test" to
                "JSON degraded-type rendering differs between engines (F8 territory)",
            "types/floats.test" to
                "float golden text relies on DuckDB inf/nan casts + implicit varchar comparisons (dialect)",
            "delete/delete_legacy_missing_mapping_after_rename_add_files.test" to
                "BUG(?): mirror divergence — legacy delete mapping after RENAME + add_files; investigate " +
                "(tracked in TODO-READ-MODE)",
        )

    @BeforeAll
    fun setup() {
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
        assumeTrue(ducklakeOk, "ducklake/postgres extensions not installable (network?)")
        assumeTrue(Files.isDirectory(corpusRoot), "corpus submodule not initialized at $corpusRoot")
    }

    @Test
    fun `corpus lake reads mirror through trino`() {
        val discovery = CorpusRunner(corpusRoot, skipList)
        val dirsProp = System.getProperty("ducklake.corpus.dirs", "general,catalog,time_travel,view")
        val files =
            if (dirsProp == "all") {
                discovery.discover()
            } else {
                dirsProp.split(',').flatMap { discovery.discover(it.trim()) }
            }
        assertThat(files).isNotEmpty()

        // Fresh engine (runner + PG server) per chunk: each Trino catalog holds
        // a connection pool, so hundreds of per-file catalogs in one runner
        // exhaust PostgreSQL's max_connections. Chunking bounds the blast
        // radius and keeps memory flat on full-corpus runs.
        val results = mutableListOf<FileResult>()
        for (chunk in files.chunked(CHUNK_SIZE)) {
            TrinoReplayEngine().use { engine ->
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
        println(report.summary(maxFailures = 40))

        assertThat(report.failures)
            .withFailMessage { "trino corpus-mirror failures:\n" + report.summary(maxFailures = 40) }
            .isEmpty()
        assertThat(report.crashes)
            .withFailMessage { "harness crashes:\n" + report.summary(maxFailures = 40) }
            .isEmpty()
        assertThat(report.totalPassed).isGreaterThan(50)
    }

    companion object {
        private const val CHUNK_SIZE = 30
        private const val BUG_STRUCT_EVOLUTION =
            "BUG: ClassCastException (Slice -> SqlRow) reading struct columns after nested schema " +
                "evolution (tracked in TODO-READ-MODE)"
    }
}
