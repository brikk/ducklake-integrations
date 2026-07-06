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

    /**
     * Per-file skips: upstream's own PostgreSQL-axis skip set (their PG CI
     * config — includes the PG-locking files that HANG a replay) plus
     * Trino-mirror-specific entries.
     */
    private val skipList: Map<String, String> =
        dev.brikk.ducklake.corpus.PostgresAxisSkips.SKIPS + mapOf(
            "metadata/appender_variant_stats.test" to
                "VARIANT-typed parquet not readable by the Trino parquet reader (F10 territory)",
            "add_files/add_files_type_check_timestamp.test" to
                "duckdb-jdbc truncates TIMESTAMP_NS to micros (oracle-side limitation; also " +
                "a ms/us rendering ordering nuance under investigation)",
            "data_inlining/data_inlining_encryption.test" to ENCRYPTED_LAKE,
            "encryption/encryption.test" to ENCRYPTED_LAKE,
            "geo/ducklake_geometry.test" to GEO_WKB,
            "geo/ducklake_geometry_add_files.test" to GEO_WKB,
            "geo/ducklake_geometry_inlining.test" to GEO_WKB,
            "geo/ducklake_geometry_merge.test" to GEO_WKB,
            "default/struct_field_default.test" to
                "BUG: nested struct-FIELD initial defaults not projected (initial_default on " +
                "catalog CHILD rows — 4th path of the issue-1135 family; tracked in TODO-READ-MODE)",
            // ---- REAL BUGS round 2 (tracked in TODO-READ-MODE; un-skip when fixed) ----
            "add_files/add_files_hive.test" to BUG_HIVE_ADD_FILES,
            "add_files/add_files_hive_mismatch.test" to BUG_HIVE_ADD_FILES,
            "add_files/add_files_hive_partition_cast.test" to BUG_HIVE_ADD_FILES,
            "add_files/add_files.test" to
                "BUG: name-map must be authoritative for mapped files — name-first matching " +
                "resurrects a dead re-added column's physical data (tracked in TODO-READ-MODE)",
            // (2026-07-06: the inlined struct/map crash family was FIXED — nested
            // text parsing in DucklakeInlinedValueConverter — and its 10 repro
            // files un-skipped.)
            "types/json.test" to
                "JSON degraded-type rendering differs between engines (F8 territory)",
            "types/floats.test" to
                "float golden text relies on DuckDB inf/nan casts + implicit varchar comparisons (dialect)",
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
        private const val BUG_HIVE_ADD_FILES =
            "BUG: DuckDB hive add_files partition columns read NULL — is_partition name-map " +
                "entries carry values in the file PATH (tracked in TODO-READ-MODE)"
        private const val ENCRYPTED_LAKE =
            "encrypted lake: parquet encryption keys not threaded to the Trino reader on the " +
                "mirror axis (footer unreadable)"
        private const val GEO_WKB =
            "GEOMETRY renders WKB blob text vs DuckDB's WKT (degraded type; same skip as the " +
                "identity-control list)"
    }
}
