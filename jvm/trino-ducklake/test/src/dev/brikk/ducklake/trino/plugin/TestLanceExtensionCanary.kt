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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

/**
 * O3 version-pin canary for the `lance` DuckDB extension (HANDOFF-lance-route-a §O3).
 *
 * lance-duckdb ships ~daily and has already churned our surface once (`lance_scan` was renamed
 * `__lance_scan`). A hard INSTALL-time pin is NOT possible: extensions.duckdb.org hosts only the
 * latest build per (DuckDB version, platform) — DuckDB 1.5.3 parses `INSTALL lance VERSION '…'`
 * but the versioned URLs 404 for both the semver and the build hash (verified 2026-06-11). Pinning
 * for real would mean vendoring per-platform binaries the way `trino_parity` is bundled. So the
 * executors deliberately INSTALL floating/latest, and THIS test is the tripwire that makes churn
 * loud instead of mysterious.
 *
 * It `FORCE INSTALL`s lance — bypassing the sticky `~/.duckdb` cache so it observes the build the
 * repo serves *today*, not whatever was cached at first install (side effect: the local cache is
 * refreshed; DuckDB swaps the file with an atomic rename, so concurrently running lance tests are
 * unaffected) — then asserts, in order:
 *
 *  1. **Signatures** ([duckdb_functions]) — the exact call shapes the connector renders survive:
 *     `__lance_scan('<path>')` (read path + `add_files` validation), and the search functions'
 *     positional prefixes + the named args `k` / `prefilter` / `alpha` rendered by
 *     [LanceSearchSplitProcessor.renderExtraArgsSql]. New *optional* upstream params do not trip
 *     these — they only need OUR usage to keep working.
 *  2. **Behavior** — a raw `COPY … (FORMAT lance)` (the [DuckDbArrowStreamFileWriter] contract),
 *     `__lance_scan` read-back, and one call each of vector / FTS / hybrid search.
 *  3. **The pin** — the served `extension_version` equals [PINNED_LANCE_EXTENSION_VERSION], last.
 *
 * **When this fails:** if only the final version assert trips, the surface above it was verified
 * against the new build — just bump [PINNED_LANCE_EXTENSION_VERSION] to the reported hash. If a
 * signature/behavior assert trips, upstream churn hit our surface: fix the render sites
 * ([LanceSearchProcessor], [DucklakePageSourceProvider], [DuckDbArrowStreamFileWriter],
 * [DucklakeAddFilesProcedure]) before touching the pin.
 *
 * Skips when the extension can't be downloaded (offline, or unsupported platform — 404 on
 * osx_amd64). Unlike the other lance tests, a warm local cache does not un-skip this one: the
 * FORCE download is the point.
 */
class TestLanceExtensionCanary {

    @Test
    fun lanceExtensionSurfaceMatchesVerifiedPin(@TempDir tempDir: Path) {
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            assumeFreshLanceInstall(connection)
            connection.createStatement().use { statement ->
                val servedVersion = queryString(statement,
                        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'lance'")

                assertScanAndSearchSignatures(statement, servedVersion)
                assertCopyScanAndSearchBehavior(statement, tempDir, servedVersion)

                assertThat(servedVersion)
                        .describedAs("lance extension build served by extensions.duckdb.org. The signature and "
                                + "behavior checks above already passed against it, so the connector surface "
                                + "survived — update PINNED_LANCE_EXTENSION_VERSION to this hash.")
                        .isEqualTo(PINNED_LANCE_EXTENSION_VERSION)
            }
        }
    }

    /** Every DuckDB call shape the connector renders, asserted against `duckdb_functions()`. */
    private fun assertScanAndSearchSignatures(statement: Statement, servedVersion: String) {
        // Read path + add_files validation: __lance_scan('<dataset dir>') — path-only positional.
        assertSignaturePresent(statement, servedVersion, "__lance_scan",
                "parameter_types[1] = 'VARCHAR'")
        // lance_vector_search('<path>', '<column>', [..]::DOUBLE[], k := n, prefilter := b)
        assertSignaturePresent(statement, servedVersion, "lance_vector_search",
                "parameter_types[1] = 'VARCHAR' AND parameter_types[2] = 'VARCHAR' "
                        + "AND parameter_types[3] = 'DOUBLE[]' "
                        + "AND list_contains(parameters, 'k') AND list_contains(parameters, 'prefilter')")
        // lance_fts('<path>', '<column>', '<query>', k := n, prefilter := b)
        assertSignaturePresent(statement, servedVersion, "lance_fts",
                "parameter_types[1] = 'VARCHAR' AND parameter_types[2] = 'VARCHAR' "
                        + "AND parameter_types[3] = 'VARCHAR' "
                        + "AND list_contains(parameters, 'k') AND list_contains(parameters, 'prefilter')")
        // lance_hybrid_search('<path>', '<vec col>', [..]::DOUBLE[], '<text col>', '<query>',
        //                     k := n, alpha := a, prefilter := b)
        assertSignaturePresent(statement, servedVersion, "lance_hybrid_search",
                "parameter_types[1] = 'VARCHAR' AND parameter_types[2] = 'VARCHAR' "
                        + "AND parameter_types[3] = 'DOUBLE[]' "
                        + "AND parameter_types[4] = 'VARCHAR' AND parameter_types[5] = 'VARCHAR' "
                        + "AND list_contains(parameters, 'k') AND list_contains(parameters, 'alpha') "
                        + "AND list_contains(parameters, 'prefilter')")
    }

    /** Live COPY → scan → search round-trip over a 3-row dataset in [tempDir]. */
    private fun assertCopyScanAndSearchBehavior(statement: Statement, tempDir: Path, servedVersion: String) {
        val dataset = tempDir.resolve("canary.lance").toAbsolutePath().toString().replace("'", "''")
        statement.execute("COPY (SELECT * FROM (VALUES "
                + "(1, 'the quick brown fox jumps', [1.0, 0.0]::FLOAT[2]), "
                + "(2, 'lazy dogs sleep all day',   [0.0, 1.0]::FLOAT[2]), "
                + "(3, 'quick silver foxes run',    [0.7, 0.3]::FLOAT[2])) t(id, body, emb)) "
                + "TO '$dataset' (FORMAT lance)")

        assertThat(queryIds(statement, "SELECT id FROM __lance_scan('$dataset') ORDER BY id"))
                .describedAs("COPY (FORMAT lance) → __lance_scan round-trip (lance build $servedVersion)")
                .containsExactly(1L, 2L, 3L)
        assertThat(queryIds(statement, "SELECT id FROM lance_vector_search("
                + "'$dataset', 'emb', [0.9, 0.1]::DOUBLE[], k := 1, prefilter := false)"))
                .describedAs("lance_vector_search named-arg call (lance build $servedVersion)")
                .containsExactly(1L)
        assertThat(queryIds(statement, "SELECT id FROM lance_fts("
                + "'$dataset', 'body', 'quick fox', k := 2, prefilter := false) ORDER BY id"))
                .describedAs("lance_fts named-arg call (lance build $servedVersion)")
                .containsExactly(1L, 3L)
        assertThat(queryIds(statement, "SELECT id FROM lance_hybrid_search("
                + "'$dataset', 'emb', [0.9, 0.1]::DOUBLE[], 'body', 'quick', "
                + "k := 2, alpha := 0.5, prefilter := false) ORDER BY id"))
                .describedAs("lance_hybrid_search named-arg call (lance build $servedVersion)")
                .containsExactly(1L, 3L)
    }

    private fun assertSignaturePresent(
            statement: Statement,
            servedVersion: String,
            functionName: String,
            predicateSql: String) {
        val overloads = queryLong(statement,
                "SELECT count(*) FROM duckdb_functions() "
                        + "WHERE function_name = '$functionName' AND function_type = 'table' AND $predicateSql")
        assertThat(overloads)
                .describedAs("overloads of $functionName satisfying [$predicateSql] in lance build "
                        + "$servedVersion — upstream churn hit a call shape the connector renders")
                .isPositive()
    }

    private fun assumeFreshLanceInstall(connection: Connection) {
        try {
            connection.createStatement().use { statement ->
                statement.execute("FORCE INSTALL lance")
                statement.execute("LOAD lance")
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "lance DuckDB extension not downloadable (offline / unsupported platform — "
                    + "404 on osx_amd64; the canary FORCE-downloads to observe the currently served build, "
                    + "so a warm local cache does not un-skip it): ${e.message}")
        }
    }

    private fun queryString(statement: Statement, sql: String): String =
        statement.executeQuery(sql).use { rs ->
            check(rs.next()) { "no row from: $sql" }
            rs.getString(1)
        }

    private fun queryLong(statement: Statement, sql: String): Long =
        statement.executeQuery(sql).use { rs ->
            check(rs.next()) { "no row from: $sql" }
            rs.getLong(1)
        }

    private fun queryIds(statement: Statement, sql: String): List<Long> =
        statement.executeQuery(sql).use { rs ->
            buildList {
                while (rs.next()) {
                    add(rs.getLong(1))
                }
            }
        }

    companion object {
        /**
         * The lance-duckdb build (its `extension_version`, a git short hash) this connector's
         * surface was last verified against, on DuckDB v1.5.3. Bump ONLY after the rest of this
         * test passes against the new build — see the class doc for the workflow.
         */
        private const val PINNED_LANCE_EXTENSION_VERSION = "533e0ee"
    }
}
