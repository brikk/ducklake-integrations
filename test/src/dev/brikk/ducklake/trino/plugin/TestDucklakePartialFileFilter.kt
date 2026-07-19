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

import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.QueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Cross-engine correctness of the partial-file read filter. DuckLake's `merge_adjacent_files`
 * merges rows from two snapshots into ONE Parquet file that physically carries each row's origin
 * `_ducklake_internal_snapshot_id` and records `partial_max` = the max such id; the source files
 * are removed. A correct time-travel read at snapshot S must therefore drop rows whose internal
 * snapshot id exceeds S (the connector filters them via [DucklakeSplit.snapshotFilterMax]).
 *
 * Setup drives DuckDB directly (local DuckLake catalog) to produce the partial file, then Trino
 * reads it through the native Parquet path. Verified earlier that this DuckDB version produces a
 * partial file with the physical column under this exact recipe.
 *
 * SAME_THREAD: single shared local catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePartialFileFilter : AbstractTestQueryFramework() {

    private lateinit var rootDir: Path
    private var beginSnapshot: Long = -1
    private var partialMax: Long = -1

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        rootDir = Files.createTempDirectory("ducklake-partial-")
        val lakeDb = rootDir.resolve("lake.db").toAbsolutePath().toString()
        val dataDir = rootDir.resolve("data").toAbsolutePath().toString()

        DriverManager.getConnection("jdbc:duckdb:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSTALL ducklake")
                st.execute("LOAD ducklake")
                st.execute("ATTACH 'ducklake:$lakeDb' AS lake (DATA_PATH '$dataDir/')")
                st.execute("USE lake")
                st.execute("CREATE SCHEMA test_schema")
                st.execute("CREATE TABLE test_schema.t (id INTEGER, data VARCHAR)")
                st.execute("CALL ducklake_set_option('lake', 'target_file_size', '1.5KB')")
                st.execute("INSERT INTO test_schema.t SELECT i, 's1' FROM range(100) t(i)")
                st.execute("INSERT INTO test_schema.t SELECT i + 100, 's2' FROM range(100) t(i)")
                // Merge the two adjacent files into one partial file (begin = first insert's
                // snapshot, partial_max = second insert's snapshot).
                st.execute("CALL merge_adjacent_files()")
            }
        }
        // Read back the partial file's begin_snapshot / partial_max (the connector reads the
        // ducklake_* tables straight out of lake.db on the DUCKDB_LOCAL path).
        DriverManager.getConnection("jdbc:duckdb:$lakeDb").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT begin_snapshot, partial_max FROM ducklake_data_file "
                        + "WHERE partial_max IS NOT NULL ORDER BY data_file_id LIMIT 1").use { rs ->
                    check(rs.next()) { "merge_adjacent_files did not produce a partial file" }
                    beginSnapshot = rs.getLong(1)
                    partialMax = rs.getLong(2)
                }
            }
        }
        check(partialMax > beginSnapshot) { "expected partial_max ($partialMax) > begin ($beginSnapshot)" }

        val session = testSessionBuilder().setCatalog("ducklake").setSchema("test_schema").build()
        val runner = DistributedQueryRunner.builder(session).build()
        try {
            runner.installPlugin(DucklakePlugin())
            runner.createCatalog("ducklake", "ducklake", mapOf(
                    "ducklake.catalog.database-url" to "jdbc:duckdb:$lakeDb",
                    "ducklake.data-path" to "$dataDir/",
                    "fs.hadoop.enabled" to "true"))
            return runner
        }
        catch (e: Throwable) {
            runner.close()
            throw e
        }
    }

    @AfterAll
    fun cleanup() {
        if (::rootDir.isInitialized && Files.exists(rootDir)) {
            Files.walk(rootDir).use { w -> w.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }

    @Test
    fun latestReadReturnsAllRows() {
        // partial_max <= latest → no filter; the whole merged file is valid.
        assertThat(computeScalar("SELECT count(*) FROM t")).isEqualTo(200L)
    }

    @Test
    fun timeTravelToFirstInsertFiltersNewerRows() {
        // AS OF the first insert's snapshot: the merged file is active (begin <= S) but partial_max
        // > S, so rows from the second insert (_ducklake_internal_snapshot_id > S) must be dropped.
        assertThat(computeScalar("SELECT count(*) FROM t FOR VERSION AS OF $beginSnapshot"))
                .`as`("only the first insert's rows survive the partial-file filter")
                .isEqualTo(100L)
        // And they are exactly the first-insert rows.
        assertThat(computeActual("SELECT DISTINCT data FROM t FOR VERSION AS OF $beginSnapshot").materializedRows
                .map { it.getField(0) as String })
                .containsExactly("s1")
        assertThat(computeScalar("SELECT max(id) FROM t FOR VERSION AS OF $beginSnapshot")).isEqualTo(99)
    }

    @Test
    fun timeTravelToPartialMaxReturnsAllRows() {
        // AS OF partial_max: every row's snapshot id <= S → no row dropped.
        assertThat(computeScalar("SELECT count(*) FROM t FOR VERSION AS OF $partialMax")).isEqualTo(200L)
    }

    /**
     * Integrity boundary: a data file the catalog marks partial (partial_max set) MUST physically
     * carry `_ducklake_internal_snapshot_id`. If it does not, the read must FAIL LOUD — returning an
     * empty drop-set (the old behavior) would silently include rows newer than the requested
     * snapshot. We simulate the corruption by setting partial_max on a plain (marker-less) file, per
     * the reviewer's suggested proof.
     */
    @Test
    fun partialFileMissingSnapshotMarkerFailsLoud() {
        // Create the marker-less file + its catalog rows through Trino (a plain write emits no
        // _ducklake_internal_snapshot_id column), then poke ONLY the raw ducklake_data_file row to
        // set partial_max above every snapshot id so the partial-file filter engages on a normal read.
        computeActual("CREATE TABLE test_schema.plain_no_marker (id INTEGER)")
        computeActual("INSERT INTO test_schema.plain_no_marker VALUES (1), (2), (3)")
        DriverManager.getConnection("jdbc:duckdb:${rootDir.resolve("lake.db").toAbsolutePath()}").use { conn ->
            conn.createStatement().use { st ->
                st.execute("UPDATE ducklake_data_file SET partial_max = 999999999 "
                        + "WHERE table_id = (SELECT table_id FROM ducklake_table "
                        + "WHERE table_name = 'plain_no_marker' AND end_snapshot IS NULL)")
            }
        }
        org.assertj.core.api.Assertions.assertThatThrownBy {
            computeActual("SELECT * FROM plain_no_marker")
        }
                .hasStackTraceContaining("Partial data file is missing its _ducklake_internal_snapshot_id")
    }
}
