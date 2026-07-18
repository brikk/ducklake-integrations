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
import java.sql.DriverManager

/**
 * Change-feed correctness over a cross-snapshot compacted ("partial") data file — the insert-side
 * `partial_max` fix (R6). DuckLake's `merge_adjacent_files` merges rows from several source
 * snapshots into ONE Parquet file whose `begin_snapshot` is only the MIN origin and `partial_max`
 * the MAX; each row physically carries its origin `_ducklake_internal_snapshot_id`.
 *
 * Before the fix, the change feed stamped EVERY merged row with the file's `begin_snapshot`, so
 * `table_insertions` / `table_changes` mis-attributed all rows to one snapshot and could include or
 * exclude rows on the wrong side of the (inclusive) window boundary. This test drives DuckDB to
 * produce the partial file, then asserts Trino attributes each row to its true origin snapshot and
 * windows correctly. Mirrors upstream DuckLake / datafusion-ducklake #185.
 *
 * Self-contained local DuckLake catalog (same recipe as TestDucklakePartialFileFilter).
 *
 * SAME_THREAD: single shared local catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeChangeFeedPartialFile : AbstractTestQueryFramework() {

    private lateinit var rootDir: Path
    private var snap1: Long = -1 // snapshot of the first insert (100 's1' rows)
    private var snap2: Long = -1 // snapshot of the second insert (100 's2' rows)

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        rootDir = Files.createTempDirectory("ducklake-cf-partial-")
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
        // Read back the partial file's begin_snapshot / partial_max: begin == the first insert's
        // snapshot, partial_max == the second insert's snapshot (each row tagged with its origin).
        DriverManager.getConnection("jdbc:duckdb:$lakeDb").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT begin_snapshot, partial_max FROM ducklake_data_file "
                        + "WHERE partial_max IS NOT NULL ORDER BY data_file_id LIMIT 1").use { rs ->
                    check(rs.next()) { "merge_adjacent_files did not produce a partial file" }
                    snap1 = rs.getLong(1)
                    snap2 = rs.getLong(2)
                }
            }
        }
        check(snap2 > snap1) { "expected partial_max ($snap2) > begin ($snap1)" }

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

    private fun insertions(from: Long, to: Long): List<Pair<Int, String>> =
            computeActual("SELECT snapshot_id, id, data FROM " +
                    "TABLE(ducklake.system.table_insertions('test_schema', 't', $from, $to)) ORDER BY id")
                    .materializedRows
                    .map { (it.getField(0) as Long).toInt() to (it.getField(2) as String) }

    @Test
    fun eachRowAttributedToItsOriginSnapshotNotBeginSnapshot() {
        // Over the FULL window both source snapshots' rows appear, and — crucially — each row's
        // reported snapshot_id is its OWN origin, not the merged file's begin_snapshot (snap1 for
        // all). Before the fix every 's2' row reported snap1.
        val all = computeActual("SELECT snapshot_id, data FROM " +
                "TABLE(ducklake.system.table_insertions('test_schema', 't', 1, $snap2))")
                .materializedRows
                .map { (it.getField(0) as Long) to (it.getField(1) as String) }
        assertThat(all).hasSize(200)
        // 's1' rows carry snap1; 's2' rows carry snap2 (NOT snap1).
        assertThat(all.filter { it.second == "s1" }.map { it.first }.distinct()).containsExactly(snap1)
        assertThat(all.filter { it.second == "s2" }.map { it.first }.distinct()).containsExactly(snap2)
    }

    @Test
    fun windowToFirstSnapshotExcludesLaterOriginRows() {
        // Window [1, snap1]: only the first insert's rows are in range. The second insert's rows
        // (origin snap2 > snap1) must be DROPPED even though they live in the same physical file
        // whose begin_snapshot == snap1. Before the fix they leaked in (all rows stamped snap1).
        val rows = insertions(1, snap1)
        assertThat(rows.map { it.second }.distinct()).containsExactly("s1")
        assertThat(rows).hasSize(100)
        assertThat(rows.map { it.first }.distinct()).containsExactly(snap1.toInt())
    }

    @Test
    fun windowToSecondSnapshotOnlyIncludesLaterOriginRows() {
        // Window [snap1 + 1, snap2]: only the SECOND insert's rows qualify. The first insert's rows
        // (origin snap1 < start) must be excluded even though the file's begin_snapshot == snap1.
        val rows = insertions(snap1 + 1, snap2)
        assertThat(rows.map { it.second }.distinct()).containsExactly("s2")
        assertThat(rows).hasSize(100)
        assertThat(rows.map { it.first }.distinct()).containsExactly(snap2.toInt())
        // The ids are exactly the second insert's (100..199).
        assertThat(rows.map { it.first == snap2.toInt() }.all { it }).isTrue()
        assertThat(rows.map { it.second }.all { it == "s2" }).isTrue()
    }

    @Test
    fun tableChangesWindowsPartialFileByOrigin() {
        // table_changes over the whole history: 200 pure inserts, each with its origin snapshot.
        val changes = computeActual("SELECT change_type, snapshot_id, data FROM " +
                "TABLE(ducklake.system.table_changes('test_schema', 't', 1, $snap2))")
                .materializedRows
        assertThat(changes).hasSize(200)
        assertThat(changes.map { it.getField(0) as String }.distinct()).containsExactly("insert")
        // Sub-window to the second snapshot only: no first-insert rows.
        val second = computeActual("SELECT DISTINCT data FROM " +
                "TABLE(ducklake.system.table_changes('test_schema', 't', ${snap1 + 1}, $snap2))")
                .materializedRows
                .map { it.getField(0) as String }
        assertThat(second).containsExactly("s2")
    }
}
