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
 * Change-feed correctness over a CONSOLIDATED ("partial") DELETE file — the delete-side analog of
 * the insert-side partial_max fix. DuckLake's delete consolidation (flush_inlined_data) folds
 * deletions recorded at several snapshots into ONE 3-column delete file (`file_path, pos,
 * _ducklake_internal_snapshot_id`) whose `begin_snapshot` = MIN(deletion snapshots) and
 * `partial_max` = MAX. The change feed must report each deletion at its OWN snapshot and window
 * accordingly — not attribute the whole batch to `begin_snapshot`.
 *
 * Recipe (probe-verified, same as TestDucklakePartialDeleteFilter): two deletes at adjacent
 * snapshots, consolidated by flush_inlined_data into one partial delete file.
 *
 * SAME_THREAD: single shared local catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeChangeFeedPartialDelete : AbstractTestQueryFramework() {

    private lateinit var rootDir: Path
    private var delSnapA: Long = -1 // first deletion (id=1) — begin_snapshot of the consolidated file
    private var delSnapB: Long = -1 // second deletion (id=5) — partial_max

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        rootDir = Files.createTempDirectory("ducklake-cf-pdel-")
        val lakeDb = rootDir.resolve("lake.db").toAbsolutePath().toString()
        val dataDir = rootDir.resolve("data").toAbsolutePath().toString()

        DriverManager.getConnection("jdbc:duckdb:").use { conn ->
            conn.createStatement().use { st ->
                st.execute("INSTALL ducklake")
                st.execute("LOAD ducklake")
                st.execute("ATTACH 'ducklake:$lakeDb' AS lake (DATA_PATH '$dataDir/')")
                st.execute("USE lake")
                st.execute("CREATE SCHEMA test_schema")
                st.execute("CREATE TABLE test_schema.t AS SELECT i::INTEGER AS id FROM range(10) t(i)")
                st.execute("CALL ducklake_flush_inlined_data('lake')")   // materialize to a data file
                st.execute("DELETE FROM test_schema.t WHERE id = 1")      // deletion @ snapshot A
                st.execute("DELETE FROM test_schema.t WHERE id = 5")      // deletion @ snapshot B
                st.execute("CALL ducklake_flush_inlined_data('lake')")   // consolidate into one partial delete file
            }
        }
        DriverManager.getConnection("jdbc:duckdb:$lakeDb").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT begin_snapshot, partial_max FROM ducklake_delete_file "
                        + "WHERE partial_max IS NOT NULL ORDER BY delete_file_id LIMIT 1").use { rs ->
                    check(rs.next()) { "delete consolidation did not produce a partial delete file" }
                    delSnapA = rs.getLong(1)
                    delSnapB = rs.getLong(2)
                }
            }
        }
        check(delSnapB > delSnapA) { "expected partial_max ($delSnapB) > begin ($delSnapA)" }

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

    private fun deletions(from: Long, to: Long): List<Pair<Long, Int>> =
            computeActual("SELECT snapshot_id, id FROM " +
                    "TABLE(ducklake.system.table_deletions('test_schema', 't', $from, $to)) ORDER BY id")
                    .materializedRows
                    .map { (it.getField(0) as Long) to (it.getField(1) as Int) }

    @Test
    fun eachDeletionAttributedToItsOwnSnapshot() {
        // Over the full window both deletions appear, each at its OWN snapshot (A for id=1, B for
        // id=5) — NOT both at begin_snapshot (A).
        val all = deletions(1, delSnapB)
        assertThat(all).containsExactlyInAnyOrder(delSnapA to 1, delSnapB to 5)
    }

    @Test
    fun windowExcludingFirstDeletionDropsIt() {
        // Window [A+1, B]: only id=5's deletion (snapshot B) qualifies; id=1's deletion (snapshot A
        // < start) must be excluded even though it lives in the same consolidated file whose
        // begin_snapshot == A.
        val rows = deletions(delSnapA + 1, delSnapB)
        assertThat(rows).containsExactly(delSnapB to 5)
    }

    @Test
    fun windowOnlyFirstDeletionExcludesLater() {
        // Window [A, A]: only id=1's deletion; id=5's (snapshot B > end) must be excluded.
        val rows = deletions(delSnapA, delSnapA)
        assertThat(rows).containsExactly(delSnapA to 1)
    }
}
