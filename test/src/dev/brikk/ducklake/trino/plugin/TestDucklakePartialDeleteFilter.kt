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
 * Cross-engine correctness of the partial DELETE-file read filter. DuckLake consolidates deletions
 * recorded across several snapshots into ONE parquet delete file (`file_path, pos,
 * _ducklake_internal_snapshot_id`) with `begin_snapshot` = first deletion's snapshot and
 * `partial_max` = last deletion's snapshot. A time-travel read at snapshot S must apply only the
 * deletions whose `_ducklake_internal_snapshot_id <= S` (the connector filters them via
 * [DucklakeSplit.deleteFileSnapshotFilters]).
 *
 * Setup drives DuckDB directly to produce the consolidated delete file (probe-verified shape:
 * two deletes at adjacent snapshots, consolidated by `flush_inlined_data`), then Trino reads.
 *
 * SAME_THREAD: single shared local catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePartialDeleteFilter : AbstractTestQueryFramework() {

    private lateinit var rootDir: Path
    private var firstDeleteSnapshot: Long = -1
    private var partialMax: Long = -1

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        rootDir = Files.createTempDirectory("ducklake-pdel-")
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
                st.execute("CALL ducklake_flush_inlined_data('lake')")     // materialize to a parquet data file
                st.execute("DELETE FROM test_schema.t WHERE id = 1")        // deletion @ snapshot A
                st.execute("DELETE FROM test_schema.t WHERE id = 5")        // deletion @ snapshot B
                st.execute("CALL ducklake_flush_inlined_data('lake')")     // consolidate into one partial delete file
            }
        }
        DriverManager.getConnection("jdbc:duckdb:$lakeDb").use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery("SELECT begin_snapshot, partial_max FROM ducklake_delete_file "
                        + "WHERE partial_max IS NOT NULL ORDER BY delete_file_id LIMIT 1").use { rs ->
                    check(rs.next()) { "delete consolidation did not produce a partial delete file" }
                    firstDeleteSnapshot = rs.getLong(1)
                    partialMax = rs.getLong(2)
                }
            }
        }
        check(partialMax > firstDeleteSnapshot) { "expected partial_max ($partialMax) > begin ($firstDeleteSnapshot)" }

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
    fun latestAppliesAllDeletions() {
        // Both deletions are <= latest → id 1 and 5 gone, 8 rows remain.
        assertThat(computeScalar("SELECT count(*) FROM t")).isEqualTo(8L)
        assertThat(computeActual("SELECT id FROM t WHERE id IN (1, 5)").materializedRows).isEmpty()
    }

    @Test
    fun timeTravelToFirstDeleteAppliesOnlyEarlierDeletion() {
        // AS OF the first deletion's snapshot: only id=1 is deleted; id=5's deletion (a later
        // snapshot) must NOT apply, so 9 rows remain and id=5 is still present.
        assertThat(computeScalar("SELECT count(*) FROM t FOR VERSION AS OF $firstDeleteSnapshot"))
                .`as`("only the first deletion applies at its snapshot")
                .isEqualTo(9L)
        assertThat(computeActual("SELECT id FROM t FOR VERSION AS OF $firstDeleteSnapshot WHERE id = 5")
                .materializedRows.map { it.getField(0) as Int })
                .`as`("id=5 not yet deleted at the first deletion's snapshot")
                .containsExactly(5)
        assertThat(computeActual("SELECT id FROM t FOR VERSION AS OF $firstDeleteSnapshot WHERE id = 1")
                .materializedRows)
                .`as`("id=1 already deleted at this snapshot")
                .isEmpty()
    }

    @Test
    fun timeTravelToPartialMaxAppliesAllDeletions() {
        assertThat(computeScalar("SELECT count(*) FROM t FOR VERSION AS OF $partialMax")).isEqualTo(8L)
    }
}
