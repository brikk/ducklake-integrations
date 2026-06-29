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
import java.util.Comparator

/**
 * End-to-end coverage of `rewrite_data_files` — the non-partial / Iceberg-style compaction WRITER
 * (dev-docs/DESIGN-maintenance.md § 7). Full-Trino: a PostgreSQL metadata catalog ATTACHed with a
 * LOCAL data path, the connector's own parquet read+write path (no DuckDB parity extension needed —
 * the v1 writer is non-partial). Asserts file count drops while the row set / values are preserved,
 * time-travel still resolves the pre-compaction files, deletes are physically applied, and the v1
 * gates (partitioned reject, below-count no-op) hold.
 *
 * SAME_THREAD: writes to one shared catalog.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeRewriteDataFiles : AbstractTestQueryFramework() {

    private lateinit var dataDir: Path

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        val pg = DucklakeTestCatalogEnvironment.getServer()
        val dbName = "ducklake_rewrite_data_files_e2e"
        dataDir = Files.createTempDirectory("ducklake-rewrite-")
        pg.createDatabase(dbName)
        DriverManager.getConnection("jdbc:duckdb:").use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("INSTALL postgres")
                stmt.execute("LOAD postgres")
                stmt.execute("INSTALL ducklake")
                stmt.execute("LOAD ducklake")
                stmt.execute("ATTACH '" + pg.getDuckDbAttachUri(dbName) + "' AS lake "
                        + "(DATA_PATH '" + dataDir.toAbsolutePath() + "/')")
                stmt.execute("CREATE SCHEMA lake.test_schema")
            }
        }
        val session = testSessionBuilder().setCatalog("ducklake").setSchema("test_schema").build()
        val runner = DistributedQueryRunner.builder(session).build()
        try {
            runner.installPlugin(DucklakePlugin())
            runner.createCatalog("ducklake", "ducklake", mapOf(
                    "ducklake.catalog.database-url" to pg.getJdbcUrl(dbName),
                    "ducklake.catalog.database-user" to pg.getUser(),
                    "ducklake.catalog.database-password" to pg.getPassword(),
                    "ducklake.data-path" to (dataDir.toAbsolutePath().toString() + "/"),
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
        if (::dataDir.isInitialized && Files.exists(dataDir)) {
            Files.walk(dataDir).use { w ->
                w.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
            }
        }
    }

    private fun fileCount(table: String): Long {
        val unqualified = table.substringAfter('.')
        return computeScalar("SELECT count(*) FROM \"$unqualified\$files\"") as Long
    }

    private fun latestSnapshot(table: String): Long {
        val unqualified = table.substringAfter('.')
        return computeScalar("SELECT max(snapshot_id) FROM \"$unqualified\$snapshots\"") as Long
    }

    private fun call(table: String): String {
        val schema = table.substringBefore('.')
        val name = table.substringAfter('.')
        return "CALL system.rewrite_data_files(schema_name => '$schema', table_name => '$name')"
    }

    private fun tryDrop(table: String) {
        try {
            computeActual("DROP TABLE $table")
        }
        catch (ignored: Exception) {
        }
    }

    @Test
    fun compactsSmallFilesPreservingRowsAndTimeTravel() {
        val table = "test_schema.rewrite_basic"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
            computeActual("INSERT INTO $table VALUES (3, 'c')")
            computeActual("INSERT INTO $table VALUES (4, 'd')")
            computeActual("INSERT INTO $table VALUES (5, 'e')")

            val filesBefore = fileCount(table)
            assertThat(filesBefore).`as`("each write produced its own small file").isGreaterThanOrEqualTo(4L)
            val preSnapshot = latestSnapshot(table)
            val rowsBefore = computeActual("SELECT id, name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as Int to it.getField(1) as String }

            computeActual(call(table))

            assertThat(fileCount(table)).`as`("small files compacted into one").isEqualTo(1L)
            val rowsAfter = computeActual("SELECT id, name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as Int to it.getField(1) as String }
            assertThat(rowsAfter).`as`("row set + values preserved").isEqualTo(rowsBefore)

            // Time travel to the pre-compaction snapshot still reads the original (now end-snapshotted) files.
            val rowsAtPre = computeActual("SELECT id, name FROM $table FOR VERSION AS OF $preSnapshot ORDER BY id")
                    .materializedRows.map { it.getField(0) as Int to it.getField(1) as String }
            assertThat(rowsAtPre).`as`("older snapshot still resolves the source files").isEqualTo(rowsBefore)
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun appliesDeletesDroppingTombstonedRows() {
        val table = "test_schema.rewrite_deletes"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
            computeActual("INSERT INTO $table VALUES (3, 'c')")
            computeActual("INSERT INTO $table VALUES (4, 'd')")
            computeActual("DELETE FROM $table WHERE id IN (2, 3)")

            val liveBefore = computeActual("SELECT id, name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as Int to it.getField(1) as String }
            assertThat(liveBefore).containsExactly(1 to "a", 4 to "d")

            computeActual(call(table))

            assertThat(fileCount(table)).`as`("compacted to a single file with tombstones applied").isEqualTo(1L)
            val liveAfter = computeActual("SELECT id, name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as Int to it.getField(1) as String }
            assertThat(liveAfter).`as`("live rows unchanged; deleted rows physically dropped").isEqualTo(liveBefore)
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(2L)
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun partitionedTableRejected() {
        val table = "test_schema.rewrite_partitioned"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR) WITH (partitioned_by = ARRAY['region'])")
            computeActual("INSERT INTO $table VALUES (1, 'US')")
            computeActual("INSERT INTO $table VALUES (2, 'US')")
            assertQueryFails(call(table), ".*does not support partitioned tables.*")
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun partialReclaimsSourcesImmediatelyAndTimeTravelStaysCorrect() {
        val table = "test_schema.rewrite_partial"
        try {
            // One row per snapshot so each source file has a distinct begin_snapshot.
            computeActual("CREATE TABLE $table AS SELECT 1 AS id")
            val s1 = latestSnapshot(table)
            computeActual("INSERT INTO $table VALUES (2)")
            val s2 = latestSnapshot(table)
            computeActual("INSERT INTO $table VALUES (3)")
            assertThat(fileCount(table)).`as`("three small files before compaction").isGreaterThanOrEqualTo(3L)

            // Partial / merge_adjacent: the merged file carries _ducklake_internal_snapshot_id and
            // back-dates to begin = s1 with partial_max = s3, so the sources can be dropped now.
            computeActual("CALL system.rewrite_data_files(schema_name => 'test_schema', "
                    + "table_name => 'rewrite_partial', reclaim_sources_immediately => true)")

            assertThat(fileCount(table)).`as`("compacted to a single partial file").isEqualTo(1L)
            // Latest read: every row.
            assertThat(computeActual("SELECT id FROM $table ORDER BY id").materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1, 2, 3)
            // Time-travel reads are served by the merged file alone (sources are gone), filtered by
            // _ducklake_internal_snapshot_id: AS OF s1 → only the row added at s1; AS OF s2 → s1+s2.
            assertThat(computeActual("SELECT id FROM $table FOR VERSION AS OF $s1 ORDER BY id")
                    .materializedRows.map { it.getField(0) as Int })
                    .`as`("partial file reproduces the s1 snapshot on its own").containsExactly(1)
            assertThat(computeActual("SELECT id FROM $table FOR VERSION AS OF $s2 ORDER BY id")
                    .materializedRows.map { it.getField(0) as Int })
                    .`as`("partial file reproduces the s2 snapshot on its own").containsExactly(1, 2)
        }
        finally {
            tryDrop(table)
        }
    }

    @Test
    fun singleFileIsNoOp() {
        val table = "test_schema.rewrite_single"
        try {
            computeActual("CREATE TABLE $table AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)")
            val before = fileCount(table)
            assertThat(before).isEqualTo(1L)

            computeActual(call(table))

            assertThat(fileCount(table)).`as`("fewer than two candidates → no-op").isEqualTo(1L)
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(2L)
        }
        finally {
            tryDrop(table)
        }
    }
}
