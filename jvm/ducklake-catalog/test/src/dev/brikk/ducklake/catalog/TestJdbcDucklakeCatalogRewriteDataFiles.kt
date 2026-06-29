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
package dev.brikk.ducklake.catalog

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILES_SCHEDULED_FOR_DELETION
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE_STATS
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

/**
 * Catalog-layer acceptance tests for [DucklakeCatalog.rewriteDataFiles] — the non-partial /
 * Iceberg-style compaction primitive that backs `optimize` / `rewrite_data_files`
 * (dev-docs/DESIGN-maintenance.md § 7). These exercise the catalog half only: the merged file's
 * bytes are never produced (placeholder paths, never opened), so each test hand-builds the
 * registration fragment a corrected procedure would produce.
 *
 * Pinned invariants:
 *  - sources end-snapshotted + merged file registered atomically; older snapshots still resolve the
 *    sources (time travel),
 *  - compaction is row-count-preserving (`record_count` unchanged) while `file_size_bytes` reflects
 *    the real space delta and `next_row_id` stays monotonic,
 *  - a source's active delete file is end-snapshotted (the merged file has the deletes applied),
 *  - a concurrent DELETE landing on a source after the read aborts the compaction non-retryably
 *    (the merged file would otherwise resurrect the newly-deleted rows).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestJdbcDucklakeCatalogRewriteDataFiles {
    private lateinit var server: TestingDucklakePostgreSqlCatalogServer
    private lateinit var isolated: JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog
    private lateinit var catalog: JdbcDucklakeCatalog
    private var tableId: Long = 0
    private var idColumnId: Long = 0
    private val pathSeq = AtomicInteger()

    @BeforeAll
    @Throws(Exception::class)
    fun setUpClass() {
        server = TestingDucklakePostgreSqlCatalogServer()
        isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "rewrite-data-files")

        val config = DucklakeCatalogConfig().apply {
            catalogDatabaseUrl = isolated.jdbcUrl
            catalogDatabaseUser = isolated.user
            catalogDatabasePassword = isolated.password
            dataPath = isolated.dataDir.toAbsolutePath().toString()
            maxCatalogConnections = 5
        }
        catalog = JdbcDucklakeCatalog(config)

        val snapshotId = catalog.currentSnapshotId
        tableId = catalog.getTable("test_schema", "simple_table", snapshotId)!!.tableId
        idColumnId = catalog.getTableColumns(tableId, snapshotId)
            .first { it.columnName == "id" }
            .columnId
    }

    @AfterAll
    fun tearDownClass() {
        if (::catalog.isInitialized) {
            catalog.close()
        }
        if (::server.isInitialized) {
            server.close()
        }
    }

    private fun uniquePath(tag: String): String = "test_data/rewrite_${tag}_${pathSeq.incrementAndGet()}.parquet"

    private fun fragmentFor(path: String, recordCount: Long, fileSizeBytes: Long, minId: Long, maxId: Long): DucklakeWriteFragment {
        val idStats = DucklakeFileColumnStats(
            idColumnId,
            /* columnSizeBytes */ 32L,
            /* valueCount */ recordCount,
            /* nullCount */ 0L,
            minId.toString(),
            maxId.toString(),
            /* containsNan */ false,
        )
        return DucklakeWriteFragment(path, fileSizeBytes, /* footerSize */ 64L, recordCount, listOf(idStats))
    }

    /** Insert one data file via the normal INSERT path; return its (path, dataFileId). */
    private fun insertFile(recordCount: Long, fileSizeBytes: Long, minId: Long, maxId: Long): Pair<String, Long> {
        val path = uniquePath("src")
        catalog.commitInsert(tableId, listOf(fragmentFor(path, recordCount, fileSizeBytes, minId, maxId)))
        val id = catalog.getDataFiles(tableId, catalog.currentSnapshotId).first { it.path == path }.dataFileId
        return path to id
    }

    private fun nextRowId(): Long {
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
            return CatalogTestSupport.dsl(conn).select(tabstats.NEXT_ROW_ID)
                .from(tabstats)
                .where(tabstats.TABLE_ID.eq(tableId))
                .fetchOne(tabstats.NEXT_ROW_ID) ?: 0L
        }
    }

    private fun activeDeleteFileCount(dataFileId: Long): Long {
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
            return CatalogTestSupport.dsl(conn).selectCount()
                .from(delfile)
                .where(delfile.DATA_FILE_ID.eq(dataFileId))
                .and(delfile.END_SNAPSHOT.isNull)
                .fetchOne(0, Long::class.java) ?: 0L
        }
    }

    @Test
    @Throws(Exception::class)
    fun compactsSourcesPreservingRowCountAndTimeTravel() {
        val (pathA, idA) = insertFile(10L, 1024L, 100L, 109L)
        val (pathB, idB) = insertFile(10L, 2048L, 110L, 119L)

        val readSnapshot = catalog.currentSnapshotId
        val statsBefore = catalog.getTableStats(tableId)!!
        val nextRowIdBefore = nextRowId()
        val activeBefore = catalog.getDataFiles(tableId, readSnapshot).map { it.path }
        assertThat(activeBefore).contains(pathA, pathB)

        // Merge A+B (20 live rows, sizes 1024+2048) into one 1500-byte file.
        val mergedPath = uniquePath("merged")
        val merged = fragmentFor(mergedPath, 20L, 1500L, 100L, 119L)
        catalog.rewriteDataFiles(tableId, setOf(idA, idB), listOf(merged), readSnapshot)

        val latest = catalog.currentSnapshotId
        val activeAfter = catalog.getDataFiles(tableId, latest)
        val activePaths = activeAfter.map { it.path }
        assertThat(activePaths)
            .`as`("sources retired, merged file registered")
            .contains(mergedPath)
            .doesNotContain(pathA, pathB)

        val statsAfter = catalog.getTableStats(tableId)!!
        assertThat(statsAfter.recordCount)
            .`as`("compaction is row-count-preserving")
            .isEqualTo(statsBefore.recordCount)
        assertThat(statsAfter.fileSizeBytes)
            .`as`("file_size reflects (merged − retired): %d − 1024 − 2048 + 1500", statsBefore.fileSizeBytes)
            .isEqualTo(statsBefore.fileSizeBytes - 1024L - 2048L + 1500L)
        assertThat(nextRowId())
            .`as`("next_row_id is monotonic (advanced by the merged file's 20 rows)")
            .isEqualTo(nextRowIdBefore + 20L)

        // Time travel: the pre-compaction snapshot still resolves the sources, not the merged file.
        val atReadSnapshot = catalog.getDataFiles(tableId, readSnapshot).map { it.path }
        assertThat(atReadSnapshot)
            .`as`("older snapshot still sees the (end-snapshotted) source files")
            .contains(pathA, pathB)
            .doesNotContain(mergedPath)
    }

    @Test
    @Throws(Exception::class)
    fun endSnapshotsSourceDeleteFileAndPreservesLiveCount() {
        val (_, idC) = insertFile(10L, 1024L, 200L, 209L)

        // Delete 3 of C's rows (the merged file will hold the 7 survivors).
        catalog.commitDelete(
            tableId,
            listOf(
                DucklakeDeleteFragment(
                    idC,
                    uniquePath("del"),
                    /* deleteCount */ 3L,
                    /* fileSizeBytes */ 256L,
                    /* footerSize */ 64L,
                    /* newDeleteCount */ 3L,
                ),
            ),
        )
        assertThat(activeDeleteFileCount(idC)).`as`("delete file active before compaction").isEqualTo(1L)

        val readSnapshot = catalog.currentSnapshotId
        val recordCountBefore = catalog.getTableStats(tableId)!!.recordCount

        val mergedPath = uniquePath("merged")
        // 7 live rows (10 − 3 deleted).
        catalog.rewriteDataFiles(tableId, setOf(idC), listOf(fragmentFor(mergedPath, 7L, 900L, 200L, 209L)), readSnapshot)

        assertThat(activeDeleteFileCount(idC))
            .`as`("source's delete file end-snapshotted — merged file already has deletes applied")
            .isEqualTo(0L)
        assertThat(catalog.getTableStats(tableId)!!.recordCount)
            .`as`("live row count unchanged by compaction")
            .isEqualTo(recordCountBefore)
        val activePaths = catalog.getDataFiles(tableId, catalog.currentSnapshotId).map { it.path }
        assertThat(activePaths).contains(mergedPath)
    }

    @Test
    @Throws(Exception::class)
    fun concurrentDeleteOnSourceAbortsCompactionNonRetryably() {
        val (pathD, idD) = insertFile(10L, 1024L, 300L, 309L)
        val readSnapshot = catalog.currentSnapshotId
        val mergedPath = uniquePath("merged")
        val merged = fragmentFor(mergedPath, 10L, 900L, 300L, 309L)

        // Winner: a DELETE that adds a delete file to D (begin_snapshot > readSnapshot) WITHOUT
        // end-snapshotting D's data-file row. Loser: the compaction whose merged file was built
        // before that delete — it must abort rather than resurrect the deleted row.
        val winnerDelete = DucklakeDeleteFragment(
            idD,
            uniquePath("winnerdel"),
            /* deleteCount */ 1L,
            /* fileSizeBytes */ 256L,
            /* footerSize */ 64L,
            /* newDeleteCount */ 1L,
        )

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.commitDelete(tableId, listOf(winnerDelete)) },
            Runnable { catalog.rewriteDataFiles(tableId, setOf(idD), listOf(merged), readSnapshot) },
        )

        assertThat(result.loserException)
            .`as`("compaction racing a delete on its source must conflict")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat((result.loserException as TransactionConflictException).retryable())
            .`as`("a stale-read compaction conflict is non-retryable")
            .isFalse()

        val latest = catalog.currentSnapshotId
        val activePaths = catalog.getDataFiles(tableId, latest).map { it.path }
        assertThat(activePaths)
            .`as`("compaction rolled back: source still active, merged file never registered")
            .contains(pathD)
            .doesNotContain(mergedPath)
        assertThat(activeDeleteFileCount(idD))
            .`as`("winner's delete is the surviving commit")
            .isEqualTo(1L)
    }

    @Test
    @Throws(Exception::class)
    fun emptyArgumentsAreNoOps() {
        val before = catalog.currentSnapshotId
        catalog.rewriteDataFiles(tableId, emptySet(), emptyList(), before)
        catalog.rewriteDataFiles(tableId, setOf(1L), emptyList(), before)
        catalog.rewriteDataFiles(tableId, emptySet(), listOf(fragmentFor(uniquePath("x"), 1L, 1L, 1L, 1L)), before)
        assertThat(catalog.currentSnapshotId)
            .`as`("no-op calls mint no snapshot")
            .isEqualTo(before)
    }

    private fun beginSnapshotOf(path: String): Long {
        val f = DUCKLAKE_DATA_FILE.`as`("f")
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
            return CatalogTestSupport.dsl(conn).select(f.BEGIN_SNAPSHOT).from(f)
                .where(f.PATH.eq(path)).fetchOne(f.BEGIN_SNAPSHOT) ?: -1L
        }
    }

    private fun fileRow(path: String): Pair<Long?, Long?> {
        val f = DUCKLAKE_DATA_FILE.`as`("f")
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
            val r = CatalogTestSupport.dsl(conn).select(f.BEGIN_SNAPSHOT, f.PARTIAL_MAX).from(f)
                .where(f.PATH.eq(path)).fetchOne() ?: return null to null
            return r.value1() to r.value2()
        }
    }

    private fun dataFileRowCount(dataFileId: Long): Long {
        val f = DUCKLAKE_DATA_FILE.`as`("f")
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
            return CatalogTestSupport.dsl(conn).selectCount().from(f)
                .where(f.DATA_FILE_ID.eq(dataFileId)).fetchOne(0, Long::class.java) ?: 0L
        }
    }

    private fun scheduledForDeletion(dataFileId: Long): Long {
        val s = DUCKLAKE_FILES_SCHEDULED_FOR_DELETION.`as`("s")
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
            return CatalogTestSupport.dsl(conn).selectCount().from(s)
                .where(s.DATA_FILE_ID.eq(dataFileId)).fetchOne(0, Long::class.java) ?: 0L
        }
    }

    @Test
    @Throws(Exception::class)
    fun partialRewriteBackDatesMergedFileAndReclaimsSources() {
        // Two files at DIFFERENT begin snapshots (separate commits).
        val (pathE, idE) = insertFile(10L, 1024L, 400L, 409L)
        val beginE = beginSnapshotOf(pathE)
        val (pathF, idF) = insertFile(10L, 2048L, 410L, 419L)
        val beginF = beginSnapshotOf(pathF)
        assertThat(beginF).`as`("second insert is a later snapshot").isGreaterThan(beginE)

        val readSnapshot = catalog.currentSnapshotId
        val statsBefore = catalog.getTableStats(tableId)!!

        val mergedPath = uniquePath("pmerged")
        catalog.rewriteDataFilesPartial(tableId, setOf(idE, idF),
            listOf(PartialMergedFile(fragmentFor(mergedPath, 20L, 1500L, 400L, 419L), beginE, beginF)), readSnapshot)

        // Merged file back-dated to min(begin), tagged partial_max = max(begin).
        val (mergedBegin, mergedPartialMax) = fileRow(mergedPath)
        assertThat(mergedBegin).`as`("back-dated begin = min source begin").isEqualTo(beginE)
        assertThat(mergedPartialMax).`as`("partial_max = max source begin").isEqualTo(beginF)

        // Sources DELETED entirely (not end-snapshotted) + scheduled for physical deletion.
        assertThat(dataFileRowCount(idE)).`as`("source E row deleted entirely").isEqualTo(0L)
        assertThat(dataFileRowCount(idF)).`as`("source F row deleted entirely").isEqualTo(0L)
        assertThat(scheduledForDeletion(idE)).`as`("source E scheduled for deletion").isEqualTo(1L)
        assertThat(scheduledForDeletion(idF)).`as`("source F scheduled for deletion").isEqualTo(1L)

        // Live row count preserved; merged file is the only active one of the three.
        assertThat(catalog.getTableStats(tableId)!!.recordCount).isEqualTo(statsBefore.recordCount)
        val activePaths = catalog.getDataFiles(tableId, catalog.currentSnapshotId).map { it.path }
        assertThat(activePaths).contains(mergedPath).doesNotContain(pathE, pathF)
    }

    @Test
    @Throws(Exception::class)
    fun partialRewriteConcurrentDeleteOnSourceConflicts() {
        val (_, idG) = insertFile(10L, 1024L, 500L, 509L)
        val readSnapshot = catalog.currentSnapshotId
        val mergedPath = uniquePath("pmerged")
        val merged = PartialMergedFile(fragmentFor(mergedPath, 10L, 900L, 500L, 509L), readSnapshot, readSnapshot)
        val winnerDelete = DucklakeDeleteFragment(idG, uniquePath("pwin"), 1L, 256L, 64L, 1L)

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.commitDelete(tableId, listOf(winnerDelete)) },
            Runnable { catalog.rewriteDataFilesPartial(tableId, setOf(idG), listOf(merged), readSnapshot) },
        )
        assertThat(result.loserException)
            .`as`("partial compaction racing a delete on its source must conflict")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat((result.loserException as TransactionConflictException).retryable()).isFalse()
        assertThat(dataFileRowCount(idG)).`as`("source survives the aborted partial compaction").isEqualTo(1L)
    }
}
