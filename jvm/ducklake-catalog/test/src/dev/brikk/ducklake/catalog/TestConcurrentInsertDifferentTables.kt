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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * Pins today's behavior for two writers concurrently committing INSERTs into
 * *different* tables. Both must commit; the loser still goes through
 * one retry because today's lineage check rejects any intervening commit, but
 * the retry must succeed cleanly. Catches snapshot-id cross-talk: the loser
 * must not orphan rows on the wrong table or collide on file IDs.
 *
 * Mirrors pg_ducklake's `concurrent_cross_table_writes` isolation spec.
 */
class TestConcurrentInsertDifferentTables {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog
        private var simpleTableId: Long = 0
        private var simpleIdColumnId: Long = 0
        private var partitionedTableId: Long = 0
        private var partitionedIdColumnId: Long = 0

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-different-tables")

            val config = DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl)
                .setCatalogDatabaseUser(isolated.user)
                .setCatalogDatabasePassword(isolated.password)
                .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                .setMaxCatalogConnections(5)
            catalog = JdbcDucklakeCatalog(config)

            val snapshotId = catalog.currentSnapshotId
            val simpleTable = catalog.getTable("test_schema", "simple_table", snapshotId).orElseThrow()
            simpleTableId = simpleTable.tableId
            simpleIdColumnId = catalog.getTableColumns(simpleTableId, snapshotId).stream()
                .filter { c -> c.columnName == "id" }
                .findFirst()
                .orElseThrow()
                .columnId

            val partitionedTable = catalog.getTable("test_schema", "partitioned_table", snapshotId).orElseThrow()
            partitionedTableId = partitionedTable.tableId
            partitionedIdColumnId = catalog.getTableColumns(partitionedTableId, snapshotId).stream()
                .filter { c -> c.columnName == "id" }
                .findFirst()
                .orElseThrow()
                .columnId
        }

        @AfterAll
        @JvmStatic
        fun tearDownClass() {
            if (::catalog.isInitialized) {
                catalog.close()
            }
            if (::server.isInitialized) {
                server.close()
            }
        }

        private fun fragmentFor(path: String, columnId: Long, minId: Long, maxId: Long): DucklakeWriteFragment {
            val idStats = DucklakeFileColumnStats(
                columnId,
                32L,
                /* valueCount */ 10L,
                /* nullCount */ 0L,
                minId.toString(),
                maxId.toString(),
                /* containsNan */ false,
            )
            return DucklakeWriteFragment(
                path,
                /* fileSizeBytes */ 1024L,
                /* footerSize */ 64L,
                /* recordCount */ 10L,
                listOf(idStats),
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun concurrentInsertsAcrossTablesBothCommit() {
        val preSimpleFileCount = catalog.getDataFiles(simpleTableId, catalog.currentSnapshotId).size.toLong()
        val prePartitionedFileCount = catalog.getDataFiles(partitionedTableId, catalog.currentSnapshotId).size.toLong()

        val winnerFragment = fragmentFor(
            "test_data/concurrent_diff_tables_winner.parquet",
            simpleIdColumnId,
            100L,
            110L,
        )
        val loserFragment = fragmentFor(
            "test_data/concurrent_diff_tables_loser.parquet",
            partitionedIdColumnId,
            200L,
            210L,
        )

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.commitInsert(simpleTableId, listOf(winnerFragment)) },
            Runnable { catalog.commitInsert(partitionedTableId, listOf(loserFragment)) },
        )

        assertThat(result.loserException)
            .`as`("loser must commit cleanly on retry; cross-table INSERTs are not a conflict")
            .isNull()
        assertThat(result.loserAttemptCount)
            .`as`("loser must hit the hook twice: first attempt (paused) + retry (no-op)")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId

        val simpleFiles = catalog.getDataFiles(simpleTableId, latestSnapshot)
        assertThat(simpleFiles)
            .`as`("winner's data file must land on simple_table")
            .hasSize((preSimpleFileCount + 1).toInt())
        assertThat(simpleFiles)
            .extracting(java.util.function.Function<DucklakeDataFile, String> { it.path })
            .contains(winnerFragment.path)

        val partitionedFiles = catalog.getDataFiles(partitionedTableId, latestSnapshot)
        assertThat(partitionedFiles)
            .`as`("loser's data file must land on partitioned_table (proves no cross-talk)")
            .hasSize((prePartitionedFileCount + 1).toInt())
        assertThat(partitionedFiles)
            .extracting(java.util.function.Function<DucklakeDataFile, String> { it.path })
            .contains(loserFragment.path)

        // Cross-talk negative checks: winner's file must NOT appear on the loser's table and vice versa.
        assertThat(simpleFiles)
            .extracting(java.util.function.Function<DucklakeDataFile, String> { it.path })
            .doesNotContain(loserFragment.path)
        assertThat(partitionedFiles)
            .extracting(java.util.function.Function<DucklakeDataFile, String> { it.path })
            .doesNotContain(winnerFragment.path)
    }
}
