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
import java.util.Optional

/**
 * Pins today's behavior for two writers concurrently committing INSERTs to the
 * same table. The loser parks before its first mutation; the winner commits;
 * the loser releases, fails the strict lineage check, retries, and commits
 * cleanly. Both data files must be visible at the latest snapshot.
 *
 * This is a baseline that must hold against unmodified production code —
 * Step 3 of the logical-conflict-checking work must not regress it (concurrent
 * INSERTs into the same table are *not* a logical conflict; they're
 * the expected concurrent-write workload).
 */
class TestConcurrentInsertSameTable {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog
        private var tableId: Long = 0
        private var idColumnId: Long = 0

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-same-table")

            val config = DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl)
                .setCatalogDatabaseUser(isolated.user)
                .setCatalogDatabasePassword(isolated.password)
                .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                .setMaxCatalogConnections(5)
            catalog = JdbcDucklakeCatalog(config)

            val snapshotId = catalog.currentSnapshotId
            val table = catalog.getTable("test_schema", "simple_table", snapshotId).orElseThrow()
            tableId = table.tableId()
            idColumnId = catalog.getTableColumns(tableId, snapshotId).stream()
                .filter { c -> c.columnName() == "id" }
                .findFirst()
                .orElseThrow()
                .columnId()
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

        private fun fragmentFor(path: String, minId: Long, maxId: Long): DucklakeWriteFragment {
            val idStats = DucklakeFileColumnStats(
                idColumnId,
                32L,
                /* valueCount */ 10L,
                /* nullCount */ 0L,
                Optional.of(minId.toString()),
                Optional.of(maxId.toString()),
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
    fun concurrentInsertsBothCommit() {
        val preDataFileCount = catalog.getDataFiles(tableId, catalog.currentSnapshotId).size.toLong()

        val winnerFragment = fragmentFor("test_data/concurrent_same_table_winner.parquet", 1L, 11L)
        val loserFragment = fragmentFor("test_data/concurrent_same_table_loser.parquet", 12L, 22L)

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.commitInsert(tableId, listOf(winnerFragment)) },
            Runnable { catalog.commitInsert(tableId, listOf(loserFragment)) },
        )

        assertThat(result.loserException)
            .`as`("loser must commit cleanly on retry; concurrent same-table INSERTs are not a conflict")
            .isNull()
        assertThat(result.loserAttemptCount)
            .`as`("loser must hit the hook twice: first attempt (paused) + retry (no-op)")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        val dataFiles = catalog.getDataFiles(tableId, latestSnapshot)
        assertThat(dataFiles)
            .`as`("both fragments must be present at the latest snapshot")
            .hasSize((preDataFileCount + 2).toInt())
        assertThat(dataFiles)
            .extracting(java.util.function.Function<DucklakeDataFile, String> { it.path() })
            .contains(winnerFragment.path(), loserFragment.path())
    }
}
