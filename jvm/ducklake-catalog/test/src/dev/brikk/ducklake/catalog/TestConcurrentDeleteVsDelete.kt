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
 * Acceptance test for the finer-grained delete-vs-delete file-overlap check
 * (port of `ducklake_transaction.cpp:1259–1283`). Two writers that
 * each commit a delete file targeting the SAME `data_file_id` would
 * silently lose one set of deletions — the second commit's INSERT into
 * `ducklake_delete_file` end-snapshots the first commit's delete file,
 * so only the second writer's deletions remain visible to readers.
 *
 * This is the case neither the basic conflict matrix
 * (`tables_deleted_from × tables_deleted_from` alone is not a conflict)
 * nor the state-based [LogicalConflictCheck] catch — the data file
 * itself stays active across delete commits. Detection requires querying
 * `ducklake_delete_file` for intervening rows referencing the
 * contended `data_file_id`s.
 */
class TestConcurrentDeleteVsDelete {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog
        private var tableId: Long = 0
        private var sharedDataFileId: Long = 0

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                server,
                "concurrent-delete-vs-delete",
            )

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
            sharedDataFileId = catalog.getDataFiles(tableId, snapshotId).first().dataFileId()
        }

        @JvmStatic
        @AfterAll
        fun tearDownClass() {
            if (::catalog.isInitialized) {
                catalog.close()
            }
            if (::server.isInitialized) {
                server.close()
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun duelingDeletesOnSameDataFileConflict() {
        val winnerFragment = DucklakeDeleteFragment(
            sharedDataFileId,
            "test_data/delete_vs_delete_winner.parquet",
            /* deleteCount */ 2L,
            /* fileSizeBytes */ 256L,
            /* footerSize */ 64L,
            /* newDeleteCount */ 2L,
        )
        val loserFragment = DucklakeDeleteFragment(
            sharedDataFileId,
            "test_data/delete_vs_delete_loser.parquet",
            /* deleteCount */ 1L,
            /* fileSizeBytes */ 256L,
            /* footerSize */ 64L,
            /* newDeleteCount */ 1L,
        )

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.commitDelete(tableId, listOf(winnerFragment)) },
            Runnable { catalog.commitDelete(tableId, listOf(loserFragment)) },
        )

        assertThat(result.loserException)
            .`as`("two concurrent delete-files on the same data_file_id must conflict")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
            .`as`("error message must name the contended data_file_id")
            .contains(java.lang.Long.toString(sharedDataFileId))
            .contains("delete files")

        assertThat((result.loserException as TransactionConflictException).retryable())
            .`as`("logical conflicts are non-retryable")
            .isFalse()
        assertThat(result.loserAttemptCount)
            .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
            .isEqualTo(2)
    }
}
