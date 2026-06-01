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
 * Acceptance test for the `tables_deleted_from × altered_tables`
 * matrix entry at `ducklake_transaction.cpp:1255`. T1 alters a
 * table's schema; T2 races with a delete against the same table. The
 * delete must abort non-retryably so we don't end-snapshot data files
 * under a now-shifted schema.
 */
class TestConcurrentDeleteVsAlterTable {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog
        private var tableId: Long = 0
        private var nameColumnId: Long = 0
        private var sharedDataFileId: Long = 0

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                server,
                "concurrent-delete-vs-alter-table",
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
            tableId = table.tableId
            nameColumnId = catalog.getTableColumns(tableId, snapshotId).stream()
                .filter { c -> c.columnName == "name" }
                .findFirst()
                .orElseThrow()
                .columnId
            sharedDataFileId = catalog.getDataFiles(tableId, snapshotId).first().dataFileId
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
    fun deleteRacingAlterTableConflicts() {
        val loserFragment = DucklakeDeleteFragment(
            sharedDataFileId,
            "test_data/delete_vs_alter_loser.parquet",
            /* deleteCount */ 1L,
            /* fileSizeBytes */ 256L,
            /* footerSize */ 64L,
            /* newDeleteCount */ 1L,
        )

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.dropColumn(tableId, nameColumnId) },
            Runnable { catalog.commitDelete(tableId, listOf(loserFragment)) },
        )

        assertThat(result.loserException)
            .`as`("delete racing an ALTER TABLE on the same table must conflict")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
            .`as`("error message must name the contended table and reference the alter")
            .contains("delete from table")
            .contains("id=$tableId")
            .contains("altered it")

        assertThat((result.loserException as TransactionConflictException).retryable())
            .`as`("logical conflicts are non-retryable")
            .isFalse()
        assertThat(result.loserAttemptCount)
            .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getTableColumns(tableId, latestSnapshot))
            .`as`("winner's DROP COLUMN visible at the latest snapshot")
            .extracting(java.util.function.Function<DucklakeColumn, Long> { it.columnId })
            .doesNotContain(nameColumnId)
    }
}
