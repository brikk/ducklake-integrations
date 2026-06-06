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
 * Acceptance test for the `tables_inserted_into × altered_tables`
 * matrix entry at `ducklake_transaction.cpp:1247`. Distinct from
 * [TestConcurrentInsertVsDropColumn]: there, the loser's fragment
 * column-stats reference a column that's been end-snapshotted, so the
 * *state-based* [LogicalConflictCheck] fires. Here T1
 * *renames* a column the loser's fragment references — the
 * `column_id` stays active across rename, so the state-based
 * check passes and the matrix is the only thing that catches the conflict.
 */
class TestConcurrentInsertVsAlterTable {
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
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-vs-alter-table")

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
            idColumnId = catalog.getTableColumns(tableId, snapshotId).stream()
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
    }

    @Test
    @Throws(Exception::class)
    fun insertRacingColumnRenameConflicts() {
        val loserFragment = DucklakeWriteFragment(
            "test_data/insert_vs_alter_table_loser.parquet",
            /* fileSizeBytes */ 1024L,
            /* footerSize */ 64L,
            /* recordCount */ 5L,
            listOf(
                DucklakeFileColumnStats(
                    idColumnId,
                    32L,
                    /* valueCount */ 5L,
                    /* nullCount */ 0L,
                    "1",
                    "100",
                    /* containsNan */ false,
                ),
            ),
        )

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.renameColumn(tableId, idColumnId, "id_renamed") },
            Runnable { catalog.commitInsert(tableId, listOf(loserFragment)) },
        )

        assertThat(result.loserException)
            .`as`(
                "insert racing a column rename must conflict via the matrix " +
                    "(state-based check passes because column_id stays active across rename)",
            )
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
            .`as`("error message must reference the matrix's insert-vs-alter pair")
            .contains("insert into table")
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
            .`as`("winner's rename visible at the latest snapshot")
            .extracting(java.util.function.Function<DucklakeColumn, String> { it.columnName })
            .contains("id_renamed")
        assertThat(catalog.getDataFiles(tableId, latestSnapshot))
            .`as`("loser's data file did NOT land — its INSERT was aborted before commit")
            .extracting(java.util.function.Function<DucklakeDataFile, String> { it.path })
            .doesNotContain(loserFragment.path)
    }
}
