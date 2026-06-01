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
 * Gold acceptance test for [LogicalConflictCheck]: an INSERT whose
 * fragment column-stats reference a column an intervening `DROP COLUMN`
 * end-snapshotted must abort with a [LogicalConflictException] naming
 * the dropped column. This is the case lineage-only checking misses — the
 * retry's action would otherwise re-run with the stale fragment payload and
 * insert `ducklake_file_column_stats` rows pointing at end-snapshotted
 * columns.
 *
 * Also pins non-retryable behavior: the loser must hit the hook exactly
 * twice (first attempt parked, then retry fails the logical check), not
 * `MAX_RETRY_COUNT` times.
 */
class TestConcurrentInsertVsDropColumn {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog
        private var tableId: Long = 0
        private var nameColumnId: Long = 0

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-insert-vs-drop-column")

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
            nameColumnId = catalog.getTableColumns(tableId, snapshotId).stream()
                .filter { c -> c.columnName() == "name" }
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

        private fun fragmentReferencingColumn(path: String, columnId: Long): DucklakeWriteFragment {
            val colStats = DucklakeFileColumnStats(
                columnId,
                64L,
                /* valueCount */ 5L,
                /* nullCount */ 0L,
                Optional.of("a"),
                Optional.of("z"),
                /* containsNan */ false,
            )
            return DucklakeWriteFragment(
                path,
                /* fileSizeBytes */ 1024L,
                /* footerSize */ 64L,
                /* recordCount */ 5L,
                listOf(colStats),
            )
        }
    }

    @Test
    @Throws(Exception::class)
    fun loserInsertReferencingDroppedColumnFailsLogicalCheck() {
        val preDataFileCount = catalog.getDataFiles(tableId, catalog.currentSnapshotId).size.toLong()

        val loserFragment = fragmentReferencingColumn(
            "test_data/insert_vs_drop_column_loser.parquet",
            nameColumnId,
        )

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.dropColumn(tableId, nameColumnId) },
            Runnable { catalog.commitInsert(tableId, listOf(loserFragment)) },
        )

        assertThat(result.loserException)
            .`as`("loser must abort with a logical conflict naming the dropped column")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
            .`as`("error message must name the dropped column_id and the table_id")
            .contains(nameColumnId.toString())
            .contains("table_id=$tableId")
            .contains("DROP COLUMN")

        assertThat((result.loserException as TransactionConflictException).retryable())
            .`as`("logical conflicts are non-retryable")
            .isFalse()

        assertThat(result.loserAttemptCount)
            .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + retry-failed")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        val dataFiles = catalog.getDataFiles(tableId, latestSnapshot)
        assertThat(dataFiles)
            .`as`("loser's data file must NOT be present — its INSERT was aborted before commit")
            .extracting(java.util.function.Function<DucklakeDataFile, String> { it.path() })
            .doesNotContain(loserFragment.path())
        assertThat(dataFiles)
            .`as`("no new data files should land on the table — winner only dropped a column")
            .hasSize(preDataFileCount.toInt())

        assertThat(catalog.getTableColumns(tableId, latestSnapshot))
            .`as`("winner's DROP COLUMN must be visible at the latest snapshot")
            .extracting(java.util.function.Function<DucklakeColumn, Long> { it.columnId() })
            .doesNotContain(nameColumnId)
    }
}
