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
import java.util.OptionalLong

/**
 * Mirrors [TestConcurrentInsertVsDropTable] for `commitAddFiles`.
 * Pins the table-active branch of `LogicalConflictCheck`: an
 * `add_files` call whose fragment payload references a table an
 * intervening `DROP TABLE` end-snapshotted must abort non-retryably.
 */
class TestConcurrentAddFilesVsDropTable {
    @Test
    @Throws(Exception::class)
    fun loserAddFilesReferencingDroppedTableFailsLogicalCheck() {
        val loserFragment = addFilesFragment(
                "/abs/path/add_files_vs_drop_table_loser.parquet",
                idColumnId)

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                Runnable { catalog.dropTable("test_schema", "simple_table") },
                Runnable { catalog.commitAddFiles(tableId, listOf(loserFragment)) })

        assertThat(result.loserException)
                .`as`("loser must abort with a logical conflict naming the dropped table")
                .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
                .`as`("error message must name the dropped table_id and the cause")
                .contains("table_id=$tableId")
                .contains("DROP TABLE")

        assertThat((result.loserException as TransactionConflictException).retryable())
                .`as`("logical conflicts are non-retryable")
                .isFalse()

        assertThat(result.loserAttemptCount)
                .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + retry-failed")
                .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getTable("test_schema", "simple_table", latestSnapshot))
                .`as`("winner's DROP TABLE must be visible at the latest snapshot")
                .isEmpty
    }

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
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                    server, "concurrent-add-files-vs-drop-table")

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
                    .filter { it.columnName() == "id" }
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

        private fun addFilesFragment(path: String, columnId: Long): DucklakeWriteFragment {
            val colStats = DucklakeFileColumnStats(
                    columnId,
                    32L,
                    5L,
                    0L,
                    Optional.of("1"),
                    Optional.of("100"),
                    false)
            val nameMap = DucklakeNameMap(listOf(
                    DucklakeNameMapEntry("id", columnId)))
            return DucklakeWriteFragment(
                    path,
                    /* pathIsRelative = */ false,
                    "parquet",
                    /* fileSizeBytes = */ 1024L,
                    /* footerSize = */ 64L,
                    /* recordCount = */ 5L,
                    listOf(colStats),
                    emptyMap(),
                    OptionalLong.empty(),
                    Optional.of(nameMap))
        }
    }
}
