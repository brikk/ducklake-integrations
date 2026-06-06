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
 * Mirrors [TestConcurrentInsertVsDropColumn] for `commitAddFiles`.
 * Pins that `add_files` participates in the conflict matrix the same way
 * as `INSERT` — both record `WriteChange.InsertedIntoTable`, so the
 * logical conflict check rejects a fragment whose column-stats reference a
 * column an intervening `DROP COLUMN` end-snapshotted.
 */
class TestConcurrentAddFilesVsDropColumn {
    @Test
    @Throws(Exception::class)
    fun loserAddFilesReferencingDroppedColumnFailsLogicalCheck() {
        val loserFragment = addFilesFragment(
                "/abs/path/add_files_vs_drop_column_loser.parquet",
                nameColumnId)

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                Runnable { catalog.dropColumn(tableId, nameColumnId) },
                Runnable { catalog.commitAddFiles(tableId, listOf(loserFragment)) })

        assertThat(result.loserException)
                .`as`("loser must abort with a logical conflict naming the dropped column")
                .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
                .`as`("error message must name the dropped column_id and the table_id")
                .contains(java.lang.Long.toString(nameColumnId))
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
                .`as`("loser's data file must NOT be present — its add_files was aborted before commit")
                .extracting(java.util.function.Function<DucklakeDataFile, String> { it.path })
                .doesNotContain(loserFragment.path)
    }

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
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                    server, "concurrent-add-files-vs-drop-column")

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
                    .filter { it.columnName == "name" }
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

        /**
         * Build an add_files-shape fragment: absolute path, a name-map that references
         * the (about-to-be-dropped) column by source name → target field_id, and column
         * stats keyed by the same target field_id. The logical-conflict check inspects
         * the column-stats payload, so this faithfully reproduces what
         * `DucklakeAddFilesProcedure` sends in production.
         */
        private fun addFilesFragment(path: String, columnId: Long): DucklakeWriteFragment {
            val colStats = DucklakeFileColumnStats(
                    columnId,
                    64L,
                    5L,
                    0L,
                    "a",
                    "z",
                    false)
            val nameMap = DucklakeNameMap(listOf(
                    DucklakeNameMapEntry("name", columnId)))
            return DucklakeWriteFragment(
                    path,
                    /* pathIsRelative = */ false,
                    "parquet",
                    /* fileSizeBytes = */ 1024L,
                    /* footerSize = */ 64L,
                    /* recordCount = */ 5L,
                    listOf(colStats),
                    emptyMap(),
                    null,
                    nameMap)
        }
    }
}
