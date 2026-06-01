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
 * Pins upstream's `altered_table × altered_table` matrix entry
 * (ducklake_transaction.cpp:1307–1310). Two concurrent `addColumn`
 * calls on the same table — even when `column_order` would technically
 * not collide because the loser's retry re-reads `maxOrder` — are
 * rejected as a logical conflict per upstream policy. The loser fails
 * non-retryably with a [LogicalConflictException] naming the contended
 * `table_id`.
 *
 * (Earlier, before the matrix landed, the loser's retry would commit and
 * both columns would land. The Step 3 test pinning that behavior was
 * marked "pin whichever is correct after Step 3 lands" — this is the
 * post-Step-4 behavior matching upstream.)
 */
class TestConcurrentAlteredTableVsAlteredTable {
    @Test
    @Throws(Exception::class)
    fun concurrentAddColumnsConflict() {
        val winnerColumn = TableColumnSpec.leaf("winner_col", "varchar", true)
        val loserColumn = TableColumnSpec.leaf("loser_col", "integer", true)

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                Runnable { catalog.addColumn(tableId, winnerColumn) },
                Runnable { catalog.addColumn(tableId, loserColumn) })

        assertThat(result.loserException)
                .`as`("upstream rejects altered_table × altered_table on the same table_id")
                .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
                .`as`("error message must name the contended table_id and the alter-vs-alter pair")
                .contains("alter table")
                .contains("id=$tableId")
                .contains("altered it")

        assertThat((result.loserException as TransactionConflictException).retryable())
                .`as`("logical conflicts are non-retryable")
                .isFalse()
        assertThat(result.loserAttemptCount)
                .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + retry-failed")
                .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getTableColumns(tableId, latestSnapshot))
                .extracting(java.util.function.Function<DucklakeColumn, String> { it.columnName() })
                .`as`("only the winner's column lands; loser's altered_table aborted")
                .contains("winner_col")
                .doesNotContain("loser_col")
    }

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog
        private var tableId: Long = 0

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                    server, "concurrent-altered-table-vs-altered-table")

            val config = DucklakeCatalogConfig()
                    .setCatalogDatabaseUrl(isolated.jdbcUrl)
                    .setCatalogDatabaseUser(isolated.user)
                    .setCatalogDatabasePassword(isolated.password)
                    .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                    .setMaxCatalogConnections(5)
            catalog = JdbcDucklakeCatalog(config)

            val snapshotId = catalog.currentSnapshotId
            tableId = catalog.getTable("test_schema", "simple_table", snapshotId).orElseThrow().tableId()
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
}
