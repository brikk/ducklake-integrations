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

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.sql.DriverManager

/**
 * Pins the DuckLake spec invariant that ≤1 active delete file may exist per
 * `data_file_id` per snapshot (README:223, checkDeleteFileOverlap:1311-1312).
 *
 * Pre-fix behavior of `applyDeleteFragments`: each commitDelete simply
 * inserts a new delete-file row and decrements `record_count` by the new
 * file's `deleteCount`. After two commits against the same `data_file_id`
 * the catalog held two active rows and `record_count` was double-decremented
 * if the second fragment was written as the union of (prior ∪ new) positions.
 *
 * Post-fix behavior asserted here:
 *  - The prior active delete file for that `data_file_id` is end-snapshotted
 *    in the same transaction (≤1 active row).
 *  - `record_count` decrements by `newDeleteCount` (the delta —
 *    positions added by THIS commit only), not by `deleteCount` (the total
 *    union size stored in the new parquet file).
 *
 * The sink-side coordination (read prior, union with new, write union) lives in
 * `DucklakeMergeSink`; this test exercises only the catalog half of the fix by
 * directly invoking `commitDelete` with hand-constructed fragments that mimic
 * what a corrected sink would produce. The fragments point to placeholder paths
 * (never opened — catalog-only storage).
 */
class TestApplyDeleteFragmentsEndSnapshotsPrior {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog
        private var catalog: JdbcDucklakeCatalog? = null
        private var tableId: Long = 0
        private var dataFileId: Long = 0
        private var initialRecordCount: Long = 0

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "apply-delete-fragments-end-snapshot")

            val config = DucklakeCatalogConfig().apply {
                catalogDatabaseUrl = isolated.jdbcUrl
                catalogDatabaseUser = isolated.user
                catalogDatabasePassword = isolated.password
                dataPath = isolated.dataDir.toAbsolutePath().toString()
                maxCatalogConnections = 5
            }
            val cat = JdbcDucklakeCatalog(config)
            catalog = cat

            val snapshotId = cat.currentSnapshotId
            val table = cat.getTable("test_schema", "simple_table", snapshotId)!!
            tableId = table.tableId
            dataFileId = cat.getDataFiles(tableId, snapshotId).first().dataFileId
            initialRecordCount = cat.getTableStats(tableId)!!.recordCount
            // simple_table is seeded with 5 rows in a single data file
            assertThat(initialRecordCount).isEqualTo(5L)
        }

        @AfterAll
        @JvmStatic
        fun tearDownClass() {
            catalog?.close()
            if (::server.isInitialized) {
                server.close()
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun twoCommitsOnSameDataFileLeaveExactlyOneActiveDeleteFile() {
        // First DELETE — one position. No prior delete file; trivially the only active row.
        val first = DucklakeDeleteFragment(
            dataFileId,
            "test_data/apply_delete_end_snapshot_first.parquet",
            /* deleteCount (positions in new file) */ 1L,
            /* fileSizeBytes */ 256L,
            /* footerSize */ 64L,
            /* newDeleteCount (delta for record_count) */ 1L,
        )
        catalog!!.commitDelete(tableId, listOf(first))

        val activeAfterFirst = countActiveDeleteFiles(dataFileId)
        val recordCountAfterFirst = catalog!!.getTableStats(tableId)!!.recordCount
        assertThat(activeAfterFirst)
            .`as`("exactly one active delete file after the first commit")
            .isEqualTo(1L)
        assertThat(recordCountAfterFirst)
            .`as`("record_count decremented by the first delta")
            .isEqualTo(initialRecordCount - 1L)

        // Second DELETE — sink would have unioned the prior position with one new one,
        // so the new file's deleteCount is 2 (total in file) but newDeleteCount is 1 (delta).
        val second = DucklakeDeleteFragment(
            dataFileId,
            "test_data/apply_delete_end_snapshot_second.parquet",
            /* deleteCount (positions in new file = prior ∪ new) */ 2L,
            /* fileSizeBytes */ 256L,
            /* footerSize */ 64L,
            /* newDeleteCount (delta only) */ 1L,
        )
        catalog!!.commitDelete(tableId, listOf(second))

        val activeAfterSecond = countActiveDeleteFiles(dataFileId)
        val recordCountAfterSecond = catalog!!.getTableStats(tableId)!!.recordCount

        assertThat(activeAfterSecond)
            .`as`(
                "DuckLake spec: ≤1 active delete file per data_file_id per snapshot — " +
                    "prior file must be end-snapshotted in the same transaction " +
                    "the new (union) file is inserted",
            )
            .isEqualTo(1L)
        assertThat(recordCountAfterSecond)
            .`as`(
                "record_count must decrement by newDeleteCount (delta), not deleteCount (union total). " +
                    "Decrementing by the union total double-counts the prior delete's positions, " +
                    "which were already subtracted at the first commit.",
            )
            .isEqualTo(initialRecordCount - 2L)
    }

    @Throws(Exception::class)
    private fun countActiveDeleteFiles(forDataFileId: Long): Long {
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
            val dsl = CatalogTestSupport.dsl(conn)
            val count: Long? = dsl.selectCount()
                .from(delfile)
                .where(delfile.DATA_FILE_ID.eq(forDataFileId))
                .and(delfile.END_SNAPSHOT.isNull())
                .fetchOne(0, Long::class.java)
            return count ?: 0L
        }
    }
}
