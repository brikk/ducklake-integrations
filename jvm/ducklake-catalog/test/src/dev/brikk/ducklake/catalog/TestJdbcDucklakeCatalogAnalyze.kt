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

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE_COLUMN_STATS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE_STATS
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager

/**
 * `analyzeTable` — the catalog half of the connector's `ANALYZE`. DuckLake maintains the cached
 * table-level aggregates (`ducklake_table_stats` + `ducklake_table_column_stats`) incrementally
 * on every write, and incremental maintenance only ever *widens* min/max, so the cached values
 * can drift loose (or simply wrong) over a table's lifetime. `analyzeTable` recomputes them from
 * ground truth: `record_count` from the supplied live count, `file_size_bytes` and the per-column
 * aggregates from the authoritative per-file stats of the currently-active data files.
 *
 * This pins the recompute by deliberately CORRUPTING the cached aggregates and asserting the call
 * restores them — while preserving `next_row_id` (the row-id allocator high-water mark, which
 * ANALYZE must never reset).
 *
 * The seeded `simple_table` holds 5 rows in a single data file: id INTEGER 1..5, price DOUBLE
 * 19.99..59.99, created_date DATE 2024-01-05..2024-03-10.
 */
class TestJdbcDucklakeCatalogAnalyze {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog
        private var catalog: JdbcDucklakeCatalog? = null
        private var tableId: Long = 0
        private var idColumnId: Long = 0
        private var priceColumnId: Long = 0
        private var baselineNextRowId: Long = 0
        private var baselineFileSizeBytes: Long = 0

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "analyze-table")

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
            val columns = cat.getTableColumns(tableId, snapshotId)
            idColumnId = columns.first { it.columnName == "id" }.columnId
            priceColumnId = columns.first { it.columnName == "price" }.columnId

            val stats = cat.getTableStats(tableId)!!
            assertThat(stats.recordCount).isEqualTo(5L)
            // Capture the truth so we can corrupt the cache and assert the recompute matches.
            baselineFileSizeBytes = stats.fileSizeBytes
            baselineNextRowId = nextRowId()
        }

        @AfterAll
        @JvmStatic
        fun tearDownClass() {
            catalog?.close()
            if (::server.isInitialized) {
                server.close()
            }
        }

        private fun nextRowId(): Long {
            val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
            openConnection().use { conn ->
                return CatalogTestSupport.dsl(conn)
                    .select(tabstats.NEXT_ROW_ID)
                    .from(tabstats)
                    .where(tabstats.TABLE_ID.eq(tableId))
                    .fetchOne(0, Long::class.java) ?: 0L
            }
        }

        private fun openConnection(): Connection =
            DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password)
    }

    @Test
    @Throws(Exception::class)
    fun analyzeRepairsDriftedTableAndColumnStats() {
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        val tabcolst = DUCKLAKE_TABLE_COLUMN_STATS.`as`("tabcolst")
        val sentinelNextRowId = 4242L

        // 1. Corrupt the cached aggregates: a wrong record_count + file_size_bytes, a wrong
        //    next_row_id sentinel (must survive), an out-of-range min/max + bogus contains_null on
        //    `id`, and a fully MISSING row for `price` (deleted) to force re-insertion.
        corruptCachedStats(sentinelNextRowId)

        // 2. Recompute from ground truth: 5 live rows.
        catalog!!.analyzeTable(tableId, 5L)

        // 3a. Table stats: record_count + file_size_bytes recomputed, next_row_id PRESERVED.
        val repaired = catalog!!.getTableStats(tableId)!!
        assertThat(repaired.recordCount).`as`("record_count set to the live count").isEqualTo(5L)
        assertThat(repaired.fileSizeBytes)
            .`as`("file_size_bytes recomputed from the active data files")
            .isEqualTo(baselineFileSizeBytes)
        openConnection().use { conn ->
            val nextRowId = CatalogTestSupport.dsl(conn)
                .select(tabstats.NEXT_ROW_ID)
                .from(tabstats)
                .where(tabstats.TABLE_ID.eq(tableId))
                .fetchOne(0, Long::class.java)
            assertThat(nextRowId)
                .`as`("next_row_id (the allocator high-water mark) is preserved, not reset")
                .isEqualTo(sentinelNextRowId)
        }

        // 3b. Column stats: id's out-of-range bounds tightened back to truth, contains_null cleared.
        openConnection().use { conn ->
            val dsl = CatalogTestSupport.dsl(conn)
            val idRow = dsl.select(tabcolst.MIN_VALUE, tabcolst.MAX_VALUE, tabcolst.CONTAINS_NULL)
                .from(tabcolst)
                .where(tabcolst.TABLE_ID.eq(tableId))
                .and(tabcolst.COLUMN_ID.eq(idColumnId))
                .fetchOne()!!
            assertThat(idRow.get(tabcolst.MIN_VALUE)).isEqualTo("1")
            assertThat(idRow.get(tabcolst.MAX_VALUE)).isEqualTo("5")
            assertThat(idRow.get(tabcolst.CONTAINS_NULL))
                .`as`("contains_null rebuilt from per-file truth (id has no NULLs)")
                .isFalse()

            // 3c. The deleted `price` row was re-inserted from per-file stats.
            val priceRow = dsl.select(tabcolst.MIN_VALUE, tabcolst.MAX_VALUE)
                .from(tabcolst)
                .where(tabcolst.TABLE_ID.eq(tableId))
                .and(tabcolst.COLUMN_ID.eq(priceColumnId))
                .fetchOne()
            assertThat(priceRow).`as`("price column-stats row re-created by ANALYZE").isNotNull()
            assertThat(priceRow!!.get(tabcolst.MIN_VALUE)!!.toDouble()).isLessThanOrEqualTo(19.99)
            assertThat(priceRow.get(tabcolst.MAX_VALUE)!!.toDouble()).isGreaterThanOrEqualTo(59.99)
        }
    }

    private fun corruptCachedStats(sentinelNextRowId: Long) {
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        val tabcolst = DUCKLAKE_TABLE_COLUMN_STATS.`as`("tabcolst")
        openConnection().use { conn ->
            val dsl = CatalogTestSupport.dsl(conn)
            dsl.update(tabstats)
                .set(tabstats.RECORD_COUNT, 999L)
                .set(tabstats.FILE_SIZE_BYTES, 7L)
                .set(tabstats.NEXT_ROW_ID, sentinelNextRowId)
                .where(tabstats.TABLE_ID.eq(tableId))
                .execute()
            dsl.update(tabcolst)
                .set(tabcolst.MIN_VALUE, "-999")
                .set(tabcolst.MAX_VALUE, "999")
                .set(tabcolst.CONTAINS_NULL, true)
                .where(tabcolst.TABLE_ID.eq(tableId))
                .and(tabcolst.COLUMN_ID.eq(idColumnId))
                .execute()
            dsl.deleteFrom(tabcolst)
                .where(tabcolst.TABLE_ID.eq(tableId))
                .and(tabcolst.COLUMN_ID.eq(priceColumnId))
                .execute()
        }
    }
}
