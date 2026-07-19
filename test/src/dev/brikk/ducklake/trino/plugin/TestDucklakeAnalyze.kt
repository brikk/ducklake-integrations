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
package dev.brikk.ducklake.trino.plugin

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE_STATS
import dev.brikk.ducklake.catalog.testing.CatalogQueries
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * `ANALYZE` — refreshes DuckLake's cached table-level statistics. The connector declares only the
 * table ROW_COUNT for the engine to collect (the engine's authoritative live count); the per-column
 * aggregates are rebuilt in the catalog from the authoritative per-file stats. Because DuckLake
 * maintains these aggregates incrementally on every write, ANALYZE is observably a no-op on a
 * never-drifted table — so the meaningful test deliberately drifts `record_count` in the catalog
 * and asserts ANALYZE recomputes it from a full scan, overwriting the wrong cached value.
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeAnalyze : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "analyze-integration"

    @Test
    fun analyzeSucceedsAndStatsAreCoherent() {
        val table = "test_schema.an_basic"
        try {
            computeActual("CREATE TABLE $table AS " +
                    "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) AS t(id, name)")

            // Before this connector wired the statistics-collection SPI, ANALYZE threw
            // "This connector does not support analyze". It must now run cleanly.
            computeActual("ANALYZE $table")

            assertThat(showStatsRowCount(table)).isEqualTo(5.0)
            // The column exposes low/high bounds to the planner (sourced from per-file stats). Exact
            // min/max encoding is pinned in the catalog test; here we just confirm they're surfaced.
            val idStats = showStatsRow(table, "id")
            assertThat(idStats.getField(LOW_VALUE)).isNotNull()
            assertThat(idStats.getField(HIGH_VALUE)).isNotNull()
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun analyzeRecomputesDriftedRecordCount() {
        val table = "test_schema.an_drift"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            computeActual("INSERT INTO $table VALUES (1), (2), (3)")

            // Drift the cached record_count directly in the catalog, behind the connector's back.
            setRecordCount("an_drift", 999L)
            assertThat(catalogRecordCount("an_drift"))
                .`as`("sanity: the corruption landed")
                .isEqualTo(999L)

            computeActual("ANALYZE $table")

            // ANALYZE scanned the table (3 live rows) and overwrote the wrong cached value...
            assertThat(catalogRecordCount("an_drift"))
                .`as`("ANALYZE recomputed record_count from the scan, not the stale cache")
                .isEqualTo(3L)
            // ...and the planner now sees the corrected count.
            assertThat(showStatsRowCount(table)).isEqualTo(3.0)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun analyzeEmptyTableSeedsStats() {
        // No data file and no prior ducklake_table_stats row — exercises analyzeTable's INSERT branch.
        val table = "test_schema.an_empty"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            computeActual("ANALYZE $table")
            assertThat(catalogRecordCount("an_empty")).isEqualTo(0L)
            assertThat(showStatsRowCount(table)).isEqualTo(0.0)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun showStatsRowCount(table: String): Double? {
        val summary = computeActual("SHOW STATS FOR $table").materializedRows
                .firstOrNull { it.getField(COLUMN_NAME) == null }
                ?: return null
        return summary.getField(ROW_COUNT) as Double?
    }

    private fun showStatsRow(table: String, column: String) =
            computeActual("SHOW STATS FOR $table").materializedRows
                    .first { it.getField(COLUMN_NAME) == column }

    private fun catalogRecordCount(tableName: String): Long =
            openCatalogConnection().use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val tableId = CatalogQueries.activeTableId(dsl, tableName)
                val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
                dsl.select(tabstats.RECORD_COUNT)
                        .from(tabstats)
                        .where(tabstats.TABLE_ID.eq(tableId))
                        .fetchOne(0, Long::class.java) ?: -1L
            }

    private fun setRecordCount(tableName: String, value: Long) {
        openCatalogConnection().use { conn ->
            val dsl = CatalogTestSupport.dsl(conn)
            val tableId = CatalogQueries.activeTableId(dsl, tableName)
            val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
            dsl.update(tabstats)
                    .set(tabstats.RECORD_COUNT, value)
                    .where(tabstats.TABLE_ID.eq(tableId))
                    .execute()
        }
    }

    private companion object {
        // SHOW STATS column ordinals: column_name, data_size, distinct_values_count,
        // nulls_fraction, row_count, low_value, high_value.
        private const val COLUMN_NAME = 0
        private const val ROW_COUNT = 4
        private const val LOW_VALUE = 5
        private const val HIGH_VALUE = 6
    }
}
