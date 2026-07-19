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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager

/**
 * Cross-engine regression for `contains_nan` (terra P1): a connector-written REAL/DOUBLE file that
 * contains NaN must persist `contains_nan = TRUE`. DuckLake's own `DuckLakeColumnStats::ToStats`
 * only builds min/max pruning stats for a float column when it positively knows there is no NaN
 * (`has_contains_nan && !contains_nan`); a SQL-NULL flag is read as "no NaN". Because our writers
 * exclude NaN from min/max, a file recorded as `[1.0, 1.0]` with a NULL flag would be PRUNED by
 * DuckDB for `x > 5` — even though NaN sorts above all finite values in DuckDB and qualifies.
 */
@Execution(ExecutionMode.SAME_THREAD)
internal class TestDucklakeCrossEngineNanStats : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "xengine-nan-stats"

    @Test
    fun trinoWrittenNaNFileIsNotPrunedByDuckDb() {
        computeActual("CREATE TABLE test_schema.nan_prune (x DOUBLE)")
        try {
            // 1.0 sets min/max = [1.0, 1.0] (NaN is excluded from min/max); nan() is the row a
            // `x > 5` predicate must still find in DuckDB.
            computeActual("INSERT INTO test_schema.nan_prune VALUES (DOUBLE '1.0'), (nan())")
            assertRowsWrittenToParquet("nan_prune")

            // The stat that stops DuckDB pruning the file.
            assertThat(containsNanPersisted("nan_prune")).`as`("contains_nan persisted TRUE").isTrue()

            // The end-to-end proof: DuckDB reads the file (not pruned) and NaN > 5 is TRUE there,
            // so the NaN row is returned. With a NULL flag the file would be pruned -> 0 rows.
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT count(*) FROM ducklake_db.test_schema.nan_prune WHERE x > 5").use { rs ->
                        rs.next()
                        assertThat(rs.getLong(1))
                                .`as`("NaN row survives DuckDB file pruning")
                                .isEqualTo(1L)
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.nan_prune")
        }
    }

    /** TRUE iff some active data file for `tableName` records `contains_nan = true`. */
    private fun containsNanPersisted(tableName: String): Boolean {
        val catalog = getIsolatedCatalog()
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { conn ->
            conn.prepareStatement(
                    "SELECT bool_or(fcs.contains_nan) FROM ducklake_file_column_stats fcs "
                            + "JOIN ducklake_table t ON t.table_id = fcs.table_id "
                            + "WHERE t.table_name = ? AND t.end_snapshot IS NULL").use { ps ->
                ps.setString(1, tableName)
                ps.executeQuery().use { rs ->
                    check(rs.next()) { "no stats rows for $tableName" }
                    return rs.getBoolean(1) // SQL NULL -> false
                }
            }
        }
    }
}
