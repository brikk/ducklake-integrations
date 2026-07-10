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

import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.DATA_FILE_FORMAT
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_DUCKDB
import io.trino.Session
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.util.UUID

/**
 * End-to-end proof of *sorted table writes*: when a DuckLake table carries a sort spec
 * (only settable via DuckDB `ALTER TABLE ... SET SORTED BY (...)` — there is no Trino
 * `sorted_by` property), a Trino INSERT physically orders the rows it writes into each
 * parquet data file by that spec's honored leading prefix.
 *
 *
 * DuckDB writes the sort metadata; Trino writes the data. We prove the physical order by
 * reading the hidden `$file_row_number` virtual (the row's position within its parquet
 * file) and asserting the sort column is monotonic when read back in file order. Each test
 * inserts values in a DELIBERATELY non-sorted order in a single INSERT, and asserts the
 * INSERT produced exactly one data file so `$file_row_number` alone linearizes the file.
 *
 *
 * The regression tests pin the gate: an unsorted table, a sorted+partitioned table, and a
 * sorted duckdb-format table must all keep working (the last two silently fall back to the
 * existing unsorted path) and return correct data.
 *
 * SAME_THREAD (like every sibling cross-engine test): the suite runs JUnit methods concurrently
 * by default, but all methods here write to ONE shared per-class catalog. Without serialization,
 * concurrent INSERT commits + the cross-engine DuckDB CREATE/ALTER commits race the snapshot
 * lineage and can surface a TRANSACTION_CONFLICT as a QueryFailedException under full-suite load.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeSortedWrites : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String {
        return "sorted-writes"
    }

    @Test
    fun testInsertSortsRowsAscending() {
        val table = uniqueTable("sorted_asc")
        val fullDuckdb = "ducklake_db.test_schema.$table"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, name VARCHAR)")
                    stmt.execute("ALTER TABLE $fullDuckdb SET SORTED BY (id ASC)")
                }
            }

            // Rows deliberately out of order in one INSERT (one data file).
            computeActual("INSERT INTO test_schema.$table VALUES (3, 'c'), (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd')")

            assertSingleDataFile(table)
            val ids = idsInFileOrder(table)
            assertThat(ids).containsExactly(1, 2, 3, 4, 5)
        }
        finally {
            dropDuckdbTable(fullDuckdb)
        }
    }

    @Test
    fun testInsertSortsRowsDescending() {
        val table = uniqueTable("sorted_desc")
        val fullDuckdb = "ducklake_db.test_schema.$table"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, name VARCHAR)")
                    stmt.execute("ALTER TABLE $fullDuckdb SET SORTED BY (id DESC)")
                }
            }

            computeActual("INSERT INTO test_schema.$table VALUES (3, 'c'), (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd')")

            assertSingleDataFile(table)
            val ids = idsInFileOrder(table)
            assertThat(ids).containsExactly(5, 4, 3, 2, 1)
        }
        finally {
            dropDuckdbTable(fullDuckdb)
        }
    }

    @Test
    fun testInsertSortsByTwoKeys() {
        val table = uniqueTable("sorted_two_key")
        val fullDuckdb = "ducklake_db.test_schema.$table"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (grp INTEGER, val INTEGER)")
                    stmt.execute("ALTER TABLE $fullDuckdb SET SORTED BY (grp ASC, val DESC)")
                }
            }

            computeActual("INSERT INTO test_schema.$table VALUES "
                    + "(2, 10), (1, 5), (2, 30), (1, 20), (2, 20), (1, 5)")

            assertSingleDataFile(table)
            // Expected physical order: grp ASC, then val DESC within each grp.
            val pairs = computeActual(
                    "SELECT grp, val FROM test_schema.$table ORDER BY \"\$file_row_number\"")
                    .materializedRows
                    .map { Pair(it.getField(0) as Int, it.getField(1) as Int) }
            assertThat(pairs).containsExactly(
                    Pair(1, 20), Pair(1, 5), Pair(1, 5),
                    Pair(2, 30), Pair(2, 20), Pair(2, 10))
        }
        finally {
            dropDuckdbTable(fullDuckdb)
        }
    }

    @Test
    fun testUnsortedTableInsertReturnsCorrectData() {
        // Regression: a table with NO sort spec must keep the existing (unsorted) write path.
        val table = uniqueTable("unsorted")
        val fullDuckdb = "ducklake_db.test_schema.$table"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, name VARCHAR)")
                }
            }

            computeActual("INSERT INTO test_schema.$table VALUES (3, 'c'), (1, 'a'), (2, 'b')")

            val result = computeActual("SELECT id, name FROM test_schema.$table ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3L)
            assertThat(result.materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1, 2, 3)
        }
        finally {
            dropDuckdbTable(fullDuckdb)
        }
    }

    @Test
    fun testSortedPartitionedTableFallsBackToUnsorted() {
        // Regression: partitioned + sorted is outside the gate. The write must still succeed
        // (falling back to the existing partitioned, unsorted path) and return correct data.
        val table = uniqueTable("sorted_partitioned")
        val fullDuckdb = "ducklake_db.test_schema.$table"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, region VARCHAR)")
                    stmt.execute("ALTER TABLE $fullDuckdb SET PARTITIONED BY (region)")
                    stmt.execute("ALTER TABLE $fullDuckdb SET SORTED BY (id ASC)")
                }
            }

            computeActual("INSERT INTO test_schema.$table VALUES "
                    + "(3, 'us'), (1, 'eu'), (2, 'us'), (4, 'eu')")

            val result = computeActual("SELECT id, region FROM test_schema.$table ORDER BY id")
            assertThat(result.rowCount).isEqualTo(4L)
            assertThat(result.materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1, 2, 3, 4)
        }
        finally {
            dropDuckdbTable(fullDuckdb)
        }
    }

    @Test
    fun testSortedDuckDbFormatTableFallsBackToUnsorted() {
        // Regression: a sorted table written in duckdb format is outside the gate (only parquet
        // sorts). The write must still succeed and return correct data.
        val table = uniqueTable("sorted_duckdb_fmt")
        val fullDuckdb = "ducklake_db.test_schema.$table"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, name VARCHAR)")
                    stmt.execute("ALTER TABLE $fullDuckdb SET SORTED BY (id ASC)")
                }
            }

            val duckSession: Session = Session.builder(session)
                    .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                    .build()
            computeActual(duckSession,
                    "INSERT INTO test_schema.$table VALUES (3, 'c'), (1, 'a'), (2, 'b')")

            val result = computeActual("SELECT id, name FROM test_schema.$table ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3L)
            assertThat(result.materializedRows.map { it.getField(0) as Int })
                    .containsExactly(1, 2, 3)
        }
        finally {
            dropDuckdbTable(fullDuckdb)
        }
    }

    private fun idsInFileOrder(table: String): List<Int> =
            computeActual("SELECT id FROM test_schema.$table ORDER BY \"\$file_row_number\"")
                    .materializedRows
                    .map { it.getField(0) as Int }

    private fun assertSingleDataFile(table: String) {
        val fileCount = computeScalar("SELECT count(*) FROM \"$table\$files\"") as Long
        assertThat(fileCount)
                .`as`("INSERT should produce exactly one parquet data file for %s", table)
                .isEqualTo(1L)
    }

    private fun uniqueTable(prefix: String): String =
            prefix + "_" + UUID.randomUUID().toString().substring(0, 8)

    private fun dropDuckdbTable(fullName: String) {
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullName")
                }
            }
        }
        catch (ignored: Exception) {
        }
    }
}
