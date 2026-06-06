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
import java.time.Instant
import java.time.LocalDate

class TestJdbcDucklakeCatalogIntegration {
    companion object {
        private var server: TestingDucklakePostgreSqlCatalogServer? = null
        private var catalog: JdbcDucklakeCatalog? = null
        private var snapshotId: Long = 0

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server!!, "catalog-integration")

            val config = DucklakeCatalogConfig().apply {
                catalogDatabaseUrl = isolated.jdbcUrl
                catalogDatabaseUser = isolated.user
                catalogDatabasePassword = isolated.password
                dataPath = isolated.dataDir.toAbsolutePath().toString()
                maxCatalogConnections = 5
            }
            catalog = JdbcDucklakeCatalog(config)
            snapshotId = catalog!!.currentSnapshotId
        }

        @JvmStatic
        @AfterAll
        fun tearDownClass() {
            catalog?.close()
            server?.close()
        }
    }

    @Test
    fun testCurrentSnapshotAndSnapshotLookup() {
        val catalog = catalog!!
        val snapshot = catalog.getSnapshot(snapshotId)!!
        assertThat(snapshotId).isGreaterThan(0)
        assertThat(catalog.getSnapshotAtOrBefore(snapshot.snapshotTime))
            .isNotNull()
            .extracting { it!!.snapshotId }
            .isEqualTo(snapshotId)
    }

    @Test
    fun testGetSnapshotAtOrBeforeMatchesScan() {
        // Regression for the SQL-pushdown rewrite of getSnapshotAtOrBefore (was: materialize the
        // whole snapshot table and filter in Java). It must return the SAME row the scan produced
        // for every probe: the reference here reproduces the old behavior (snapshot_time <= ts,
        // first by snapshot_id DESC) over listSnapshots() — itself ordered by snapshot_id DESC.
        val catalog = catalog!!
        val snapshots = catalog.listSnapshots()
        assertThat(snapshots).isNotEmpty()

        fun scanAtOrBefore(ts: Instant): DucklakeSnapshot? =
            snapshots.firstOrNull { !it.snapshotTime.isAfter(ts) }

        val probes = buildList {
            add(snapshots.last().snapshotTime.minusSeconds(1)) // strictly before earliest -> empty
            snapshots.forEach { add(it.snapshotTime) }          // each exact snapshot time
            add(snapshots.first().snapshotTime.plusSeconds(3600)) // after latest -> latest row
        }
        for (ts in probes) {
            assertThat(catalog.getSnapshotAtOrBefore(ts)?.snapshotId)
                .`as`("getSnapshotAtOrBefore(%s)", ts)
                .isEqualTo(scanAtOrBefore(ts)?.snapshotId)
        }
    }

    @Test
    fun testListSchemasAndTables() {
        val catalog = catalog!!
        assertThat(catalog.listSchemas(snapshotId))
            .extracting(java.util.function.Function<DucklakeSchema, String> { it.schemaName })
            .contains("test_schema")

        val schema = getSchema("test_schema")
        assertThat(catalog.listTables(schema.schemaId, snapshotId))
            .extracting(java.util.function.Function<DucklakeTable, String> { it.tableName })
            .contains("simple_table", "partitioned_table")
    }

    @Test
    fun testGetTableAndDataFiles() {
        val catalog = catalog!!
        val table = getTable("test_schema", "simple_table")
        assertThat(catalog.getTableById(table.tableId, snapshotId))
            .isNotNull()
            .extracting { it!!.tableName }
            .isEqualTo("simple_table")

        assertThat(catalog.getDataFiles(table.tableId, snapshotId))
            .isNotEmpty()
            .allSatisfy { file ->
                assertThat(file.path).isNotBlank()
                assertThat(file.fileFormat).isEqualTo("parquet")
                assertThat(file.recordCount).isGreaterThan(0)
                assertThat(file.fileSizeBytes).isGreaterThan(0)
            }
    }

    @Test
    fun testGetDataFileIdsForPredicate() {
        val catalog = catalog!!
        val table = getTable("test_schema", "simple_table")
        val columns = catalog.getTableColumns(table.tableId, snapshotId)

        val priceColumnId = getColumnId(columns, "price")
        val dateColumnId = getColumnId(columns, "created_date")

        assertThat(
            catalog.findDataFileIdsInRange(
                table.tableId,
                snapshotId,
                ColumnRangePredicate(priceColumnId, "30.0", "30.0"),
            ),
        ).isNotEmpty()
        assertThat(
            catalog.findDataFileIdsInRange(
                table.tableId,
                snapshotId,
                ColumnRangePredicate(priceColumnId, "1000.0", "1000.0"),
            ),
        ).isEmpty()

        assertThat(
            catalog.findDataFileIdsInRange(
                table.tableId,
                snapshotId,
                ColumnRangePredicate(dateColumnId, "2024-02-01", "2024-02-01"),
            ),
        ).isNotEmpty()
        assertThat(
            catalog.findDataFileIdsInRange(
                table.tableId,
                snapshotId,
                ColumnRangePredicate(dateColumnId, "2025-01-01", "2025-01-01"),
            ),
        ).isEmpty()
    }

    @Test
    fun testGetColumnStatsReturnsTypedMinMax() {
        val catalog = catalog!!
        val table = getTable("test_schema", "simple_table")
        val columns = catalog.getTableColumns(table.tableId, snapshotId)
        val statsList = catalog.getColumnStats(table.tableId, snapshotId)

        val priceColumnId = getColumnId(columns, "price")
        val idColumnId = getColumnId(columns, "id")
        val priceStats = statsList.stream()
            .filter { it.columnId == priceColumnId }
            .findFirst()
            .orElseThrow()
        val idStats = statsList.stream()
            .filter { it.columnId == idColumnId }
            .findFirst()
            .orElseThrow()

        assertThat(priceStats.minValue!!.toDouble()).isLessThanOrEqualTo(19.99)
        assertThat(priceStats.maxValue!!.toDouble()).isGreaterThanOrEqualTo(59.99)
        assertThat(idStats.minValue!!.toLong()).isEqualTo(1L)
        assertThat(idStats.maxValue!!.toLong()).isEqualTo(5L)
        assertThat(priceStats.totalValueCount).isEqualTo(5L)
        assertThat(priceStats.totalNullCount).isEqualTo(0L)
    }

    @Test
    fun testPartitionSpecsReturned() {
        val catalog = catalog!!
        val table = getTable("test_schema", "partitioned_table")
        val specs = catalog.getPartitionSpecs(table.tableId, snapshotId)

        assertThat(specs).hasSize(1)
        assertThat(specs.first().fields).hasSize(1)
        assertThat(specs.first().fields.first().transform).isEqualTo(DucklakePartitionTransform.IDENTITY)
    }

    @Test
    fun testFilePartitionValuesReturned() {
        val catalog = catalog!!
        val table = getTable("test_schema", "partitioned_table")
        val values = catalog.getFilePartitionValues(table.tableId, snapshotId)

        assertThat(values).hasSize(3)
        assertThat(values.values)
            .allSatisfy { fileValues ->
                assertThat(fileValues).hasSize(1)
                assertThat(fileValues.first().partitionKeyIndex).isEqualTo(0)
            }
    }

    @Test
    fun testDateRangeStatsCanBeInterpretedAsEpochDays() {
        val catalog = catalog!!
        val table = getTable("test_schema", "simple_table")
        val columns = catalog.getTableColumns(table.tableId, snapshotId)
        val statsList = catalog.getColumnStats(table.tableId, snapshotId)

        val dateColumnId = getColumnId(columns, "created_date")
        val dateStats = statsList.stream()
            .filter { it.columnId == dateColumnId }
            .findFirst()
            .orElseThrow()

        assertThat(LocalDate.parse(dateStats.minValue!!)).isEqualTo(LocalDate.of(2024, 1, 5))
        assertThat(LocalDate.parse(dateStats.maxValue!!)).isEqualTo(LocalDate.of(2024, 3, 10))
    }

    private fun getSchema(schemaName: String): DucklakeSchema {
        return catalog!!.listSchemas(snapshotId).stream()
            .filter { it.schemaName == schemaName }
            .findFirst()
            .orElseThrow { AssertionError("Missing schema: $schemaName") }
    }

    private fun getTable(schemaName: String, tableName: String): DucklakeTable {
        val schema = getSchema(schemaName)
        return catalog!!.listTables(schema.schemaId, snapshotId).stream()
            .filter { it.tableName == tableName }
            .findFirst()
            .orElseThrow { AssertionError("Missing table: $schemaName.$tableName") }
    }

    private fun getColumnId(columns: List<DucklakeColumn>, columnName: String): Long {
        return columns.stream()
            .filter { it.columnName == columnName }
            .findFirst()
            .orElseThrow { AssertionError("Missing column: $columnName") }
            .columnId
    }
}
