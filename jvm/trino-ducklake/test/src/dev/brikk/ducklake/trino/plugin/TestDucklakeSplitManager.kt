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

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableList.toImmutableList
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DoubleType.DOUBLE
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate

class TestDucklakeSplitManager {
    private var catalog: DucklakeCatalog? = null
    private var splitManager: DucklakeSplitManager? = null

    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {
        val config = DucklakeTestCatalogEnvironment.createDucklakeConfig()

        catalog = JdbcDucklakeCatalog(config.toCatalogConfig())
        splitManager = DucklakeSplitManager(catalog, config, DucklakePathResolver(catalog, config), io.trino.filesystem.cache.NoopSplitAffinityProvider())
    }

    @AfterEach
    fun tearDown() {
        if (catalog != null) {
            catalog!!.close()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsWithoutPredicate() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)

        val splits = getSplits(tableHandle, Constraint.alwaysTrue())
        assertThat(splits).hasSize(catalog!!.getDataFiles(table.tableId, snapshotId).size)
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsPrunesByNumericStats() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")

        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)
        val matchingConstraint = Constraint(TupleDomain.withColumnDomains(mapOf(
                (priceColumn as ColumnHandle) to Domain.singleValue(DOUBLE, 30.0))))
        val nonMatchingConstraint = Constraint(TupleDomain.withColumnDomains(mapOf(
                (priceColumn as ColumnHandle) to Domain.singleValue(DOUBLE, 1000.0))))

        val matchingSplits = getSplits(tableHandle, matchingConstraint)
        val nonMatchingSplits = getSplits(tableHandle, nonMatchingConstraint)

        assertThat(matchingSplits).hasSize(catalog!!.getDataFiles(table.tableId, snapshotId).size)
        assertThat(nonMatchingSplits).isEmpty()
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsPrunesByDateStats() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val createdDateColumnId = getColumnId(table.tableId, snapshotId, "created_date")

        val createdDateColumn = DucklakeColumnHandle(createdDateColumnId, "created_date", DATE, true)
        val matchingDate = LocalDate.parse("2024-02-01").toEpochDay()
        val nonMatchingDate = LocalDate.parse("2025-01-01").toEpochDay()

        val matchingConstraint = Constraint(TupleDomain.withColumnDomains(mapOf(
                (createdDateColumn as ColumnHandle) to Domain.singleValue(DATE, matchingDate))))
        val nonMatchingConstraint = Constraint(TupleDomain.withColumnDomains(mapOf(
                (createdDateColumn as ColumnHandle) to Domain.singleValue(DATE, nonMatchingDate))))

        val matchingSplits = getSplits(tableHandle, matchingConstraint)
        val nonMatchingSplits = getSplits(tableHandle, nonMatchingConstraint)

        assertThat(matchingSplits).hasSize(catalog!!.getDataFiles(table.tableId, snapshotId).size)
        assertThat(nonMatchingSplits).isEmpty()
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsCarryFileStatisticsDomain() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")
        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)
        val expectedDomain: TupleDomain<DucklakeColumnHandle> = TupleDomain.withColumnDomains(mapOf(priceColumn to Domain.singleValue(DOUBLE, 30.0)))
        val constraint = Constraint(TupleDomain.withColumnDomains(mapOf(
                (priceColumn as ColumnHandle) to Domain.singleValue(DOUBLE, 30.0))))

        val splits = getSplits(tableHandle, constraint)

        assertThat(splits).hasSize(catalog!!.getDataFiles(table.tableId, snapshotId).size)
        assertThat(splits)
                .allSatisfy { split -> assertThat(split.fileStatisticsDomain()).isEqualTo(expectedDomain) }
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsWithAlwaysTrueConstraintCarryAllDomain() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)

        val splits = getSplits(tableHandle, Constraint.alwaysTrue())

        assertThat(splits).hasSize(catalog!!.getDataFiles(table.tableId, snapshotId).size)
        assertThat(splits)
                .allSatisfy { split -> assertThat(split.fileStatisticsDomain().isAll).isTrue() }
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsReturnsInlinedSplitForInlinedTable() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "inlined_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "inlined_table", table.tableId, snapshotId)

        val splits = getRawSplits(tableHandle, Constraint.alwaysTrue())
        assertThat(splits).hasSize(1)
        assertThat(splits.first()).isInstanceOf(DucklakeInlinedSplit::class.java)

        val inlinedSplit = splits.first() as DucklakeInlinedSplit
        assertThat(inlinedSplit.tableId()).isEqualTo(table.tableId)
        assertThat(inlinedSplit.snapshotId()).isEqualTo(snapshotId)
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsReturnsFileSplitsForFlushedTable() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)

        val splits = getRawSplits(tableHandle, Constraint.alwaysTrue())
        assertThat(splits).hasSize(catalog!!.getDataFiles(table.tableId, snapshotId).size)
        assertThat(splits).allSatisfy { split -> assertThat(split).isInstanceOf(DucklakeSplit::class.java) }
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsCarryFooterSizeHintFromCatalog() {
        // End-to-end wiring check for the Parquet footer_size hint — the read-path
        // optimization that lets Trino skip its 48 KB blind tail read. DucklakeDataFile
        // pulls footer_size from the catalog, and DucklakeSplitManager copies it onto
        // the DucklakeSplit so the worker can feed it to
        // FooterPrefetchingParquetDataSource. If this field gets dropped anywhere in
        // that chain, every read falls back to the default path silently — tests still
        // pass, S3 bills quietly double. This test pins every link.
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)

        val splits = getSplits(tableHandle, Constraint.alwaysTrue())

        // Sanity: the catalog row really does have a footer size — otherwise this test
        // would pass vacuously against a corrupt fixture.
        assertThat(catalog!!.getDataFiles(table.tableId, snapshotId))
                .allSatisfy { dataFile ->
                    assertThat(dataFile.footerSize)
                            .`as`("catalog footer_size for data file %s", dataFile.path)
                            .isPositive()
                }

        assertThat(splits)
                .isNotEmpty()
                .allSatisfy { split ->
                    assertThat(split.footerSize())
                            .`as`("split footer_size hint for %s", split.dataFilePath())
                            .isPositive()
                }
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsReturnsEmptyForEmptyTable() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "empty_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "empty_table", table.tableId, snapshotId)

        val splits = getRawSplits(tableHandle, Constraint.alwaysTrue())
        assertThat(splits).hasSize(1)
        assertThat(splits.first()).isInstanceOf(DucklakeInlinedSplit::class.java)

        val inlinedSplit = splits.first() as DucklakeInlinedSplit
        assertThat(inlinedSplit.tableId()).isEqualTo(table.tableId)
        assertThat(inlinedSplit.snapshotId()).isEqualTo(snapshotId)

        val columns = catalog!!.getTableColumns(table.tableId, snapshotId)
        val rows = catalog!!.readInlinedData(
                inlinedSplit.tableId(),
                inlinedSplit.schemaVersion(),
                inlinedSplit.snapshotId(),
                columns)
        assertThat(rows).isEmpty()
    }

    @Test
    @Throws(Exception::class)
    fun testGetSplitsReturnsParquetAndInlinedSplitsForMixedTable() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "mixed_inline_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "mixed_inline_table", table.tableId, snapshotId)

        val splits = getRawSplits(tableHandle, Constraint.alwaysTrue())
        val parquetSplitCount = splits.stream().filter { it is DucklakeSplit }.count()
        val inlinedSplitCount = splits.stream().filter { it is DucklakeInlinedSplit }.count()

        assertThat(parquetSplitCount).isEqualTo(catalog!!.getDataFiles(table.tableId, snapshotId).size.toLong())
        assertThat(parquetSplitCount).isGreaterThan(0)
        assertThat(inlinedSplitCount).isEqualTo(1)
    }

    @Test
    @Throws(Exception::class)
    fun testSplitStorageModesAreIntentionalForRepresentativeTables() {
        val snapshotId = catalog!!.currentSnapshotId

        assertSplitStorageMode(snapshotId, "simple_table", true, false)
        assertSplitStorageMode(snapshotId, "multi_file_table", true, false)
        assertSplitStorageMode(snapshotId, "inlined_table", false, true)
        assertSplitStorageMode(snapshotId, "inlined_nullable_table", false, true)
        assertSplitStorageMode(snapshotId, "mixed_inline_table", true, true)
        assertSplitStorageMode(snapshotId, "empty_table", false, true)
    }

    @Throws(Exception::class)
    private fun getSplits(tableHandle: DucklakeTableHandle, constraint: Constraint): List<DucklakeSplit> {
        return getRawSplits(tableHandle, constraint).stream()
                .map { it as DucklakeSplit }
                .collect(toImmutableList())
    }

    @Throws(Exception::class)
    private fun getRawSplits(tableHandle: DucklakeTableHandle, constraint: Constraint): List<ConnectorSplit> {
        splitManager!!.getSplits(
                null,
                null,
                tableHandle,
                DynamicFilter.EMPTY,
                constraint).use { splitSource ->
            val splits: ImmutableList.Builder<ConnectorSplit> = ImmutableList.builder()
            while (!splitSource.isFinished) {
                splits.addAll(splitSource.getNextBatch(1000).get().splits)
            }
            return splits.build()
        }
    }

    private fun getSchema(schemaName: String, snapshotId: Long): DucklakeSchema {
        return catalog!!.listSchemas(snapshotId).stream()
                .filter { schema -> schema.schemaName == schemaName }
                .findFirst()
                .orElseThrow { AssertionError("Missing schema: $schemaName") }
    }

    private fun getTable(schemaName: String, tableName: String, snapshotId: Long): DucklakeTable {
        val schema = getSchema(schemaName, snapshotId)
        return catalog!!.listTables(schema.schemaId, snapshotId).stream()
                .filter { table -> table.tableName == tableName }
                .findFirst()
                .orElseThrow { AssertionError("Missing table: $schemaName.$tableName") }
    }

    private fun getColumnId(tableId: Long, snapshotId: Long, columnName: String): Long {
        return catalog!!.getTableColumns(tableId, snapshotId).stream()
                .filter { column -> column.columnName == columnName }
                .findFirst()
                .orElseThrow { AssertionError("Missing column: $columnName") }
                .columnId
    }

    @Throws(Exception::class)
    private fun assertSplitStorageMode(snapshotId: Long, tableName: String, expectParquetSplit: Boolean, expectInlinedSplit: Boolean) {
        val table = getTable("test_schema", tableName, snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", tableName, table.tableId, snapshotId)
        val splits = getRawSplits(tableHandle, Constraint.alwaysTrue())
        val parquetSplits = splits.stream().filter { it is DucklakeSplit }.count()
        val inlinedSplits = splits.stream().filter { it is DucklakeInlinedSplit }.count()

        if (expectParquetSplit) {
            assertThat(parquetSplits).`as`("parquet split count for %s", tableName).isGreaterThan(0)
        }
        else {
            assertThat(parquetSplits).`as`("parquet split count for %s", tableName).isZero()
        }

        if (expectInlinedSplit) {
            assertThat(inlinedSplits).`as`("inlined split count for %s", tableName).isGreaterThan(0)
        }
        else {
            assertThat(inlinedSplits).`as`("inlined split count for %s", tableName).isZero()
        }
    }
}
