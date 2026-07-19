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
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.filesystem.cache.NoopSplitAffinityProvider
import io.trino.filesystem.local.LocalFileSystemFactory
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.connector.DynamicFilterSnapshot
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.Range
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.predicate.ValueSet
import io.trino.spi.type.ArrayType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.Type
import io.trino.spi.type.TypeOperators
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.Optional
import java.util.concurrent.CompletableFuture

class TestDucklakePageSourceProvider {
    private var catalog: DucklakeCatalog? = null
    private lateinit var splitManager: DucklakeSplitManager
    private lateinit var pageSourceProvider: DucklakePageSourceProvider

    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {
        val config = DucklakeTestCatalogEnvironment.createDucklakeConfig()

        val catalog = JdbcDucklakeCatalog(config.toCatalogConfig())
        this.catalog = catalog
        splitManager = DucklakeSplitManager(catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider())
        pageSourceProvider = DucklakePageSourceProvider(
            LocalFileSystemFactory(Path.of("/")),
            FileFormatDataSourceStats(),
            ParquetReaderConfig().toParquetReaderOptions(),
            catalog,
            config)
    }

    @AfterEach
    fun tearDown() {
        catalog?.close()
    }

    @Test
    @Throws(Exception::class)
    fun testParquetPredicatePrunesRowGroups() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")
        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)

        val baseSplit = getSplits(tableHandle).first()

        val matchingSplit = DucklakeSplit(
            baseSplit.dataFilePath,
            baseSplit.deleteFilePaths,
            baseSplit.rowIdStart,
            baseSplit.recordCount,
            baseSplit.fileSizeBytes,
            baseSplit.fileFormat,
            TupleDomain.withColumnDomains(java.util.Map.of(priceColumn, Domain.singleValue(DOUBLE, 30.0))))
        val nonMatchingSplit = DucklakeSplit(
            baseSplit.dataFilePath,
            baseSplit.deleteFilePaths,
            baseSplit.rowIdStart,
            baseSplit.recordCount,
            baseSplit.fileSizeBytes,
            baseSplit.fileFormat,
            TupleDomain.withColumnDomains(java.util.Map.of(priceColumn, Domain.singleValue(DOUBLE, 1000.0))))

        val allRows = countRows(tableHandle, baseSplit, priceColumn)
        val matchingRows = countRows(tableHandle, matchingSplit, priceColumn)
        val nonMatchingRows = countRows(tableHandle, nonMatchingSplit, priceColumn)

        assertThat(allRows).isEqualTo(baseSplit.recordCount)
        assertThat(matchingRows).isEqualTo(allRows)
        assertThat(nonMatchingRows).isEqualTo(0)
    }

    @Test
    @Throws(Exception::class)
    fun testParquetPredicateNoneReturnsNoRows() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")
        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)

        val baseSplit = getSplits(tableHandle).first()
        val noneSplit = DucklakeSplit(
            baseSplit.dataFilePath,
            baseSplit.deleteFilePaths,
            baseSplit.rowIdStart,
            baseSplit.recordCount,
            baseSplit.fileSizeBytes,
            baseSplit.fileFormat,
            TupleDomain.none())

        assertThat(countRows(tableHandle, noneSplit, priceColumn)).isEqualTo(0)
    }

    @Test
    @Throws(Exception::class)
    fun testParquetPredicateForMissingColumnDoesNotFilterRows() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")
        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)

        val baseSplit = getSplits(tableHandle).first()
        val missingColumn = DucklakeColumnHandle(-1, "missing_col", DOUBLE, true)
        val missingColumnPredicateSplit = DucklakeSplit(
            baseSplit.dataFilePath,
            baseSplit.deleteFilePaths,
            baseSplit.rowIdStart,
            baseSplit.recordCount,
            baseSplit.fileSizeBytes,
            baseSplit.fileFormat,
            TupleDomain.withColumnDomains(java.util.Map.of(missingColumn, Domain.singleValue(DOUBLE, 1.0))))

        val allRows = countRows(tableHandle, baseSplit, priceColumn)
        val rowsWithMissingColumnPredicate = countRows(tableHandle, missingColumnPredicateSplit, priceColumn)

        assertThat(allRows).isEqualTo(baseSplit.recordCount)
        assertThat(rowsWithMissingColumnPredicate).isEqualTo(allRows)
    }

    @Test
    @Throws(Exception::class)
    fun testParquetPageSourceSupportsFileUriPath() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")
        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)

        val baseSplit = getSplits(tableHandle).first()
        val fileUriSplit = DucklakeSplit(
            Path.of(baseSplit.dataFilePath).toUri().toString(),
            baseSplit.deleteFilePaths,
            baseSplit.rowIdStart,
            baseSplit.recordCount,
            baseSplit.fileSizeBytes,
            baseSplit.fileFormat,
            baseSplit.fileStatisticsDomain)

        val expectedRows = countRows(tableHandle, baseSplit, priceColumn)
        assertThat(expectedRows).isEqualTo(baseSplit.recordCount)
        assertThat(countRows(tableHandle, fileUriSplit, priceColumn)).isEqualTo(expectedRows)
    }

    @Test
    @Throws(Exception::class)
    fun testReadStructColumn() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nested_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "nested_table", table.tableId, snapshotId)

        val structType: Type = RowType.from(listOf(
            RowType.Field(Optional.of("key"), VARCHAR),
            RowType.Field(Optional.of("value"), VARCHAR)))
        val metadataColumnId = getColumnId(table.tableId, snapshotId, "metadata")
        val metadataColumn = DucklakeColumnHandle(metadataColumnId, "metadata", structType, true)

        val split = getSplits(tableHandle).first()
        assertThat(countRows(tableHandle, split, metadataColumn)).isEqualTo(3)
    }

    @Test
    @Throws(Exception::class)
    fun testReadMapColumn() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nested_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "nested_table", table.tableId, snapshotId)

        val typeOperators = TypeOperators()
        val mapType: Type = MapType(VARCHAR, INTEGER, typeOperators)
        val tagsColumnId = getColumnId(table.tableId, snapshotId, "tags")
        val tagsColumn = DucklakeColumnHandle(tagsColumnId, "tags", mapType, true)

        val split = getSplits(tableHandle).first()
        assertThat(countRows(tableHandle, split, tagsColumn)).isEqualTo(3)
    }

    @Test
    @Throws(Exception::class)
    fun testReadNestedArrayColumn() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nested_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "nested_table", table.tableId, snapshotId)

        val nestedListType: Type = ArrayType(ArrayType(INTEGER))
        val nestedListColumnId = getColumnId(table.tableId, snapshotId, "nested_list")
        val nestedListColumn = DucklakeColumnHandle(nestedListColumnId, "nested_list", nestedListType, true)

        val split = getSplits(tableHandle).first()
        assertThat(countRows(tableHandle, split, nestedListColumn)).isEqualTo(3)
    }

    @Test
    @Throws(Exception::class)
    fun testReadComplexStructColumn() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nested_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "nested_table", table.tableId, snapshotId)

        val typeOperators = TypeOperators()
        val complexStructType: Type = RowType.from(listOf(
            RowType.Field(Optional.of("name"), VARCHAR),
            RowType.Field(Optional.of("scores"), ArrayType(INTEGER)),
            RowType.Field(Optional.of("attrs"), MapType(VARCHAR, VARCHAR, typeOperators))))
        val complexColumnId = getColumnId(table.tableId, snapshotId, "complex_struct")
        val complexColumn = DucklakeColumnHandle(complexColumnId, "complex_struct", complexStructType, true)

        val split = getSplits(tableHandle).first()
        assertThat(countRows(tableHandle, split, complexColumn)).isEqualTo(3)
    }

    @Test
    @Throws(Exception::class)
    fun testDynamicFilterExcludesAllReturnsNoRows() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")
        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)

        val split = getSplits(tableHandle).first()

        // Dynamic filter that excludes all data (price = 999999.0, no such value)
        val exclusiveFilter = createDynamicFilter(
            priceColumn, Domain.singleValue(DOUBLE, 999999.0))

        var rows = 0L
        pageSourceProvider.createPageSource(
            null, SESSION, split, tableHandle, Optional.empty(), ImmutableList.of<ColumnHandle>(priceColumn), exclusiveFilter).use { pageSource ->
            while (!pageSource.isFinished) {
                val page = pageSource.nextSourcePage
                if (page != null) {
                    rows += page.positionCount
                }
            }
        }
        // The fileStatisticsDomain intersected with the dynamic filter should yield NONE
        assertThat(rows).isEqualTo(0)
    }

    @Test
    @Throws(Exception::class)
    fun testDynamicFilterIncludesAllReturnsAllRows() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")
        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)

        val split = getSplits(tableHandle).first()

        // Dynamic filter with a wide range that includes all data
        val wideFilter = createDynamicFilter(
            priceColumn, Domain.create(ValueSet.ofRanges(
                Range.range(DOUBLE, 0.0, true, 100.0, true)), false))

        val baseRows = countRows(tableHandle, split, priceColumn)

        var filteredRows = 0L
        pageSourceProvider.createPageSource(
            null, SESSION, split, tableHandle, Optional.empty(), ImmutableList.of<ColumnHandle>(priceColumn), wideFilter).use { pageSource ->
            while (!pageSource.isFinished) {
                val page = pageSource.nextSourcePage
                if (page != null) {
                    filteredRows += page.positionCount
                }
            }
        }
        assertThat(filteredRows).isEqualTo(baseRows)
    }

    @Test
    @Throws(Exception::class)
    fun testSchemaEvolutionMissingColumnReturnsNulls() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)

        // Request a column that does not exist in the Parquet file
        val missingColumn = DucklakeColumnHandle(-1, "new_column", VARCHAR, true)
        val split = getSplits(tableHandle).first()

        // Should return all rows, with null values for the missing column
        val rows = countRows(tableHandle, split, missingColumn)
        assertThat(rows).isEqualTo(split.recordCount)

        // Verify all values are null
        pageSourceProvider.createPageSource(
            null, SESSION, split, tableHandle, Optional.empty(), ImmutableList.of<ColumnHandle>(missingColumn), DynamicFilter.EMPTY).use { pageSource ->
            while (!pageSource.isFinished) {
                val page = pageSource.nextSourcePage
                if (page != null) {
                    val block = page.getBlock(0)
                    for (i in 0 until block.positionCount) {
                        assertThat(block.isNull(i)).isTrue()
                    }
                }
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testSchemaEvolutionMixedColumns() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
        val priceColumnId = getColumnId(table.tableId, snapshotId, "price")
        val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)
        val missingColumn = DucklakeColumnHandle(-1, "new_column", VARCHAR, true)

        val split = getSplits(tableHandle).first()

        // Read both existing and missing columns together
        val mixedColumns: List<ColumnHandle> = ImmutableList.of(priceColumn, missingColumn)
        var rows = 0L
        pageSourceProvider.createPageSource(
            null, SESSION, split, tableHandle, Optional.empty(), mixedColumns, DynamicFilter.EMPTY).use { pageSource ->
            while (!pageSource.isFinished) {
                val page = pageSource.nextSourcePage
                if (page != null) {
                    rows += page.positionCount
                    // price column (index 0) should have non-null values
                    val priceBlock = page.getBlock(0)
                    // missing column (index 1) should be all nulls
                    val missingBlock = page.getBlock(1)
                    for (i in 0 until page.positionCount) {
                        assertThat(priceBlock.isNull(i)).isFalse()
                        assertThat(missingBlock.isNull(i)).isTrue()
                    }
                }
            }
        }
        assertThat(rows).isEqualTo(split.recordCount)
    }

    @Test
    @Throws(Exception::class)
    fun testInlinedPageSourceReturnsCorrectRows() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "inlined_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "inlined_table", table.tableId, snapshotId)

        // Get the inlined split
        val split = getAnySplits(tableHandle).first()
        assertThat(split).isInstanceOf(DucklakeInlinedSplit::class.java)

        val idColumnId = getColumnId(table.tableId, snapshotId, "id")
        val nameColumnId = getColumnId(table.tableId, snapshotId, "name")
        val valueColumnId = getColumnId(table.tableId, snapshotId, "value")
        val idColumn = DucklakeColumnHandle(idColumnId, "id", INTEGER, true)
        val nameColumn = DucklakeColumnHandle(nameColumnId, "name", VARCHAR, true)
        val valueColumn = DucklakeColumnHandle(valueColumnId, "value", DOUBLE, true)

        var rows = 0L
        pageSourceProvider.createPageSource(
            null, SESSION, split, tableHandle, Optional.empty(), ImmutableList.of<ColumnHandle>(idColumn, nameColumn, valueColumn), DynamicFilter.EMPTY).use { pageSource ->
            while (!pageSource.isFinished) {
                val page = pageSource.nextSourcePage
                if (page != null) {
                    rows += page.positionCount
                }
            }
        }
        assertThat(rows).isEqualTo(3)
    }

    @Test
    @Throws(Exception::class)
    fun testInlinedPageSourceColumnProjection() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "inlined_table", snapshotId)
        val tableHandle = DucklakeTableHandle("test_schema", "inlined_table", table.tableId, snapshotId)

        val split = getAnySplits(tableHandle).first()
        assertThat(split).isInstanceOf(DucklakeInlinedSplit::class.java)

        // Request only the name column
        val nameColumnId = getColumnId(table.tableId, snapshotId, "name")
        val nameColumn = DucklakeColumnHandle(nameColumnId, "name", VARCHAR, true)

        var rows = 0L
        pageSourceProvider.createPageSource(
            null, SESSION, split, tableHandle, Optional.empty(), ImmutableList.of<ColumnHandle>(nameColumn), DynamicFilter.EMPTY).use { pageSource ->
            while (!pageSource.isFinished) {
                val page = pageSource.nextSourcePage
                if (page != null) {
                    rows += page.positionCount
                    // Verify we only get one column
                    assertThat(page.channelCount).isEqualTo(1)
                }
            }
        }
        assertThat(rows).isEqualTo(3)
    }

    @Throws(Exception::class)
    private fun countRows(tableHandle: DucklakeTableHandle, split: DucklakeSplit, column: DucklakeColumnHandle): Long {
        var rows = 0L
        pageSourceProvider.createPageSource(
            null,
            SESSION,
            split,
            tableHandle,
            Optional.empty(),
            ImmutableList.of<ColumnHandle>(column),
            DynamicFilter.EMPTY).use { pageSource ->
            while (!pageSource.isFinished) {
                val page = pageSource.nextSourcePage
                if (page != null) {
                    rows += page.positionCount
                }
            }
        }
        return rows
    }

    @Throws(Exception::class)
    private fun getSplits(tableHandle: DucklakeTableHandle): List<DucklakeSplit> {
        return getAnySplits(tableHandle).stream()
            .map { DucklakeSplit::class.java.cast(it) }
            .collect(com.google.common.collect.ImmutableList.toImmutableList())
    }

    @Throws(Exception::class)
    private fun getAnySplits(tableHandle: DucklakeTableHandle): List<ConnectorSplit> {
        splitManager.getSplits(
            null,
            SESSION,
            tableHandle,
            mutableSetOf(),
            Constraint.alwaysTrue()).use { splitSource ->
            val splits = ImmutableList.builder<ConnectorSplit>()
            while (!splitSource.isFinished) {
                splits.addAll(splitSource.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).get())
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

    companion object {
        private fun createDynamicFilter(column: DucklakeColumnHandle, domain: Domain): DynamicFilter {
            val predicate: TupleDomain<ColumnHandle> = TupleDomain.withColumnDomains(
                java.util.Map.of(column, domain))
            return object : DynamicFilter {
                override fun getColumnsCovered(): MutableSet<ColumnHandle> {
                    return java.util.Set.of<ColumnHandle>(column)
                }

                override fun isBlocked(): CompletableFuture<*> {
                    return DynamicFilter.NOT_BLOCKED
                }

                override fun isComplete(): Boolean = true

                override fun isAwaitable(): Boolean = false

                override fun getCurrentPredicate(): TupleDomain<ColumnHandle> {
                    return predicate
                }
            }
        }
    }
}
