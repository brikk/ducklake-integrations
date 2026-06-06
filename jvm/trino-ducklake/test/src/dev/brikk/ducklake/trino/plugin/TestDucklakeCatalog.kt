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

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorTableVersion
import io.trino.spi.connector.PointerType
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.type.BigintType.BIGINT
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.util.Optional

/**
 * Tests for Ducklake catalog reading from DuckDB-generated test metadata.
 */
class TestDucklakeCatalog {
    private var catalog: DucklakeCatalog? = null

    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {
        catalog = JdbcDucklakeCatalog(DucklakeTestCatalogEnvironment.createDucklakeConfig().toCatalogConfig())
    }

    @AfterEach
    fun tearDown() {
        if (catalog != null) {
            catalog!!.close()
        }
    }

    @Test
    fun testGetTableColumnsResolvesListType() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "array_table", snapshotId)

        val columns = catalog!!.getTableColumns(table.tableId, snapshotId)
        assertThat(columns)
                .extracting(java.util.function.Function<DucklakeColumn, String> { it.columnName })
                .containsExactly("id", "product_name", "tags", "quantity")

        val tagsColumn = columns.stream()
                .filter { column -> column.columnName == "tags" }
                .findFirst()
                .orElseThrow()
        assertThat(tagsColumn.columnType).isEqualTo("list<varchar>")
    }

    @Test
    fun testGetTableColumnsResolvesStructType() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nested_table", snapshotId)

        val columns = catalog!!.getTableColumns(table.tableId, snapshotId)
        assertThat(columns)
                .extracting(java.util.function.Function<DucklakeColumn, String> { it.columnName })
                .containsExactly("id", "metadata", "tags", "nested_list", "complex_struct")

        val metadataColumn = columns.stream()
                .filter { column -> column.columnName == "metadata" }
                .findFirst()
                .orElseThrow()
        assertThat(metadataColumn.columnType).isEqualTo("struct<key:varchar,value:varchar>")
    }

    @Test
    fun testGetTableColumnsResolvesMapType() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nested_table", snapshotId)

        val columns = catalog!!.getTableColumns(table.tableId, snapshotId)

        val tagsColumn = columns.stream()
                .filter { column -> column.columnName == "tags" }
                .findFirst()
                .orElseThrow()
        assertThat(tagsColumn.columnType).isEqualTo("map<varchar,int32>")
    }

    @Test
    fun testGetTableColumnsResolvesNestedListType() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nested_table", snapshotId)

        val columns = catalog!!.getTableColumns(table.tableId, snapshotId)

        val nestedListColumn = columns.stream()
                .filter { column -> column.columnName == "nested_list" }
                .findFirst()
                .orElseThrow()
        assertThat(nestedListColumn.columnType).isEqualTo("list<list<int32>>")
    }

    @Test
    fun testGetTableColumnsResolvesComplexStructType() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nested_table", snapshotId)

        val columns = catalog!!.getTableColumns(table.tableId, snapshotId)

        val complexColumn = columns.stream()
                .filter { column -> column.columnName == "complex_struct" }
                .findFirst()
                .orElseThrow()
        // struct<name:varchar,scores:list<int32>,attrs:map<varchar,varchar>>
        assertThat(complexColumn.columnType)
                .startsWith("struct<")
                .contains("name:varchar")
                .contains("scores:list<int32>")
                .contains("attrs:map<varchar,varchar>")
    }

    @Test
    fun testGetTableStatisticsRowCountAndRanges() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle(
                "test_schema", "simple_table", table.tableId, snapshotId)

        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)

        val stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle)

        // Row count should be 5
        assertThat(stats.rowCount.value).isEqualTo(5.0)

        // Should have column statistics
        assertThat(stats.columnStatistics).isNotEmpty()
    }

    @Test
    fun testGetTableStatisticsTracksNullFractionsAndRanges() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "nullable_table", snapshotId)
        val tableHandle = DucklakeTableHandle(
                "test_schema", "nullable_table", table.tableId, snapshotId)

        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)
        val stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle)

        // nullable_table has 4 rows
        assertThat(stats.rowCount.value).isEqualTo(4.0)

        val handles = metadata.getColumnHandles(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                tableHandle)

        val idStats = stats.columnStatistics[handles["id"]]
        val nameStats = stats.columnStatistics[handles["name"]]
        val priceStats = stats.columnStatistics[handles["price"]]
        assertThat(idStats!!.nullsFraction.value).isEqualTo(0.25)
        assertThat(nameStats!!.nullsFraction.value).isEqualTo(0.5)
        assertThat(priceStats!!.nullsFraction.value).isEqualTo(0.5)

        // non-null prices are 10.0 and 20.0
        val priceRange = priceStats.range.orElseThrow()
        assertThat(priceRange.min).isEqualTo(10.0)
        assertThat(priceRange.max).isEqualTo(20.0)
    }

    @Test
    fun testGetTableStatisticsTracksDateRange() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "simple_table", snapshotId)
        val tableHandle = DucklakeTableHandle(
                "test_schema", "simple_table", table.tableId, snapshotId)

        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)
        val stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle)

        val handles = metadata.getColumnHandles(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                tableHandle)
        val dateStats = stats.columnStatistics[handles["created_date"]]
        val dateRange = dateStats!!.range.orElseThrow()

        val expectedMin = LocalDate.parse("2024-01-05").toEpochDay().toDouble()
        val expectedMax = LocalDate.parse("2024-03-10").toEpochDay().toDouble()
        assertThat(dateRange.min).isEqualTo(expectedMin)
        assertThat(dateRange.max).isEqualTo(expectedMax)
    }

    @Test
    fun testGetTableStatisticsForEmptyTable() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "empty_table", snapshotId)
        val tableHandle = DucklakeTableHandle(
                "test_schema", "empty_table", table.tableId, snapshotId)

        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)
        val stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle)

        assertThat(stats.rowCount.value).isEqualTo(0.0)
        assertThat(stats.columnStatistics).isEmpty()
    }

    @Test
    fun testGetTableStatisticsForInlinedTable() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "inlined_table", snapshotId)
        val tableHandle = DucklakeTableHandle(
                "test_schema", "inlined_table", table.tableId, snapshotId)

        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)
        val stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle)

        assertThat(stats.rowCount.value).isEqualTo(3.0)
    }

    @Test
    fun testGetTableStatisticsSuppressesColumnStatsForMixedInlineAndParquet() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "mixed_inline_table", snapshotId)
        val tableHandle = DucklakeTableHandle(
                "test_schema", "mixed_inline_table", table.tableId, snapshotId)

        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)
        val stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle)

        assertThat(stats.rowCount.value).isEqualTo(7.0)
        assertThat(stats.columnStatistics).isEmpty()
    }

    @Test
    fun testGetTableStatisticsSuppressesColumnStatsWhenDeleteFilesPresent() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "deleted_rows_table", snapshotId)
        val tableHandle = DucklakeTableHandle(
                "test_schema", "deleted_rows_table", table.tableId, snapshotId)

        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)
        val stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle)

        assertThat(stats.rowCount.isUnknown).isTrue()
        assertThat(stats.columnStatistics).isEmpty()
    }

    @Test
    fun testGetTableStatisticsSkipsColumnsWithIncompleteCoverageAfterSchemaEvolution() {
        val snapshotId = catalog!!.currentSnapshotId
        val table = getTable("test_schema", "schema_evolution_table", snapshotId)
        val tableHandle = DucklakeTableHandle(
                "test_schema", "schema_evolution_table", table.tableId, snapshotId)

        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        val metadata = DucklakeMetadata(catalog, typeConverter)
        val stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle)

        assertThat(stats.rowCount.value).isEqualTo(4.0)

        val handles = metadata.getColumnHandles(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                tableHandle)
        assertThat(stats.columnStatistics).containsKey(handles["id"])
        assertThat(stats.columnStatistics).doesNotContainKey(handles["added_col"])
    }

    @Test
    fun testGetTableHandleUsesStartVersionWhenEndVersionMissing() {
        val historicalSnapshotId = getSchemaEvolutionHistoricalSnapshotId()
        val metadata = createMetadata()

        val tableHandle = metadata.getTableHandle(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                SchemaTableName("test_schema", "schema_evolution_table"),
                Optional.of(ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, historicalSnapshotId)),
                Optional.empty())

        assertThat(tableHandle).isInstanceOf(DucklakeTableHandle::class.java)
        assertThat((tableHandle as DucklakeTableHandle).snapshotId).isEqualTo(historicalSnapshotId)
    }

    @Test
    fun testGetTableHandleRejectsVersionRanges() {
        val historicalSnapshotId = getSchemaEvolutionHistoricalSnapshotId()
        val currentSnapshotId = catalog!!.currentSnapshotId
        val metadata = createMetadata()

        val startVersion = ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, historicalSnapshotId)
        val endVersion = ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, currentSnapshotId)

        assertThatThrownBy {
            metadata.getTableHandle(
                    io.trino.testing.connector.TestingConnectorSession.SESSION,
                    SchemaTableName("test_schema", "schema_evolution_table"),
                    Optional.of(startVersion),
                    Optional.of(endVersion))
        }
                .isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("does not support version ranges")
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

    private fun getColumnId(columns: List<DucklakeColumn>, columnName: String): Long {
        return columns.stream()
                .filter { column -> column.columnName == columnName }
                .findFirst()
                .orElseThrow { AssertionError("Missing column: $columnName") }
                .columnId
    }

    private fun createMetadata(): DucklakeMetadata {
        val typeConverter = DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
        return DucklakeMetadata(catalog, typeConverter)
    }

    private fun getSchemaEvolutionHistoricalSnapshotId(): Long {
        val currentSnapshotId = catalog!!.currentSnapshotId
        val schemaEvolutionTable = getTable("test_schema", "schema_evolution_table", currentSnapshotId)
        var historicalSnapshotId = -1L
        var snapshotId = currentSnapshotId
        while (snapshotId >= 1) {
            if (catalog!!.getSnapshot(snapshotId) == null) {
                snapshotId--
                continue
            }
            if (catalog!!.getTable("test_schema", "schema_evolution_table", snapshotId) == null) {
                snapshotId--
                continue
            }
            val recordCount = catalog!!.getDataFiles(schemaEvolutionTable.tableId, snapshotId).stream()
                    .mapToLong(java.util.function.ToLongFunction<DucklakeDataFile> { it.recordCount })
                    .sum()
            if (recordCount == 2L) {
                historicalSnapshotId = snapshotId
                break
            }
            snapshotId--
        }
        assertThat(historicalSnapshotId).isGreaterThan(0)
        return historicalSnapshotId
    }
}
