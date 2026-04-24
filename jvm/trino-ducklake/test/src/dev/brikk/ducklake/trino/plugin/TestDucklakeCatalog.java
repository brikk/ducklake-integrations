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
package dev.brikk.ducklake.trino.plugin;

import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeColumn;
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Ducklake catalog reading from DuckDB-generated test metadata.
 */
public class TestDucklakeCatalog
{
    private DucklakeCatalog catalog;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        catalog = new JdbcDucklakeCatalog(DucklakeTestCatalogEnvironment.createDucklakeConfig().toCatalogConfig());
    }

    @AfterEach
    public void tearDown()
    {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testGetTableColumnsResolvesListType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "array_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);
        assertThat(columns)
                .extracting(DucklakeColumn::columnName)
                .containsExactly("id", "product_name", "tags", "quantity");

        DucklakeColumn tagsColumn = columns.stream()
                .filter(column -> column.columnName().equals("tags"))
                .findFirst()
                .orElseThrow();
        assertThat(tagsColumn.columnType()).isEqualTo("list<varchar>");
    }

    @Test
    public void testGetTableColumnsResolvesStructType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);
        assertThat(columns)
                .extracting(DucklakeColumn::columnName)
                .containsExactly("id", "metadata", "tags", "nested_list", "complex_struct");

        DucklakeColumn metadataColumn = columns.stream()
                .filter(column -> column.columnName().equals("metadata"))
                .findFirst()
                .orElseThrow();
        assertThat(metadataColumn.columnType()).isEqualTo("struct<key:varchar,value:varchar>");
    }

    @Test
    public void testGetTableColumnsResolvesMapType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        DucklakeColumn tagsColumn = columns.stream()
                .filter(column -> column.columnName().equals("tags"))
                .findFirst()
                .orElseThrow();
        assertThat(tagsColumn.columnType()).isEqualTo("map<varchar,int32>");
    }

    @Test
    public void testGetTableColumnsResolvesNestedListType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        DucklakeColumn nestedListColumn = columns.stream()
                .filter(column -> column.columnName().equals("nested_list"))
                .findFirst()
                .orElseThrow();
        assertThat(nestedListColumn.columnType()).isEqualTo("list<list<int32>>");
    }

    @Test
    public void testGetTableColumnsResolvesComplexStructType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        DucklakeColumn complexColumn = columns.stream()
                .filter(column -> column.columnName().equals("complex_struct"))
                .findFirst()
                .orElseThrow();
        // struct<name:varchar,scores:list<int32>,attrs:map<varchar,varchar>>
        assertThat(complexColumn.columnType())
                .startsWith("struct<")
                .contains("name:varchar")
                .contains("scores:list<int32>")
                .contains("attrs:map<varchar,varchar>");
    }

    @Test
    public void testGetTableStatisticsRowCountAndRanges()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "simple_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        // Row count should be 5
        assertThat(stats.getRowCount().getValue()).isEqualTo(5.0);

        // Should have column statistics
        assertThat(stats.getColumnStatistics()).isNotEmpty();
    }

    @Test
    public void testGetTableStatisticsTracksNullFractionsAndRanges()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nullable_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "nullable_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        // nullable_table has 4 rows
        assertThat(stats.getRowCount().getValue()).isEqualTo(4.0);

        Map<String, ColumnHandle> handles = metadata.getColumnHandles(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                tableHandle);

        ColumnStatistics idStats = stats.getColumnStatistics().get(handles.get("id"));
        ColumnStatistics nameStats = stats.getColumnStatistics().get(handles.get("name"));
        ColumnStatistics priceStats = stats.getColumnStatistics().get(handles.get("price"));
        assertThat(idStats.getNullsFraction().getValue()).isEqualTo(0.25);
        assertThat(nameStats.getNullsFraction().getValue()).isEqualTo(0.5);
        assertThat(priceStats.getNullsFraction().getValue()).isEqualTo(0.5);

        // non-null prices are 10.0 and 20.0
        DoubleRange priceRange = priceStats.getRange().orElseThrow();
        assertThat(priceRange.getMin()).isEqualTo(10.0);
        assertThat(priceRange.getMax()).isEqualTo(20.0);
    }

    @Test
    public void testGetTableStatisticsTracksDateRange()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "simple_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        Map<String, ColumnHandle> handles = metadata.getColumnHandles(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                tableHandle);
        ColumnStatistics dateStats = stats.getColumnStatistics().get(handles.get("created_date"));
        DoubleRange dateRange = dateStats.getRange().orElseThrow();

        double expectedMin = LocalDate.parse("2024-01-05").toEpochDay();
        double expectedMax = LocalDate.parse("2024-03-10").toEpochDay();
        assertThat(dateRange.getMin()).isEqualTo(expectedMin);
        assertThat(dateRange.getMax()).isEqualTo(expectedMax);
    }

    @Test
    public void testGetTableStatisticsForEmptyTable()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "empty_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "empty_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        assertThat(stats.getRowCount().getValue()).isEqualTo(0.0);
        assertThat(stats.getColumnStatistics()).isEmpty();
    }

    @Test
    public void testGetTableStatisticsForInlinedTable()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "inlined_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "inlined_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        assertThat(stats.getRowCount().getValue()).isEqualTo(3.0);
    }

    @Test
    public void testGetTableStatisticsSuppressesColumnStatsForMixedInlineAndParquet()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "mixed_inline_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "mixed_inline_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        assertThat(stats.getRowCount().getValue()).isEqualTo(7.0);
        assertThat(stats.getColumnStatistics()).isEmpty();
    }

    @Test
    public void testGetTableStatisticsSuppressesColumnStatsWhenDeleteFilesPresent()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "deleted_rows_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "deleted_rows_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        assertThat(stats.getRowCount().isUnknown()).isTrue();
        assertThat(stats.getColumnStatistics()).isEmpty();
    }

    @Test
    public void testGetTableStatisticsSkipsColumnsWithIncompleteCoverageAfterSchemaEvolution()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "schema_evolution_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "schema_evolution_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        assertThat(stats.getRowCount().getValue()).isEqualTo(4.0);

        Map<String, ColumnHandle> handles = metadata.getColumnHandles(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                tableHandle);
        assertThat(stats.getColumnStatistics()).containsKey(handles.get("id"));
        assertThat(stats.getColumnStatistics()).doesNotContainKey(handles.get("added_col"));
    }

    @Test
    public void testGetTableHandleUsesStartVersionWhenEndVersionMissing()
    {
        long historicalSnapshotId = getSchemaEvolutionHistoricalSnapshotId();
        DucklakeMetadata metadata = createMetadata();

        ConnectorTableHandle tableHandle = metadata.getTableHandle(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                new SchemaTableName("test_schema", "schema_evolution_table"),
                Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, historicalSnapshotId)),
                Optional.empty());

        assertThat(tableHandle).isInstanceOf(DucklakeTableHandle.class);
        assertThat(((DucklakeTableHandle) tableHandle).snapshotId()).isEqualTo(historicalSnapshotId);
    }

    @Test
    public void testGetTableHandleRejectsVersionRanges()
    {
        long historicalSnapshotId = getSchemaEvolutionHistoricalSnapshotId();
        long currentSnapshotId = catalog.getCurrentSnapshotId();
        DucklakeMetadata metadata = createMetadata();

        ConnectorTableVersion startVersion = new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, historicalSnapshotId);
        ConnectorTableVersion endVersion = new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, currentSnapshotId);

        assertThatThrownBy(() -> metadata.getTableHandle(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                new SchemaTableName("test_schema", "schema_evolution_table"),
                Optional.of(startVersion),
                Optional.of(endVersion)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("does not support version ranges");
    }

    private DucklakeSchema getSchema(String schemaName, long snapshotId)
    {
        return catalog.listSchemas(snapshotId).stream()
                .filter(schema -> schema.schemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing schema: " + schemaName));
    }

    private DucklakeTable getTable(String schemaName, String tableName, long snapshotId)
    {
        DucklakeSchema schema = getSchema(schemaName, snapshotId);
        return catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(table -> table.tableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing table: " + schemaName + "." + tableName));
    }

    private long getColumnId(List<DucklakeColumn> columns, String columnName)
    {
        return columns.stream()
                .filter(column -> column.columnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing column: " + columnName))
                .columnId();
    }

    private DucklakeMetadata createMetadata()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        return new DucklakeMetadata(catalog, typeConverter);
    }

    private long getSchemaEvolutionHistoricalSnapshotId()
    {
        long currentSnapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable schemaEvolutionTable = getTable("test_schema", "schema_evolution_table", currentSnapshotId);
        long historicalSnapshotId = -1;
        for (long snapshotId = currentSnapshotId; snapshotId >= 1; snapshotId--) {
            if (catalog.getSnapshot(snapshotId).isEmpty()) {
                continue;
            }
            if (catalog.getTable("test_schema", "schema_evolution_table", snapshotId).isEmpty()) {
                continue;
            }
            long recordCount = catalog.getDataFiles(schemaEvolutionTable.tableId(), snapshotId).stream()
                    .mapToLong(DucklakeDataFile::recordCount)
                    .sum();
            if (recordCount == 2) {
                historicalSnapshotId = snapshotId;
                break;
            }
        }
        assertThat(historicalSnapshotId).isGreaterThan(0);
        return historicalSnapshotId;
    }
}
