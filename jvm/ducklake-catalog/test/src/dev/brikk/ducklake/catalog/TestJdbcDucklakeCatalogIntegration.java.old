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
package dev.brikk.ducklake.catalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcDucklakeCatalogIntegration
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;
    private static long snapshotId;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "catalog-integration");

        DucklakeCatalogConfig config = new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);
        catalog = new JdbcDucklakeCatalog(config);
        snapshotId = catalog.getCurrentSnapshotId();
    }

    @AfterAll
    public static void tearDownClass()
    {
        if (catalog != null) {
            catalog.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testCurrentSnapshotAndSnapshotLookup()
    {
        DucklakeSnapshot snapshot = catalog.getSnapshot(snapshotId).orElseThrow();
        assertThat(snapshotId).isGreaterThan(0);
        assertThat(catalog.getSnapshotAtOrBefore(snapshot.snapshotTime()))
                .isPresent()
                .get()
                .extracting(DucklakeSnapshot::snapshotId)
                .isEqualTo(snapshotId);
    }

    @Test
    public void testListSchemasAndTables()
    {
        assertThat(catalog.listSchemas(snapshotId))
                .extracting(DucklakeSchema::schemaName)
                .contains("test_schema");

        DucklakeSchema schema = getSchema("test_schema");
        assertThat(catalog.listTables(schema.schemaId(), snapshotId))
                .extracting(DucklakeTable::tableName)
                .contains("simple_table", "partitioned_table");
    }

    @Test
    public void testGetTableAndDataFiles()
    {
        DucklakeTable table = getTable("test_schema", "simple_table");
        assertThat(catalog.getTableById(table.tableId(), snapshotId))
                .isPresent()
                .get()
                .extracting(DucklakeTable::tableName)
                .isEqualTo("simple_table");

        assertThat(catalog.getDataFiles(table.tableId(), snapshotId))
                .isNotEmpty()
                .allSatisfy(file -> {
                    assertThat(file.path()).isNotBlank();
                    assertThat(file.fileFormat()).isEqualTo("parquet");
                    assertThat(file.recordCount()).isGreaterThan(0);
                    assertThat(file.fileSizeBytes()).isGreaterThan(0);
                });
    }

    @Test
    public void testGetDataFileIdsForPredicate()
    {
        DucklakeTable table = getTable("test_schema", "simple_table");
        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        long priceColumnId = getColumnId(columns, "price");
        long dateColumnId = getColumnId(columns, "created_date");

        assertThat(catalog.findDataFileIdsInRange(
                table.tableId(),
                snapshotId,
                new ColumnRangePredicate(priceColumnId, "30.0", "30.0")))
                .isNotEmpty();
        assertThat(catalog.findDataFileIdsInRange(
                table.tableId(),
                snapshotId,
                new ColumnRangePredicate(priceColumnId, "1000.0", "1000.0")))
                .isEmpty();

        assertThat(catalog.findDataFileIdsInRange(
                table.tableId(),
                snapshotId,
                new ColumnRangePredicate(dateColumnId, "2024-02-01", "2024-02-01")))
                .isNotEmpty();
        assertThat(catalog.findDataFileIdsInRange(
                table.tableId(),
                snapshotId,
                new ColumnRangePredicate(dateColumnId, "2025-01-01", "2025-01-01")))
                .isEmpty();
    }

    @Test
    public void testGetColumnStatsReturnsTypedMinMax()
    {
        DucklakeTable table = getTable("test_schema", "simple_table");
        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);
        List<DucklakeColumnStats> statsList = catalog.getColumnStats(table.tableId(), snapshotId);

        long priceColumnId = getColumnId(columns, "price");
        long idColumnId = getColumnId(columns, "id");
        DucklakeColumnStats priceStats = statsList.stream()
                .filter(stat -> stat.columnId() == priceColumnId)
                .findFirst()
                .orElseThrow();
        DucklakeColumnStats idStats = statsList.stream()
                .filter(stat -> stat.columnId() == idColumnId)
                .findFirst()
                .orElseThrow();

        assertThat(Double.parseDouble(priceStats.minValue().orElseThrow())).isLessThanOrEqualTo(19.99);
        assertThat(Double.parseDouble(priceStats.maxValue().orElseThrow())).isGreaterThanOrEqualTo(59.99);
        assertThat(Long.parseLong(idStats.minValue().orElseThrow())).isEqualTo(1L);
        assertThat(Long.parseLong(idStats.maxValue().orElseThrow())).isEqualTo(5L);
        assertThat(priceStats.totalValueCount()).isEqualTo(5L);
        assertThat(priceStats.totalNullCount()).isEqualTo(0L);
    }

    @Test
    public void testPartitionSpecsReturned()
    {
        DucklakeTable table = getTable("test_schema", "partitioned_table");
        List<DucklakePartitionSpec> specs = catalog.getPartitionSpecs(table.tableId(), snapshotId);

        assertThat(specs).hasSize(1);
        assertThat(specs.getFirst().fields()).hasSize(1);
        assertThat(specs.getFirst().fields().getFirst().transform()).isEqualTo(DucklakePartitionTransform.IDENTITY);
    }

    @Test
    public void testFilePartitionValuesReturned()
    {
        DucklakeTable table = getTable("test_schema", "partitioned_table");
        Map<Long, List<DucklakeFilePartitionValue>> values = catalog.getFilePartitionValues(table.tableId(), snapshotId);

        assertThat(values).hasSize(3);
        assertThat(values.values())
                .allSatisfy(fileValues -> {
                    assertThat(fileValues).hasSize(1);
                    assertThat(fileValues.getFirst().partitionKeyIndex()).isEqualTo(0);
                });
    }

    @Test
    public void testDateRangeStatsCanBeInterpretedAsEpochDays()
    {
        DucklakeTable table = getTable("test_schema", "simple_table");
        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);
        List<DucklakeColumnStats> statsList = catalog.getColumnStats(table.tableId(), snapshotId);

        long dateColumnId = getColumnId(columns, "created_date");
        DucklakeColumnStats dateStats = statsList.stream()
                .filter(stat -> stat.columnId() == dateColumnId)
                .findFirst()
                .orElseThrow();

        assertThat(LocalDate.parse(dateStats.minValue().orElseThrow())).isEqualTo(LocalDate.of(2024, 1, 5));
        assertThat(LocalDate.parse(dateStats.maxValue().orElseThrow())).isEqualTo(LocalDate.of(2024, 3, 10));
    }

    private DucklakeSchema getSchema(String schemaName)
    {
        return catalog.listSchemas(snapshotId).stream()
                .filter(schema -> schema.schemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing schema: " + schemaName));
    }

    private DucklakeTable getTable(String schemaName, String tableName)
    {
        DucklakeSchema schema = getSchema(schemaName);
        return catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(table -> table.tableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing table: " + schemaName + "." + tableName));
    }

    private static long getColumnId(List<DucklakeColumn> columns, String columnName)
    {
        return columns.stream()
                .filter(column -> column.columnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing column: " + columnName))
                .columnId();
    }
}
