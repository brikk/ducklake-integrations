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
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeSplitManager
{
    private DucklakeCatalog catalog;
    private DucklakeSplitManager splitManager;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        DucklakeConfig config = DucklakeTestCatalogEnvironment.createDucklakeConfig();

        catalog = new JdbcDucklakeCatalog(config.toCatalogConfig());
        splitManager = new DucklakeSplitManager(catalog, config, new DucklakePathResolver(catalog, config));
    }

    @AfterEach
    public void tearDown()
    {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testGetSplitsWithoutPredicate()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

        List<DucklakeSplit> splits = getSplits(tableHandle, Constraint.alwaysTrue());
        assertThat(splits).hasSize(catalog.getDataFiles(table.tableId(), snapshotId).size());
    }

    @Test
    public void testGetSplitsPrunesByNumericStats()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");

        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);
        Constraint matchingConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) priceColumn, Domain.singleValue(DOUBLE, 30.0))));
        Constraint nonMatchingConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) priceColumn, Domain.singleValue(DOUBLE, 1000.0))));

        List<DucklakeSplit> matchingSplits = getSplits(tableHandle, matchingConstraint);
        List<DucklakeSplit> nonMatchingSplits = getSplits(tableHandle, nonMatchingConstraint);

        assertThat(matchingSplits).hasSize(catalog.getDataFiles(table.tableId(), snapshotId).size());
        assertThat(nonMatchingSplits).isEmpty();
    }

    @Test
    public void testGetSplitsPrunesByDateStats()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long createdDateColumnId = getColumnId(table.tableId(), snapshotId, "created_date");

        DucklakeColumnHandle createdDateColumn = new DucklakeColumnHandle(createdDateColumnId, "created_date", DATE, true);
        long matchingDate = LocalDate.parse("2024-02-01").toEpochDay();
        long nonMatchingDate = LocalDate.parse("2025-01-01").toEpochDay();

        Constraint matchingConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) createdDateColumn, Domain.singleValue(DATE, matchingDate))));
        Constraint nonMatchingConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) createdDateColumn, Domain.singleValue(DATE, nonMatchingDate))));

        List<DucklakeSplit> matchingSplits = getSplits(tableHandle, matchingConstraint);
        List<DucklakeSplit> nonMatchingSplits = getSplits(tableHandle, nonMatchingConstraint);

        assertThat(matchingSplits).hasSize(catalog.getDataFiles(table.tableId(), snapshotId).size());
        assertThat(nonMatchingSplits).isEmpty();
    }

    @Test
    public void testGetSplitsCarryFileStatisticsDomain()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);
        TupleDomain<DucklakeColumnHandle> expectedDomain = TupleDomain.withColumnDomains(Map.of(priceColumn, Domain.singleValue(DOUBLE, 30.0)));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) priceColumn, Domain.singleValue(DOUBLE, 30.0))));

        List<DucklakeSplit> splits = getSplits(tableHandle, constraint);

        assertThat(splits).hasSize(catalog.getDataFiles(table.tableId(), snapshotId).size());
        assertThat(splits)
                .allSatisfy(split -> assertThat(split.fileStatisticsDomain()).isEqualTo(expectedDomain));
    }

    @Test
    public void testGetSplitsWithAlwaysTrueConstraintCarryAllDomain()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

        List<DucklakeSplit> splits = getSplits(tableHandle, Constraint.alwaysTrue());

        assertThat(splits).hasSize(catalog.getDataFiles(table.tableId(), snapshotId).size());
        assertThat(splits)
                .allSatisfy(split -> assertThat(split.fileStatisticsDomain().isAll()).isTrue());
    }

    @Test
    public void testGetSplitsReturnsInlinedSplitForInlinedTable()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "inlined_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "inlined_table", table.tableId(), snapshotId);

        List<ConnectorSplit> splits = getRawSplits(tableHandle, Constraint.alwaysTrue());
        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst()).isInstanceOf(DucklakeInlinedSplit.class);

        DucklakeInlinedSplit inlinedSplit = (DucklakeInlinedSplit) splits.getFirst();
        assertThat(inlinedSplit.tableId()).isEqualTo(table.tableId());
        assertThat(inlinedSplit.snapshotId()).isEqualTo(snapshotId);
    }

    @Test
    public void testGetSplitsReturnsFileSplitsForFlushedTable()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

        List<ConnectorSplit> splits = getRawSplits(tableHandle, Constraint.alwaysTrue());
        assertThat(splits).hasSize(catalog.getDataFiles(table.tableId(), snapshotId).size());
        assertThat(splits).allSatisfy(split -> assertThat(split).isInstanceOf(DucklakeSplit.class));
    }

    @Test
    public void testGetSplitsCarryFooterSizeHintFromCatalog()
            throws Exception
    {
        // End-to-end wiring check for the Parquet footer_size hint — the read-path
        // optimization that lets Trino skip its 48 KB blind tail read. DucklakeDataFile
        // pulls footer_size from the catalog, and DucklakeSplitManager copies it onto
        // the DucklakeSplit so the worker can feed it to
        // FooterPrefetchingParquetDataSource. If this field gets dropped anywhere in
        // that chain, every read falls back to the default path silently — tests still
        // pass, S3 bills quietly double. This test pins every link.
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

        List<DucklakeSplit> splits = getSplits(tableHandle, Constraint.alwaysTrue());

        // Sanity: the catalog row really does have a footer size — otherwise this test
        // would pass vacuously against a corrupt fixture.
        assertThat(catalog.getDataFiles(table.tableId(), snapshotId))
                .allSatisfy(dataFile -> assertThat(dataFile.footerSize())
                        .as("catalog footer_size for data file %s", dataFile.path())
                        .isPositive());

        assertThat(splits)
                .isNotEmpty()
                .allSatisfy(split -> assertThat(split.footerSize())
                        .as("split footer_size hint for %s", split.dataFilePath())
                        .isPositive());
    }

    @Test
    public void testGetSplitsReturnsEmptyForEmptyTable()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "empty_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "empty_table", table.tableId(), snapshotId);

        List<ConnectorSplit> splits = getRawSplits(tableHandle, Constraint.alwaysTrue());
        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst()).isInstanceOf(DucklakeInlinedSplit.class);

        DucklakeInlinedSplit inlinedSplit = (DucklakeInlinedSplit) splits.getFirst();
        assertThat(inlinedSplit.tableId()).isEqualTo(table.tableId());
        assertThat(inlinedSplit.snapshotId()).isEqualTo(snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);
        List<List<Object>> rows = catalog.readInlinedData(
                inlinedSplit.tableId(),
                inlinedSplit.schemaVersion(),
                inlinedSplit.snapshotId(),
                columns);
        assertThat(rows).isEmpty();
    }

    @Test
    public void testGetSplitsReturnsParquetAndInlinedSplitsForMixedTable()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "mixed_inline_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "mixed_inline_table", table.tableId(), snapshotId);

        List<ConnectorSplit> splits = getRawSplits(tableHandle, Constraint.alwaysTrue());
        long parquetSplitCount = splits.stream().filter(DucklakeSplit.class::isInstance).count();
        long inlinedSplitCount = splits.stream().filter(DucklakeInlinedSplit.class::isInstance).count();

        assertThat(parquetSplitCount).isEqualTo(catalog.getDataFiles(table.tableId(), snapshotId).size());
        assertThat(parquetSplitCount).isGreaterThan(0);
        assertThat(inlinedSplitCount).isEqualTo(1);
    }

    @Test
    public void testSplitStorageModesAreIntentionalForRepresentativeTables()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();

        assertSplitStorageMode(snapshotId, "simple_table", true, false);
        assertSplitStorageMode(snapshotId, "multi_file_table", true, false);
        assertSplitStorageMode(snapshotId, "inlined_table", false, true);
        assertSplitStorageMode(snapshotId, "inlined_nullable_table", false, true);
        assertSplitStorageMode(snapshotId, "mixed_inline_table", true, true);
        assertSplitStorageMode(snapshotId, "empty_table", false, true);
    }

    private List<DucklakeSplit> getSplits(DucklakeTableHandle tableHandle, Constraint constraint)
            throws Exception
    {
        return getRawSplits(tableHandle, constraint).stream()
                .map(DucklakeSplit.class::cast)
                .collect(toImmutableList());
    }

    private List<ConnectorSplit> getRawSplits(DucklakeTableHandle tableHandle, Constraint constraint)
            throws Exception
    {
        try (ConnectorSplitSource splitSource = splitManager.getSplits(
                null,
                null,
                tableHandle,
                DynamicFilter.EMPTY,
                constraint)) {
            com.google.common.collect.ImmutableList.Builder<ConnectorSplit> splits = com.google.common.collect.ImmutableList.builder();
            while (!splitSource.isFinished()) {
                splits.addAll(splitSource.getNextBatch(1000).get().getSplits());
            }
            return splits.build();
        }
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

    private long getColumnId(long tableId, long snapshotId, String columnName)
    {
        return catalog.getTableColumns(tableId, snapshotId).stream()
                .filter(column -> column.columnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing column: " + columnName))
                .columnId();
    }

    private void assertSplitStorageMode(long snapshotId, String tableName, boolean expectParquetSplit, boolean expectInlinedSplit)
            throws Exception
    {
        DucklakeTable table = getTable("test_schema", tableName, snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", tableName, table.tableId(), snapshotId);
        List<ConnectorSplit> splits = getRawSplits(tableHandle, Constraint.alwaysTrue());
        long parquetSplits = splits.stream().filter(DucklakeSplit.class::isInstance).count();
        long inlinedSplits = splits.stream().filter(DucklakeInlinedSplit.class::isInstance).count();

        if (expectParquetSplit) {
            assertThat(parquetSplits).as("parquet split count for %s", tableName).isGreaterThan(0);
        }
        else {
            assertThat(parquetSplits).as("parquet split count for %s", tableName).isZero();
        }

        if (expectInlinedSplit) {
            assertThat(inlinedSplits).as("inlined split count for %s", tableName).isGreaterThan(0);
        }
        else {
            assertThat(inlinedSplits).as("inlined split count for %s", tableName).isZero();
        }
    }
}
