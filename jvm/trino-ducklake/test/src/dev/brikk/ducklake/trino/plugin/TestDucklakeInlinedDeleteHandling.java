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

import com.google.common.collect.ImmutableList;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.statistics.TableStatistics;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the inlined-deletion read path: DuckLake stores small deletes
 * (below the writer's {@code DATA_INLINING_ROW_LIMIT}) in a per-table
 * metadata table {@code ducklake_inlined_delete_<tableId>}
 * (file_id, row_id, begin_snapshot) instead of as a parquet delete file.
 * The reader must apply those positions as a per-file positional-delete
 * mask at scan time.
 *
 * <p>Tests inject inlined-delete rows directly via JDBC against an isolated
 * catalog, mirroring {@link TestDucklakeDeleteFileHandling}. The split
 * manager + page source provider stack is exercised end-to-end; the
 * stats path is verified separately against the {@link DucklakeMetadata}
 * surface.
 */
public class TestDucklakeInlinedDeleteHandling
{
    @Test
    public void inlinedDeleteSuppressesRows()
            throws Exception
    {
        try (Fixture fx = Fixture.create("inlined-delete-basic")) {
            long snapshotId = fx.catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(fx.catalog, "test_schema", "simple_table", snapshotId);
            DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                    "test_schema", "simple_table", table.tableId(), snapshotId);
            DucklakeColumnHandle priceColumn = priceColumn(fx.catalog, table.tableId(), snapshotId);

            DucklakeSplit baselineSplit = getSplits(fx.splitManager, tableHandle).getFirst();
            long baselineRows = countRows(fx.pageSourceProvider, tableHandle, baselineSplit, priceColumn);
            assertThat(baselineRows).isGreaterThan(1);

            DucklakeDataFile victim = findDataFileForSplit(fx.catalog, table.tableId(), snapshotId, baselineSplit);
            createInlinedDeleteTable(fx.isolated.jdbcUrl(), fx.isolated.user(), fx.isolated.password(), table.tableId());
            insertInlinedDelete(fx.isolated.jdbcUrl(), fx.isolated.user(), fx.isolated.password(),
                    table.tableId(), victim.dataFileId(), 0L, snapshotId);

            DucklakeSplit splitAfter = getSplits(fx.splitManager, tableHandle).stream()
                    .filter(split -> split.dataFilePath().equals(baselineSplit.dataFilePath()))
                    .findFirst()
                    .orElseThrow();
            assertThat(splitAfter.inlinedDeletedRowPositions()).containsExactly(0L);

            long rowsAfter = countRows(fx.pageSourceProvider, tableHandle, splitAfter, priceColumn);
            assertThat(rowsAfter).isEqualTo(baselineRows - 1);
        }
    }

    @Test
    public void inlinedDeletePerFileGranularity()
            throws Exception
    {
        // partitioned_table is partitioned by region (US/EU/APAC), producing one
        // parquet file per partition value. Injecting an inlined delete pointing
        // at one data file must not affect rows in the other files.
        try (Fixture fx = Fixture.create("inlined-delete-per-file")) {
            long snapshotId = fx.catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(fx.catalog, "test_schema", "partitioned_table", snapshotId);
            DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                    "test_schema", "partitioned_table", table.tableId(), snapshotId);
            DucklakeColumnHandle idColumn = idColumn(fx.catalog, table.tableId(), snapshotId);

            List<DucklakeSplit> baselineSplits = getSplits(fx.splitManager, tableHandle);
            assertThat(baselineSplits).hasSizeGreaterThanOrEqualTo(2);

            DucklakeSplit targetSplit = baselineSplits.getFirst();
            DucklakeSplit otherSplit = baselineSplits.get(1);
            long targetBaseline = countRows(fx.pageSourceProvider, tableHandle, targetSplit, idColumn);
            long otherBaseline = countRows(fx.pageSourceProvider, tableHandle, otherSplit, idColumn);
            assertThat(targetBaseline).isGreaterThan(0);
            assertThat(otherBaseline).isGreaterThan(0);

            DucklakeDataFile targetFile = findDataFileForSplit(fx.catalog, table.tableId(), snapshotId, targetSplit);
            createInlinedDeleteTable(fx.isolated.jdbcUrl(), fx.isolated.user(), fx.isolated.password(), table.tableId());
            insertInlinedDelete(fx.isolated.jdbcUrl(), fx.isolated.user(), fx.isolated.password(),
                    table.tableId(), targetFile.dataFileId(), 0L, snapshotId);

            List<DucklakeSplit> splitsAfter = getSplits(fx.splitManager, tableHandle);
            DucklakeSplit targetAfter = splitsAfter.stream()
                    .filter(split -> split.dataFilePath().equals(targetSplit.dataFilePath()))
                    .findFirst()
                    .orElseThrow();
            DucklakeSplit otherAfter = splitsAfter.stream()
                    .filter(split -> split.dataFilePath().equals(otherSplit.dataFilePath()))
                    .findFirst()
                    .orElseThrow();

            assertThat(targetAfter.inlinedDeletedRowPositions()).containsExactly(0L);
            assertThat(otherAfter.inlinedDeletedRowPositions()).isEmpty();

            long targetRowsAfter = countRows(fx.pageSourceProvider, tableHandle, targetAfter, idColumn);
            long otherRowsAfter = countRows(fx.pageSourceProvider, tableHandle, otherAfter, idColumn);
            assertThat(targetRowsAfter).isEqualTo(targetBaseline - 1);
            assertThat(otherRowsAfter).isEqualTo(otherBaseline);
        }
    }

    @Test
    public void inlinedDeleteRespectsTimeTravelSnapshotId()
            throws Exception
    {
        // Deletion has begin_snapshot strictly greater than the read snapshot, so
        // the WHERE begin_snapshot <= snapshotId filter excludes it. The reader
        // must see all rows in the file.
        try (Fixture fx = Fixture.create("inlined-delete-time-travel")) {
            long snapshotId = fx.catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(fx.catalog, "test_schema", "simple_table", snapshotId);
            DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                    "test_schema", "simple_table", table.tableId(), snapshotId);
            DucklakeColumnHandle priceColumn = priceColumn(fx.catalog, table.tableId(), snapshotId);

            DucklakeSplit baselineSplit = getSplits(fx.splitManager, tableHandle).getFirst();
            long baselineRows = countRows(fx.pageSourceProvider, tableHandle, baselineSplit, priceColumn);

            DucklakeDataFile victim = findDataFileForSplit(fx.catalog, table.tableId(), snapshotId, baselineSplit);
            createInlinedDeleteTable(fx.isolated.jdbcUrl(), fx.isolated.user(), fx.isolated.password(), table.tableId());
            // begin_snapshot = snapshotId + 1000 — definitely in the future.
            insertInlinedDelete(fx.isolated.jdbcUrl(), fx.isolated.user(), fx.isolated.password(),
                    table.tableId(), victim.dataFileId(), 0L, snapshotId + 1000);

            // hasInlinedDeletes uses the same begin_snapshot <= filter, so it
            // must return false for the current snapshot.
            assertThat(fx.catalog.hasInlinedDeletes(table.tableId(), snapshotId)).isFalse();

            DucklakeSplit splitAfter = getSplits(fx.splitManager, tableHandle).stream()
                    .filter(split -> split.dataFilePath().equals(baselineSplit.dataFilePath()))
                    .findFirst()
                    .orElseThrow();
            assertThat(splitAfter.inlinedDeletedRowPositions()).isEmpty();

            long rowsAfter = countRows(fx.pageSourceProvider, tableHandle, splitAfter, priceColumn);
            assertThat(rowsAfter).isEqualTo(baselineRows);
        }
    }

    @Test
    public void inlinedDeleteInvalidatesTableStats()
            throws Exception
    {
        try (Fixture fx = Fixture.create("inlined-delete-stats")) {
            long snapshotId = fx.catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(fx.catalog, "test_schema", "simple_table", snapshotId);
            DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                    "test_schema", "simple_table", table.tableId(), snapshotId);

            DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                    io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
            DucklakeMetadata metadata = new DucklakeMetadata(fx.catalog, typeConverter);

            // Baseline: stats should be present with row count + column stats.
            TableStatistics baseline = metadata.getTableStatistics(SESSION, tableHandle);
            assertThat(baseline.getRowCount().isUnknown()).isFalse();
            assertThat(baseline.getColumnStatistics()).isNotEmpty();

            DucklakeDataFile victim = fx.catalog.getDataFiles(table.tableId(), snapshotId).getFirst();
            createInlinedDeleteTable(fx.isolated.jdbcUrl(), fx.isolated.user(), fx.isolated.password(), table.tableId());
            insertInlinedDelete(fx.isolated.jdbcUrl(), fx.isolated.user(), fx.isolated.password(),
                    table.tableId(), victim.dataFileId(), 0L, snapshotId);

            TableStatistics afterStats = metadata.getTableStatistics(SESSION, tableHandle);
            assertThat(afterStats.getRowCount().isUnknown()).isTrue();
            assertThat(afterStats.getColumnStatistics()).isEmpty();
        }
    }

    @Test
    public void absentInlinedDeleteTableProbesCleanly()
            throws Exception
    {
        // Common case: most tables never had a deletion small enough to inline,
        // so ducklake_inlined_delete_<tableId> doesn't exist at all. The probe
        // must return false (and not propagate a SQLException) so the read
        // proceeds normally.
        try (Fixture fx = Fixture.create("inlined-delete-absent")) {
            long snapshotId = fx.catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(fx.catalog, "test_schema", "simple_table", snapshotId);

            assertThat(fx.catalog.hasInlinedDeletes(table.tableId(), snapshotId)).isFalse();
            assertThat(fx.catalog.getInlinedDeletes(table.tableId(), snapshotId)).isEmpty();
        }
    }

    // ==================== helpers ====================

    private static final class Fixture
            implements AutoCloseable
    {
        final DucklakeCatalogGenerator.IsolatedCatalog isolated;
        final DucklakeCatalog catalog;
        final DucklakeSplitManager splitManager;
        final DucklakePageSourceProvider pageSourceProvider;

        private Fixture(
                DucklakeCatalogGenerator.IsolatedCatalog isolated,
                DucklakeCatalog catalog,
                DucklakeSplitManager splitManager,
                DucklakePageSourceProvider pageSourceProvider)
        {
            this.isolated = isolated;
            this.catalog = catalog;
            this.splitManager = splitManager;
            this.pageSourceProvider = pageSourceProvider;
        }

        static Fixture create(String tag)
                throws Exception
        {
            TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
            DucklakeCatalogGenerator.IsolatedCatalog isolated =
                    DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(
                            server, tag + "-" + UUID.randomUUID().toString().substring(0, 8));

            DucklakeConfig config = new DucklakeConfig()
                    .setCatalogDatabaseUrl(isolated.jdbcUrl())
                    .setCatalogDatabaseUser(isolated.user())
                    .setCatalogDatabasePassword(isolated.password())
                    .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                    .setMaxCatalogConnections(5);

            DucklakeCatalog catalog = new JdbcDucklakeCatalog(config.toCatalogConfig());
            DucklakeSplitManager splitManager = new DucklakeSplitManager(
                    catalog, config, new DucklakePathResolver(catalog, config));
            DucklakePageSourceProvider pageSourceProvider = new DucklakePageSourceProvider(
                    new LocalFileSystemFactory(Path.of("/")),
                    new FileFormatDataSourceStats(),
                    new ParquetReaderConfig().toParquetReaderOptions(),
                    catalog,
                    new DucklakeMaterializedFileCache(),
                    DuckDbS3Config.fromCatalogConfig(java.util.Map.of()),
                    config);
            return new Fixture(isolated, catalog, splitManager, pageSourceProvider);
        }

        @Override
        public void close()
        {
            catalog.close();
        }
    }

    private static void createInlinedDeleteTable(String jdbcUrl, String user, String password, long tableId)
            throws Exception
    {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    "CREATE TABLE IF NOT EXISTS ducklake_inlined_delete_%d " +
                            "(file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)",
                    tableId));
        }
    }

    private static void insertInlinedDelete(
            String jdbcUrl, String user, String password,
            long tableId, long dataFileId, long rowId, long beginSnapshot)
            throws Exception
    {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    "INSERT INTO ducklake_inlined_delete_%d VALUES (%d, %d, %d)",
                    tableId, dataFileId, rowId, beginSnapshot));
        }
    }

    private static List<DucklakeSplit> getSplits(DucklakeSplitManager splitManager, DucklakeTableHandle tableHandle)
            throws Exception
    {
        try (ConnectorSplitSource splitSource = splitManager.getSplits(
                null, SESSION, tableHandle, DynamicFilter.EMPTY, Constraint.alwaysTrue())) {
            ImmutableList.Builder<DucklakeSplit> splits = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                for (ConnectorSplit split : splitSource.getNextBatch(1000).get().getSplits()) {
                    if (split instanceof DucklakeSplit ducklakeSplit) {
                        splits.add(ducklakeSplit);
                    }
                }
            }
            return splits.build();
        }
    }

    private static long countRows(
            DucklakePageSourceProvider pageSourceProvider,
            DucklakeTableHandle tableHandle,
            DucklakeSplit split,
            DucklakeColumnHandle column)
            throws Exception
    {
        long rows = 0;
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null, SESSION, split, tableHandle, ImmutableList.of(column), DynamicFilter.EMPTY)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    rows += page.getPositionCount();
                }
            }
        }
        return rows;
    }

    private static DucklakeDataFile findDataFileForSplit(
            DucklakeCatalog catalog, long tableId, long snapshotId, DucklakeSplit split)
    {
        return catalog.getDataFiles(tableId, snapshotId).stream()
                .filter(dataFile -> split.dataFilePath().endsWith(dataFile.path()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing data file metadata for split path: " + split.dataFilePath()));
    }

    private static DucklakeColumnHandle priceColumn(DucklakeCatalog catalog, long tableId, long snapshotId)
    {
        long columnId = catalog.getTableColumns(tableId, snapshotId).stream()
                .filter(column -> column.columnName().equals("price"))
                .findFirst()
                .orElseThrow()
                .columnId();
        return new DucklakeColumnHandle(columnId, "price", DOUBLE, true);
    }

    private static DucklakeColumnHandle idColumn(DucklakeCatalog catalog, long tableId, long snapshotId)
    {
        long columnId = catalog.getTableColumns(tableId, snapshotId).stream()
                .filter(column -> column.columnName().equals("id"))
                .findFirst()
                .orElseThrow()
                .columnId();
        return new DucklakeColumnHandle(columnId, "id", io.trino.spi.type.IntegerType.INTEGER, true);
    }

    private static DucklakeSchema getSchema(DucklakeCatalog catalog, String schemaName, long snapshotId)
    {
        return catalog.listSchemas(snapshotId).stream()
                .filter(schema -> schema.schemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing schema: " + schemaName));
    }

    private static DucklakeTable getTable(DucklakeCatalog catalog, String schemaName, String tableName, long snapshotId)
    {
        DucklakeSchema schema = getSchema(catalog, schemaName, snapshotId);
        return catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(table -> table.tableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing table: " + schemaName + "." + tableName));
    }

}
