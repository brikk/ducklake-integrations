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
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeDeleteFileHandling
{
    @Test
    public void testDeleteFileSuppressesRows()
            throws Exception
    {
        TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
        DucklakeCatalogGenerator.IsolatedCatalog isolated =
                DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(server, "delete-file-" + UUID.randomUUID().toString().substring(0, 8));

        DucklakeConfig config = new DucklakeConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);

        DucklakeCatalog catalog = new JdbcDucklakeCatalog(config.toCatalogConfig());
        try {
            DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config, new DucklakePathResolver(catalog, config), new io.trino.filesystem.cache.NoopSplitAffinityProvider());
            DucklakePageSourceProvider pageSourceProvider = new DucklakePageSourceProvider(
                    new LocalFileSystemFactory(Path.of("/")),
                    new FileFormatDataSourceStats(),
                    new ParquetReaderConfig().toParquetReaderOptions(),
                    catalog,
                    new DucklakeMaterializedFileCache(),
                    DuckDbS3Config.fromCatalogConfig(java.util.Map.of()),
                    config,
                    new DucklakeDuckDbExecutorFactory(config));

            long snapshotId = catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(catalog, "test_schema", "simple_table", snapshotId);
            DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
            long priceColumnId = getColumnId(catalog, table.tableId(), snapshotId, "price");
            DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);

            DucklakeSplit splitBeforeDelete = getSplits(splitManager, tableHandle).getFirst();
            long baselineRows = countRows(pageSourceProvider, tableHandle, splitBeforeDelete, priceColumn);
            assertThat(baselineRows).isGreaterThan(1);

            DucklakeDataFile dataFile = findDataFileForSplit(catalog, table.tableId(), snapshotId, splitBeforeDelete);
            DeleteFilePath deleteFilePath = writeDeleteParquetFile(splitBeforeDelete.dataFilePath(), dataFile.rowIdStart());
            insertDeleteFileMetadata(
                    isolated.jdbcUrl(),
                    isolated.user(),
                    isolated.password(),
                    table.tableId(),
                    snapshotId,
                    dataFile.dataFileId(),
                    deleteFilePath.pathForCatalog(),
                    deleteFilePath.pathIsRelative(),
                    Files.size(deleteFilePath.absolutePath()));

            DucklakeSplit splitAfterDelete = getSplits(splitManager, tableHandle).stream()
                    .filter(split -> split.dataFilePath().equals(splitBeforeDelete.dataFilePath()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected split for data file: " + splitBeforeDelete.dataFilePath()));

            assertThat(splitAfterDelete.deleteFilePath()).isPresent();
            long rowsAfterDelete = countRows(pageSourceProvider, tableHandle, splitAfterDelete, priceColumn);
            assertThat(rowsAfterDelete).isEqualTo(baselineRows - 1);
        }
        finally {
            catalog.close();
        }
    }

    private static List<DucklakeSplit> getSplits(DucklakeSplitManager splitManager, DucklakeTableHandle tableHandle)
            throws Exception
    {
        try (ConnectorSplitSource splitSource = splitManager.getSplits(
                null,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue())) {
            ImmutableList.Builder<DucklakeSplit> splits = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                for (ConnectorSplit split : splitSource.getNextBatch(1000).get().getSplits()) {
                    splits.add((DucklakeSplit) split);
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
                null,
                SESSION,
                split,
                tableHandle,
                java.util.Optional.empty(),
                ImmutableList.of(column),
                DynamicFilter.EMPTY)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    rows += page.getPositionCount();
                }
            }
        }
        return rows;
    }

    private static DucklakeDataFile findDataFileForSplit(DucklakeCatalog catalog, long tableId, long snapshotId, DucklakeSplit split)
    {
        return catalog.getDataFiles(tableId, snapshotId).stream()
                .filter(dataFile -> split.dataFilePath().endsWith(dataFile.path()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing data file metadata for split path: " + split.dataFilePath()));
    }

    private static DeleteFilePath writeDeleteParquetFile(String dataFilePath, long deletedRowId)
            throws Exception
    {
        Path sourcePath = Path.of(dataFilePath);
        Path sourceParent = sourcePath.toAbsolutePath().getParent();
        Path deleteFileName = Path.of("ducklake-delete-" + UUID.randomUUID() + ".parquet");

        if (sourceParent == null) {
            throw new IllegalArgumentException("Cannot resolve delete file parent for path: " + dataFilePath);
        }

        Path deleteAbsolutePath = sourceParent.resolve(deleteFileName);

        Files.createDirectories(deleteAbsolutePath.getParent());
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("COPY (SELECT " + deletedRowId + "::BIGINT AS row_id) TO '" +
                    escapeSql(deleteAbsolutePath.toAbsolutePath().toString()) +
                    "' (FORMAT PARQUET)");
        }

        return new DeleteFilePath(deleteAbsolutePath.toString(), false, deleteAbsolutePath);
    }

    private static void insertDeleteFileMetadata(
            String jdbcUrl,
            String user,
            String password,
            long tableId,
            long snapshotId,
            long dataFileId,
            String deletePath,
            boolean pathIsRelative,
            long fileSizeBytes)
            throws Exception
    {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            DSLContext dsl = CatalogTestSupport.dsl(conn);
            var delfile = DUCKLAKE_DELETE_FILE.as("delfile");

            // Allocate the next id manually because the test bypasses the runtime catalog write
            // path (which owns sequencing). COALESCE handles the empty-table case on the first
            // insert. Race-free for tests because they run serially against an isolated catalog.
            Long nextDeleteFileId = dsl.select(DSL.coalesce(DSL.max(delfile.DELETE_FILE_ID), DSL.inline(0L)).plus(1))
                    .from(delfile)
                    .fetchOne(0, Long.class);

            dsl.insertInto(delfile)
                    .set(delfile.DELETE_FILE_ID, nextDeleteFileId)
                    .set(delfile.TABLE_ID, tableId)
                    .set(delfile.BEGIN_SNAPSHOT, snapshotId)
                    // END_SNAPSHOT, ENCRYPTION_KEY: omitted → SQL NULL (nullable columns).
                    .set(delfile.DATA_FILE_ID, dataFileId)
                    .set(delfile.PATH, deletePath)
                    .set(delfile.PATH_IS_RELATIVE, pathIsRelative)
                    .set(delfile.FORMAT, "parquet")
                    .set(delfile.DELETE_COUNT, 1L)
                    .set(delfile.FILE_SIZE_BYTES, fileSizeBytes)
                    .set(delfile.FOOTER_SIZE, 0L)
                    .execute();
        }
    }

    private static String escapeSql(String value)
    {
        return value.replace("'", "''");
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

    private static long getColumnId(DucklakeCatalog catalog, long tableId, long snapshotId, String columnName)
    {
        return catalog.getTableColumns(tableId, snapshotId).stream()
                .filter(column -> column.columnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing column: " + columnName))
                .columnId();
    }

    private record DeleteFilePath(String pathForCatalog, boolean pathIsRelative, Path absolutePath) {}
}
