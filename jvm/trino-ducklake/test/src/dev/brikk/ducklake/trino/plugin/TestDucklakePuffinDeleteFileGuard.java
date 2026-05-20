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
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.UUID;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE;
import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the delete-file-format gate in {@code DucklakeSplitManager.validateDeleteFileFormats}.
 * {@code parquet} and {@code puffin} are accepted (the actual decode happens in the page
 * source, exercised by {@code TestDucklakeCrossEnginePuffinDeleteRoundTrip}); any other
 * format hard-fails the query rather than silently dropping deletes.
 */
public class TestDucklakePuffinDeleteFileGuard
{
    @Test
    public void testPuffinFormatIsAccepted()
            throws Exception
    {
        runWithCatalog("puffin-accept", (isolated, catalog, splitManager, table, snapshotId) -> {
            DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
            DucklakeDataFile victim = catalog.getDataFiles(table.tableId(), snapshotId).stream()
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No data files found for simple_table"));

            // Inject a puffin-format delete file row pointing at a non-existent path. The
            // catalog-side format check runs before any file IO, so split building should
            // succeed without throwing. Actual decode of the puffin bytes is covered by
            // TestDucklakeCrossEnginePuffinDeleteRoundTrip with a real puffin file.
            insertDeleteFileMetadata(isolated, table.tableId(), snapshotId, victim.dataFileId(), "puffin",
                    "ducklake-deletion-vector-" + UUID.randomUUID() + ".puffin");

            // No exception — the guard accepts puffin.
            splitManager.getSplits(null, SESSION, tableHandle, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        });
    }

    @Test
    public void testUnknownFormatIsRejectedWithClearError()
            throws Exception
    {
        runWithCatalog("puffin-reject", (isolated, catalog, splitManager, table, snapshotId) -> {
            DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
            DucklakeDataFile victim = catalog.getDataFiles(table.tableId(), snapshotId).stream()
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No data files found for simple_table"));

            insertDeleteFileMetadata(isolated, table.tableId(), snapshotId, victim.dataFileId(), "flatfile-v9",
                    "made-up-format.bin");

            assertThatThrownBy(() -> splitManager.getSplits(
                    null,
                    SESSION,
                    tableHandle,
                    DynamicFilter.EMPTY,
                    Constraint.alwaysTrue()))
                    .isInstanceOfSatisfying(TrinoException.class, ex -> {
                        assertThat(ex.getErrorCode()).isEqualTo(StandardErrorCode.NOT_SUPPORTED.toErrorCode());
                        assertThat(ex.getMessage())
                                .contains("test_schema")
                                .contains("simple_table")
                                .contains("flatfile-v9")
                                .contains("parquet")
                                .contains("puffin");
                    });
        });
    }

    @FunctionalInterface
    private interface CatalogTest
    {
        void run(DucklakeCatalogGenerator.IsolatedCatalog isolated, DucklakeCatalog catalog, DucklakeSplitManager splitManager,
                DucklakeTable table, long snapshotId)
                throws Exception;
    }

    private static void runWithCatalog(String suffix, CatalogTest test)
            throws Exception
    {
        TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
        DucklakeCatalogGenerator.IsolatedCatalog isolated =
                DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(server, suffix + "-" + UUID.randomUUID().toString().substring(0, 8));

        DucklakeConfig config = new DucklakeConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);

        DucklakeCatalog catalog = new JdbcDucklakeCatalog(config.toCatalogConfig());
        try {
            DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config, new DucklakePathResolver(catalog, config));
            long snapshotId = catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(catalog, "test_schema", "simple_table", snapshotId);
            test.run(isolated, catalog, splitManager, table, snapshotId);
        }
        finally {
            catalog.close();
        }
    }

    private static void insertDeleteFileMetadata(
            DucklakeCatalogGenerator.IsolatedCatalog isolated,
            long tableId,
            long snapshotId,
            long dataFileId,
            String format,
            String path)
            throws Exception
    {
        try (Connection conn = DriverManager.getConnection(isolated.jdbcUrl(), isolated.user(), isolated.password())) {
            DSLContext dsl = CatalogTestSupport.dsl(conn);
            var delfile = DUCKLAKE_DELETE_FILE.as("delfile");

            Long nextDeleteFileId = dsl.select(DSL.coalesce(DSL.max(delfile.DELETE_FILE_ID), DSL.inline(0L)).plus(1))
                    .from(delfile)
                    .fetchOne(0, Long.class);

            dsl.insertInto(delfile)
                    .set(delfile.DELETE_FILE_ID, nextDeleteFileId)
                    .set(delfile.TABLE_ID, tableId)
                    .set(delfile.BEGIN_SNAPSHOT, snapshotId)
                    .set(delfile.DATA_FILE_ID, dataFileId)
                    .set(delfile.PATH, path)
                    .set(delfile.PATH_IS_RELATIVE, true)
                    .set(delfile.FORMAT, format)
                    .set(delfile.DELETE_COUNT, 1L)
                    .set(delfile.FILE_SIZE_BYTES, 0L)
                    .set(delfile.FOOTER_SIZE, 0L)
                    .execute();
        }
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
