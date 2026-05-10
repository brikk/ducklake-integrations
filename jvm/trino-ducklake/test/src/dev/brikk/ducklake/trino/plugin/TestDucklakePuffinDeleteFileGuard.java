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
 * Verifies the connector refuses to read a snapshot whose delete files use a format
 * the connector cannot decode (today: anything other than {@code parquet}, in practice
 * {@code puffin} written by DuckDB with {@code write_deletion_vectors=true}). Without
 * this guard the puffin delete file is silently ignored and rows that should be deleted
 * are returned.
 */
public class TestDucklakePuffinDeleteFileGuard
{
    @Test
    public void testPuffinDeleteFileFailsWithClearError()
            throws Exception
    {
        TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
        DucklakeCatalogGenerator.IsolatedCatalog isolated =
                DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(server, "puffin-guard-" + UUID.randomUUID().toString().substring(0, 8));

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
            DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

            DucklakeDataFile victim = catalog.getDataFiles(table.tableId(), snapshotId).stream()
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No data files found for simple_table"));

            // Inject a puffin-format delete file row pointing at a non-existent path.
            // The format check happens on metadata before any file IO, so the path
            // does not need to resolve to a real artifact.
            insertPuffinDeleteFileMetadata(
                    isolated.jdbcUrl(),
                    isolated.user(),
                    isolated.password(),
                    table.tableId(),
                    snapshotId,
                    victim.dataFileId());

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
                                .contains("puffin")
                                .contains("write_deletion_vectors");
                    });
        }
        finally {
            catalog.close();
        }
    }

    private static void insertPuffinDeleteFileMetadata(
            String jdbcUrl,
            String user,
            String password,
            long tableId,
            long snapshotId,
            long dataFileId)
            throws Exception
    {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
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
                    .set(delfile.PATH, "ducklake-deletion-vector-" + UUID.randomUUID() + ".puffin")
                    .set(delfile.PATH_IS_RELATIVE, true)
                    .set(delfile.FORMAT, "puffin")
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
