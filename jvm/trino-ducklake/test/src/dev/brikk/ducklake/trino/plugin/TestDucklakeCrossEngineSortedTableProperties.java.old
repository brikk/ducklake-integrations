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
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.SortingProperty;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check that DuckDB-written sort metadata surfaces to Trino's
 * planner as a {@link SortingProperty} list. DuckDB does the writing
 * (Trino-side {@code ALTER TABLE ... SET SORTED BY} isn't implemented and
 * doesn't need to be for the read-awareness story to work); Trino sees the
 * same catalog through {@link DucklakeMetadata#getTableProperties}.
 *
 * <p>We assert the SPI shape directly rather than going through
 * {@code EXPLAIN} — that keeps the test stable against Trino planner
 * output formatting changes, and the unit tests in
 * {@link TestDucklakeSortPropertyMapper} already cover the translation
 * permutations.
 */
public class TestDucklakeCrossEngineSortedTableProperties
        extends AbstractDucklakeCrossEngineTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "cross-engine-sorted-table-properties";
    }

    @Test
    public void testDuckdbSortedTableSurfacesAsSortingProperty()
            throws Exception
    {
        String tableName = "xengine_sorted_" + UUID.randomUUID().toString().substring(0, 8);
        String fullDuckdb = "ducklake_db.test_schema." + tableName;

        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, name VARCHAR, price DOUBLE)");
                // Two-key sort with mixed direction + explicit null ordering — exercises
                // both branches of the SortOrder mapping plus multi-key ordering.
                stmt.execute("ALTER TABLE " + fullDuckdb
                        + " SET SORTED BY (name ASC NULLS LAST, price DESC NULLS FIRST)");
            }

            ConnectorTableProperties properties = readTableProperties("test_schema", tableName);

            assertThat(properties.getLocalProperties()).hasSize(2);
            assertSortingProperty(properties.getLocalProperties().get(0), "name", SortOrder.ASC_NULLS_LAST);
            assertSortingProperty(properties.getLocalProperties().get(1), "price", SortOrder.DESC_NULLS_FIRST);
        }
        finally {
            tryDropDuckdbTable(fullDuckdb);
        }
    }

    @Test
    public void testUnsortedTableYieldsEmptyLocalProperties()
            throws Exception
    {
        String tableName = "xengine_unsorted_" + UUID.randomUUID().toString().substring(0, 8);
        String fullDuckdb = "ducklake_db.test_schema." + tableName;

        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, name VARCHAR)");
            }

            ConnectorTableProperties properties = readTableProperties("test_schema", tableName);

            assertThat(properties.getLocalProperties()).isEmpty();
        }
        finally {
            tryDropDuckdbTable(fullDuckdb);
        }
    }

    @Test
    public void testSortResetClearsLocalProperties()
            throws Exception
    {
        // RESET SORTED BY end-snapshots the sort_info row. The next snapshot read should
        // see no sort spec — confirming our active-snapshot filter actually filters.
        String tableName = "xengine_reset_sort_" + UUID.randomUUID().toString().substring(0, 8);
        String fullDuckdb = "ducklake_db.test_schema." + tableName;

        try {
            try (Connection duck = createDuckdbConnection();
                    Statement stmt = duck.createStatement()) {
                stmt.execute("CREATE TABLE " + fullDuckdb + " (id INTEGER, name VARCHAR)");
                stmt.execute("ALTER TABLE " + fullDuckdb + " SET SORTED BY (name ASC NULLS LAST)");
                stmt.execute("ALTER TABLE " + fullDuckdb + " RESET SORTED BY");
            }

            ConnectorTableProperties properties = readTableProperties("test_schema", tableName);

            assertThat(properties.getLocalProperties()).isEmpty();
        }
        finally {
            tryDropDuckdbTable(fullDuckdb);
        }
    }

    private ConnectorTableProperties readTableProperties(String schemaName, String tableName)
            throws Exception
    {
        DucklakeCatalogGenerator.IsolatedCatalog isolated = getIsolatedCatalog();
        DucklakeConfig config = new DucklakeConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);
        DucklakeCatalog catalog = new JdbcDucklakeCatalog(config.toCatalogConfig());
        try {
            long snapshotId = catalog.getCurrentSnapshotId();
            DucklakeSchema schema = catalog.listSchemas(snapshotId).stream()
                    .filter(s -> s.schemaName().equals(schemaName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing schema: " + schemaName));
            DucklakeTable table = catalog.listTables(schema.schemaId(), snapshotId).stream()
                    .filter(t -> t.tableName().equals(tableName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing table: " + schemaName + "." + tableName));

            DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                    io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
            DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
            return metadata.getTableProperties(
                    SESSION,
                    new DucklakeTableHandle(schemaName, tableName, table.tableId(), snapshotId));
        }
        finally {
            catalog.close();
        }
    }

    private static void assertSortingProperty(LocalProperty<ColumnHandle> property, String expectedColumnName, SortOrder expectedOrder)
    {
        assertThat(property).isInstanceOf(SortingProperty.class);
        SortingProperty<ColumnHandle> sorting = (SortingProperty<ColumnHandle>) property;
        ColumnHandle handle = sorting.getColumn();
        assertThat(handle).isInstanceOf(DucklakeColumnHandle.class);
        assertThat(((DucklakeColumnHandle) handle).columnName()).isEqualTo(expectedColumnName);
        assertThat(sorting.getOrder()).isEqualTo(expectedOrder);
    }

    private void tryDropDuckdbTable(String fullName)
    {
        try (Connection duck = createDuckdbConnection();
                Statement stmt = duck.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + fullName);
        }
        catch (Exception ignored) {
        }
    }
}
