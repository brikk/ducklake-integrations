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

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins behavior for two writers concurrently dropping the SAME catalog
 * object. Upstream's matrix flags
 * {@code dropped_tables × dropped_tables} (ducklake_transaction.cpp:1188–1190),
 * {@code dropped_schemas × dropped_schemas} (:1206), and
 * {@code dropped_views × dropped_views} (:1192–1194).
 *
 * <p>In our short-lived-transaction model, the loser's retry's action calls
 * {@code resolveTableId} / {@code resolveSchemaId} / {@code resolveActiveViewRow}
 * before the matrix runs, so the abort actually surfaces as a generic
 * "{Table,Schema,View} not found" {@link RuntimeException} from the action,
 * not as a {@link LogicalConflictException} from the matrix. Either way
 * the loser fails non-silently and the catalog ends in a clean state —
 * which is what these tests pin.
 *
 * <p>The matrix entries remain live for the same scenarios in upstream's
 * deferred-write model and would fire in our flow too if any future action
 * accumulated a {@link WriteChange.DroppedTable} / etc. without re-resolving
 * its target.
 */
public class TestConcurrentDropDueling
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-drop-dueling");

        DucklakeCatalogConfig config = new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);
        catalog = new JdbcDucklakeCatalog(config);
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
    public void duelingDropTableLoserFails()
            throws Exception
    {
        catalog.createTable("test_schema", "drop_dueling_table",
                List.of(TableColumnSpec.leaf("id", "integer", false)),
                Optional.empty());

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.dropTable("test_schema", "drop_dueling_table"),
                () -> catalog.dropTable("test_schema", "drop_dueling_table"));

        assertThat(result.loserException())
                .as("loser must fail when the table is already dropped")
                .isNotNull();
        assertThat(result.loserAttemptCount())
                .as("loser tried at least twice (parked, then retry)")
                .isGreaterThanOrEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getTable("test_schema", "drop_dueling_table", latestSnapshot))
                .as("table is dropped at the latest snapshot — winner's drop landed exactly once")
                .isEmpty();
    }

    @Test
    public void duelingDropSchemaLoserFails()
            throws Exception
    {
        catalog.createSchema("drop_dueling_schema");

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.dropSchema("drop_dueling_schema"),
                () -> catalog.dropSchema("drop_dueling_schema"));

        assertThat(result.loserException())
                .as("loser must fail when the schema is already dropped")
                .isNotNull();

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getSchema("drop_dueling_schema", latestSnapshot))
                .as("schema is dropped at the latest snapshot — winner's drop landed exactly once")
                .isEmpty();
    }

    @Test
    public void duelingDropViewLoserFails()
            throws Exception
    {
        catalog.createView("test_schema", "drop_dueling_view",
                "SELECT 1 AS x", "trino", null);

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.dropView("test_schema", "drop_dueling_view"),
                () -> catalog.dropView("test_schema", "drop_dueling_view"));

        assertThat(result.loserException())
                .as("loser must fail when the view is already dropped")
                .isNotNull();

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getView("test_schema", "drop_dueling_view", latestSnapshot))
                .as("view is dropped at the latest snapshot — winner's drop landed exactly once")
                .isEmpty();
    }
}
