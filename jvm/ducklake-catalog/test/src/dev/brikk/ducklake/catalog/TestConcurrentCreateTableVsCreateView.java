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
 * Acceptance test for the cross-kind name-collision branch of
 * {@link ConflictMatrix} — port of upstream's
 * {@code created_tables} loop at
 * {@code ducklake_transaction.cpp:1218–1242}, where tables and views share
 * a namespace within a schema. T1 creates table {@code S.foo}, T2 races
 * with creating view {@code S.foo}; the loser must fail non-retryably
 * with a message naming the existing entry's kind ("table"), not silently
 * land both.
 */
public class TestConcurrentCreateTableVsCreateView
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-create-table-vs-create-view");

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
    public void createTableWinnerCreateViewLoserConflicts()
            throws Exception
    {
        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.createTable("test_schema", "shared_name",
                        List.of(TableColumnSpec.leaf("id", "integer", false)),
                        Optional.empty()),
                () -> catalog.createView("test_schema", "shared_name",
                        "SELECT 1 AS x", "trino", null));

        assertThat(result.loserException())
                .as("creating a view with a name a concurrent transaction just created as a table must conflict")
                .isInstanceOf(LogicalConflictException.class);
        assertThat(result.loserException().getMessage())
                .as("error message must name the entry kind already created (table) and the contended name")
                .contains("create view")
                .contains("shared_name")
                .contains("test_schema")
                .contains("table"); // the existing entry's kind, per upstream message shape

        assertThat(((TransactionConflictException) result.loserException()).retryable())
                .as("logical conflicts are non-retryable")
                .isFalse();
        assertThat(result.loserAttemptCount())
                .as("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getTable("test_schema", "shared_name", latestSnapshot))
                .as("winner's table lands at the latest snapshot")
                .isPresent();
        assertThat(catalog.getView("test_schema", "shared_name", latestSnapshot))
                .as("loser's view did NOT land — its action was rolled back")
                .isEmpty();
    }
}
