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
 * Acceptance test for {@link ConflictMatrix}'s
 * {@code created_table × created_table} entry
 * (port of {@code ducklake_transaction.cpp:1232–1242}).
 *
 * <p>Two writers concurrently creating a table with the same
 * {@code (schema, name)} must not both land. Upstream's DDL has no UNIQUE
 * on {@code (schema_id, table_name)} for active rows (see
 * {@code ducklake_metadata_manager.cpp:199}); the matrix is the only safety
 * net.
 */
public class TestConcurrentCreateTableSameName
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-create-table-same-name");

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
    public void duelingCreateTableConflicts()
            throws Exception
    {
        List<TableColumnSpec> columns = List.of(
                TableColumnSpec.leaf("id", "integer", false));

        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.createTable("test_schema", "dueling_table", columns, Optional.empty(), Optional.empty()),
                () -> catalog.createTable("test_schema", "dueling_table", columns, Optional.empty(), Optional.empty()));

        assertThat(result.loserException())
                .as("two concurrent createTable with same (schema, name) must conflict")
                .isInstanceOf(LogicalConflictException.class);
        assertThat(result.loserException().getMessage())
                .as("error message must name the conflicting table and schema")
                .contains("create table")
                .contains("dueling_table")
                .contains("test_schema")
                .contains("created by another transaction already");

        assertThat(((TransactionConflictException) result.loserException()).retryable())
                .as("logical conflicts are non-retryable")
                .isFalse();
        assertThat(result.loserAttemptCount())
                .as("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getTable("test_schema", "dueling_table", latestSnapshot))
                .as("winner's createTable lands; loser's aborted before commit")
                .isPresent();
    }
}
