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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance test for {@link ConflictMatrix}'s
 * {@code created_schema × created_schema} entry
 * (port of {@code ducklake_transaction.cpp:1212–1215}).
 *
 * <p>Two writers concurrently calling {@code createSchema("dueling_schema")}
 * must not both land. Without the matrix this corrupts the catalog —
 * upstream's metadata DDL has no UNIQUE on {@code ducklake_schema.schema_name}
 * (only single-column PK on {@code schema_id}, see
 * {@code ducklake_metadata_manager.cpp:198}), so two active rows with the
 * same name would otherwise both INSERT successfully on retry.
 */
public class TestConcurrentCreateSchemaSameName
{
    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-create-schema-same-name");

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
    public void duelingCreateSchemaConflicts()
            throws Exception
    {
        ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
                catalog,
                () -> catalog.createSchema("dueling_schema"),
                () -> catalog.createSchema("dueling_schema"));

        assertThat(result.loserException())
                .as("two concurrent createSchema with same name must conflict — upstream matrix entry")
                .isInstanceOf(LogicalConflictException.class);
        assertThat(result.loserException().getMessage())
                .as("error message must name the conflicting schema and the cause")
                .contains("create schema")
                .contains("dueling_schema")
                .contains("created a schema with this name already");

        assertThat(((TransactionConflictException) result.loserException()).retryable())
                .as("logical conflicts are non-retryable")
                .isFalse();
        assertThat(result.loserAttemptCount())
                .as("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
                .isEqualTo(2);

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getSchema("dueling_schema", latestSnapshot))
                .as("winner's createSchema lands; loser's aborted before commit")
                .isPresent();
    }
}
