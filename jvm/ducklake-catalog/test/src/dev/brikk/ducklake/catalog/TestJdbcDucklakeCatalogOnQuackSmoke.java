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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end smoke for {@link JdbcDucklakeCatalog} against a Quack-backed remote
 * DuckLake catalog. The catalog is configured with the synthetic
 * {@code jdbc:duckdb:quack://host:port?metadata_catalog=name} URL; HikariCP's
 * per-connection init script bootstraps the remote metadata schema via
 * {@code ATTACH 'ducklake:quack:...'} on first use, then bare jOOQ DSL against
 * {@code ducklake_*} resolves through the {@code USE meta.main} that ends the
 * init.
 *
 * <p>Validates that:
 * <ul>
 *   <li>HikariCP accepts the multi-statement init script we generate.</li>
 *   <li>jOOQ + the DuckDB JDBC driver round-trip a basic read
 *       ({@code listSchemas}) against the Quack-attached metadata.</li>
 *   <li>A write transaction (createSchema) commits via the same connection path
 *       that {@code attemptWriteTransaction} uses for PG today.</li>
 * </ul>
 *
 * <p>If this passes, we have evidence that the existing
 * {@code JdbcDucklakeCatalog} works against DuckDB-as-catalog without further
 * SQL-dialect surgery, which unblocks the backend-selector wiring.
 */
public class TestJdbcDucklakeCatalogOnQuackSmoke
{
    private static TestingDucklakeDuckDbQuackCatalogServer server;
    private static JdbcDucklakeCatalog catalog;
    private static Path dataDir;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakeDuckDbQuackCatalogServer();
        dataDir = Files.createTempDirectory("ducklake-quack-jdbc-smoke-");

        String url = "jdbc:duckdb:quack://" + server.getHost() + ":" + server.getMappedPort()
                + "?metadata_catalog=smoke_meta";
        DucklakeCatalogConfig config = new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(url)
                .setCatalogDatabasePassword(server.getToken())
                .setDataPath(dataDir.toAbsolutePath().toString())
                .setMaxCatalogConnections(3);
        catalog = new JdbcDucklakeCatalog(config);
    }

    @AfterAll
    public static void tearDownClass()
            throws Exception
    {
        if (catalog != null) {
            catalog.close();
        }
        if (server != null) {
            server.close();
        }
        if (dataDir != null) {
            deleteRecursively(dataDir);
        }
    }

    @Test
    public void initialListSchemasReturnsBootstrappedMetadata()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        List<DucklakeSchema> schemas = catalog.listSchemas(snapshotId);
        assertThat(schemas)
                .as("ATTACH 'ducklake:quack:...' must auto-bootstrap the metadata schema and "
                        + "create the default 'main' DuckLake schema visible to listSchemas")
                .extracting(DucklakeSchema::schemaName)
                .contains("main");
    }

    @Disabled("Verified against DuckDB 1.5.3 (2026-05-22): still fails with "
            + "\"Not implemented Error: Multiple streaming scans or streaming scans + CTAS / insert in the "
            + "same query are not currently supported\" in attemptWriteTransaction's snapshot-read SELECT "
            + "(`SELECT ... WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)`). Quack is "
            + "still beta per the 1.5.3 announcement. JdbcDucklakeCatalog's SQL is intentionally not "
            + "compromised to work around this — local DuckDB handles the same SQL fine (see "
            + "TestJdbcDucklakeCatalogOnLocalDuckDbSmoke). Lift this @Disabled when upstream Quack adds "
            + "multi-streaming-scan support; tracked in dev-docs/TODO-WRITE-MODE.md § Quack Catalog Backend.")
    @Test
    public void createSchemaCommitsAndListSchemasSeesIt()
    {
        String name = "smoke_created_schema";
        catalog.createSchema(name);

        long snapshotId = catalog.getCurrentSnapshotId();
        assertThat(catalog.listSchemas(snapshotId))
                .extracting(DucklakeSchema::schemaName)
                .contains(name);
    }

    private static void deleteRecursively(Path dir)
    {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                }
                catch (IOException ignored) {
                }
            });
        }
        catch (IOException ignored) {
        }
    }
}
