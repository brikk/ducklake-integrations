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

import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeSnapshotRecord;
import org.jooq.DSLContext;
import org.jooq.ResultQuery;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;
import java.util.stream.Stream;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SNAPSHOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the {@link QuackWrappedMetadataQuery} routing fixes the same-table
 * multi-scan SQL shape we hit in {@code attemptWriteTransaction}'s snapshot
 * read, while {@link DirectMetadataQuery} fails on it as expected against the
 * Quack-attached metadata catalog. Uses the testcontainer fixture so we exercise
 * a real out-of-process Quack server pinned to the in-tree DuckDB CLI version.
 */
final class TestQuackWrappedMetadataQuery
{
    private static final String METADATA_CATALOG = "probe_meta";

    private static TestingDucklakeDuckDbQuackCatalogServer server;
    private static Path dataDir;

    @BeforeAll
    static void setUp() throws Exception
    {
        server = new TestingDucklakeDuckDbQuackCatalogServer();
        dataDir = Files.createTempDirectory("ducklake-quack-wrapper-test-");
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        if (server != null) {
            server.close();
        }
        if (dataDir != null) {
            deleteRecursively(dataDir);
        }
    }

    @Test
    void wrapperRoutesSameTableMultiScanThatDirectExecutionRejects() throws Exception
    {
        try (Connection conn = openAttachedConnection()) {
            populateOneDuckLakeSnapshot(conn);

            DSLContext dsl = DSL.using(conn, jooqSettings());
            var snap = DUCKLAKE_SNAPSHOT.as("snap");

            // The exact SQL shape attemptWriteTransaction issues: SELECT against
            // ducklake_snapshot with a same-table subquery for max(snapshot_id).
            ResultQuery<DucklakeSnapshotRecord> brokenShape = dsl.selectFrom(snap)
                    .where(snap.SNAPSHOT_ID.eq(
                            DSL.select(DSL.max(snap.SNAPSHOT_ID)).from(snap)));

            MetadataQuery direct = new DirectMetadataQuery();
            assertThatThrownBy(() -> direct.fetchOne(dsl, brokenShape))
                    .as("direct execution against Quack-attached metadata catalog must trip the "
                            + "multi-streaming-scan optimizer check")
                    .isInstanceOf(DataAccessException.class)
                    .hasMessageContaining("Multiple streaming scans");

            MetadataQuery wrapped = new QuackWrappedMetadataQuery(METADATA_CATALOG);
            DucklakeSnapshotRecord row = wrapped.fetchOne(dsl, brokenShape);

            assertThat(row)
                    .as("the same query routed through quack_query_by_name must succeed and "
                            + "map back to the generated DucklakeSnapshotRecord type")
                    .isNotNull();
            assertThat(row.getSnapshotId())
                    .as("snapshot_id of the row returned by max() — should be the latest snapshot")
                    .isPositive();
            // schema_version is BIGINT in the metadata; assert it survives the wrapper
            // round-trip with the right Java type binding via the generated record.
            assertThat(row.getSchemaVersion()).isNotNull();
        }
    }

    @Test
    void wrapperReturnsNullWhenNoRowMatches() throws Exception
    {
        try (Connection conn = openAttachedConnection()) {
            populateOneDuckLakeSnapshot(conn);

            DSLContext dsl = DSL.using(conn, jooqSettings());
            var snap = DUCKLAKE_SNAPSHOT.as("snap");

            ResultQuery<DucklakeSnapshotRecord> noMatch = dsl.selectFrom(snap)
                    .where(snap.SNAPSHOT_ID.eq(Long.MIN_VALUE));

            MetadataQuery wrapped = new QuackWrappedMetadataQuery(METADATA_CATALOG);
            assertThat(wrapped.fetchOne(dsl, noMatch)).isNull();
        }
    }

    private static Connection openAttachedConnection() throws Exception
    {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = conn.createStatement()) {
            s.execute("INSTALL quack");
            s.execute("LOAD quack");
            s.execute("INSTALL ducklake");
            s.execute("LOAD ducklake");
            s.execute("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '" + server.getToken() + "')");
            s.execute("ATTACH '" + server.getDucklakeAttachUri() + "' AS lake "
                    + "(DATA_PATH '" + dataDir.toAbsolutePath() + "', METADATA_CATALOG '" + METADATA_CATALOG + "')");
            // USE the metadata catalog so bare ducklake_* references in jOOQ-rendered SQL
            // resolve via the sibling catalog (matches what
            // QuackBackedDuckDbCatalogUrl.connectionInitSql does in production).
            s.execute("USE " + METADATA_CATALOG + ".main");
        }
        return conn;
    }

    private static void populateOneDuckLakeSnapshot(Connection conn) throws Exception
    {
        try (Statement s = conn.createStatement()) {
            // Touching the lake catalog with any DDL forces DuckLake to write at least
            // one snapshot row past the bootstrap snapshot, so the max(snapshot_id)
            // subquery returns a deterministically-existing row.
            s.execute("USE lake");
            s.execute("CREATE SCHEMA IF NOT EXISTS wrapper_probe_schema");
            s.execute("USE " + METADATA_CATALOG + ".main");
        }
    }

    private static Settings jooqSettings()
    {
        return new Settings()
                .withRenderQuotedNames(RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED)
                .withRenderSchema(false);
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
