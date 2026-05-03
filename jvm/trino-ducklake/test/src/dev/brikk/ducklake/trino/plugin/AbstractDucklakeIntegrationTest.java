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

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Common harness for integration test suites that read from a fresh, fully-seeded DuckLake
 * catalog. Each suite gets its own isolated PostgreSQL database via
 * {@link DucklakeQueryRunner.Builder#useIsolatedCatalog} (keyed on
 * {@link #isolatedCatalogName()}), so no cross-suite state leaks even when tests run in
 * parallel.
 *
 * <p>The seeded fixture tables — created by {@link DucklakeCatalogGenerator} — include the
 * standard read targets used by the suites:
 *
 * <pre>
 *   simple_table (5 rows)              — id, name, price, active, created_date
 *   array_table (5 rows)               — id, product_name, tags[], quantity
 *   partitioned_table (5 rows)         — id, name, region, amount (partitioned by region)
 *   temporal_partitioned_table (6 rows) — id, event_name, event_date, amount (year/month)
 *   daily_partitioned_table (5 rows)   — id, event_name, event_date, amount (year/month/day)
 *   nested_table (3 rows)              — id, metadata(struct), tags(map), nested_list, complex_struct
 *   wide_types_table (3 rows)          — full numeric/date/timestamp/blob coverage
 *   nullable_table (4 rows)            — column types with NULLs
 *   empty_table (0 rows)               — empty result set testing
 *   schema_evolution_table (4 rows)    — column added after initial write
 *   aggregation_table (30 rows)        — category A/B/C, amount, quantity
 *   inlined_table (3 rows)             — rows stored in metadata catalog
 *   inlined_nullable_table (3 rows)    — inlined rows with NULL handling
 *   mixed_inline_table (7 rows)        — 2 inlined rows + 5 parquet rows
 *   deleted_rows_table (3 rows live)   — 3 of 6 rows removed via merge-on-read deletes
 *   complex_nulls_table (5 rows)       — full-NULL vs null-subfield struct/array variants
 *   multi_file_table (5 rows)          — split across 3 Parquet files
 * </pre>
 */
abstract class AbstractDucklakeIntegrationTest
        extends AbstractTestQueryFramework
{
    /**
     * Unique catalog identifier for this suite. Becomes the suffix on the PostgreSQL database
     * name and the on-disk data directory, so each suite is fully isolated.
     */
    protected abstract String isolatedCatalogName();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog(isolatedCatalogName())
                .build();
    }

    /**
     * Best-effort {@code DROP TABLE}; swallows failures because cleanup runs in test
     * {@code finally} blocks where surfacing a secondary exception would mask the
     * primary failure.
     */
    protected void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }

    /**
     * Best-effort {@code DROP SCHEMA}; same rationale as {@link #tryDropTable}.
     */
    protected void tryDropSchema(String schemaName)
    {
        try {
            computeActual("DROP SCHEMA " + schemaName);
        }
        catch (Exception ignored) {
        }
    }

    /**
     * Direct JDBC connection to the suite's isolated PostgreSQL catalog database.
     * Used by tests that need to inspect catalog state at the table level (snapshots,
     * schema versions, data files, etc.) — anything {@code computeActual} can't reach.
     */
    protected Connection openCatalogConnection()
            throws Exception
    {
        TestingDucklakePostgreSqlCatalogServer server = DucklakeTestCatalogEnvironment.getServer();
        String databaseName = "ducklake_" + isolatedCatalogName().replace('-', '_');
        return DriverManager.getConnection(
                server.getJdbcUrl(databaseName),
                server.getUser(),
                server.getPassword());
    }

    /**
     * Reads {@code max(snapshot_id)} directly from the catalog. Use this when you need
     * the snapshot id outside any specific table's lineage; for table-scoped lookups
     * prefer {@code SELECT max(snapshot_id) FROM "<table>$snapshots"} via
     * {@code computeScalar}, which doesn't require a JDBC handle.
     */
    protected long getCurrentSnapshotIdFromCatalog()
            throws Exception
    {
        try (Connection connection = openCatalogConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT max(snapshot_id) FROM ducklake_snapshot");
                ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                throw new AssertionError("No snapshots in catalog");
            }
            return resultSet.getLong(1);
        }
    }
}
