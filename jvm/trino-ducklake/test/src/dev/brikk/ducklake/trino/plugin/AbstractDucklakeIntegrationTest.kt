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
package dev.brikk.ducklake.trino.plugin

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import dev.brikk.ducklake.catalog.testing.CatalogQueries
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import java.sql.Connection
import java.sql.DriverManager

/**
 * Common harness for integration test suites that read from a fresh, fully-seeded DuckLake
 * catalog. Each suite gets its own isolated PostgreSQL database via
 * [DucklakeQueryRunner.Builder.useIsolatedCatalog] (keyed on
 * [isolatedCatalogName]), so no cross-suite state leaks even when tests run in
 * parallel.
 *
 *
 * The seeded fixture tables — created by [DucklakeCatalogGenerator] — include the
 * standard read targets used by the suites:
 *
 * ```
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
 * ```
 */
abstract class AbstractDucklakeIntegrationTest : AbstractTestQueryFramework() {
    /**
     * Unique catalog identifier for this suite. Becomes the suffix on the PostgreSQL database
     * name and the on-disk data directory, so each suite is fully isolated.
     */
    protected abstract fun isolatedCatalogName(): String

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog(isolatedCatalogName())
                .build()
    }

    /**
     * Best-effort `DROP TABLE`; swallows failures because cleanup runs in test
     * `finally` blocks where surfacing a secondary exception would mask the
     * primary failure.
     */
    protected fun tryDropTable(tableName: String) {
        try {
            computeActual("DROP TABLE $tableName")
        }
        catch (ignored: Exception) {
        }
    }

    /**
     * Best-effort `DROP SCHEMA`; same rationale as [tryDropTable].
     */
    protected fun tryDropSchema(schemaName: String) {
        try {
            computeActual("DROP SCHEMA $schemaName")
        }
        catch (ignored: Exception) {
        }
    }

    /**
     * Direct JDBC connection to the suite's isolated PostgreSQL catalog database.
     * Used by tests that need to inspect catalog state at the table level (snapshots,
     * schema versions, data files, etc.) — anything `computeActual` can't reach.
     */
    @Throws(Exception::class)
    protected fun openCatalogConnection(): Connection {
        val server: TestingDucklakePostgreSqlCatalogServer = DucklakeTestCatalogEnvironment.getServer()
        val databaseName = "ducklake_" + isolatedCatalogName().replace('-', '_')
        return DriverManager.getConnection(
                server.getJdbcUrl(databaseName),
                server.getUser(),
                server.getPassword())
    }

    /**
     * Reads `max(snapshot_id)` directly from the catalog. Use this when you need
     * the snapshot id outside any specific table's lineage; for table-scoped lookups
     * prefer `SELECT max(snapshot_id) FROM "<table>$snapshots"` via
     * `computeScalar`, which doesn't require a JDBC handle.
     */
    @Throws(Exception::class)
    protected fun getCurrentSnapshotIdFromCatalog(): Long {
        openCatalogConnection().use { connection ->
            return CatalogQueries.latestSnapshotId(CatalogTestSupport.dsl(connection))
        }
    }
}
