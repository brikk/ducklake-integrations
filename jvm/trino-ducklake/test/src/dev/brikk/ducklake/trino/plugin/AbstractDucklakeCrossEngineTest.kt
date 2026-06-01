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
import org.assertj.core.api.Assertions.assertThat
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * Shared fixture + helpers for cross-engine compatibility test suites: Trino writes through the
 * connector, DuckDB reads the same PostgreSQL-backed DuckLake catalog, and we assert that what
 * one engine emits the other can parse without bespoke sync because PostgreSQL MVCC takes care
 * of visibility.
 *
 *
 * Subclasses declare a unique [isolatedCatalogName] so each suite gets its own
 * fresh PostgreSQL database (created via [DucklakeQueryRunner.Builder.useIsolatedCatalog])
 * and they cannot interfere with each other's catalog state.
 */
abstract class AbstractDucklakeCrossEngineTest : AbstractTestQueryFramework() {
    private var isolatedCatalog: DucklakeCatalogGenerator.IsolatedCatalog? = null

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

    @Throws(Exception::class)
    protected fun getIsolatedCatalog(): DucklakeCatalogGenerator.IsolatedCatalog {
        if (isolatedCatalog == null) {
            // The isolated catalog was already created by useIsolatedCatalog(name) inside
            // createQueryRunner(); reconstruct the same descriptor here so we can open
            // direct DuckDB / PostgreSQL JDBC connections against it.
            val server: TestingDucklakePostgreSqlCatalogServer = DucklakeTestCatalogEnvironment.getServer()
            val name = isolatedCatalogName()
            val databaseName = "ducklake_" + name.replace('-', '_')
            isolatedCatalog = DucklakeCatalogGenerator.IsolatedCatalog(
                    server.getJdbcUrl(databaseName),
                    server.getUser(),
                    server.getPassword(),
                    Path.of("build", "test-data", "test-catalog-isolated-$name", "data"),
                    server.getDuckDbAttachUri(databaseName))
        }
        return isolatedCatalog!!
    }

    /**
     * Creates a fresh DuckDB connection attached to the PostgreSQL-backed DuckLake catalog.
     * No sync workaround needed — PostgreSQL MVCC ensures immediate visibility.
     */
    @Throws(Exception::class)
    protected fun createDuckdbConnection(): Connection {
        val catalog = getIsolatedCatalog()
        val conn = DriverManager.getConnection("jdbc:duckdb:")
        conn.createStatement().use { stmt ->
            stmt.execute("INSTALL ducklake")
            stmt.execute("LOAD ducklake")
            stmt.execute("INSTALL postgres")
            stmt.execute("LOAD postgres")
            stmt.execute("ATTACH '" + catalog.duckDbAttachUri + "' AS ducklake_db " +
                    "(DATA_PATH '" + catalog.dataDir.toAbsolutePath() + "')")
        }
        return conn
    }

    protected fun tryDropTable(tableName: String) {
        try {
            computeActual("DROP TABLE $tableName")
        }
        catch (ignored: Exception) {
        }
    }

    /**
     * Asserts that `tableName` has all rows kept in the
     * `ducklake_inlined_data_<tableId>_<schemaVersion>` metadata tables and zero active
     * Parquet files, with the inlined row count matching `expectedRowCount`. Used to
     * guarantee the *inlined* read path is the one under test.
     */
    @Throws(Exception::class)
    protected fun assertRowsStayedInlined(tableName: String, expectedRowCount: Long) {
        val catalog = getIsolatedCatalog()
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { pgConn ->
            val dsl = CatalogTestSupport.dsl(pgConn)
            val snapshotId = CatalogQueries.latestSnapshotId(dsl)
            val tableId = CatalogQueries.activeTableId(dsl, tableName)

            assertThat(CatalogQueries.activeDataFileCount(dsl, tableId))
                    .`as`("table %s should have no active Parquet files if rows are inlined", tableName)
                    .isZero()

            assertThat(CatalogQueries.activeInlinedRowCount(dsl, tableId, snapshotId))
                    .`as`("table %s active inlined row count", tableName)
                    .isEqualTo(expectedRowCount)
        }
    }

    /**
     * Mirror of [assertRowsStayedInlined]: asserts at least one active Parquet data file
     * and zero active inlined rows. Used to guarantee the *Parquet* read path is the one under
     * test.
     */
    @Throws(Exception::class)
    protected fun assertRowsWrittenToParquet(tableName: String) {
        val catalog = getIsolatedCatalog()
        DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { pgConn ->
            val dsl = CatalogTestSupport.dsl(pgConn)
            val snapshotId = CatalogQueries.latestSnapshotId(dsl)
            val tableId = CatalogQueries.activeTableId(dsl, tableName)

            assertThat(CatalogQueries.activeDataFileCount(dsl, tableId))
                    .`as`("table %s should have at least one active Parquet file when inlining is off", tableName)
                    .isGreaterThanOrEqualTo(1)

            assertThat(CatalogQueries.activeInlinedRowCount(dsl, tableId, snapshotId))
                    .`as`("table %s should have zero active inlined rows when inlining is off", tableName)
                    .isZero()
        }
    }
}
