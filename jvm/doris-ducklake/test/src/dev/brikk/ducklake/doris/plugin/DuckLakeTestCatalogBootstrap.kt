package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Comparator
import java.util.Locale

/**
 * Bootstraps a Postgres-backed DuckLake catalog using DuckDB's
 * `ducklake` + `postgres` extensions, seeded with two schemas
 * and a handful of tables. Mirrors the pattern in
 * `:ducklake-catalog`'s `JdbcDucklakeCatalogTestDataGenerator`
 * but trimmed to what the listing tests need.
 */
object DuckLakeTestCatalogBootstrap {

    class IsolatedCatalog(
        private val jdbcUrl: String,
        private val user: String,
        private val password: String,
        private val dataDir: Path,
    ) {
        fun jdbcUrl(): String = jdbcUrl

        fun user(): String = user

        fun password(): String = password

        fun dataDir(): Path = dataDir
    }

    @JvmStatic
    @Throws(Exception::class)
    fun bootstrap(server: TestingDucklakePostgreSqlCatalogServer, testName: String): IsolatedCatalog {
        val databaseName = "ducklake_" + testName.lowercase(Locale.ROOT).replace(Regex("[^a-z0-9_]"), "_")
        server.createDatabase(databaseName)

        val target = Path.of("build", "test-data", "doris-$testName")
        val dataDir = target.resolve("data")
        recreateDirectory(target)
        Files.createDirectories(dataDir)

        val attachUri = server.getDuckDbAttachUri(databaseName)
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake")
                statement.execute("LOAD ducklake")
                statement.execute("INSTALL postgres")
                statement.execute("LOAD postgres")
                statement.execute(
                    "ATTACH '" + escape(attachUri) + "' AS dl (DATA_PATH '" +
                        escape(dataDir.toAbsolutePath().toString()) + "')",
                )

                statement.execute("CREATE SCHEMA dl.sales")
                statement.execute("CREATE TABLE dl.sales.orders (id INTEGER, total DOUBLE)")
                statement.execute("CREATE TABLE dl.sales.customers (id INTEGER, name VARCHAR)")
                // sales.returns_file exercises the Step-7 file-based delete path
                // (DuckLake writes a position-delete parquet + ducklake_delete_file
                // row). sales.returns_inline is a placeholder for the Step-7.5
                // inline-delete fixture (`ducklake_inlined_delete_<tableId>`
                // catalog rows). DuckDB-JDBC 1.5.2 doesn't currently honour
                // `data_inlining_row_limit` for DELETE inlining the way the
                // Python driver does — it always writes file-based deletes —
                // so seeding a real inline-delete row in the test fixture
                // needs a direct ducklake_inlined_delete_<tableId> INSERT
                // via PG JDBC (Step-7.5 fixture work). For now returns_inline
                // is seeded with INSERTs only and the inline-delete test stays
                // @Disabled. See ducklake-doris-friction.md (2026-05-19).
                statement.execute("CREATE TABLE dl.sales.returns_file (id INTEGER, amount DOUBLE)")
                statement.execute("CREATE TABLE dl.sales.returns_inline (id INTEGER, amount DOUBLE)")

                statement.execute("CREATE SCHEMA dl.analytics")
                statement.execute("CREATE TABLE dl.analytics.events (ts TIMESTAMP, kind VARCHAR)")

                // Seed sales.orders so the unpartitioned scan-plan tests have files to
                // enumerate. Two INSERTs, but DO NOT assume two data files: DuckDB 1.5.4's
                // CHECKPOINT (line 119) can consolidate multiple small INSERTs into ONE
                // parquet file, and the writer may coalesce regardless. The tests that use
                // sales.orders assert stability-at-pin — `isNotEmpty()`, or a size relative
                // to another plan, or the count-pushdown collapse-to-one — never an exact
                // per-INSERT file count. Only partition-based fixtures (by_region,
                // by_name_bucket) assert a floor of ≥N files, and only because CHECKPOINT
                // cannot compact ACROSS partitions.
                statement.execute("INSERT INTO dl.sales.orders VALUES (1, 9.99), (2, 19.50)")
                statement.execute("INSERT INTO dl.sales.orders VALUES (3, 5.25)")

                // sales.by_region exercises IDENTITY partition pruning: two
                // partition values ('us','eu') write to separate data files, so a
                // `region = 'us'` filter must drop the 'eu' file(s).
                statement.execute(
                    "CREATE TABLE dl.sales.by_region (id INTEGER, region VARCHAR, amount DOUBLE)",
                )
                statement.execute("ALTER TABLE dl.sales.by_region SET PARTITIONED BY (region)")
                statement.execute("INSERT INTO dl.sales.by_region VALUES (1, 'us', 10.0), (2, 'us', 20.0)")
                statement.execute("INSERT INTO dl.sales.by_region VALUES (3, 'eu', 30.0), (4, 'eu', 40.0)")

                // sales.by_name_bucket exercises BUCKET partition pruning. DuckLake's
                // murmur3 bucket(4) sends alice→1, bob→2, charlie→3 — three distinct
                // bucket files — so `name = 'alice'` must keep only bucket 1.
                statement.execute("CREATE TABLE dl.sales.by_name_bucket (id INTEGER, name VARCHAR)")
                statement.execute("ALTER TABLE dl.sales.by_name_bucket SET PARTITIONED BY (bucket(4, name))")
                statement.execute(
                    "INSERT INTO dl.sales.by_name_bucket VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
                )

                statement.execute(
                    "INSERT INTO dl.sales.returns_inline " +
                        "VALUES (10, 1.00), (11, 2.00), (12, 3.00), (13, 4.00)",
                )

                // File-based delete path: even with the default DuckDB-JDBC
                // behaviour we'd get a file here, but we also `set_option` to
                // make the intent explicit — anyone reading this knows we
                // want a position-delete parquet, not a catalog-row inline.
                statement.execute("CALL dl.set_option('data_inlining_row_limit', '0')")
                statement.execute(
                    "INSERT INTO dl.sales.returns_file " +
                        "VALUES (10, 1.00), (11, 2.00), (12, 3.00), (13, 4.00)",
                )
                statement.execute("DELETE FROM dl.sales.returns_file WHERE id = 12")

                statement.execute("CHECKPOINT dl")
            }
        }

        return IsolatedCatalog(
            server.getJdbcUrl(databaseName),
            server.getUser(),
            server.getPassword(),
            dataDir,
        )
    }

    @Throws(Exception::class)
    private fun recreateDirectory(directory: Path) {
        if (Files.exists(directory)) {
            Files.walk(directory).use { walk ->
                walk.sorted(Comparator.reverseOrder()).forEach { path ->
                    try {
                        Files.delete(path)
                    } catch (e: Exception) {
                        throw RuntimeException("Failed to delete $path", e)
                    }
                }
            }
        }
        Files.createDirectories(directory)
    }

    private fun escape(value: String): String = value.replace("'", "''")
}
