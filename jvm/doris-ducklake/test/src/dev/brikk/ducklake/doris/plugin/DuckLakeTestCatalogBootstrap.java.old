package dev.brikk.ducklake.doris.plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Locale;

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer;

/**
 * Bootstraps a Postgres-backed DuckLake catalog using DuckDB's
 * {@code ducklake} + {@code postgres} extensions, seeded with two schemas
 * and a handful of tables. Mirrors the pattern in
 * {@code :ducklake-catalog}'s {@code JdbcDucklakeCatalogTestDataGenerator}
 * but trimmed to what the listing tests need.
 */
final class DuckLakeTestCatalogBootstrap {

    private DuckLakeTestCatalogBootstrap() {}

    record IsolatedCatalog(String jdbcUrl, String user, String password, Path dataDir) {}

    static IsolatedCatalog bootstrap(TestingDucklakePostgreSqlCatalogServer server, String testName)
            throws Exception {
        String databaseName = "ducklake_" + testName.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
        server.createDatabase(databaseName);

        Path target = Path.of("build", "test-data", "doris-" + testName);
        Path dataDir = target.resolve("data");
        recreateDirectory(target);
        Files.createDirectories(dataDir);

        String attachUri = server.getDuckDbAttachUri(databaseName);
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("INSTALL ducklake");
            statement.execute("LOAD ducklake");
            statement.execute("INSTALL postgres");
            statement.execute("LOAD postgres");
            statement.execute("ATTACH '" + escape(attachUri) + "' AS dl (DATA_PATH '"
                    + escape(dataDir.toAbsolutePath().toString()) + "')");

            statement.execute("CREATE SCHEMA dl.sales");
            statement.execute("CREATE TABLE dl.sales.orders (id INTEGER, total DOUBLE)");
            statement.execute("CREATE TABLE dl.sales.customers (id INTEGER, name VARCHAR)");
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
            statement.execute("CREATE TABLE dl.sales.returns_file (id INTEGER, amount DOUBLE)");
            statement.execute("CREATE TABLE dl.sales.returns_inline (id INTEGER, amount DOUBLE)");

            statement.execute("CREATE SCHEMA dl.analytics");
            statement.execute("CREATE TABLE dl.analytics.events (ts TIMESTAMP, kind VARCHAR)");

            // Seed two data files on sales.orders so scan-plan tests have files to
            // enumerate. Each INSERT into DuckLake creates a separate parquet file
            // — the second insert hands us a second DucklakeDataFile to assert against.
            statement.execute("INSERT INTO dl.sales.orders VALUES (1, 9.99), (2, 19.50)");
            statement.execute("INSERT INTO dl.sales.orders VALUES (3, 5.25)");

            statement.execute(
                    "INSERT INTO dl.sales.returns_inline "
                            + "VALUES (10, 1.00), (11, 2.00), (12, 3.00), (13, 4.00)");

            // File-based delete path: even with the default DuckDB-JDBC
            // behaviour we'd get a file here, but we also `set_option` to
            // make the intent explicit — anyone reading this knows we
            // want a position-delete parquet, not a catalog-row inline.
            statement.execute("CALL dl.set_option('data_inlining_row_limit', '0')");
            statement.execute(
                    "INSERT INTO dl.sales.returns_file "
                            + "VALUES (10, 1.00), (11, 2.00), (12, 3.00), (13, 4.00)");
            statement.execute("DELETE FROM dl.sales.returns_file WHERE id = 12");

            statement.execute("CHECKPOINT dl");
        }

        return new IsolatedCatalog(
                server.getJdbcUrl(databaseName),
                server.getUser(),
                server.getPassword(),
                dataDir);
    }

    private static void recreateDirectory(Path directory) throws Exception {
        if (Files.exists(directory)) {
            try (var walk = Files.walk(directory)) {
                walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to delete " + path, e);
                    }
                });
            }
        }
        Files.createDirectories(directory);
    }

    private static String escape(String value) {
        return value.replace("'", "''");
    }
}
