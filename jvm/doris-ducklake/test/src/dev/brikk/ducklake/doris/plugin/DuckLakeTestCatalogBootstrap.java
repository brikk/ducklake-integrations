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

            statement.execute("CREATE SCHEMA dl.analytics");
            statement.execute("CREATE TABLE dl.analytics.events (ts TIMESTAMP, kind VARCHAR)");

            // Seed two data files on sales.orders so scan-plan tests have files to
            // enumerate. Each INSERT into DuckLake creates a separate parquet file
            // — the second insert hands us a second DucklakeDataFile to assert against.
            statement.execute("INSERT INTO dl.sales.orders VALUES (1, 9.99), (2, 19.50)");
            statement.execute("INSERT INTO dl.sales.orders VALUES (3, 5.25)");

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
