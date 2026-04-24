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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Locale;

public final class JdbcDucklakeCatalogTestDataGenerator
{
    private static final Path TARGET_DIR = Path.of("build", "test-data");

    private JdbcDucklakeCatalogTestDataGenerator() {}

    public static IsolatedCatalog generateIsolatedCatalog(
            TestingDucklakePostgreSqlCatalogServer server,
            String testName)
            throws Exception
    {
        String databaseName = toDatabaseName(testName);
        server.createDatabase(databaseName);

        Path catalogDir = TARGET_DIR.resolve("test-catalog-isolated-" + testName);
        Path dataDir = catalogDir.resolve("data");
        recreateDirectory(catalogDir);
        Files.createDirectories(dataDir);

        String attachUri = server.getDuckDbAttachUri(databaseName);
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = connection.createStatement()) {
            statement.execute("INSTALL ducklake");
            statement.execute("LOAD ducklake");
            statement.execute("INSTALL postgres");
            statement.execute("LOAD postgres");
            statement.execute("ATTACH '" + escapeSql(attachUri) + "' AS ducklake_db (DATA_PATH '" +
                    escapeSql(dataDir.toAbsolutePath().toString()) + "')");

            statement.execute("CREATE SCHEMA ducklake_db.test_schema");

            statement.execute("""
                    CREATE TABLE ducklake_db.test_schema.simple_table (
                        id INTEGER,
                        name VARCHAR,
                        price DOUBLE,
                        active BOOLEAN,
                        created_date DATE
                    )
                    """);
            statement.execute("""
                    INSERT INTO ducklake_db.test_schema.simple_table VALUES
                        (1, 'Product A', 19.99, true, '2024-01-15'),
                        (2, 'Product B', 29.99, true, '2024-02-20'),
                        (3, 'Product C', 39.99, false, '2024-03-10'),
                        (4, 'Product D', 49.99, true, '2024-01-05'),
                        (5, 'Product E', 59.99, false, '2024-02-28')
                    """);

            statement.execute("""
                    CREATE TABLE ducklake_db.test_schema.partitioned_table (
                        id INTEGER,
                        name VARCHAR,
                        region VARCHAR,
                        amount DOUBLE
                    )
                    """);
            statement.execute("ALTER TABLE ducklake_db.test_schema.partitioned_table SET PARTITIONED BY (region)");
            statement.execute("""
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (1, 'Alice', 'US', 100.0),
                        (2, 'Bob', 'US', 200.0)
                    """);
            statement.execute("""
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (3, 'Charlie', 'EU', 150.0),
                        (4, 'Diana', 'EU', 250.0)
                    """);
            statement.execute("""
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (5, 'Emi', 'APAC', 300.0)
                    """);

            statement.execute("CALL ducklake_flush_inlined_data('ducklake_db')");
            statement.execute("CHECKPOINT ducklake_db");
        }

        return new IsolatedCatalog(
                server.getJdbcUrl(databaseName),
                server.getUser(),
                server.getPassword(),
                dataDir);
    }

    private static void recreateDirectory(Path directory)
            throws Exception
    {
        if (Files.exists(directory)) {
            try (var walk = Files.walk(directory)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            }
                            catch (Exception e) {
                                throw new RuntimeException("Failed to delete " + path, e);
                            }
                        });
            }
        }
        Files.createDirectories(directory);
    }

    private static String toDatabaseName(String testName)
    {
        String normalized = testName.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
        if (normalized.length() > 40) {
            normalized = normalized.substring(0, 40);
        }
        return "ducklake_" + normalized;
    }

    private static String escapeSql(String value)
    {
        return value.replace("'", "''");
    }

    public record IsolatedCatalog(
            String jdbcUrl,
            String user,
            String password,
            Path dataDir) {}
}
