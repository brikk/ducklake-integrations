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
package dev.brikk.ducklake.catalog

import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Comparator
import java.util.Locale

object JdbcDucklakeCatalogTestDataGenerator {
    private val TARGET_DIR: Path = Path.of("build", "test-data")

    @JvmStatic
    @Throws(Exception::class)
    fun generateIsolatedCatalog(
        server: TestingDucklakePostgreSqlCatalogServer,
        testName: String,
    ): IsolatedCatalog {
        val databaseName = toDatabaseName(testName)
        server.createDatabase(databaseName)

        val catalogDir = TARGET_DIR.resolve("test-catalog-isolated-$testName")
        val dataDir = catalogDir.resolve("data")
        recreateDirectory(catalogDir)
        Files.createDirectories(dataDir)

        val attachUri = server.getDuckDbAttachUri(databaseName)
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake")
                statement.execute("LOAD ducklake")
                statement.execute("INSTALL postgres")
                statement.execute("LOAD postgres")
                statement.execute(
                    "ATTACH '" + escapeSql(attachUri) + "' AS ducklake_db (DATA_PATH '" +
                        escapeSql(dataDir.toAbsolutePath().toString()) + "')",
                )

                statement.execute("CREATE SCHEMA ducklake_db.test_schema")

                statement.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.simple_table (
                        id INTEGER,
                        name VARCHAR,
                        price DOUBLE,
                        active BOOLEAN,
                        created_date DATE
                    )
                    """.trimIndent(),
                )
                statement.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.simple_table VALUES
                        (1, 'Product A', 19.99, true, '2024-01-15'),
                        (2, 'Product B', 29.99, true, '2024-02-20'),
                        (3, 'Product C', 39.99, false, '2024-03-10'),
                        (4, 'Product D', 49.99, true, '2024-01-05'),
                        (5, 'Product E', 59.99, false, '2024-02-28')
                    """.trimIndent(),
                )

                statement.execute(
                    """
                    CREATE TABLE ducklake_db.test_schema.partitioned_table (
                        id INTEGER,
                        name VARCHAR,
                        region VARCHAR,
                        amount DOUBLE
                    )
                    """.trimIndent(),
                )
                statement.execute("ALTER TABLE ducklake_db.test_schema.partitioned_table SET PARTITIONED BY (region)")
                statement.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (1, 'Alice', 'US', 100.0),
                        (2, 'Bob', 'US', 200.0)
                    """.trimIndent(),
                )
                statement.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (3, 'Charlie', 'EU', 150.0),
                        (4, 'Diana', 'EU', 250.0)
                    """.trimIndent(),
                )
                statement.execute(
                    """
                    INSERT INTO ducklake_db.test_schema.partitioned_table VALUES
                        (5, 'Emi', 'APAC', 300.0)
                    """.trimIndent(),
                )

                statement.execute("CALL ducklake_flush_inlined_data('ducklake_db')")
                statement.execute("CHECKPOINT ducklake_db")
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
                walk.sorted(Comparator.reverseOrder())
                    .forEach { path ->
                        try {
                            Files.delete(path)
                        }
                        catch (e: Exception) {
                            throw RuntimeException("Failed to delete $path", e)
                        }
                    }
            }
        }
        Files.createDirectories(directory)
    }

    private fun toDatabaseName(testName: String): String {
        var normalized = testName.lowercase(Locale.ROOT).replace(Regex("[^a-z0-9_]"), "_")
        if (normalized.length > 40) {
            normalized = normalized.substring(0, 40)
        }
        return "ducklake_$normalized"
    }

    private fun escapeSql(value: String): String {
        return value.replace("'", "''")
    }

    @JvmRecord
    data class IsolatedCatalog(
        val jdbcUrl: String,
        val user: String,
        val password: String,
        val dataDir: Path,
    )
}
