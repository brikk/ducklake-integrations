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

import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.sql.DriverManager

/**
 * Shared Testcontainer wrapper for the PostgreSQL-backed DuckLake catalog. Exposed via
 * `java-test-fixtures` so both `ducklake-catalog` and `trino-ducklake`
 * test suites use the same container image, credentials, and JDBC URL shape — drift in
 * one used to silently leave the other stale.
 */
class TestingDucklakePostgreSqlCatalogServer : AutoCloseable {

    private val container: PostgreSQLContainer = PostgreSQLContainer(DockerImageName.parse(IMAGE))
        .withDatabaseName(DEFAULT_DATABASE)
        .withUsername(USER)
        .withPassword(PASSWORD)
        .withStartupAttempts(3)
        // Default 100 is too low for parallel test classes — each test JVM-internal
        // worker opens a few PG connections via DuckDB's postgres extension, and we
        // run many test classes concurrently. 500 is empirical headroom.
        .withCommand("postgres", "-c", "max_connections=500")

    init {
        container.start()
    }

    fun getJdbcUrl(): String = container.jdbcUrl

    fun getJdbcUrl(databaseName: String): String {
        // Testcontainers JDBC URL format: jdbc:postgresql://host:port/database
        // Replace the default database name with the requested one
        return container.jdbcUrl.replace("/$DEFAULT_DATABASE", "/$databaseName")
    }

    fun getUser(): String = USER

    fun getPassword(): String = PASSWORD

    fun getDuckDbAttachUri(): String = getDuckDbAttachUri(DEFAULT_DATABASE)

    fun getDuckDbAttachUri(databaseName: String): String {
        return "ducklake:postgres:dbname=" + databaseName +
            " host=" + container.host +
            " port=" + container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT) +
            " user=" + USER +
            " password=" + PASSWORD
    }

    /**
     * Creates a new database in the PostgreSQL container.
     * Used for test isolation — each test class gets its own database.
     */
    @Throws(Exception::class)
    fun createDatabase(databaseName: String) {
        DriverManager.getConnection(getJdbcUrl(), USER, PASSWORD).use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE DATABASE $databaseName")
            }
        }
    }

    override fun close() {
        container.close()
    }

    companion object {
        private const val IMAGE = "postgres:18"
        private const val DEFAULT_DATABASE = "ducklake"
        private const val USER = "test"
        private const val PASSWORD = "test"
    }
}
