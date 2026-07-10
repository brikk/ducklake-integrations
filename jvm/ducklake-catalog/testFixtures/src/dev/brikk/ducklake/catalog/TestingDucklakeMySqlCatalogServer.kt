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

import org.testcontainers.mysql.MySQLContainer
import org.testcontainers.utility.DockerImageName

/**
 * Shared Testcontainer wrapper for a MySQL 8+ -backed DuckLake catalog. Mirrors
 * [TestingDucklakePostgreSqlCatalogServer] so the MySQL smoke coverage uses the same shape as PG.
 *
 * Scope note: the connector's own read/write path talks to MySQL directly via the MySQL
 * Connector/J driver + jOOQ (dialect auto-detected from the `jdbc:mysql:` URL) — it does NOT use
 * DuckDB's `mysql` extension. Bootstrapping the empty `ducklake_*` schema still goes through a
 * one-shot DuckDB `ATTACH 'ducklake:mysql:...'` (reliable), but ongoing cross-engine DuckDB reads
 * of a MySQL-backed catalog are DEFERRED: DuckDB 1.5.4's `mysql` extension is unstable reading
 * DuckLake-on-MySQL (protocol desyncs / SIGSEGV in its SSL layer). See dev-docs.
 */
class TestingDucklakeMySqlCatalogServer : AutoCloseable {

    private val container: MySQLContainer =
        MySQLContainer(DockerImageName.parse(IMAGE))
            .withDatabaseName(DEFAULT_DATABASE)
            .withUsername(USER)
            .withPassword(PASSWORD)
            .withStartupAttempts(3)

    init {
        container.start()
    }

    fun getJdbcUrl(): String = container.jdbcUrl

    fun getJdbcUrl(databaseName: String): String =
        // Testcontainers MySQL URL: jdbc:mysql://host:port/database[?params]. Swap the db segment,
        // preserving any query params the container appended.
        container.jdbcUrl.replaceFirst("/$DEFAULT_DATABASE", "/$databaseName")

    fun getUser(): String = USER

    fun getPassword(): String = PASSWORD

    fun getHost(): String = container.host

    fun getMappedPort(): Int = container.getMappedPort(MySQLContainer.MYSQL_PORT)

    fun getDuckDbAttachUri(): String = getDuckDbAttachUri(DEFAULT_DATABASE)

    /** DuckDB `mysql` extension connection string for `ATTACH 'ducklake:mysql:...'`. */
    fun getDuckDbAttachUri(databaseName: String): String =
        "ducklake:mysql:db=" + databaseName +
            " host=" + container.host +
            " port=" + container.getMappedPort(MySQLContainer.MYSQL_PORT) +
            " user=" + USER +
            " password=" + PASSWORD

    override fun close() {
        container.close()
    }

    companion object {
        private const val IMAGE = "mysql:8.4"
        private const val DEFAULT_DATABASE = "ducklake"
        private const val USER = "test"
        private const val PASSWORD = "test"
    }
}
