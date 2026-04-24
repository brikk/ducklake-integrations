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

import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class TestingDucklakePostgreSqlCatalogServer
        implements AutoCloseable
{
    private static final String IMAGE = "postgres:18";
    private static final String DEFAULT_DATABASE = "ducklake";
    private static final String USER = "test";
    private static final String PASSWORD = "test";

    private final PostgreSQLContainer container;

    public TestingDucklakePostgreSqlCatalogServer()
    {
        container = new PostgreSQLContainer(DockerImageName.parse(IMAGE))
                .withDatabaseName(DEFAULT_DATABASE)
                .withUsername(USER)
                .withPassword(PASSWORD)
                .withStartupAttempts(3);
        container.start();
    }

    public String getJdbcUrl()
    {
        return container.getJdbcUrl();
    }

    public String getJdbcUrl(String databaseName)
    {
        // Testcontainers JDBC URL format: jdbc:postgresql://host:port/database
        // Replace the default database name with the requested one
        return container.getJdbcUrl().replace("/" + DEFAULT_DATABASE, "/" + databaseName);
    }

    public String getUser()
    {
        return USER;
    }

    public String getPassword()
    {
        return PASSWORD;
    }

    public String getDuckDbAttachUri()
    {
        return getDuckDbAttachUri(DEFAULT_DATABASE);
    }

    public String getDuckDbAttachUri(String databaseName)
    {
        return "ducklake:postgres:dbname=" + databaseName +
                " host=" + container.getHost() +
                " port=" + container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT) +
                " user=" + USER +
                " password=" + PASSWORD;
    }

    /**
     * Creates a new database in the PostgreSQL container.
     * Used for test isolation — each test class gets its own database.
     */
    public void createDatabase(String databaseName)
            throws Exception
    {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), USER, PASSWORD);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE " + databaseName);
        }
    }

    @Override
    public void close()
    {
        container.close();
    }
}
