package dev.brikk.jooq.ducklake

import org.gradle.api.provider.Property
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
 * Gradle build service that stands up a Postgres container and bootstraps the DuckLake
 * metadata schema inside it via DuckDB's ducklake extension. The container remains alive
 * for the duration of the build (jooq codegen connects to it), and is closed automatically
 * at build end.
 *
 * Lifecycle: the service is constructed lazily on first `get()`. Construction starts the
 * container and runs the bootstrap. `close()` is invoked by Gradle at build end.
 */
abstract class DucklakePostgresService :
    BuildService<DucklakePostgresService.Params>,
    AutoCloseable {

    interface Params : BuildServiceParameters {
        val image: Property<String>
        val databaseName: Property<String>
        val dataPath: Property<String>
    }

    private val container: PostgreSQLContainer<*>
    val jdbcUrl: String
    val username: String
    val password: String

    init {
        val image = parameters.image.getOrElse("postgres:18")
        val databaseName = parameters.databaseName.getOrElse("ducklake")
        val dataPath = parameters.dataPath.get()
        Files.createDirectories(Paths.get(dataPath))

        @Suppress("UNCHECKED_CAST")
        container = PostgreSQLContainer(DockerImageName.parse(image))
            .withDatabaseName(databaseName)
            .withUsername("test")
            .withPassword("test")
            .withStartupAttempts(3) as PostgreSQLContainer<*>
        container.start()

        jdbcUrl = container.jdbcUrl
        username = container.username
        password = container.password

        bootstrapDucklakeSchema(databaseName, dataPath)
    }

    private fun bootstrapDucklakeSchema(databaseName: String, dataPath: String) {
        val attachUri = buildString {
            append("ducklake:postgres:dbname=").append(databaseName)
            append(" host=").append(container.host)
            append(" port=").append(container.firstMappedPort)
            append(" user=").append(container.username)
            append(" password=").append(container.password)
        }
        DriverManager.getConnection("jdbc:duckdb:").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL ducklake")
                statement.execute("LOAD ducklake")
                statement.execute("INSTALL postgres")
                statement.execute("LOAD postgres")
                statement.execute(
                    "ATTACH '" + escape(attachUri) + "' AS ducklake_db (DATA_PATH '" +
                        escape(dataPath) + "')"
                )
                statement.execute("DETACH ducklake_db")
            }
        }
    }

    private fun escape(value: String): String = value.replace("'", "''")

    override fun close() {
        container.close()
    }
}
