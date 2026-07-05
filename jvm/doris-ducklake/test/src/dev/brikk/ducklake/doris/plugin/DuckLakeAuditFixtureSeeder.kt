package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Comparator
import java.util.Locale

/**
 * Seeds an isolated Postgres-backed DuckLake catalog with caller-supplied DuckDB
 * statements, for the read-side audit tests ported from trino-ducklake (see
 * `DuckLakeTemporalTypeAuditTest`, `DuckLakeUnicodeIdentifierAuditTest`,
 * `DuckLakeSnapshotPinningAuditTest`).
 *
 * Why a second bootstrap next to [DuckLakeTestCatalogBootstrap]: the audits need
 * bespoke fixtures (every DuckLake temporal type, unicode identifiers, a
 * pin-then-mutate table) that would bloat the shared listing fixture, and the
 * important property is that a REAL DuckDB writer produced the catalog rows —
 * `ducklake_column.column_type` strings, unicode identifier bytes, snapshot
 * bookkeeping — rather than our own catalog writer, so the doris metadata layer
 * is audited against what DuckLake actually persists (the same cross-engine
 * stance as trino's `AbstractDucklakeCrossEngineTest` fixtures).
 */
object DuckLakeAuditFixtureSeeder {

    /**
     * Creates a fresh catalog database named after [testName], attaches it via
     * DuckDB's `ducklake` extension as `dl`, runs [statements] in order, and
     * checkpoints. Statements reference objects as `dl.<schema>.<table>`.
     */
    @JvmStatic
    @Throws(Exception::class)
    fun seed(
        server: TestingDucklakePostgreSqlCatalogServer,
        testName: String,
        statements: List<String>,
    ): DuckLakeTestCatalogBootstrap.IsolatedCatalog {
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
                for (sql in statements) {
                    statement.execute(sql)
                }
                statement.execute("CHECKPOINT dl")
            }
        }

        return DuckLakeTestCatalogBootstrap.IsolatedCatalog(
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
                        // IllegalStateException (not RuntimeException) keeps detekt's
                        // TooGenericExceptionThrown gate green without a baseline entry.
                        throw IllegalStateException("Failed to delete $path", e)
                    }
                }
            }
        }
        Files.createDirectories(directory)
    }

    private fun escape(value: String): String = value.replace("'", "''")
}
