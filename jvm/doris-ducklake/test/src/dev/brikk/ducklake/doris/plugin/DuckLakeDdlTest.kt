package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.api.ConnectorType
import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * Exercises [DuckLakeConnectorMetadata]'s DDL (W1) against the real Postgres-backed
 * DuckLake catalog — CREATE/DROP DATABASE + CREATE/DROP TABLE are pure catalog metadata
 * (no BE), so the whole path is headless. The live route is the same `SPI_READY_TYPES`
 * gate as INSERT (W1b); this is the independent oracle for the mapping + wiring.
 */
internal class DuckLakeDdlTest {

    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUp() {
            server = TestingDucklakePostgreSqlCatalogServer()
            isolated = DuckLakeTestCatalogBootstrap.bootstrap(server, "writeddl")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server.close()
        }
    }

    private fun openCatalog(): JdbcDucklakeCatalog {
        val config = DucklakeCatalogConfig().apply {
            catalogDatabaseUrl = isolated.jdbcUrl()
            catalogDatabaseUser = isolated.user()
            catalogDatabasePassword = isolated.password()
            dataPath = isolated.dataDir().toAbsolutePath().toString()
        }
        return JdbcDucklakeCatalog(config)
    }

    private inline fun <R> withCatalog(block: (JdbcDucklakeCatalog) -> R): R {
        val catalog = openCatalog()
        return try {
            block(catalog)
        } finally {
            catalog.close()
        }
    }

    private fun col(name: String, type: ConnectorType, nullable: Boolean) =
        ConnectorColumn(name, type, "", nullable, null)

    @Test
    @Throws(Exception::class)
    fun createDropDatabaseAndTableLifecycle() {
        withCatalog { catalog ->
            val md = DuckLakeConnectorMetadata(catalog)

            // CREATE DATABASE
            md.createDatabase(null, "w1_schema", emptyMap())
            assertThat(md.listDatabaseNames(null)).contains("w1_schema")

            // CREATE TABLE with typed columns (the type mapping lands in the catalog).
            val request = ConnectorCreateTableRequest.builder()
                .dbName("w1_schema")
                .tableName("t1")
                .columns(
                    listOf(
                        col("id", ConnectorType.of("INT"), nullable = false),
                        col("label", ConnectorType.of("STRING"), nullable = true),
                        col("amount", ConnectorType.of("DECIMALV3", 10, 2), nullable = true),
                    ),
                )
                .ifNotExists(false)
                .build()
            md.createTable(null, request)

            val snap = catalog.currentSnapshotId
            val table = requireNotNull(catalog.getTable("w1_schema", "t1", snap)) { "table not created" }
            val columns = catalog.getTableColumns(table.tableId, snap)
            val typeByName = columns.associate { it.columnName to it.columnType }
            assertThat(typeByName).containsEntry("id", "int32")
                .containsEntry("label", "varchar")
                .containsEntry("amount", "decimal(10,2)")
            val nullableByName = columns.associate { it.columnName to it.nullsAllowed }
            assertThat(nullableByName).containsEntry("id", false).containsEntry("label", true)

            // DROP TABLE
            val handle = md.getTableHandle(null, "w1_schema", "t1").orElseThrow()
            md.dropTable(null, handle)
            assertThat(catalog.getTable("w1_schema", "t1", catalog.currentSnapshotId)).isNull()

            // DROP DATABASE (now empty)
            md.dropDatabase(null, "w1_schema", false)
            assertThat(md.listDatabaseNames(null)).doesNotContain("w1_schema")
        }
    }

    @Test
    @Throws(Exception::class)
    fun rejectsPartitionedCreateTableForNow() {
        withCatalog { catalog ->
            val request = ConnectorCreateTableRequest.builder()
                .dbName("sales")
                .tableName("wont_exist")
                .columns(listOf(col("id", ConnectorType.of("INT"), nullable = true), col("name", ConnectorType.of("STRING"), nullable = true)))
                .bucketSpec(ConnectorBucketSpec(listOf("name"), 4, "murmur3"))
                .build()
            assertThatThrownBy { DuckLakeConnectorMetadata(catalog).createTable(null, request) }
                .isInstanceOf(DorisConnectorException::class.java)
                .hasMessageContaining("partitioned")
            // And nothing was created.
            assertThat(catalog.getTable("sales", "wont_exist", catalog.currentSnapshotId)).isNull()
        }
    }

    @Test
    @Throws(Exception::class)
    fun dropDatabaseIfExistsIsNoOpWhenAbsent() {
        withCatalog { catalog ->
            // ifExists=true on a missing schema must not throw.
            DuckLakeConnectorMetadata(catalog).dropDatabase(null, "never_existed", true)
        }
    }

    @Test
    @Throws(Exception::class)
    fun rejectsUnsupportedColumnType() {
        withCatalog { catalog ->
            val request = ConnectorCreateTableRequest.builder()
                .dbName("sales")
                .tableName("wont_exist2")
                .columns(listOf(col("v", ConnectorType.of("BITMAP"), nullable = true)))
                .build()
            assertThatThrownBy { DuckLakeConnectorMetadata(catalog).createTable(null, request) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
