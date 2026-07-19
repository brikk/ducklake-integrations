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
package dev.brikk.ducklake.trino.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_METADATA
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import io.trino.spi.StandardErrorCode
import io.trino.spi.TrinoException
import io.trino.spi.connector.Constraint
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jooq.impl.DSL
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.util.UUID

/**
 * Pins the §7 fail-loud guard (PLAN-duckdb-parity-moveout.md): this connector reads only parquet
 * data files. A catalog that still references a removed non-parquet `file_format` (duckdb / vortex
 * / lance, from an older deployment) must surface a NAMED [StandardErrorCode.NOT_SUPPORTED] error —
 * naming the table and the format found — at BOTH split generation and metadata load, rather than
 * silently under-returning rows.
 *
 * Two guard sites:
 *   - `DucklakeSplitManager.validateDataFileFormats` — a stale `ducklake_data_file.file_format`.
 *   - `DucklakeMetadata.validateDataFileFormatIsParquet` (via `getTableHandle`) — a stale
 *     table-scoped `data_file_format` metadata setting.
 * Also verifies the legacy `trino/<fmt>` catalog namespacing still decodes to the bare name via
 * `CatalogFileFormat.fromStored`, so the error names `vortex`, not `trino/vortex`.
 */
class TestDucklakeNonParquetDataFileGuard {
    @Test
    fun splitGenerationRejectsStaleDuckdbDataFileWithNamedError() {
        runWithCatalog("guard-split-duckdb") { isolated, catalog, splitManager, table, snapshotId ->
            val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
            val victim = catalog.getDataFiles(table.tableId, snapshotId).stream()
                    .findFirst()
                    .orElseThrow { AssertionError("No data files found for simple_table") }

            // Rewrite an existing data file's format to the removed 'duckdb' format.
            setDataFileFormat(isolated, victim.dataFileId, "duckdb")

            assertThatThrownBy {
                splitManager.getSplits(null, SESSION, tableHandle, mutableSetOf(), Constraint.alwaysTrue())
            }.isInstanceOfSatisfying(TrinoException::class.java) { ex ->
                assertThat(ex.errorCode).isEqualTo(StandardErrorCode.NOT_SUPPORTED.toErrorCode())
                assertThat(ex.message)
                        .contains("test_schema")
                        .contains("simple_table")
                        .contains("duckdb")
                        .contains("parquet")
            }
        }
    }

    @Test
    fun splitGenerationRejectsLegacyNamespacedVortexWithBareNameInError() {
        runWithCatalog("guard-split-vortex") { isolated, catalog, splitManager, table, snapshotId ->
            val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
            val victim = catalog.getDataFiles(table.tableId, snapshotId).stream()
                    .findFirst()
                    .orElseThrow { AssertionError("No data files found for simple_table") }

            // A catalog written before P6 namespaced our formats as `trino/<fmt>`; the catalog
            // layer's fromStored strips it, so the guard should name the BARE `vortex`.
            setDataFileFormat(isolated, victim.dataFileId, "trino/vortex")

            assertThatThrownBy {
                splitManager.getSplits(null, SESSION, tableHandle, mutableSetOf(), Constraint.alwaysTrue())
            }.isInstanceOfSatisfying(TrinoException::class.java) { ex ->
                assertThat(ex.errorCode).isEqualTo(StandardErrorCode.NOT_SUPPORTED.toErrorCode())
                assertThat(ex.message)
                        .contains("vortex")
                        .doesNotContain("trino/vortex")
            }
        }
    }

    @Test
    fun metadataLoadRejectsStaleTableDataFileFormatSettingWithNamedError() {
        runWithCatalog("guard-metadata-lance") { isolated, catalog, splitManager, table, snapshotId ->
            // Inject a table-scoped `data_file_format = trino/lance` metadata setting, as a catalog
            // created before P6 with WITH (data_file_format = 'lance') would carry.
            insertTableDataFileFormatSetting(isolated, table.tableId, "trino/lance")

            // The catalog decoder strips the legacy namespace (fromStored STAYS, powering the guard).
            assertThat(catalog.getTableDataFileFormat(table.tableId)).isEqualTo("lance")

            // Metadata load runs the guard through getTableHandle. Build a session that carries the
            // connector's session properties so snapshot resolution doesn't trip on an unknown one.
            val session = io.trino.testing.TestingConnectorSession.builder()
                    .setPropertyMetadata(DucklakeSessionProperties().getSessionProperties())
                    .build()
            val metadata: DucklakeMetadata = newMetadata(catalog, isolated)
            assertThatThrownBy {
                metadata.getTableHandle(
                        session,
                        io.trino.spi.connector.SchemaTableName("test_schema", "simple_table"),
                        java.util.Optional.empty(),
                        java.util.Optional.empty())
            }.isInstanceOfSatisfying(TrinoException::class.java) { ex ->
                assertThat(ex.errorCode).isEqualTo(StandardErrorCode.NOT_SUPPORTED.toErrorCode())
                assertThat(ex.message)
                        .contains("test_schema")
                        .contains("simple_table")
                        .contains("lance")
                        .contains("parquet")
            }
        }
    }

    private fun interface CatalogTest {
        @Throws(Exception::class)
        fun run(
                isolated: DucklakeCatalogGenerator.IsolatedCatalog,
                catalog: DucklakeCatalog,
                splitManager: DucklakeSplitManager,
                table: DucklakeTable,
                snapshotId: Long)
    }

    companion object {
        @Throws(Exception::class)
        private fun runWithCatalog(suffix: String, test: CatalogTest) {
            val server = DucklakeTestCatalogEnvironment.getServer()
            val isolated = DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(
                    server, suffix + "-" + UUID.randomUUID().toString().substring(0, 8))

            val config = configFor(isolated)

            val catalog: DucklakeCatalog = JdbcDucklakeCatalog(config.toCatalogConfig())
            try {
                val splitManager = DucklakeSplitManager(
                        catalog, config, DucklakePathResolver(catalog, config),
                        io.trino.filesystem.cache.NoopSplitAffinityProvider())
                val snapshotId = catalog.currentSnapshotId
                val table = getTable(catalog, "test_schema", "simple_table", snapshotId)
                test.run(isolated, catalog, splitManager, table, snapshotId)
            }
            finally {
                catalog.close()
            }
        }

        private fun configFor(isolated: DucklakeCatalogGenerator.IsolatedCatalog): DucklakeConfig =
                DucklakeConfig()
                        .setCatalogDatabaseUrl(isolated.jdbcUrl)
                        .setCatalogDatabaseUser(isolated.user)
                        .setCatalogDatabasePassword(isolated.password)
                        .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                        .setMaxCatalogConnections(5)

        private fun newMetadata(
                catalog: DucklakeCatalog,
                isolated: DucklakeCatalogGenerator.IsolatedCatalog): DucklakeMetadata {
            val typeConverter = DucklakeTypeConverter(
                    io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
            return DucklakeMetadata(catalog, typeConverter, DucklakeSnapshotResolver(catalog, configFor(isolated)))
        }

        @Throws(Exception::class)
        private fun setDataFileFormat(
                isolated: DucklakeCatalogGenerator.IsolatedCatalog,
                dataFileId: Long,
                storedFormat: String) {
            DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val df = DUCKLAKE_DATA_FILE.`as`("df")
                dsl.update(df)
                        .set(df.FILE_FORMAT, storedFormat)
                        .where(df.DATA_FILE_ID.eq(dataFileId))
                        .execute()
            }
        }

        @Throws(Exception::class)
        private fun insertTableDataFileFormatSetting(
                isolated: DucklakeCatalogGenerator.IsolatedCatalog,
                tableId: Long,
                storedFormat: String) {
            DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val meta = DUCKLAKE_METADATA.`as`("meta")
                dsl.insertInto(meta)
                        .set(meta.KEY, "data_file_format")
                        .set(meta.VALUE, storedFormat)
                        .set(meta.SCOPE, "table")
                        .set(meta.SCOPE_ID, tableId)
                        .execute()
            }
        }

        private fun getSchema(catalog: DucklakeCatalog, schemaName: String, snapshotId: Long): DucklakeSchema {
            return catalog.listSchemas(snapshotId).stream()
                    .filter { schema -> schema.schemaName == schemaName }
                    .findFirst()
                    .orElseThrow { AssertionError("Missing schema: $schemaName") }
        }

        private fun getTable(catalog: DucklakeCatalog, schemaName: String, tableName: String, snapshotId: Long): DucklakeTable {
            val schema = getSchema(catalog, schemaName, snapshotId)
            return catalog.listTables(schema.schemaId, snapshotId).stream()
                    .filter { table -> table.tableName == tableName }
                    .findFirst()
                    .orElseThrow { AssertionError("Missing table: $schemaName.$tableName") }
        }
    }
}
