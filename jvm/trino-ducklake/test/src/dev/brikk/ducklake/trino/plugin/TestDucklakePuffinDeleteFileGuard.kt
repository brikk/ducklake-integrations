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
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE
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
 * Pins the delete-file-format gate in `DucklakeSplitManager.validateDeleteFileFormats`.
 * `parquet` and `puffin` are accepted (the actual decode happens in the page
 * source, exercised by `TestDucklakeCrossEnginePuffinDeleteRoundTrip`); any other
 * format hard-fails the query rather than silently dropping deletes.
 */
class TestDucklakePuffinDeleteFileGuard {
    @Test
    fun testPuffinFormatIsAccepted() {
        runWithCatalog("puffin-accept") { isolated, catalog, splitManager, table, snapshotId ->
            val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
            val victim = catalog.getDataFiles(table.tableId, snapshotId).stream()
                    .findFirst()
                    .orElseThrow { AssertionError("No data files found for simple_table") }

            // Inject a puffin-format delete file row pointing at a non-existent path. The
            // catalog-side format check runs before any file IO, so split building should
            // succeed without throwing. Actual decode of the puffin bytes is covered by
            // TestDucklakeCrossEnginePuffinDeleteRoundTrip with a real puffin file.
            insertDeleteFileMetadata(isolated, table.tableId, snapshotId, victim.dataFileId, "puffin",
                    "ducklake-deletion-vector-" + UUID.randomUUID() + ".puffin")

            // No exception — the guard accepts puffin.
            splitManager.getSplits(null, SESSION, tableHandle, mutableSetOf(), Constraint.alwaysTrue())
        }
    }

    @Test
    fun testUnknownFormatIsRejectedWithClearError() {
        runWithCatalog("puffin-reject") { isolated, catalog, splitManager, table, snapshotId ->
            val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
            val victim = catalog.getDataFiles(table.tableId, snapshotId).stream()
                    .findFirst()
                    .orElseThrow { AssertionError("No data files found for simple_table") }

            insertDeleteFileMetadata(isolated, table.tableId, snapshotId, victim.dataFileId, "flatfile-v9",
                    "made-up-format.bin")

            assertThatThrownBy {
                splitManager.getSplits(
                        null,
                        SESSION,
                        tableHandle,
                        mutableSetOf(),
                        Constraint.alwaysTrue())
            }
                    .isInstanceOfSatisfying(TrinoException::class.java) { ex ->
                        assertThat(ex.errorCode).isEqualTo(StandardErrorCode.NOT_SUPPORTED.toErrorCode())
                        assertThat(ex.message)
                                .contains("test_schema")
                                .contains("simple_table")
                                .contains("flatfile-v9")
                                .contains("parquet")
                                .contains("puffin")
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

            val config = DucklakeConfig()
                    .setCatalogDatabaseUrl(isolated.jdbcUrl)
                    .setCatalogDatabaseUser(isolated.user)
                    .setCatalogDatabasePassword(isolated.password)
                    .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                    .setMaxCatalogConnections(5)

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

        @Throws(Exception::class)
        private fun insertDeleteFileMetadata(
                isolated: DucklakeCatalogGenerator.IsolatedCatalog,
                tableId: Long,
                snapshotId: Long,
                dataFileId: Long,
                format: String,
                path: String) {
            DriverManager.getConnection(isolated.jdbcUrl, isolated.user, isolated.password).use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")

                val nextDeleteFileId = dsl.select(DSL.coalesce(DSL.max(delfile.DELETE_FILE_ID), DSL.inline(0L)).plus(1))
                        .from(delfile)
                        .fetchOne(0, Long::class.java)

                dsl.insertInto(delfile)
                        .set(delfile.DELETE_FILE_ID, nextDeleteFileId)
                        .set(delfile.TABLE_ID, tableId)
                        .set(delfile.BEGIN_SNAPSHOT, snapshotId)
                        .set(delfile.DATA_FILE_ID, dataFileId)
                        .set(delfile.PATH, path)
                        .set(delfile.PATH_IS_RELATIVE, true)
                        .set(delfile.FORMAT, format)
                        .set(delfile.DELETE_COUNT, 1L)
                        .set(delfile.FILE_SIZE_BYTES, 0L)
                        .set(delfile.FOOTER_SIZE, 0L)
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
