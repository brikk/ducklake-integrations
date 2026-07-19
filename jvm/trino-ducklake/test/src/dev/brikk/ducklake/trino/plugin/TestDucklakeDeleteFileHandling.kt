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

import com.google.common.collect.ImmutableList
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import io.trino.filesystem.cache.NoopSplitAffinityProvider
import io.trino.filesystem.local.LocalFileSystemFactory
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.DynamicFilter
import io.trino.spi.connector.DynamicFilterSnapshot
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.testing.connector.TestingConnectorSession.SESSION
import org.assertj.core.api.Assertions.assertThat
import org.jooq.impl.DSL
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Optional
import java.util.UUID

class TestDucklakeDeleteFileHandling {
    @Test
    fun testDeleteFileSuppressesRows() {
        val server = DucklakeTestCatalogEnvironment.getServer()
        val isolated = DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(
            server, "delete-file-" + UUID.randomUUID().toString().substring(0, 8))

        val config = DucklakeConfig()
            .setCatalogDatabaseUrl(isolated.jdbcUrl)
            .setCatalogDatabaseUser(isolated.user)
            .setCatalogDatabasePassword(isolated.password)
            .setDataPath(isolated.dataDir.toAbsolutePath().toString())
            .setMaxCatalogConnections(5)

        val catalog: DucklakeCatalog = JdbcDucklakeCatalog(config.toCatalogConfig())
        try {
            val splitManager = DucklakeSplitManager(catalog, config, DucklakePathResolver(catalog, config), NoopSplitAffinityProvider())
            val pageSourceProvider = DucklakePageSourceProvider(
                LocalFileSystemFactory(Path.of("/")),
                FileFormatDataSourceStats(),
                ParquetReaderConfig().toParquetReaderOptions(),
                catalog,
                config)

            val snapshotId = catalog.currentSnapshotId
            val table = getTable(catalog, "test_schema", "simple_table", snapshotId)
            val tableHandle = DucklakeTableHandle("test_schema", "simple_table", table.tableId, snapshotId)
            val priceColumnId = getColumnId(catalog, table.tableId, snapshotId, "price")
            val priceColumn = DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true)

            val splitBeforeDelete = getSplits(splitManager, tableHandle).first()
            val baselineRows = countRows(pageSourceProvider, tableHandle, splitBeforeDelete, priceColumn)
            assertThat(baselineRows).isGreaterThan(1)

            val dataFile = findDataFileForSplit(catalog, table.tableId, snapshotId, splitBeforeDelete)
            val deleteFilePath = writeDeleteParquetFile(splitBeforeDelete.dataFilePath, dataFile.rowIdStart)
            insertDeleteFileMetadata(
                isolated.jdbcUrl,
                isolated.user!!,
                isolated.password!!,
                table.tableId,
                snapshotId,
                dataFile.dataFileId,
                deleteFilePath.pathForCatalog,
                deleteFilePath.pathIsRelative,
                Files.size(deleteFilePath.absolutePath))

            val splitAfterDelete = getSplits(splitManager, tableHandle).stream()
                .filter { split -> split.dataFilePath == splitBeforeDelete.dataFilePath }
                .findFirst()
                .orElseThrow { AssertionError("Expected split for data file: " + splitBeforeDelete.dataFilePath) }

            assertThat(splitAfterDelete.deleteFilePath()).isPresent()
            val rowsAfterDelete = countRows(pageSourceProvider, tableHandle, splitAfterDelete, priceColumn)
            assertThat(rowsAfterDelete).isEqualTo(baselineRows - 1)
        }
        finally {
            catalog.close()
        }
    }

    @JvmRecord
    private data class DeleteFilePath(val pathForCatalog: String, val pathIsRelative: Boolean, val absolutePath: Path)

    companion object {
        @Throws(Exception::class)
        private fun getSplits(splitManager: DucklakeSplitManager, tableHandle: DucklakeTableHandle): List<DucklakeSplit> {
            splitManager.getSplits(
                null,
                SESSION,
                tableHandle,
                mutableSetOf(),
                Constraint.alwaysTrue()).use { splitSource ->
                val splits = ImmutableList.builder<DucklakeSplit>()
                while (!splitSource.isFinished) {
                    for (split: ConnectorSplit in splitSource.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).get()) {
                        splits.add(split as DucklakeSplit)
                    }
                }
                return splits.build()
            }
        }

        @Throws(Exception::class)
        private fun countRows(
            pageSourceProvider: DucklakePageSourceProvider,
            tableHandle: DucklakeTableHandle,
            split: DucklakeSplit,
            column: DucklakeColumnHandle
        ): Long {
            var rows: Long = 0
            pageSourceProvider.createPageSource(
                null,
                SESSION,
                split,
                tableHandle,
                Optional.empty(),
                ImmutableList.of<ColumnHandle>(column),
                DynamicFilter.EMPTY).use { pageSource ->
                while (!pageSource.isFinished) {
                    val page = pageSource.nextSourcePage
                    if (page != null) {
                        rows += page.positionCount.toLong()
                    }
                }
            }
            return rows
        }

        private fun findDataFileForSplit(catalog: DucklakeCatalog, tableId: Long, snapshotId: Long, split: DucklakeSplit): DucklakeDataFile {
            return catalog.getDataFiles(tableId, snapshotId).stream()
                .filter { dataFile -> split.dataFilePath.endsWith(dataFile.path) }
                .findFirst()
                .orElseThrow { AssertionError("Missing data file metadata for split path: " + split.dataFilePath) }
        }

        @Throws(Exception::class)
        private fun writeDeleteParquetFile(dataFilePath: String, deletedRowId: Long): DeleteFilePath {
            val sourcePath = Path.of(dataFilePath)
            val sourceParent: Path = sourcePath.toAbsolutePath().parent
                ?: throw IllegalArgumentException("Cannot resolve delete file parent for path: $dataFilePath")
            val deleteFileName = Path.of("ducklake-delete-" + UUID.randomUUID() + ".parquet")

            val deleteAbsolutePath = sourceParent.resolve(deleteFileName)

            Files.createDirectories(deleteAbsolutePath.parent)
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("COPY (SELECT " + deletedRowId + "::BIGINT AS row_id) TO '" +
                            escapeSql(deleteAbsolutePath.toAbsolutePath().toString()) +
                            "' (FORMAT PARQUET)")
                }
            }

            return DeleteFilePath(deleteAbsolutePath.toString(), false, deleteAbsolutePath)
        }

        @Throws(Exception::class)
        private fun insertDeleteFileMetadata(
            jdbcUrl: String,
            user: String,
            password: String,
            tableId: Long,
            snapshotId: Long,
            dataFileId: Long,
            deletePath: String,
            pathIsRelative: Boolean,
            fileSizeBytes: Long
        ) {
            DriverManager.getConnection(jdbcUrl, user, password).use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")

                // Allocate the next id manually because the test bypasses the runtime catalog write
                // path (which owns sequencing). COALESCE handles the empty-table case on the first
                // insert. Race-free for tests because they run serially against an isolated catalog.
                val nextDeleteFileId = dsl.select(DSL.coalesce(DSL.max(delfile.DELETE_FILE_ID), DSL.inline(0L)).plus(1))
                    .from(delfile)
                    .fetchOne(0, Long::class.javaObjectType)

                dsl.insertInto(delfile)
                    .set(delfile.DELETE_FILE_ID, nextDeleteFileId)
                    .set(delfile.TABLE_ID, tableId)
                    .set(delfile.BEGIN_SNAPSHOT, snapshotId)
                    // END_SNAPSHOT, ENCRYPTION_KEY: omitted → SQL NULL (nullable columns).
                    .set(delfile.DATA_FILE_ID, dataFileId)
                    .set(delfile.PATH, deletePath)
                    .set(delfile.PATH_IS_RELATIVE, pathIsRelative)
                    .set(delfile.FORMAT, "parquet")
                    .set(delfile.DELETE_COUNT, 1L)
                    .set(delfile.FILE_SIZE_BYTES, fileSizeBytes)
                    .set(delfile.FOOTER_SIZE, 0L)
                    .execute()
            }
        }

        private fun escapeSql(value: String): String {
            return value.replace("'", "''")
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

        private fun getColumnId(catalog: DucklakeCatalog, tableId: Long, snapshotId: Long, columnName: String): Long {
            return catalog.getTableColumns(tableId, snapshotId).stream()
                .filter { column -> column.columnName == columnName }
                .findFirst()
                .orElseThrow { AssertionError("Missing column: $columnName") }
                .columnId
        }
    }
}
