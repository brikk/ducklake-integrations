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
import dev.brikk.ducklake.catalog.TestingDucklakePostgreSqlCatalogServer
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
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.sql.DriverManager
import java.util.UUID

/**
 * Exercises the inlined-deletion read path: DuckLake stores small deletes
 * (below the writer's `DATA_INLINING_ROW_LIMIT`) in a per-table
 * metadata table `ducklake_inlined_delete_<tableId>`
 * (file_id, row_id, begin_snapshot) instead of as a parquet delete file.
 * The reader must apply those positions as a per-file positional-delete
 * mask at scan time.
 *
 * Tests inject inlined-delete rows directly via JDBC against an isolated
 * catalog, mirroring [TestDucklakeDeleteFileHandling]. The split
 * manager + page source provider stack is exercised end-to-end; the
 * stats path is verified separately against the [DucklakeMetadata]
 * surface.
 */
class TestDucklakeInlinedDeleteHandling {
    @Test
    @Throws(Exception::class)
    fun inlinedDeleteSuppressesRows() {
        Fixture.create("inlined-delete-basic").use { fx ->
            val snapshotId = fx.catalog.currentSnapshotId
            val table = getTable(fx.catalog, "test_schema", "simple_table", snapshotId)
            val tableHandle = DucklakeTableHandle(
                    "test_schema", "simple_table", table.tableId, snapshotId)
            val priceColumn = priceColumn(fx.catalog, table.tableId, snapshotId)

            val baselineSplit = getSplits(fx.splitManager, tableHandle).first()
            val baselineRows = countRows(fx.pageSourceProvider, tableHandle, baselineSplit, priceColumn)
            assertThat(baselineRows).isGreaterThan(1)

            val victim = findDataFileForSplit(fx.catalog, table.tableId, snapshotId, baselineSplit)
            createInlinedDeleteTable(fx.isolated.jdbcUrl, fx.isolated.user, fx.isolated.password, table.tableId)
            insertInlinedDelete(fx.isolated.jdbcUrl, fx.isolated.user, fx.isolated.password,
                    table.tableId, victim.dataFileId, 0L, snapshotId)

            val splitAfter = getSplits(fx.splitManager, tableHandle).stream()
                    .filter { split -> split.dataFilePath == baselineSplit.dataFilePath }
                    .findFirst()
                    .orElseThrow()
            assertThat(splitAfter.inlinedDeletedRowPositions).containsExactly(0L)

            val rowsAfter = countRows(fx.pageSourceProvider, tableHandle, splitAfter, priceColumn)
            assertThat(rowsAfter).isEqualTo(baselineRows - 1)
        }
    }

    @Test
    @Throws(Exception::class)
    fun inlinedDeletePerFileGranularity() {
        // partitioned_table is partitioned by region (US/EU/APAC), producing one
        // parquet file per partition value. Injecting an inlined delete pointing
        // at one data file must not affect rows in the other files.
        Fixture.create("inlined-delete-per-file").use { fx ->
            val snapshotId = fx.catalog.currentSnapshotId
            val table = getTable(fx.catalog, "test_schema", "partitioned_table", snapshotId)
            val tableHandle = DucklakeTableHandle(
                    "test_schema", "partitioned_table", table.tableId, snapshotId)
            val idColumn = idColumn(fx.catalog, table.tableId, snapshotId)

            val baselineSplits = getSplits(fx.splitManager, tableHandle)
            assertThat(baselineSplits).hasSizeGreaterThanOrEqualTo(2)

            val targetSplit = baselineSplits.first()
            val otherSplit = baselineSplits[1]
            val targetBaseline = countRows(fx.pageSourceProvider, tableHandle, targetSplit, idColumn)
            val otherBaseline = countRows(fx.pageSourceProvider, tableHandle, otherSplit, idColumn)
            assertThat(targetBaseline).isGreaterThan(0)
            assertThat(otherBaseline).isGreaterThan(0)

            val targetFile = findDataFileForSplit(fx.catalog, table.tableId, snapshotId, targetSplit)
            createInlinedDeleteTable(fx.isolated.jdbcUrl, fx.isolated.user, fx.isolated.password, table.tableId)
            insertInlinedDelete(fx.isolated.jdbcUrl, fx.isolated.user, fx.isolated.password,
                    table.tableId, targetFile.dataFileId, 0L, snapshotId)

            val splitsAfter = getSplits(fx.splitManager, tableHandle)
            val targetAfter = splitsAfter.stream()
                    .filter { split -> split.dataFilePath == targetSplit.dataFilePath }
                    .findFirst()
                    .orElseThrow()
            val otherAfter = splitsAfter.stream()
                    .filter { split -> split.dataFilePath == otherSplit.dataFilePath }
                    .findFirst()
                    .orElseThrow()

            assertThat(targetAfter.inlinedDeletedRowPositions).containsExactly(0L)
            assertThat(otherAfter.inlinedDeletedRowPositions).isEmpty()

            val targetRowsAfter = countRows(fx.pageSourceProvider, tableHandle, targetAfter, idColumn)
            val otherRowsAfter = countRows(fx.pageSourceProvider, tableHandle, otherAfter, idColumn)
            assertThat(targetRowsAfter).isEqualTo(targetBaseline - 1)
            assertThat(otherRowsAfter).isEqualTo(otherBaseline)
        }
    }

    @Test
    @Throws(Exception::class)
    fun inlinedDeleteRespectsTimeTravelSnapshotId() {
        // Deletion has begin_snapshot strictly greater than the read snapshot, so
        // the WHERE begin_snapshot <= snapshotId filter excludes it. The reader
        // must see all rows in the file.
        Fixture.create("inlined-delete-time-travel").use { fx ->
            val snapshotId = fx.catalog.currentSnapshotId
            val table = getTable(fx.catalog, "test_schema", "simple_table", snapshotId)
            val tableHandle = DucklakeTableHandle(
                    "test_schema", "simple_table", table.tableId, snapshotId)
            val priceColumn = priceColumn(fx.catalog, table.tableId, snapshotId)

            val baselineSplit = getSplits(fx.splitManager, tableHandle).first()
            val baselineRows = countRows(fx.pageSourceProvider, tableHandle, baselineSplit, priceColumn)

            val victim = findDataFileForSplit(fx.catalog, table.tableId, snapshotId, baselineSplit)
            createInlinedDeleteTable(fx.isolated.jdbcUrl, fx.isolated.user, fx.isolated.password, table.tableId)
            // begin_snapshot = snapshotId + 1000 — definitely in the future.
            insertInlinedDelete(fx.isolated.jdbcUrl, fx.isolated.user, fx.isolated.password,
                    table.tableId, victim.dataFileId, 0L, snapshotId + 1000)

            // hasInlinedDeletes uses the same begin_snapshot <= filter, so it
            // must return false for the current snapshot.
            assertThat(fx.catalog.hasInlinedDeletes(table.tableId, snapshotId)).isFalse()

            val splitAfter = getSplits(fx.splitManager, tableHandle).stream()
                    .filter { split -> split.dataFilePath == baselineSplit.dataFilePath }
                    .findFirst()
                    .orElseThrow()
            assertThat(splitAfter.inlinedDeletedRowPositions).isEmpty()

            val rowsAfter = countRows(fx.pageSourceProvider, tableHandle, splitAfter, priceColumn)
            assertThat(rowsAfter).isEqualTo(baselineRows)
        }
    }

    @Test
    @Throws(Exception::class)
    fun inlinedDeleteInvalidatesTableStats() {
        Fixture.create("inlined-delete-stats").use { fx ->
            val snapshotId = fx.catalog.currentSnapshotId
            val table = getTable(fx.catalog, "test_schema", "simple_table", snapshotId)
            val tableHandle = DucklakeTableHandle(
                    "test_schema", "simple_table", table.tableId, snapshotId)

            val typeConverter = DucklakeTypeConverter(
                    io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER)
            val metadata = DucklakeMetadata(fx.catalog, typeConverter)

            // Baseline: stats should be present with row count + column stats.
            val baseline = metadata.getTableStatistics(SESSION, tableHandle)
            assertThat(baseline.rowCount.isUnknown).isFalse()
            assertThat(baseline.columnStatistics).isNotEmpty()

            val victim = fx.catalog.getDataFiles(table.tableId, snapshotId).first()
            createInlinedDeleteTable(fx.isolated.jdbcUrl, fx.isolated.user, fx.isolated.password, table.tableId)
            insertInlinedDelete(fx.isolated.jdbcUrl, fx.isolated.user, fx.isolated.password,
                    table.tableId, victim.dataFileId, 0L, snapshotId)

            val afterStats = metadata.getTableStatistics(SESSION, tableHandle)
            assertThat(afterStats.rowCount.isUnknown).isTrue()
            assertThat(afterStats.columnStatistics).isEmpty()
        }
    }

    @Test
    @Throws(Exception::class)
    fun absentInlinedDeleteTableProbesCleanly() {
        // Common case: most tables never had a deletion small enough to inline,
        // so ducklake_inlined_delete_<tableId> doesn't exist at all. The probe
        // must return false (and not propagate a SQLException) so the read
        // proceeds normally.
        Fixture.create("inlined-delete-absent").use { fx ->
            val snapshotId = fx.catalog.currentSnapshotId
            val table = getTable(fx.catalog, "test_schema", "simple_table", snapshotId)

            assertThat(fx.catalog.hasInlinedDeletes(table.tableId, snapshotId)).isFalse()
            assertThat(fx.catalog.getInlinedDeletes(table.tableId, snapshotId)).isEmpty()
        }
    }

    // ==================== helpers ====================

    private class Fixture private constructor(
            val isolated: DucklakeCatalogGenerator.IsolatedCatalog,
            val catalog: DucklakeCatalog,
            val splitManager: DucklakeSplitManager,
            val pageSourceProvider: DucklakePageSourceProvider) : AutoCloseable {

        override fun close() {
            catalog.close()
        }

        companion object {
            @Throws(Exception::class)
            fun create(tag: String): Fixture {
                val server: TestingDucklakePostgreSqlCatalogServer = DucklakeTestCatalogEnvironment.getServer()
                val isolated = DucklakeCatalogGenerator.generateIsolatedPostgreSqlCatalog(
                        server, tag + "-" + UUID.randomUUID().toString().substring(0, 8))

                val config = DucklakeConfig()
                        .setCatalogDatabaseUrl(isolated.jdbcUrl)
                        .setCatalogDatabaseUser(isolated.user)
                        .setCatalogDatabasePassword(isolated.password)
                        .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                        .setMaxCatalogConnections(5)

                val catalog: DucklakeCatalog = JdbcDucklakeCatalog(config.toCatalogConfig())
                val splitManager = DucklakeSplitManager(
                        catalog, config, DucklakePathResolver(catalog, config), io.trino.filesystem.cache.NoopSplitAffinityProvider())
                val pageSourceProvider = DucklakePageSourceProvider(
                        LocalFileSystemFactory(Path.of("/")),
                        FileFormatDataSourceStats(),
                        ParquetReaderConfig().toParquetReaderOptions(),
                        catalog,
                        config)
                return Fixture(isolated, catalog, splitManager, pageSourceProvider)
            }
        }
    }

    companion object {
        @Throws(Exception::class)
        private fun createInlinedDeleteTable(jdbcUrl: String, user: String?, password: String?, tableId: Long) {
            DriverManager.getConnection(jdbcUrl, user, password).use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(String.format(
                            "CREATE TABLE IF NOT EXISTS ducklake_inlined_delete_%d " +
                                    "(file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)",
                            tableId))
                }
            }
        }

        @Throws(Exception::class)
        private fun insertInlinedDelete(
                jdbcUrl: String, user: String?, password: String?,
                tableId: Long, dataFileId: Long, rowId: Long, beginSnapshot: Long) {
            DriverManager.getConnection(jdbcUrl, user, password).use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute(String.format(
                            "INSERT INTO ducklake_inlined_delete_%d VALUES (%d, %d, %d)",
                            tableId, dataFileId, rowId, beginSnapshot))
                }
            }
        }

        @Throws(Exception::class)
        private fun getSplits(splitManager: DucklakeSplitManager, tableHandle: DucklakeTableHandle): List<DucklakeSplit> {
            splitManager.getSplits(
                    null, SESSION, tableHandle, mutableSetOf(), Constraint.alwaysTrue()).use { splitSource ->
                val splits = ImmutableList.builder<DucklakeSplit>()
                while (!splitSource.isFinished) {
                    for (split: ConnectorSplit in splitSource.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).get()) {
                        if (split is DucklakeSplit) {
                            splits.add(split)
                        }
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
                column: DucklakeColumnHandle): Long {
            var rows = 0L
            pageSourceProvider.createPageSource(
                    null, SESSION, split, tableHandle, java.util.Optional.empty(), ImmutableList.of<ColumnHandle>(column), DynamicFilter.EMPTY).use { pageSource ->
                while (!pageSource.isFinished) {
                    val page = pageSource.nextSourcePage
                    if (page != null) {
                        rows += page.positionCount
                    }
                }
            }
            return rows
        }

        private fun findDataFileForSplit(
                catalog: DucklakeCatalog, tableId: Long, snapshotId: Long, split: DucklakeSplit): DucklakeDataFile {
            return catalog.getDataFiles(tableId, snapshotId).stream()
                    .filter { dataFile -> split.dataFilePath.endsWith(dataFile.path) }
                    .findFirst()
                    .orElseThrow { AssertionError("Missing data file metadata for split path: " + split.dataFilePath) }
        }

        private fun priceColumn(catalog: DucklakeCatalog, tableId: Long, snapshotId: Long): DucklakeColumnHandle {
            val columnId = catalog.getTableColumns(tableId, snapshotId).stream()
                    .filter { column -> column.columnName == "price" }
                    .findFirst()
                    .orElseThrow()
                    .columnId
            return DucklakeColumnHandle(columnId, "price", DOUBLE, true)
        }

        private fun idColumn(catalog: DucklakeCatalog, tableId: Long, snapshotId: Long): DucklakeColumnHandle {
            val columnId = catalog.getTableColumns(tableId, snapshotId).stream()
                    .filter { column -> column.columnName == "id" }
                    .findFirst()
                    .orElseThrow()
                    .columnId
            return DucklakeColumnHandle(columnId, "id", io.trino.spi.type.IntegerType.INTEGER, true)
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
