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
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext
import io.trino.parquet.Column
import io.trino.parquet.Field
import io.trino.parquet.ParquetDataSource
import io.trino.parquet.ParquetDataSourceId
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.ParquetTypeUtils.getColumnIO
import io.trino.parquet.ParquetTypeUtils.getDescriptors
import io.trino.parquet.predicate.PredicateUtils.buildPredicate
import io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups
import io.trino.parquet.reader.MetadataReader
import io.trino.parquet.reader.ParquetReader
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetPageSource
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.Type
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.ColumnIO
import org.apache.parquet.io.MessageColumnIO
import org.apache.parquet.io.PrimitiveColumnIO
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.joda.time.DateTimeZone.UTC
import java.io.IOException
import java.nio.file.Path
import java.util.HashSet
import java.util.Optional
import java.util.OptionalLong

/**
 * Reads positional rows from a DuckLake delete parquet file. Delete files contain a
 * single primitive column (INT64 or INT32) whose values are either global row IDs or
 * file-local row offsets — the page source decides which interpretation applies at
 * filter time, so this reader returns the raw values verbatim.
 *
 * Centralized so both the read path (apply-deletes during scan) and the write
 * path (merge sink unioning prior positions into a superseding file) call the same
 * code. Keeping the reader in one place is a B3a invariant: if the two paths drift,
 * the union could mis-interpret values and either lose or re-introduce deletions.
 */
public object DucklakeDeleteFileReader {
    /**
     * Reads all non-null positions from a single parquet delete file. The returned set
     * is unordered and dedup'd (the read path tolerates duplicate aliasing — see
     * `TestDeleteRowFilterTransformOverlap`).
     */
    @Throws(IOException::class)
    public fun readPositions(
            fileSystem: TrinoFileSystem,
            deleteFilePath: String,
            footerSizeHint: Long,
            parquetReaderOptions: ParquetReaderOptions,
            stats: FileFormatDataSourceStats): Set<Long> {
        val inputFile = fileSystem.newInputFile(toLocation(deleteFilePath))

        val memoryContext = newSimpleAggregatedMemoryContext()
        var dataSource: ParquetDataSource? = null
        try {
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.empty(),
                    parquetReaderOptions,
                    memoryContext,
                    stats)

            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(dataSource, footerSizeHint)

            val parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions,
                    Optional.empty(),
                    Optional.empty())
            val fileMetadata = parquetMetadata.getFileMetaData()
            val dataSourceId: ParquetDataSourceId = dataSource.getId()
            val fileSchema: MessageType = fileMetadata.getSchema()
            val messageColumnIO = getColumnIO(fileSchema, fileSchema)

            val deleteFileColumn = getDeleteFileColumn(fileSchema, messageColumnIO)
            val descriptorsByPath: Map<List<String>, ColumnDescriptor> = getDescriptors(fileSchema, fileSchema)
            val parquetTupleDomain: TupleDomain<ColumnDescriptor> = TupleDomain.all()
            val parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC)
            val rowGroups = getFilteredRowGroups(
                    0,
                    inputFile.length(),
                    dataSource,
                    parquetMetadata,
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    UTC,
                    Domain.DEFAULT_COMPACTION_THRESHOLD,
                    parquetReaderOptions)

            val capturedDataSourceId = dataSourceId
            val parquetReader = ParquetReader(
                    Optional.ofNullable(fileMetadata.getCreatedBy()),
                    ImmutableList.of(Column(deleteFileColumn.columnName, deleteFileColumn.field)),
                    false,
                    rowGroups,
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    { exception -> handleParquetException(capturedDataSourceId, exception) },
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty())

            val deletedRows: MutableSet<Long> = HashSet()
            val pageSource: ConnectorPageSource = ParquetPageSource(parquetReader)
            try {
                while (!pageSource.isFinished()) {
                    val page = pageSource.getNextSourcePage() ?: continue
                    val block = page.getBlock(0)
                    var position = 0
                    while (position < block.getPositionCount()) {
                        if (block.isNull(position)) {
                            position++
                            continue
                        }
                        deletedRows.add(readDeleteValue(deleteFileColumn.columnType, block, position))
                        position++
                    }
                }
            }
            finally {
                pageSource.close()
            }
            return deletedRows
        }
        catch (e: IOException) {
            if (dataSource != null) {
                try {
                    dataSource.close()
                }
                catch (ex: IOException) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex)
                    }
                }
            }
            throw RuntimeException("Failed to read delete file: " + deleteFilePath, e)
        }
        catch (e: RuntimeException) {
            if (dataSource != null) {
                try {
                    dataSource.close()
                }
                catch (ex: IOException) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex)
                    }
                }
            }
            throw RuntimeException("Failed to read delete file: " + deleteFilePath, e)
        }
    }

    private fun getDeleteFileColumn(fileSchema: MessageType, messageColumnIO: MessageColumnIO): DeleteFileColumn {
        for (field in fileSchema.getFields()) {
            if (!field.isPrimitive()) {
                continue
            }
            val columnIO: ColumnIO = messageColumnIO.getChild(field.getName())
            if (columnIO !is PrimitiveColumnIO) {
                continue
            }
            val primitiveColumnIO: PrimitiveColumnIO = columnIO

            val primitiveTypeName: PrimitiveTypeName = primitiveColumnIO.getPrimitive()
            val columnType: Type? = when (primitiveTypeName) {
                PrimitiveTypeName.INT64 -> BIGINT
                PrimitiveTypeName.INT32 -> INTEGER
                else -> null
            }

            if (columnType != null) {
                val fieldDefinition: Field = DucklakeParquetTypeUtils.constructField(columnType, columnIO)
                        .orElseThrow { TrinoException(NOT_SUPPORTED, "Could not construct field for delete file column: " + field.getName()) }
                return DeleteFileColumn(field.getName(), columnType, fieldDefinition)
            }
        }

        throw TrinoException(NOT_SUPPORTED, "Delete file must contain at least one INT32/INT64 primitive column")
    }

    private fun readDeleteValue(type: Type, block: io.trino.spi.block.Block, position: Int): Long {
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position)
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position).toLong()
        }
        throw IllegalArgumentException("Unsupported delete file value type: " + type)
    }

    private fun handleParquetException(dataSourceId: ParquetDataSourceId, exception: Exception): RuntimeException {
        if (exception is TrinoException) {
            return exception
        }
        return TrinoException(
                NOT_SUPPORTED,
                "Error reading Parquet file: " + dataSourceId,
                exception)
    }

    private fun toLocation(path: String): Location {
        val location = Location.of(path)
        if (location.scheme().isPresent) {
            return location
        }
        return Location.of(Path.of(path).toUri().toString())
    }

    @JvmRecord
    private data class DeleteFileColumn(val columnName: String, val columnType: Type, val field: Field)
}
