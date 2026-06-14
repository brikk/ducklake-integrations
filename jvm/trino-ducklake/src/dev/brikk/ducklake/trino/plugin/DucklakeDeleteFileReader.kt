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
import io.trino.parquet.metadata.ParquetMetadata
import io.trino.parquet.predicate.PredicateUtils.buildPredicate
import io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups
import io.trino.parquet.reader.MetadataReader
import io.trino.parquet.reader.ParquetReader
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetPageSource
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
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
import java.util.Optional
import java.util.OptionalLong

/**
 * Reads positional rows from a DuckLake delete parquet file. Two producer schemas exist,
 * and the value vocabulary follows from which one a file carries:
 *
 *  - DuckLake-spec files — what the DuckDB extension AND this connector's merge sink write —
 *    carry `(file_path, pos)`, whose INT64 `pos` column holds FILE-LOCAL row offsets;
 *  - LEGACY Trino files (merge-sink output before the cross-engine fix) carry a single
 *    [TRINO_ROW_ID_COLUMN] (`row_id`) INT64 column holding GLOBAL row ids
 *    (`rowIdStart + file position`). DuckDB rejects this shape, which is why the sink no
 *    longer writes it; the reader keeps accepting it for tables deleted by older builds.
 *
 * The reader reports which vocabulary applies via [DeletePositions.global] so callers match
 * each value under exactly one interpretation. Checking both per row — the previous design —
 * phantom-deletes rows whenever a data file's `rowIdStart < recordCount`, because global ids
 * then numerically alias local offsets of the same file.
 *
 * Centralized so both the read path (apply-deletes during scan) and the write
 * path (merge sink unioning prior positions into a superseding file) call the same
 * code. Keeping the reader in one place is a B3a invariant: if the two paths drift,
 * the union could mis-interpret values and either lose or re-introduce deletions.
 */
object DucklakeDeleteFileReader {
    /** Column name of LEGACY Trino delete files; marks a file as global-row-id valued. */
    const val TRINO_ROW_ID_COLUMN: String = "row_id"

    /** DuckLake-spec delete file columns (what upstream's writer and reader use). */
    const val SPEC_FILE_PATH_COLUMN: String = "file_path"
    const val SPEC_POSITION_COLUMN: String = "pos"

    /**
     * Non-null values of one delete file, unordered and dedup'd, plus their vocabulary:
     * [global] = true for global row ids (`row_id` schema), false for file-local offsets
     * (DuckLake-spec `pos` schema).
     */
    data class DeletePositions(val values: Set<Long>, val global: Boolean)

    /**
     * Reads all non-null positions from a single parquet delete file. The returned set
     * is unordered and dedup'd (the read path tolerates duplicate aliasing — see
     * `TestDeleteRowFilterTransformOverlap`).
     */
    @Throws(IOException::class)
    fun readPositions(
            fileSystem: TrinoFileSystem,
            deleteFilePath: String,
            footerSizeHint: Long,
            parquetReaderOptions: ParquetReaderOptions,
            stats: FileFormatDataSourceStats): DeletePositions {
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

            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(
                    dataSource, footerSizeHint, parquetReaderOptions.maxFooterReadSize.toBytes())

            val parquetMetadata = MetadataReader.readFooter(
                    dataSource,
                    parquetReaderOptions,
                    Optional.empty(),
                    Optional.empty())
            val deleteFileColumn = getDeleteFileColumn(
                    parquetMetadata.fileMetaData.schema,
                    getColumnIO(parquetMetadata.fileMetaData.schema, parquetMetadata.fileMetaData.schema))
            val parquetReader = createParquetReader(
                    dataSource, inputFile.length(), parquetMetadata, deleteFileColumn, memoryContext, parquetReaderOptions)

            val deletedRows: Set<Long> = readNonNullValues(ParquetPageSource(parquetReader), deleteFileColumn.columnType)
            val global: Boolean = deleteFileColumn.columnName.equals(TRINO_ROW_ID_COLUMN, ignoreCase = true)
            return DeletePositions(deletedRows, global)
        }
        catch (e: IOException) {
            throw closeAndWrap(dataSource, deleteFilePath, e)
        }
        catch (e: RuntimeException) {
            throw closeAndWrap(dataSource, deleteFilePath, e)
        }
    }

    /** Best-effort close of [dataSource] (suppressing its failure into [e]), then wrap [e]. */
    private fun closeAndWrap(dataSource: ParquetDataSource?, deleteFilePath: String, e: Exception): RuntimeException {
        if (dataSource != null) {
            try {
                dataSource.close()
            }
            catch (ex: IOException) {
                if (e !== ex) {
                    e.addSuppressed(ex)
                }
            }
        }
        return RuntimeException("Failed to read delete file: $deleteFilePath", e)
    }

    private fun createParquetReader(
            dataSource: ParquetDataSource,
            fileLength: Long,
            parquetMetadata: ParquetMetadata,
            deleteFileColumn: DeleteFileColumn,
            memoryContext: io.trino.memory.context.AggregatedMemoryContext,
            parquetReaderOptions: ParquetReaderOptions): ParquetReader {
        val fileMetadata = parquetMetadata.fileMetaData
        val fileSchema: MessageType = fileMetadata.schema
        val descriptorsByPath: Map<List<String>, ColumnDescriptor> = getDescriptors(fileSchema, fileSchema)
        val parquetTupleDomain: TupleDomain<ColumnDescriptor> = TupleDomain.all()
        val parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC)
        val rowGroups = getFilteredRowGroups(
                0,
                fileLength,
                dataSource,
                parquetMetadata,
                ImmutableList.of(parquetTupleDomain),
                ImmutableList.of(parquetPredicate),
                descriptorsByPath,
                UTC,
                Domain.DEFAULT_COMPACTION_THRESHOLD,
                parquetReaderOptions)

        val dataSourceId: ParquetDataSourceId = dataSource.id
        return ParquetReader(
                Optional.ofNullable(fileMetadata.createdBy),
                ImmutableList.of(Column(deleteFileColumn.columnName, deleteFileColumn.field)),
                false,
                rowGroups,
                dataSource,
                UTC,
                memoryContext,
                parquetReaderOptions,
                { exception -> handleParquetException(dataSourceId, exception) },
                Optional.empty(),
                Optional.empty(),
                Optional.empty())
    }

    /** Drains the page source, collecting every non-null value of its single column. */
    private fun readNonNullValues(pageSource: ConnectorPageSource, columnType: Type): Set<Long> {
        val values = mutableSetOf<Long>()
        pageSource.use { source ->
            while (!source.isFinished) {
                val page = source.nextSourcePage ?: continue
                val block = page.getBlock(0)
                for (position in 0 until block.positionCount) {
                    if (!block.isNull(position)) {
                        values.add(readDeleteValue(columnType, block, position))
                    }
                }
            }
        }
        return values
    }

    private fun getDeleteFileColumn(fileSchema: MessageType, messageColumnIO: MessageColumnIO): DeleteFileColumn {
        for (field in fileSchema.fields) {
            if (!field.isPrimitive) {
                continue
            }
            val columnIO: ColumnIO = messageColumnIO.getChild(field.name)
            if (columnIO !is PrimitiveColumnIO) {
                continue
            }

            val primitiveTypeName: PrimitiveTypeName = columnIO.primitive
            val columnType: Type? = when (primitiveTypeName) {
                PrimitiveTypeName.INT64 -> BIGINT
                PrimitiveTypeName.INT32 -> INTEGER
                else -> null
            }

            if (columnType != null) {
                val fieldDefinition: Field = DucklakeParquetTypeUtils.constructField(columnType, columnIO)
                        .orElseThrow { TrinoException(NOT_SUPPORTED, "Could not construct field for delete file column: ${field.name}") }
                return DeleteFileColumn(field.name, columnType, fieldDefinition)
            }
        }

        throw TrinoException(NOT_SUPPORTED, "Delete file must contain at least one INT32/INT64 primitive column")
    }

    private fun readDeleteValue(type: Type, block: Block, position: Int): Long = when {
        type == BIGINT -> BIGINT.getLong(block, position)
        type == INTEGER -> INTEGER.getInt(block, position).toLong()
        else -> throw IllegalArgumentException("Unsupported delete file value type: $type")
    }

    private fun handleParquetException(dataSourceId: ParquetDataSourceId, exception: Exception): RuntimeException =
        exception as? TrinoException
            ?: TrinoException(
                NOT_SUPPORTED,
                "Error reading Parquet file: $dataSourceId",
                exception)

    private fun toLocation(path: String): Location {
        val location = Location.of(path)
        if (location.scheme().isPresent) {
            return location
        }
        return Location.of(Path.of(path).toUri().toString())
    }

    private data class DeleteFileColumn(val columnName: String, val columnType: Type, val field: Field)
}
