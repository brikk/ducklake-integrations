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
     * Physical per-row column DuckLake writes into a cross-snapshot compacted ("partial") data
     * file: each row's origin snapshot id. A read at snapshot S keeps only rows whose value <= S.
     */
    const val INTERNAL_SNAPSHOT_ID_COLUMN: String = "_ducklake_internal_snapshot_id"

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
                    dataSource, inputFile.length(), parquetMetadata,
                    Column(deleteFileColumn.columnName, deleteFileColumn.field), memoryContext, parquetReaderOptions)

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

    /**
     * Iceberg / DuckLake reserved parquet field-id for the row-lineage column. Files written by
     * UPDATE / compaction embed a column tagged with this field-id (typically named
     * `_ducklake_internal_row_id`) holding each row's ORIGINAL rowid, so lineage survives file
     * rewrites. Matches DuckDB's `MultiFileReader::ROW_ID_FIELD_ID` and datafusion-ducklake's
     * `ROW_ID_PARQUET_FIELD_ID`.
     */
    const val ROW_ID_PARQUET_FIELD_ID: Int = 2_147_483_540

    /**
     * Reads the embedded row-lineage column (parquet field-id [ROW_ID_PARQUET_FIELD_ID]) of a data
     * file, returning each row's ORIGINAL rowid in file order (index = file-local position), or
     * null when the file does not carry it (fresh-INSERT files — the caller then uses
     * `row_id_start + position`). Used by the change feed so UPDATE/compaction-preserved rowids
     * pair into update pre/post-image instead of surfacing as delete+insert. Parquet only (the
     * non-parquet formats this connector writes never carry lineage).
     */
    @Throws(IOException::class)
    fun readInternalRowIds(
            fileSystem: TrinoFileSystem,
            dataFilePath: String,
            footerSizeHint: Long,
            parquetReaderOptions: ParquetReaderOptions,
            stats: FileFormatDataSourceStats): LongArray? {
        val inputFile = fileSystem.newInputFile(toLocation(dataFilePath))
        val memoryContext = newSimpleAggregatedMemoryContext()
        var dataSource: ParquetDataSource? = null
        try {
            dataSource = createDataSource(inputFile, OptionalLong.empty(), parquetReaderOptions, memoryContext, stats)
            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(
                    dataSource, footerSizeHint, parquetReaderOptions.maxFooterReadSize.toBytes())
            val parquetMetadata = MetadataReader.readFooter(dataSource, parquetReaderOptions, Optional.empty(), Optional.empty())
            val schema: MessageType = parquetMetadata.fileMetaData.schema
            // The lineage column is matched by its reserved FIELD-ID (its name is not guaranteed).
            val lineageField = schema.fields.firstOrNull { it.id?.intValue() == ROW_ID_PARQUET_FIELD_ID }
                    ?: return null
            val messageColumnIO = getColumnIO(schema, schema)
            val columnIO: ColumnIO = messageColumnIO.getChild(lineageField.name) ?: return null
            val field: Field = DucklakeParquetTypeUtils.constructField(BIGINT, columnIO).orElse(null) ?: return null
            val parquetReader = createParquetReader(
                    dataSource, inputFile.length(), parquetMetadata,
                    Column(lineageField.name, field), memoryContext, parquetReaderOptions)
            return collectRowIdsInOrder(ParquetPageSource(parquetReader))
        }
        catch (e: IOException) {
            throw closeAndWrap(dataSource, dataFilePath, e)
        }
        catch (e: RuntimeException) {
            throw closeAndWrap(dataSource, dataFilePath, e)
        }
    }

    /** Collects every BIGINT value in file order; null if any value is null (untrusted → fall back). */
    private fun collectRowIdsInOrder(pageSource: ConnectorPageSource): LongArray? {
        val values = ArrayList<Long>()
        pageSource.use { source ->
            while (!source.isFinished) {
                val page = source.nextSourcePage ?: continue
                val block = page.getBlock(0)
                for (i in 0 until block.positionCount) {
                    if (block.isNull(i)) {
                        return null
                    }
                    values.add(BIGINT.getLong(block, i))
                }
            }
        }
        return values.toLongArray()
    }

    /**
     * Reads the `_ducklake_internal_snapshot_id` column of a cross-snapshot compacted ("partial")
     * DATA file and returns the FILE-LOCAL row positions whose origin snapshot id exceeds
     * [snapshotFilterMax] — i.e. rows written by a snapshot NEWER than the read, which a
     * time-travel read at that snapshot must drop. Positions are physical row indices in file
     * order, matching the file-local-offset vocabulary [DeleteRowFilterTransform] applies. Returns
     * empty if the column is absent (file isn't actually partial).
     */
    @Throws(IOException::class)
    fun readSnapshotDropPositions(
            fileSystem: TrinoFileSystem,
            dataFilePath: String,
            footerSizeHint: Long,
            snapshotFilterMax: Long,
            parquetReaderOptions: ParquetReaderOptions,
            stats: FileFormatDataSourceStats): Set<Long> {
        val inputFile = fileSystem.newInputFile(toLocation(dataFilePath))
        val memoryContext = newSimpleAggregatedMemoryContext()
        var dataSource: ParquetDataSource? = null
        try {
            dataSource = createDataSource(inputFile, OptionalLong.empty(), parquetReaderOptions, memoryContext, stats)
            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(
                    dataSource, footerSizeHint, parquetReaderOptions.maxFooterReadSize.toBytes())
            val parquetMetadata = MetadataReader.readFooter(dataSource, parquetReaderOptions, Optional.empty(), Optional.empty())
            val schema = parquetMetadata.fileMetaData.schema
            val messageColumnIO = getColumnIO(schema, schema)
            val columnIO: ColumnIO = messageColumnIO.getChild(INTERNAL_SNAPSHOT_ID_COLUMN) ?: return emptySet()
            val field: Field = DucklakeParquetTypeUtils.constructField(BIGINT, columnIO)
                    .orElseThrow { TrinoException(NOT_SUPPORTED, "Could not construct field for $INTERNAL_SNAPSHOT_ID_COLUMN") }
            val parquetReader = createParquetReader(
                    dataSource, inputFile.length(), parquetMetadata,
                    Column(INTERNAL_SNAPSHOT_ID_COLUMN, field), memoryContext, parquetReaderOptions)
            return collectPositionsAbove(ParquetPageSource(parquetReader), snapshotFilterMax)
        }
        catch (e: IOException) {
            throw closeAndWrap(dataSource, dataFilePath, e)
        }
        catch (e: RuntimeException) {
            throw closeAndWrap(dataSource, dataFilePath, e)
        }
    }

    /** File-local positions whose BIGINT value is strictly greater than [threshold], in file order. */
    private fun collectPositionsAbove(pageSource: ConnectorPageSource, threshold: Long): Set<Long> {
        val positions = mutableSetOf<Long>()
        var base = 0L
        pageSource.use { source ->
            while (!source.isFinished) {
                val page = source.nextSourcePage ?: continue
                val block = page.getBlock(0)
                for (i in 0 until block.positionCount) {
                    if (!block.isNull(i) && BIGINT.getLong(block, i) > threshold) {
                        positions.add(base + i)
                    }
                }
                base += block.positionCount
            }
        }
        return positions
    }

    /**
     * Reads a consolidated ("partial") PARQUET delete file — columns `pos` (file-local position)
     * and `_ducklake_internal_snapshot_id` (the snapshot that recorded each deletion) — and returns
     * only the positions whose deletion snapshot is <= [snapshotFilterMax], i.e. deletions that had
     * already taken effect at the read snapshot. `file_path` is ignored (same as [readPositions] —
     * each catalog delete-file row is scoped to its data file). Always file-local (`global=false`).
     */
    @Throws(IOException::class)
    fun readPositionsWithSnapshotFilter(
            fileSystem: TrinoFileSystem,
            deleteFilePath: String,
            footerSizeHint: Long,
            snapshotFilterMax: Long,
            parquetReaderOptions: ParquetReaderOptions,
            stats: FileFormatDataSourceStats): DeletePositions {
        val inputFile = fileSystem.newInputFile(toLocation(deleteFilePath))
        val memoryContext = newSimpleAggregatedMemoryContext()
        var dataSource: ParquetDataSource? = null
        try {
            dataSource = createDataSource(inputFile, OptionalLong.empty(), parquetReaderOptions, memoryContext, stats)
            dataSource = FooterPrefetchingParquetDataSource.wrapIfHintUsable(
                    dataSource, footerSizeHint, parquetReaderOptions.maxFooterReadSize.toBytes())
            val parquetMetadata = MetadataReader.readFooter(dataSource, parquetReaderOptions, Optional.empty(), Optional.empty())
            val schema: MessageType = parquetMetadata.fileMetaData.schema
            val messageColumnIO = getColumnIO(schema, schema)
            val posColumn = requiredNamedColumn(messageColumnIO, SPEC_POSITION_COLUMN, deleteFilePath)
            val snapColumn = requiredNamedColumn(messageColumnIO, INTERNAL_SNAPSHOT_ID_COLUMN, deleteFilePath)
            val parquetReader = createParquetReaderForColumns(
                    dataSource, inputFile.length(), parquetMetadata,
                    ImmutableList.of(posColumn, snapColumn), memoryContext, parquetReaderOptions)
            return DeletePositions(collectPositionsAtOrBelow(ParquetPageSource(parquetReader), snapshotFilterMax), false)
        }
        catch (e: IOException) {
            throw closeAndWrap(dataSource, deleteFilePath, e)
        }
        catch (e: RuntimeException) {
            throw closeAndWrap(dataSource, deleteFilePath, e)
        }
    }

    /** Resolves a named BIGINT/INT column to a projection [Column], or null if absent. */
    private fun namedColumn(messageColumnIO: MessageColumnIO, name: String): Column? {
        val columnIO: ColumnIO = messageColumnIO.getChild(name) ?: return null
        val field: Field = DucklakeParquetTypeUtils.constructField(BIGINT, columnIO).orElse(null) ?: return null
        return Column(name, field)
    }

    /** Like [namedColumn] but throws a clear error when the expected column is missing. */
    private fun requiredNamedColumn(messageColumnIO: MessageColumnIO, name: String, deleteFilePath: String): Column =
        namedColumn(messageColumnIO, name)
            ?: throw TrinoException(NOT_SUPPORTED, "Partial delete file missing '$name' column: $deleteFilePath")

    /** Block 0 = `pos`, block 1 = snapshot id; keep `pos` where snapshot id <= [threshold]. */
    private fun collectPositionsAtOrBelow(pageSource: ConnectorPageSource, threshold: Long): Set<Long> {
        val positions = mutableSetOf<Long>()
        pageSource.use { source ->
            while (!source.isFinished) {
                val page = source.nextSourcePage ?: continue
                val posBlock = page.getBlock(0)
                val snapBlock = page.getBlock(1)
                for (i in 0 until posBlock.positionCount) {
                    if (!posBlock.isNull(i) && !snapBlock.isNull(i) && BIGINT.getLong(snapBlock, i) <= threshold) {
                        positions.add(BIGINT.getLong(posBlock, i))
                    }
                }
            }
        }
        return positions
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
            column: Column,
            memoryContext: io.trino.memory.context.AggregatedMemoryContext,
            parquetReaderOptions: ParquetReaderOptions): ParquetReader =
        createParquetReaderForColumns(dataSource, fileLength, parquetMetadata,
                ImmutableList.of(column), memoryContext, parquetReaderOptions)

    private fun createParquetReaderForColumns(
            dataSource: ParquetDataSource,
            fileLength: Long,
            parquetMetadata: ParquetMetadata,
            columns: List<Column>,
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
                ImmutableList.copyOf(columns),
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
        // Prefix file:// on the RAW path rather than routing through Path.toUri(), which
        // percent-encodes and DOUBLE-encodes hive partition dirs DuckDB writes URL-encoded
        // (e.g. `category=home%20appliances`). Trino's Location does not percent-decode.
        return Location.of("file://" + path)
    }

    private data class DeleteFileColumn(val columnName: String, val columnType: Type, val field: Field)
}
