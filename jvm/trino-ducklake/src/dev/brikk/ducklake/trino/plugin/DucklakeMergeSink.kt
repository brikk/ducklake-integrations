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
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.trino.plugin.DucklakeMergeTableHandle.DataFileRange
import io.airlift.json.JsonCodec
import io.airlift.log.Logger
import io.airlift.slice.Slice
import io.airlift.slice.Slices
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.spi.Page
import io.trino.spi.block.Block
import io.trino.spi.connector.ConnectorMergeSink
import io.trino.spi.connector.ConnectorPageSink
import io.trino.spi.connector.MergePage
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.Type
import org.apache.parquet.format.CompressionCodec
import java.io.IOException
import java.io.OutputStream
import java.io.UncheckedIOException
import java.util.LinkedHashSet
import java.util.NavigableMap
import java.util.TreeMap
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture

/**
 * Merge sink for DuckLake DELETE/UPDATE/MERGE operations.
 * Collects deleted row IDs grouped by data file, writes Parquet delete files,
 * and delegates inserts to the standard DucklakePageSink.
 */
open class DucklakeMergeSink(
        private val mergeHandle: DucklakeMergeTableHandle,
        private val fileSystem: TrinoFileSystem,
        private val deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>,
        private val writerOptions: ParquetWriterOptions,
        private val parquetReaderOptions: ParquetReaderOptions,
        private val fileFormatDataSourceStats: FileFormatDataSourceStats,
        private val trinoVersion: String,
        // Insert sink for handling UPDATE inserts
        private val insertSink: ConnectorPageSink)
    : ConnectorMergeSink
{
    private val dataColumnCount: Int = mergeHandle.insertHandle.columns.size

    // Accumulated delete row IDs grouped by data file ID
    private val deletesByDataFile: MutableMap<Long, MutableList<Long>> = mutableMapOf()

    // Data-file ranges keyed by their (unique, non-overlapping) rowIdStart so resolveDataFileId
    // can resolve a global rowId in O(log F) via floorEntry instead of an O(F) linear scan per
    // deleted row. Built once here; ranges never change for the life of the sink.
    private val rangeByRowIdStart: NavigableMap<Long, DataFileRange> = TreeMap()

    init {
        for (range in mergeHandle.dataFileRanges) {
            rangeByRowIdStart[range.rowIdStart] = range
        }
    }

    override fun storeMergedRows(page: Page) {
        val mergePage: MergePage = MergePage.createDeleteAndInsertPages(page, dataColumnCount)

        mergePage.deletionsPage.ifPresent { p -> this.processDeletes(p) }
        mergePage.insertionsPage.ifPresent { insertPage -> insertSink.appendPage(insertPage) }
    }

    private fun processDeletes(deletePage: Page) {
        // Delete page has data columns followed by row ID column (last column)
        val rowIdChannel = deletePage.channelCount - 1
        val rowIdBlock: Block = deletePage.getBlock(rowIdChannel)

        var position = 0
        while (position < deletePage.positionCount) {
            val rowId = BIGINT.getLong(rowIdBlock, position)
            val dataFileId = resolveDataFileId(rowId)
            deletesByDataFile.getOrPut(dataFileId) { mutableListOf() }.add(rowId)
            position++
        }
    }

    private fun resolveDataFileId(rowId: Long): Long {
        // Ranges are contiguous [rowIdStart, rowIdStart + recordCount) and non-overlapping, so
        // the range owning rowId is the one with the greatest rowIdStart <= rowId — a single
        // floorEntry lookup, then a containment guard for the case rowId falls past the end of
        // that range (no file covers it).
        val entry = rangeByRowIdStart.floorEntry(rowId)
        if (entry != null && entry.value.containsRowId(rowId)) {
            return entry.value.dataFileId
        }
        throw IllegalStateException("No data file found for row ID: $rowId")
    }

    override fun finish(): CompletableFuture<Collection<Slice>> {
        val fragments: MutableList<Slice> = mutableListOf()

        // Write delete Parquet files
        for (entry in deletesByDataFile.entries) {
            val dataFileId = entry.key
            val rowIds: List<Long> = entry.value

            if (rowIds.isEmpty()) {
                continue
            }

            try {
                val fragment = writeDeleteFile(dataFileId, rowIds)
                fragments.add(Slices.wrappedBuffer(*deleteFragmentCodec.toJsonBytes(fragment)))
            }
            catch (e: IOException) {
                throw UncheckedIOException("Failed to write delete file for data file $dataFileId", e)
            }
        }

        // Collect insert fragments. They are already DucklakeWriteFragment JSON — the insert
        // DucklakePageSink serialized them with that codec — and finishMerge distinguishes
        // delete from insert fragments structurally (delete-fragment trial-parse plus a
        // "ducklake-delete-" path-prefix check), not by any tag. So they pass through
        // unchanged; the prior deserialize-then-reserialize was a byte-for-byte no-op.
        try {
            val insertFragments: Collection<Slice> = insertSink.finish().get()
            fragments.addAll(insertFragments)
        }
        catch (e: Exception) {
            throw RuntimeException("Failed to finish insert sink", e)
        }

        val asCollection: Collection<Slice> = fragments
        return completedFuture(asCollection)
    }

    @Throws(IOException::class)
    private fun writeDeleteFile(dataFileId: Long, rowIds: List<Long>): DucklakeDeleteFragment {
        // B3a: read any prior-active delete files for this data_file_id (paths captured on the
        // merge handle at beginMerge time) and UNION their positions with this commit's new
        // deletes. The catalog end-snapshots the prior files in the same transaction we insert
        // this new one (JdbcDucklakeCatalog.applyDeleteFragments), so the new file must carry
        // every still-deleted position or rows resurrect. record_count is decremented by
        // newDeleteCount (positions truly new this commit), since the prior file's positions
        // were already deducted when it was first committed.
        val existingDeleteFilePaths: List<String> = findExistingDeleteFilePaths(dataFileId)
        val unionPositions: LinkedHashSet<Long> = LinkedHashSet()
        for (existingPath in existingDeleteFilePaths) {
            val priorPositions: Set<Long> = DucklakeDeleteFileReader.readPositions(
                    fileSystem,
                    existingPath,
                    0L,
                    parquetReaderOptions,
                    fileFormatDataSourceStats)
            unionPositions.addAll(priorPositions)
        }
        // rowIds are disjoint from prior positions by engine invariant: the SELECT phase of
        // DELETE/UPDATE/MERGE only sees rows that aren't already tombstoned, so the engine
        // never issues a delete for a position that's in any prior active delete file. We
        // still go through a set to defend against pathological input rather than relying on
        // engine semantics for correctness of the size math.
        val preNewSize: Long = unionPositions.size.toLong()
        unionPositions.addAll(rowIds)
        val totalPositions: Long = unionPositions.size.toLong()
        val newDeleteCount: Long = totalPositions - preNewSize

        val fileName = "ducklake-delete-" + UUID.randomUUID() + ".parquet"
        val tableDataPath: String = mergeHandle.insertHandle.tableDataPath
        val filePath: Location = Location.of(tableDataPath).appendPath(fileName)

        val outputFile = fileSystem.newOutputFile(filePath)
        val outputStream: OutputStream = outputFile.create()

        val columnNames: List<String> = ImmutableList.of("row_id")
        val columnTypes: List<Type> = ImmutableList.of(BIGINT)

        val schemaConverter = ParquetSchemaConverter(
                columnTypes, columnNames, false, false)

        val parquetWriter = ParquetWriter(
                outputStream,
                schemaConverter.messageType,
                schemaConverter.primitiveTypes,
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                java.util.Optional.empty(),
                java.util.Optional.empty())

        // Write all positions (prior ∪ new) as a single page. close() must be called even on
        // write failure or both the writer and the wrapped output stream leak.
        val fileMetaData: org.apache.parquet.format.FileMetaData
        val fileSize: Long
        try {
            val blockBuilder: io.trino.spi.block.BlockBuilder = BIGINT.createBlockBuilder(null, totalPositions.toInt())
            for (position in unionPositions) {
                BIGINT.writeLong(blockBuilder, position)
            }
            val block: Block = blockBuilder.build()
            parquetWriter.write(Page(block.positionCount, block))
            parquetWriter.close()
            fileMetaData = parquetWriter.getFileMetaData()
            fileSize = parquetWriter.estimatedWrittenBytes
        }
        catch (t: Throwable) {
            try { parquetWriter.close() } catch (_: Exception) {}
            throw t
        }

        // Compute footer size
        var footerSize: Long
        try {
            val footerOutput = io.airlift.slice.DynamicSliceOutput(40)
            org.apache.parquet.format.Util.writeFileMetaData(fileMetaData, footerOutput)
            footerSize = footerOutput.size().toLong()
        }
        catch (e: IOException) {
            footerSize = 0
        }

        log.debug("Wrote delete file %s with %d total positions (%d new this commit, %d superseded from %d prior file(s)) for data file %d",
                filePath, totalPositions, newDeleteCount, preNewSize, existingDeleteFilePaths.size, dataFileId)

        return DucklakeDeleteFragment(
                dataFileId,
                fileName,
                totalPositions,
                fileSize,
                footerSize,
                newDeleteCount)
    }

    private fun findExistingDeleteFilePaths(dataFileId: Long): List<String> {
        for (range in mergeHandle.dataFileRanges) {
            if (range.dataFileId == dataFileId) {
                return range.existingDeleteFilePaths
            }
        }
        return emptyList()
    }

    override fun abort() {
        insertSink.abort()
        // Delete files written during this operation will become orphans —
        // cleaned up by DuckLake's maintenance procedures
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeMergeSink::class.java)
    }
}
