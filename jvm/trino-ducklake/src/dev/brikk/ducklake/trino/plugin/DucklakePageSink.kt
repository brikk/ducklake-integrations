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

import com.google.common.collect.ImmutableList.toImmutableList
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_DUCKDB
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_LANCE
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_PARQUET
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_VORTEX
import io.airlift.json.JsonCodec
import io.airlift.log.Logger
import io.airlift.slice.Slice
import io.airlift.slice.Slices
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.parquet.writer.ParquetWriter
import io.trino.parquet.writer.ParquetWriterOptions
import io.trino.plugin.hive.parquet.ParquetWriterConfig
import io.trino.spi.Page
import io.trino.spi.PageIndexerFactory
import io.trino.spi.StandardErrorCode
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorPageSink
import io.trino.spi.type.Type
import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.schema.MessageType
import java.io.IOException
import java.io.UncheckedIOException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Objects.requireNonNull
import java.util.Optional
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.completedFuture

class DucklakePageSink(
        private val handle: DucklakeWritableTableHandle,
        private val fileSystem: TrinoFileSystem,
        private val fragmentCodec: JsonCodec<DucklakeWriteFragment>,
        parquetWriterConfig: ParquetWriterConfig,
        duckdbTargetWriteBytes: Long,
        private val trinoVersion: String,
        pageIndexerFactory: PageIndexerFactory?,
) : ConnectorPageSink {
    private val writerOptions: ParquetWriterOptions = ParquetWriterOptions.builder()
            .setMaxBlockSize(parquetWriterConfig.blockSize)
            .setMaxPageSize(parquetWriterConfig.pageSize)
            .setMaxPageValueCount(parquetWriterConfig.pageValueCount)
            .setBatchSize(parquetWriterConfig.batchSize)
            .build()
    private val targetMaxFileSize: Long
    private val fileFormat: String = handle.fileFormat

    private val columnTypes: List<Type>
    private val columnNames: List<String>
    private val messageType: MessageType?
    private val primitiveTypes: Map<List<String>, Type>?
    private val unsignedRangeChecker: DucklakeUnsignedRangeChecker

    // Partition support
    private val partitioner: DucklakePagePartitioner?

    // Writers: for unpartitioned tables, only index 0 is used.
    // For partitioned tables, one writer per unique partition combination.
    private val writers: MutableList<DucklakeFileWriter?> = mutableListOf()
    private val completedFragments: MutableList<DucklakeWriteFragment> = mutableListOf()
    private val writtenFilePaths: MutableList<Location> = mutableListOf()

    init {

        // Per-format rollover threshold. Parquet's block size targets compressed
        // on-disk row-group size; DuckDB's target is logical input bytes (we can't
        // probe on-disk size mid-write without a CHECKPOINT).
        this.targetMaxFileSize = if (FORMAT_DUCKDB.equals(fileFormat, ignoreCase = true)) {
            duckdbTargetWriteBytes
        }
        else {
            parquetWriterConfig.blockSize.toBytes()
        }

        this.columnTypes = handle.columns.map(DucklakeColumnHandle::columnType)
        this.columnNames = handle.columns.map(DucklakeColumnHandle::columnName)
        this.unsignedRangeChecker = DucklakeUnsignedRangeChecker.build(
                handle.columns, handle.allCatalogColumns)

        // Build the Parquet schema only when we're actually writing Parquet.
        // The DuckDB-format path doesn't use these fields, and ParquetSchemaConverter
        // rejects column types it doesn't recognize (UUID, future types) — building it
        // unconditionally would block those types from ever reaching the duckdb writer.
        if (FORMAT_PARQUET.equals(fileFormat, ignoreCase = true)) {
            val schemaConverter = ParquetSchemaConverter(
                    columnTypes, columnNames, false, false)
            this.primitiveTypes = schemaConverter.primitiveTypes
            this.messageType = DucklakeParquetSchemaBuilder.buildMessageType(
                    handle.columns,
                    handle.allCatalogColumns,
                    schemaConverter.messageType
            )
        }
        else {
            this.primitiveTypes = null
            this.messageType = null
        }

        // Set up partitioner if table is partitioned
        if (handle.partitionSpec.isPresent) {
            this.partitioner = DucklakePagePartitioner(
                    requireNonNull(pageIndexerFactory, "pageIndexerFactory is null")!!,
                    handle.partitionSpec.get(),
                    handle.columns,
                    handle.temporalPartitionEncoding)
        }
        else {
            this.partitioner = null
        }
    }

    override fun getMemoryUsage(): Long {
        var total: Long = 0
        for (writer in writers) {
            if (writer != null) {
                total += writer.getRetainedBytes()
            }
        }
        return total
    }

    override fun getCompletedBytes(): Long {
        var total: Long = 0
        for (fragment in completedFragments) {
            total += fragment.fileSizeBytes
        }
        for (writer in writers) {
            if (writer != null) {
                total += writer.getApproximateWrittenBytes()
            }
        }
        return total
    }

    override fun appendPage(page: Page): CompletableFuture<*> {
        if (page.positionCount == 0) {
            return ConnectorPageSink.NOT_BLOCKED
        }

        // Validate unsigned column ranges *before* any bytes hit the Parquet writer — once
        // the writer has consumed the page, the offending value is already encoded in a
        // row-group buffer and throwing here would leave a half-written file behind. The
        // checker is a no-op (zero per-page overhead) when the table has no unsigned
        // columns, which is the common case.
        unsignedRangeChecker.validate(page)

        try {
            if (partitioner == null) {
                appendUnpartitioned(page)
            }
            else {
                appendPartitioned(page)
            }
        }
        catch (e: IOException) {
            throw UncheckedIOException("Failed to write page", e)
        }

        return ConnectorPageSink.NOT_BLOCKED
    }

    @Throws(IOException::class)
    private fun appendUnpartitioned(page: Page) {
        // Open the writer lazily — including the successor after a rollover, which leaves slot 0
        // null rather than eagerly opening a fresh writer. Eager re-open would emit a zero-row
        // data file (header+footer object + a recordCount=0 catalog row) whenever a rollover
        // lands on the final page and no further write arrives.
        var writer = writers.firstOrNull()
        if (writer == null) {
            writer = openNewWriter(mapOf())
            if (writers.isEmpty()) {
                writers.add(writer)
            }
            else {
                writers[0] = writer
            }
        }
        writer.write(page)

        if (writer.getApproximateWrittenBytes() >= targetMaxFileSize) {
            closeWriter(0)
        }
    }

    @Throws(IOException::class)
    private fun appendPartitioned(page: Page) {
        val partitioner = this.partitioner!!
        val writerIndexes = partitioner.partitionPage(page)
        val maxIndex = partitioner.getMaxIndex()
        val positionCount = page.positionCount

        // Ensure writers list is big enough
        while (writers.size <= maxIndex) {
            writers.add(null)
        }

        // Group positions by writer index directly into int[] buckets — one counting pass to
        // size each bucket, one fill pass — avoiding the per-row Integer autoboxing and the
        // per-entry stream re-materialization of a Map<Integer, List<Integer>> on the hot
        // append path. Filling in position order keeps each bucket ascending, as before.
        val counts = IntArray(maxIndex + 1)
        for (position in 0 until positionCount) {
            counts[writerIndexes[position]]++
        }
        val positionsByWriter = arrayOfNulls<IntArray>(maxIndex + 1)
        for (writerIndex in 0..maxIndex) {
            if (counts[writerIndex] > 0) {
                positionsByWriter[writerIndex] = IntArray(counts[writerIndex])
            }
        }
        val fillOffsets = IntArray(maxIndex + 1)
        for (position in 0 until positionCount) {
            val writerIndex = writerIndexes[position]
            positionsByWriter[writerIndex]!![fillOffsets[writerIndex]++] = position
        }

        // Write to each partition's writer
        for (writerIndex in 0..maxIndex) {
            val posArray = positionsByWriter[writerIndex] ?: continue

            // Get or create writer for this partition
            var writer = writers[writerIndex]
            if (writer == null) {
                // Compute partition values from the first row in this partition
                val partitionValues = partitioner.getPartitionValues(page, posArray[0])
                writer = openNewWriter(partitionValues)
                writers[writerIndex] = writer
            }

            // Extract sub-page for this partition
            val partitionPage = page.getPositions(posArray, 0, posArray.size)
            writer.write(partitionPage)

            // Rotate if over target size. Leave the slot null and re-open lazily on the next
            // page that routes to this partition — opening the successor eagerly here would
            // emit a zero-row data file if no further page lands on this writer, and would
            // recompute this partition's (page-invariant) values a second time.
            if (writer.getApproximateWrittenBytes() >= targetMaxFileSize) {
                closeWriter(writerIndex)
            }
        }
    }

    override fun finish(): CompletableFuture<Collection<Slice>> {
        try {
            for (i in writers.indices) {
                if (writers[i] != null) {
                    closeWriter(i)
                }
            }
        }
        catch (e: IOException) {
            throw UncheckedIOException("Failed to close writer", e)
        }

        val fragments: List<Slice> = completedFragments.stream()
                .map { fragment -> Slices.wrappedBuffer(*fragmentCodec.toJsonBytes(fragment)) }
                .collect(toImmutableList())

        return completedFuture(fragments)
    }

    override fun abort() {
        for (writer in writers) {
            if (writer != null) {
                try {
                    writer.abort()
                }
                catch (e: RuntimeException) {
                    log.warn(e, "Failed to abort writer")
                }
            }
        }
        writers.clear()

        // Best-effort delete all written files
        for (path in writtenFilePaths) {
            try {
                fileSystem.deleteFile(path)
            }
            catch (e: IOException) {
                log.warn(e, "Failed to delete file during abort: %s", path)
            }
        }
    }

    @Throws(IOException::class)
    private fun openNewWriter(partitionValues: Map<Int, String?>):DucklakeFileWriter {
        if (FORMAT_PARQUET.equals(fileFormat, ignoreCase = true)) {
            return openParquetWriter(partitionValues)
        }
        if (FORMAT_DUCKDB.equals(fileFormat, ignoreCase = true)) {
            return openDuckDbWriter(partitionValues)
        }
        if (FORMAT_VORTEX.equals(fileFormat, ignoreCase = true)) {
            return openVortexWriter(partitionValues)
        }
        if (FORMAT_LANCE.equals(fileFormat, ignoreCase = true)) {
            return openLanceWriter(partitionValues)
        }
        throw TrinoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported data file format: $fileFormat")
    }

    /**
     * Lance writer: same Arrow-stream path as vortex, but `COPY … (FORMAT lance)` produces a
     * dataset *directory* (manifest + data + index files), which [DuckDbArrowStreamFileWriter]
     * writes to local temp then walks+uploads file-by-file to the remote dataset location. Stats
     * are gathered inline from the same pages. (Embedding/ARRAY columns are not yet writable — the
     * Arrow-stream writer's type mapping is scalar-only; register externally-written embedding
     * datasets via `add_files(file_format => 'lance')` instead.)
     */
    @Throws(IOException::class)
    private fun openLanceWriter(partitionValues: Map<Int, String?>): DucklakeFileWriter {
        val fileName = "ducklake-${UUID.randomUUID()}.lance"
        val relativePath = buildRelativePath(partitionValues, fileName)
        val filePath = Location.of(handle.tableDataPath).appendPath(relativePath)
        writtenFilePaths.add(filePath)
        val partitionId = partitioner?.getPartitionId()
        return DuckDbArrowStreamFileWriter(
                fileSystem,
                filePath,
                relativePath,
                partitionValues,
                partitionId,
                handle.columns,
                duckDbLocalTempDir(),
                FORMAT_LANCE)
    }

    /**
     * Vortex writer: streams Trino pages through the Arrow-stream writer, which COPYs them
     * straight to a local `.vortex` file (then uploads it) and gathers column stats inline from
     * the same pages — see [DuckDbArrowStreamFileWriter] with FORMAT_VORTEX. No scratch `.db`,
     * no read-back. Always uses the arrow-stream path regardless of duckdb_writer_mode.
     */
    @Throws(IOException::class)
    private fun openVortexWriter(partitionValues: Map<Int, String?>): DucklakeFileWriter {
        val fileName = "ducklake-${UUID.randomUUID()}.vortex"
        val relativePath = buildRelativePath(partitionValues, fileName)
        val filePath = Location.of(handle.tableDataPath).appendPath(relativePath)
        writtenFilePaths.add(filePath)
        val partitionId = partitioner?.getPartitionId()
        return DuckDbArrowStreamFileWriter(
                fileSystem,
                filePath,
                relativePath,
                partitionValues,
                partitionId,
                handle.columns,
                duckDbLocalTempDir(),
                FORMAT_VORTEX)
    }

    @Throws(IOException::class)
    private fun openParquetWriter(partitionValues: Map<Int, String?>):ParquetFileWriter {
        val fileName = "ducklake-${UUID.randomUUID()}.parquet"
        val relativePath = buildRelativePath(partitionValues, fileName)

        val filePath = Location.of(handle.tableDataPath).appendPath(relativePath)
        writtenFilePaths.add(filePath)

        val outputFile = fileSystem.newOutputFile(filePath)
        val outputStream = outputFile.create()

        val parquetWriter = ParquetWriter(
                outputStream,
                messageType!!,
                primitiveTypes!!,
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                Optional.empty(),
                Optional.empty())

        val partitionId = partitioner?.getPartitionId()
        return ParquetFileWriter(parquetWriter, outputStream, relativePath, partitionValues, partitionId, handle.columns, handle.allCatalogColumns)
    }

    @Throws(IOException::class)
    private fun openDuckDbWriter(partitionValues: Map<Int, String?>):DucklakeFileWriter {
        val fileName = "ducklake-${UUID.randomUUID()}.db"
        val relativePath = buildRelativePath(partitionValues, fileName)

        val filePath = Location.of(handle.tableDataPath).appendPath(relativePath)
        writtenFilePaths.add(filePath)

        val partitionId = partitioner?.getPartitionId()

        // Pick the writer impl based on the session-driven duckdb_writer_mode on the handle.
        if (DucklakeSessionProperties.WRITER_MODE_ARROW_STREAM.equals(handle.duckDbWriterMode, ignoreCase = true)) {
            return DuckDbArrowStreamFileWriter(
                    fileSystem,
                    filePath,
                    relativePath,
                    partitionValues,
                    partitionId,
                    handle.columns,
                    duckDbLocalTempDir())
        }
        return DuckDbFileWriter(
                fileSystem,
                filePath,
                relativePath,
                partitionValues,
                partitionId,
                handle.columns,
                duckDbLocalTempDir())
    }

    private fun buildRelativePath(partitionValues: Map<Int, String?>, fileName: String): String {
        if (partitionValues.isEmpty()) {
            return fileName
        }
        val pathBuilder = StringBuilder()
        partitionValues.entries.stream()
                .sorted(java.util.Map.Entry.comparingByKey())
                .forEach { entry ->
                    val colName = partitioner!!.getPartitionColumnName(entry.key)
                    val value: String = if (entry.value != null) entry.value!! else "__HIVE_DEFAULT_PARTITION__"
                    pathBuilder.append(colName).append("=").append(value).append("/")
                }
        pathBuilder.append(fileName)
        return pathBuilder.toString()
    }

    @Throws(IOException::class)
    private fun closeWriter(index: Int) {
        val writer = writers[index] ?: return
        completedFragments.add(writer.finishAndBuildFragment())
        writers[index] = null
    }

    companion object {
        private val log: Logger = Logger.get(DucklakePageSink::class.java)

        private fun duckDbLocalTempDir(): Path {
            return Paths.get(System.getProperty("java.io.tmpdir"), "ducklake-write")
        }
    }
}
