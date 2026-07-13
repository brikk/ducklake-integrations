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
import io.trino.spi.PageSorter
import io.trino.spi.StandardErrorCode
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorPageSink
import io.trino.spi.type.BigintType.BIGINT
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
        /**
         * Trino's in-memory sorter, used ONLY when [DucklakeWritableTableHandle.sortColumns] is
         * non-empty (gated to parquet + unpartitioned + resolvable sort spec in
         * [DucklakeMetadata]). Null-safe: when sorting is inactive it is never touched.
         */
        private val pageSorter: PageSorter? = null,
        /**
         * Lineage-preserving mode (UPDATE/MERGE rewrites): every page appended to
         * this sink carries ONE extra trailing BIGINT channel holding each row's
         * ORIGINAL DuckLake rowid, written as the embedded
         * `_ducklake_internal_row_id` column (parquet field-id 2147483540 —
         * DuckDB's MultiFileReader::ROW_ID_FIELD_ID). The column is annotated in
         * the parquet schema but is NOT a catalog column: it stays out of stats
         * (leaf targets come from the catalog tree) and out of the fragment's
         * columnStats, matching upstream. Parquet format only.
         */
        private val writeRowLineage: Boolean = false,
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

    // Sorted-write support. Active ONLY for the gated case (parquet + unpartitioned + a resolvable
    // sort spec), enforced upstream in DucklakeMetadata by leaving sortColumns empty otherwise; we
    // re-check the local invariants (parquet, no partitioner) defensively so a stray handle can
    // never flip on buffering for a path that must stay byte-for-byte unchanged. When active,
    // appendPage BUFFERS pages in memory instead of writing; finish() sorts the whole buffer with
    // PageSorter and replays it through the normal unpartitioned write path. The buffer is
    // deliberately unbounded: it is confined to the gated scope and is the accepted v1 memory
    // trade-off (a single INSERT into a sorted, unpartitioned table). getMemoryUsage() reports the
    // buffered bytes so the engine still accounts for it.
    private val sortColumns: List<DucklakeSortColumn> = handle.sortColumns
    private val sortingActive: Boolean
    private val sortTypes: List<Type>
    private val bufferedPages: MutableList<Page> = mutableListOf()
    private var bufferedRetainedBytes: Long = 0

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

        require(!writeRowLineage || FORMAT_PARQUET.equals(fileFormat, ignoreCase = true)) {
            "writeRowLineage is parquet-only (fileFormat=$fileFormat)"
        }

        // Build the Parquet schema only when we're actually writing Parquet.
        // The DuckDB-format path doesn't use these fields, and ParquetSchemaConverter
        // rejects column types it doesn't recognize (UUID, future types) — building it
        // unconditionally would block those types from ever reaching the duckdb writer.
        if (FORMAT_PARQUET.equals(fileFormat, ignoreCase = true)) {
            // Lineage mode appends the synthetic rowid column LAST so all
            // catalog-derived leaf stats indexes stay valid.
            // JSON is physically a UTF-8 VARCHAR column in parquet (ParquetSchemaConverter rejects
            // JsonType; DuckDB stores JSON the same way) — the catalog type stays 'json'.
            val converterTypes =
                    (if (writeRowLineage) columnTypes + io.trino.spi.type.BigintType.BIGINT else columnTypes)
                            .map { DucklakeJsonSupport.toParquetWriteType(it) }
            val converterNames =
                    if (writeRowLineage) columnNames + LINEAGE_COLUMN_NAME else columnNames
            val schemaConverter = ParquetSchemaConverter(
                    converterTypes, converterNames, false, false)
            this.primitiveTypes = schemaConverter.primitiveTypes
            this.messageType = DucklakeParquetSchemaBuilder.buildMessageType(
                    handle.columns,
                    handle.allCatalogColumns,
                    schemaConverter.messageType,
                    if (writeRowLineage) {
                        mapOf(LINEAGE_COLUMN_NAME to DucklakeDeleteFileReader.ROW_ID_PARQUET_FIELD_ID.toLong())
                    } else {
                        emptyMap()
                    }
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

        // Sorting is active only when the handle carries a resolved prefix AND the local
        // invariants hold: parquet writer path and no partitioner. (DucklakeMetadata already
        // guarantees these when it populates sortColumns; the defensive re-check keeps the
        // no-buffering contract for every other path.) pageSorter must be present when active.
        this.sortingActive = sortColumns.isNotEmpty() &&
                partitioner == null &&
                FORMAT_PARQUET.equals(fileFormat, ignoreCase = true)
        if (sortingActive) {
            requireNonNull(pageSorter, "pageSorter is null but sorting is active")
        }
        // Channel types the PageSorter sees. Mirrors the parquet writer's channel layout: the
        // catalog columns, plus the trailing synthetic rowid channel in lineage mode. Sort
        // channels only ever index the leading catalog columns, so the extra channel simply
        // rides along with each sorted row.
        this.sortTypes = if (writeRowLineage) columnTypes + BIGINT else columnTypes
    }

    override fun getMemoryUsage(): Long {
        var total: Long = bufferedRetainedBytes
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

        if (sortingActive) {
            // Buffer for the finish()-time global sort. The engine hands the sink fully
            // materialized pages, so retaining the reference is safe; PageSorter copies the
            // rows into its own index when we sort.
            bufferedPages.add(page)
            bufferedRetainedBytes += page.retainedSizeInBytes
            return ConnectorPageSink.NOT_BLOCKED
        }

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
            if (sortingActive && bufferedPages.isNotEmpty()) {
                writeSortedBuffer()
            }
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

    /**
     * Sort the buffered pages globally by the resolved prefix and replay them through the normal
     * unpartitioned write path (which handles lazy writer open and size-based rollover). Writing
     * in sorted order means each emitted file is internally sorted AND files are emitted
     * smallest-first, so the whole INSERT is globally sorted even if it rolls over to several
     * files. Only reached when sorting is active (parquet, unpartitioned).
     */
    @Throws(IOException::class)
    private fun writeSortedBuffer() {
        val totalPositions: Long = bufferedPages.sumOf { it.positionCount.toLong() }
        val expectedPositions: Int = totalPositions.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        val sortChannels: List<Int> = sortColumns.map { it.channel }
        val sortOrders: List<io.trino.spi.connector.SortOrder> = sortColumns.map { it.sortOrder }
        val sorted: Iterator<Page> = pageSorter!!.sort(
                sortTypes,
                bufferedPages,
                sortChannels,
                sortOrders,
                expectedPositions)
        // Release the input buffer's accounting up front; the sorter has taken ownership of the
        // rows and the sorted output is streamed page-by-page below.
        bufferedPages.clear()
        bufferedRetainedBytes = 0
        while (sorted.hasNext()) {
            appendUnpartitioned(sorted.next())
        }
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
        bufferedPages.clear()
        bufferedRetainedBytes = 0

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
                    // Percent-encode key AND value the same way DuckDB/DuckLake do
                    // (HivePartitioning::Escape), so the path round-trips through our own read side
                    // and matches cross-engine. A NULL projects the raw sentinel (its chars are all
                    // unreserved, so it is byte-identical whether encoded or not). See
                    // DucklakeHivePartitionCodec.
                    val encodedKey = DucklakeHivePartitionCodec.encode(colName)
                    val encodedValue: String = if (entry.value != null) {
                        DucklakeHivePartitionCodec.encode(entry.value!!)
                    }
                    else {
                        DucklakeHivePartitionCodec.HIVE_DEFAULT_PARTITION
                    }
                    pathBuilder.append(encodedKey).append("=").append(encodedValue).append("/")
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

        /** Embedded row-lineage column name (readers match by field-id; the name is convention). */
        const val LINEAGE_COLUMN_NAME: String = "_ducklake_internal_row_id"

        private fun duckDbLocalTempDir(): Path {
            return Paths.get(System.getProperty("java.io.tmpdir"), "ducklake-write")
        }
    }
}
