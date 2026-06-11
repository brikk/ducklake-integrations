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
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_DUCKDB
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_LANCE
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.FORMAT_VORTEX
import io.airlift.log.Logger
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
import io.trino.spi.block.SqlMap
import io.trino.spi.block.SqlRow
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Int128
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType
import org.apache.arrow.c.ArrowArrayStream
import org.apache.arrow.c.Data
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.DateDayVector
import org.apache.arrow.vector.DecimalVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.SmallIntVector
import org.apache.arrow.vector.TimeStampMicroVector
import org.apache.arrow.vector.TimeStampMilliTZVector
import org.apache.arrow.vector.TimeStampMilliVector
import org.apache.arrow.vector.TimeStampNanoTZVector
import org.apache.arrow.vector.TimeStampNanoVector
import org.apache.arrow.vector.TimeStampSecVector
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.complex.impl.UnionListWriter
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema
import org.duckdb.DuckDBConnection
import java.io.IOException
import java.lang.Float.intBitsToFloat
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.sql.SQLException
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.ArrayList
import java.util.Comparator
import java.util.Optional
import java.util.OptionalLong
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicReference

/**
 * Alternative [DuckDbFileWriter] implementation that streams Trino Pages to
 * DuckDB columnarly via Apache Arrow.
 *
 *
 * Architecture: Trino's page-sink thread feeds [Page]s into a small
 * blocking queue; a worker thread drives `INSERT INTO target SELECT * FROM
 * <registered_arrow_stream>` against DuckDB; the registered stream's reader pulls
 * from the same queue, converting one Page → [VectorSchemaRoot] per
 * `loadNextBatch()`. Memory is bounded to a small constant number of Pages
 * regardless of total CTAS size, and the per-cell JNI cost of the JDBC
 * `Appender` is replaced by the Arrow C-data interface (one batch
 * round-trip per Page, no per-cell crossing).
 *
 *
 * Selected per-CREATE / per-INSERT via the `duckdb_writer_mode` session
 * property — see [DucklakeSessionProperties.WRITER_MODE_ARROW_STREAM]. The
 * existing Appender path stays available as the default fallback while we
 * compare.
 */
class DuckDbArrowStreamFileWriter
@Throws(IOException::class)
constructor(
    private val fileSystem: TrinoFileSystem,
    private val remoteLocation: Location,
    private val relativePath: String,
    partitionValues: Map<Int, String?>,
    private val partitionId: OptionalLong,
    columns: List<DucklakeColumnHandle>,
    localTempDir: Path,
    // FORMAT_DUCKDB: INSERT the Arrow stream into an ATTACHed .db (the .db is the output, stats
    // via SQL). FORMAT_VORTEX: COPY the stream straight to a .vortex file (no ATTACH/table; stats
    // gathered inline from the same pages via [statsAccumulator]). Shared Arrow plumbing either way.
    private val outputFormat: String = FORMAT_DUCKDB) : DucklakeFileWriter {
    private val partitionValues: Map<Int, String?> = partitionValues.toMap()
    private val columns: List<DucklakeColumnHandle> = columns.toList()
    private val columnTypes: List<Type> = this.columns.map { it.columnType }
    private val isVortex: Boolean = FORMAT_VORTEX.equals(outputFormat, ignoreCase = true)
    private val isLance: Boolean = FORMAT_LANCE.equals(outputFormat, ignoreCase = true)
    // vortex + lance both COPY the Arrow stream straight to the output (no ATTACH/table); stats are
    // gathered inline from the same pages. Lance additionally writes a *directory* dataset, not a
    // single file — so size/upload/cleanup walk the tree (see uploadToRemote / cleanupLocalFile).
    private val usesCopy: Boolean = isVortex || isLance
    // Inline single-pass stats for the COPY formats (no queryable table to MIN/MAX); null for duckdb.
    private val statsAccumulator: DucklakeColumnStatsAccumulator? =
            if (usesCopy) DucklakeColumnStatsAccumulator(this.columns) else null
    private val localTempFile: Path

    private val connection: DuckDBConnection
    private val allocator: BufferAllocator
    private val reader: QueueArrowReader
    private val arrayStream: ArrowArrayStream
    private val streamName: String

    private val pageQueue: BlockingQueue<Page> = ArrayBlockingQueue(2)
    private val consumerError: AtomicReference<Throwable?> = AtomicReference()
    private val consumerThread: Thread

    private var rowCount: Long = 0
    private var writtenBytes: Long = 0
    private var closed: Boolean = false

    init {
        // The vortex COPY path natively crashes or hangs the process on MAP data (probed
        // 2026-06-11, vortex extension on DuckDB 1.5.3 — memory corruption, not a clean error),
        // so MAP anywhere in a column type is rejected up front for vortex. Lance rejects MAP
        // itself with a clean versioned error ("Map ... only supported in Lance file format
        // 2.2+"), and the .db path supports MAP fully — neither needs this gate.
        if (isVortex) {
            for (col in columns) {
                if (DuckDbComplexVectorWriter.containsMapType(col.columnType)) {
                    throw TrinoException(NOT_SUPPORTED,
                            "vortex write of MAP columns is not supported (the DuckDB vortex "
                                    + "extension fails natively on MAP COPY); column "
                                    + "'${col.columnName}' has type ${col.columnType}. Use the "
                                    + "parquet or duckdb data_file_format for MAP columns.")
                }
            }
        }
        // The lance COPY leg silently morphs NULL ROW values into ROW-of-NULLs when the COPY
        // source is an arrow scan (probed 2026-06-11: a VALUES-sourced lance COPY preserves
        // struct-level nulls, and the same arrow stream INSERTed into a .db preserves them too —
        // the loss is specific to arrow-scan → lance COPY). A silent NULL→ROW(NULL,..) rewrite
        // is a correctness bug, so ROW columns are rejected for lance writes until the upstream
        // interplay is fixed; externally-written lance datasets with structs read fine via
        // add_files (their nulls survive).
        if (isLance) {
            for (col in columns) {
                if (DuckDbComplexVectorWriter.containsRowType(col.columnType)) {
                    throw TrinoException(NOT_SUPPORTED,
                            "lance write of ROW columns is not supported (the lance COPY of an "
                                    + "Arrow-streamed source loses struct-level NULLs); column "
                                    + "'${col.columnName}' has type ${col.columnType}. Use the "
                                    + "parquet, duckdb or vortex data_file_format, or register an "
                                    + "externally-written lance dataset via add_files.")
                }
            }
        }
        Files.createDirectories(localTempDir)
        // For lance this path is a *directory* DuckDB's COPY will create; for vortex a single file.
        val extension = if (isLance) "lance" else if (isVortex) "vortex" else "db"
        this.localTempFile = localTempDir.resolve("ducklake-write-${UUID.randomUUID()}.$extension")
        this.streamName = "trino_in_${UUID.randomUUID().toString().replace('-', '_')}"

        var conn: DuckDBConnection? = null
        var alloc: BufferAllocator? = null
        var rdr: QueueArrowReader? = null
        var stream: ArrowArrayStream? = null
        try {
            conn = DriverManager.getConnection("jdbc:duckdb:") as DuckDBConnection
            conn.createStatement().use { stmt ->
                if (usesCopy) {
                    // No ATTACH/table: the consumer COPYs the stream straight to the output
                    // file/dir. INSTALL may require network on first use.
                    val ext = if (isLance) "lance" else "vortex"
                    stmt.execute("INSTALL $ext")
                    stmt.execute("LOAD $ext")
                }
                else {
                    stmt.execute(
                            "ATTACH '${localTempFile.toAbsolutePath().toString().replace("'", "''")}' AS $ATTACHED_DB (READ_WRITE)")
                    stmt.execute(buildCreateTableSql())
                }
            }

            alloc = RootAllocator()
            rdr = QueueArrowReader(alloc, buildArrowSchema())
            stream = ArrowArrayStream.allocateNew(alloc)
            Data.exportArrayStream(alloc, rdr, stream)
            conn.registerArrowStream(streamName, stream)
        }
        catch (e: SQLException) {
            closeQuietly(stream)
            closeQuietly(rdr)
            closeQuietly(alloc)
            closeQuietly(conn)
            try {
                Files.deleteIfExists(localTempFile)
            }
            catch (ignored: IOException) {
                // best-effort
            }
            throw IOException("Failed to initialize Arrow-stream DuckDB writer for $remoteLocation", e)
        }
        this.connection = conn!!
        this.allocator = alloc!!
        this.reader = rdr!!
        this.arrayStream = stream!!

        this.consumerThread = Thread(this::runInsert, "ducklake-arrow-insert-$streamName")
        this.consumerThread.isDaemon = true
        this.consumerThread.start()
    }

    private fun runInsert() {
        val sql = if (usesCopy) {
            val copyFormat = if (isLance) "lance" else "vortex"
            "COPY (SELECT * FROM $streamName) TO '${localTempFile.toAbsolutePath().toString().replace("'", "''")}' (FORMAT $copyFormat)"
        }
        else {
            "INSERT INTO $ATTACHED_DB.$ATTACHED_SCHEMA.$ATTACHED_TABLE SELECT * FROM $streamName"
        }
        try {
            connection.createStatement().use { stmt ->
                stmt.execute(sql)
            }
        }
        catch (t: Throwable) {
            consumerError.set(t)
            // Drain the queue to unblock any producer waiting on put().
            pageQueue.clear()
        }
    }

    private fun buildCreateTableSql(): String {
        val sql = StringBuilder("CREATE TABLE ")
        sql.append(ATTACHED_DB).append('.').append(ATTACHED_SCHEMA).append('.').append(ATTACHED_TABLE)
        sql.append(" (")
        for (i in columns.indices) {
            if (i > 0) {
                sql.append(", ")
            }
            val col = columns[i]
            sql.append('"').append(col.columnName.replace("\"", "\"\"")).append('"')
            sql.append(' ').append(DuckDbWriterSupport.toDuckDbSqlType(col.columnType, "DuckDB Arrow-stream writer"))
            if (!col.nullable) {
                sql.append(" NOT NULL")
            }
        }
        sql.append(')')
        return sql.toString()
    }

    private fun buildArrowSchema(): Schema {
        val fields: ImmutableList.Builder<Field> = ImmutableList.builder()
        for (col in columns) {
            fields.add(toArrowField(col.columnName, col.columnType, col.nullable))
        }
        return Schema(fields.build())
    }

    @Throws(IOException::class)
    override fun write(page: Page) {
        if (page.positionCount == 0) {
            return
        }
        val err: Throwable? = consumerError.get()
        if (err != null) {
            throw IOException("Arrow-stream INSERT failed", err)
        }
        // Gather stats inline on this (producer) thread before handing the page off. Pages are
        // immutable, so reading them here while the consumer thread also reads them is safe.
        statsAccumulator?.add(page)
        try {
            // Block on a bounded queue so we don't outrun the consumer.
            pageQueue.put(page)
        }
        catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw IOException("Interrupted while feeding page to Arrow-stream writer", e)
        }
        rowCount += page.positionCount
        writtenBytes += page.getSizeInBytes()
    }

    override fun getApproximateWrittenBytes(): Long {
        // Logical input bytes appended (uncompressed). DuckDB's on-disk file is
        // smaller after columnar compression — typically 3-5×. The page sink uses
        // this to decide rollover at ducklake.duckdb.target-write-bytes.
        return writtenBytes
    }

    @Throws(IOException::class)
    override fun finishAndBuildFragment(): DucklakeWriteFragment {
        if (closed) {
            throw IOException("Arrow-stream DuckDB writer already closed: $remoteLocation")
        }
        closed = true

        // Signal end-of-stream and wait for the consumer's INSERT to drain.
        try {
            pageQueue.put(END_OF_STREAM)
            if (!consumerThread.join(java.time.Duration.ofSeconds(INSERT_JOIN_TIMEOUT_SECONDS))) {
                cleanupAfterFailure()
                throw IOException("Arrow-stream INSERT did not finish within ${INSERT_JOIN_TIMEOUT_SECONDS}s for $remoteLocation")
            }
        }
        catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            cleanupAfterFailure()
            throw IOException("Interrupted waiting for Arrow-stream INSERT to complete", e)
        }
        val err: Throwable? = consumerError.get()
        if (err != null) {
            cleanupAfterFailure()
            throw IOException("Arrow-stream INSERT failed for $remoteLocation", err)
        }

        // Stats. Vortex: the COPY already wrote the file and stats were gathered inline from the
        // stream — no table to query, no DETACH. DuckDB: query the just-written attached table.
        val columnStats: List<DucklakeFileColumnStats>
        try {
            if (usesCopy) {
                columnStats = statsAccumulator!!.build()
            }
            else {
                columnStats = extractColumnStats()
                connection.createStatement().use { stmt ->
                    stmt.execute("DETACH $ATTACHED_DB")
                }
            }
        }
        catch (e: SQLException) {
            cleanupAfterFailure()
            throw IOException("Failed to finalize Arrow-stream $outputFormat file $remoteLocation", e)
        }

        // Release Arrow + DuckDB resources before uploading.
        releaseEngineResources()

        val fileSize: Long
        try {
            fileSize = if (isLance) localDirectorySize(localTempFile) else Files.size(localTempFile)
            uploadToRemote()
        }
        catch (e: IOException) {
            cleanupLocalFile()
            throw IOException("Failed to upload Arrow-stream DuckDB file to $remoteLocation", e)
        }
        cleanupLocalFile()

        return DucklakeWriteFragment(
                relativePath,
                outputFormat,
                fileSize,
                0L,
                rowCount,
                columnStats,
                partitionValues,
                if (partitionId.isPresent) partitionId.asLong else null)
    }

    /** Whether the catalog records min/max for this type. VARBINARY/UUID never did; complex
     * types (ARRAY/ROW/MAP) get counts only — DuckDB would happily MIN/MAX them
     * lexicographically, but formatStatValue has no catalog representation for the result. */
    private fun statsMinMaxSupported(type: Type): Boolean =
        !(type == VARBINARY || type == UuidType.UUID
                || type is ArrayType || type is RowType || type is MapType)

    @Throws(SQLException::class)
    private fun extractColumnStats(): List<DucklakeFileColumnStats> {
        if (columns.isEmpty()) {
            return emptyList()
        }
        val sql = StringBuilder("SELECT COUNT(*)")
        val minMaxColumns = mutableListOf<Int>()
        for (i in columns.indices) {
            val col: DucklakeColumnHandle = columns[i]
            val name = '"' + col.columnName.replace("\"", "\"\"") + '"'
            sql.append(", COUNT(").append(name).append(")")
            if (statsMinMaxSupported(col.columnType)) {
                sql.append(", MIN(").append(name).append("), MAX(").append(name).append(")")
                minMaxColumns.add(i)
            }
        }
        sql.append(" FROM ").append(ATTACHED_DB).append('.').append(ATTACHED_SCHEMA).append('.').append(ATTACHED_TABLE)

        val totalCount: Long
        val valueCounts = LongArray(columns.size)
        val minValues = arrayOfNulls<Any>(columns.size)
        val maxValues = arrayOfNulls<Any>(columns.size)
        connection.createStatement().use { stmt ->
            stmt.executeQuery(sql.toString()).use { rs ->
                if (!rs.next()) {
                    throw SQLException("Stats query returned no rows for $remoteLocation")
                }
                var colIdx = 1
                totalCount = rs.getLong(colIdx++)
                var minMaxCursor = 0
                for (i in columns.indices) {
                    valueCounts[i] = rs.getLong(colIdx++)
                    if (minMaxCursor < minMaxColumns.size && minMaxColumns[minMaxCursor] == i) {
                        minValues[i] = rs.getObject(colIdx++)
                        maxValues[i] = rs.getObject(colIdx++)
                        minMaxCursor++
                    }
                }
            }
        }

        val result = ArrayList<DucklakeFileColumnStats>(columns.size)
        for (i in columns.indices) {
            val col: DucklakeColumnHandle = columns[i]
            val valueCount: Long = valueCounts[i]
            val nullCount = maxOf(0L, totalCount - valueCount)
            val min: Optional<String> = DuckDbWriterSupport.formatStatValue(col.columnType, minValues[i])
            val max: Optional<String> = DuckDbWriterSupport.formatStatValue(col.columnType, maxValues[i])
            result.add(DucklakeFileColumnStats(col.columnId, 0L, valueCount, nullCount, min.orElse(null), max.orElse(null), false))
        }
        return result
    }

    @Throws(IOException::class)
    private fun uploadToRemote() {
        if (isLance) {
            uploadDirectoryToRemote()
            return
        }
        val outputFile = fileSystem.newOutputFile(remoteLocation)
        outputFile.create().use { out ->
            Files.newInputStream(localTempFile).use { input ->
                input.transferTo(out)
            }
        }
    }

    /**
     * Uploads every file in the locally-written Lance dataset *directory* to the remote dataset
     * location, preserving the relative tree. Lance reads (`__lance_scan('<dir>')`) need the whole
     * tree — manifest, data, and index files.
     */
    @Throws(IOException::class)
    private fun uploadDirectoryToRemote() {
        Files.walk(localTempFile).use { paths ->
            val iter = paths.iterator()
            while (iter.hasNext()) {
                val path: Path = iter.next()
                if (!Files.isRegularFile(path)) {
                    continue
                }
                val relative: String = localTempFile.relativize(path).toString().replace('\\', '/')
                val target: Location = remoteLocation.appendPath(relative)
                fileSystem.newOutputFile(target).create().use { out ->
                    Files.newInputStream(path).use { input ->
                        input.transferTo(out)
                    }
                }
            }
        }
    }

    /** Sum of regular-file sizes under a Lance dataset directory. */
    @Throws(IOException::class)
    private fun localDirectorySize(dir: Path): Long {
        var total = 0L
        Files.walk(dir).use { paths ->
            val iter = paths.iterator()
            while (iter.hasNext()) {
                val path: Path = iter.next()
                if (Files.isRegularFile(path)) {
                    total += Files.size(path)
                }
            }
        }
        return total
    }

    private fun cleanupAfterFailure() {
        try {
            consumerThread.interrupt()
        }
        catch (ignored: Throwable) {
            // best-effort
        }
        // Wait for the consumer to actually exit before freeing Arrow resources:
        // the INSERT...SELECT reads from arrayStream/reader using buffers owned by
        // allocator, so closing those while the consumer is still running can
        // crash the JVM. Mirrors the happy-path join at finishAndBuildFragment.
        try {
            consumerThread.join(5_000)
        }
        catch (ignored: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        releaseEngineResources()
        cleanupLocalFile()
    }

    /** Closes the Arrow C-stream, reader, allocator, and DuckDB connection (best-effort). */
    private fun releaseEngineResources() {
        closeQuietly(arrayStream)
        closeQuietly(reader)
        closeQuietly(allocator)
        closeQuietly(connection)
    }

    private fun cleanupLocalFile() {
        try {
            if (isLance) {
                deleteRecursively(localTempFile)
            }
            else {
                Files.deleteIfExists(localTempFile)
            }
        }
        catch (e: IOException) {
            log.warn(e, "Failed to delete Arrow-stream writer temp file: %s", localTempFile)
        }
    }

    /** Recursively deletes a local directory tree (children before parents). */
    @Throws(IOException::class)
    private fun deleteRecursively(root: Path) {
        if (!Files.exists(root)) {
            return
        }
        Files.walk(root).use { paths ->
            // reverseOrder puts deeper paths (longer, prefixed by their parent) before their
            // parents, so children are removed first.
            val iter = paths.sorted(Comparator.reverseOrder()).iterator()
            while (iter.hasNext()) {
                Files.deleteIfExists(iter.next())
            }
        }
    }

    override fun abort() {
        if (closed) {
            return
        }
        closed = true
        // Wake the consumer so the worker thread exits.
        pageQueue.offer(END_OF_STREAM)
        try {
            consumerThread.join(5_000)
        }
        catch (ignored: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        cleanupAfterFailure()
        try {
            if (isLance) {
                fileSystem.deleteDirectory(remoteLocation)
            }
            else {
                fileSystem.deleteFile(remoteLocation)
            }
        }
        catch (ignored: IOException) {
            // file/dir may not have been uploaded yet
        }
    }

    /**
     * [ArrowReader] subclass that pulls one Page off the queue per
     * `loadNextBatch()` call and populates the schema root with its
     * column data. DuckDB drives this via the C-data interface.
     */
    private inner class QueueArrowReader(allocator: BufferAllocator, private val schema: Schema)
            : ArrowReader(allocator) {

        @Throws(IOException::class)
        override fun loadNextBatch(): Boolean {
            val page: Page
            try {
                while (true) {
                    val polled = pageQueue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS)
                    if (polled != null) {
                        page = polled
                        break
                    }
                    // Loop back if Trino-side hasn't given us a page yet.
                }
            }
            catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IOException("Interrupted while waiting for next page", e)
            }
            if (page === END_OF_STREAM) {
                return false
            }
            try {
                populateRoot(page)
            }
            catch (e: IOException) {
                throw e
            }
            return true
        }

        override fun bytesRead(): Long = 0L

        override fun getDictionaryVectors(): Map<Long, Dictionary> = emptyMap()

        @Throws(IOException::class)
        override fun closeReadSource() {
            // Nothing to do; the queue is owned by the writer.
        }

        @Throws(IOException::class)
        override fun readSchema(): Schema = schema
    }

    @Throws(IOException::class)
    private fun populateRoot(page: Page) {
        val root: VectorSchemaRoot = reader.getVectorSchemaRoot()
        val rowCount: Int = page.positionCount
        val vectors: List<FieldVector> = root.fieldVectors
        for (channel in columnTypes.indices) {
            val vector: FieldVector = vectors[channel]
            // Do NOT call vector.reset() here. reset() zeros the existing buffer's
            // memory in place. The previous batch's exported Arrow C-data still
            // holds pointers to those buffers via Apache Arrow Java's retain/release
            // refcount machinery — DuckDB hasn't necessarily called release on the
            // prior batch by the time the next loadNextBatch runs (parallel scan,
            // pipelined consumption). Zeroing in place corrupts the prior batch's
            // data DuckDB is still reading. allocateNew() inside populateVector
            // does its own clear() which DECREMENTS refs (without zeroing the
            // memory the now-detached buffer points to) and then allocates fresh
            // buffers — so the old buffers stay readable for DuckDB until DuckDB
            // releases. Covered by
            // TestDucklakeDuckDbArrowStreamWriter.testArrowStreamPreservesAllDistinctValuesFromConnectorSource.
            populateVector(vector, columnTypes[channel], page.getBlock(channel), rowCount)
            vector.valueCount = rowCount
        }
        root.setRowCount(rowCount)
    }

    companion object {
        private val log: Logger = Logger.get(DuckDbArrowStreamFileWriter::class.java)

        private const val ATTACHED_DB: String = "ducklake_out"
        private const val ATTACHED_SCHEMA: String = "main"
        private const val ATTACHED_TABLE: String = "t"
        private const val INSERT_JOIN_TIMEOUT_SECONDS: Long = 600

        /** Sentinel to wake the consumer thread when no more pages will arrive.  */
        private val END_OF_STREAM: Page = Page(0)
        private fun toArrowField(name: String, type: Type, nullable: Boolean): Field {
            if (type is ArrayType) {
                // ARRAY(scalar) rides as Arrow List<element>; DuckDB stores it as LIST and the
                // lance COPY writer materializes uniform-length float lists as FixedSizeList —
                // exactly the embedding shape lance_vector_search consumes (verified live: the
                // probe fixtures were written from DuckDB FLOAT[] lists and read back FLOAT[N]).
                // Elements are always nullable in the Arrow schema, but populateListVector
                // rejects NULL elements (no embedding use case; see there). Nested element
                // types stay fail-fast here, like every other unsupported type.
                if (type.elementType !is ArrayType && isSupportedListElement(type.elementType)) {
                    return Field(name, FieldType(nullable, ArrowType.List.INSTANCE, null),
                            listOf(toArrowField("item", type.elementType, true)))
                }
                throw TrinoException(NOT_SUPPORTED,
                        "DuckDB Arrow-stream writer does not yet support array element type: ${type.elementType}")
            }
            if (type is RowType) {
                // ROW rides as Arrow Struct (DuckDB STRUCT); children positional, names from the
                // Trino row fields (anonymous fields get fN — DuckDB only needs valid entry names,
                // matching is positional through the arrow-stream INSERT).
                val children: List<Field> = type.fields.mapIndexed { idx, f ->
                    toArrowField(f.name.orElse("f$idx"), f.type, true)
                }
                return Field(name, FieldType(nullable, ArrowType.Struct.INSTANCE, null), children)
            }
            if (type is MapType) {
                // MAP rides as Arrow Map: a list of non-null key/value entry structs. Keys are
                // non-null by Trino contract; keysSorted=false (DuckDB does not require it).
                val entries = Field("entries", FieldType(false, ArrowType.Struct.INSTANCE, null), listOf(
                        toArrowField(MapVector.KEY_NAME, type.keyType, false),
                        toArrowField(MapVector.VALUE_NAME, type.valueType, true)))
                return Field(name, FieldType(nullable, ArrowType.Map(false), null), listOf(entries))
            }
            val arrow: ArrowType = toArrowType(type)
            return Field(name, FieldType(nullable, arrow, null), null)
        }

        private fun isSupportedListElement(elementType: Type): Boolean =
            elementType == BOOLEAN || elementType == TINYINT || elementType == SMALLINT
                    || elementType == INTEGER || elementType == BIGINT
                    || elementType == REAL || elementType == DOUBLE
                    || elementType is VarcharType

        private fun toArrowType(type: Type): ArrowType {
            if (type == BOOLEAN) {
                return ArrowType.Bool.INSTANCE
            }
            if (type == TINYINT) {
                return ArrowType.Int(8, true)
            }
            if (type == SMALLINT) {
                return ArrowType.Int(16, true)
            }
            if (type == INTEGER) {
                return ArrowType.Int(32, true)
            }
            if (type == BIGINT) {
                return ArrowType.Int(64, true)
            }
            if (type == REAL) {
                return ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
            }
            if (type == DOUBLE) {
                return ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
            }
            if (type == DATE) {
                return ArrowType.Date(DateUnit.DAY)
            }
            if (type == VARBINARY) {
                return ArrowType.Binary()
            }
            if (type is DecimalType) {
                return ArrowType.Decimal(type.precision, type.scale, 128)
            }
            if (type is TimestampType) {
                val unit: TimeUnit = when (type.precision) {
                    0 -> TimeUnit.SECOND
                    3 -> TimeUnit.MILLISECOND
                    6 -> TimeUnit.MICROSECOND
                    9 -> TimeUnit.NANOSECOND
                    else -> TimeUnit.MICROSECOND
                }
                return ArrowType.Timestamp(unit, null)
            }
            if (type is TimestampWithTimeZoneType) {
                // Trino TIMESTAMPTZ short-precision is millisecond-packed; long uses LongTimestampWithTimeZone (millis + picosOfMilli)
                val unit: TimeUnit = if (type.isShort) TimeUnit.MILLISECOND else TimeUnit.NANOSECOND
                return ArrowType.Timestamp(unit, "UTC")
            }
            if (type is VarcharType) {
                return ArrowType.Utf8()
            }
            if (type == UuidType.UUID) {
                // DuckDB's Arrow integration represents UUID columns as Utf8 (the printed
                // hex form: 'aabbccdd-eeff-0011-2233-445566778899'), not FixedSizeBinary(16)
                // as the spec might suggest. DuckDB casts the string back to UUID on its
                // side. Empirically verified — using FixedSizeBinary(16) here results in
                // the consumer getting a VarCharVector anyway, which is the type-system
                // shape of DuckDB's Arrow exchange for UUID.
                return ArrowType.Utf8()
            }
            throw TrinoException(NOT_SUPPORTED, "DuckDB Arrow-stream writer does not yet support type: $type")
        }
        private fun closeQuietly(c: AutoCloseable?) {
            c ?: return
            try {
                c.close()
            }
            catch (ignored: Throwable) {
                // best-effort
            }
        }

        private fun populateVector(vector: FieldVector, type: Type, block: Block, rowCount: Int) {
            if (type == BOOLEAN) {
                val v = vector as BitVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, if (BOOLEAN.getBoolean(block, i)) 1 else 0)
                    }
                }
            }
            else if (type == TINYINT) {
                val v = vector as TinyIntVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, TINYINT.getByte(block, i).toInt())
                    }
                }
            }
            else if (type == SMALLINT) {
                val v = vector as SmallIntVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, SMALLINT.getShort(block, i).toInt())
                    }
                }
            }
            else if (type == INTEGER) {
                val v = vector as IntVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, INTEGER.getInt(block, i))
                    }
                }
            }
            else if (type == BIGINT) {
                val v = vector as BigIntVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, BIGINT.getLong(block, i))
                    }
                }
            }
            else if (type == REAL) {
                val v = vector as Float4Vector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, intBitsToFloat(REAL.getInt(block, i)))
                    }
                }
            }
            else if (type == DOUBLE) {
                val v = vector as Float8Vector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, DOUBLE.getDouble(block, i))
                    }
                }
            }
            else if (type == DATE) {
                val v = vector as DateDayVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, DATE.getInt(block, i))
                    }
                }
            }
            else if (type == VARBINARY) {
                val v = vector as VarBinaryVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, VARBINARY.getSlice(block, i).bytes)
                    }
                }
            }
            else if (type is DecimalType) {
                val v = vector as DecimalVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                        continue
                    }
                    val value: BigDecimal
                    if (type.isShort) {
                        value = BigDecimal.valueOf(type.getLong(block, i), type.scale)
                    }
                    else {
                        val unscaled: Int128 = type.getObject(block, i) as Int128
                        value = BigDecimal(BigInteger(unscaled.toBigEndianBytes()), type.scale)
                    }
                    v.setSafe(i, value)
                }
            }
            else if (type is TimestampType) {
                populateTimestampVector(vector, type, block, rowCount)
            }
            else if (type is TimestampWithTimeZoneType) {
                populateTimestampTzVector(vector, type, block, rowCount)
            }
            else if (type is VarcharType) {
                val v = vector as VarCharVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        val bytes: ByteArray = type.getSlice(block, i).toStringUtf8().toByteArray(StandardCharsets.UTF_8)
                        v.setSafe(i, bytes)
                    }
                }
            }
            else if (type == UuidType.UUID) {
                // See toArrowType: UUID rides as Utf8 over the Arrow exchange.
                val v = vector as VarCharVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        val uuid: UUID = UuidType.trinoUuidToJavaUuid(UuidType.UUID.getSlice(block, i))
                        v.setSafe(i, uuid.toString().toByteArray(StandardCharsets.UTF_8))
                    }
                }
            }
            else if (type is ArrayType) {
                DuckDbComplexVectorWriter.populateListVector(vector as ListVector, type, block, rowCount)
            }
            else if (type is RowType) {
                DuckDbComplexVectorWriter.populateStructVector(vector as StructVector, type, block, rowCount)
            }
            else if (type is MapType) {
                DuckDbComplexVectorWriter.populateMapVector(vector as MapVector, type, block, rowCount)
            }
            else {
                throw TrinoException(NOT_SUPPORTED, "DuckDB Arrow-stream writer does not yet support type: $type")
            }
        }

        private fun populateTimestampVector(vector: FieldVector, type: TimestampType, block: Block, rowCount: Int) {
            if (type.precision == 0) {
                val v = vector as TimeStampSecVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                        continue
                    }
                    val micros: Long = type.getLong(block, i)
                    v.setSafe(i, floorDiv(micros, 1_000_000L))
                }
            }
            else if (type.precision == 3) {
                val v = vector as TimeStampMilliVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                        continue
                    }
                    val micros: Long = type.getLong(block, i)
                    v.setSafe(i, floorDiv(micros, 1_000L))
                }
            }
            else if (type.precision == 6) {
                val v = vector as TimeStampMicroVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                        continue
                    }
                    v.setSafe(i, type.getLong(block, i))
                }
            }
            else if (type.precision == 9) {
                val v = vector as TimeStampNanoVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                        continue
                    }
                    if (type.isShort) {
                        val micros: Long = type.getLong(block, i)
                        v.setSafe(i, Math.multiplyExact(micros, 1_000L))
                    }
                    else {
                        val ts: LongTimestamp = type.getObject(block, i) as LongTimestamp
                        val nanos: Long = Math.multiplyExact(ts.epochMicros, 1_000L) +
                                ts.picosOfMicro / 1_000
                        v.setSafe(i, nanos)
                    }
                }
            }
            else {
                throw TrinoException(NOT_SUPPORTED,
                        "Unsupported TIMESTAMP precision in Arrow-stream writer: ${type.precision}")
            }
        }

        private fun populateTimestampTzVector(vector: FieldVector, type: TimestampWithTimeZoneType, block: Block, rowCount: Int) {
            if (type.isShort) {
                val v = vector as TimeStampMilliTZVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                        continue
                    }
                    val packed: Long = type.getLong(block, i)
                    v.setSafe(i, unpackMillisUtc(packed))
                }
                return
            }
            val v = vector as TimeStampNanoTZVector
            v.allocateNew(rowCount)
            for (i in 0 until rowCount) {
                if (block.isNull(i)) {
                    v.setNull(i)
                    continue
                }
                val ts: LongTimestampWithTimeZone = type.getObject(block, i) as LongTimestampWithTimeZone
                val nanos: Long = Math.multiplyExact(ts.epochMillis, 1_000_000L) +
                        ts.picosOfMilli / 1_000
                v.setSafe(i, nanos)
            }
        }

        @Suppress("unused")
        private fun nanosFloor(instant: Instant): Instant {
            return instant
        }

        @Suppress("unused")
        private fun microsToLocalDateTime(epochMicros: Long): LocalDateTime {
            val epochSecond: Long = floorDiv(epochMicros, 1_000_000L)
            val nanoOfSecond: Int = (floorMod(epochMicros, 1_000_000L)).toInt() * 1_000
            return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, ZoneOffset.UTC)
        }
    }
}
