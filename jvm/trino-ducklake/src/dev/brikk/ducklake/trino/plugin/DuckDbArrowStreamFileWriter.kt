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
import io.airlift.log.Logger
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.spi.Page
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.Block
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
import io.trino.spi.type.RealType.REAL
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
import java.lang.String.format
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.ArrayList
import java.util.HashMap
import java.util.Locale
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
        fileSystem: TrinoFileSystem,
        remoteLocation: Location,
        relativePath: String,
        partitionValues: Map<Int, String?>,
        partitionId: OptionalLong,
        columns: List<DucklakeColumnHandle>,
        localTempDir: Path) : DucklakeFileWriter {
    private val fileSystem: TrinoFileSystem = fileSystem
    private val remoteLocation: Location = remoteLocation
    private val relativePath: String = relativePath
    private val partitionValues: Map<Int, String?> = HashMap(partitionValues)
    private val partitionId: OptionalLong = partitionId
    private val columns: List<DucklakeColumnHandle> = columns.toList()
    private val columnTypes: List<Type> = this.columns.stream().map { it.columnType }.toList()
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
        Files.createDirectories(localTempDir)
        this.localTempFile = localTempDir.resolve("ducklake-write-" + UUID.randomUUID() + ".db")
        this.streamName = "trino_in_" + UUID.randomUUID().toString().replace('-', '_')

        var conn: DuckDBConnection? = null
        var alloc: BufferAllocator? = null
        var rdr: QueueArrowReader? = null
        var stream: ArrowArrayStream? = null
        try {
            conn = DriverManager.getConnection("jdbc:duckdb:") as DuckDBConnection
            conn.createStatement().use { stmt ->
                stmt.execute(format(
                        "ATTACH '%s' AS %s (READ_WRITE)",
                        localTempFile.toAbsolutePath().toString().replace("'", "''"),
                        ATTACHED_DB))
                stmt.execute(buildCreateTableSql())
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
            throw IOException("Failed to initialize Arrow-stream DuckDB writer for " + remoteLocation, e)
        }
        this.connection = conn!!
        this.allocator = alloc!!
        this.reader = rdr!!
        this.arrayStream = stream!!

        this.consumerThread = Thread(this::runInsert, "ducklake-arrow-insert-" + streamName)
        this.consumerThread.isDaemon = true
        this.consumerThread.start()
    }

    private fun runInsert() {
        val sql: String = format(
                "INSERT INTO %s.%s.%s SELECT * FROM %s",
                ATTACHED_DB, ATTACHED_SCHEMA, ATTACHED_TABLE, streamName)
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
            val col: DucklakeColumnHandle = columns.get(i)
            sql.append('"').append(col.columnName.replace("\"", "\"\"")).append('"')
            sql.append(' ').append(toDuckDbSqlType(col.columnType))
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
        if (page.getPositionCount() == 0) {
            return
        }
        val err: Throwable? = consumerError.get()
        if (err != null) {
            throw IOException("Arrow-stream INSERT failed", err)
        }
        try {
            // Block on a bounded queue so we don't outrun the consumer.
            pageQueue.put(page)
        }
        catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw IOException("Interrupted while feeding page to Arrow-stream writer", e)
        }
        rowCount += page.getPositionCount()
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
            throw IOException("Arrow-stream DuckDB writer already closed: " + remoteLocation)
        }
        closed = true

        // Signal end-of-stream and wait for the consumer's INSERT to drain.
        try {
            pageQueue.put(END_OF_STREAM)
            if (!consumerThread.join(java.time.Duration.ofSeconds(INSERT_JOIN_TIMEOUT_SECONDS))) {
                cleanupAfterFailure()
                throw IOException("Arrow-stream INSERT did not finish within "
                        + INSERT_JOIN_TIMEOUT_SECONDS + "s for " + remoteLocation)
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
            throw IOException("Arrow-stream INSERT failed for " + remoteLocation, err)
        }

        // Stats: query the just-written table while still attached. Same shape as
        // the Appender path's extractColumnStats.
        val columnStats: List<DucklakeFileColumnStats>
        try {
            columnStats = extractColumnStats()
            connection.createStatement().use { stmt ->
                stmt.execute("DETACH " + ATTACHED_DB)
            }
        }
        catch (e: SQLException) {
            cleanupAfterFailure()
            throw IOException("Failed to finalize Arrow-stream DuckDB file " + remoteLocation, e)
        }

        // Release Arrow + DuckDB resources before uploading.
        closeQuietly(arrayStream)
        closeQuietly(reader)
        closeQuietly(allocator)
        closeQuietly(connection)

        val fileSize: Long
        try {
            fileSize = Files.size(localTempFile)
            uploadToRemote()
        }
        catch (e: IOException) {
            cleanupLocalFile()
            throw IOException("Failed to upload Arrow-stream DuckDB file to " + remoteLocation, e)
        }
        cleanupLocalFile()

        return DucklakeWriteFragment(
                relativePath,
                FORMAT_DUCKDB,
                fileSize,
                0L,
                rowCount,
                columnStats,
                partitionValues,
                partitionId)
    }

    @Throws(SQLException::class)
    private fun extractColumnStats(): List<DucklakeFileColumnStats> {
        if (columns.isEmpty()) {
            return listOf()
        }
        val sql = StringBuilder("SELECT COUNT(*)")
        val minMaxColumns: MutableList<Int> = ArrayList()
        for (i in columns.indices) {
            val col: DucklakeColumnHandle = columns.get(i)
            val name = '"' + col.columnName.replace("\"", "\"\"") + '"'
            sql.append(", COUNT(").append(name).append(")")
            if (!(col.columnType.equals(VARBINARY) || col.columnType.equals(UuidType.UUID))) {
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
                    throw SQLException("Stats query returned no rows for " + remoteLocation)
                }
                var colIdx = 1
                totalCount = rs.getLong(colIdx++)
                var minMaxCursor = 0
                for (i in columns.indices) {
                    valueCounts[i] = rs.getLong(colIdx++)
                    if (minMaxCursor < minMaxColumns.size && minMaxColumns.get(minMaxCursor) == i) {
                        minValues[i] = rs.getObject(colIdx++)
                        maxValues[i] = rs.getObject(colIdx++)
                        minMaxCursor++
                    }
                }
            }
        }

        val result: MutableList<DucklakeFileColumnStats> = ArrayList(columns.size)
        for (i in columns.indices) {
            val col: DucklakeColumnHandle = columns.get(i)
            val valueCount: Long = valueCounts[i]
            val nullCount: Long = Math.max(0, totalCount - valueCount)
            val min: Optional<String> = formatStatValue(col.columnType, minValues[i])
            val max: Optional<String> = formatStatValue(col.columnType, maxValues[i])
            result.add(DucklakeFileColumnStats(col.columnId, 0L, valueCount, nullCount, min, max, false))
        }
        return result
    }

    @Throws(IOException::class)
    private fun uploadToRemote() {
        val outputFile = fileSystem.newOutputFile(remoteLocation)
        outputFile.create().use { out ->
            Files.newInputStream(localTempFile).use { input ->
                input.transferTo(out)
            }
        }
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
        closeQuietly(arrayStream)
        closeQuietly(reader)
        closeQuietly(allocator)
        closeQuietly(connection)
        cleanupLocalFile()
    }

    private fun cleanupLocalFile() {
        try {
            Files.deleteIfExists(localTempFile)
        }
        catch (e: IOException) {
            log.warn(e, "Failed to delete Arrow-stream writer temp file: %s", localTempFile)
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
            fileSystem.deleteFile(remoteLocation)
        }
        catch (ignored: IOException) {
            // file may not have been uploaded yet
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

        override fun bytesRead(): Long {
            return 0L
        }

        override fun getDictionaryVectors(): Map<Long, Dictionary> {
            return mapOf()
        }

        @Throws(IOException::class)
        override fun closeReadSource() {
            // Nothing to do; the queue is owned by the writer.
        }

        @Throws(IOException::class)
        override fun readSchema(): Schema {
            return schema
        }
    }

    @Throws(IOException::class)
    private fun populateRoot(page: Page) {
        val root: VectorSchemaRoot = reader.getVectorSchemaRoot()
        val rowCount: Int = page.getPositionCount()
        val vectors: List<FieldVector> = root.getFieldVectors()
        for (channel in 0 until columnTypes.size) {
            val vector: FieldVector = vectors.get(channel)
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
            populateVector(vector, columnTypes.get(channel), page.getBlock(channel), rowCount)
            vector.setValueCount(rowCount)
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

        private fun toDuckDbSqlType(type: Type): String {
            if (type.equals(BOOLEAN)) {
                return "BOOLEAN"
            }
            if (type.equals(TINYINT)) {
                return "TINYINT"
            }
            if (type.equals(SMALLINT)) {
                return "SMALLINT"
            }
            if (type.equals(INTEGER)) {
                return "INTEGER"
            }
            if (type.equals(BIGINT)) {
                return "BIGINT"
            }
            if (type.equals(REAL)) {
                return "REAL"
            }
            if (type.equals(DOUBLE)) {
                return "DOUBLE"
            }
            if (type.equals(DATE)) {
                return "DATE"
            }
            if (type.equals(VARBINARY)) {
                return "BLOB"
            }
            if (type is DecimalType) {
                return format(Locale.ROOT, "DECIMAL(%d,%d)", type.getPrecision(), type.getScale())
            }
            if (type is TimestampType) {
                return when (type.getPrecision()) {
                    0 -> "TIMESTAMP_S"
                    3 -> "TIMESTAMP_MS"
                    6 -> "TIMESTAMP"
                    9 -> "TIMESTAMP_NS"
                    else -> "TIMESTAMP"
                }
            }
            if (type is TimestampWithTimeZoneType) {
                return "TIMESTAMPTZ"
            }
            if (type is VarcharType) {
                return "VARCHAR"
            }
            if (type.equals(UuidType.UUID)) {
                return "UUID"
            }
            throw TrinoException(NOT_SUPPORTED, "DuckDB Arrow-stream writer does not yet support type: " + type)
        }

        private fun toArrowField(name: String, type: Type, nullable: Boolean): Field {
            val arrow: ArrowType = toArrowType(type)
            return Field(name, FieldType(nullable, arrow, null), null)
        }

        private fun toArrowType(type: Type): ArrowType {
            if (type.equals(BOOLEAN)) {
                return ArrowType.Bool.INSTANCE
            }
            if (type.equals(TINYINT)) {
                return ArrowType.Int(8, true)
            }
            if (type.equals(SMALLINT)) {
                return ArrowType.Int(16, true)
            }
            if (type.equals(INTEGER)) {
                return ArrowType.Int(32, true)
            }
            if (type.equals(BIGINT)) {
                return ArrowType.Int(64, true)
            }
            if (type.equals(REAL)) {
                return ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
            }
            if (type.equals(DOUBLE)) {
                return ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
            }
            if (type.equals(DATE)) {
                return ArrowType.Date(DateUnit.DAY)
            }
            if (type.equals(VARBINARY)) {
                return ArrowType.Binary()
            }
            if (type is DecimalType) {
                return ArrowType.Decimal(type.getPrecision(), type.getScale(), 128)
            }
            if (type is TimestampType) {
                val unit: TimeUnit = when (type.getPrecision()) {
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
                val unit: TimeUnit = if (type.isShort()) TimeUnit.MILLISECOND else TimeUnit.NANOSECOND
                return ArrowType.Timestamp(unit, "UTC")
            }
            if (type is VarcharType) {
                return ArrowType.Utf8()
            }
            if (type.equals(UuidType.UUID)) {
                // DuckDB's Arrow integration represents UUID columns as Utf8 (the printed
                // hex form: 'aabbccdd-eeff-0011-2233-445566778899'), not FixedSizeBinary(16)
                // as the spec might suggest. DuckDB casts the string back to UUID on its
                // side. Empirically verified — using FixedSizeBinary(16) here results in
                // the consumer getting a VarCharVector anyway, which is the type-system
                // shape of DuckDB's Arrow exchange for UUID.
                return ArrowType.Utf8()
            }
            throw TrinoException(NOT_SUPPORTED, "DuckDB Arrow-stream writer does not yet support type: " + type)
        }

        private fun formatStatValue(type: Type, value: Any?): Optional<String> {
            if (value == null) {
                return Optional.empty()
            }
            if (type.equals(BOOLEAN)) {
                return Optional.of(if ((value as Boolean)) "true" else "false")
            }
            if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
                return Optional.of((value as Number).toLong().toString() + "")
            }
            if (type.equals(REAL)) {
                val f: Float = (value as Number).toFloat()
                return if (java.lang.Float.isNaN(f)) Optional.empty() else Optional.of(f.toString())
            }
            if (type.equals(DOUBLE)) {
                val d: Double = (value as Number).toDouble()
                return if (java.lang.Double.isNaN(d)) Optional.empty() else Optional.of(d.toString())
            }
            if (type is DecimalType) {
                val bd: BigDecimal = if (value is BigDecimal) value else BigDecimal(value.toString())
                return Optional.of(bd.toPlainString())
            }
            if (type.equals(DATE)) {
                if (value is java.sql.Date) {
                    return Optional.of(value.toLocalDate().toString())
                }
                if (value is LocalDate) {
                    return Optional.of(value.toString())
                }
                return Optional.of(value.toString())
            }
            if (type is TimestampType) {
                if (value is LocalDateTime) {
                    return Optional.of(value.toString())
                }
                if (value is java.sql.Timestamp) {
                    return Optional.of(value.toLocalDateTime().toString())
                }
                return Optional.of(value.toString())
            }
            if (type is TimestampWithTimeZoneType) {
                if (value is OffsetDateTime) {
                    return Optional.of(value.toInstant().toString())
                }
                if (value is java.sql.Timestamp) {
                    return Optional.of(value.toInstant().toString())
                }
                return Optional.of(value.toString())
            }
            if (type is VarcharType) {
                return Optional.of(value.toString())
            }
            return Optional.empty()
        }

        private fun closeQuietly(c: AutoCloseable?) {
            if (c == null) {
                return
            }
            try {
                c.close()
            }
            catch (ignored: Throwable) {
                // best-effort
            }
        }

        private fun populateVector(vector: FieldVector, type: Type, block: Block, rowCount: Int) {
            if (type.equals(BOOLEAN)) {
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
            else if (type.equals(TINYINT)) {
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
            else if (type.equals(SMALLINT)) {
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
            else if (type.equals(INTEGER)) {
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
            else if (type.equals(BIGINT)) {
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
            else if (type.equals(REAL)) {
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
            else if (type.equals(DOUBLE)) {
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
            else if (type.equals(DATE)) {
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
            else if (type.equals(VARBINARY)) {
                val v = vector as VarBinaryVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        v.setSafe(i, VARBINARY.getSlice(block, i).getBytes())
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
                    if (type.isShort()) {
                        value = BigDecimal.valueOf(type.getLong(block, i), type.getScale())
                    }
                    else {
                        val unscaled: Int128 = type.getObject(block, i) as Int128
                        value = BigDecimal(BigInteger(unscaled.toBigEndianBytes()), type.getScale())
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
            else if (type.equals(UuidType.UUID)) {
                // See toArrowType: UUID rides as Utf8 over the Arrow exchange.
                val v = vector as VarCharVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                    }
                    else {
                        val uuid: java.util.UUID = UuidType.trinoUuidToJavaUuid(UuidType.UUID.getSlice(block, i))
                        v.setSafe(i, uuid.toString().toByteArray(StandardCharsets.UTF_8))
                    }
                }
            }
            else {
                throw TrinoException(NOT_SUPPORTED, "DuckDB Arrow-stream writer does not yet support type: " + type)
            }
        }

        private fun populateTimestampVector(vector: FieldVector, type: TimestampType, block: Block, rowCount: Int) {
            if (type.getPrecision() == 0) {
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
            else if (type.getPrecision() == 3) {
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
            else if (type.getPrecision() == 6) {
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
            else if (type.getPrecision() == 9) {
                val v = vector as TimeStampNanoVector
                v.allocateNew(rowCount)
                for (i in 0 until rowCount) {
                    if (block.isNull(i)) {
                        v.setNull(i)
                        continue
                    }
                    if (type.isShort()) {
                        val micros: Long = type.getLong(block, i)
                        v.setSafe(i, Math.multiplyExact(micros, 1_000L))
                    }
                    else {
                        val ts: LongTimestamp = type.getObject(block, i) as LongTimestamp
                        val nanos: Long = Math.multiplyExact(ts.getEpochMicros(), 1_000L) +
                                ts.getPicosOfMicro() / 1_000
                        v.setSafe(i, nanos)
                    }
                }
            }
            else {
                throw TrinoException(NOT_SUPPORTED,
                        "Unsupported TIMESTAMP precision in Arrow-stream writer: " + type.getPrecision())
            }
        }

        private fun populateTimestampTzVector(vector: FieldVector, type: TimestampWithTimeZoneType, block: Block, rowCount: Int) {
            if (type.isShort()) {
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
                val nanos: Long = Math.multiplyExact(ts.getEpochMillis(), 1_000_000L) +
                        ts.getPicosOfMilli() / 1_000
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
