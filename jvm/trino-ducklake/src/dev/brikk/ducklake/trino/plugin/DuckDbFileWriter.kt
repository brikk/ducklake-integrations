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
import io.trino.spi.type.UuidType.UUID
import io.trino.spi.type.UuidType.trinoUuidToJavaUuid
import io.trino.spi.type.VarbinaryType.VARBINARY
import io.trino.spi.type.VarcharType
import org.duckdb.DuckDBAppender
import org.duckdb.DuckDBConnection
import java.io.IOException
import java.lang.Float.intBitsToFloat
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.lang.String.format
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.sql.SQLException
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional

/**
 * Phase 1 DuckDB-format writer. Writes one DuckDB database file per
 * [DucklakeFileWriter] via the JDBC [DuckDBAppender] API:
 *
 *   - Open an in-memory DuckDB session, `ATTACH` a fresh local temp file
 *       READ_WRITE, and `CREATE TABLE` matching the Trino schema.
 *   - Per page, walk the [Block]s and feed each row through the appender
 *       (one `beginRow`/`endRow` per Trino position).
 *   - On finish: close the appender and connection, then upload the local file to
 *       the destination via [TrinoFileSystem] so the result lands wherever
 *       parquet would have. The local temp file is deleted after upload.
 *
 * Phase 1 supports primitive scalar types only. Nested types and high-precision
 * timestamps fall through to [TrinoException] with
 * [io.trino.spi.StandardErrorCode.NOT_SUPPORTED].
 */
internal class DuckDbFileWriter
@Throws(IOException::class)
constructor(
    private val fileSystem: TrinoFileSystem,
    private val remoteLocation: Location,
    private val relativePath: String,
    partitionValues: Map<Int, String?>,
    private val partitionId: Long?,
    columns: List<DucklakeColumnHandle>,
    localTempDir: Path,
) : DucklakeFileWriter {
    private val partitionValues: Map<Int, String?> = partitionValues.toMap()
    private val columns: List<DucklakeColumnHandle> = columns.toList()
    private val columnTypes: List<Type> = this.columns.map(DucklakeColumnHandle::columnType)
    private val localTempFile: Path
    private val connection: DuckDBConnection
    private val appender: DuckDBAppender

    private var rowCount: Long = 0
    private var writtenBytes: Long = 0
    private var closed: Boolean = false

    init {
        // Fail at schema time, not mid-append: toDuckDbSqlType renders complex types now (for the
        // Arrow-stream writer), but this appender writes per-cell and has no complex-value path.
        for (col in columns) {
            if (col.columnType is io.trino.spi.type.ArrayType
                    || col.columnType is io.trino.spi.type.RowType
                    || col.columnType is io.trino.spi.type.MapType) {
                throw TrinoException(NOT_SUPPORTED,
                        "DuckDB-format appender writer does not support type ${col.columnType}; "
                                + "use duckdb_writer_mode = 'arrow_stream' (the default) for complex types")
            }
        }
        Files.createDirectories(localTempDir)
        this.localTempFile = localTempDir.resolve("ducklake-write-${java.util.UUID.randomUUID()}.db")

        var conn: DuckDBConnection? = null
        var app: DuckDBAppender? = null
        try {
            conn = DriverManager.getConnection("jdbc:duckdb:") as DuckDBConnection
            conn.createStatement().use { stmt ->
                stmt.execute(format(
                        "ATTACH '%s' AS %s (READ_WRITE)",
                        localTempFile.toAbsolutePath().toString().replace("'", "''"),
                        ATTACHED_DB))
                stmt.execute(buildCreateTableSql())
            }
            app = conn.createAppender(ATTACHED_DB, ATTACHED_SCHEMA, ATTACHED_TABLE)
            this.connection = conn
            this.appender = app
        }
        catch (e: SQLException) {
            // Close in reverse construction order; swallow secondary failures so the original
            // SQLException surfaces. Without this, a failure after the connection opens would
            // leak the native DuckDB connection.
            try { app?.close() } catch (_: Exception) {}
            try { conn?.close() } catch (_: Exception) {}
            try { Files.deleteIfExists(localTempFile) } catch (_: IOException) {}
            throw IOException("Failed to initialize DuckDB writer for $remoteLocation", e)
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
            sql.append(' ').append(DuckDbWriterSupport.toDuckDbSqlType(col.columnType, "DuckDB-format writer"))
            if (!col.nullable) {
                sql.append(" NOT NULL")
            }
        }
        sql.append(')')
        return sql.toString()
    }

    @Throws(IOException::class)
    override fun write(page: Page) {
        val positionCount = page.positionCount
        try {
            for (position in 0 until positionCount) {
                appender.beginRow()
                for (channel in columnTypes.indices) {
                    appendValue(page.getBlock(channel), position, columnTypes[channel])
                }
                appender.endRow()
            }
            // Bound memory: without this the appender buffers every row of the
            // CTAS until close(), which OOMs on large loads. Flushing per-Page is
            // cheap (Pages are ~1024–65536 rows) and lets DuckDB persist as we go.
            appender.flush()
            rowCount += positionCount
            writtenBytes += page.getSizeInBytes()
        }
        catch (e: SQLException) {
            throw IOException("Failed to append row to DuckDB file $remoteLocation", e)
        }
    }

    @Throws(SQLException::class)
    private fun appendValue(block: Block, position: Int, type: Type) {
        if (block.isNull(position)) {
            appender.appendNull()
            return
        }
        when (type) {
            BOOLEAN -> {
                appender.append(BOOLEAN.getBoolean(block, position))
                return
            }
            TINYINT -> {
                appender.append(TINYINT.getByte(block, position).toByte())
                return
            }
            SMALLINT -> {
                appender.append(SMALLINT.getShort(block, position).toShort())
                return
            }
            INTEGER -> {
                appender.append(INTEGER.getInt(block, position))
                return
            }
            BIGINT -> {
                appender.append(BIGINT.getLong(block, position))
                return
            }
            REAL -> {
                appender.append(intBitsToFloat(REAL.getInt(block, position)))
                return
            }
            DOUBLE -> {
                appender.append(DOUBLE.getDouble(block, position))
                return
            }
            DATE -> {
                // Trino DATE is days since 1970-01-01
                val days = DATE.getInt(block, position)
                appender.append(LocalDate.ofEpochDay(days.toLong()))
                return
            }
            VARBINARY -> {
                val bytes: ByteArray = VARBINARY.getSlice(block, position).bytes
                appender.append(bytes)
                return
            }
            UUID -> {
                appender.append(trinoUuidToJavaUuid(UUID.getSlice(block, position)))
                return
            }
            is DecimalType -> {
                val value: BigDecimal
                if (type.isShort) {
                    val unscaled = type.getLong(block, position)
                    value = BigDecimal.valueOf(unscaled, type.scale)
                }
                else {
                    val unscaled = type.getObject(block, position) as Int128
                    value = BigDecimal(BigInteger(unscaled.toBigEndianBytes()), type.scale)
                }
                appender.append(value)
                return
            }
            is TimestampType -> {
                if (type.isShort) {
                    val micros = type.getLong(block, position)
                    appender.append(microsToLocalDateTime(micros))
                    return
                }
                val ts = type.getObject(block, position) as LongTimestamp
                appender.append(microsToLocalDateTime(ts.epochMicros)
                        .plusNanos((ts.picosOfMicro / 1_000).toLong()))
                return
            }
            is TimestampWithTimeZoneType -> {
                val instant: Instant
                if (type.isShort) {
                    val packed = type.getLong(block, position)
                    instant = Instant.ofEpochMilli(unpackMillisUtc(packed))
                }
                else {
                    // LongTimestampWithTimeZone carries epochMillis + picosOfMilli. DuckDB TIMESTAMPTZ
                    // is microsecond precision, so carry the sub-millisecond nanos (picos/1000) through
                    // instead of truncating to whole milliseconds — otherwise TIMESTAMP(6)/(9) WITH TIME
                    // ZONE silently loses its micros. Mirrors the Arrow writer's populateTimestampTzVector
                    // and the sibling TIMESTAMP long path above.
                    val ts = type.getObject(block, position) as LongTimestampWithTimeZone
                    instant = Instant.ofEpochMilli(ts.epochMillis)
                            .plusNanos((ts.picosOfMilli / 1_000).toLong())
                }
                appender.append(OffsetDateTime.ofInstant(instant, ZoneOffset.UTC))
                return
            }
            is VarcharType -> {
                appender.append(type.getSlice(block, position).toStringUtf8())
                return
            }
        }
        // JSON is a UTF-8-Slice type (io.trino.type.JsonType, not on the compile classpath); the
        // column was created as DuckDB JSON, and the Appender coerces the JSON text string into it.
        if (DucklakeJsonSupport.isJson(type)) {
            appender.append(type.getSlice(block, position).toStringUtf8())
            return
        }
        throw TrinoException(NOT_SUPPORTED, "DuckDB-format writer does not yet support type: $type")
    }

    override fun getApproximateWrittenBytes(): Long = writtenBytes

    @Throws(IOException::class)
    override fun finishAndBuildFragment(): DucklakeWriteFragment {
        if (closed) {
            throw IOException("DuckDB file writer already closed: $remoteLocation")
        }
        closed = true

        val columnStats: List<DucklakeFileColumnStats>
        try {
            appender.close()
            // Stats must be queried while the database is still attached (after appender
            // close so all rows are flushed, before DETACH).
            columnStats = extractColumnStats()
            connection.createStatement().use { stmt ->
                stmt.execute("DETACH $ATTACHED_DB")
            }
            connection.close()
        }
        catch (e: SQLException) {
            // appender or DETACH failure leaves the connection open — close it best-effort
            // so we don't leak the native DuckDB handle before propagating the original error.
            try { connection.close() } catch (_: Exception) {}
            cleanupLocalFile()
            throw IOException("Failed to finalize DuckDB file $remoteLocation", e)
        }

        val fileSize: Long
        try {
            fileSize = Files.size(localTempFile)
            uploadToRemote()
        }
        catch (e: IOException) {
            cleanupLocalFile()
            throw IOException("Failed to upload DuckDB file to $remoteLocation", e)
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

    /**
     * Run a single aggregate query against the freshly-written table to collect
     * per-column min/max/value_count, then derive null_count from the total row
     * count. The DuckLake DuckDB extension's introspection paths
     * (e.g. `duckdb_tables()`, `TransformGlobalStats`) crash on data
     * file rows whose `ducklake_column_stats` entries are missing or NULL,
     * so we always emit a row per column — even if no min/max can be computed
     * (e.g. for VARBINARY) — to keep cross-engine introspection working.
     */
    @Throws(SQLException::class)
    private fun extractColumnStats(): List<DucklakeFileColumnStats> {
        if (columns.isEmpty()) {
            return emptyList()
        }

        val sql = StringBuilder("SELECT COUNT(*)")
        val minMaxColumns: MutableList<Int> = mutableListOf()
        for (i in columns.indices) {
            val col = columns[i]
            val name = '"' + col.columnName.replace("\"", "\"\"") + '"'
            // Always emit a per-column COUNT(col) so we can derive null_count.
            sql.append(", COUNT(").append(name).append(")")
            if (supportsMinMax(col.columnType)) {
                sql.append(", MIN(").append(name).append("), MAX(").append(name).append(")")
                minMaxColumns.add(i)
            }
        }
        sql.append(" FROM ").append(ATTACHED_DB).append('.').append(ATTACHED_SCHEMA).append('.').append(ATTACHED_TABLE)

        var totalCount: Long = 0
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

        val result: MutableList<DucklakeFileColumnStats> = ArrayList(columns.size)
        for (i in columns.indices) {
            val col = columns[i]
            val valueCount = valueCounts[i]
            val nullCount = maxOf(0L, totalCount - valueCount)
            val min = DuckDbWriterSupport.formatStatValue(col.columnType, minValues[i])
            val max = DuckDbWriterSupport.formatStatValue(col.columnType, maxValues[i])
            result.add(DucklakeFileColumnStats(
                    col.columnId,
                    0L, // column_size_bytes — not readily available without per-block scan; safe at 0
                    valueCount,
                    nullCount,
                    min,
                    max,
                    false))
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

    private fun cleanupLocalFile() {
        try {
            Files.deleteIfExists(localTempFile)
        }
        catch (e: IOException) {
            log.warn(e, "Failed to delete DuckDB writer temp file: %s", localTempFile)
        }
    }

    override fun abort() {
        if (closed) {
            return
        }
        closed = true
        try {
            appender.close()
        }
        catch (ignored: SQLException) {
            // best-effort
        }
        try {
            connection.close()
        }
        catch (ignored: SQLException) {
            // best-effort
        }
        cleanupLocalFile()
        try {
            fileSystem.deleteFile(remoteLocation)
        }
        catch (ignored: IOException) {
            // file may not have been uploaded yet
        }
    }

    companion object {
        private val log: Logger = Logger.get(DuckDbFileWriter::class.java)

        private const val ATTACHED_DB: String = "ducklake_out"
        private const val ATTACHED_SCHEMA: String = "main"
        private const val ATTACHED_TABLE: String = "t"
        private fun microsToLocalDateTime(epochMicros: Long): LocalDateTime {
            val epochSecond = floorDiv(epochMicros, 1_000_000L)
            val nanoOfSecond = floorMod(epochMicros, 1_000_000L).toInt() * 1_000
            return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, ZoneOffset.UTC)
        }

        private fun supportsMinMax(type: Type): Boolean {
            // JSON is not an orderable type in DuckDB — MIN/MAX(json) errors — so skip stats
            // for it, same as VARBINARY/UUID. Nested types are already rejected at write time.
            return !(type.equals(VARBINARY) || type.equals(UUID) || DucklakeJsonSupport.isJson(type))
        }

        /**
         * Format a JDBC-returned stat value into the DuckLake-string form the parquet
         * writer uses, so the catalog stays consistent across formats. See
         * [DucklakeStatsExtractor.convertStatValue].
         */
    }
}
