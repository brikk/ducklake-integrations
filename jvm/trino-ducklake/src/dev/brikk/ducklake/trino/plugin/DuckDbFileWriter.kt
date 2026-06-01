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
import java.sql.Statement
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.HashMap
import java.util.Locale
import java.util.Objects.requireNonNull
import java.util.Optional
import java.util.OptionalLong

/**
 * Phase 1 DuckDB-format writer. Writes one DuckDB database file per
 * {@link DucklakeFileWriter} via the JDBC {@link DuckDBAppender} API:
 *
 * <ol>
 *   <li>Open an in-memory DuckDB session, {@code ATTACH} a fresh local temp file
 *       READ_WRITE, and {@code CREATE TABLE} matching the Trino schema.
 *   <li>Per page, walk the {@link Block}s and feed each row through the appender
 *       (one {@code beginRow}/{@code endRow} per Trino position).
 *   <li>On finish: close the appender and connection, then upload the local file to
 *       the destination via {@link TrinoFileSystem} so the result lands wherever
 *       parquet would have. The local temp file is deleted after upload.
 * </ol>
 *
 * Phase 1 supports primitive scalar types only. Nested types and high-precision
 * timestamps fall through to {@link TrinoException} with {@link
 * io.trino.spi.StandardErrorCode#NOT_SUPPORTED}.
 */
internal class DuckDbFileWriter
@Throws(IOException::class)
constructor(
        fileSystem: TrinoFileSystem,
        remoteLocation: Location,
        relativePath: String,
        partitionValues: Map<Int, String?>,
        partitionId: OptionalLong,
        columns: List<DucklakeColumnHandle>,
        localTempDir: Path,
) : DucklakeFileWriter {
    private val fileSystem: TrinoFileSystem = requireNonNull(fileSystem, "fileSystem is null")
    private val remoteLocation: Location = requireNonNull(remoteLocation, "remoteLocation is null")
    private val relativePath: String = requireNonNull(relativePath, "relativePath is null")
    private val partitionValues: Map<Int, String?> = HashMap(requireNonNull(partitionValues, "partitionValues is null"))
    private val partitionId: OptionalLong = requireNonNull(partitionId, "partitionId is null")
    private val columns: List<DucklakeColumnHandle> = java.util.List.copyOf(requireNonNull(columns, "columns is null"))
    private val columnTypes: List<Type> = this.columns.stream().map(DucklakeColumnHandle::columnType).toList()
    private val localTempFile: Path
    private val connection: DuckDBConnection
    private val appender: DuckDBAppender

    private var rowCount: Long = 0
    private var writtenBytes: Long = 0
    private var closed: Boolean = false

    init {
        Files.createDirectories(localTempDir)
        this.localTempFile = localTempDir.resolve("ducklake-write-" + java.util.UUID.randomUUID() + ".db")

        try {
            this.connection = DriverManager.getConnection("jdbc:duckdb:") as DuckDBConnection
            connection.createStatement().use { stmt ->
                stmt.execute(format(
                        "ATTACH '%s' AS %s (READ_WRITE)",
                        localTempFile.toAbsolutePath().toString().replace("'", "''"),
                        ATTACHED_DB))
                stmt.execute(buildCreateTableSql())
            }
            this.appender = connection.createAppender(ATTACHED_DB, ATTACHED_SCHEMA, ATTACHED_TABLE)
        }
        catch (e: SQLException) {
            try {
                Files.deleteIfExists(localTempFile)
            }
            catch (ignored: IOException) {
                // best-effort cleanup
            }
            throw IOException("Failed to initialize DuckDB writer for " + remoteLocation, e)
        }
    }

    private fun buildCreateTableSql(): String {
        val sql = StringBuilder("CREATE TABLE ")
        sql.append(ATTACHED_DB).append('.').append(ATTACHED_SCHEMA).append('.').append(ATTACHED_TABLE)
        sql.append(" (")
        for (i in 0 until columns.size) {
            if (i > 0) {
                sql.append(", ")
            }
            val col = columns.get(i)
            sql.append('"').append(col.columnName().replace("\"", "\"\"")).append('"')
            sql.append(' ').append(toDuckDbSqlType(col.columnType()))
            if (!col.nullable()) {
                sql.append(" NOT NULL")
            }
        }
        sql.append(')')
        return sql.toString()
    }

    @Throws(IOException::class)
    override fun write(page: Page) {
        val positionCount = page.getPositionCount()
        try {
            for (position in 0 until positionCount) {
                appender.beginRow()
                for (channel in 0 until columnTypes.size) {
                    appendValue(page.getBlock(channel), position, columnTypes.get(channel))
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
            throw IOException("Failed to append row to DuckDB file " + remoteLocation, e)
        }
    }

    @Throws(SQLException::class)
    private fun appendValue(block: Block, position: Int, type: Type) {
        if (block.isNull(position)) {
            appender.appendNull()
            return
        }
        if (type.equals(BOOLEAN)) {
            appender.append(BOOLEAN.getBoolean(block, position))
            return
        }
        if (type.equals(TINYINT)) {
            appender.append(TINYINT.getByte(block, position).toByte())
            return
        }
        if (type.equals(SMALLINT)) {
            appender.append(SMALLINT.getShort(block, position).toShort())
            return
        }
        if (type.equals(INTEGER)) {
            appender.append(INTEGER.getInt(block, position))
            return
        }
        if (type.equals(BIGINT)) {
            appender.append(BIGINT.getLong(block, position))
            return
        }
        if (type.equals(REAL)) {
            appender.append(intBitsToFloat(REAL.getInt(block, position)))
            return
        }
        if (type.equals(DOUBLE)) {
            appender.append(DOUBLE.getDouble(block, position))
            return
        }
        if (type.equals(DATE)) {
            // Trino DATE is days since 1970-01-01
            val days = DATE.getInt(block, position)
            appender.append(LocalDate.ofEpochDay(days.toLong()))
            return
        }
        if (type.equals(VARBINARY)) {
            val bytes: ByteArray = VARBINARY.getSlice(block, position).getBytes()
            appender.append(bytes)
            return
        }
        if (type.equals(UUID)) {
            appender.append(trinoUuidToJavaUuid(UUID.getSlice(block, position)))
            return
        }
        if (type is DecimalType) {
            val value: BigDecimal
            if (type.isShort()) {
                val unscaled = type.getLong(block, position)
                value = BigDecimal.valueOf(unscaled, type.getScale())
            }
            else {
                val unscaled = type.getObject(block, position) as Int128
                value = BigDecimal(BigInteger(unscaled.toBigEndianBytes()), type.getScale())
            }
            appender.append(value)
            return
        }
        if (type is TimestampType) {
            if (type.isShort()) {
                val micros = type.getLong(block, position)
                appender.append(microsToLocalDateTime(micros))
                return
            }
            val ts = type.getObject(block, position) as LongTimestamp
            appender.append(microsToLocalDateTime(ts.getEpochMicros())
                    .plusNanos((ts.getPicosOfMicro() / 1_000).toLong()))
            return
        }
        if (type is TimestampWithTimeZoneType) {
            val epochMillis: Long
            if (type.isShort()) {
                val packed = type.getLong(block, position)
                epochMillis = unpackMillisUtc(packed)
            }
            else {
                val ts = type.getObject(block, position) as LongTimestampWithTimeZone
                epochMillis = ts.getEpochMillis()
            }
            appender.append(OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC))
            return
        }
        if (type is VarcharType) {
            appender.append(type.getSlice(block, position).toStringUtf8())
            return
        }
        throw TrinoException(NOT_SUPPORTED, "DuckDB-format writer does not yet support type: " + type)
    }

    override fun getApproximateWrittenBytes(): Long {
        return writtenBytes
    }

    @Throws(IOException::class)
    override fun finishAndBuildFragment(): DucklakeWriteFragment {
        if (closed) {
            throw IOException("DuckDB file writer already closed: " + remoteLocation)
        }
        closed = true

        val columnStats: List<DucklakeFileColumnStats>
        try {
            appender.close()
            // Stats must be queried while the database is still attached (after appender
            // close so all rows are flushed, before DETACH).
            columnStats = extractColumnStats()
            connection.createStatement().use { stmt ->
                stmt.execute("DETACH " + ATTACHED_DB)
            }
            connection.close()
        }
        catch (e: SQLException) {
            cleanupLocalFile()
            throw IOException("Failed to finalize DuckDB file " + remoteLocation, e)
        }

        val fileSize: Long
        try {
            fileSize = Files.size(localTempFile)
            uploadToRemote()
        }
        catch (e: IOException) {
            cleanupLocalFile()
            throw IOException("Failed to upload DuckDB file to " + remoteLocation, e)
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
     * (e.g. {@code duckdb_tables()}, {@code TransformGlobalStats}) crash on data
     * file rows whose {@code ducklake_column_stats} entries are missing or NULL,
     * so we always emit a row per column — even if no min/max can be computed
     * (e.g. for VARBINARY) — to keep cross-engine introspection working.
     */
    @Throws(SQLException::class)
    private fun extractColumnStats(): List<DucklakeFileColumnStats> {
        if (columns.isEmpty()) {
            return listOf()
        }

        val sql = StringBuilder("SELECT COUNT(*)")
        val minMaxColumns: MutableList<Int> = java.util.ArrayList()
        for (i in 0 until columns.size) {
            val col = columns.get(i)
            val name = '"' + col.columnName().replace("\"", "\"\"") + '"'
            // Always emit a per-column COUNT(col) so we can derive null_count.
            sql.append(", COUNT(").append(name).append(")")
            if (supportsMinMax(col.columnType())) {
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
                    throw SQLException("Stats query returned no rows for " + remoteLocation)
                }
                var colIdx = 1
                totalCount = rs.getLong(colIdx++)
                var minMaxCursor = 0
                for (i in 0 until columns.size) {
                    valueCounts[i] = rs.getLong(colIdx++)
                    if (minMaxCursor < minMaxColumns.size && minMaxColumns.get(minMaxCursor) == i) {
                        minValues[i] = rs.getObject(colIdx++)
                        maxValues[i] = rs.getObject(colIdx++)
                        minMaxCursor++
                    }
                }
            }
        }

        val result: MutableList<DucklakeFileColumnStats> = java.util.ArrayList(columns.size)
        for (i in 0 until columns.size) {
            val col = columns.get(i)
            val valueCount = valueCounts[i]
            val nullCount = Math.max(0, totalCount - valueCount)
            val min = formatStatValue(col.columnType(), minValues[i])
            val max = formatStatValue(col.columnType(), maxValues[i])
            result.add(DucklakeFileColumnStats(
                    col.columnId(),
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
            if (type.equals(UUID)) {
                return "UUID"
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
            throw TrinoException(NOT_SUPPORTED, "DuckDB-format writer does not yet support type: " + type)
        }

        private fun microsToLocalDateTime(epochMicros: Long): LocalDateTime {
            val epochSecond = floorDiv(epochMicros, 1_000_000L)
            val nanoOfSecond = floorMod(epochMicros, 1_000_000L).toInt() * 1_000
            return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, ZoneOffset.UTC)
        }

        private fun supportsMinMax(type: Type): Boolean {
            if (type.equals(VARBINARY) || type.equals(UUID)) {
                return false
            }
            // Nested types are already rejected at write time.
            return true
        }

        /**
         * Format a JDBC-returned stat value into the DuckLake-string form the parquet
         * writer uses, so the catalog stays consistent across formats. See
         * {@link DucklakeStatsExtractor#convertStatValue}.
         */
        private fun formatStatValue(type: Type, value: Any?): Optional<String> {
            if (value == null) {
                return Optional.empty()
            }
            if (type.equals(BOOLEAN)) {
                return Optional.of(if (value as Boolean) "true" else "false")
            }
            if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
                return Optional.of((value as Number).toLong().toString() + "")
            }
            if (type.equals(REAL)) {
                val f = (value as Number).toFloat()
                return if (java.lang.Float.isNaN(f)) Optional.empty() else Optional.of(java.lang.String.valueOf(f))
            }
            if (type.equals(DOUBLE)) {
                val d = (value as Number).toDouble()
                return if (java.lang.Double.isNaN(d)) Optional.empty() else Optional.of(java.lang.String.valueOf(d))
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
    }
}
