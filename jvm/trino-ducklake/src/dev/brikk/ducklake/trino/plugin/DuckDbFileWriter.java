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
package dev.brikk.ducklake.trino.plugin;

import dev.brikk.ducklake.catalog.DucklakeFileColumnStats;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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
final class DuckDbFileWriter
        implements DucklakeFileWriter
{
    private static final Logger log = Logger.get(DuckDbFileWriter.class);

    private static final String ATTACHED_DB = "ducklake_out";
    private static final String ATTACHED_SCHEMA = "main";
    private static final String ATTACHED_TABLE = "t";

    private final TrinoFileSystem fileSystem;
    private final Location remoteLocation;
    private final String relativePath;
    private final Map<Integer, String> partitionValues;
    private final OptionalLong partitionId;
    private final List<DucklakeColumnHandle> columns;
    private final List<Type> columnTypes;
    private final Path localTempFile;
    private final DuckDBConnection connection;
    private final DuckDBAppender appender;

    private long rowCount;
    private long writtenBytes;
    private boolean closed;

    DuckDbFileWriter(
            TrinoFileSystem fileSystem,
            Location remoteLocation,
            String relativePath,
            Map<Integer, String> partitionValues,
            OptionalLong partitionId,
            List<DucklakeColumnHandle> columns,
            Path localTempDir)
            throws IOException
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.remoteLocation = requireNonNull(remoteLocation, "remoteLocation is null");
        this.relativePath = requireNonNull(relativePath, "relativePath is null");
        this.partitionValues = new HashMap<>(requireNonNull(partitionValues, "partitionValues is null"));
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.columnTypes = this.columns.stream().map(DucklakeColumnHandle::columnType).toList();

        Files.createDirectories(localTempDir);
        this.localTempFile = localTempDir.resolve("ducklake-write-" + java.util.UUID.randomUUID() + ".db");

        try {
            this.connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(format(
                        "ATTACH '%s' AS %s (READ_WRITE)",
                        localTempFile.toAbsolutePath().toString().replace("'", "''"),
                        ATTACHED_DB));
                stmt.execute(buildCreateTableSql());
            }
            this.appender = connection.createAppender(ATTACHED_DB, ATTACHED_SCHEMA, ATTACHED_TABLE);
        }
        catch (SQLException e) {
            try {
                Files.deleteIfExists(localTempFile);
            }
            catch (IOException ignored) {
                // best-effort cleanup
            }
            throw new IOException("Failed to initialize DuckDB writer for " + remoteLocation, e);
        }
    }

    private String buildCreateTableSql()
    {
        StringBuilder sql = new StringBuilder("CREATE TABLE ");
        sql.append(ATTACHED_DB).append('.').append(ATTACHED_SCHEMA).append('.').append(ATTACHED_TABLE);
        sql.append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            DucklakeColumnHandle col = columns.get(i);
            sql.append('"').append(col.columnName().replace("\"", "\"\"")).append('"');
            sql.append(' ').append(toDuckDbSqlType(col.columnType()));
            if (!col.nullable()) {
                sql.append(" NOT NULL");
            }
        }
        sql.append(')');
        return sql.toString();
    }

    private static String toDuckDbSqlType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return "BOOLEAN";
        }
        if (type.equals(TINYINT)) {
            return "TINYINT";
        }
        if (type.equals(SMALLINT)) {
            return "SMALLINT";
        }
        if (type.equals(INTEGER)) {
            return "INTEGER";
        }
        if (type.equals(BIGINT)) {
            return "BIGINT";
        }
        if (type.equals(REAL)) {
            return "REAL";
        }
        if (type.equals(DOUBLE)) {
            return "DOUBLE";
        }
        if (type.equals(DATE)) {
            return "DATE";
        }
        if (type.equals(VARBINARY)) {
            return "BLOB";
        }
        if (type.equals(UUID)) {
            return "UUID";
        }
        if (type instanceof DecimalType decimalType) {
            return format(Locale.ROOT, "DECIMAL(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof TimestampType timestampType) {
            return switch (timestampType.getPrecision()) {
                case 0 -> "TIMESTAMP_S";
                case 3 -> "TIMESTAMP_MS";
                case 6 -> "TIMESTAMP";
                case 9 -> "TIMESTAMP_NS";
                default -> "TIMESTAMP";
            };
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return "TIMESTAMPTZ";
        }
        if (type instanceof VarcharType) {
            return "VARCHAR";
        }
        throw new TrinoException(NOT_SUPPORTED, "DuckDB-format writer does not yet support type: " + type);
    }

    @Override
    public void write(Page page)
            throws IOException
    {
        int positionCount = page.getPositionCount();
        try {
            for (int position = 0; position < positionCount; position++) {
                appender.beginRow();
                for (int channel = 0; channel < columnTypes.size(); channel++) {
                    appendValue(page.getBlock(channel), position, columnTypes.get(channel));
                }
                appender.endRow();
            }
            // Bound memory: without this the appender buffers every row of the
            // CTAS until close(), which OOMs on large loads. Flushing per-Page is
            // cheap (Pages are ~1024–65536 rows) and lets DuckDB persist as we go.
            appender.flush();
            rowCount += positionCount;
            writtenBytes += page.getSizeInBytes();
        }
        catch (SQLException e) {
            throw new IOException("Failed to append row to DuckDB file " + remoteLocation, e);
        }
    }

    private void appendValue(Block block, int position, Type type)
            throws SQLException
    {
        if (block.isNull(position)) {
            appender.appendNull();
            return;
        }
        if (type.equals(BOOLEAN)) {
            appender.append(BOOLEAN.getBoolean(block, position));
            return;
        }
        if (type.equals(TINYINT)) {
            appender.append((byte) TINYINT.getByte(block, position));
            return;
        }
        if (type.equals(SMALLINT)) {
            appender.append((short) SMALLINT.getShort(block, position));
            return;
        }
        if (type.equals(INTEGER)) {
            appender.append(INTEGER.getInt(block, position));
            return;
        }
        if (type.equals(BIGINT)) {
            appender.append(BIGINT.getLong(block, position));
            return;
        }
        if (type.equals(REAL)) {
            appender.append(intBitsToFloat(REAL.getInt(block, position)));
            return;
        }
        if (type.equals(DOUBLE)) {
            appender.append(DOUBLE.getDouble(block, position));
            return;
        }
        if (type.equals(DATE)) {
            // Trino DATE is days since 1970-01-01
            int days = DATE.getInt(block, position);
            appender.append(LocalDate.ofEpochDay(days));
            return;
        }
        if (type.equals(VARBINARY)) {
            byte[] bytes = VARBINARY.getSlice(block, position).getBytes();
            appender.append(bytes);
            return;
        }
        if (type.equals(UUID)) {
            appender.append(trinoUuidToJavaUuid(UUID.getSlice(block, position)));
            return;
        }
        if (type instanceof DecimalType decimalType) {
            BigDecimal value;
            if (decimalType.isShort()) {
                long unscaled = decimalType.getLong(block, position);
                value = BigDecimal.valueOf(unscaled, decimalType.getScale());
            }
            else {
                Int128 unscaled = (Int128) decimalType.getObject(block, position);
                value = new BigDecimal(new BigInteger(unscaled.toBigEndianBytes()), decimalType.getScale());
            }
            appender.append(value);
            return;
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                long micros = timestampType.getLong(block, position);
                appender.append(microsToLocalDateTime(micros));
                return;
            }
            LongTimestamp ts = (LongTimestamp) timestampType.getObject(block, position);
            appender.append(microsToLocalDateTime(ts.getEpochMicros())
                    .plusNanos(ts.getPicosOfMicro() / 1_000));
            return;
        }
        if (type instanceof TimestampWithTimeZoneType tzType) {
            long epochMillis;
            if (tzType.isShort()) {
                long packed = tzType.getLong(block, position);
                epochMillis = unpackMillisUtc(packed);
            }
            else {
                LongTimestampWithTimeZone ts = (LongTimestampWithTimeZone) tzType.getObject(block, position);
                epochMillis = ts.getEpochMillis();
            }
            appender.append(OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC));
            return;
        }
        if (type instanceof VarcharType varcharType) {
            appender.append(varcharType.getSlice(block, position).toStringUtf8());
            return;
        }
        throw new TrinoException(NOT_SUPPORTED, "DuckDB-format writer does not yet support type: " + type);
    }

    private static LocalDateTime microsToLocalDateTime(long epochMicros)
    {
        long epochSecond = floorDiv(epochMicros, 1_000_000L);
        int nanoOfSecond = (int) floorMod(epochMicros, 1_000_000L) * 1_000;
        return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, ZoneOffset.UTC);
    }

    @Override
    public long getApproximateWrittenBytes()
    {
        return writtenBytes;
    }

    @Override
    public DucklakeWriteFragment finishAndBuildFragment()
            throws IOException
    {
        if (closed) {
            throw new IOException("DuckDB file writer already closed: " + remoteLocation);
        }
        closed = true;

        List<DucklakeFileColumnStats> columnStats;
        try {
            appender.close();
            // Stats must be queried while the database is still attached (after appender
            // close so all rows are flushed, before DETACH).
            columnStats = extractColumnStats();
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DETACH " + ATTACHED_DB);
            }
            connection.close();
        }
        catch (SQLException e) {
            cleanupLocalFile();
            throw new IOException("Failed to finalize DuckDB file " + remoteLocation, e);
        }

        long fileSize;
        try {
            fileSize = Files.size(localTempFile);
            uploadToRemote();
        }
        catch (IOException e) {
            cleanupLocalFile();
            throw new IOException("Failed to upload DuckDB file to " + remoteLocation, e);
        }
        cleanupLocalFile();

        return new DucklakeWriteFragment(
                relativePath,
                FORMAT_DUCKDB,
                fileSize,
                0L,
                rowCount,
                columnStats,
                partitionValues,
                partitionId);
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
    private List<DucklakeFileColumnStats> extractColumnStats()
            throws SQLException
    {
        if (columns.isEmpty()) {
            return List.of();
        }

        StringBuilder sql = new StringBuilder("SELECT COUNT(*)");
        List<Integer> minMaxColumns = new java.util.ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            DucklakeColumnHandle col = columns.get(i);
            String name = '"' + col.columnName().replace("\"", "\"\"") + '"';
            // Always emit a per-column COUNT(col) so we can derive null_count.
            sql.append(", COUNT(").append(name).append(")");
            if (supportsMinMax(col.columnType())) {
                sql.append(", MIN(").append(name).append("), MAX(").append(name).append(")");
                minMaxColumns.add(i);
            }
        }
        sql.append(" FROM ").append(ATTACHED_DB).append('.').append(ATTACHED_SCHEMA).append('.').append(ATTACHED_TABLE);

        long totalCount;
        long[] valueCounts = new long[columns.size()];
        Object[] minValues = new Object[columns.size()];
        Object[] maxValues = new Object[columns.size()];

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql.toString())) {
            if (!rs.next()) {
                throw new SQLException("Stats query returned no rows for " + remoteLocation);
            }
            int colIdx = 1;
            totalCount = rs.getLong(colIdx++);
            int minMaxCursor = 0;
            for (int i = 0; i < columns.size(); i++) {
                valueCounts[i] = rs.getLong(colIdx++);
                if (minMaxCursor < minMaxColumns.size() && minMaxColumns.get(minMaxCursor) == i) {
                    minValues[i] = rs.getObject(colIdx++);
                    maxValues[i] = rs.getObject(colIdx++);
                    minMaxCursor++;
                }
            }
        }

        List<DucklakeFileColumnStats> result = new java.util.ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            DucklakeColumnHandle col = columns.get(i);
            long valueCount = valueCounts[i];
            long nullCount = Math.max(0, totalCount - valueCount);
            Optional<String> min = formatStatValue(col.columnType(), minValues[i]);
            Optional<String> max = formatStatValue(col.columnType(), maxValues[i]);
            result.add(new DucklakeFileColumnStats(
                    col.columnId(),
                    0L, // column_size_bytes — not readily available without per-block scan; safe at 0
                    valueCount,
                    nullCount,
                    min,
                    max,
                    false));
        }
        return result;
    }

    private static boolean supportsMinMax(Type type)
    {
        if (type.equals(VARBINARY) || type.equals(UUID)) {
            return false;
        }
        // Nested types are already rejected at write time.
        return true;
    }

    /**
     * Format a JDBC-returned stat value into the DuckLake-string form the parquet
     * writer uses, so the catalog stays consistent across formats. See
     * {@link DucklakeStatsExtractor#convertStatValue}.
     */
    private static Optional<String> formatStatValue(Type type, Object value)
    {
        if (value == null) {
            return Optional.empty();
        }
        if (type.equals(BOOLEAN)) {
            return Optional.of(((Boolean) value) ? "true" : "false");
        }
        if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            return Optional.of(((Number) value).longValue() + "");
        }
        if (type.equals(REAL)) {
            float f = ((Number) value).floatValue();
            return Float.isNaN(f) ? Optional.empty() : Optional.of(String.valueOf(f));
        }
        if (type.equals(DOUBLE)) {
            double d = ((Number) value).doubleValue();
            return Double.isNaN(d) ? Optional.empty() : Optional.of(String.valueOf(d));
        }
        if (type instanceof DecimalType) {
            BigDecimal bd = (value instanceof BigDecimal b) ? b : new BigDecimal(value.toString());
            return Optional.of(bd.toPlainString());
        }
        if (type.equals(DATE)) {
            if (value instanceof java.sql.Date d) {
                return Optional.of(d.toLocalDate().toString());
            }
            if (value instanceof LocalDate ld) {
                return Optional.of(ld.toString());
            }
            return Optional.of(value.toString());
        }
        if (type instanceof TimestampType) {
            if (value instanceof LocalDateTime ldt) {
                return Optional.of(ldt.toString());
            }
            if (value instanceof java.sql.Timestamp ts) {
                return Optional.of(ts.toLocalDateTime().toString());
            }
            return Optional.of(value.toString());
        }
        if (type instanceof TimestampWithTimeZoneType) {
            if (value instanceof OffsetDateTime odt) {
                return Optional.of(odt.toInstant().toString());
            }
            if (value instanceof java.sql.Timestamp ts) {
                return Optional.of(ts.toInstant().toString());
            }
            return Optional.of(value.toString());
        }
        if (type instanceof VarcharType) {
            return Optional.of(value.toString());
        }
        return Optional.empty();
    }

    private void uploadToRemote()
            throws IOException
    {
        TrinoOutputFile outputFile = fileSystem.newOutputFile(remoteLocation);
        try (OutputStream out = outputFile.create();
                InputStream in = Files.newInputStream(localTempFile)) {
            in.transferTo(out);
        }
    }

    private void cleanupLocalFile()
    {
        try {
            Files.deleteIfExists(localTempFile);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete DuckDB writer temp file: %s", localTempFile);
        }
    }

    @Override
    public void abort()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            appender.close();
        }
        catch (SQLException ignored) {
            // best-effort
        }
        try {
            connection.close();
        }
        catch (SQLException ignored) {
            // best-effort
        }
        cleanupLocalFile();
        try {
            fileSystem.deleteFile(remoteLocation);
        }
        catch (IOException ignored) {
            // file may not have been uploaded yet
        }
    }
}
