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

import com.google.common.collect.ImmutableList;
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
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

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
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Alternative {@link DuckDbFileWriter} implementation that streams Trino Pages to
 * DuckDB columnarly via Apache Arrow.
 *
 * <p>Architecture: Trino's page-sink thread feeds {@link Page}s into a small
 * blocking queue; a worker thread drives {@code INSERT INTO target SELECT * FROM
 * <registered_arrow_stream>} against DuckDB; the registered stream's reader pulls
 * from the same queue, converting one Page → {@link VectorSchemaRoot} per
 * {@code loadNextBatch()}. Memory is bounded to a small constant number of Pages
 * regardless of total CTAS size, and the per-cell JNI cost of the JDBC
 * {@code Appender} is replaced by the Arrow C-data interface (one batch
 * round-trip per Page, no per-cell crossing).
 *
 * <p>Selected per-CREATE / per-INSERT via the {@code duckdb_writer_mode} session
 * property — see {@link DucklakeSessionProperties#WRITER_MODE_ARROW_STREAM}. The
 * existing Appender path stays available as the default fallback while we
 * compare.
 */
final class DuckDbArrowStreamFileWriter
        implements DucklakeFileWriter
{
    private static final Logger log = Logger.get(DuckDbArrowStreamFileWriter.class);

    private static final String ATTACHED_DB = "ducklake_out";
    private static final String ATTACHED_SCHEMA = "main";
    private static final String ATTACHED_TABLE = "t";
    private static final long INSERT_JOIN_TIMEOUT_SECONDS = 600;

    /** Sentinel to wake the consumer thread when no more pages will arrive. */
    private static final Page END_OF_STREAM = new Page(0);

    private final TrinoFileSystem fileSystem;
    private final Location remoteLocation;
    private final String relativePath;
    private final Map<Integer, String> partitionValues;
    private final OptionalLong partitionId;
    private final List<DucklakeColumnHandle> columns;
    private final List<Type> columnTypes;
    private final Path localTempFile;

    private final DuckDBConnection connection;
    private final BufferAllocator allocator;
    private final QueueArrowReader reader;
    private final ArrowArrayStream arrayStream;
    private final String streamName;

    private final BlockingQueue<Page> pageQueue = new ArrayBlockingQueue<>(2);
    private final AtomicReference<Throwable> consumerError = new AtomicReference<>();
    private final Thread consumerThread;

    private long rowCount;
    private long writtenBytes;
    private boolean closed;

    DuckDbArrowStreamFileWriter(
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
        this.localTempFile = localTempDir.resolve("ducklake-write-" + UUID.randomUUID() + ".db");
        this.streamName = "trino_in_" + UUID.randomUUID().toString().replace('-', '_');

        DuckDBConnection conn = null;
        BufferAllocator alloc = null;
        QueueArrowReader rdr = null;
        ArrowArrayStream stream = null;
        try {
            conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(format(
                        "ATTACH '%s' AS %s (READ_WRITE)",
                        localTempFile.toAbsolutePath().toString().replace("'", "''"),
                        ATTACHED_DB));
                stmt.execute(buildCreateTableSql());
            }

            alloc = new RootAllocator();
            rdr = new QueueArrowReader(alloc, buildArrowSchema());
            stream = ArrowArrayStream.allocateNew(alloc);
            Data.exportArrayStream(alloc, rdr, stream);
            conn.registerArrowStream(streamName, stream);
        }
        catch (SQLException e) {
            closeQuietly(stream);
            closeQuietly(rdr);
            closeQuietly(alloc);
            closeQuietly(conn);
            try {
                Files.deleteIfExists(localTempFile);
            }
            catch (IOException ignored) {
                // best-effort
            }
            throw new IOException("Failed to initialize Arrow-stream DuckDB writer for " + remoteLocation, e);
        }
        this.connection = conn;
        this.allocator = alloc;
        this.reader = rdr;
        this.arrayStream = stream;

        this.consumerThread = new Thread(this::runInsert, "ducklake-arrow-insert-" + streamName);
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
    }

    private void runInsert()
    {
        String sql = format(
                "INSERT INTO %s.%s.%s SELECT * FROM %s",
                ATTACHED_DB, ATTACHED_SCHEMA, ATTACHED_TABLE, streamName);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
        catch (Throwable t) {
            consumerError.set(t);
            // Drain the queue to unblock any producer waiting on put().
            pageQueue.clear();
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
        if (type.equals(UuidType.UUID)) {
            return "UUID";
        }
        throw new TrinoException(NOT_SUPPORTED, "DuckDB Arrow-stream writer does not yet support type: " + type);
    }

    private Schema buildArrowSchema()
    {
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (DucklakeColumnHandle col : columns) {
            fields.add(toArrowField(col.columnName(), col.columnType(), col.nullable()));
        }
        return new Schema(fields.build());
    }

    private static Field toArrowField(String name, Type type, boolean nullable)
    {
        ArrowType arrow = toArrowType(type);
        return new Field(name, new FieldType(nullable, arrow, null), null);
    }

    private static ArrowType toArrowType(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return ArrowType.Bool.INSTANCE;
        }
        if (type.equals(TINYINT)) {
            return new ArrowType.Int(8, true);
        }
        if (type.equals(SMALLINT)) {
            return new ArrowType.Int(16, true);
        }
        if (type.equals(INTEGER)) {
            return new ArrowType.Int(32, true);
        }
        if (type.equals(BIGINT)) {
            return new ArrowType.Int(64, true);
        }
        if (type.equals(REAL)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        if (type.equals(DOUBLE)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if (type.equals(DATE)) {
            return new ArrowType.Date(DateUnit.DAY);
        }
        if (type.equals(VARBINARY)) {
            return new ArrowType.Binary();
        }
        if (type instanceof DecimalType decimalType) {
            return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128);
        }
        if (type instanceof TimestampType timestampType) {
            TimeUnit unit = switch (timestampType.getPrecision()) {
                case 0 -> TimeUnit.SECOND;
                case 3 -> TimeUnit.MILLISECOND;
                case 6 -> TimeUnit.MICROSECOND;
                case 9 -> TimeUnit.NANOSECOND;
                default -> TimeUnit.MICROSECOND;
            };
            return new ArrowType.Timestamp(unit, null);
        }
        if (type instanceof TimestampWithTimeZoneType tzType) {
            // Trino TIMESTAMPTZ short-precision is millisecond-packed; long uses LongTimestampWithTimeZone (millis + picosOfMilli)
            TimeUnit unit = tzType.isShort() ? TimeUnit.MILLISECOND : TimeUnit.NANOSECOND;
            return new ArrowType.Timestamp(unit, "UTC");
        }
        if (type instanceof VarcharType) {
            return new ArrowType.Utf8();
        }
        if (type.equals(UuidType.UUID)) {
            // DuckDB's Arrow integration represents UUID columns as Utf8 (the printed
            // hex form: 'aabbccdd-eeff-0011-2233-445566778899'), not FixedSizeBinary(16)
            // as the spec might suggest. DuckDB casts the string back to UUID on its
            // side. Empirically verified — using FixedSizeBinary(16) here results in
            // the consumer getting a VarCharVector anyway, which is the type-system
            // shape of DuckDB's Arrow exchange for UUID.
            return new ArrowType.Utf8();
        }
        throw new TrinoException(NOT_SUPPORTED, "DuckDB Arrow-stream writer does not yet support type: " + type);
    }

    @Override
    public void write(Page page)
            throws IOException
    {
        if (page.getPositionCount() == 0) {
            return;
        }
        Throwable err = consumerError.get();
        if (err != null) {
            throw new IOException("Arrow-stream INSERT failed", err);
        }
        try {
            // Block on a bounded queue so we don't outrun the consumer.
            pageQueue.put(page);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while feeding page to Arrow-stream writer", e);
        }
        rowCount += page.getPositionCount();
        writtenBytes += page.getSizeInBytes();
    }

    @Override
    public long getApproximateWrittenBytes()
    {
        // Logical input bytes appended (uncompressed). DuckDB's on-disk file is
        // smaller after columnar compression — typically 3-5×. The page sink uses
        // this to decide rollover at ducklake.duckdb.target-write-bytes.
        return writtenBytes;
    }

    @Override
    public DucklakeWriteFragment finishAndBuildFragment()
            throws IOException
    {
        if (closed) {
            throw new IOException("Arrow-stream DuckDB writer already closed: " + remoteLocation);
        }
        closed = true;

        // Signal end-of-stream and wait for the consumer's INSERT to drain.
        try {
            pageQueue.put(END_OF_STREAM);
            if (!consumerThread.join(java.time.Duration.ofSeconds(INSERT_JOIN_TIMEOUT_SECONDS))) {
                cleanupAfterFailure();
                throw new IOException("Arrow-stream INSERT did not finish within "
                        + INSERT_JOIN_TIMEOUT_SECONDS + "s for " + remoteLocation);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cleanupAfterFailure();
            throw new IOException("Interrupted waiting for Arrow-stream INSERT to complete", e);
        }
        Throwable err = consumerError.get();
        if (err != null) {
            cleanupAfterFailure();
            throw new IOException("Arrow-stream INSERT failed for " + remoteLocation, err);
        }

        // Stats: query the just-written table while still attached. Same shape as
        // the Appender path's extractColumnStats.
        List<DucklakeFileColumnStats> columnStats;
        try {
            columnStats = extractColumnStats();
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DETACH " + ATTACHED_DB);
            }
        }
        catch (SQLException e) {
            cleanupAfterFailure();
            throw new IOException("Failed to finalize Arrow-stream DuckDB file " + remoteLocation, e);
        }

        // Release Arrow + DuckDB resources before uploading.
        closeQuietly(arrayStream);
        closeQuietly(reader);
        closeQuietly(allocator);
        closeQuietly(connection);

        long fileSize;
        try {
            fileSize = Files.size(localTempFile);
            uploadToRemote();
        }
        catch (IOException e) {
            cleanupLocalFile();
            throw new IOException("Failed to upload Arrow-stream DuckDB file to " + remoteLocation, e);
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

    private List<DucklakeFileColumnStats> extractColumnStats()
            throws SQLException
    {
        if (columns.isEmpty()) {
            return List.of();
        }
        StringBuilder sql = new StringBuilder("SELECT COUNT(*)");
        List<Integer> minMaxColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            DucklakeColumnHandle col = columns.get(i);
            String name = '"' + col.columnName().replace("\"", "\"\"") + '"';
            sql.append(", COUNT(").append(name).append(")");
            if (!(col.columnType().equals(VARBINARY) || col.columnType().equals(UuidType.UUID))) {
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

        List<DucklakeFileColumnStats> result = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            DucklakeColumnHandle col = columns.get(i);
            long valueCount = valueCounts[i];
            long nullCount = Math.max(0, totalCount - valueCount);
            Optional<String> min = formatStatValue(col.columnType(), minValues[i]);
            Optional<String> max = formatStatValue(col.columnType(), maxValues[i]);
            result.add(new DucklakeFileColumnStats(col.columnId(), 0L, valueCount, nullCount, min, max, false));
        }
        return result;
    }

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

    private void cleanupAfterFailure()
    {
        try {
            consumerThread.interrupt();
        }
        catch (Throwable ignored) {
            // best-effort
        }
        // Wait for the consumer to actually exit before freeing Arrow resources:
        // the INSERT...SELECT reads from arrayStream/reader using buffers owned by
        // allocator, so closing those while the consumer is still running can
        // crash the JVM. Mirrors the happy-path join at finishAndBuildFragment.
        try {
            consumerThread.join(5_000);
        }
        catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        closeQuietly(arrayStream);
        closeQuietly(reader);
        closeQuietly(allocator);
        closeQuietly(connection);
        cleanupLocalFile();
    }

    private void cleanupLocalFile()
    {
        try {
            Files.deleteIfExists(localTempFile);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete Arrow-stream writer temp file: %s", localTempFile);
        }
    }

    @Override
    public void abort()
    {
        if (closed) {
            return;
        }
        closed = true;
        // Wake the consumer so the worker thread exits.
        pageQueue.offer(END_OF_STREAM);
        try {
            consumerThread.join(5_000);
        }
        catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        cleanupAfterFailure();
        try {
            fileSystem.deleteFile(remoteLocation);
        }
        catch (IOException ignored) {
            // file may not have been uploaded yet
        }
    }

    private static void closeQuietly(AutoCloseable c)
    {
        if (c == null) {
            return;
        }
        try {
            c.close();
        }
        catch (Throwable ignored) {
            // best-effort
        }
    }

    /**
     * {@link ArrowReader} subclass that pulls one Page off the queue per
     * {@code loadNextBatch()} call and populates the schema root with its
     * column data. DuckDB drives this via the C-data interface.
     */
    private final class QueueArrowReader
            extends ArrowReader
    {
        private final Schema schema;

        QueueArrowReader(BufferAllocator allocator, Schema schema)
        {
            super(allocator);
            this.schema = schema;
        }

        @Override
        public boolean loadNextBatch()
                throws IOException
        {
            Page page;
            try {
                while (true) {
                    page = pageQueue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (page != null) {
                        break;
                    }
                    // Loop back if Trino-side hasn't given us a page yet.
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for next page", e);
            }
            if (page == END_OF_STREAM) {
                return false;
            }
            try {
                populateRoot(page);
            }
            catch (IOException e) {
                throw e;
            }
            return true;
        }

        @Override
        public long bytesRead()
        {
            return 0L;
        }

        @Override
        public Map<Long, Dictionary> getDictionaryVectors()
        {
            return Map.of();
        }

        @Override
        protected void closeReadSource()
                throws IOException
        {
            // Nothing to do; the queue is owned by the writer.
        }

        @Override
        protected Schema readSchema()
                throws IOException
        {
            return schema;
        }
    }

    private void populateRoot(Page page)
            throws IOException
    {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        int rowCount = page.getPositionCount();
        List<FieldVector> vectors = root.getFieldVectors();
        for (int channel = 0; channel < columnTypes.size(); channel++) {
            FieldVector vector = vectors.get(channel);
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
            populateVector(vector, columnTypes.get(channel), page.getBlock(channel), rowCount);
            vector.setValueCount(rowCount);
        }
        root.setRowCount(rowCount);
    }

    private static void populateVector(FieldVector vector, Type type, Block block, int rowCount)
    {
        if (type.equals(BOOLEAN)) {
            BitVector v = (BitVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, BOOLEAN.getBoolean(block, i) ? 1 : 0);
                }
            }
        }
        else if (type.equals(TINYINT)) {
            TinyIntVector v = (TinyIntVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, TINYINT.getByte(block, i));
                }
            }
        }
        else if (type.equals(SMALLINT)) {
            SmallIntVector v = (SmallIntVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, SMALLINT.getShort(block, i));
                }
            }
        }
        else if (type.equals(INTEGER)) {
            IntVector v = (IntVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, INTEGER.getInt(block, i));
                }
            }
        }
        else if (type.equals(BIGINT)) {
            BigIntVector v = (BigIntVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, BIGINT.getLong(block, i));
                }
            }
        }
        else if (type.equals(REAL)) {
            Float4Vector v = (Float4Vector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, intBitsToFloat(REAL.getInt(block, i)));
                }
            }
        }
        else if (type.equals(DOUBLE)) {
            Float8Vector v = (Float8Vector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, DOUBLE.getDouble(block, i));
                }
            }
        }
        else if (type.equals(DATE)) {
            DateDayVector v = (DateDayVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, DATE.getInt(block, i));
                }
            }
        }
        else if (type.equals(VARBINARY)) {
            VarBinaryVector v = (VarBinaryVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    v.setSafe(i, VARBINARY.getSlice(block, i).getBytes());
                }
            }
        }
        else if (type instanceof DecimalType decimalType) {
            DecimalVector v = (DecimalVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                    continue;
                }
                BigDecimal value;
                if (decimalType.isShort()) {
                    value = BigDecimal.valueOf(decimalType.getLong(block, i), decimalType.getScale());
                }
                else {
                    Int128 unscaled = (Int128) decimalType.getObject(block, i);
                    value = new BigDecimal(new BigInteger(unscaled.toBigEndianBytes()), decimalType.getScale());
                }
                v.setSafe(i, value);
            }
        }
        else if (type instanceof TimestampType timestampType) {
            populateTimestampVector(vector, timestampType, block, rowCount);
        }
        else if (type instanceof TimestampWithTimeZoneType tzType) {
            populateTimestampTzVector(vector, tzType, block, rowCount);
        }
        else if (type instanceof VarcharType varcharType) {
            VarCharVector v = (VarCharVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    byte[] bytes = varcharType.getSlice(block, i).toStringUtf8().getBytes(StandardCharsets.UTF_8);
                    v.setSafe(i, bytes);
                }
            }
        }
        else if (type.equals(UuidType.UUID)) {
            // See toArrowType: UUID rides as Utf8 over the Arrow exchange.
            VarCharVector v = (VarCharVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                }
                else {
                    java.util.UUID uuid = UuidType.trinoUuidToJavaUuid(UuidType.UUID.getSlice(block, i));
                    v.setSafe(i, uuid.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "DuckDB Arrow-stream writer does not yet support type: " + type);
        }
    }

    private static void populateTimestampVector(FieldVector vector, TimestampType type, Block block, int rowCount)
    {
        if (type.getPrecision() == 0) {
            TimeStampSecVector v = (TimeStampSecVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                    continue;
                }
                long micros = type.getLong(block, i);
                v.setSafe(i, floorDiv(micros, 1_000_000L));
            }
        }
        else if (type.getPrecision() == 3) {
            TimeStampMilliVector v = (TimeStampMilliVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                    continue;
                }
                long micros = type.getLong(block, i);
                v.setSafe(i, floorDiv(micros, 1_000L));
            }
        }
        else if (type.getPrecision() == 6) {
            TimeStampMicroVector v = (TimeStampMicroVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                    continue;
                }
                v.setSafe(i, type.getLong(block, i));
            }
        }
        else if (type.getPrecision() == 9) {
            TimeStampNanoVector v = (TimeStampNanoVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                    continue;
                }
                if (type.isShort()) {
                    long micros = type.getLong(block, i);
                    v.setSafe(i, Math.multiplyExact(micros, 1_000L));
                }
                else {
                    LongTimestamp ts = (LongTimestamp) type.getObject(block, i);
                    long nanos = Math.multiplyExact(ts.getEpochMicros(), 1_000L)
                            + ts.getPicosOfMicro() / 1_000;
                    v.setSafe(i, nanos);
                }
            }
        }
        else {
            throw new TrinoException(NOT_SUPPORTED,
                    "Unsupported TIMESTAMP precision in Arrow-stream writer: " + type.getPrecision());
        }
    }

    private static void populateTimestampTzVector(FieldVector vector, TimestampWithTimeZoneType type, Block block, int rowCount)
    {
        if (type.isShort()) {
            TimeStampMilliTZVector v = (TimeStampMilliTZVector) vector;
            v.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (block.isNull(i)) {
                    v.setNull(i);
                    continue;
                }
                long packed = type.getLong(block, i);
                v.setSafe(i, unpackMillisUtc(packed));
            }
            return;
        }
        TimeStampNanoTZVector v = (TimeStampNanoTZVector) vector;
        v.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            if (block.isNull(i)) {
                v.setNull(i);
                continue;
            }
            LongTimestampWithTimeZone ts = (LongTimestampWithTimeZone) type.getObject(block, i);
            long nanos = Math.multiplyExact(ts.getEpochMillis(), 1_000_000L)
                    + ts.getPicosOfMilli() / 1_000;
            v.setSafe(i, nanos);
        }
    }

    @SuppressWarnings("unused")
    private static Instant nanosFloor(Instant instant)
    {
        return instant;
    }

    @SuppressWarnings("unused")
    private static LocalDateTime microsToLocalDateTime(long epochMicros)
    {
        long epochSecond = floorDiv(epochMicros, 1_000_000L);
        int nanoOfSecond = (int) floorMod(epochMicros, 1_000_000L) * 1_000;
        return LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, ZoneOffset.UTC);
    }
}
