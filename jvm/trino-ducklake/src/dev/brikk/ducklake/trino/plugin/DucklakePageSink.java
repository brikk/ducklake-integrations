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

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.Page;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.Type;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DucklakePageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(DucklakePageSink.class);

    private final DucklakeWritableTableHandle handle;
    private final TrinoFileSystem fileSystem;
    private final JsonCodec<DucklakeWriteFragment> fragmentCodec;
    private final ParquetWriterOptions writerOptions;
    private final String trinoVersion;
    private final long targetMaxFileSize;

    private final List<Type> columnTypes;
    private final List<String> columnNames;
    private final MessageType messageType;
    private final Map<List<String>, Type> primitiveTypes;
    private final DucklakeUnsignedRangeChecker unsignedRangeChecker;

    // Partition support
    private final DucklakePagePartitioner partitioner;

    // Writers: for unpartitioned tables, only index 0 is used.
    // For partitioned tables, one writer per unique partition combination.
    private final List<WriterContext> writers = new ArrayList<>();
    private final List<DucklakeWriteFragment> completedFragments = new ArrayList<>();
    private final List<Location> writtenFilePaths = new ArrayList<>();

    public DucklakePageSink(
            DucklakeWritableTableHandle handle,
            TrinoFileSystem fileSystem,
            JsonCodec<DucklakeWriteFragment> fragmentCodec,
            ParquetWriterConfig parquetWriterConfig,
            String trinoVersion,
            PageIndexerFactory pageIndexerFactory)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.fragmentCodec = requireNonNull(fragmentCodec, "fragmentCodec is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");

        this.writerOptions = ParquetWriterOptions.builder()
                .setMaxBlockSize(parquetWriterConfig.getBlockSize())
                .setMaxPageSize(parquetWriterConfig.getPageSize())
                .setMaxPageValueCount(parquetWriterConfig.getPageValueCount())
                .setBatchSize(parquetWriterConfig.getBatchSize())
                .build();

        this.targetMaxFileSize = parquetWriterConfig.getBlockSize().toBytes();

        this.columnTypes = handle.columns().stream()
                .map(DucklakeColumnHandle::columnType)
                .collect(toImmutableList());
        this.columnNames = handle.columns().stream()
                .map(DucklakeColumnHandle::columnName)
                .collect(toImmutableList());
        this.unsignedRangeChecker = DucklakeUnsignedRangeChecker.build(
                handle.columns(), handle.allCatalogColumns());

        // Build Parquet schema with field_id annotations for DuckDB compatibility
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                columnTypes, columnNames, false, false);
        this.primitiveTypes = schemaConverter.getPrimitiveTypes();
        this.messageType = DucklakeParquetSchemaBuilder.buildMessageType(
                handle.columns(),
                handle.allCatalogColumns(),
                schemaConverter.getMessageType());

        // Set up partitioner if table is partitioned
        if (handle.partitionSpec().isPresent()) {
            this.partitioner = new DucklakePagePartitioner(
                    requireNonNull(pageIndexerFactory, "pageIndexerFactory is null"),
                    handle.partitionSpec().get(),
                    handle.columns(),
                    handle.temporalPartitionEncoding());
        }
        else {
            this.partitioner = null;
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        // Validate unsigned column ranges *before* any bytes hit the Parquet writer — once
        // the writer has consumed the page, the offending value is already encoded in a
        // row-group buffer and throwing here would leave a half-written file behind. The
        // checker is a no-op (zero per-page overhead) when the table has no unsigned
        // columns, which is the common case.
        unsignedRangeChecker.validate(page);

        try {
            if (partitioner == null) {
                appendUnpartitioned(page);
            }
            else {
                appendPartitioned(page);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write page", e);
        }

        return NOT_BLOCKED;
    }

    private void appendUnpartitioned(Page page)
            throws IOException
    {
        if (writers.isEmpty()) {
            writers.add(openNewWriter(Map.of()));
        }

        WriterContext writer = writers.getFirst();
        writer.write(page);

        if (writer.getWrittenBytes() >= targetMaxFileSize) {
            closeWriter(0);
            writers.set(0, openNewWriter(Map.of()));
        }
    }

    private void appendPartitioned(Page page)
            throws IOException
    {
        int[] writerIndexes = partitioner.partitionPage(page);
        int maxIndex = partitioner.getMaxIndex();

        // Ensure writers list is big enough
        while (writers.size() <= maxIndex) {
            writers.add(null);
        }

        // Group positions by writer index
        Map<Integer, List<Integer>> positionsByWriter = new HashMap<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            positionsByWriter.computeIfAbsent(writerIndexes[position], _ -> new ArrayList<>()).add(position);
        }

        // Write to each partition's writer
        for (Map.Entry<Integer, List<Integer>> entry : positionsByWriter.entrySet()) {
            int writerIndex = entry.getKey();
            List<Integer> positions = entry.getValue();
            int[] posArray = positions.stream().mapToInt(Integer::intValue).toArray();

            // Get or create writer for this partition
            WriterContext writer = writers.get(writerIndex);
            if (writer == null) {
                // Compute partition values from the first row in this partition
                Map<Integer, String> partitionValues = partitioner.getPartitionValues(page, posArray[0]);
                writer = openNewWriter(partitionValues);
                writers.set(writerIndex, writer);
            }

            // Extract sub-page for this partition
            Page partitionPage = page.getPositions(posArray, 0, posArray.length);
            writer.write(partitionPage);

            // Rotate if over target size
            if (writer.getWrittenBytes() >= targetMaxFileSize) {
                closeWriter(writerIndex);
                Map<Integer, String> partitionValues = partitioner.getPartitionValues(page, posArray[0]);
                writers.set(writerIndex, openNewWriter(partitionValues));
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            for (int i = 0; i < writers.size(); i++) {
                if (writers.get(i) != null) {
                    closeWriter(i);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to close writer", e);
        }

        List<Slice> fragments = completedFragments.stream()
                .map(fragment -> Slices.wrappedBuffer(fragmentCodec.toJsonBytes(fragment)))
                .collect(toImmutableList());

        return completedFuture(fragments);
    }

    @Override
    public void abort()
    {
        // Close all open writers
        for (WriterContext writer : writers) {
            if (writer != null) {
                try {
                    writer.close();
                }
                catch (IOException e) {
                    log.warn(e, "Failed to close writer during abort");
                }
            }
        }
        writers.clear();

        // Best-effort delete all written files
        for (Location path : writtenFilePaths) {
            try {
                fileSystem.deleteFile(path);
            }
            catch (IOException e) {
                log.warn(e, "Failed to delete file during abort: %s", path);
            }
        }
    }

    private WriterContext openNewWriter(Map<Integer, String> partitionValues)
            throws IOException
    {
        String fileName = "ducklake-" + UUID.randomUUID() + ".parquet";

        // Build path: for partitioned tables, create subdirectories like region=us/
        String relativePath;
        if (!partitionValues.isEmpty()) {
            StringBuilder pathBuilder = new StringBuilder();
            partitionValues.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        String colName = partitioner.getPartitionColumnName(entry.getKey());
                        String value = entry.getValue() != null ? entry.getValue() : "__HIVE_DEFAULT_PARTITION__";
                        pathBuilder.append(colName).append("=").append(value).append("/");
                    });
            pathBuilder.append(fileName);
            relativePath = pathBuilder.toString();
        }
        else {
            relativePath = fileName;
        }

        Location filePath = Location.of(handle.tableDataPath()).appendPath(relativePath);
        writtenFilePaths.add(filePath);

        TrinoOutputFile outputFile = fileSystem.newOutputFile(filePath);
        OutputStream outputStream = outputFile.create();

        ParquetWriter parquetWriter = new ParquetWriter(
                outputStream,
                messageType,
                primitiveTypes,
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                Optional.empty(),
                Optional.empty());

        OptionalLong partitionId = partitioner != null ? OptionalLong.of(partitioner.getPartitionId()) : OptionalLong.empty();
        return new WriterContext(parquetWriter, outputStream, relativePath, partitionValues, partitionId);
    }

    private void closeWriter(int index)
            throws IOException
    {
        WriterContext writer = writers.get(index);
        if (writer == null) {
            return;
        }

        writer.close();

        FileMetaData fileMetaData = writer.getFileMetaData();
        long recordCount = fileMetaData.getNum_rows();
        long fileSize = writer.getWrittenBytes();

        // Compute footer size
        long footerSize;
        try {
            io.airlift.slice.DynamicSliceOutput footerOutput = new io.airlift.slice.DynamicSliceOutput(40);
            org.apache.parquet.format.Util.writeFileMetaData(fileMetaData, footerOutput);
            footerSize = footerOutput.size();
        }
        catch (IOException e) {
            footerSize = 0;
        }

        List<DucklakeFileColumnStats> columnStats =
                DucklakeStatsExtractor.extractStats(fileMetaData, handle.columns());

        completedFragments.add(new DucklakeWriteFragment(
                writer.relativePath,
                fileSize,
                footerSize,
                recordCount,
                columnStats,
                writer.partitionValues,
                writer.partitionId));

        writers.set(index, null);
    }

    private static class WriterContext
    {
        private final ParquetWriter parquetWriter;
        private final OutputStream outputStream;
        private final String relativePath;
        private final Map<Integer, String> partitionValues;
        private final OptionalLong partitionId;

        WriterContext(
                ParquetWriter parquetWriter,
                OutputStream outputStream,
                String relativePath,
                Map<Integer, String> partitionValues,
                OptionalLong partitionId)
        {
            this.parquetWriter = requireNonNull(parquetWriter, "parquetWriter is null");
            this.outputStream = requireNonNull(outputStream, "outputStream is null");
            this.relativePath = requireNonNull(relativePath, "relativePath is null");
            this.partitionValues = new HashMap<>(requireNonNull(partitionValues, "partitionValues is null"));
            this.partitionId = requireNonNull(partitionId, "partitionId is null");
        }

        void write(Page page)
                throws IOException
        {
            parquetWriter.write(page);
        }

        long getWrittenBytes()
        {
            return parquetWriter.getWrittenBytes() + parquetWriter.getBufferedBytes();
        }

        void close()
                throws IOException
        {
            parquetWriter.close();
        }

        FileMetaData getFileMetaData()
        {
            return parquetWriter.getFileMetaData();
        }
    }
}
