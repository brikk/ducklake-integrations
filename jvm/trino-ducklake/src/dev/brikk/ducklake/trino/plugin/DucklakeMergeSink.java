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
import dev.brikk.ducklake.trino.plugin.DucklakeMergeTableHandle.DataFileRange;
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.MergePage;
import io.trino.spi.type.Type;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Merge sink for DuckLake DELETE/UPDATE/MERGE operations.
 * Collects deleted row IDs grouped by data file, writes Parquet delete files,
 * and delegates inserts to the standard DucklakePageSink.
 */
public class DucklakeMergeSink
        implements ConnectorMergeSink
{
    private static final Logger log = Logger.get(DucklakeMergeSink.class);

    private final DucklakeMergeTableHandle mergeHandle;
    private final TrinoFileSystem fileSystem;
    private final JsonCodec<DucklakeDeleteFragment> deleteFragmentCodec;
    private final JsonCodec<DucklakeWriteFragment> writeFragmentCodec;
    private final ParquetWriterOptions writerOptions;
    private final String trinoVersion;
    private final int dataColumnCount;

    // Insert sink for handling UPDATE inserts
    private final ConnectorPageSink insertSink;

    // Accumulated delete row IDs grouped by data file ID
    private final Map<Long, List<Long>> deletesByDataFile = new HashMap<>();

    public DucklakeMergeSink(
            DucklakeMergeTableHandle mergeHandle,
            TrinoFileSystem fileSystem,
            JsonCodec<DucklakeDeleteFragment> deleteFragmentCodec,
            JsonCodec<DucklakeWriteFragment> writeFragmentCodec,
            ParquetWriterOptions writerOptions,
            String trinoVersion,
            ConnectorPageSink insertSink)
    {
        this.mergeHandle = requireNonNull(mergeHandle, "mergeHandle is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.deleteFragmentCodec = requireNonNull(deleteFragmentCodec, "deleteFragmentCodec is null");
        this.writeFragmentCodec = requireNonNull(writeFragmentCodec, "writeFragmentCodec is null");
        this.writerOptions = requireNonNull(writerOptions, "writerOptions is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.insertSink = requireNonNull(insertSink, "insertSink is null");
        this.dataColumnCount = mergeHandle.insertHandle().columns().size();
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = MergePage.createDeleteAndInsertPages(page, dataColumnCount);

        mergePage.getDeletionsPage().ifPresent(this::processDeletes);
        mergePage.getInsertionsPage().ifPresent(insertPage -> insertSink.appendPage(insertPage));
    }

    private void processDeletes(Page deletePage)
    {
        // Delete page has data columns followed by row ID column (last column)
        int rowIdChannel = deletePage.getChannelCount() - 1;
        Block rowIdBlock = deletePage.getBlock(rowIdChannel);

        for (int position = 0; position < deletePage.getPositionCount(); position++) {
            long rowId = BIGINT.getLong(rowIdBlock, position);
            long dataFileId = resolveDataFileId(rowId);
            deletesByDataFile.computeIfAbsent(dataFileId, _ -> new ArrayList<>()).add(rowId);
        }
    }

    private long resolveDataFileId(long rowId)
    {
        for (DataFileRange range : mergeHandle.dataFileRanges()) {
            if (range.containsRowId(rowId)) {
                return range.dataFileId();
            }
        }
        throw new IllegalStateException("No data file found for row ID: " + rowId);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>();

        // Write delete Parquet files
        for (Map.Entry<Long, List<Long>> entry : deletesByDataFile.entrySet()) {
            long dataFileId = entry.getKey();
            List<Long> rowIds = entry.getValue();

            if (rowIds.isEmpty()) {
                continue;
            }

            try {
                DucklakeDeleteFragment fragment = writeDeleteFile(dataFileId, rowIds);
                fragments.add(Slices.wrappedBuffer(deleteFragmentCodec.toJsonBytes(fragment)));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to write delete file for data file " + dataFileId, e);
            }
        }

        // Collect insert fragments
        try {
            Collection<Slice> insertFragments = insertSink.finish().get();
            // Prefix insert fragments with a type marker so finishMerge can distinguish them
            for (Slice insertFragment : insertFragments) {
                // Insert fragments use the DucklakeWriteFragment codec — we wrap them with a type tag
                fragments.add(Slices.wrappedBuffer(writeFragmentCodec.toJsonBytes(
                        writeFragmentCodec.fromJson(insertFragment.getBytes()))));
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to finish insert sink", e);
        }

        return completedFuture(fragments);
    }

    private DucklakeDeleteFragment writeDeleteFile(long dataFileId, List<Long> rowIds)
            throws IOException
    {
        String fileName = "ducklake-delete-" + UUID.randomUUID() + ".parquet";
        String tableDataPath = mergeHandle.insertHandle().tableDataPath();
        Location filePath = Location.of(tableDataPath).appendPath(fileName);

        TrinoOutputFile outputFile = fileSystem.newOutputFile(filePath);
        OutputStream outputStream = outputFile.create();

        List<String> columnNames = ImmutableList.of("row_id");
        List<Type> columnTypes = ImmutableList.of(BIGINT);

        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                columnTypes, columnNames, false, false);

        ParquetWriter parquetWriter = new ParquetWriter(
                outputStream,
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                writerOptions,
                CompressionCodec.ZSTD,
                trinoVersion,
                java.util.Optional.empty(),
                java.util.Optional.empty());

        // Write all row IDs as a single page
        io.trino.spi.block.BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, rowIds.size());
        for (long rowId : rowIds) {
            BIGINT.writeLong(blockBuilder, rowId);
        }
        Block block = blockBuilder.build();
        parquetWriter.write(new Page(block.getPositionCount(), block));
        parquetWriter.close();

        FileMetaData fileMetaData = parquetWriter.getFileMetaData();
        long fileSize = parquetWriter.getWrittenBytes();

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

        log.debug("Wrote delete file %s with %d row IDs for data file %d", filePath, rowIds.size(), dataFileId);

        return new DucklakeDeleteFragment(
                dataFileId,
                fileName,
                rowIds.size(),
                fileSize,
                footerSize);
    }

    @Override
    public void abort()
    {
        insertSink.abort();
        // Delete files written during this operation will become orphans —
        // cleaned up by DuckLake's maintenance procedures
    }
}
