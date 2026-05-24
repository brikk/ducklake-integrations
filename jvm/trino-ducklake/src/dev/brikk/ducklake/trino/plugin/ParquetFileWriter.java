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

import dev.brikk.ducklake.catalog.DucklakeColumn;
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;
import io.airlift.slice.DynamicSliceOutput;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.spi.Page;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_PARQUET;
import static java.util.Objects.requireNonNull;

/**
 * {@link DucklakeFileWriter} that delegates to Trino's {@link ParquetWriter}, streaming
 * directly to the destination via {@link TrinoOutputFile} (no local staging).
 */
final class ParquetFileWriter
        implements DucklakeFileWriter
{
    private final ParquetWriter parquetWriter;
    private final OutputStream outputStream;
    private final String relativePath;
    private final Map<Integer, String> partitionValues;
    private final OptionalLong partitionId;
    private final List<LeafStatsTarget> leafStatsTargets;

    ParquetFileWriter(
            ParquetWriter parquetWriter,
            OutputStream outputStream,
            String relativePath,
            Map<Integer, String> partitionValues,
            OptionalLong partitionId,
            List<DucklakeColumnHandle> columns,
            List<DucklakeColumn> allCatalogColumns)
    {
        this.parquetWriter = requireNonNull(parquetWriter, "parquetWriter is null");
        this.outputStream = requireNonNull(outputStream, "outputStream is null");
        this.relativePath = requireNonNull(relativePath, "relativePath is null");
        this.partitionValues = new HashMap<>(requireNonNull(partitionValues, "partitionValues is null"));
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(allCatalogColumns, "allCatalogColumns is null");
        this.leafStatsTargets = DucklakeStatsLeafProjector.projectFromCatalogTree(columns, allCatalogColumns);
    }

    @Override
    public void write(Page page)
            throws IOException
    {
        parquetWriter.write(page);
    }

    @Override
    public long getApproximateWrittenBytes()
    {
        return parquetWriter.getEstimatedWrittenBytes();
    }

    @Override
    public DucklakeWriteFragment finishAndBuildFragment()
            throws IOException
    {
        parquetWriter.close();

        FileMetaData fileMetaData = parquetWriter.getFileMetaData();
        long recordCount = fileMetaData.getNum_rows();
        long fileSize = parquetWriter.getEstimatedWrittenBytes();

        long footerSize;
        try {
            DynamicSliceOutput footerOutput = new DynamicSliceOutput(40);
            Util.writeFileMetaData(fileMetaData, footerOutput);
            footerSize = footerOutput.size();
        }
        catch (IOException e) {
            footerSize = 0;
        }

        List<DucklakeFileColumnStats> columnStats =
                DucklakeStatsExtractor.extractStats(fileMetaData, leafStatsTargets);

        return new DucklakeWriteFragment(
                relativePath,
                FORMAT_PARQUET,
                fileSize,
                footerSize,
                recordCount,
                columnStats,
                partitionValues,
                partitionId);
    }

    @Override
    public void abort()
    {
        try {
            parquetWriter.close();
        }
        catch (IOException ignored) {
            // best-effort
        }
        try {
            outputStream.close();
        }
        catch (IOException ignored) {
            // best-effort
        }
    }
}
