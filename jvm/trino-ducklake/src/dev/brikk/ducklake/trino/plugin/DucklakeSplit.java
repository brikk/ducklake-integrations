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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * Represents a split for reading Ducklake data.
 * Each split corresponds to a Parquet data file.
 */
public record DucklakeSplit(
        @JsonProperty("dataFilePath") String dataFilePath,
        @JsonProperty("deleteFilePaths") List<String> deleteFilePaths,
        @JsonProperty("rowIdStart") long rowIdStart,
        @JsonProperty("recordCount") long recordCount,
        @JsonProperty("fileSizeBytes") long fileSizeBytes,
        @JsonProperty("fileFormat") String fileFormat,
        @JsonProperty("fileStatisticsDomain") TupleDomain<DucklakeColumnHandle> fileStatisticsDomain,
        // DuckLake footer-size hint (Thrift FileMetaData length, excluding the 8-byte
        // post-script). 0 means "no hint" — the reader falls back to Trino's default
        // 48 KB blind tail read. See ducklake_data_file.footer_size in the spec.
        @JsonProperty("footerSize") long footerSize,
        // Parallel hints for the delete files listed in deleteFilePaths. Map is used
        // instead of a parallel list because deleteFilePaths is deduplicated; a missing
        // entry (or 0 value) means "no hint for this path".
        @JsonProperty("deleteFileFooterSizes") Map<String, Long> deleteFileFooterSizes)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(DucklakeSplit.class);

    @JsonCreator
    public DucklakeSplit
    {
        requireNonNull(dataFilePath, "dataFilePath is null");
        requireNonNull(deleteFilePaths, "deleteFilePaths is null");
        deleteFilePaths = List.copyOf(deleteFilePaths);
        requireNonNull(fileFormat, "fileFormat is null");
        requireNonNull(fileStatisticsDomain, "fileStatisticsDomain is null");
        requireNonNull(deleteFileFooterSizes, "deleteFileFooterSizes is null");
        deleteFileFooterSizes = Map.copyOf(deleteFileFooterSizes);
    }

    // Convenience constructor without footer-size hints — used by tests that don't
    // exercise the hint path. Production code in DucklakeSplitManager always uses the
    // canonical constructor with hints populated from the catalog.
    public DucklakeSplit(
            String dataFilePath,
            List<String> deleteFilePaths,
            long rowIdStart,
            long recordCount,
            long fileSizeBytes,
            String fileFormat,
            TupleDomain<DucklakeColumnHandle> fileStatisticsDomain)
    {
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, 0L, Map.of());
    }

    /**
     * Backward-compatible accessor for the single delete file path.
     * Returns the first delete file path if present.
     */
    public Optional<String> deleteFilePath()
    {
        return deleteFilePaths.isEmpty() ? Optional.empty() : Optional.of(deleteFilePaths.getFirst());
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // Ducklake files can be on object storage
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        // No specific host affinity for object storage
        return List.of();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long deleteFooterSizesRetained = deleteFileFooterSizes.entrySet().stream()
                .mapToLong(entry -> estimatedSizeOf(entry.getKey()) + SIZE_OF_LONG)
                .sum();
        return INSTANCE_SIZE
                + estimatedSizeOf(dataFilePath)
                + deleteFilePaths.stream().mapToLong(SizeOf::estimatedSizeOf).sum()
                + (SIZE_OF_LONG * 4) // rowIdStart, recordCount, fileSizeBytes, footerSize
                + estimatedSizeOf(fileFormat)
                + fileStatisticsDomain.getRetainedSizeInBytes(DucklakeColumnHandle::getRetainedSizeInBytes)
                + deleteFooterSizesRetained;
    }
}
