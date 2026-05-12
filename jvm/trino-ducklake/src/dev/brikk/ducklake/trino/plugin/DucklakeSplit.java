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
        @JsonProperty("deleteFileFooterSizes") Map<String, Long> deleteFileFooterSizes,
        // Catalog-recorded partition values for this file (keyed by the partition
        // column's catalog column_id). Only populated for IDENTITY-transform partition
        // fields — temporal/bucket transforms produce derived values that can't be
        // projected back as the original column. The page source provider consults this
        // map to constant-fill partition columns missing from the parquet body
        // (hive-style external file imports).
        @JsonProperty("partitionValuesByColumnId") Map<Long, String> partitionValuesByColumnId,
        // Per-file source-name override: maps each top-level field_id to the parquet
        // column name to look up in this file's schema, when that differs from the
        // table column name. Populated from {@code ducklake_name_mapping} when the data
        // file has a non-null {@code mapping_id} (today: files registered via
        // {@code add_files}). The page source consults this after name-then-field_id
        // lookups miss in the parquet schema.
        @JsonProperty("fieldIdToParquetSourceName") Map<Long, String> fieldIdToParquetSourceName)
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
        requireNonNull(partitionValuesByColumnId, "partitionValuesByColumnId is null");
        partitionValuesByColumnId = Map.copyOf(partitionValuesByColumnId);
        requireNonNull(fieldIdToParquetSourceName, "fieldIdToParquetSourceName is null");
        fieldIdToParquetSourceName = Map.copyOf(fieldIdToParquetSourceName);
    }

    // Convenience constructor without footer-size hints / partition values — used by
    // tests that don't exercise those paths. Production code in DucklakeSplitManager
    // always uses the canonical constructor.
    public DucklakeSplit(
            String dataFilePath,
            List<String> deleteFilePaths,
            long rowIdStart,
            long recordCount,
            long fileSizeBytes,
            String fileFormat,
            TupleDomain<DucklakeColumnHandle> fileStatisticsDomain)
    {
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, 0L, Map.of(), Map.of(), Map.of());
    }

    // Eight-arg legacy constructor (no partition values) — kept for existing call sites
    // that don't carry partition-value data yet.
    public DucklakeSplit(
            String dataFilePath,
            List<String> deleteFilePaths,
            long rowIdStart,
            long recordCount,
            long fileSizeBytes,
            String fileFormat,
            TupleDomain<DucklakeColumnHandle> fileStatisticsDomain,
            long footerSize,
            Map<String, Long> deleteFileFooterSizes)
    {
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes, Map.of(), Map.of());
    }

    // Ten-arg constructor used during the partition-value-projection introduction —
    // kept for callers that don't yet supply per-file source-name overrides.
    public DucklakeSplit(
            String dataFilePath,
            List<String> deleteFilePaths,
            long rowIdStart,
            long recordCount,
            long fileSizeBytes,
            String fileFormat,
            TupleDomain<DucklakeColumnHandle> fileStatisticsDomain,
            long footerSize,
            Map<String, Long> deleteFileFooterSizes,
            Map<Long, String> partitionValuesByColumnId)
    {
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes, partitionValuesByColumnId, Map.of());
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
        long partitionValuesRetained = partitionValuesByColumnId.entrySet().stream()
                .mapToLong(entry -> SIZE_OF_LONG + estimatedSizeOf(entry.getValue()))
                .sum();
        long sourceNameRetained = fieldIdToParquetSourceName.entrySet().stream()
                .mapToLong(entry -> SIZE_OF_LONG + estimatedSizeOf(entry.getValue()))
                .sum();
        return INSTANCE_SIZE
                + estimatedSizeOf(dataFilePath)
                + deleteFilePaths.stream().mapToLong(SizeOf::estimatedSizeOf).sum()
                + (SIZE_OF_LONG * 4) // rowIdStart, recordCount, fileSizeBytes, footerSize
                + estimatedSizeOf(fileFormat)
                + fileStatisticsDomain.getRetainedSizeInBytes(DucklakeColumnHandle::getRetainedSizeInBytes)
                + deleteFooterSizesRetained
                + partitionValuesRetained
                + sourceNameRetained;
    }
}
