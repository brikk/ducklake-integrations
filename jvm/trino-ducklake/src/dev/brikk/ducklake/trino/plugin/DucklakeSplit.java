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
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
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
        @JsonProperty("fieldIdToParquetSourceName") Map<Long, String> fieldIdToParquetSourceName,
        // File-local row positions deleted via DuckLake's inlined-delete mechanism
        // ({@code ducklake_inlined_delete_<tableId>.row_id} where {@code file_id} matches
        // this data file's {@code data_file_id} and {@code begin_snapshot <= snapshotId}).
        // The page source merges these into the same deleted-row set as any parquet
        // positional delete files. Empty for files without inlined deletions.
        @JsonProperty("inlinedDeletedRowPositions") Set<Long> inlinedDeletedRowPositions,
        // Affinity key from SplitAffinityProvider. The engine routes splits with the
        // same key to the same worker(s) across queries via a consistent-hash ring,
        // enabling filesystem-cache warm reuse. Empty when caching is not enabled.
        @JsonProperty("affinityKey") Optional<String> affinityKey)
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
        requireNonNull(inlinedDeletedRowPositions, "inlinedDeletedRowPositions is null");
        inlinedDeletedRowPositions = Set.copyOf(inlinedDeletedRowPositions);
        requireNonNull(affinityKey, "affinityKey is null");
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
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, 0L, Map.of(), Map.of(), Map.of(), Set.of(), Optional.empty());
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
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes, Map.of(), Map.of(), Set.of(), Optional.empty());
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
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes, partitionValuesByColumnId, Map.of(), Set.of(), Optional.empty());
    }

    // Eleven-arg constructor — kept for callers that don't carry inlined-delete row positions.
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
            Map<Long, String> partitionValuesByColumnId,
            Map<Long, String> fieldIdToParquetSourceName)
    {
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes, partitionValuesByColumnId, fieldIdToParquetSourceName, Set.of(), Optional.empty());
    }

    // Twelve-arg constructor — kept for callers that don't yet supply an affinity key.
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
            Map<Long, String> partitionValuesByColumnId,
            Map<Long, String> fieldIdToParquetSourceName,
            Set<Long> inlinedDeletedRowPositions)
    {
        this(dataFilePath, deleteFilePaths, rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes, partitionValuesByColumnId, fieldIdToParquetSourceName, inlinedDeletedRowPositions, Optional.empty());
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
    public Optional<String> getAffinityKey()
    {
        return affinityKey;
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
        long inlinedDeletesRetained = (long) inlinedDeletedRowPositions.size() * SIZE_OF_LONG;
        return INSTANCE_SIZE
                + estimatedSizeOf(dataFilePath)
                + deleteFilePaths.stream().mapToLong(SizeOf::estimatedSizeOf).sum()
                + (SIZE_OF_LONG * 4) // rowIdStart, recordCount, fileSizeBytes, footerSize
                + estimatedSizeOf(fileFormat)
                + fileStatisticsDomain.getRetainedSizeInBytes(DucklakeColumnHandle::getRetainedSizeInBytes)
                + deleteFooterSizesRetained
                + partitionValuesRetained
                + sourceNameRetained
                + inlinedDeletesRetained
                + sizeOf(affinityKey, SizeOf::estimatedSizeOf);
    }
}
