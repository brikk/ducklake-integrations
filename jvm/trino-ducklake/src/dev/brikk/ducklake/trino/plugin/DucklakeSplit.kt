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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.airlift.slice.SizeOf
import io.airlift.slice.SizeOf.SIZE_OF_LONG
import io.airlift.slice.SizeOf.estimatedSizeOf
import io.airlift.slice.SizeOf.instanceSize
import io.airlift.slice.SizeOf.sizeOf
import io.trino.spi.connector.ConnectorSplit
import io.trino.spi.predicate.TupleDomain
import java.util.Optional

/**
 * Represents a split for reading Ducklake data.
 * Each split corresponds to a Parquet data file.
 */
@JvmRecord
data class DucklakeSplit @JsonCreator constructor(
        @get:JvmName("dataFilePath")
        @param:JsonProperty("dataFilePath") val dataFilePath: String,
        @get:JvmName("deleteFilePaths")
        @param:JsonProperty("deleteFilePaths") val deleteFilePaths: List<String>,
        @get:JvmName("rowIdStart")
        @param:JsonProperty("rowIdStart") val rowIdStart: Long,
        @get:JvmName("recordCount")
        @param:JsonProperty("recordCount") val recordCount: Long,
        @get:JvmName("fileSizeBytes")
        @param:JsonProperty("fileSizeBytes") val fileSizeBytes: Long,
        @get:JvmName("fileFormat")
        @param:JsonProperty("fileFormat") val fileFormat: String,
        @get:JvmName("fileStatisticsDomain")
        @param:JsonProperty("fileStatisticsDomain") val fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>,
        // DuckLake footer-size hint (Thrift FileMetaData length, excluding the 8-byte
        // post-script). 0 means "no hint" — the reader falls back to Trino's default
        // 48 KB blind tail read. See ducklake_data_file.footer_size in the spec.
        @get:JvmName("footerSize")
        @param:JsonProperty("footerSize") val footerSize: Long,
        // Parallel hints for the delete files listed in deleteFilePaths. Map is used
        // instead of a parallel list because deleteFilePaths is deduplicated; a missing
        // entry (or 0 value) means "no hint for this path".
        @get:JvmName("deleteFileFooterSizes")
        @param:JsonProperty("deleteFileFooterSizes") val deleteFileFooterSizes: Map<String, Long>,
        // Catalog-recorded partition values for this file (keyed by the partition
        // column's catalog column_id). Only populated for IDENTITY-transform partition
        // fields — temporal/bucket transforms produce derived values that can't be
        // projected back as the original column. The page source provider consults this
        // map to constant-fill partition columns missing from the parquet body
        // (hive-style external file imports).
        @get:JvmName("partitionValuesByColumnId")
        @param:JsonProperty("partitionValuesByColumnId") val partitionValuesByColumnId: Map<Long, String>,
        // Per-file source-name override: maps each top-level field_id to the parquet
        // column name to look up in this file's schema, when that differs from the
        // table column name. Populated from `ducklake_name_mapping` when the data
        // file has a non-null `mapping_id` (today: files registered via
        // `add_files`). The page source consults this after name-then-field_id
        // lookups miss in the parquet schema.
        @get:JvmName("fieldIdToParquetSourceName")
        @param:JsonProperty("fieldIdToParquetSourceName") val fieldIdToParquetSourceName: Map<Long, String>,
        // File-local row positions deleted via DuckLake's inlined-delete mechanism
        // (`ducklake_inlined_delete_<tableId>.row_id` where `file_id` matches
        // this data file's `data_file_id` and `begin_snapshot <= snapshotId`).
        // The page source merges these into the same deleted-row set as any parquet
        // positional delete files. Empty for files without inlined deletions.
        @get:JvmName("inlinedDeletedRowPositions")
        @param:JsonProperty("inlinedDeletedRowPositions") val inlinedDeletedRowPositions: Set<Long>,
        // Affinity key from SplitAffinityProvider. The engine routes splits with the
        // same key to the same worker(s) across queries via a consistent-hash ring,
        // enabling filesystem-cache warm reuse. Empty when caching is not enabled.
        @get:JvmName("affinityKey")
        @param:JsonProperty("affinityKey") val affinityKey: Optional<String>,
        // begin_snapshot of the primary data file (ducklake_data_file.begin_snapshot) —
        // the source for the $snapshot_id virtual column. The default applies only to the
        // test-only convenience constructors below; production (DucklakeSplitManager.
        // createMergedSplit) always passes the real value through the canonical constructor.
        @get:JvmName("beginSnapshot")
        @param:JsonProperty("beginSnapshot") val beginSnapshot: Long = 0L,
        // Set only when this data file is a cross-snapshot compacted ("partial") file whose
        // partial_max exceeds the read's snapshot — i.e. it physically holds rows newer than this
        // read. Its value is the QUERY snapshot S: the page source drops rows whose
        // `_ducklake_internal_snapshot_id > S`. Null for ordinary files (no filter needed).
        @get:JvmName("snapshotFilterMax")
        @param:JsonProperty("snapshotFilterMax") val snapshotFilterMax: Long? = null)
        : ConnectorSplit
{
    // Convenience constructor without footer-size hints / partition values — used by
    // tests that don't exercise those paths. Production code in DucklakeSplitManager
    // always uses the canonical constructor.
    constructor(
            dataFilePath: String,
            deleteFilePaths: List<String>,
            rowIdStart: Long,
            recordCount: Long,
            fileSizeBytes: Long,
            fileFormat: String,
            fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>)
            : this(dataFilePath, deleteFilePaths.toList(), rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, 0L, emptyMap(), emptyMap(), emptyMap(), emptySet(), Optional.empty())

    // Eight-arg legacy constructor (no partition values) — kept for existing call sites
    // that don't carry partition-value data yet.
    constructor(
            dataFilePath: String,
            deleteFilePaths: List<String>,
            rowIdStart: Long,
            recordCount: Long,
            fileSizeBytes: Long,
            fileFormat: String,
            fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>,
            footerSize: Long,
            deleteFileFooterSizes: Map<String, Long>)
            : this(dataFilePath, deleteFilePaths.toList(), rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes.toMap(), emptyMap(), emptyMap(), emptySet(), Optional.empty())

    // Ten-arg constructor used during the partition-value-projection introduction —
    // kept for callers that don't yet supply per-file source-name overrides.
    constructor(
            dataFilePath: String,
            deleteFilePaths: List<String>,
            rowIdStart: Long,
            recordCount: Long,
            fileSizeBytes: Long,
            fileFormat: String,
            fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>,
            footerSize: Long,
            deleteFileFooterSizes: Map<String, Long>,
            partitionValuesByColumnId: Map<Long, String>)
            : this(dataFilePath, deleteFilePaths.toList(), rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes.toMap(), partitionValuesByColumnId.toMap(), emptyMap(), emptySet(), Optional.empty())

    // Eleven-arg constructor — kept for callers that don't carry inlined-delete row positions.
    constructor(
            dataFilePath: String,
            deleteFilePaths: List<String>,
            rowIdStart: Long,
            recordCount: Long,
            fileSizeBytes: Long,
            fileFormat: String,
            fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>,
            footerSize: Long,
            deleteFileFooterSizes: Map<String, Long>,
            partitionValuesByColumnId: Map<Long, String>,
            fieldIdToParquetSourceName: Map<Long, String>)
            : this(dataFilePath, deleteFilePaths.toList(), rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes.toMap(), partitionValuesByColumnId.toMap(), fieldIdToParquetSourceName.toMap(), emptySet(), Optional.empty())

    // Twelve-arg constructor — kept for callers that don't yet supply an affinity key.
    constructor(
            dataFilePath: String,
            deleteFilePaths: List<String>,
            rowIdStart: Long,
            recordCount: Long,
            fileSizeBytes: Long,
            fileFormat: String,
            fileStatisticsDomain: TupleDomain<DucklakeColumnHandle>,
            footerSize: Long,
            deleteFileFooterSizes: Map<String, Long>,
            partitionValuesByColumnId: Map<Long, String>,
            fieldIdToParquetSourceName: Map<Long, String>,
            inlinedDeletedRowPositions: Set<Long>)
            : this(dataFilePath, deleteFilePaths.toList(), rowIdStart, recordCount, fileSizeBytes, fileFormat, fileStatisticsDomain, footerSize, deleteFileFooterSizes.toMap(), partitionValuesByColumnId.toMap(), fieldIdToParquetSourceName.toMap(), inlinedDeletedRowPositions.toSet(), Optional.empty())

    /**
     * Backward-compatible accessor for the single delete file path.
     * Returns the first delete file path if present.
     */
    fun deleteFilePath(): Optional<String> {
        return if (deleteFilePaths.isEmpty()) Optional.empty() else Optional.of(deleteFilePaths.first())
    }

    override fun isRemotelyAccessible(): Boolean {
        // Ducklake files can be on object storage
        return true
    }

    override fun getAffinityKey(): Optional<String> {
        return affinityKey
    }

    override fun getRetainedSizeInBytes(): Long {
        // deleteFileFooterSizes is keyed by the same strings already held in deleteFilePaths
        // (the map is a sparse hint table over the list). Count the strings once via the list
        // and only the value-slots here so they aren't double-counted.
        val deleteFooterSizesRetained: Long = deleteFileFooterSizes.size.toLong() * SIZE_OF_LONG
        val partitionValuesRetained: Long = partitionValuesByColumnId.entries.stream()
                .mapToLong { entry -> SIZE_OF_LONG + estimatedSizeOf(entry.value) }
                .sum()
        val sourceNameRetained: Long = fieldIdToParquetSourceName.entries.stream()
                .mapToLong { entry -> SIZE_OF_LONG + estimatedSizeOf(entry.value) }
                .sum()
        val inlinedDeletesRetained: Long = inlinedDeletedRowPositions.size.toLong() * SIZE_OF_LONG
        // INSTANCE_SIZE accounts for the primitive long fields (rowIdStart, recordCount,
        // fileSizeBytes, footerSize), so no extra SIZE_OF_LONG additions are needed for them.
        return (INSTANCE_SIZE
                + estimatedSizeOf(dataFilePath)
                + deleteFilePaths.stream().mapToLong { s -> estimatedSizeOf(s) }.sum()
                + estimatedSizeOf(fileFormat)
                + fileStatisticsDomain.getRetainedSizeInBytes { handle: DucklakeColumnHandle -> handle.getRetainedSizeInBytes() }
                + deleteFooterSizesRetained
                + partitionValuesRetained
                + sourceNameRetained
                + inlinedDeletesRetained
                + sizeOf(affinityKey) { s -> estimatedSizeOf(s) })
    }

    companion object {
        private val INSTANCE_SIZE: Int = instanceSize(DucklakeSplit::class.java)
    }
}
