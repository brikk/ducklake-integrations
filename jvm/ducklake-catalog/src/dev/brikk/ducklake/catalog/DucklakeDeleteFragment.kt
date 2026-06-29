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
package dev.brikk.ducklake.catalog

/**
 * Describes a delete Parquet file written during a DELETE/UPDATE/MERGE operation.
 * Each fragment corresponds to one data file's deletions.
 *
 * [deleteCount] is the TOTAL positions stored in the new parquet file
 * (union of prior-active-delete-file positions plus this commit's new deletes).
 * [newDeleteCount] is the DELTA that should be subtracted from the table's
 * `record_count` — i.e. the count of positions added by THIS commit only.
 * The prior file's positions were already deducted from `record_count` when
 * that file was first committed; the new file supersedes it (end-snapshotted in
 * the same transaction) and re-introducing its count would double-subtract.
 */
@JvmRecord
@JacksonSerializedInternalJavaCompatibleClass
data class DucklakeDeleteFragment(
    val dataFileId: Long,
    val path: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long,
    val newDeleteCount: Long,
    /**
     * Delete-file format: `"parquet"` (the default — `(file_path, pos)` positional delete file) or
     * `"puffin"` (a DuckLake deletion-vector blob, written when `write_deletion_vectors` is on).
     * Persisted to `ducklake_delete_file.format`; both are read by Trino and DuckDB.
     */
    val format: String = "parquet",
)
