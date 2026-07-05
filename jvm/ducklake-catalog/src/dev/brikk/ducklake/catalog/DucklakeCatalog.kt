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

import java.time.Instant

/**
 * Interface for Ducklake SQL-based catalog operations.
 * Abstracts access to the 28 Ducklake metadata tables.
 */
interface DucklakeCatalog {
    /**
     * Get the current (maximum) snapshot ID
     */
    val currentSnapshotId: Long

    /**
     * Get a specific snapshot ID (for time travel queries)
     */
    fun getSnapshot(snapshotId: Long): DucklakeSnapshot?

    /**
     * Get the latest snapshot with snapshot_time <= timestamp.
     */
    fun getSnapshotAtOrBefore(timestamp: Instant): DucklakeSnapshot?

    /**
     * List all snapshots in the catalog, newest first.
     */
    fun listSnapshots(): List<DucklakeSnapshot>

    /**
     * List all snapshot change records, newest first.
     */
    fun listSnapshotChanges(): List<DucklakeSnapshotChange>

    /**
     * List all schemas visible at the given snapshot
     */
    fun listSchemas(snapshotId: Long): List<DucklakeSchema>

    /**
     * Get a specific schema by name at the given snapshot
     */
    fun getSchema(schemaName: String, snapshotId: Long): DucklakeSchema?

    /**
     * List all tables in a schema at the given snapshot
     */
    fun listTables(schemaId: Long, snapshotId: Long): List<DucklakeTable>

    /**
     * Get a specific table by name at the given snapshot
     */
    fun getTable(schemaName: String, tableName: String, snapshotId: Long): DucklakeTable?

    /**
     * Get table by ID at the given snapshot
     */
    fun getTableById(tableId: Long, snapshotId: Long): DucklakeTable?

    /**
     * Get top-level columns for a table at the given snapshot (nested types resolved into type strings).
     */
    fun getTableColumns(tableId: Long, snapshotId: Long): List<DucklakeColumn>

    /**
     * Get all columns (including nested) for a table at the given snapshot as a flat list.
     * Each column retains its `parentColumn` reference. Used for Parquet field_id mapping.
     */
    fun getAllColumnsWithParentage(tableId: Long, snapshotId: Long): List<DucklakeColumn>

    /**
     * Get data files for a table at the given snapshot
     */
    fun getDataFiles(tableId: Long, snapshotId: Long): List<DucklakeDataFile>

    /**
     * Data files whose rows were INSERTED in the inclusive snapshot window `[startSnapshot,
     * endSnapshot]` — i.e. `begin_snapshot >= startSnapshot AND begin_snapshot <= endSnapshot`.
     * The change-feed insert side (`table_insertions` / the insert half of `table_changes`): every
     * row of such a file was inserted at the file's `begin_snapshot`, with row identifier
     * `row_id_start + file position`. Delete-file columns are left null (the feed reads ALL rows of
     * the file regardless of any later deletions — those are separate delete events). Ordered by
     * `begin_snapshot`, then `file_order`.
     */
    fun getDataFilesAddedBetween(tableId: Long, startSnapshot: Long, endSnapshot: Long): List<DucklakeDataFile>

    /**
     * Deletion events in the inclusive snapshot window `[startSnapshot, endSnapshot]` — the
     * change-feed delete side (`table_deletions` / the delete half of `table_changes`). Each
     * [DucklakeChangeFeedDeletion] captures, per data file, the rows retired at one snapshot as the
     * difference between the delete state at that snapshot and the state just before it (see that
     * class for the two shapes: incremental delete file vs. full-file retire). The caller reads the
     * actual deleted positions from the current/previous delete files and subtracts.
     */
    fun getDeletionsBetween(tableId: Long, startSnapshot: Long, endSnapshot: Long): List<DucklakeChangeFeedDeletion>

    /**
     * True only if the table has a partial DELETE file active at [snapshotId] in a format this
     * connector cannot snapshot-filter. Both PARQUET (filtered via `_ducklake_internal_snapshot_id`)
     * and PUFFIN (each blob's embedded `ducklake-snapshot-id`) partial delete files are now handled
     * on read, so this returns false for them; it only flags an unknown delete-file format, which
     * [io.trino] validateDeleteFileFormats already rejects regardless of `partial_max`. Kept as a
     * defensive double-gate. See dev-docs/DESIGN-maintenance.md § 6.
     */
    fun hasPartialDeleteFilesRequiringSnapshotFilter(tableId: Long, snapshotId: Long): Boolean

    /**
     * Every storage path the catalog references for this table, regardless of snapshot liveness:
     * all `ducklake_data_file` and `ducklake_delete_file` rows for the table (including
     * end-snapshotted ones — those physical files are still owned by the catalog until cleanup),
     * plus rows already in `ducklake_files_scheduled_for_deletion`. This is the "known set" for
     * orphan-file detection ([remove_orphan_files]): a file under the table's data path that is
     * NOT in this set has no catalog row at all and is a candidate orphan. Paths are returned raw
     * (relative or absolute, per `path_is_relative`); the caller resolves them against the table
     * data path.
     */
    fun listReferencedFilePaths(tableId: Long): List<DucklakeFilePathRef>

    /**
     * Snapshot ids eligible for expiry — every snapshot EXCEPT the latest (which is never
     * expirable), optionally narrowed to those older than [olderThan] or to an explicit [versions]
     * set. [olderThan] and [versions] are mutually exclusive; pass both null to list all
     * non-latest snapshots. Returned ascending. Read-only (used by both the dry-run and the op).
     */
    fun listExpirableSnapshots(olderThan: java.time.Instant?, versions: Set<Long>?): List<Long>

    /**
     * Expire [snapshotIds] (must NOT include the latest snapshot). Deletes the snapshot +
     * snapshot_changes rows, then cascade-deletes the now-dead data/delete files (+ their column
     * stats, variant stats, and partition values), **scheduling** those files into
     * `ducklake_files_scheduled_for_deletion` (stored as absolute paths) for a later age-gated
     * [physical cleanup][listFilesScheduledForDeletion]. Liveness uses the half-open
     * `[begin_snapshot, end_snapshot)` survivor test against the snapshots that REMAIN; files of a
     * fully-expired dropped table are caught via that table's id too, so no data file leaks.
     * A plain catalog transaction — no new snapshot, no `changes_made` entry (matches `ANALYZE`;
     * expiry is destructive GC, not a forward commit).
     *
     * Also GCs the dead *metadata* rows of fully-expired dropped tables/views/macros/schemas
     * (`ducklake_table` + column/stats/partition/sort/mapping rows; `ducklake_view`;
     * `ducklake_macro`/`_impl`/`_parameters`; `ducklake_schema`) and name-mapping rows orphaned by
     * that cleanup — a full catalog sweep so a long-lived warehouse doesn't accumulate dead
     * metadata. Not supported on the Quack backend yet.
     */
    fun expireSnapshots(snapshotIds: Set<Long>): ExpireSnapshotsResult

    /**
     * Files due for physical deletion: `ducklake_files_scheduled_for_deletion` rows whose
     * `schedule_start` is strictly before [olderThan] (the grace period). Paths are returned raw;
     * relative ones are resolved by the caller against the catalog `data_path` ROOT (NOT a table
     * dir — see [DucklakeScheduledFile]).
     */
    fun listFilesScheduledForDeletion(olderThan: java.time.Instant): List<DucklakeScheduledFile>

    /** Delete the schedule rows for [dataFileIds] (after their physical files were removed). */
    fun removeScheduledFileRows(dataFileIds: Collection<Long>)

    /**
     * Get the `file_format` of the most recent active data file for this table at the given
     * snapshot. Used by INSERT (and the insert leg of MERGE/UPDATE) to inherit the format of the
     * existing data when neither a session property nor a statement-level override is in play.
     * Returns empty when the table has no data files yet.
     */
    fun getLatestDataFileFormat(tableId: Long, snapshotId: Long): String?

    /**
     * The table's DECLARED data file format: the table-scoped `data_file_format` setting in
     * `ducklake_metadata` (scope = 'table', scope_id = tableId), persisted when the table was
     * created with an explicit `WITH (data_file_format = ...)`. Null when the table never
     * declared a format — writes then fall back to latest-data-file inheritance. Settings rows
     * are unversioned (no snapshot range), matching upstream's table-scoped option semantics.
     */
    fun getTableDataFileFormat(tableId: Long): String?

    /**
     * Find data file IDs whose column statistics overlap with the given range.
     * Used for predicate pushdown — files outside the range are pruned.
     */
    fun findDataFileIdsInRange(tableId: Long, snapshotId: Long, predicate: ColumnRangePredicate): List<Long>

    /**
     * Get table-level statistics (record count, file size) from ducklake_table_stats
     */
    fun getTableStats(tableId: Long): DucklakeTableStats?

    /**
     * Get aggregated column statistics across all active data files.
     * Min/max values are compared using typed comparison (numeric, not lexicographic)
     * based on column types resolved internally.
     */
    fun getColumnStats(tableId: Long, snapshotId: Long): List<DucklakeColumnStats>

    /**
     * Recompute the cached table-level statistics for a table from ground truth — the
     * connector's `ANALYZE` entry point.
     *
     * DuckLake maintains [DucklakeTableStats] (`ducklake_table_stats`) and the per-column
     * aggregates (`ducklake_table_column_stats`) incrementally on every write. Incremental
     * maintenance only ever *widens* a column's min/max (it merges each new file's bounds in)
     * and never narrows them after a DELETE or file expiry, so the cached bounds can drift
     * loose over a table's lifetime. This forces a full refresh:
     *  - `record_count` is set to [rowCount] (the engine's authoritative live count from the
     *    ANALYZE scan — already net of deletes and inlined rows); `next_row_id` (the row-id
     *    allocator high-water mark) is preserved.
     *  - `file_size_bytes` is recomputed from the active data files.
     *  - the per-column aggregates are rebuilt from the authoritative per-file stats
     *    (`ducklake_file_column_stats`) of the currently-active data files, tightening any
     *    bounds that incremental maintenance left stale.
     *
     * These are mutable, non-snapshot-versioned side tables, so this runs in a plain catalog
     * transaction and does NOT mint a new snapshot or record a `changes_made` entry (there is
     * no stats change-type in the DuckLake vocabulary). The refreshed values are advisory
     * planner hints; run `ANALYZE` while the table is quiescent for an exact result.
     */
    fun analyzeTable(tableId: Long, rowCount: Long)

    /**
     * Get partition specs for a table at the given snapshot
     */
    fun getPartitionSpecs(tableId: Long, snapshotId: Long): List<DucklakePartitionSpec>

    /**
     * Read the table's sort spec at the given snapshot, ordered by
     * `sort_key_index`. Returns an empty list when the table has no
     * `ducklake_sort_info` row active at the snapshot (which is the
     * common case — sorted tables are opt-in). The catalog stores expression
     * text in the writer's dialect (typically `"duckdb"`); callers
     * choosing to translate the expression are responsible for downgrading
     * to "no sort property" silently when interpretation fails, rather than
     * misreporting an order.
     */
    fun getSortKeys(tableId: Long, snapshotId: Long): List<DucklakeSortKey>

    /**
     * Load the top-level entries of one or more name maps. Returns a map keyed by
     * `mapping_id` where each value maps `target_field_id → source_name`
     * for non-hive-partition entries whose `parent_column` is NULL. Used by the
     * read path to find the parquet column corresponding to a table field id when the
     * file's column names don't match the table's (e.g. case-difference, registered
     * via `add_files`). Returns an empty map for the empty input set.
     */
    fun getNameMaps(mappingIds: Set<Long>): Map<Long, Map<Long, String>>

    /**
     * Get partition values for all active data files of a table at the given snapshot
     */
    fun getFilePartitionValues(tableId: Long, snapshotId: Long): Map<Long, List<DucklakeFilePartitionValue>>

    /**
     * List inlined data table descriptors for a table at the given snapshot.
     * A table can have multiple inlined data tables (one per schema_version).
     * Descriptors whose physical table is missing (stale catalog metadata for a
     * dropped/non-materialized table) are filtered out; each returned descriptor
     * carries [DucklakeInlinedDataInfo.hasLiveRows] so callers can distinguish a
     * real inlined split from the empty-table synthetic split without a second
     * per-table probe.
     */
    fun getInlinedDataInfos(tableId: Long, snapshotId: Long): List<DucklakeInlinedDataInfo>

    /**
     * Check if an inlined data table has any live rows at a given snapshot.
     */
    fun hasInlinedRows(tableId: Long, schemaVersion: Long, snapshotId: Long): Boolean

    /**
     * Count the rows of an inlined data table live at a given snapshot via a
     * `SELECT COUNT(*)`, without materialising the row payload. Returns 0 when
     * the physical table does not exist.
     */
    fun countInlinedRows(tableId: Long, schemaVersion: Long, snapshotId: Long): Long

    /**
     * Check whether this table has an inlined-delete metadata table
     * (`ducklake_inlined_delete_<tableId>`) populated with deletions
     * visible at the given snapshot. Returns `false` when the table
     * doesn't exist (the common case — DuckDB only creates it the first
     * time `DATA_INLINING_ROW_LIMIT` causes a deletion to be inlined).
     *
     * Used by readers to decide whether to invalidate file-level
     * statistics conservatively and to short-circuit the more expensive
     * [getInlinedDeletes] when no inlined deletions are present.
     */
    fun hasInlinedDeletes(tableId: Long, snapshotId: Long): Boolean

    /**
     * Read all inlined-delete rows visible at the given snapshot, grouped by
     * `data_file_id`. Each value is the set of file-local row positions
     * that the writer recorded as deleted (`row_id` in
     * `ducklake_inlined_delete_<tableId>`).
     *
     * Snapshot filter: `begin_snapshot <= snapshotId`. There is no
     * `end_snapshot` — once a row is inlined-deleted it stays deleted
     * until compaction rewrites the data file.
     *
     * Returns an empty map when the per-table inlined-delete metadata table
     * does not exist (the common case for tables DuckDB never inlined deletes
     * for) or when no rows match the snapshot filter.
     */
    fun getInlinedDeletes(tableId: Long, snapshotId: Long): Map<Long, Set<Long>>

    /**
     * Inlined rows (in `ducklake_inlined_data_<tableId>_<schemaVersion>`) that CHANGED in the
     * inclusive window — inserted (`begin_snapshot ∈ [start,end]`) or deleted
     * (`end_snapshot ∈ [start,end]`) — for the change feed. [columnIds] are the feed's requested
     * columns (by id) as of the end snapshot; each row's [DucklakeInlinedChangeRow.values] are
     * projected to them in order (null-filled where a column didn't exist in the row's schema
     * version). Empty when the inlined table doesn't exist.
     */
    fun getInlinedChangesBetween(
        tableId: Long,
        schemaVersion: Long,
        startSnapshot: Long,
        endSnapshot: Long,
        columnIds: List<Long>,
    ): List<DucklakeInlinedChangeRow>

    /**
     * Inlined DELETEs of data-file rows (`ducklake_inlined_delete_<tableId>`) with
     * `begin_snapshot ∈ [start,end]` — the change-feed delete side for small DELETEs DuckDB records
     * inline instead of as positional delete files. Empty when the inlined-delete table is absent.
     */
    fun getInlinedFileDeletesBetween(tableId: Long, startSnapshot: Long, endSnapshot: Long): List<DucklakeInlinedFileDelete>

    /**
     * Data files by id (regardless of snapshot liveness) — used to read rows at inline-deleted
     * positions ([getInlinedFileDeletesBetween]). Delete-file columns are left null.
     */
    fun getDataFilesByIds(tableId: Long, dataFileIds: Collection<Long>): List<DucklakeDataFile>

    /**
     * Read inlined data rows for a table at a given snapshot.
     * Queries ducklake_inlined_data_{tableId}_{schemaVersion} with snapshot filtering.
     * Returns raw JDBC values for each row, one List per row, ordered by column.
     */
    fun readInlinedData(tableId: Long, schemaVersion: Long, snapshotId: Long, columns: List<DucklakeColumn>): List<List<Any?>>

    /**
     * Per-row `begin_snapshot` for the inlined rows live at [snapshotId], ordered by `row_id`
     * — the SAME order [readInlinedData] returns, so the lists align positionally. Inlined rows
     * are versioned catalog rows that each carry their own begin_snapshot, so this is the source
     * for the `$snapshot_id` virtual column on inlined data. Returns empty when the inlined table
     * is absent (never created / already flushed to Parquet).
     */
    fun readInlinedBeginSnapshots(tableId: Long, schemaVersion: Long, snapshotId: Long): List<Long>

    /**
     * Get the base data path from ducklake_metadata
     */
    fun getDataPath(): String?

    // ==================== Schema DDL ====================

    /**
     * Create a new schema. Creates a new snapshot atomically.
     */
    fun createSchema(schemaName: String)

    /**
     * Drop a schema. Fails if the schema contains tables.
     * Creates a new snapshot atomically.
     */
    fun dropSchema(schemaName: String)

    // ==================== Table DDL ====================

    /**
     * Create a new empty table with columns and optional partition spec.
     * Creates a new snapshot atomically.
     *
     * `location`, when present, lands in `ducklake_table.path`
     * (and `path_is_relative`) instead of the default
     * `<tableName>/` relative path. The catalog stores it verbatim;
     * callers (e.g. the Trino connector's `DucklakeTableProperties`)
     * are responsible for trailing-slash, traversal, and scheme normalization.
     *
     * `dataFileFormat`, when present, is persisted as the table-scoped
     * `data_file_format` setting in `ducklake_metadata` (same transaction), so later
     * INSERTs into the still-empty table resolve the declared format instead of falling
     * back to the connector default. Upstream DuckDB loads table-scoped settings into an
     * untyped options map at ATTACH, so the extra key is interop-safe.
     */
    fun createTable(
        schemaName: String,
        tableName: String,
        columns: List<TableColumnSpec>,
        partitionSpec: List<PartitionFieldSpec>?,
        location: TableLocationSpec?,
        dataFileFormat: String? = null,
    )

    /**
     * Drop a table. Sets end_snapshot on the table and all its columns,
     * data files, delete files, and partition info.
     * Creates a new snapshot atomically.
     */
    fun dropTable(schemaName: String, tableName: String)

    /**
     * Truncate a table: remove all rows but keep the table, its columns, partition spec, and
     * settings. End-snapshots every active data file, delete file, and inlined-data row for the
     * table at a new snapshot — a bulk metadata clear, NOT merge-on-read (so it also empties
     * inlined rows, which positional DELETE cannot, and costs O(1) writes regardless of row
     * count). The schema version is NOT bumped (data change, not schema). Recorded as
     * `deleted_from_table`. Creates a new snapshot atomically.
     */
    fun truncateTable(schemaName: String, tableName: String)

    /**
     * Flush a table's inlined rows into data files. Registers [fragments] (the materialized
     * data file(s) the connector wrote from the inlined rows) AND end-snapshots every live
     * inlined row, atomically in one snapshot. The caller must have written [fragments] to
     * contain exactly the live inlined rows. No schema-version bump (a data move). Recorded as
     * `flushed_inlined`. Conflicts (via the conflict matrix) with any intervening commit that
     * dropped/altered the table or changed its inlined data, so a concurrent change aborts
     * rather than duplicating or dropping rows.
     */
    fun flushInlinedData(tableId: Long, fragments: List<DucklakeWriteFragment>)

    /**
     * Rename a table within its schema. End-snapshots the current `ducklake_table` row and
     * inserts a new version with the same table_id, uuid, and path — data files and history
     * are untouched; the table's data directory keeps its original name. Recorded as
     * `altered_table` (upstream's vocabulary for renames). Creates a new snapshot atomically.
     * Fails when the target name is already taken, and rejects a `targetSchemaName` other
     * than the table's own — table data paths are schema-relative, so a cross-schema move
     * would leave the data unreachable.
     */
    fun renameTable(tableId: Long, targetSchemaName: String, newTableName: String)

    /**
     * Rename a schema. `ducklake_schema` carries a PRIMARY KEY on schema_id, so the renamed
     * schema gets a NEW schema_id (old row end-snapshotted — time travel keeps the old name)
     * and every active table/view/macro row is re-pointed to it via versioned-row
     * replacement. The new schema row keeps the OLD path, so schema-relative table paths
     * resolve unchanged and no data moves. Recorded as `dropped_schema` + `created_schema` —
     * upstream's change-type vocabulary has no schema-rename entry, and that pair yields the
     * right conflict surface. Creates a new snapshot atomically. Fails when the target name
     * is already taken.
     */
    fun renameSchema(schemaName: String, newName: String)

    /**
     * Set (or clear, with null) a table's comment: the versioned `comment` tag in
     * `ducklake_tag` keyed by the table id — the same storage upstream's COMMENT ON uses,
     * so comments round-trip cross-engine. Recorded as `altered_table`.
     */
    fun setTableComment(tableId: Long, comment: String?)

    /** The table's `comment` tag active at the snapshot, or null. */
    fun getTableComment(tableId: Long, snapshotId: Long): String?

    /**
     * Set (or clear, with null) a column's comment: the versioned `comment` tag in
     * `ducklake_column_tag` keyed by (table id, column id). Recorded as `altered_table`.
     */
    fun setColumnComment(tableId: Long, columnId: Long, comment: String?)

    /** Active column `comment` tags at the snapshot, keyed by column id. */
    fun getColumnComments(tableId: Long, snapshotId: Long): Map<Long, String>

    /**
     * Add a column to a table. Creates a new ducklake_column row with a new column_id.
     * Increments schema version. Creates a new snapshot atomically.
     */
    fun addColumn(tableId: Long, column: TableColumnSpec)

    /**
     * Drop a column from a table. Sets end_snapshot on the column's current row.
     * Increments schema version. Creates a new snapshot atomically.
     */
    fun dropColumn(tableId: Long, columnId: Long)

    /**
     * Rename a column. End-snapshots the current column row and inserts a new row
     * with the same column_id but updated column_name.
     * Increments schema version. Creates a new snapshot atomically.
     */
    fun renameColumn(tableId: Long, columnId: Long, newName: String)

    /**
     * Add a nested field to a struct column (`ALTER TABLE ... ADD COLUMN parent.child <type>`).
     *
     * [parentPath] is the dotted path to the containing struct, INCLUDING the top-level column name
     * (e.g. `[s]` for `s.child`, `[s, inner]` for `s.inner.child`). [field] is the new subfield
     * subtree (itself possibly nested). The field is inserted as a new `ducklake_column` row with
     * `parent_column` = the struct's column_id and a fresh column_id; the struct row is unchanged.
     * Increments schema version, creates a new snapshot atomically. When [ignoreExisting] is true and
     * the struct already has an active field of that name, this is a no-op (no snapshot).
     */
    fun addField(tableId: Long, parentPath: List<String>, field: TableColumnSpec, ignoreExisting: Boolean)

    /**
     * Drop a nested field from a struct column (`ALTER TABLE ... DROP COLUMN parent.child`).
     *
     * [fieldPath] is the dotted path to the field, INCLUDING the top-level column name (e.g.
     * `[s, child]`). End-snapshots the field's `ducklake_column` row and all of its descendants.
     * Increments schema version, creates a new snapshot atomically.
     */
    fun dropField(tableId: Long, fieldPath: List<String>)

    /**
     * Commit inserted data files to a table.
     * Creates a new snapshot with data file rows, file column stats,
     * and updated table stats.
     */
    fun commitInsert(tableId: Long, fragments: List<DucklakeWriteFragment>)

    /**
     * Register pre-existing parquet files as DuckLake data files of a table.
     * The catalog inserts `ducklake_data_file` + `ducklake_file_column_stats`
     * + `ducklake_column_mapping` / `ducklake_name_mapping` rows for each
     * fragment, dedupes identical name maps within the call (multiple files with the
     * same parquet schema share one `mapping_id`), and records a
     * `tables_inserted_into` snapshot change — same as a normal INSERT, so the
     * existing conflict matrix catches add_files × dropTable / × dropColumn races
     * for free.
     *
     * Fragments are expected to carry `pathIsRelative=false` and an absolute
     * file path. Mirrors upstream's `ducklake_add_data_files` (its
     * `created_by_ducklake=false` flag has no on-disk column in our schema —
     * the difference is observable only via the presence of a name-map row).
     */
    fun commitAddFiles(tableId: Long, fragments: List<DucklakeWriteFragment>)

    /**
     * Commit delete files for a table.
     * Creates a new snapshot with ducklake_delete_file rows and updated table stats.
     */
    fun commitDelete(tableId: Long, deleteFragments: List<DucklakeDeleteFragment>)

    /**
     * Atomically commit both delete files and inserted data files in a single snapshot.
     * Used for UPDATE (delete old rows + insert new rows) and MERGE operations.
     */
    fun commitMerge(
        tableId: Long,
        deleteFragments: List<DucklakeDeleteFragment>,
        insertFragments: List<DucklakeWriteFragment>,
    )

    /**
     * Compaction primitive: atomically register the merged [fragments] (begin = the new snapshot)
     * and **end-snapshot** the source data files in [sourceDataFileIds] (and any active delete file
     * attached to them) at that same new snapshot — the non-partial / Iceberg-style `optimize` /
     * `rewrite_data_files` shape (see dev-docs/DESIGN-maintenance.md § 7).
     *
     * Contract: the merged fragments must hold **exactly the live rows** of the retired sources
     * (deletes already applied), so compaction is **row-count-preserving** — `ducklake_table_stats`
     * `record_count` is left unchanged, `file_size_bytes` is adjusted by (Σmerged − Σretired), and
     * `next_row_id` advances monotonically. The retired source/delete files are NOT physically
     * removed here; time-travel still resolves them via `[begin,end)` liveness until
     * `expire_snapshots` → `cleanup_old_files` reclaims them.
     *
     * Recorded as `DeletedFromTable(sourceDataFileIds)` + `InsertedIntoTable(mergedColumnIds)`, so
     * the existing conflict machinery applies with no spec-locked changes: a concurrent commit that
     * end-snapshotted any source between the caller's read and this commit aborts non-retryably
     * (the read is stale), and a concurrent drop/alter of the table aborts too.
     *
     * [readSnapshotId] is the snapshot the caller read the source rows at. A concurrent DELETE/MERGE
     * adds a delete file to a source **without** end-snapshotting the data file, so the active-file
     * check alone would NOT catch it — the merged file (built before that delete) would resurrect
     * the deleted rows. We therefore additionally abort non-retryably if any delete file referencing
     * a source has `begin_snapshot > readSnapshotId` (a deletion newer than the caller's read). v1
     * limitation: a concurrent DuckDB-written *inlined* delete (dynamic `ducklake_inlined_delete_*`
     * table) on a source is not covered by this guard (the connector never writes inlined deletes;
     * same rarity class as the gated partial-puffin deletes).
     *
     * No-op if [sourceDataFileIds] or [fragments] is empty.
     */
    fun rewriteDataFiles(
        tableId: Long,
        sourceDataFileIds: Set<Long>,
        fragments: List<DucklakeWriteFragment>,
        readSnapshotId: Long,
    )

    /**
     * Partial-emitting ("merge_adjacent") compaction primitive — the variant that reclaims sources
     * IMMEDIATELY (dev-docs/DESIGN-maintenance.md § 7). Each [mergedFiles] entry physically carries a
     * per-row `_ducklake_internal_snapshot_id` column (each row tagged with its source file's
     * begin_snapshot), so it serves time-travel reads across `[beginSnapshot, now]` on its own; it is
     * registered **back-dated** to that entry's `beginSnapshot` with its `partialMax`. The source
     * files are **deleted from the catalog entirely** (their `ducklake_data_file` + stats +
     * delete-file + partition-value + variant-stats rows) and scheduled for physical deletion — NOT
     * end-snapshotted. Mirrors upstream `DuckLakeMetadataManager::WriteMergeAdjacent`.
     *
     * Multiple merged files cover partitioned tables (one+ per partition) and size-bounded output.
     * Contract: every source must be NON-partial (`partial_max IS NULL`) so each source's rows share
     * one origin snapshot (its begin), and the merged files together must hold exactly the live source
     * rows (row-count-preserving). Validated up front (active-source + no-newer-delete since
     * [readSnapshotId]); a concurrent commit touching a source aborts non-retryably. No-op if either
     * argument is empty.
     */
    fun rewriteDataFilesPartial(
        tableId: Long,
        sourceDataFileIds: Set<Long>,
        mergedFiles: List<PartialMergedFile>,
        readSnapshotId: Long,
    )

    // ==================== View operations ====================

    /**
     * List all views in a schema at the given snapshot
     */
    fun listViews(schemaId: Long, snapshotId: Long): List<DucklakeView>

    /**
     * Get a specific view by name at the given snapshot
     */
    fun getView(schemaName: String, viewName: String, snapshotId: Long): DucklakeView?

    /**
     * Create a new view in the catalog.
     * Creates a new snapshot and inserts the view row atomically.
     */
    fun createView(schemaName: String, viewName: String, sql: String, dialect: String, viewMetadata: String?)

    /**
     * Rename an existing view, optionally moving it across schemas.
     * Preserves view identity (view_id/view_uuid) and creates a new snapshot atomically.
     */
    fun renameView(sourceSchemaName: String, sourceViewName: String, targetSchemaName: String, targetViewName: String)

    /**
     * Replace active view metadata while preserving view identity.
     * Used for view comment and column-comment updates.
     */
    fun replaceViewMetadata(schemaName: String, viewName: String, sql: String, dialect: String, viewMetadata: String?)

    /**
     * Drop an existing view from the catalog.
     * Creates a new snapshot and sets end_snapshot on the view row atomically.
     */
    fun dropView(schemaName: String, viewName: String)

    /**
     * Close any resources (JDBC connections, etc)
     */
    fun close()
}
