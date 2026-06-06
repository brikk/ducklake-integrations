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
     * Get the `file_format` of the most recent active data file for this table at the given
     * snapshot. Used by INSERT (and the insert leg of MERGE/UPDATE) to inherit the format of the
     * existing data when neither a session property nor a statement-level override is in play.
     * Returns empty when the table has no data files yet.
     */
    fun getLatestDataFileFormat(tableId: Long, snapshotId: Long): String?

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
     * Read inlined data rows for a table at a given snapshot.
     * Queries ducklake_inlined_data_{tableId}_{schemaVersion} with snapshot filtering.
     * Returns raw JDBC values for each row, one List per row, ordered by column.
     */
    fun readInlinedData(tableId: Long, schemaVersion: Long, snapshotId: Long, columns: List<DucklakeColumn>): List<List<Any?>>

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
     */
    fun createTable(
        schemaName: String,
        tableName: String,
        columns: List<TableColumnSpec>,
        partitionSpec: List<PartitionFieldSpec>?,
        location: TableLocationSpec?,
    )

    /**
     * Drop a table. Sets end_snapshot on the table and all its columns,
     * data files, delete files, and partition info.
     * Creates a new snapshot atomically.
     */
    fun dropTable(schemaName: String, tableName: String)

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
