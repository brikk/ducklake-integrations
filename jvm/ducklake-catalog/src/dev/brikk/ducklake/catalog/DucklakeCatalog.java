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
package dev.brikk.ducklake.catalog;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for Ducklake SQL-based catalog operations.
 * Abstracts access to the 28 Ducklake metadata tables.
 */
public interface DucklakeCatalog
{
    /**
     * Get the current (maximum) snapshot ID
     */
    long getCurrentSnapshotId();

    /**
     * Get a specific snapshot ID (for time travel queries)
     */
    Optional<DucklakeSnapshot> getSnapshot(long snapshotId);

    /**
     * Get the latest snapshot with snapshot_time <= timestamp.
     */
    Optional<DucklakeSnapshot> getSnapshotAtOrBefore(Instant timestamp);

    /**
     * List all snapshots in the catalog, newest first.
     */
    List<DucklakeSnapshot> listSnapshots();

    /**
     * List all snapshot change records, newest first.
     */
    List<DucklakeSnapshotChange> listSnapshotChanges();

    /**
     * List all schemas visible at the given snapshot
     */
    List<DucklakeSchema> listSchemas(long snapshotId);

    /**
     * Get a specific schema by name at the given snapshot
     */
    Optional<DucklakeSchema> getSchema(String schemaName, long snapshotId);

    /**
     * List all tables in a schema at the given snapshot
     */
    List<DucklakeTable> listTables(long schemaId, long snapshotId);

    /**
     * Get a specific table by name at the given snapshot
     */
    Optional<DucklakeTable> getTable(String schemaName, String tableName, long snapshotId);

    /**
     * Get table by ID at the given snapshot
     */
    Optional<DucklakeTable> getTableById(long tableId, long snapshotId);

    /**
     * Get top-level columns for a table at the given snapshot (nested types resolved into type strings).
     */
    List<DucklakeColumn> getTableColumns(long tableId, long snapshotId);

    /**
     * Get all columns (including nested) for a table at the given snapshot as a flat list.
     * Each column retains its {@code parentColumn} reference. Used for Parquet field_id mapping.
     */
    List<DucklakeColumn> getAllColumnsWithParentage(long tableId, long snapshotId);

    /**
     * Get data files for a table at the given snapshot
     */
    List<DucklakeDataFile> getDataFiles(long tableId, long snapshotId);

    /**
     * Find data file IDs whose column statistics overlap with the given range.
     * Used for predicate pushdown — files outside the range are pruned.
     */
    List<Long> findDataFileIdsInRange(long tableId, long snapshotId, ColumnRangePredicate predicate);

    /**
     * Get table-level statistics (record count, file size) from ducklake_table_stats
     */
    Optional<DucklakeTableStats> getTableStats(long tableId);

    /**
     * Get aggregated column statistics across all active data files.
     * Min/max values are compared using typed comparison (numeric, not lexicographic)
     * based on column types resolved internally.
     */
    List<DucklakeColumnStats> getColumnStats(long tableId, long snapshotId);

    /**
     * Get partition specs for a table at the given snapshot
     */
    List<DucklakePartitionSpec> getPartitionSpecs(long tableId, long snapshotId);

    /**
     * Get partition values for all active data files of a table at the given snapshot
     */
    Map<Long, List<DucklakeFilePartitionValue>> getFilePartitionValues(long tableId, long snapshotId);

    /**
     * List inlined data table descriptors for a table at the given snapshot.
     * A table can have multiple inlined data tables (one per schema_version).
     */
    List<DucklakeInlinedDataInfo> getInlinedDataInfos(long tableId, long snapshotId);

    /**
     * Check if an inlined data table has any live rows at a given snapshot.
     */
    boolean hasInlinedRows(long tableId, long schemaVersion, long snapshotId);

    /**
     * Read inlined data rows for a table at a given snapshot.
     * Queries ducklake_inlined_data_{tableId}_{schemaVersion} with snapshot filtering.
     * Returns raw JDBC values for each row, one List per row, ordered by column.
     */
    List<List<Object>> readInlinedData(long tableId, long schemaVersion, long snapshotId, List<DucklakeColumn> columns);

    /**
     * Get the base data path from ducklake_metadata
     */
    Optional<String> getDataPath();

    // ==================== Schema DDL ====================

    /**
     * Create a new schema. Creates a new snapshot atomically.
     */
    void createSchema(String schemaName);

    /**
     * Drop a schema. Fails if the schema contains tables.
     * Creates a new snapshot atomically.
     */
    void dropSchema(String schemaName);

    // ==================== Table DDL ====================

    /**
     * Create a new empty table with columns and optional partition spec.
     * Creates a new snapshot atomically.
     */
    void createTable(String schemaName, String tableName,
            List<TableColumnSpec> columns,
            Optional<List<PartitionFieldSpec>> partitionSpec);

    /**
     * Drop a table. Sets end_snapshot on the table and all its columns,
     * data files, delete files, and partition info.
     * Creates a new snapshot atomically.
     */
    void dropTable(String schemaName, String tableName);

    /**
     * Add a column to a table. Creates a new ducklake_column row with a new column_id.
     * Increments schema version. Creates a new snapshot atomically.
     */
    void addColumn(long tableId, TableColumnSpec column);

    /**
     * Drop a column from a table. Sets end_snapshot on the column's current row.
     * Increments schema version. Creates a new snapshot atomically.
     */
    void dropColumn(long tableId, long columnId);

    /**
     * Rename a column. End-snapshots the current column row and inserts a new row
     * with the same column_id but updated column_name.
     * Increments schema version. Creates a new snapshot atomically.
     */
    void renameColumn(long tableId, long columnId, String newName);

    /**
     * Commit inserted data files to a table.
     * Creates a new snapshot with data file rows, file column stats,
     * and updated table stats.
     */
    void commitInsert(long tableId, List<DucklakeWriteFragment> fragments);

    /**
     * Commit delete files for a table.
     * Creates a new snapshot with ducklake_delete_file rows and updated table stats.
     */
    void commitDelete(long tableId, List<DucklakeDeleteFragment> deleteFragments);

    /**
     * Atomically commit both delete files and inserted data files in a single snapshot.
     * Used for UPDATE (delete old rows + insert new rows) and MERGE operations.
     */
    void commitMerge(long tableId,
            List<DucklakeDeleteFragment> deleteFragments, List<DucklakeWriteFragment> insertFragments);

    // ==================== View operations ====================

    /**
     * List all views in a schema at the given snapshot
     */
    List<DucklakeView> listViews(long schemaId, long snapshotId);

    /**
     * Get a specific view by name at the given snapshot
     */
    Optional<DucklakeView> getView(String schemaName, String viewName, long snapshotId);

    /**
     * Create a new view in the catalog.
     * Creates a new snapshot and inserts the view row atomically.
     */
    void createView(String schemaName, String viewName, String sql, String dialect, String viewMetadata);

    /**
     * Rename an existing view, optionally moving it across schemas.
     * Preserves view identity (view_id/view_uuid) and creates a new snapshot atomically.
     */
    void renameView(String sourceSchemaName, String sourceViewName, String targetSchemaName, String targetViewName);

    /**
     * Replace active view metadata while preserving view identity.
     * Used for view comment and column-comment updates.
     */
    void replaceViewMetadata(String schemaName, String viewName, String sql, String dialect, String viewMetadata);

    /**
     * Drop an existing view from the catalog.
     * Creates a new snapshot and sets end_snapshot on the view row atomically.
     */
    void dropView(String schemaName, String viewName);

    /**
     * Close any resources (JDBC connections, etc)
     */
    void close();
}
