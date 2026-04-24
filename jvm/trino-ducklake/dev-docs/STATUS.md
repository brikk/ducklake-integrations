# Ducklake Connector Status

Last updated: 2026-04-15

## Read Side — Complete

The read path is fully implemented and tested.

### Data Access
- Catalog reads from Ducklake SQL metadata tables (JDBC/HikariCP, PostgreSQL backend).
- Snapshot-scoped reads for current snapshot and table-version queries (`FOR VERSION AS OF`, `FOR TIMESTAMP AS OF`), with optional session/catalog pinning.
- Parquet data files read through Trino's native Parquet reader.
- Inlined data read directly from the metadata catalog (DuckLake's default for tables with <=10 rows).
- Merge-on-read delete file filtering.
- Schema evolution: missing columns return NULLs.

### Query Optimization
- **File-level pruning**: eliminates whole Parquet files via `ducklake_file_column_stats` min/max.
- **Partition pruning**: identity and temporal transforms via `ducklake_partition_info` / `ducklake_file_partition_value`.
- **Row-group pruning**: Parquet footer statistics checked via `getFilteredRowGroups()`.
- **Page-level filtering**: Parquet page indexes passed to `ParquetReader` when available.
- **Dynamic filters**: intersected with file stats domain at page source creation.
- **Table statistics**: exposed via `ducklake_table_stats` + typed aggregated column min/max, with conservative guards to prefer unknown over wrong values for delete-file snapshots, mixed inline+Parquet snapshots, and schema-evolved columns with incomplete stats coverage.
- `applyFilter` planner hook splits predicates into enforced (partition) and unenforced (engine-verified).

### Type Support
- Full: boolean, tinyint, smallint, integer, bigint, real, double, decimal, varchar, varbinary, date, time, timestamp, timestamptz, uuid.
- Full: arrays, structs/rows, maps, nested combinations.
- Degraded: `json` -> VARCHAR, `variant` -> VARCHAR, geometry family -> VARBINARY.
  These types are readable but lack type-specific operators and functions.

### Test Coverage
- 385 tests, 0 failures, 7 skipped across test classes.
- `TestDucklakeIntegration`: 145 end-to-end SQL tests via `DucklakeQueryRunner`.
- 15 test tables covering primitives, arrays, structs, maps, partitioning (identity/temporal/daily), schema evolution, NULLs, empty tables, delete files, multi-file scans, complex NULL patterns, and inlined data.
- Unit tests for catalog, split manager, partition pruning, page source provider, delete file handling, plugin wiring.
- All tests use PostgreSQL backend via Testcontainers (requires Docker).

## Known Gaps and Concerns

### Temporal partition transform values (open issue)
DuckDB's ducklake extension writes literal calendar values (e.g., year=2023, month=6) to `ducklake_file_partition_value` instead of the epoch-based values described in the spec (e.g., year=53, month=641). Our implementation supports both encodings — the write path uses the configured encoding (`ducklake.temporal-partition-encoding`, default: calendar), and the read path has a lenient mode that handles both. See [REPORT_DUCKLAKE_PARTITION_PROB.md](REPORT_DUCKLAKE_PARTITION_PROB.md) and [duckdb/ducklake-web#312](https://github.com/duckdb/ducklake-web/issues/312).

### Time travel
`FOR VERSION AS OF` and `FOR TIMESTAMP AS OF` are implemented through Trino table-version arguments in `DucklakeMetadata#getTableHandle(...)`.
Supported version pointer types:
- `TARGET_ID`: `tinyint`, `smallint`, `integer`, `bigint` (mapped to snapshot ID lookup)
- `TEMPORAL`: `date`, `timestamp`, `timestamp with time zone` (resolved to latest snapshot at or before pointer time)

Precise errors are returned for unsupported pointer types, missing snapshot IDs, and timestamps earlier than first snapshot.
Session/catalog snapshot pinning remains implemented (`ducklake.read_snapshot_id`, `ducklake.read_snapshot_timestamp`, `ducklake.default-snapshot-id`, `ducklake.default-snapshot-timestamp`) with precedence `query > session > catalog > current`.

### Data inlining
Trino now supports mixed-mode reads when a snapshot has both active Parquet files and active inlined rows: split planning emits both split types and reads them as a union. Merge-on-read delete filtering remains in place for Parquet splits, while inlined row visibility is still governed by `begin_snapshot`/`end_snapshot` filtering in metadata reads.

Stale inlined metadata pointers remain non-fatal. If metadata references an inlined table that is missing or has no active rows at the snapshot, the connector does not fail and does not emit dead inlined splits for mixed Parquet scans.

Schema evolution with inlined data now follows DuckDB behavior: Trino reads across multiple active `ducklake_inlined_data_<table_id>_<schema_version>` tables at a snapshot, instead of selecting a single schema-version table. For evolved schemas, projected columns are mapped by `column_id` and missing fields from older inline tables are returned as `NULL`.

Schema metadata alignment for inlined reads now prefers table-scoped `ducklake_schema_versions` (`table_id`, `schema_version`, `begin_snapshot`) to resolve the source schema snapshot, with backward-compatible fallback to `ducklake_snapshot.schema_version`.

### Statistics resilience
Stats policy is now intentionally conservative ("don't be wrong"):

- Snapshots with delete files return unknown table/column stats.
- Snapshots with mixed inline + Parquet rows keep row count but suppress file-derived column stats.
- For schema-evolved columns where file-level `value_count + null_count` does not cover all active data-file rows, column stats are suppressed.

Decision (2026-04-15):

- Keep strict invalidation instead of `% changed > N` heuristics. Cross-engine delete semantics and stale metadata can make threshold-based stats unsafe.
- Keep this strict mode as the default safety behavior.

Learning from Iceberg-style behavior:

- Engines can keep stale manifest/file-derived stats after row-level deletes and rely on maintenance (`OPTIMIZE`) or `ANALYZE` to refresh.
- For Ducklake interoperability, we prefer unknown over potentially wrong stats at read time.

Planned follow-up:

- Add explicit stats recomputation utilities (`recalc stats`) that rescan data and write refreshed stats into Ducklake catalog tables (PostgreSQL backend), decoupled from rewrite/merge operations.

### Degraded type semantics
`json`, `variant`, and geometry types are stored as VARCHAR/VARBINARY. No type-specific functions or operators. Variant shredding is not implemented.

### Catalog backend
PostgreSQL is the only supported catalog backend. The connector uses `JdbcDucklakeCatalog` with a generic JDBC code path that is compatible with any JDBC-compliant database, but testing and configuration are PostgreSQL-focused.

## Write Side — Data Writes Implemented

### DDL (metadata-only writes)

Implemented through snapshot-scoped catalog commits:

- Views: `CREATE VIEW`, `DROP VIEW`, `ALTER VIEW ... RENAME TO`, `COMMENT ON VIEW`, `COMMENT ON COLUMN <view>.<column>`
- Schemas: `CREATE SCHEMA`, `DROP SCHEMA` (non-empty schema drop rejected)
- Tables: `CREATE TABLE`, `DROP TABLE`
  - supports nested type metadata mapping (`ARRAY`, `ROW`, `MAP`)
  - supports partition spec parsing from `WITH (partitioned_by = ARRAY[...])`
  - `SAVE MODE REPLACE` remains unsupported

### DML (data writes)

`INSERT` and `CREATE TABLE AS SELECT` are implemented for both unpartitioned and partitioned tables:

- `DucklakePageSink` writes Parquet data files via Trino's `ParquetWriter` (ZSTD compression, configurable block/page size).
- `DucklakePageSinkProvider` handles both INSERT and CTAS through a shared `DucklakeWritableTableHandle`.
- `DucklakeStatsExtractor` extracts per-column statistics from Parquet footer metadata.
- `DucklakePathResolver` resolves table data paths from catalog/schema/table hierarchy.
- `DucklakeMetadata` implements `beginInsert`/`finishInsert` and `beginCreateTable`/`finishCreateTable`.
- `JdbcDucklakeCatalog.commitInsert()` atomically commits data file rows, file column stats, partition values, and table stats updates in a single write transaction, with batched metadata writes to reduce per-file/per-column statement overhead.
- Abort path deletes written files on failure.
- File rotation by target file size (configurable via `ParquetWriterConfig`).

### Partitioned writes

- `DucklakePagePartitioner` routes rows to per-partition writers via `PageIndexerFactory`.
- `DucklakePartitionComputer` computes partition values from Trino blocks.
- Supports identity and temporal transforms (YEAR, MONTH, DAY, HOUR).
- Supports both calendar (DuckDB-compatible, default) and epoch encoding via `ducklake.temporal-partition-encoding`.
- Partitioned files are written under subdirectories (e.g., `region=US/ducklake-uuid.parquet`).
- `ducklake_file_partition_value` rows are committed atomically with data files.
- Null partition values are supported.

### Cross-engine compatibility

Parquet files include `field_id` annotations matching DuckLake `column_id` values, enabling DuckDB to correctly map columns when reading Trino-written files.

- `DucklakeParquetSchemaBuilder` rebuilds the Parquet `MessageType` with `field_id` from catalog column IDs, including nested types (ROW, ARRAY, MAP).
- Column metadata: 1-based `column_order`, `default_value='NULL'`, `default_value_type='literal'`, `default_value_dialect='duckdb'`
- Stats: `ducklake_table_stats` created on first INSERT (not CREATE TABLE), `contains_nan` stored as NULL for non-floating types
- Footer size, schema_versions.table_id, and UUID type handling are all DuckDB-compatible
- See [REPORT_CROSS_ENGINE_WRITE.md](REPORT_CROSS_ENGINE_WRITE.md) for open spec issues filed with the DuckDB team

Cross-engine validation (Trino writes -> DuckDB reads, PostgreSQL catalog):
- Cross-engine compatibility tests pass, including full column value round-trips
- Catalog metadata operations: SHOW TABLES, DESCRIBE, row counts all correct
- Column value reads: DuckDB correctly reads all values from Trino-written Parquet files
- Inlined schema-evolution parity validated: DuckDB `4 inline -> ALTER ADD COLUMN -> 4 inline` and `9 inline -> ALTER ADD COLUMN -> 9 inline` both remain inlined (no Parquet flush), and Trino returns matching rows.

### Row-level mutations (DELETE, UPDATE, MERGE)

Implemented via Trino's merge-on-read pattern (`ConnectorMergeSink`):

- `DELETE`: writes Parquet delete files containing row IDs, committed to `ducklake_delete_file` metadata table.
  Supports multiple delete files per data file (accumulated across snapshots, merged at read time).
- `UPDATE`: implemented as atomic delete+insert in a single snapshot via `commitMerge`.
- `MERGE INTO`: supports `WHEN MATCHED THEN UPDATE/DELETE`, `WHEN NOT MATCHED THEN INSERT` — all in one atomic snapshot.
- `$row_id` synthetic column injected by `RowIdInjectingPageSource` for row identification during merge scans.
- `DucklakeMergeSink` groups deleted row IDs by data file, writes per-file Parquet delete files.
- Snapshot changes use spec-compliant comma-separated format (one row per snapshot).

### ALTER TABLE (schema evolution)

Snapshot-versioned column modifications via `ducklake_column` begin/end_snapshot:

- `ALTER TABLE ADD COLUMN`: inserts new `ducklake_column` row with new `column_id`. Old data files return NULLs for the new column (schema evolution on read). Supports nested types.
- `ALTER TABLE DROP COLUMN`: sets `end_snapshot` on the column and its children. Old data files retain the dropped column data (ignored on read).
- `ALTER TABLE RENAME COLUMN`: end-snapshots current row, inserts new row with same `column_id` but new name. Field_id-based column matching in the page source ensures renamed columns map correctly to existing Parquet files.

All operations increment schema version and record `altered_table:<id>` in snapshot changes.

### Concurrency / conflict handling

- Write commits now enforce strict optimistic snapshot lineage checks: if `max(snapshot_id)` advanced since the transaction started, commit aborts.
- Snapshot ID insert races are translated to `TRANSACTION_CONFLICT` instead of surfacing raw duplicate-key SQL errors.
- Conflict messages include intervening `ducklake_snapshot_changes` (when present) to make stale-base diagnosis explicit.

### Not yet implemented

- `ALTER TABLE SET TYPE` (type promotion)
- `ALTER TABLE ADD/DROP FIELD` (nested struct field manipulation)
- Commit-failure file cleanup (abort cleanup exists; orphaned files from commit failures are cleaned by DuckLake's `ducklake_delete_orphaned_files()` maintenance procedure)
- Maintenance operations (optimize, rewrite, expire snapshots, etc.)
- Unsigned integer range validation for cross-engine writes (DuckLake uint* types mapped to larger signed Trino types; no overflow check when writing back)

### Test coverage

- All tests use **PostgreSQL** backend (via Testcontainers, requires Docker)
- DDL + ALTER: `TestDucklakeDDLIntegration` (20 tests: 12 DDL + 8 ALTER TABLE)
- Data writes: `TestDucklakeWriteIntegration` (25 tests, including 6 partitioned write tests), `TestDucklakeWriteFragment` (5 tests), `TestDucklakeStatsExtractor` (12 tests)
- Row-level mutations: `TestDucklakeDeleteIntegration` (25 tests: 15 DELETE, 5 UPDATE, 5 MERGE)
- Cross-engine: `TestDucklakeCrossEngineCompatibility` (10 tests, all enabled)
- Parquet schema: `TestDucklakeParquetSchemaBuilder` (6 tests)
- Partition computation: `TestDucklakePartitionComputer` (18 tests, both calendar/epoch encodings)
- Total: 418 tests pass, 0 failures, 7 skipped
