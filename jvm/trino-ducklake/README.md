# Trino DuckLake Connector — Feature Support

Connector for the [DuckLake](https://ducklake.select) open table format (spec v1.0).
Supports PostgreSQL as the catalog metadata backend, with SQLite and DuckDB backends
planned for local development and testing workflows.

Tested with DuckDB 1.5.2 for cross-engine compatibility.

The [DuckLake spec](ducklake-web/docs/stable/specification/) is included as a submodule.
All documentation and feature tables in this module are current against that version of the spec.

## Build

Requires Java 25. Install via [SDKMan](https://sdkman.io/install/):

```shell
cd jvm
sdk env install
sdk env
```

Build the plugin:

```shell
./gradlew :trino-ducklake:pluginAssemble
```

The assembled plugin is at `trino-ducklake/build/trino-plugin/trino-ducklake-<version>/`.

### Running Tests

Tests require Docker or Podman (PostgreSQL runs via TestContainers).

```shell
./gradlew :trino-ducklake:test
```

## Install

Copy the assembled plugin directory into your Trino installation's plugin directory:

```bash
cp -r trino-ducklake/build/trino-plugin/trino-ducklake-<version> /usr/lib/trino/plugin/ducklake
```

The plugin directory should contain all JARs flat (not nested in a subdirectory):

```
/usr/lib/trino/plugin/ducklake/
  io.trino_trino-ducklake-<version>.jar
  ...other jars...
```

Create a catalog properties file at `etc/catalog/ducklake.properties`:

```properties
connector.name=ducklake

# DuckLake metadata catalog database (PostgreSQL)
ducklake.catalog.database-url=jdbc:postgresql://<host>:5432/ducklake
ducklake.catalog.database-user=ducklake
ducklake.catalog.database-password=<password>

# Base location for data files (local path or S3)
ducklake.data-path=s3://<bucket>/<prefix>/

# Optional tuning
ducklake.catalog.max-connections=10

# Temporal partition behavior (defaults shown)
ducklake.temporal-partition-encoding=calendar
ducklake.temporal-partition-encoding-read-leniency=true
```

For S3 storage, add to the same catalog properties file:

```properties
fs.native-s3.enabled=true
s3.region=<region>
```

Restart Trino for the new plugin and catalog to take effect.

## Type System

| DuckLake Type | Trino Type | Read | Write | Notes |
|---------------|------------|:----:|:-----:|-------|
| `boolean` | BOOLEAN | Yes | Yes | |
| `int8` (tinyint) | TINYINT | Yes | Yes | |
| `int16` (smallint) | SMALLINT | Yes | Yes | |
| `int32` (integer) | INTEGER | Yes | Yes | |
| `int64` (bigint) | BIGINT | Yes | Yes | |
| `uint8` | SMALLINT | Yes | Yes | Widened on read; writes outside 0..255 are rejected |
| `uint16` | INTEGER | Yes | Yes | Widened on read; writes outside 0..65535 are rejected |
| `uint32` | BIGINT | Yes | Yes | Widened on read; writes outside 0..2^32-1 are rejected |
| `uint64` | DECIMAL(20,0) | Yes | Yes | Widened on read; writes outside 0..2^64-1 are rejected |
| `float32` | REAL | Yes | Yes | |
| `float64` | DOUBLE | Yes | Yes | |
| `decimal(p,s)` | DECIMAL(p,s) | Yes | Yes | Precision up to 38 |
| `varchar` | VARCHAR | Yes | Yes | |
| `blob` | VARBINARY | Yes | Yes | |
| `uuid` | UUID | Yes | Yes | |
| `date` | DATE | Yes | Yes | |
| `time` | TIME(6) | Yes | Yes | Microsecond precision |
| `timetz` | TIME WITH TIME ZONE | Yes | Yes | Microsecond precision |
| `timestamp` | TIMESTAMP(6) | Yes | Yes | Microsecond precision |
| `timestamp_s` | TIMESTAMP(0) | Yes | Yes | Second precision |
| `timestamp_ms` | TIMESTAMP(3) | Yes | Yes | Millisecond precision |
| `timestamp_ns` | TIMESTAMP(9) | Yes | Yes | Nanosecond precision |
| `timestamptz` | TIMESTAMP WITH TIME ZONE | Yes | Yes | Microsecond precision |
| `list<T>` | ARRAY(T) | Yes | Yes | Full nesting supported |
| `struct<...>` | ROW(...) | Yes | Yes | Full nesting supported |
| `map<K,V>` | MAP(K,V) | Yes | Yes | Full nesting supported |
| `json` | VARCHAR | Yes | Yes | Degraded — stored as string, no JSON functions |
| `variant` | VARCHAR | Yes | Yes | Degraded — no shredding or field access |
| `interval` | VARCHAR | Yes | Yes | Degraded — stored as string |
| `geometry` | VARBINARY | Yes | Yes | Degraded — no spatial functions |
| `point` | VARBINARY | Yes | Yes | Degraded |
| `linestring` | VARBINARY | Yes | Yes | Degraded |
| `linestring_z` | VARBINARY | Yes | Yes | Degraded — DuckLake 1.0 name; legacy pre-1.0 form `linestring z` also accepted |
| `polygon` | VARBINARY | Yes | Yes | Degraded |
| `multipoint` | VARBINARY | Yes | Yes | Degraded |
| `multilinestring` | VARBINARY | Yes | Yes | Degraded |
| `multipolygon` | VARBINARY | Yes | Yes | Degraded |
| `geometrycollection` | VARBINARY | Yes | Yes | Degraded |
| `int128` | DECIMAL(38,0) | Yes | Yes | Values beyond ±10^38 overflow the decimal and will error |
| `uint128` | VARCHAR | Yes | Yes | Degraded — Trino caps decimal at 38 digits; preserved as text, no numeric ops |

"Degraded" means data is fully preserved and round-trips correctly, but type-specific
operators and functions are not available through Trino.

## Read Operations

| Feature | Supported | Notes |
|---------|:---------:|-------|
| SELECT / table scans | Yes | |
| Predicate pushdown (WHERE) | Yes | All types |
| File-level pruning (min/max stats) | Yes | Eliminates whole Parquet files |
| Partition pruning | Yes | Identity and temporal partitions |
| Row-group pruning (Parquet footer) | Yes | Uses Parquet internal statistics |
| Page-level filtering (Parquet page index) | Yes | |
| Dynamic filter pushdown | Yes | Intersected with file-level stats |
| Parquet data files | Yes | Via Trino's native Parquet reader |
| Parquet footer-size hint | Yes | Uses `ducklake_data_file.footer_size` / `ducklake_delete_file.footer_size` to skip Trino's default 48 KB blind footer read |
| Inlined data (small tables) | Yes | Reads from catalog metadata tables |
| Mixed inline + Parquet snapshots | Yes | Both sources unioned transparently |
| Delete files (merge-on-read) | Yes | Parquet positional delete files |
| Delete-file evolution across snapshots | Yes | Reads the currently-valid delete file per data file at the active snapshot (spec: at most one delete file per data file per snapshot) |
| Schema evolution on read | Yes | Missing columns return NULL |
| Time travel — FOR VERSION AS OF | Yes | By snapshot ID |
| Time travel — FOR TIMESTAMP AS OF | Yes | By timestamp |
| Snapshot pinning (session) | Yes | `read_snapshot_id`, `read_snapshot_timestamp` |
| Snapshot pinning (catalog) | Yes | `ducklake.default-snapshot-id`, `ducklake.default-snapshot-timestamp` |
| Table statistics | Yes | Row count + column min/max from catalog |
| Metadata tables (`$files`) | Yes | Inspect data files for a table |
| Metadata tables (`$snapshots`) | Yes | List all snapshots |
| Metadata tables (`$current_snapshot`) | Yes | Current snapshot info |
| Metadata tables (`$snapshot_changes`) | Yes | Snapshot audit trail |
| Views (Trino dialect) | Yes | |
| Views (other dialects) | No | Filtered out; only Trino-created views exposed |
| Puffin deletion vectors | No | Experimental in DuckLake 1.0; not yet supported |
| Bucket partition pruning | No | Planned |
| Sorted table optimizations | No | Tables are still readable; sort metadata ignored |

## Write Operations

| Feature | Supported | Notes |
|---------|:---------:|-------|
| INSERT INTO | Yes | Writes Parquet files (ZSTD compression) |
| CREATE TABLE AS SELECT | Yes | |
| DELETE | Yes | Writes Parquet positional delete files |
| UPDATE | Yes | Atomic delete + insert in one snapshot |
| MERGE INTO | Yes | WHEN MATCHED THEN UPDATE/DELETE + WHEN NOT MATCHED THEN INSERT |
| CREATE SCHEMA | Yes | |
| DROP SCHEMA | Yes | Non-empty schema drop rejected |
| CREATE TABLE | Yes | Supports nested types and partition spec |
| DROP TABLE | Yes | |
| CREATE VIEW | Yes | Stored with Trino dialect marker |
| DROP VIEW | Yes | |
| RENAME VIEW | Yes | |
| COMMENT ON VIEW | Yes | |
| COMMENT ON VIEW COLUMN | Yes | |
| ALTER TABLE ADD COLUMN | Yes | Supports nested types |
| ALTER TABLE DROP COLUMN | Yes | |
| ALTER TABLE RENAME COLUMN | Yes | Field-ID based; existing files read correctly |
| Partitioned writes | Yes | Identity and temporal transforms |
| Cross-engine Parquet compatibility | Yes | `field_id` annotations for DuckDB interop |
| Concurrent conflict detection | Yes | Snapshot lineage check; aborts on stale base |
| ALTER TABLE SET TYPE | No | Type promotion not supported |
| ALTER TABLE ADD/DROP FIELD | No | Nested struct field manipulation |
| RENAME TABLE | No | |
| RENAME SCHEMA | No | |
| COMMENT ON TABLE | No | |
| COMMENT ON COLUMN | No | |
| ANALYZE | No | Statistics are read-only from the catalog |
| Bucket partitioned writes | No | Planned |
| Sorted writes | No | Trino-written files are unsorted |

## Partitioning

| Transform | Read | Write | Notes |
|-----------|:----:|:-----:|-------|
| Identity | Yes | Yes | Partition by column value |
| `year(col)` | Yes | Yes | Date or timestamp column |
| `month(col)` | Yes | Yes | Date or timestamp column |
| `day(col)` | Yes | Yes | Date or timestamp column |
| `hour(col)` | Yes | Yes | Timestamp column |
| `bucket(N, col)` | No | No | Planned — Murmur3 hash partitioning |

Temporal partition encoding supports both calendar (DuckDB default) and epoch (spec-defined)
modes, configurable via `ducklake.temporal-partition-encoding`. The read path is lenient by
default and handles both encodings transparently.

## Statistics

| Statistic | Supported | Notes |
|-----------|:---------:|-------|
| Table row count | Yes | From `ducklake_table_stats` |
| Column min/max (table-level) | Yes | Typed parsing of string-encoded values |
| Column min/max (file-level, for pruning) | Yes | From `ducklake_file_column_stats` |
| Column null count (file-level) | Yes | Used in file pruning decisions |
| Conservative mode for deletes | Yes | Returns unknown stats when delete files are present |
| Conservative mode for mixed inline+Parquet | Yes | Row count preserved, column stats suppressed |
| Conservative mode for schema evolution | Yes | Stats suppressed when coverage is incomplete |

## Cross-Engine Compatibility

The connector is tested for bidirectional compatibility with DuckDB:

| Direction | Tested | Notes |
|-----------|:------:|-------|
| DuckDB writes, Trino reads | Yes | Full column value round-trips validated |
| Trino writes, DuckDB reads | Yes | Parquet field_id mapping ensures correct column matching |
| Shared PostgreSQL catalog | Yes | Both engines operate on the same metadata |
| Inlined data created by DuckDB | Yes | Trino reads inlined rows from catalog tables |
| Schema evolution across engines | Yes | ADD COLUMN by one engine, read by the other |

## Configuration

| Property | Required | Default | Description |
|----------|:--------:|---------|-------------|
| `ducklake.catalog.database-url` | Yes | — | JDBC URL for PostgreSQL catalog |
| `ducklake.catalog.database-user` | Yes | — | Catalog database username |
| `ducklake.catalog.database-password` | Yes | — | Catalog database password |
| `ducklake.data-path` | Yes | — | Base path for data files |
| `ducklake.catalog.max-connections` | No | 10 | Max JDBC connections to catalog |
| `ducklake.default-snapshot-id` | No | — | Pin all reads to a snapshot ID |
| `ducklake.default-snapshot-timestamp` | No | — | Pin all reads to a point in time |
| `ducklake.temporal-partition-encoding` | No | `calendar` | `calendar` or `epoch` |
| `ducklake.temporal-partition-encoding-read-leniency` | No | `true` | Accept both encodings on read |

Session properties: `read_snapshot_id`, `read_snapshot_timestamp`

Snapshot resolution precedence: query clause > session property > catalog config > current snapshot.

## Not Yet Implemented

### Maintenance Operations

Not available through Trino. Use DuckDB's ducklake extension against the shared PostgreSQL
catalog for these operations. Planned for a future milestone:

- `ALTER TABLE ... EXECUTE optimize` (merge adjacent files)
- `ALTER TABLE ... EXECUTE rewrite_data_files`
- `expire_snapshots` (connector procedure)
- `cleanup_old_files` (connector procedure)
- `remove_orphan_files` (connector procedure)
- `flush_inlined_data` (connector procedure)
- `recalc stats` (rescan data files and recompute table/column stats)

### DDL

- `RENAME TABLE`
- `RENAME SCHEMA`
- `COMMENT ON TABLE`
- `COMMENT ON COLUMN` (table columns; view column comments are supported)
- `ALTER TABLE SET TYPE` (type promotion)
- `ALTER TABLE ADD/DROP FIELD` (nested struct field manipulation)

### Commit Context

Planned session properties for annotating write snapshots (DuckDB `set_commit_message`
equivalent):

- `commit_author`
- `commit_message`
- `commit_extra_info`

### Change Feed

DuckDB equivalents not yet exposed through Trino:

- `table_changes(table, start_snapshot, end_snapshot)`
- `table_insertions(table, start_snapshot, end_snapshot)`
- `table_deletions(table, start_snapshot, end_snapshot)`

### Cross-Dialect View Transpilation

Views created by DuckDB (or other engines) are not visible in Trino. Only Trino-dialect
views are exposed. Cross-dialect transpilation (e.g., DuckDB SQL to Trino SQL) is a
research item.

### Catalog Backends

Only PostgreSQL is supported today. SQLite and DuckDB catalog backends are coming soon
for single-user, local development, and testing workflows.

## Known Limitations

- The `variant` type is readable as VARCHAR but without shredded field access
  (e.g., `payload.user` syntax is not supported). Variant statistics for shredded
  sub-fields are not used for pushdown.
- Geometry types are readable as VARBINARY but without spatial functions or bounding-box
  statistics for file pruning.
- `int128` maps to `DECIMAL(38, 0)`. Values in the narrow ±1.7e38..±10^38 edge bands exceed
  the decimal's range and will error on read.
- `uint128` is degraded to `VARCHAR`; Trino cannot represent a ≥39-digit signed-or-unsigned
  integer. Values round-trip as text; no numeric operations are available.
- Unsigned integer types (uint8/16/32/64) are widened to larger signed Trino types on
  read (uint8 → SMALLINT, uint16 → INTEGER, uint32 → BIGINT, uint64 → DECIMAL(20, 0)).
  On the write path, values outside the unsigned range of the target DuckLake column
  are rejected at INSERT time with `NUMERIC_VALUE_OUT_OF_RANGE` — e.g. a SMALLINT 300
  or a negative value written to a `uint8` column fails cleanly instead of silently
  wrapping to 44.
- Files written before a failed commit become orphans. DuckLake's
  `ducklake_delete_orphaned_files()` maintenance procedure handles cleanup.
- Puffin deletion vectors (experimental in DuckLake 1.0, opt-in) are not supported. Tables
  using `write_deletion_vectors=true` will not be readable through this connector.

## Additional Documentation

- [SAMPLES.md](SAMPLES.md) — SQL examples for DDL, DML, time travel, metadata tables, and DuckDB interop
- [dev-docs/STATUS.md](dev-docs/STATUS.md) — Detailed implementation status and test coverage
  - [TODO for READ side](dev-docs/TODO-READ-MODE.md) - Read mode issue and ideas/notes
  - [TODO for WRITE side](dev-docs/TODO-WRITE-MODE.md) - Write mode issue and ideas/notes
- [dev-docs/REUSE.md](dev-docs/REUSE.md) — What is reused from Trino/Iceberg and what is custom
- [dev-docs/DUCKLAKE_1_0_IMPACT.md](dev-docs/DUCKLAKE_1_0_IMPACT.md) — DuckLake 1.0 impact assessment
