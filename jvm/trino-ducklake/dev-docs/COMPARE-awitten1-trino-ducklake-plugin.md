# Comparison: awitten1/trino-ducklake vs brikk/trino-ducklake

Date: 2026-04-23

This document compares the two independent Trino DuckLake connector implementations. The
awitten1 plugin source was pulled into `ducklake-other-trino-plugin/` for analysis. The
brikk plugin is the main implementation in this repository.

## Build & Dependency Summary

| | awitten1 | brikk (ours) |
|---|---|---|
| Trino version | 479 | 480 |
| DuckDB JDBC | 1.5.0.0 | 1.5.2.0 (DuckLake 1.0) |
| DuckLake spec targeted | 0.3 (inferred) | 1.0 |
| Java target | 21 | 25 |
| Source files (main) | 35 | ~66 (40 core + 26 catalog) |
| Source files (test) | 6 classes + 1 helper | 20+ classes |

## Feature Comparison

| Feature | awitten1 | brikk | Notes |
|---------|----------|---------|-------|
| **Read Path** | | | |
| Parquet file reads | Yes | Yes | Both use Trino's native ParquetReader |
| Delete file handling (merge-on-read) | Yes | Yes | Both write/read parquet positional deletes |
| Inlined data | **No** | Yes | awitten1 cannot read DuckLake tables with ≤10 rows (the default inlining threshold). Small tables created by DuckDB return zero rows. |
| Schema evolution (ADD/DROP/RENAME COLUMN) | Minimal | Full | awitten1 filters columns by snapshot but does not handle dropped/renamed columns across data files. brikk uses field_id-based column matching. |
| Time travel (FOR VERSION/TIMESTAMP AS OF) | **No** (throws NOT_SUPPORTED) | Yes | brikk supports snapshot ID and temporal lookups with session/catalog pinning |
| Partition pruning | **No** | Yes | brikk eliminates whole files via partition value matching |
| File-level stats pruning | Yes (numeric, date, varchar, boolean) | Yes (all types) | awitten1 skips timestamp pruning |
| Row-group pruning (Parquet footer) | Yes | Yes | |
| Dynamic filters | **No** | Yes | brikk intersects dynamic filters with file stats at page source creation |
| Complex types (ARRAY, ROW, MAP) | **VARCHAR fallback** | Full native support | awitten1 returns arrays/structs/maps as VARCHAR strings. brikk maps to native Trino ARRAY/ROW/MAP with full nesting. |
| Views | **No** | Yes | brikk supports CREATE/DROP/RENAME VIEW with dialect-aware storage |
| Metadata tables ($files, $snapshots, etc.) | **No** | Yes | |
| **Write Path** | | | |
| INSERT / CTAS | Yes | Yes | |
| DELETE | Yes | Yes | Both write parquet positional delete files |
| UPDATE | Yes | Yes | |
| MERGE INTO | Partial | Full | brikk supports WHEN MATCHED THEN UPDATE/DELETE + WHEN NOT MATCHED THEN INSERT in one atomic snapshot |
| CREATE/DROP SCHEMA | Yes | Yes | |
| CREATE/DROP TABLE | Yes | Yes | |
| CREATE/DROP VIEW | **No** | Yes | |
| ALTER TABLE (ADD/DROP/RENAME COLUMN) | **No** | Yes | brikk implements full schema evolution via snapshot-versioned column metadata |
| Partitioned writes | **No** | Yes | brikk supports identity + temporal transforms (YEAR/MONTH/DAY/HOUR), calendar and epoch encoding |
| Cross-engine Parquet field_id | Yes | Yes | Both annotate Parquet files with field_id matching DuckLake column_id |
| Concurrent conflict detection | **No** | Yes | brikk enforces snapshot lineage checks; aborts on stale-base conflicts with diagnostic messages |
| **Partitioning** | | | |
| Identity partitions | **No** | Yes | |
| Temporal partitions (YEAR/MONTH/DAY/HOUR) | **No** | Yes | |
| Bucket partitions | No | No | Neither; planned for brikk |
| Partition pruning on reads | **No** | Yes | |
| **Type System** | | | |
| Primitives (bool, int, float, decimal, varchar, varbinary, date, time, timestamp, timestamptz, uuid) | Yes | Yes | |
| Unsigned integers (uint8/16/32/64) | Yes | Yes | Both use next-larger-signed-type / DECIMAL(20,0) |
| int128 / HUGEINT | Yes → DECIMAL(38,0) | Yes → DECIMAL(38,0) | Both accept the overflow risk for the narrow ±1.7e38..±10^38 edge band. Spec canonical name is `int128` (not `hugeint`). |
| uint128 / UHUGEINT | Not mapped | Yes → VARCHAR (degraded) | brikk degrades to VARCHAR — Trino caps decimal at 38 digits and ≥39-digit unsigned integers cannot be represented numerically. |
| ARRAY / LIST | VARCHAR fallback | Native ARRAY | |
| STRUCT | VARCHAR fallback | Native ROW | |
| MAP | VARCHAR fallback | Native MAP | |
| Nested complex types | VARCHAR fallback | Full recursive nesting | |
| JSON | VARCHAR | VARCHAR | Both degrade |
| Variant | Not mapped | VARCHAR (degraded) | |
| Geometry types | Not mapped | VARBINARY (degraded) | |
| Interval | VARCHAR | VARCHAR | Both degrade |
| **Statistics** | | | |
| Table row count | Yes | Yes | |
| Column min/max (table-level) | Yes | Yes | |
| Column min/max (file-level, for pruning) | Yes | Yes | |
| Conservative stats for delete-file snapshots | **No** | Yes | brikk returns unknown stats when delete files are present to avoid misleading the optimizer |
| Conservative stats for mixed inline+Parquet | **No** | Yes | |
| Conservative stats for schema-evolved columns | **No** | Yes | brikk suppresses stats when coverage is incomplete |
| **Infrastructure** | | | |
| Catalog backends | SQLite, DuckDB, PostgreSQL | PostgreSQL | awitten1 supports 3 backends; brikk is PostgreSQL-focused (others removed to reduce noise during development) |
| Connection management | Direct driver instantiation | HikariCP connection pool | brikk uses pooled connections for production workloads |
| Catalog architecture | Monolithic DuckLakeClient (1,443 lines) | Interface + JDBC impl (DucklakeCatalog + JdbcDucklakeCatalog) | brikk separates interface from implementation |

## Architecture Differences

### Catalog Layer

**awitten1**: Single `DuckLakeClient.java` (1,443 lines) containing all SQL queries and catalog
logic. Direct JDBC connections via driver instantiation (`new org.sqlite.JDBC().connect()`).
Supports SQLite, DuckDB, and PostgreSQL backends in one class.

**brikk**: Separated into `DucklakeCatalog` interface and `JdbcDucklakeCatalog` implementation.
HikariCP connection pooling. PostgreSQL-only (other backends were previously supported but removed
to reduce scope during development). Write operations use `DucklakeWriteTransaction` for atomic
multi-statement commits.

### Page Source Pipeline

**awitten1**: Wrapping chain pattern — four separate classes composed at read time:
1. `DuckLakeParquetPageSource` (base Parquet reader)
2. `DuckLakeDeleteFilteringPageSource` (masks deleted rows)
3. `DuckLakeConstraintFilteringPageSource` (applies domain filters)
4. `DuckLakeProjectionPageSource` (column reordering)

**brikk**: More integrated approach — delete filtering and projection handled within the page
source provider, with dynamic filter intersection at split creation time.

### Connection Management

**awitten1**: Opens a new JDBC connection per operation via direct driver instantiation. This
avoids classloader issues with `DriverManager` but creates/closes connections frequently.

**brikk**: Uses HikariCP connection pooling with configurable pool sizes. Better for production
workloads with concurrent queries.

## Test Suite Comparison

| | awitten1 | brikk |
|---|---|---|
| Total test methods | 68 | 429 |
| Total test code (lines) | ~1,360 | ~10,000+ |
| Test classes | 6 + 1 helper | 20+ |
| Requires Docker | **No** (SQLite in-process) | Yes (PostgreSQL via Testcontainers) |

### Test Category Breakdown

| Category | awitten1 | brikk |
|----------|----------|---------|
| DDL (CREATE/DROP/ALTER) | 4 tests | 20 tests (12 DDL + 8 ALTER TABLE) |
| SQL query (SELECT, WHERE, aggregates) | 18 tests | 145 end-to-end SQL tests |
| Type mapping unit tests | 14 tests | Covered within integration tests |
| Handle serialization | 5 tests | Covered within integration tests |
| Connection management | 7 tests | N/A (HikariCP handles this) |
| Client API unit tests | 18 tests | Covered in catalog tests |
| Cross-engine (Trino writes → DuckDB reads) | 6 tests (SQLite backend) | 12 tests (PostgreSQL backend) |
| Partition computation | 0 | 18 tests (calendar + epoch) |
| Schema evolution | 0 | 8 tests (ADD/DROP/RENAME) |
| Delete/Update/Merge | 2 tests (basic) | 25 tests (15 DELETE, 5 UPDATE, 5 MERGE) |
| Write path | ~4 tests | 25 tests + 5 fragment tests |
| Statistics extraction | 0 | 12 tests |
| Parquet schema builder | 0 | 6 tests |
| NULL handling | None explicit | Dedicated test tables with NULLs |
| Inlined data | 0 | Multiple (including mixed inline+Parquet) |
| Concurrency | 0 | Yes |
| Empty tables | 1 test | Yes |

### Test Infrastructure

**awitten1**: Uses SQLite as the metadata backend for all tests. Tests run in-process without
Docker. `DuckLakeQueryRunner` initializes SQLite with full DuckLake schema DDL (30 tables).
Cross-engine tests use DuckDB JDBC to read Trino-written data.

**brikk**: Uses PostgreSQL via Testcontainers (requires Docker). `DucklakeQueryRunner` creates
a PostgreSQL container and bootstraps the catalog using DuckDB's ducklake extension (which
creates the schema tables). Cross-engine tests use DuckDB JDBC with the ducklake extension
to read from the same PostgreSQL catalog.

The awitten1 approach is faster to run (no Docker startup), but the brikk approach tests
against the actual production backend and uses DuckDB's own schema initialization rather than
hand-maintained DDL.

## Correctness Concerns in awitten1

### 1. Inlined data not handled

DuckLake inlines tables with ≤10 rows by default (`DATA_INLINING_ROW_LIMIT`). The awitten1
plugin only reads from `ducklake_data_file` — it never queries `ducklake_inlined_data_*` tables.
Any small table created by DuckDB with default settings will appear empty when queried through
Trino via this plugin.

### 2. Complex types as VARCHAR

Arrays, structs, and maps are returned as VARCHAR strings. Queries like
`SELECT my_array[1]` or `SELECT my_struct.field` will fail. Nested data is accessible only
as raw text.

### 3. No partition awareness

If DuckDB creates a partitioned table, the awitten1 plugin will still try to read it, but:
- No partition pruning (full table scans always)
- Partition metadata is not validated or used
- File paths under partition subdirectories should still resolve correctly

### 4. No concurrent write safety

No snapshot lineage validation on commits. Two concurrent writers can create conflicting
snapshots without detection.

### 5. Statistics not conservative

Table/column statistics are returned regardless of delete files, mixed inline+Parquet data,
or schema evolution. This can mislead the Trino query optimizer into suboptimal plans
(e.g., choosing wrong join sides based on stale row counts after deletes).

## Things awitten1 Has That We Don't (Yet)

### 1. SQLite and DuckDB catalog backends

Useful for local development and single-user workflows. We previously supported these but
removed them to reduce scope. Planned to re-add once the core implementation is stable.

### 2. Separate page source wrapper classes

Their wrapping chain (base → delete filter → constraint filter → projection) is arguably more
composable and easier to test in isolation. Whether this is better or worse depends on
performance characteristics and maintenance preferences.

## Developer Experience & DevOps

awitten1 has a polished local-dev setup that's worth noting. We don't have any of this yet.

### run-trino.sh

Shell script that downloads Trino 479 (if not cached), builds the plugin, installs it, generates
`etc/` config files, and starts a single-node Trino server on port 8080. Supports 4 modes:
`sqlite` (default), `postgres`, `s3`, `gcs`. One command to go from checkout to running server.

### docker-compose.yml (multi-profile)

Three Docker Compose profiles:

- **`sqlite`**: DuckDB init container creates SQLite metadata + checkpoint → Trino container
  with local FS. Shared volume for data.
- **`postgres`**: PostgreSQL 16 + DuckDB init container (loads ducklake extension, attaches to
  PG) → Trino. Shared volumes for data.
- **`minio`**: PostgreSQL + MinIO (S3-compatible, web console on 9001) + DuckDB init with AWS
  extensions → Trino with S3 FS mode. Full lakehouse-on-laptop setup.

Usage: `docker compose --profile [sqlite|postgres|minio] up`

### Dockerfile (2-stage)

Stage 1: OpenJDK 25 → Maven build with dependency caching.
Stage 2: Trino 479 base image → copy plugin. `docker-entrypoint.sh` generates catalog
properties at runtime from environment variables (`METADATA_CONNECTION_STRING`, `FS_MODE`,
`S3_REGION`, etc.).

### generate-ducklake-database.sh

Creates a test DuckLake catalog with sample data (2000 rows, 100 deletes) using DuckDB CLI.
Supports sqlite/postgres/s3 modes.

### CI

GitHub Actions: Java 21, build + full test suite on Ubuntu.

### What we should consider for our own dev setup

We should eventually build a docker-compose that sets up:
- PostgreSQL (our catalog backend)
- Trino (with our plugin installed)
- Optionally MinIO (for S3 testing)

Their compose setup is a reasonable reference for the structure. The DuckDB init container
pattern (use DuckDB to bootstrap the DuckLake schema in PostgreSQL, then hand off to Trino)
is the right approach since we don't maintain our own schema DDL — we let the ducklake
extension create the tables.

Not urgent — Testcontainers handles this for tests today. But useful for manual dev/demo
workflows and for anyone wanting to try the connector without building from source.
