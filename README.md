# Trino DuckLake Connector — Feature Support

Connector for the [DuckLake](https://ducklake.select) open table format (spec v1.0).
Supports PostgreSQL and MySQL 8+ (shared, multi-process) and a local DuckDB `.db`
file (single-process; ideal for dev, local interactive use, and single-node Trino
deployments) as catalog metadata backends. SQLite/Turso, and a remote-DuckDB backend
over [Quack RPC](https://duckdb.org/2026/05/12/quack-remote-protocol) for shared
DuckDB-as-catalog without filesystem mounting, are planned next. (For a catalog
shared with DuckDB itself, use PostgreSQL — DuckDB's own MySQL reader is not yet
stable; see [dev-docs/CATALOG-BACKENDS.md](dev-docs/CATALOG-BACKENDS.md).)

Tested with DuckDB 1.5.4 for cross-engine compatibility.

The [DuckLake spec](ducklake-web/docs/stable/specification/) is included as a submodule.
All documentation and feature tables in this module are current against that version of the spec.

> **Note:** This connector reads and writes **parquet data files only**. The experimental
> DuckDB/vortex/lance data-file formats and the DuckDB-engine predicate-pushdown path were
> removed and now live in the standalone **[duckbridge](https://github.com/brikk/duckbridge)**
> Trino → DuckDB connector. The `ducklake-catalog` metadata/catalog plane is unaffected.

## Install

### Download

Grab the latest plugin archive from the
[GitHub Releases](https://github.com/brikk/trino-ducklake/releases) page — each release ships
`trino-ducklake-<version>.zip` and `trino-ducklake-<version>.tar.gz` (with `.sha256` sidecars).
Both expand to a single `trino-ducklake-<version>/` directory of JARs.

```bash
# verify + unpack (example, zip)
sha256sum -c trino-ducklake-<version>.zip.sha256
unzip trino-ducklake-<version>.zip
```

(Or build it yourself — see [Building from source](#building-from-source) at the bottom.)

### Install into Trino

Copy the unpacked plugin directory into your Trino installation's plugin directory, renaming it
to `ducklake`:

```bash
cp -r trino-ducklake-<version> /usr/lib/trino/plugin/ducklake
```

The plugin directory should contain all JARs flat (not nested in a subdirectory):

```
/usr/lib/trino/plugin/ducklake/
  trino-ducklake-<version>.jar
  ...other jars...
```

### Configure a catalog

Create a catalog properties file at `etc/catalog/ducklake.properties`. Pick the
backend that matches your deployment shape.

**PostgreSQL** — shared multi-process deployment (multiple Trino workers, mixed
DuckDB + Trino access):

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
```

**MySQL 8+** — shared multi-process deployment, same shape as PostgreSQL (the
connector talks to the catalog directly over JDBC; the backend dialect is
inferred from the URL scheme):

```properties
connector.name=ducklake

# DuckLake metadata catalog database (MySQL 8+)
ducklake.catalog.database-url=jdbc:mysql://<host>:3306/ducklake
ducklake.catalog.database-user=ducklake
ducklake.catalog.database-password=<password>

# Base location for data files (local path or S3)
ducklake.data-path=s3://<bucket>/<prefix>/
```

The MySQL Connector/J driver ships with the connector, so no extra jar is
needed. The MySQL database must already contain a DuckLake metadata schema
before Trino connects — initialize it once from a DuckDB session (the DuckDB
`mysql` extension bootstraps the `ducklake_*` tables on ATTACH):

```bash
duckdb -c "INSTALL ducklake; LOAD ducklake; INSTALL mysql; LOAD mysql; \
  ATTACH 'ducklake:mysql:db=ducklake host=<host> port=3306 user=ducklake password=<password>' AS lake \
    (DATA_PATH 's3://<bucket>/<prefix>/'); \
  DETACH lake;"
```

> **Cross-engine note.** Trino read/write against a MySQL-backed catalog is
> supported and tested. However, *DuckDB itself* currently cannot reliably read
> a DuckLake-on-MySQL catalog — DuckDB 1.5.4's `mysql` extension is unstable on
> that path (upstream issue, not this connector). If you need DuckDB and Trino to
> share one catalog today, use PostgreSQL. See
> [dev-docs/CATALOG-BACKENDS.md](dev-docs/CATALOG-BACKENDS.md).

**Local DuckDB `.db` file** — single-process deployment (one Trino coordinator,
zero or one workers on the same host), dev/test, or interactive single-user use:

```properties
connector.name=ducklake

# DuckLake metadata catalog database (DuckDB file)
ducklake.catalog.database-url=jdbc:duckdb:/var/lib/trino/ducklake/lake.db

# Base location for data files
ducklake.data-path=/var/lib/trino/ducklake/data/
```

The `.db` file must already contain a DuckLake metadata schema before Trino
connects. The simplest way to initialize one is from a one-shot DuckDB session:

```bash
duckdb -c "INSTALL ducklake; LOAD ducklake; \
  ATTACH 'ducklake:/var/lib/trino/ducklake/lake.db' AS lake \
    (DATA_PATH '/var/lib/trino/ducklake/data/'); \
  DETACH lake;"
```

`user` and `password` are not required for the local DuckDB backend.

Concurrency limits: a single DuckDB `.db` file can be safely accessed by
multiple connections within one process (HikariCP pool sharing one DuckDB
`DatabaseInstance`), but not by multiple OS processes simultaneously. For
multi-worker Trino clusters or for sharing the catalog with concurrent DuckDB
CLI sessions, use the PostgreSQL backend.

For S3 storage, add to the same catalog properties file:

```properties
fs.native-s3.enabled=true
s3.region=<region>
```

Restart Trino for the new plugin and catalog to take effect.

## Configuration

Catalog properties (set in `etc/catalog/<name>.properties`):

| Property | Required | Default | Description |
|----------|:--------:|---------|-------------|
| `ducklake.catalog.database-url` | Yes | — | JDBC URL for the catalog metadata DB; the backend dialect is inferred from the scheme. PostgreSQL: `jdbc:postgresql://host:port/db`. MySQL 8+: `jdbc:mysql://host:3306/db`. Local DuckDB: `jdbc:duckdb:/abs/path/to/lake.db` |
| `ducklake.catalog.database-user` | Conditional | — | Catalog database username. Required for PostgreSQL and MySQL; omit for local DuckDB |
| `ducklake.catalog.database-password` | Conditional | — | Catalog database password. Required for PostgreSQL and MySQL; omit for local DuckDB |
| `ducklake.data-path` | Yes | — | Base path for data files |
| `ducklake.catalog.max-connections` | No | 10 | Max JDBC connections to catalog |
| `ducklake.default-snapshot-id` | No | — | Pin all reads to a snapshot ID |
| `ducklake.default-snapshot-timestamp` | No | — | Pin all reads to a point in time |
| `ducklake.temporal-partition-encoding` | No | `calendar` | **Deprecated.** Default `calendar` is the DuckLake 1.0 spec contract; `epoch` retained for legacy catalogs |
| `ducklake.temporal-partition-encoding-read-leniency` | No | `true` | **Deprecated.** Companion to the above; accepts both encodings on read |

### Session Properties

| Property | Default | Description |
|----------|---------|-------------|
| `read_snapshot_id` | — | Pin reads in this session to a snapshot ID |
| `read_snapshot_timestamp` | — | Pin reads in this session to an ISO-8601 instant |
| `data_file_format` | session-inherited | Only `'parquet'` is supported. When unset, inherits the format of the most recent existing data file in the table (parquet for empty tables). |

Snapshot resolution precedence: query clause > session property > catalog config > current snapshot.

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
| `timestamptz` | TIMESTAMP WITH TIME ZONE | Yes | Yes | Stored at microsecond precision; columns declared at a lower precision (incl. Trino's default `TIMESTAMP WITH TIME ZONE`, which is precision 3) widen to precision 6 on read. CTAS and INSERT of any precision round-trip. Non-parquet data formats only (parquet rejects timestamptz). |
| `list<T>` | ARRAY(T) | Yes | Yes | Full nesting supported |
| `struct<...>` | ROW(...) | Yes | Yes | Full nesting supported |
| `map<K,V>` | MAP(K,V) | Yes | Yes | Full nesting supported |
| `json` | JSON | Yes | Yes | Native Trino JSON type — JSON path/accessor functions work (`json_extract`, `json_format`, `->`). Physically a UTF-8 string; nested JSON inside ARRAY/MAP/ROW not yet supported |
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
| Predicate pushdown (WHERE) | Yes | TupleDomain on all types, with partition-transform classification for pruning |
| File-level pruning (min/max stats) | Yes | Eliminates whole Parquet files via `ducklake_file_column_stats` (top-level and nested-leaf rows). Trino predicates today land on top-level handles; the nested-leaf stats are emitted on write so DuckDB readers can prune subfield predicates against Trino-written tables. |
| Partition pruning | Yes | Identity and temporal partitions |
| Bucket partition pruning | Yes | Murmur3 hash; prunes files for equality predicates (ranges aren't pruned — bucketing scrambles ordering) |
| Row-group pruning (Parquet footer) | Yes | Uses Parquet internal statistics |
| Page-level filtering (Parquet page index) | Yes | |
| Dynamic filter pushdown | Yes | Intersected with file-level stats |
| Parquet data files | Yes | Via Trino's native Parquet reader |
| Parquet footer-size hint | Yes | Uses `ducklake_data_file.footer_size` / `ducklake_delete_file.footer_size` to skip Trino's default 48 KB blind footer read |
| Inlined data (small tables) | Yes | Reads from catalog metadata tables |
| Mixed inline + Parquet snapshots | Yes | Both sources unioned transparently |
| Delete files (merge-on-read) | Yes | Parquet positional delete files |
| Delete-file evolution across snapshots | Yes | Reads the currently-valid delete file per data file at the active snapshot (spec: at most one delete file per data file per snapshot) |
| Schema evolution on read | Yes | Renamed columns and columns added after a data file was written read correctly (added columns return NULL for older rows) — parquet self-heals via field_id |
| Time travel — FOR VERSION AS OF | Yes | By snapshot ID |
| Time travel — FOR TIMESTAMP AS OF | Yes | By timestamp |
| Snapshot pinning (session) | Yes | `read_snapshot_id`, `read_snapshot_timestamp` |
| Snapshot pinning (catalog) | Yes | `ducklake.default-snapshot-id`, `ducklake.default-snapshot-timestamp` |
| Table statistics | Yes | Row count + column min/max from catalog |
| Metadata tables (`$files`) | Yes | Inspect data files for a table |
| Metadata tables (`$snapshots`) | Yes | List all snapshots |
| Metadata tables (`$current_snapshot`) | Yes | Current snapshot info |
| Metadata tables (`$snapshot_changes`) | Yes | Snapshot audit trail |
| Virtual (hidden) columns | Yes | `$path`, `$snapshot_id`, `$file_row_number`, `$row_id`, `$file_size_bytes` — see [Virtual Columns](#virtual-columns) below |
| Views (Trino dialect) | Yes | |
| Views (other dialects) | No | Filtered out; only Trino-created views exposed |
| Puffin deletion vectors | Yes | DuckLake's Roaring-bitmap delete files (`write_deletion_vectors=true` on the writer) |
| Sorted table optimizations | Yes | Catalog sort spec surfaces as `SortingProperty` so the planner can skip sort operators when `ORDER BY` matches the leading prefix |

### Virtual Columns

Row-lineage metadata is exposed as `$`-prefixed **hidden** columns, matching the convention
Trino's Iceberg and Delta connectors use. They are **not** included in `SELECT *` or `DESCRIBE`
but are queryable by explicit name (and usable in `WHERE` / `GROUP BY`):

| Column | Type | Meaning |
|--------|------|---------|
| `$path` | `VARCHAR` | Absolute path of the data file backing the row (`NULL` for inlined rows) |
| `$snapshot_id` | `BIGINT` | The snapshot that wrote the row's data file; per-row `begin_snapshot` for inlined rows |
| `$file_row_number` | `BIGINT` | 0-based row position within its data file (`NULL` for inlined rows) |
| `$row_id` | `BIGINT` | Globally-unique-within-table id, `file's row_id_start + $file_row_number` (`NULL` for inlined rows) |
| `$file_size_bytes` | `BIGINT` | Size in bytes of the backing data file (`NULL` for inlined rows) |

```sql
SELECT "$path", "$row_id", "$file_row_number", "$snapshot_id" FROM orders WHERE id = 42;

-- File distribution of a table
SELECT "$path", count(*) FROM orders GROUP BY "$path";

-- $path predicate prunes data files before the scan (optimization; results are correct either way)
SELECT * FROM orders WHERE "$path" = 's3://bucket/.../ducklake-abc.parquet';
```

Notes: virtual columns are read-only (writing them is rejected); `$file_index` and `$filename`
are intentionally not exposed (see [DESIGN-virtual-columns.md](dev-docs/DESIGN-virtual-columns.md)
§ 8); DuckDB-style unprefixed aliases (`rowid`, `filename`, …) are deferred.

## Write Operations

| Feature | Supported | Notes |
|---------|:---------:|-------|
| INSERT INTO | Yes | Writes Parquet files (ZSTD compression) by default; inherits the table's declared or latest data file format |
| CREATE TABLE AS SELECT | Yes | |
| DELETE | Yes | Writes Parquet positional delete files (or puffin deletion vectors) over parquet data files |
| UPDATE | Yes | Atomic delete + insert in one snapshot. Rewritten rows keep their original `rowid` (embedded lineage column, `write_row_lineage` on by default) so change feeds pair them as updates and rowids stay stable across engines |
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
| Partitioned writes | Yes | Identity and temporal transforms (hive-style `key=value/` paths) |
| Bucket partitioned writes | Yes | `partitioned_by = ARRAY['bucket(N, col)']`; Iceberg-compatible Murmur3 hash |
| Register existing files (`add_files`) | Yes | `CALL system.add_files(...)`; parquet only (IDENTITY hive partitioning supported) |
| Cross-engine Parquet compatibility | Yes | `field_id` annotations for DuckDB interop |
| Concurrent conflict detection | Yes | Snapshot lineage check; aborts on stale base |
| TRUNCATE TABLE | Yes | Bulk metadata clear (end-snapshots all data/delete files + inlined rows; O(1), no delete files written); keeps the schema. Clears inlined rows too, so it works where DELETE is gated. Time travel still sees pre-truncate rows. |
| RENAME TABLE | Yes | Same-schema only (table data paths are schema-relative, so cross-schema moves are rejected) |
| RENAME SCHEMA | Yes | Tables/views/macros follow; data stays in place; DuckDB cross-engine verified |
| COMMENT ON TABLE | Yes | Stored as the `comment` tag in `ducklake_tag`; visible to DuckDB |
| COMMENT ON COLUMN | Yes | Stored in `ducklake_column_tag`; visible to DuckDB |
| DELETE/UPDATE/MERGE over inlined rows | No | Rejected with guidance — the merge sink writes parquet positional delete files, which can't tombstone a row held inline in the catalog. Run `CALL system.flush_inlined_data(schema, table)` first (or `data_inlining_row_limit = 0`) to make the rows file-resident, then DELETE/UPDATE/MERGE work. Reads over inlined+file mixes work. |
| ALTER TABLE SET TYPE | Yes | Widening type promotions only (`ALTER COLUMN c SET DATA TYPE …`): the signed-integer chain (TINYINT→SMALLINT→INTEGER→BIGINT), integer→REAL/DOUBLE, REAL→DOUBLE, and TIMESTAMP→TIMESTAMP WITH TIME ZONE; a same-type change is a no-op. Narrowing / incompatible changes are rejected at DDL time so files written under the old physical type stay readable. Files written before the change read at the widened type: parquet self-heals via the reader's type coercion, and inlined values convert under the current type. Nested struct-field promotions (`ALTER COLUMN s.child SET DATA TYPE …`, any nesting depth) are supported too. List-element / map-value type changes are out of scope. |
| ALTER TABLE ADD/DROP FIELD | Yes | Nested struct field add/drop (`ADD COLUMN s.child …` / `DROP COLUMN s.child`). Files written before the change are reconciled per file: parquet self-heals (struct fields bind by name/field-id, missing subfields read NULL). See dev-docs/DESIGN-nested-field-evolution.md. |
| ANALYZE | Yes | Refreshes the cached table-level stats (`ducklake_table_stats` + `ducklake_table_column_stats`). The engine scans for an authoritative live row count; the per-column aggregates are rebuilt from the authoritative per-file stats, tightening any min/max that incremental maintenance left stale after a delete. Stats are otherwise maintained on every write, so ANALYZE is a no-op on a never-drifted table. A non-versioned side-table refresh: no new snapshot, `next_row_id` preserved. |
| Sorted writes | Yes | When a table carries a DuckLake sort spec (set via DuckDB `ALTER TABLE … SET SORTED BY (…)` — there is no Trino `sorted_by` property yet), a Trino INSERT physically orders the rows it writes into each parquet data file by the spec's honored leading prefix (same prefix the read side advertises as a `SortingProperty`). Gated to parquet + unpartitioned tables with a resolvable spec; partitioned / non-parquet / no-spec writes are unchanged. In-memory sort (buffers the INSERT's pages), so it targets the bounded gated scope. |

## Partitioning

| Transform | Read | Write | Notes |
|-----------|:----:|:-----:|-------|
| Identity | Yes | Yes | Partition by column value |
| `year(col)` | Yes | Yes | Date or timestamp column |
| `month(col)` | Yes | Yes | Date or timestamp column |
| `day(col)` | Yes | Yes | Date or timestamp column |
| `hour(col)` | Yes | Yes | Timestamp column |
| `bucket(N, col)` | Yes | Yes | Iceberg-compatible Murmur3 hash; equality predicates pruned, ranges not |

Temporal partition values follow the DuckLake 1.0 calendar encoding (the default; spec
PR [duckdb/ducklake-web#349](https://github.com/duckdb/ducklake-web/pull/349) settled
this). A deprecated epoch path is preserved behind
`ducklake.temporal-partition-encoding=epoch` for compatibility with pre-resolution
catalogs; it should not be needed against any v1-conformant catalog.

## Statistics

| Statistic | Supported | Notes |
|-----------|:---------:|-------|
| Table row count | Yes | From `ducklake_table_stats` |
| Column min/max (table-level) | Yes | Typed parsing of string-encoded values |
| Column min/max (file-level, for pruning) | Yes | From `ducklake_file_column_stats`; one row per primitive leaf (top-level columns and nested STRUCT/ARRAY/MAP leaves) |
| Column null count (file-level) | Yes | Used in file pruning decisions; emitted for nested leaves too |
| Conservative mode for deletes | Yes | Returns unknown stats when delete files are present |
| Conservative mode for mixed inline+Parquet | Yes | Row count preserved, column stats suppressed |
| Conservative mode for schema evolution | Yes | Stats suppressed when coverage is incomplete |

## Non-Parquet Data Files (removed — moved to duckbridge)

This connector reads and writes **parquet data files only** (delete files: parquet/puffin;
inlined data unchanged). The experimental **duckdb** (`.db`), **vortex**, and **lance**
data-file formats — along with the DuckDB-engine read/write path, function/expression pushdown,
and the lance vector/FTS/hybrid search table functions — were removed and now live in the
standalone **[duckbridge](https://github.com/brikk/duckbridge)** Trino → DuckDB connector.

A catalog that still references a non-parquet `file_format` (from an older deployment) fails
loud with a named `NOT_SUPPORTED` error at metadata load and split generation, rather than
returning silently-wrong results. The `ducklake-catalog` module's DuckDB/Quack usage is the
metadata/catalog plane and is unaffected.

## Change Feed

Three table functions expose the rows that changed between two snapshots (DuckLake's data change
feed):

```sql
-- Rows changed between snapshots 3 and 4 (bounds inclusive):
SELECT * FROM TABLE(ducklake.system.table_changes(
        schema_name => 'sales', table_name => 'orders',
        start_snapshot => 3, end_snapshot => 4))
ORDER BY snapshot_id;
```

| Function | Output columns |
|---|---|
| `table_insertions(schema_name, table_name, ...)` | `snapshot_id`, `rowid`, then the table's columns |
| `table_deletions(schema_name, table_name, ...)` | `snapshot_id`, `rowid`, then the table's columns |
| `table_changes(schema_name, table_name, ...)` | `snapshot_id`, `rowid`, `change_type` (`insert` / `delete` / `update_preimage` / `update_postimage`), then the table's columns |

- **Bounds** are inclusive on both ends. Each bound is given as a snapshot id
  (`start_snapshot` / `end_snapshot`, BIGINT) or a timestamp (`start_timestamp` / `end_timestamp`,
  `TIMESTAMP WITH TIME ZONE`, resolved to the snapshot active at-or-before it — like `AS OF`).
  A bound may not be given both ways; `start` is required, `end` defaults to the current snapshot.
- Columns are read as of the **end**-snapshot schema (schema evolution applies — a column added in
  the window shows as its default/NULL for earlier changes; a dropped column is omitted).
- `rowid` follows DuckLake row lineage: when a file carries the embedded lineage column (parquet
  field-id `2147483540` / `_ducklake_internal_row_id`, written by lineage-preserving UPDATE /
  compaction — e.g. DuckDB), that preserved rowid is used; otherwise it's `row_id_start + file
  position`. So a lineage-preserving UPDATE's delete and re-insert land on the same `rowid` in one
  snapshot and pair into `update_preimage` + `update_postimage`. This connector's OWN
  UPDATE/MERGE writes emit the lineage column too (session property `write_row_lineage`,
  **on by default** — DuckDB preserves lineage unconditionally, so this is the
  cross-engine-faithful behavior; parquet data files): rewritten rows keep their original
  `rowid` — stable across engines and across CHAINED updates — and Trino-written updates
  pair in the change feed exactly like DuckDB-written ones. Compaction
  (`rewrite_data_files`) preserves rowids the same way, and the queryable `$row_id`
  virtual resolves the embedded lineage on plain reads. Set `write_row_lineage = false`
  for the legacy shape: a fresh `row_id_start` per rewrite, updates surfacing as
  `delete` + `insert`.
- The feed reads file-based data/delete files **and** DuckLake's **inlined** data (small writes
  DuckDB keeps in `ducklake_inlined_*` tables): inlined inserts, inlined-row deletes, and inline
  file-position deletes all surface. Compaction that expires snapshots can still limit what the feed
  can report (per the DuckLake spec).

## Table Properties

Set on `CREATE TABLE ... WITH (...)` and `CREATE TABLE ... AS SELECT ... WITH (...)`.

| Property | Type | Description |
|----------|------|-------------|
| `partitioned_by` | `ARRAY(VARCHAR)` | Partition columns with optional transform, e.g. `ARRAY['region', 'year(event_date)', 'bucket(4, customer_id)']`. See [Partitioning](#partitioning). |
| `data_file_format` | `VARCHAR` | The table's DECLARED data file format. Only `'parquet'` (the default) is supported; the experimental duckdb/vortex/lance formats were removed (see [duckbridge](https://github.com/brikk/duckbridge)). |
| `location` | `VARCHAR` | Per-table storage path landed in `ducklake_table.path`. Values with a URI scheme (`s3://`, `gs://`, `file://`, `abfss://`, ...) are stored absolute (`path_is_relative=false`); other values are stored relative to the schema's data path. Trailing slash is appended if missing; `..` segments are rejected. Defaults to `<tableName>/`. |

Example:

```sql
CREATE TABLE sales.orders (
    id BIGINT,
    region VARCHAR,
    event_date DATE,
    amount DOUBLE
) WITH (
    partitioned_by = ARRAY['region', 'year(event_date)'],
    location = 's3://my-bucket/cold-tier/orders/'
);
```

## Procedures

Procedures are exposed under the `system` schema of the catalog, following the Trino
convention (`CALL <catalog>.system.<procedure>(...)`). The `<catalog>` token is the
catalog name configured in `etc/catalog/<name>.properties` — a single procedure invocation
operates on whichever DuckLake catalog you invoke through.

| Procedure | Description |
|-----------|-------------|
| `add_files(schema_name, table_name, files, [allow_missing], [ignore_extra_columns], [hive_partitioning], [file_format])` | Register pre-existing **parquet** data files of an existing DuckLake table without rewriting (`file_format => 'parquet'`, the only supported value; mirrors upstream's `ducklake_add_data_files`). Partitioned datasets are registered with `hive_partitioning => true`, which learns identity-partition values from the `key=value/` path (the partition column must be present in the file). |
| `flush_inlined_data([schema_name], [table_name])` | Materialize inlined rows (written cross-engine by DuckDB under `data_inlining_row_limit`) into a Parquet data file and clear the inlined rows, atomically. Unblocks DELETE/UPDATE/MERGE, which are gated while a table has inlined rows. **Scope by args:** both → one table; `schema_name` only → every table in the schema; neither → every table with inlined rows across the catalog (partitioned tables are skipped with a log in the wide scopes; an explicit single-table call errors on partitioned). No-op when nothing is inlined. |
| `rewrite_data_files(schema_name, table_name, [file_size_threshold], [target_file_size], [reclaim_sources_immediately])` | Compaction / `optimize`: merge a table's small data files into fewer larger files through the real read path (so delete files / partial files / schema evolution all apply). Per-partition groups for partitioned tables; size-bounded output (`target_file_size`, default 512MB). Registers the merged files + end-snapshots the sources atomically; `reclaim_sources_immediately => true` writes the partial-emitting (`merge_adjacent`) variant that deletes sources outright. |
| `remove_orphan_files([schema_name], [table_name], [retention_threshold], [dry_run])` | Delete files under the target data path(s) that no catalog row references (the residue of failed/aborted commits) and that are older than `retention_threshold` (default `'7d'`). **Only DuckLake-written residue is deleted** — `ducklake-`-prefixed data/delete files (`.parquet`/`.puffin`); foreign files a user parked under the path (`_SUCCESS`, `foo.txt`, their own non-`ducklake-` parquet) are never touched. **Scope by args:** both → one table; `schema_name` only → the whole schema; neither → the whole catalog (the wide scopes reclaim residue from a failed `CREATE TABLE` that a per-table call can't name). The threshold is floored by `ducklake.remove-orphan-files.min-retention` (default `7d`) — a grace period protecting files an in-flight, possibly cross-engine, writer just produced. `dry_run => true` reports and removes nothing. Touches storage only. |
| `expire_snapshots([retention_threshold], [snapshot_ids], [dry_run])` | Catalog-wide. Remove old DuckLake snapshots and reclaim the data/delete files only they referenced. Select either by `retention_threshold` (default `'7d'`, floored by `ducklake.maintenance.min-retention`) or an explicit `snapshot_ids` ARRAY (not floored); the latest snapshot is never expirable. Plain catalog transaction (no new snapshot); only **schedules** dead files for deletion — run `cleanup_old_files` to reclaim the space. `dry_run => true` lists the snapshots that would be expired. Mirrors upstream `ducklake_expire_snapshots`. |
| `cleanup_old_files([retention_threshold], [dry_run])` | Catalog-wide. Physically delete files previously scheduled by `expire_snapshots` (or a cross-engine DuckLake op) once they are older than `retention_threshold` (default `'7d'`, floored by `ducklake.maintenance.min-retention` — the grace period protecting in-flight readers). The second phase of two-phase deletion. Mirrors upstream `ducklake_cleanup_old_files`. |

### `add_files`

```sql
CALL ducklake.system.add_files(
    schema_name => 'sales',
    table_name => 'orders',
    files => ARRAY['s3://bucket/legacy/2024/orders.parquet'],
    allow_missing => false,         -- default: false. Reject if a table column is missing from the file
    ignore_extra_columns => false,  -- default: false. Reject if the file has columns not in the table
    hive_partitioning => false      -- default: false. Parse {key=value}/ path segments as partition values
)
```

Parquet column names are matched to table column names case-insensitively at
registration time, and the registration writes a `ducklake_name_mapping` row that
the read path consults — so files with case-differing or otherwise-renamed
columns read correctly through both Trino and DuckDB. Column reordering between
file and table is supported. Each unique parquet schema seen across the `files`
array shares one `ducklake_column_mapping` row. The procedure produces one
snapshot per invocation and participates in the connector's concurrent-conflict
matrix (an `add_files` racing a `DROP COLUMN` against the same column will fail
non-retryably, matching upstream).

**Known limitation in v1:** hive partitioning is supported only for the IDENTITY
transform. Tables with `year(col)` / `month(col)` / `bucket(N, col)` partition
specs are not yet accepted with `hive_partitioning => true`, since their stored
partition value is derived (not the original column value) and can't be projected
back. Identity-partition columns missing from the parquet body are projected from
the catalog's `ducklake_file_partition_value` row at read time, so hive-style
external file imports round-trip through `SELECT`.

### `remove_orphan_files`

```sql
CALL ducklake.system.remove_orphan_files(
    schema_name => 'sales',
    table_name => 'orders',
    retention_threshold => '7d',    -- default '7d'; floored by ducklake.remove-orphan-files.min-retention
    dry_run => false                -- default false; true logs candidates and deletes nothing
)
```

Deletes files under the table's data path that no catalog row references — the residue of
failed/aborted commits, which previously had no Trino-side remedy. Only files **older than the
retention threshold** are removed; that grace period is what makes the op safe without a global
lock (a file young enough to still be referenced by an in-flight, possibly cross-engine, writer is
never touched), and the threshold is floored by the `ducklake.remove-orphan-files.min-retention`
config so it can't be set dangerously low. The known set spans every data/delete-file path the
catalog references at **any** snapshot (so end-snapshotted-but-not-yet-cleaned files are safe) plus
files already scheduled for deletion. Touches storage only — no snapshot, no catalog
mutation. See [dev-docs/DESIGN-maintenance.md](dev-docs/DESIGN-maintenance.md).

### `expire_snapshots` + `cleanup_old_files`

```sql
-- Expire snapshots older than 30 days (catalog-wide), then reclaim the freed files.
CALL ducklake.system.expire_snapshots(retention_threshold => '30d');
CALL ducklake.system.cleanup_old_files(retention_threshold => '7d');

-- Or expire specific snapshot ids (not floored by min-retention):
CALL ducklake.system.expire_snapshots(snapshot_ids => ARRAY[12, 13, 14]);
```

`expire_snapshots` removes old DuckLake snapshots — which are **catalog-wide** versions, so this is
a catalog-scoped procedure, not table-scoped — and reclaims the data/delete files that only those
snapshots referenced (liveness is the half-open `[begin_snapshot, end_snapshot)` window against the
surviving snapshots; the latest snapshot is never expirable). It is a plain catalog transaction
(no new snapshot) that only **schedules** the dead files into `ducklake_files_scheduled_for_deletion`;
`cleanup_old_files` is the second phase that physically deletes them once they age past the grace
period. This two-phase split (schedule, then age-gated unlink) is what keeps deletion safe against
in-flight readers on other engines — see [dev-docs/DESIGN-maintenance.md](dev-docs/DESIGN-maintenance.md).
Expire also GCs the metadata rows of fully-expired dropped tables; dead schema/view/macro rows are
left as harmless dangling rows (a follow-up).

**Partial (cross-snapshot compacted) files:** DuckLake's `merge_adjacent_files` can merge rows from
multiple snapshots into one file (`ducklake_data_file.partial_max` set), physically carrying each
row's origin snapshot. The connector reads such files correctly: a time-travel read at snapshot `S`
drops rows whose `_ducklake_internal_snapshot_id > S` (only when `partial_max > S`), so time travel
over a DuckDB-compacted table returns the right rows. Consolidated **parquet delete files** spanning
snapshots (`ducklake_delete_file.partial_max`) are filtered the same way — only the deletions
recorded at or before the read snapshot apply. Consolidated **puffin** (deletion-vector) delete
files are filtered too: a real PFA1 container tags each blob with a `ducklake-snapshot-id`, and a
time-travel read applies only the blobs whose snapshot id is `<= S`. Every partial-file shape (data,
parquet-delete, puffin-delete) is now read correctly — no gate remains.

## Cross-Engine Compatibility

The connector is tested for bidirectional compatibility with DuckDB:

| Direction | Tested | Notes |
|-----------|:------:|-------|
| DuckDB writes, Trino reads | Yes | Full column value round-trips validated |
| Trino writes, DuckDB reads | Yes | Parquet field_id mapping ensures correct column matching |
| Shared PostgreSQL catalog | Yes | Both engines operate on the same metadata; preferred for multi-process |
| DuckDB `.db` catalog (local) | Yes | Single-process — DuckDB sessions and Trino can't access the file simultaneously, so cross-engine workflow is sequential rather than concurrent |
| Inlined data created by DuckDB | Yes | Trino reads inlined rows from catalog tables |
| Schema evolution across engines | Yes | ADD COLUMN by one engine, read by the other |

### Upstream corpus replay

Beyond the hand-written cross-engine suites, the connector is verified against
**DuckLake's own upstream test corpus** — the 466 sqllogictest files (7,600+ test
cases) that the reference C++ implementation ships in `duckdb/ducklake` `test/sql/`,
vendored as the `ducklake` git submodule and driven by the published
[`dev.brikk.ducklake:ducklake-test-corpus-replay`](https://github.com/brikk/ducklake-catalog)
replay harness. Each corpus file is executed **verbatim** through an embedded DuckDB
oracle (no dialect translation), building real catalog state on an isolated
PostgreSQL metadata database; every lake read the corpus performs is then
re-executed through a live Trino query runner against the *same* catalog and
compared row-for-row against the oracle's result — including intermediate
states mid-file. A divergence means Trino read the same DuckLake state
differently than DuckDB: exactly the class of spec-conformance bug that is
hardest to catch by hand (on first contact the harness found two real bugs that
138 hand-written test classes had missed). The corpus grows with every upstream
release, so conformance coverage compounds for free:

```shell
./gradlew test --tests "*TestTrinoCorpusReplay"                            # starter set
./gradlew test --tests "*TestTrinoCorpusReplay" -Dducklake.corpus.dirs=all # full corpus
```

## Not Yet Implemented

### Maintenance Operations

All maintenance operations are shipped as `CALL system.*` procedures — see
[Procedures](#procedures): `rewrite_data_files` (compaction / `optimize`, both non-partial and
partial-emitting shapes), `expire_snapshots`, `cleanup_old_files`, `remove_orphan_files`,
`flush_inlined_data`, plus stats recompute via `ANALYZE`. The connector exposes these as
procedures rather than the `ALTER TABLE ... EXECUTE optimize` alias (which needs a separate SPI for
no capability gain). Design notes: [dev-docs/DESIGN-maintenance.md](dev-docs/DESIGN-maintenance.md).

### DDL

- `ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE` (type promotion) is supported for **widening**
  changes, on both top-level columns and nested struct fields (see the feature chart above). Still
  **not** supported: narrowing / incompatible changes (rejected at DDL time), and type changes to
  list elements or map values.

### Commit Context

Session properties for annotating write snapshots (DuckDB `set_commit_message` equivalent —
`commit_author` / `commit_message` / `commit_extra_info`) are a deliberate **non-goal**: the
required columns are not in the DuckLake spec's `ducklake_snapshot` table, so writing them would
risk cross-engine divergence. Revisit only if DuckLake upstream defines them.

### Cross-Dialect View Transpilation

Views created by DuckDB (or other engines) are not visible in Trino. Only Trino-dialect
views are exposed. Cross-dialect transpilation (e.g., DuckDB SQL to Trino SQL) is a
research item.

### Catalog Backends

| Backend | Status | Notes |
|---------|--------|-------|
| PostgreSQL | Supported | Multi-process; preferred for shared / clustered deployments (incl. sharing with DuckDB) |
| MySQL 8+ | Supported (Trino) | Multi-process. Connector read/write is tested; **cross-engine with DuckDB is deferred** — DuckDB 1.5.4's `mysql` extension is unstable reading a DuckLake-on-MySQL catalog (upstream). See `dev-docs/CATALOG-BACKENDS.md` |
| Local DuckDB `.db` file | Supported | Single-process; dev, single-node Trino, interactive use |
| SQLite | Planned | Single-process; lighter-weight alternative to local DuckDB |
| Turso (libSQL) | Planned | Distributed SQLite-wire; blocked on a formal JDBC driver in Maven Central |
| Remote DuckDB over Quack RPC | Planned | Shared DuckDB-as-catalog without filesystem mounting; landed experimentally upstream 2026-05-12 (`duckdb/ducklake#1151`) and gated here on the Quack RPC layer adding multi-table-query and UPDATE/DELETE support. Fixture and URL plumbing are in tree behind `-Dducklake.test.catalog-backend=DUCKDB_QUACK`; see `dev-docs/TODO-WRITE-MODE.md § DuckDB-as-Catalog Backend` |

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
- Files written before a failed commit become orphans. Reclaim them with
  `CALL system.remove_orphan_files(schema_name, table_name)` (see [Procedures](#procedures)).
- Puffin deletion vectors (DuckLake's Roaring-bitmap delete files) are both read AND written.
  By default Trino-side DELETE/UPDATE/MERGE emits DuckLake-spec parquet positional delete files
  (`(file_path, pos)` with file-local positions — readable by DuckDB); set the session property
  `write_deletion_vectors = true` to emit `.puffin` deletion-vector files instead (matching DuckDB's
  `write_deletion_vectors` option). Both shapes are read by Trino and DuckDB. Position-delete
  filtering over parquet data files is cross-engine tested in both directions
  (Trino-writes/DuckDB-reads and DuckDB-writes/Trino-reads).
- Row lineage is both read AND written (session property `write_row_lineage`, on by default).
  Trino's UPDATE/MERGE rewrites AND `rewrite_data_files` compaction embed each row's original
  rowid (`_ducklake_internal_row_id`, parquet field-id `2147483540` — DuckDB's own encoding), so
  rowids stay stable across engines, across chained updates, and across compaction; change feeds
  (Trino's `table_changes` AND DuckDB's) pair rewrites into `update_preimage`/`update_postimage`,
  and the `$row_id` virtual resolves the preserved ids on plain reads. Parquet data files only;
  cross-engine tested (DuckDB observes the same rowid after a Trino lineage UPDATE).
- The experimental **duckdb** / **vortex** / **lance** data-file formats, the DuckDB-engine
  read/write path, function/expression pushdown, and the lance search table functions were
  removed and moved to the standalone **[duckbridge](https://github.com/brikk/duckbridge)**
  Trino → DuckDB connector (see [Non-Parquet Data Files](#non-parquet-data-files-removed--moved-to-duckbridge)).

## Building from source

Requires **JDK 25**. The repo pins it via [mise](https://mise.jdx.dev) (`mise.toml`) — run
`mise install` — or point `JAVA_HOME` at any JDK 25. The shared catalog layer
(`dev.brikk.ducklake:ducklake-catalog`) resolves from Maven Central; clone with submodules
(`git clone --recurse-submodules`, or `git submodule update --init`) so the corpus fixture and
`ducklake-web` spec are present.

### Build the plugin distribution

```shell
./gradlew pluginDist
```

Produces `build/distributions/trino-ducklake-<version>.{zip,tar.gz}` (+ `.sha256`), each
expanding to a `trino-ducklake-<version>/` directory of jars — the same artifacts attached to a
GitHub Release. For just the exploded plugin dir (no archive), use `./gradlew pluginAssemble`
(output under `build/trino-plugin/trino-ducklake-<version>/`).

### Local dev stack (Docker Compose)

For interactive hacking against a real Trino + Postgres + MinIO + DuckLake stack with optional
TPC-H seed data, see [compose/README.md](compose/README.md).

### Running tests

Tests require Docker or Podman (PostgreSQL — and the Quack DuckDB sidecar when that backend is
selected — run via Testcontainers). The default test backend is PostgreSQL; flip backends with
the `ducklake.test.catalog-backend` system property:

```shell
./gradlew test                                                # PostgreSQL (default)
./gradlew test -Dducklake.test.catalog-backend=DUCKDB_LOCAL   # local DuckDB .db file
./gradlew test -Dducklake.test.catalog-backend=DUCKDB_QUACK   # remote DuckDB over Quack (experimental)
```

### Releasing

Push a `v<version>` tag (e.g. `v0.1.0`); the [`release.yml`](.github/workflows/release.yml)
workflow builds `pluginDist` and attaches the archives + checksums to the GitHub Release for
that tag.

## Additional Documentation

- [SAMPLES.md](SAMPLES.md) — SQL examples for DDL, DML, time travel, metadata tables, and DuckDB interop
- [TODO for READ side](dev-docs/TODO-READ-MODE.md) — Read mode open items + research notes
- [TODO for WRITE side](dev-docs/TODO-WRITE-MODE.md) — Write mode open items + design rationale
- [duckbridge](https://github.com/brikk/duckbridge) — the standalone Trino → DuckDB connector that now owns the DuckDB-engine pushdown, executors, and lance/vortex surfaces
- [dev-docs/archive/](dev-docs/archive/) — Historical context: closed work, archived plans, the DuckLake 1.0 spec impact reference, the reuse audit, and the date/time pushdown program design + empirical TZ findings (incl. the tombstoned duckdb-format trackers)
