# Trino DuckLake Connector — Feature Support

Connector for the [DuckLake](https://ducklake.select) open table format (spec v1.0).
Supports PostgreSQL (shared, multi-process) and a local DuckDB `.db` file
(single-process; ideal for dev, local interactive use, and single-node Trino
deployments) as catalog metadata backends. SQLite, and a remote-DuckDB backend
over [Quack RPC](https://duckdb.org/2026/05/12/quack-remote-protocol) for shared
DuckDB-as-catalog without filesystem mounting, are planned next.

Tested with DuckDB 1.5.4 for cross-engine compatibility.

The [DuckLake spec](ducklake-web/docs/stable/specification/) is included as a submodule.
All documentation and feature tables in this module are current against that version of the spec.

## Build

Requires Java 25. Install via [SDKMan](https://sdkman.io/install/):

```shell
cd jvm
sdk env install
sdk env
```

### Prerequisite: build the trino_parity DuckDB extension first

This connector's predicate-pushdown path REQUIRES the
[`trino_parity` DuckDB extension](../../duckdb-trino-parity-extension/) to be loadable into
the DuckDB instance at attach time. The gradle build bundles every available platform's
`.duckdb_extension` binary into the plugin jar as classpath resources, and
`TrinoParityExtensionResolver` extracts the matching one at runtime.

> **The extension is a git submodule** (and it nests the `duckdb` and
> `extension-ci-tools` submodules). If you cloned without `--recurse-submodules`,
> initialise them from the repo root before building:
> `git submodule update --init --recursive`. Full clone/build walkthrough:
> [root README → Getting Started](../../README.md#getting-started).

Build at least the host-platform binary before assembling the plugin:

```shell
cd ../../duckdb-trino-parity-extension
GEN=ninja make
```

For the Quack-engine tests (`-Dducklake.test.catalog-backend=DUCKDB_QUACK` and the
default `TestDucklakeDuckDbExecutorBackends`), the Quack server runs as a Linux
testcontainer regardless of the host OS. On a macOS dev box you also need a Linux build
of the extension so the testcontainer can LOAD it:

```shell
cd ../../duckdb-trino-parity-extension
make linux-arm64    # native on Apple Silicon
make linux-amd64    # slower under Rosetta/qemu emulation
```

The gradle bundling is non-fatal on missing platforms — it'll just ship a plugin jar
without that variant. At deploy time, set `ducklake.duckdb.parity-extension-path` to
override the bundled binary with an explicit filesystem path.

### Build the plugin

```shell
./gradlew :trino-ducklake:pluginAssemble
```

The assembled plugin is at `trino-ducklake/build/trino-plugin/trino-ducklake-<version>/`.

### Local dev stack (Docker Compose)

For interactive hacking against a real Trino + Postgres + MinIO + DuckLake
stack with optional TPC-H seed data, see [compose/README.md](compose/README.md).

### Running Tests

Tests require Docker or Podman (PostgreSQL — and the Quack DuckDB sidecar when
that backend is selected — run via Testcontainers). The default test backend is
PostgreSQL; flip backends with the `ducklake.test.catalog-backend` system
property:

```shell
./gradlew :trino-ducklake:test                                                # PostgreSQL (default)
./gradlew :trino-ducklake:test -Dducklake.test.catalog-backend=DUCKDB_LOCAL   # local DuckDB .db file
./gradlew :trino-ducklake:test -Dducklake.test.catalog-backend=DUCKDB_QUACK   # remote DuckDB over Quack (experimental; see TODO doc)
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
| Predicate pushdown (WHERE) | Yes | TupleDomain on all types (both formats); duckdb-format splits also get operator/`CAST`/`LIKE`/function pushdown into DuckDB — see [`README-duckdb-format-pushdown-reference.md`](README-duckdb-format-pushdown-reference.md) |
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
| Schema evolution on read | Yes | Renamed columns and columns added after a data file was written read correctly (added columns return NULL for older rows) — across all four data file formats (parquet via field_id; duckdb/vortex/lance via catalog name resolution at the file's write snapshot) |
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
| DELETE | Yes | Writes Parquet positional delete files; verified against parquet, duckdb, vortex, and lance data files |
| UPDATE | Yes | Atomic delete + insert in one snapshot; rewritten rows inherit the table's data file format (all four formats verified) |
| MERGE INTO | Yes | WHEN MATCHED THEN UPDATE/DELETE + WHEN NOT MATCHED THEN INSERT; verified against all four data file formats |
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
| Partitioned writes | Yes | Identity and temporal transforms; verified for parquet, duckdb, vortex, and lance data files (hive-style `key=value/` paths per format) |
| Bucket partitioned writes | Yes | `partitioned_by = ARRAY['bucket(N, col)']`; Iceberg-compatible Murmur3 hash |
| Register existing files (`add_files`) | Yes | `CALL system.add_files(...)`; parquet (IDENTITY hive partitioning supported), lance dataset directories, and vortex files (`file_format => 'lance'`/`'vortex'`) |
| Cross-engine Parquet compatibility | Yes | `field_id` annotations for DuckDB interop |
| Concurrent conflict detection | Yes | Snapshot lineage check; aborts on stale base |
| TRUNCATE TABLE | Yes | Bulk metadata clear (end-snapshots all data/delete files + inlined rows; O(1), no delete files written); keeps the schema. Clears inlined rows too, so it works where DELETE is gated. Time travel still sees pre-truncate rows. |
| RENAME TABLE | Yes | Same-schema only (table data paths are schema-relative, so cross-schema moves are rejected) |
| RENAME SCHEMA | Yes | Tables/views/macros follow; data stays in place; DuckDB cross-engine verified |
| COMMENT ON TABLE | Yes | Stored as the `comment` tag in `ducklake_tag`; visible to DuckDB |
| COMMENT ON COLUMN | Yes | Stored in `ducklake_column_tag`; visible to DuckDB |
| DELETE/UPDATE/MERGE over inlined rows | No | Rejected with guidance — the merge sink writes parquet positional delete files, which can't tombstone a row held inline in the catalog. Run `CALL system.flush_inlined_data(schema, table)` first (or `data_inlining_row_limit = 0`) to make the rows file-resident, then DELETE/UPDATE/MERGE work. Reads over inlined+file mixes work. |
| ALTER TABLE SET TYPE | No | Type promotion not supported |
| ALTER TABLE ADD/DROP FIELD | Yes | Nested struct field add/drop (`ADD COLUMN s.child …` / `DROP COLUMN s.child`) on every data format. Files written before the change are reconciled per file: parquet self-heals (struct fields bind by name/field-id, missing subfields read NULL); non-parquet (duckdb/vortex) reshapes the struct with `struct_pack` — added subfields read NULL, dropped ones are skipped (no positional misbind), renames/reorders map by column_id, and a NULL struct stays NULL. Lance is excluded (its ROW writes are gated upstream, so a struct can't be written to a lance file). See dev-docs/DESIGN-nested-field-evolution.md. |
| ANALYZE | Yes | Refreshes the cached table-level stats (`ducklake_table_stats` + `ducklake_table_column_stats`). The engine scans for an authoritative live row count; the per-column aggregates are rebuilt from the authoritative per-file stats, tightening any min/max that incremental maintenance left stale after a delete. Stats are otherwise maintained on every write, so ANALYZE is a no-op on a never-drifted table. A non-versioned side-table refresh: no new snapshot, `next_row_id` preserved. |
| Sorted writes | No | Trino-written files are unsorted |

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

## DuckDB-Format Data Files (EXPERIMENTAL)

> **Experimental.** The `.db`-format read/write path is feature-complete with cross-engine tests, but the surface is still evolving (executor abstractions, session properties, write modes). Default to parquet for production workloads unless you've evaluated the trade-offs.

DuckLake's `ducklake_data_file.file_format` column enumerates the file format per data file; values today are `parquet` and `duckdb`. A duckdb-format split is one whose data lives in a single-table DuckDB database file (`.db`).

**Predicate & function pushdown.** duckdb-format splits get an extra pushdown layer on top of the standard TupleDomain + file-pruning: the connector translates Trino predicates — standard operators, `CAST`, `LIKE`, `concat`, and ~95 catalogued functions (string, numeric, regex, encoding, distance, hash, date/time) — into DuckDB SQL and evaluates them server-side. Parquet splits use Trino's standard reader and don't get this layer. **Full reference (every operator, transform, and function):** [`README-duckdb-format-pushdown-reference.md`](README-duckdb-format-pushdown-reference.md). Roadmap/history: [`dev-docs/TODO-pushdown-duckdb.md`](dev-docs/TODO-pushdown-duckdb.md).

**Read modes** (session property `duckdb_read_mode`, default `httpfs`):

| Mode | Behaviour |
|---|-------|
| `materialize` | Download the `.db` to a local tmp cache, then `ATTACH 'path' AS lake (READ_ONLY)`. Fast steady-state when files are local; the cache amortises ATTACH cost across queries. |
| `httpfs` | Load DuckDB's httpfs extension, `ATTACH 's3://...' AS lake (READ_ONLY)`. DuckDB streams blocks server-side; no whole-file download. |
| `auto` | Pick per file based on the `ducklake.duckdb.auto-httpfs-threshold` config — files below the threshold materialize, files at or above stream via httpfs. |

**Executors** (the in-process strategy is the default; Quack is gated on upstream RPC maturity):

- **In-process** — DuckDB embedded via `jdbc:duckdb:`. Per-split connection lifecycle. A DuckDB crash takes the JVM down.
- **Quack** — out-of-process DuckDB reached via Quack RPC. The local JDBC client ships SQL server-side via `quack_query_by_name`. Sidecar crash recoverable independently of the Trino worker.

Both executors set `SET TimeZone = '<session_zone>'` on attach (required for Tier C predicate pushdown correctness over `TIMESTAMP WITH TIME ZONE`) and install the same `trino_*` macro library so function pushdown lands consistently on both paths.

**Write modes** (session property `duckdb_writer_mode`, default `arrow_stream`):

| Mode | Notes |
|---|-------|
| `arrow_stream` | Page → Arrow → `INSERT FROM` registered stream. Columnar end-to-end. |
| `appender` | JDBC Appender; per-cell JNI calls. Kept for comparison; the Arrow path is faster. |

**Mixed-format tables.** A DuckLake table can carry both parquet and duckdb splits within the same logical table. The connector returns translated function-shape conjuncts both as a pushdown hint (used by `.db` splits) AND in `remainingExpression` (so Trino re-evaluates above the scan for parquet splits). Each path handles its own correctness independently.

**User-visible behaviour change (chunk 3.5):** `SELECT timestamptz_col FROM duckdb_format_table` renders in the session zone — matching how Iceberg / Hive / parquet `isAdjustedToUtc=true` connectors return WTZ. The connector's Arrow-to-Page converter resolves the TimeZone from the Arrow schema TZ instead of hardcoding UTC. Parquet-format reads through Trino's standard parquet reader still render in UTC (upstream Trino behaviour); mixed-format tables show this asymmetry today.

**Complex types** (`ROW`/`MAP`/`ARRAY`, nested arbitrarily) read and write through the
duckdb-format path. List *elements* are limited to scalars on write; complex columns get
value/null-count stats only (no min/max).

## Vortex Data Files (EXPERIMENTAL)

> **Experimental**, like the other non-parquet formats.

`data_file_format = 'vortex'` writes each CTAS/INSERT payload as a single
[Vortex](https://vortex.dev) file via DuckDB's `vortex` extension (`COPY ... (FORMAT vortex)`),
and reads dispatch through the same DuckDB execution engine as the `.db` format — but via the
`read_vortex('<path>')` table function instead of an ATTACH. TupleDomain predicates render into
the scan's `WHERE` and evaluate inside DuckDB.

- **Types:** scalars, `ARRAY` of scalar, and `ROW` columns round-trip (NULL rows included).
  **`MAP` writes are rejected at schema time** — the DuckDB vortex extension fails *natively*
  (crash/hang, not an error) on MAP COPY (probed 2026-06-11); use parquet or duckdb format for
  MAP columns.
- **Read modes:** the `.db` format's materialize-vs-httpfs decision is reused. Sub-threshold s3
  files download through Trino's filesystem (credentials handled by Trino). **Streaming s3 reads
  need the `AWS_*` env channel** on the executing process: `read_vortex` binds through Rust
  `object_store`, which ignores DuckDB httpfs secrets — same channel as lance, see
  [README-lance-format.md § S3 datasets](README-lance-format.md#s3-datasets--the-aws_-env-credential-channel).
  (The vortex COPY *write* path goes through Trino's filesystem upload and needs no env.)

## Lance Data Files & Vector Search (EXPERIMENTAL)

> **Experimental**, like the other non-parquet formats.

`data_file_format = 'lance'` stores each write as a [Lance](https://lancedb.github.io/lance/)
**dataset directory** and enables three search table functions over the table's lance data:

```sql
SELECT id, title, _distance
FROM TABLE(ducklake.system.lance_vector_search(
        schema_name => 'vectors', table_name => 'docs',
        column_name => 'emb', query_vec => ARRAY[0.12, 0.85], k => 10))
ORDER BY _distance LIMIT 10;
```

| Capability | Notes |
|---|---|
| CTAS / INSERT (`data_file_format='lance'`) | Scalars + `ARRAY` of scalar; embedding columns (`ARRAY(REAL)`) round-trip into `lance_vector_search`. `ROW` writes are gated (upstream NULL-struct loss) and `MAP` writes are gated at schema time (lance < format 2.2 fails the COPY at close) — register externally-written datasets via `add_files` instead. |
| Reads | `__lance_scan('<dataset dir>')` through the DuckDB engine; TupleDomain pushdown; full complex-type reads |
| `add_files(file_format => 'lance')` | Register externally-produced lance datasets (the embedding-pipeline route) |
| `lance_vector_search` | k-NN over an embedding column; appends `_distance` |
| `lance_fts` | BM25 full-text (no index required); appends `_score` |
| `lance_hybrid_search` | Combined vector + text with optional `alpha`; appends `_distance`, `_score`, `_hybrid_score` |
| Search pushdown | `WHERE` / `ORDER BY <score> LIMIT n` / projection push down (`prefilter => true` for filter-then-search semantics) |
| s3 datasets | Quack engine + `AWS_*` sidecar env (lance ignores DuckDB secrets) |
| Upstream churn guard | `TestLanceExtensionCanary` pins the verified extension build |

**Full reference — arguments, exact-top-k recipe, prefilter pushability, s3 channel, gates:**
[`README-lance-format.md`](README-lance-format.md). Architecture decision (DuckDB extension vs
lance-core JNI): [`dev-docs/archive/REPORT-lance-route-a-vs-b.md`](dev-docs/archive/REPORT-lance-route-a-vs-b.md).

## Change Feed

Three table functions expose the rows that changed between two snapshots (DuckLake's data change
feed), for **all** data file formats:

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
  UPDATE/MERGE writes do not emit the lineage column (the rewritten row gets a fresh `row_id_start`),
  so a Trino-written UPDATE surfaces as a `delete` + `insert` — a faithful description of its
  delete-then-insert implementation.
- The feed reads file-based data and delete files. Tables carrying **inlined** data/deletes (small
  writes DuckDB keeps in `ducklake_inlined_*` tables) are rejected with a pointer to
  `flush_inlined_data` rather than silently omitting those changes. Compaction that expires
  snapshots can also limit what the feed can report (per the DuckLake spec).

## Table Properties

Set on `CREATE TABLE ... WITH (...)` and `CREATE TABLE ... AS SELECT ... WITH (...)`.

| Property | Type | Description |
|----------|------|-------------|
| `partitioned_by` | `ARRAY(VARCHAR)` | Partition columns with optional transform, e.g. `ARRAY['region', 'year(event_date)', 'bucket(4, customer_id)']`. See [Partitioning](#partitioning). |
| `data_file_format` | `VARCHAR` | The table's DECLARED data file format: `'parquet'` (default), `'duckdb'`, `'vortex'`, or `'lance'`. Beats the session property at CREATE time and is persisted as a table-scoped setting in `ducklake_metadata`, so later plain INSERTs (including into a still-empty `CREATE TABLE`) keep writing the declared format. Undeclared tables instead inherit the format of their most recent data file. |
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
| `add_files(schema_name, table_name, files, [allow_missing], [ignore_extra_columns], [hive_partitioning], [file_format])` | Register pre-existing data files of an existing DuckLake table without rewriting. `file_format => 'parquet'` (default) mirrors upstream's `ducklake_add_data_files`; `'lance'` registers externally-written Lance dataset directories and `'vortex'` single `.vortex` files (both opaquely: row count scanned through the read engine, no stats/name map). Partitioned lance/vortex datasets are registered with `hive_partitioning => true`, which learns identity-partition values from the `key=value/` path (the partition column must be present in the file). |
| `flush_inlined_data(schema_name, table_name)` | Materialize a table's inlined rows (written cross-engine by DuckDB under `data_inlining_row_limit`) into a Parquet data file and clear the inlined rows, atomically. Unblocks DELETE/UPDATE/MERGE, which are gated while a table has inlined rows. No-op when nothing is inlined; not supported for partitioned tables yet. |
| `rewrite_data_files(schema_name, table_name, [file_size_threshold], [target_file_size], [reclaim_sources_immediately])` | Compaction / `optimize`: merge a table's small data files into fewer larger files through the real read path (so delete files / partial files / schema evolution all apply). Per-partition groups for partitioned tables; size-bounded output (`target_file_size`, default 512MB). Registers the merged files + end-snapshots the sources atomically; `reclaim_sources_immediately => true` writes the partial-emitting (`merge_adjacent`) variant that deletes sources outright. |
| `remove_orphan_files(schema_name, table_name, [retention_threshold], [dry_run])` | Delete files under the table's data path that no catalog row references (the residue of failed/aborted commits) and that are older than `retention_threshold` (default `'7d'`). The threshold is floored by `ducklake.remove-orphan-files.min-retention` (default `7d`) — that grace period protects files an in-flight, possibly cross-engine, writer just produced. `dry_run => true` logs what would be deleted and removes nothing. Touches storage only (no catalog mutation). Mirrors upstream `ducklake_delete_orphaned_files`. |
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
files already scheduled for deletion; lance/vortex dataset *directories* are matched by prefix so
their member files are never mistaken for orphans. Touches storage only — no snapshot, no catalog
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

## Configuration

| Property | Required | Default | Description |
|----------|:--------:|---------|-------------|
| `ducklake.catalog.database-url` | Yes | — | JDBC URL for the catalog metadata DB. PostgreSQL: `jdbc:postgresql://host:port/db`. Local DuckDB: `jdbc:duckdb:/abs/path/to/lake.db` |
| `ducklake.catalog.database-user` | Conditional | — | Catalog database username. Required for PostgreSQL; omit for local DuckDB |
| `ducklake.catalog.database-password` | Conditional | — | Catalog database password. Required for PostgreSQL; omit for local DuckDB |
| `ducklake.data-path` | Yes | — | Base path for data files |
| `ducklake.catalog.max-connections` | No | 10 | Max JDBC connections to catalog |
| `ducklake.default-snapshot-id` | No | — | Pin all reads to a snapshot ID |
| `ducklake.default-snapshot-timestamp` | No | — | Pin all reads to a point in time |
| `ducklake.duckdb.auto-httpfs-threshold` | No | `64MiB` | **EXPERIMENTAL.** File-size threshold for the `auto` setting of `duckdb_read_mode`. Files at or above this size stream via httpfs; smaller files materialize to local tmp. No effect when `data_file_format` is `parquet`. |
| `ducklake.execution-engine` | No | in-process | **EXPERIMENTAL.** DuckDB execution engine for non-parquet splits: in-process (default) or `quack` (out-of-process sidecar over Quack RPC; requires the `ducklake.quack.*` properties below). |
| `ducklake.quack.host` / `ducklake.quack.port` / `ducklake.quack.token` | With `quack` | — | **EXPERIMENTAL.** Quack sidecar endpoint + auth token. For lance/vortex s3 datasets, the sidecar container's environment also carries the `AWS_*` credential channel (see [README-lance-format.md](README-lance-format.md#s3-datasets--the-aws_-env-credential-channel)). |
| `ducklake.duckdb.parity-extension-path` | No | bundled | Filesystem path to the `trino_parity.duckdb_extension` binary; overrides the platform-matched binary bundled into the plugin jar. REQUIRED (server-side path) when the Quack engine is used. |
| `ducklake.temporal-partition-encoding` | No | `calendar` | **Deprecated.** Default `calendar` is the DuckLake 1.0 spec contract; `epoch` retained for legacy catalogs |
| `ducklake.temporal-partition-encoding-read-leniency` | No | `true` | **Deprecated.** Companion to the above; accepts both encodings on read |

### Session Properties

| Property | Default | Description |
|----------|---------|-------------|
| `read_snapshot_id` | — | Pin reads in this session to a snapshot ID |
| `read_snapshot_timestamp` | — | Pin reads in this session to an ISO-8601 instant |
| `data_file_format` | session-inherited | `'parquet'`, `'duckdb'`, `'vortex'`, or `'lance'`. Controls the format of writes (CTAS / INSERT) in this session. When unset, inherits the format of the most recent existing data file in the table (parquet for empty tables). The table-level `data_file_format` property overrides this for a specific `CREATE`. All non-parquet formats are **EXPERIMENTAL**. |
| `duckdb_writer_mode` | `arrow_stream` | **EXPERIMENTAL.** `'arrow_stream'` (default, columnar) or `'appender'` (JDBC Appender; kept for comparison). No effect when `data_file_format` is `parquet`. |
| `duckdb_read_mode` | `httpfs` | **EXPERIMENTAL.** Read strategy for duckdb-format data files: `'httpfs'` (DuckDB streams blocks from S3), `'materialize'` (download `.db` to local tmp then ATTACH), or `'auto'` (per-file decision against `ducklake.duckdb.auto-httpfs-threshold`). No effect when `data_file_format` is `parquet`. |
| `pushdown_timestamp_with_timezone` | `true` | Enable function pushdown of date/time predicates over `TIMESTAMP WITH TIME ZONE` columns on the duckdb-format read path (Tier C). On by default. Requires successful `SET TimeZone` on attach — automatic for all named IANA zones and integer-hour offsets; fractional bare-offset session zones get a one-shot WARN and pushdown silently degrades to Trino-side evaluation. Set to `false` to keep these predicates above the scan. See [`dev-docs/TODO-pushdown-duckdb.md`](dev-docs/TODO-pushdown-duckdb.md). |

Snapshot resolution precedence: query clause > session property > catalog config > current snapshot.

## Not Yet Implemented

### Maintenance Operations

All maintenance operations are shipped as `CALL system.*` procedures — see
[Procedures](#procedures): `rewrite_data_files` (compaction / `optimize`, both non-partial and
partial-emitting shapes), `expire_snapshots`, `cleanup_old_files`, `remove_orphan_files`,
`flush_inlined_data`, plus stats recompute via `ANALYZE`. The connector exposes these as
procedures rather than the `ALTER TABLE ... EXECUTE optimize` alias (which needs a separate SPI for
no capability gain). Design notes: [dev-docs/DESIGN-maintenance.md](dev-docs/DESIGN-maintenance.md).

### DDL

- `ALTER TABLE SET TYPE` (type promotion) — deferred (the read path assumes column types are
  stable across snapshots).

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
| PostgreSQL | Supported | Multi-process; preferred for shared / clustered deployments |
| Local DuckDB `.db` file | Supported | Single-process; dev, single-node Trino, interactive use |
| SQLite | Planned | Single-process; lighter-weight alternative to local DuckDB |
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
  filtering is verified against parquet, duckdb, vortex, and lance data files, and both directions
  (Trino-writes/DuckDB-reads and DuckDB-writes/Trino-reads) are cross-engine tested for each format.
- The duckdb-format data file path (`data_file_format = 'duckdb'`) is **EXPERIMENTAL**.
  Read modes (`materialize` / `httpfs` / `auto`), writer modes (`arrow_stream` / `appender`),
  function pushdown, and Tier C TIMESTAMP-WITH-TIME-ZONE semantics are all wired and
  tested but the API surface is still evolving — see [DuckDB-Format Data Files](#duckdb-format-data-files-experimental).
  The user-visible WTZ rendering change (session-zone instead of UTC) applies to
  duckdb-format reads only; parquet-format reads through Trino's standard reader
  still render WTZ in UTC.
- Vortex and lance formats are **EXPERIMENTAL** with format-specific write gates from upstream
  issues: vortex rejects `MAP` columns at schema time (native failure in the vortex extension's
  MAP COPY), lance rejects `ROW` columns (NULL-struct loss in the arrow-scan → lance COPY leg)
  and `MAP` columns (the installed lance extension needs file format 2.2+ and otherwise fails
  the COPY at close) at schema time. Reads of externally-written files with those types work. Streaming s3 reads
  of both formats require the `AWS_*` env credential channel (Rust `object_store` ignores
  DuckDB httpfs secrets) — see [README-lance-format.md](README-lance-format.md#s3-datasets--the-aws_-env-credential-channel).
- The lance search table functions reject (v1): mixed-format tables, tables with row-level
  deletes, and s3-resident datasets on the in-process engine (Quack-only).

## Additional Documentation

- [SAMPLES.md](SAMPLES.md) — SQL examples for DDL, DML, time travel, metadata tables, and DuckDB interop
- [TODO for READ side](dev-docs/TODO-READ-MODE.md) — Read mode open items + research notes
- [TODO for WRITE side](dev-docs/TODO-WRITE-MODE.md) — Write mode open items + design rationale
- [Pushdown reference (DuckDB-format reads)](README-duckdb-format-pushdown-reference.md) — complete current surface: predicate/pruning, operators, transforms, all ~95 functions
- [Lance format & vector search reference](README-lance-format.md) — search function arguments, exact-top-k recipe, prefilter semantics, s3 channel, write gates, upstream canary
- [TODO for pushdown](dev-docs/TODO-pushdown-duckdb.md) — Pushdown program tracker (steps 1-6, catalog totals, round notes)
- [TODO for DuckDB-format support](dev-docs/TODO-duckdb-lake-format.md) — Living tracker for the `.db` format feature (phases, test gaps)
- [TODO for lance](dev-docs/TODO-lance.md) / [TODO for vortex](dev-docs/TODO-vortex.md) — format trackers incl. probe findings and the Route A/B decision record
- [Lance Route A-vs-B benchmark](dev-docs/archive/REPORT-lance-route-a-vs-b.md) — why the DuckDB-extension architecture stays primary
- [dev-docs/archive/](dev-docs/archive/) — Historical context: closed work, archived plans, the DuckLake 1.0 spec impact reference, the reuse audit, the date/time pushdown program design + empirical TZ findings, and the per-extension community catalog detail
