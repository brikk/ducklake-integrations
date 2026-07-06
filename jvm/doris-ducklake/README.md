# Apache Doris DuckLake Connector — Feature Support

> ⚠️ **Pre-alpha. Do not use in production.** This is an out-of-tree Doris
> catalog **SPI plugin** for the [DuckLake](https://ducklake.select) open
> table format (spec v1.0). It targets the unreleased `fe-connector` catalog
> SPI on Apache Doris's `branch-catalog-spi` (baseline `8b391c7`, the P6
> iceberg SPI cutover) and depends on a two-line FE patch
> ([`fe-patches/`](fe-patches/)) not present in any Doris release, so a stock
> Doris build cannot load it. The connector is **read-focused today** — the
> write path is nascent (basic INSERT/CTAS/DDL work; DELETE/UPDATE/MERGE do
> not). If you want a working DuckLake adapter for an OLAP engine today, use
> [`trino-ducklake`](../trino-ducklake/) instead.

Connector for the DuckLake format using **PostgreSQL** as the catalog metadata
backend (the shared, multi-process shape a Doris FE+BE cluster needs). Tested
with DuckDB 1.5.4 for cross-engine compatibility.

## Architecture — why this is read-focused, and different from Trino

Doris and Trino run the DuckLake plugin very differently, and it shapes the
whole feature surface:

| | `trino-ducklake` | `doris-ducklake` |
|---|---|---|
| Where scans execute | **In-JVM** (Trino ParquetReader, or DuckDB for the `.db` path) | **In the Doris BE** (C++), via its native Parquet reader |
| Connector's job | Metadata **and** data: plans splits *and* produces pages | **FE-side only**: metadata + scan *planning*; emits Iceberg-shaped thrift ranges, the BE does the reading |
| Predicate eval | Trino operators, or pushed into DuckDB | Doris BE evaluates during its native scan; `applyFilter` hands BE a filter + prunes files |
| `trino_parity` extension | Central to the DuckDB path | **Not in the read path at all** |
| DuckDB-native `.db` files | Read via DuckDB executor | **Unreadable by BE** → Parquet-only |

**Consequences:** the Doris connector is simpler on execution (no executor /
Arrow bridge, no function pushdown to translate) but is **bounded by what the
Doris BE Parquet reader accepts** — which is where the one open read blocker
lives (position-delete nullability, below). Function/expression pushdown and
DuckDB-native file reads are **out of scope by design** (execution-model
mismatch), not backlog.

## Build

Requires JDK 17 for the FE-facing plugin ABI, built on the project-wide JDK 25
toolchain (Kotlin emits 17 bytecode). Managed via [mise](https://mise.jdx.dev/)
(`jvm/mise.toml`) or [SDKMan](https://sdkman.io/).

```shell
cd jvm
./gradlew :doris-ducklake:assemble        # builds the plugin zip
```

The plugin zip is at
`doris-ducklake/build/distributions/doris-ducklake-<version>-plugin.zip` — a
flat `lib/` layout containing the connector jar plus the runtime deps the FE
parent classloader doesn't already supply (Postgres driver, jOOQ, kotlin
stdlib, …). SPI jars (`fe-connector-api`/`spi`, `fe-thrift`, iceberg) are
`compileOnly` — the FE provides them at runtime.

### FE patches

The plugin needs two one-line FE guards it can't self-apply (whitelist
`type=ducklake` in `SPI_READY_TYPES`; pad `ENGINE_ICEBERG` for no-ENGINE
CREATE TABLE). They're kept as a reapplyable patch and are tracked upstream
asks — see [`fe-patches/FE-PATCHES.md`](fe-patches/FE-PATCHES.md).

### Running tests

The module tests use Testcontainers (PostgreSQL + a real DuckLake catalog via
DuckDB's `postgres`/`ducklake` extensions) — no live cluster needed:

```shell
./gradlew :doris-ducklake:test :doris-ducklake:detekt
```

The **corpus replay** test mirrors upstream DuckLake reads through a live
compose FE+BE — see [Upstream corpus replay](#upstream-corpus-replay).

## Install

Unzip the plugin into the FE's `plugins/connector/ducklake/` directory
(the compose overlay does this into a named volume; see below), then restart
the FE (plugins load at startup). Create a catalog:

```sql
CREATE CATALOG dl PROPERTIES (
    'type'              = 'ducklake',
    'metadata.url'      = 'jdbc:postgresql://<host>:5432/ducklake',
    'metadata.user'     = 'ducklake',
    'metadata.password' = '<password>',
    'storage.warehouse' = 's3://<bucket>/<prefix>/',
    -- S3 storage credentials (forwarded to the BE Parquet reader):
    's3.endpoint'       = 'http://<host>:9000',
    's3.region'         = 'us-east-1',
    's3.access_key'     = '<key>',
    's3.secret_key'     = '<secret>',
    'use_path_style'    = 'true'
);
```

### Local dev stack (Docker Compose)

An FE+BE cluster (over the shared Postgres+MinIO substrate the trino stack
uses) is in [`compose/`](compose/). One-command bring-up + read/write smoke:
`compose/smoke.sh`; headless bring-up for the corpus/dev loop:
`compose/smoke.sh --up-only`. See [`compose/README.md`](compose/README.md).

## Type System

DuckLake catalog type → Doris type. **Read** covers every type below (the BE
reads the Parquet body). **Write** (CREATE TABLE column types) covers the
non-degraded core only.

| DuckLake Type | Doris Type | Read | Write | Notes |
|---------------|------------|:----:|:-----:|-------|
| `boolean` | BOOLEAN | Yes | Yes | |
| `int8` (tinyint) | TINYINT | Yes | Yes | BE Iceberg writer crashes on narrow-int writes today — see Known Limitations |
| `int16` (smallint) | SMALLINT | Yes | Yes | (narrow-int write caveat) |
| `int32` (integer) | INT | Yes | Yes | |
| `int64` (bigint) | BIGINT | Yes | Yes | |
| `uint8` | SMALLINT | Yes | — | Widened on read |
| `uint16` | INT | Yes | — | Widened on read |
| `uint32` | BIGINT | Yes | — | Widened on read |
| `uint64` | DECIMALV3(20,0) | Yes | — | Widened on read |
| `int128` | DECIMALV3(38,0) | Yes | Yes | Doris LARGEINT ↔ int128 on write |
| `uint128` | STRING | Yes | — | Degraded — no numeric fit past 38 digits |
| `float32` | FLOAT | Yes | Yes | |
| `float64` | DOUBLE | Yes | Yes | |
| `decimal(p,s)` | DECIMALV3(p,s) | Yes | Yes | Precision up to 38 |
| `varchar` | STRING | Yes | Yes | |
| `blob` | VARBINARY | Yes | Yes | |
| `uuid` | VARBINARY(16) | Yes | — | Degraded — raw bytes, no UUID type |
| `date` | DATEV2 | Yes | Yes | |
| `timestamp` | DATETIMEV2(6) | Yes | Yes | Microsecond precision (DuckLake native) |
| `timestamp_s` | DATETIMEV2(0) | Yes | Yes | Second precision |
| `timestamp_ms` | DATETIMEV2(3) | Yes | Yes | Millisecond precision |
| `timestamp_ns` | DATETIMEV2(6) | Yes | — | Degraded — Doris caps datetime scale at 6; nanos clamp to micros (does not round-trip on write) |
| `timestamptz` | TIMESTAMPTZV2(6) | Yes | Yes | Zone-aware; stored at microsecond precision |
| `time` | STRING | Yes | — | Degraded — Doris has no first-class TIME |
| `timetz` | STRING | Yes | — | Degraded |
| `list<T>` | ARRAY(T) | Yes | — | Full nesting on read |
| `struct<...>` | STRUCT(...) | Yes | — | Full nesting on read |
| `map<K,V>` | MAP(K,V) | Yes | — | Full nesting on read |
| `json` | STRING | Yes | — | Degraded — stored as string, no JSON functions |
| `variant` | STRING | Yes | — | Degraded |
| `interval` | STRING | Yes | — | Degraded |
| `geometry` / `point` / `linestring` / `polygon` / `multipoint` / `multilinestring` / `multipolygon` / `geometrycollection` | VARBINARY | Yes | — | Degraded — raw bytes, no spatial functions |

"Degraded" means data is preserved and reads correctly, but type-specific
operators/functions aren't available through Doris. Write of nested and most
degraded types is not yet implemented (v1 CREATE TABLE accepts the
non-degraded scalar core).

## Read Operations

Verified against the upstream DuckLake corpus (live FE+BE mirror vs a DuckDB
oracle) across catalog, time_travel, types, insert, delete, update, merge,
partitioning, schema_evolution, stats, alter, view, general, table_changes,
metadata, and comments dirs.

| Feature | Supported | Notes |
|---------|:---------:|-------|
| SELECT / table scans | Yes | Planned FE-side; BE reads Parquet natively |
| Projection pushdown | Yes | Column pruning via `applyProjection` |
| Predicate pushdown (WHERE) | Yes | `applyFilter` hands the BE a filter + prunes whole files via `ducklake_file_column_stats` |
| Partition pruning | Yes | Identity and temporal partitions |
| Bucket partition pruning | Yes | Murmur3 hash; equality predicates pruned (ranges aren't — bucketing scrambles ordering) |
| Partition-bearing scan ranges | Yes | Identity partition values surfaced so the BE stops path-parsing the (non-hive) layout |
| COUNT(*) pushdown | Yes | Served from `ducklake_data_file.record_count` when exactly safe; refuses (BE counts by reading) on any filter, delete file, inlined state, non-latest snapshot, or partial (compacted) file |
| Parquet data files | Yes | Via the Doris BE native Parquet reader |
| Time travel — FOR VERSION AS OF | Yes | By snapshot ID (DuckLake version == snapshot id) |
| Time travel — FOR TIMESTAMP AS OF | Yes | Resolves to the snapshot active at-or-before the instant |
| Snapshot pinning (MVCC) | Yes | `beginQuerySnapshot` / `applySnapshot` thread the resolved snapshot onto the scan |
| Table statistics | Yes | Row count + on-disk size from `ducklake_table_stats` |
| Schema evolution on read | Yes | Renamed / added columns read correctly (added columns NULL for older rows); across the alter suite |
| Inlined data (small tables) | No | **Guarded** — reads over live `ducklake_inlined_*` rows fail loudly at plan time (rows live in the catalog DB, not Parquet). Serving them is the top read backlog item. |
| Delete files (merge-on-read) | Blocked | FE plumbing done; **blocked on the BE** — DuckLake position-delete Parquet uses OPTIONAL columns, the BE Iceberg reader requires REQUIRED (`[CORRUPTION] Not nullable column has null values`). See Known Limitations. |
| Time travel over a compaction boundary | Guarded | A read AS OF a snapshot older than a `merge_adjacent_files` compaction needs a per-row hidden-column snapshot filter the BE can't apply — fails loudly instead of over-returning. Latest-snapshot reads unaffected. |
| Views | No | DuckLake views are not surfaced yet (they skip cleanly) |
| Function / expression pushdown | No | **Out of scope** — the BE evaluates predicates natively; there's no in-JVM engine to push to |
| DuckDB-native `.db` / Vortex / Lance data files | No | **Out of scope** — the BE has no reader for them; Parquet-only |

## Write Operations

The write path is **nascent** — basic INSERT/CTAS and metadata DDL work
end-to-end (Doris writes a `TIcebergTableSink`; the BE writes Parquet and
reports commit data the connector maps into DuckLake). Row-level DML and the
richer DDL/maintenance surface are not built.

| Feature | Supported | Notes |
|---------|:---------:|-------|
| INSERT INTO | Yes | BE writes Parquet via the Iceberg sink; commit mapped to DuckLake data files. Verified cross-engine (Doris writes, DuckDB reads back). |
| CREATE TABLE AS SELECT | Yes | Composed DDL + INSERT |
| CREATE SCHEMA / DROP SCHEMA | Yes | Non-empty schema drop rejected by the catalog |
| CREATE TABLE | Yes | Non-degraded scalar columns; Iceberg-style `PARTITIONED BY (…)` including `bucket(N, col)` |
| DROP TABLE | Yes | |
| Partitioned writes | Yes | Identity/temporal transforms + `bucket(N, col)` (Murmur3, verified equal to DuckLake) |
| DELETE / UPDATE / MERGE | No | Not built. The P6 SPI admits row-level DML by capability, so this is the main write build-out — see [`dev-docs/TODO-write.md`](dev-docs/TODO-write.md). |
| INSERT OVERWRITE / static-partition INSERT | No | Not declared |
| Sorted writes | No | |
| ALTER TABLE (columns/branches/partition fields) | No | Generic P6 DDL SPI is available; not adopted yet |
| Maintenance procedures (compaction, expire, …) | No | Generic P6 procedure SPI available; not adopted |

## Partitioning

| Transform | Read | Write | Notes |
|-----------|:----:|:-----:|-------|
| Identity | Yes | Yes | Partition by column value |
| `year(col)` | Yes | Yes | Date or timestamp column |
| `month(col)` | Yes | Yes | Date or timestamp column |
| `day(col)` | Yes | Yes | Date or timestamp column |
| `hour(col)` | Yes | Yes | Timestamp column |
| `bucket(N, col)` | Yes | Yes | Iceberg-compatible Murmur3 hash; equality predicates pruned, ranges not. `DISTRIBUTED BY` (Doris CRC32) is rejected, not mis-mapped. |

## Statistics

| Statistic | Supported | Notes |
|-----------|:---------:|-------|
| Table row count | Yes | From `ducklake_table_stats` |
| On-disk data size | Yes | From `ducklake_table_stats` |
| Column min/max (file-level, for pruning) | Yes | From `ducklake_file_column_stats`; drives file pruning in `applyFilter` |
| Bucket-partition pruning | Yes | Murmur3 target-bucket matching for equality/IN predicates |

## Cross-Engine Compatibility

| Direction | Tested | Notes |
|-----------|:------:|-------|
| DuckDB writes, Doris reads | Yes | Corpus mirror validates row-for-row against DuckDB |
| Doris writes, DuckDB reads | Yes | INSERT/CTAS Parquet round-trips (field_id mapping); bucket equivalence verified |
| Shared PostgreSQL catalog | Yes | Both engines operate on the same metadata |
| Schema evolution across engines | Yes | Read side, across the alter suite |

### Upstream corpus replay

The connector is verified against **DuckLake's own upstream test corpus** —
the sqllogictest files the reference C++ implementation ships in
`duckdb/ducklake` `test/sql/`, pinned as a git submodule in the sibling
[`ducklake-corpus-replay`](../ducklake-corpus-replay/) module. Each corpus file
is executed **verbatim** through an embedded DuckDB oracle (no dialect
translation), building real catalog state on an isolated PostgreSQL database;
every lake read the corpus performs is then re-executed through a **live Doris
FE+BE** against the *same* catalog and compared row-for-row against the
oracle's result. A divergence means Doris read the same DuckLake state
differently than DuckDB — the class of bug this harness exists to catch (first
contact found a real one: tables with inlined rows silently returned wrong
results; another found time-travel-over-compaction over-reads).

Because Doris is FE-metadata + BE-native-read (not in-JVM), the adapter only
**mirrors read-only queries** against the oracle-built catalog: it never runs
DuckDB-dialect writes, and a dialect gate (`DorisCorpusDialect`) skips SQL the
Doris path can't attempt (DuckDB-only syntax, degraded-type-returning
functions, `FILTER (WHERE)`, infinity temporal literals, catalog/file table
functions) — those count as documented engine-skips, never failures.

Requires a live cluster (`compose/smoke.sh --up-only`):

```shell
./gradlew :doris-ducklake:corpusReplayTest                              # starter dirs (~1 min)
./gradlew :doris-ducklake:corpusReplayTest -Dducklake.corpus.dirs=delete   # one dir
./gradlew :doris-ducklake:corpusReplayTest -Dducklake.corpus.dirs=all      # full corpus (CI/long)
```

## Configuration

Catalog properties on `CREATE CATALOG dl PROPERTIES (...)`:

| Property | Required | Description |
|----------|:--------:|-------------|
| `type` | Yes | Must be `ducklake` (whitelisted via the FE patch) |
| `metadata.url` | Yes | JDBC URL of the DuckLake metadata database (`jdbc:postgresql://host:5432/db`) |
| `metadata.user` | Yes | Metadata database username |
| `metadata.password` | No | Metadata database password (empty for trust auth) |
| `storage.warehouse` | Yes | Warehouse root, e.g. `s3://bucket/path` or `file:///local/path` |
| `s3.endpoint` / `s3.region` / `s3.access_key` / `s3.secret_key` | For S3 | Forwarded to the BE Parquet reader; the connector also emits the canonical `AWS_*` aliases the BE's `S3ObjStorage` expects |
| `use_path_style` | For S3 | `true` for MinIO / path-style S3 |

## Not Yet Implemented / Out of Scope

**Read backlog (real gaps):**
- **Serving inlined data rows** — turn the loud plan-time guard into an actual
  read (FE-side synthesis of a Parquet range from `readInlinedData`, or a JNI
  scanner seam). Also unblocks inline DELETE application.
- **BE position-delete nullability** — DuckLake writes position-delete Parquet
  with OPTIONAL columns; the BE Iceberg reader's fast path requires REQUIRED.
  Needs a BE-side fix (or a DuckLake writer change). Blocks merge-on-read
  delete *reads*.
- **Time-travel over compaction** — needs a BE hook to apply the hidden
  `_ducklake_internal_snapshot_id` per-row filter for partial files.
- **`ducklake_name_mapping`** — renamed columns over legacy `add_files`
  Parquet not yet applied on the Doris scan.
- **Views** — DuckLake views not surfaced.

**Write backlog:** DELETE/UPDATE/MERGE (P6 admits them by capability),
INSERT OVERWRITE, sorted writes, ALTER, maintenance procedures — see
[`dev-docs/TODO-write.md`](dev-docs/TODO-write.md).

**Out of scope by design (execution-model mismatch, not backlog):**
- Function / expression pushdown — the Doris BE evaluates predicates natively.
- DuckDB-native `.db`, Vortex, and Lance data files — the BE has no reader;
  Doris is Parquet-only. (Reconsider only if a BE JNI-scanner seam appears.)

The plan and per-phase working docs live in [`dev-docs/`](dev-docs/) — start
with [`dev-docs/PLAN.md`](dev-docs/PLAN.md) (phases: read-side Parquet fully →
write-side Parquet → other formats only if they still make sense for Doris).
