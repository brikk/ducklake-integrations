# Comparison — `pg_ducklake` (Rely Cloud, thin glue over upstream) vs this repo (JVM)

> **Freshness (JVM side, 2026-07):** since this snapshot our JVM implementation has SHIPPED the
> **change feed** (`table_insertions`/`table_deletions`/`table_changes`) and the **full maintenance
> suite** (`remove_orphan_files`, `expire_snapshots`, `cleanup_old_files`, `flush_inlined_data`,
> `rewrite_data_files`/optimize). This supersedes the two "Our JVM: No" rows (Data change feed;
> Maintenance) and the whole "Maintenance operations — scope notes for roadmap" section below, which
> is now retrospective rather than future work. Change feed reads DuckLake's embedded row-lineage
> column, so lineage-preserving UPDATEs pair into `update_preimage`/`update_postimage`; Trino's own
> writes emit no lineage column, so a Trino-written UPDATE surfaces as `delete`+`insert`.

Snapshot of the `pg_ducklake` project reviewed against our JVM implementation
(`ducklake-catalog` + `trino-ducklake`).

Last refreshed against upstream `main` at `011ab8d` (2026-05-18). The vendored
DuckLake reference C++ extension at `third_party/ducklake/` was **not** bumped in
this refresh — all 7 incoming commits touch only pg_ducklake's PG-side glue
(direct-insert fast path, hooks, metadata manager helpers, new CREATE TABLE WITH
options, observability), so there are no new spec-level signals.

**What this comparison actually is.** `pg_ducklake` is thin: it static-links the upstream
DuckDB `ducklake` extension (`third_party/ducklake/`, same repo as the DuckLake spec) and
wires it into PostgreSQL via `pg_duckdb` hooks, event triggers, and a background worker.
The catalog-write and maintenance logic all lives in the upstream C++ extension. That
extension is authored by the DuckDB/DuckLake team and is effectively the **reference
implementation**. So this document is mostly a **spec-conformance audit of our JVM
write/read paths against the reference C++**, using `pg_ducklake`'s docs as a secondary
enumeration of supported features.

Where `pg_ducklake` does add something interesting on top (PG event-trigger DDL plumbing,
Postgres-native types for inlined data, a maintenance background worker, role-based
access, an FDW for cross-catalog reads), that's called out separately.

## TL;DR

The reference largely agrees with what we do. No catastrophic spec-level breakage. Most
of the original deltas from this audit have since been closed (changes_made quoting,
inlined `list<T>` reads including the trailing `list<blob>` hex-escape + `list<uuid>`
16-byte packing cases, `int128`/`uint128` mapping, `linestring_z`, UUIDv7,
`renamed_view` → `altered_view` folding, schema_version bumps on view/schema DDL,
unsigned write-side range checks, per-table `location` table property, puffin
deletion-vector reads). Maintenance operations are a major feature area we still don't
expose, but the scope is well understood and tractable.

## Bugs / fragility in our JVM impl surfaced by this audit

Earlier audit rounds surfaced 5 real bugs (B1–B5) plus 2 additional gaps (C1, C2);
all are **closed** with regression coverage:

- B1 (snapshot_changes quoting) — `writeQuotedValue()` + `changeCreated{Table,View,Schema}`
  helpers + `TestJdbcDucklakeCatalogChangesMadeFormat` + cross-engine pathological-name tests.
- B2 (inlined `list<T>` reads incl. `list<uuid>`, `list<blob>`) — `DucklakeInlinedValueConverter`
  parses DuckDB's `[elem, ...]` text form with escape handling + native PG-array fallback.
- B3 (int128/uint128 mapping) — `int128` → `DECIMAL(38, 0)`, `uint128` → `VARCHAR`;
  `TestDucklakeTypeConverter` + cross-engine round-trips.
- B4 (`linestring_z` underscore vs legacy space form) — converter matches both.
- B5 (unsigned-write range checks) — `DucklakeUnsignedRangeChecker` before Parquet
  writer; cross-engine `testTrinoRejectsOutOfRangeInsertsIntoUnsignedColumns`.
- C1 (`renamed_view:<viewId>` not a spec change-type) — emit `altered_view:<viewId>`;
  pinned by `testDuckdbParsesTrinoWrittenViewAndSchemaDdlChanges`.
- C2 (`schema_version` not bumped on view/schema DDL) —
  `DucklakeWriteTransaction.incrementSchemaVersion()` wired into all view/schema DDL;
  pinned by `testSchemaVersionBumpsOnViewAndSchemaDdl`.

For the original deep findings and exact file:line / test names, walk git history
in `JdbcDucklakeCatalog.java`, `DucklakeInlinedValueConverter.java`,
`DucklakeTypeConverter.java`, `DucklakeUnsignedRangeChecker.java` — each fix has a
distinct commit referencing the bug ID.

### Still open

| # | Finding | Severity |
|---|---|---|
| B6 | **Decimal `p > 38` errors hard.** Trino can't represent it, so there is no graceful path — but right now we throw at type construction time rather than at `CREATE TABLE` time with a clearer message. Upstream allows higher precisions. Unlikely to be hit. | Low |

## Things pg_ducklake (and therefore the reference) does that we don't

| Feature | Upstream | Our JVM | Notes |
|---|---|---|---|
| **Data change feed** (`table_changes`, `table_insertions`, `table_deletions`) | Yes | **Yes** (shipped) | `system.*` table functions over data/delete files per snapshot range. Reads the embedded row-lineage column so lineage-preserving UPDATEs pair into update pre/post-image; Trino-written UPDATEs surface as `delete`+`insert`. Inlined-data tables are gated. |
| **Virtual columns** (`rowid`, `snapshot_id`, `filename`, `file_row_number`, `file_index`) | Yes | No | These are reserved column expressions exposed in scan. Trino could expose them via synthetic column handles. Likely useful for our MERGE story too. |
| **Sorted tables** (write path applies sort) | Yes | No (readable, sort ignored) | pg_ducklake exposes `ducklake_sorted` index AM + `set_sort()`. Our README acknowledges we don't sort on write. |
| **`variant` with `->`/`->>` extraction** | Yes (pg_ducklake wraps it) | No (degrades to VARCHAR) | Real structural type. Requires Trino-side support to be useful; VARCHAR degradation is honest. |
| **Maintenance: `flush_inlined_data`, `merge_adjacent_files`, `rewrite_data_files`, `expire_snapshots`, `cleanup_old_files`, `cleanup_orphaned_files`** | Yes | **Yes** (shipped) | Shipped as `CALL system.*`: `flush_inlined_data`, `rewrite_data_files` (optimize + `merge_adjacent` variant), `expire_snapshots`, `cleanup_old_files`, `remove_orphan_files`, plus `ANALYZE`. |
| **`set_commit_message()`** (author/message on snapshot) | Yes | No (session properties planned) | Trivial once we add session properties for `commit_author`, `commit_message`, `commit_extra_info`. |
| **`freeze()` / export-to-`.ducklake` single-file** | Yes | No | Nice-to-have. |
| **CHECKPOINT (umbrella maintenance)** | Yes | No | Downstream of the individual ops above. |
| **Per-table data path at CREATE** | Yes — `CREATE TABLE ... USING ducklake WITH (ducklake.table_path = '...')` (pg_ducklake #199, 2026-05-18) | Yes — `CREATE TABLE ... WITH (location = '...')` (2026-05-20) | Closed. `DucklakeTableProperties.LOCATION_PROPERTY` flows into `JdbcDucklakeCatalog.createTable` and lands in `ducklake_table.path` / `path_is_relative`. URI-scheme prefix → absolute, no traversal segments, trailing slash normalized. |

## Things we do that pg_ducklake doesn't

| Feature | Our JVM | pg_ducklake | Notes |
|---|---|---|---|
| **`CREATE SCHEMA`** | Yes | **No** (!) | Surprising: pg_ducklake feature coverage lists this as unsupported. We read/write DuckLake schemas correctly; pg_ducklake pins tables to a single PG-catalog schema. |
| **`CREATE VIEW` / `DROP VIEW`** | Yes (Trino dialect, cross-dialect filtered) | No | pg_ducklake flags views + macros as todo. |
| **Encryption (PME reads)** | Partial (reads encrypted Parquet if key supplied) | No | datafusion-ducklake is further along here than both of us. |
| **File-level partition pruning via column stats** | Yes | Inherited from DuckDB; not explicitly tested | Upstream does it in DuckDB's optimizer. We do it in the Trino split manager. |
| **Cross-engine compatibility harness** (Trino writes, DuckDB reads via shared PG catalog) | Yes (`TestDucklakeCrossEngineCompatibility`) | Not explicitly tested | pg_ducklake doesn't need to test the "foreign writer" case because its writer *is* DuckDB. |

## pg_ducklake-specific add-ons (not portable to us)

These are things that exist because pg_ducklake lives *inside* Postgres; they aren't
spec features and they don't apply to our Trino connector:

- Event-trigger DDL plumbing (`pgducklake_ddl.cpp`)
- Role-based access control (`ducklake_superuser`/`writer`/`reader`)
- Foreign data wrapper for read-only access (`pgducklake_fdw.cpp`)
- `IMPORT FOREIGN SCHEMA` bulk import
- Direct insert fast path for `INSERT ... SELECT UNNEST($n)` and `INSERT ... VALUES`.
  Several recent fixes here (#188 partial-column-list with DEFAULT, #190 deferred VALUES
  evaluation for STABLE coercions, #193 snapshot_id race surfaced as SQLSTATE 40001) are
  specific to PG's planner and the inlined-heap write path — not portable. Worth noting:
  the SQLSTATE 40001 race (#193, 2026-05-16) is the same hazard our
  `ensureSnapshotLineageUnchanged()` guards against; pg_ducklake surfaces it as a
  serialization-failure SQLSTATE, we throw `DUCKLAKE_TRANSACTION_CONFLICT`. Same shape,
  same trigger, different driver — at parity.
- Direct-insert observability (`ducklake.direct_insert_stats()`, pg_ducklake #187,
  2026-05-12): per-process counters bucketed by why a candidate insert took the fast vs
  slow path (`matched_values`, `unmatched`, `schema_version_mismatch`, …). PG-specific
  because the buckets correspond to pg_ducklake's planner decisions, but the pattern
  ("expose connector-internal optimization stats") is generic. If we add a Trino
  fast-path later, an analogous JMX/SHOW STATS surface would be cheap.
- Background maintenance worker (PG-style autovacuum-shaped launcher + per-database
  workers). Trino equivalent would be operator-scheduled `CALL` procedures; same
  functions, different driver.

## Maintenance operations — scope notes for roadmap

Each maintenance op is its own snapshot on commit; all are snapshot-isolated from
concurrent DML (append-only catalog semantics, no locks).

- **`flush_inlined_data`** — Reads `ducklake_inlined_data_<tableId>_<schemaVersion>`,
  writes one or more Parquet files, inserts matching `ducklake_data_file` rows, drops
  the inlined rows. Atomic in one snapshot. Must also handle inlined deletes by writing
  a delete file. **Stream rather than buffer.** pg_ducklake #195 (2026-05-15) hit OOM
  reading large inlined heaps into a single result set; the fix added a streaming SPI
  cursor. When we implement this, pull rows in bounded batches rather than
  `select * from ducklake_inlined_data_<...>` into memory.
- **`expire_snapshots`** — Deletes rows from `ducklake_snapshot` older than a retention
  window. Catalog-only; never deletes data files (that's `cleanup_old_files`). Must
  never expire the current snapshot.
- **`merge_adjacent_files`** — Bin-packs small data files into bigger ones. Skips files
  with delete files attached. Appends the merged file, marks source files' `end_snapshot`.
- **`rewrite_data_files`** — For files whose deletion ratio exceeds a threshold, reads
  data + delete files and writes a compacted Parquet without the deleted rows. Marks
  source data+delete files closed. May produce `partial_max` on delete-file consolidation.
- **`cleanup_old_files`** — Deletes from storage any file whose `end_snapshot` is
  earlier than the oldest live snapshot. Race-safe under snapshot isolation: files
  referenced by any live snapshot remain.
- **`cleanup_orphaned_files`** — Removes files present in storage but not referenced by
  the catalog (e.g., orphans from failed commits). Listing the storage prefix is
  necessary, which is why this is typically an opt-in batch job.
- **CHECKPOINT** — Umbrella. Runs flush → expire → merge + rewrite → cleanup. One
  snapshot per op. Useful entry point for an operator.

**Recommended Trino surface**: expose each as `ALTER TABLE <t> EXECUTE <op>(...)` or as
connector procedures `CALL ducklake.<op>(...)`. Let operators (Airflow, cron, a periodic
task) schedule. Don't run a background worker inside the coordinator — Trino's process
model isn't shaped for it, and scheduling externally is more observable anyway.

## Test-coverage deltas

pg_ducklake's regression suite has ~43 `.sql` files plus 3 pg_isolation specs. A few
categories exist there that we don't exercise in our Java tests:

- **Concurrency isolation specs** — pg_ducklake has dedicated `concurrent_writes`,
  `concurrent_cross_table_writes`, `explicit_transaction_commit` specs using
  pg_isolation's multi-session framework. We cover `ensureSnapshotLineageUnchanged()`
  in exactly one Trino integration test (`TestDucklakeDDLIntegration.testConcurrentSchemaCommitsFailWithTransactionConflict`).
  Worth building: writer-vs-writer on the same table, writer-vs-writer on different
  tables (catches snapshot-ID cross-talk), and explicit-rollback visibility.
- **`data_change_feed`** — tied to the feature above.
- **`virtual_columns`** — tied to the feature above.
- **`hybrid_scan`** — mixed inline + Parquet reads in one query. We do have `TestDucklakeInlinedValueConverter`
  but not a full hybrid integration test.
- **`inlined_data_schema_change`** — ALTER during an accumulated inline buffer.
  Interesting edge case we probably don't hit.
- **`maintenance.sql`** — moot until we add maintenance ops.

Categories we can ignore (PG-specific): `fdw`, `frozen_fdw`, `import_foreign_schema`,
`access_control`, `ddl_triggers`, `gucs`, `connection_string`, `recycle_ddb`,
`non_ducklake_commit`.

## Action items still open

- **Start planning maintenance ops.** Scope above. Earliest wins are probably
  `expire_snapshots` and `cleanup_orphaned_files` — both are catalog-driven and don't
  require a compaction engine.
- **Add a concurrency isolation test suite.** Two scenarios first: two writers on the
  same table, two writers on different tables. These are the scenarios pg_isolation's
  specs protect against and our code has no tests for.
- **Consider session properties** for `commit_author`/`commit_message`/`commit_extra_info`.
  Already on the roadmap; pg_ducklake exposing `set_commit_message()` is a nudge it's a
  commonly wanted feature.

## Appendix — Type mapping differences worth knowing

pg_ducklake's `docs/data_types.md` is the authoritative record of how the upstream
Postgres metadata manager encodes **inlined** data in PG columns. Our
`DucklakeInlinedValueConverter` must match this when reading inlined data from a DuckDB
or pg_ducklake writer.

| DuckLake type | Inlined PG column (reference) | Our assumption | Match? |
|---|---|---|---|
| `boolean`, `int*`, `float*`, `decimal`, `time`, `timetz`, `interval`, `json`, `uuid` | Native PG types | Native | ✓ |
| `uint8`/`uint16` | INTEGER | SMALLINT/INTEGER | Mismatch — verify we don't assume SMALLINT for `uint8` |
| `uint32` | BIGINT | BIGINT | ✓ |
| `uint64` | VARCHAR | — | Verify converter |
| `int128` | VARCHAR | VARCHAR → `DECIMAL(38, 0)` on read | ✓ B3 fixed |
| `uint128` | VARCHAR | VARCHAR → VARCHAR on read (degraded) | ✓ B3 fixed |
| `date`, `timestamp`, `timestamp_s/ms/ns`, `timestamptz` | VARCHAR | String-parse via `DucklakeInlinedValueConverter` | ✓ Verified for `date`, `timestamp`, `timestamptz` via cross-engine fixtures |
| `varchar`, `blob` | BYTEA (not text — to allow null bytes) | BYTEA | ✓ Verified with embedded null byte + high-bit bytes |
| `list<T>` (primitive T) | VARCHAR (DuckDB `[elem, ...]` text form) OR VARCHAR[] (PG array) | Parses both | ✓ B2 fully fixed — primitives, `list<uuid>`, and `list<blob>` (`\xNN` hex decode) all round-trip |
| `list<struct>` / `list<map>` / `list<list>` | nested | Throws `UnsupportedOperationException` | Tracked separately |
| `struct`, `map` | VARCHAR (DuckDB serialized form) | Treated as string | Probably broken for anything non-trivial |
| `variant`, geometry family | No inline (Parquet only) | n/a | ✓ |
