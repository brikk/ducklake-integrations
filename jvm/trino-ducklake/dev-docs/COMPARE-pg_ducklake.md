# Comparison — `pg_ducklake` (Rely Cloud, thin glue over upstream) vs this repo (JVM)

Snapshot of the `pg_ducklake` project reviewed against our JVM implementation
(`ducklake-catalog` + `trino-ducklake`).

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
of our deltas are either **already tracked** (hugeint/uhugeint, variant, partial_max,
Puffin, linestring_z, unsigned range validation, maintenance ops) or **cosmetic**. The
handful of new concerns surfaced here are: a couple of encoding/escaping fragility spots
on our write path, and a real gap against the reference in how we read *inlined* data
containing nested types. Maintenance operations are a major feature area we still don't
expose, but the scope is well understood and tractable.

## Bugs / fragility in our JVM impl surfaced by this audit

### Real bugs (worth fixing)

| # | Finding | File / line | Severity |
|---|---|---|---|
| B1 | **`snapshot_changes` column is built with `String.join(",", ...)` and no escaping.** Upstream uses `KeywordHelper::WriteQuoted()` with double quotes for values, e.g. `created_table:"main.users"`. If an entry ever contains a comma or quote (legal in table names), our row is malformed and the DuckDB parser will trip. Low probability in practice, but it's a silent correctness bomb. | `JdbcDucklakeCatalog.java:1090` | Medium |
| B2 | **Inlined `list<T>` rows stored as `VARCHAR[]` (Postgres array), not a serialized scalar.** Reference Postgres metadata manager writes native PG arrays (`INT4[]`, etc.) for inlined lists and lets the reader reconstruct. Our `DucklakeInlinedValueConverter` path assumes a single string and calls `toStringValue()`. If a DuckDB-written catalog has small tables with list columns inlined, our reads will be wrong or fail. Needs confirmation with a real fixture, but the C++ clearly takes the array path. | `DucklakeInlinedValueConverter.java` (array path) | High (when triggered) |
| B3 | **`hugeint` / `uhugeint` are unmapped and throw** on any column that uses them, including nested (a `struct<amount: hugeint>` rejects the whole struct). Spec has them as 128-bit integer types; pg_ducklake maps to VARCHAR in inline + native HUGEINT/UHUGEINT for DuckDB/Parquet. Viable JVM target: `DECIMAL(38, 0)` for `hugeint`, `DECIMAL(39, 0)` impossible (Trino caps at 38) so for `uhugeint` it has to be VARCHAR. Our README lists these under "Not yet mapped" — this audit just confirms they're reachable from a real DuckDB-written catalog. | `DucklakeTypeConverter.java:172` | High (when triggered) |
| B4 | **`linestring z` in the type converter switch** — spec 1.0 renamed it `linestring_z` (underscore). A DuckDB-written catalog with that column fails to parse. Our README already flags this; it's a one-line fix. | `DucklakeTypeConverter.java:161` | Low but trivial |
| B5 | **Unsigned writes silently truncate.** We widen on read (uint8→SMALLINT etc.) but do no range check on write. A Trino `SMALLINT 300` into a `uint8` column becomes 44 through integer wrap. Reference doesn't enforce this either (DuckDB has native unsigneds), but we don't — so the issue is purely on writes from Trino. Known; README acknowledges. | `DucklakePageSink` write path | Medium |
| B6 | **Decimal `p > 38` errors hard.** Trino can't represent it, so there is no graceful path — but right now we throw at type construction time rather than at `CREATE TABLE` time with a clearer message. Upstream allows higher precisions. Unlikely to be hit. | `DucklakeTypeConverter.java:166` | Low |

### Known, documented, no action required

- **`partial_max` not read.** Already in `DUCKLAKE_1_0_IMPACT.md`. One-file delete model
  remains spec-correct; we just don't read the compaction hint yet.
- **`default_value_type='literal'`, `default_value_dialect='duckdb'` hardcoded.** We
  already documented this in `REPORT_CROSS_ENGINE_WRITE.md`. Reference actually does
  something similar — it writes the dialect that wrote the column. Fine until we want
  to support expression defaults.
- **`UUID.randomUUID()` (v4) vs upstream UUIDv7.** Spec doesn't mandate a version; both
  are valid 128-bit UUIDs and `schema_uuid`/`table_uuid` are opaque identity. Cosmetic;
  no action needed.

### Things the earlier audit flagged that I verified as *not* bugs

- **Snapshot lineage conflict detection.** Agent flagged "is this the same as upstream" —
  yes, `ensureSnapshotLineageUnchanged()` in `JdbcDucklakeCatalog.java:1103` does the same
  max-snapshot fence the reference uses on commit. Match.
- **Per-table schema versioning (`ducklake_schema_versions.table_id`).** We write it; the
  reference writes it; both fall back gracefully when it's missing.
- **Parquet `field_id` annotations.** Our `DucklakeParquetSchemaBuilder` produces the same
  annotation layout the reference produces. Bidirectional compat test already covers this.
- **Inlined data table naming** (`ducklake_inlined_data_<tableId>_<schemaVersion>`).
  Matches.
- **`row_id_start` being a plain `long`.** An agent claimed upstream uses `optional_idx`
  and we miss the "unallocated" state. Looking at the spec, `row_id_start` is required on
  data file insert. The optional form upstream uses is a C++ convenience for transient
  transaction state, not a catalog-column possibility. Non-issue.

## Things pg_ducklake (and therefore the reference) does that we don't

| Feature | Upstream | Our JVM | Notes |
|---|---|---|---|
| **Data change feed** (`table_changes`, `table_insertions`, `table_deletions`) | Yes | No | Already in our "Not yet implemented" list. Reads `ducklake_snapshot_changes` + scans data/delete files per snapshot range. Non-trivial but well-scoped. |
| **Virtual columns** (`rowid`, `snapshot_id`, `filename`, `file_row_number`, `file_index`) | Yes | No | These are reserved column expressions exposed in scan. Trino could expose them via synthetic column handles. Likely useful for our MERGE story too. |
| **Sorted tables** (write path applies sort) | Yes | No (readable, sort ignored) | pg_ducklake exposes `ducklake_sorted` index AM + `set_sort()`. Our README acknowledges we don't sort on write. |
| **`variant` with `->`/`->>` extraction** | Yes (pg_ducklake wraps it) | No (degrades to VARCHAR) | Real structural type. Requires Trino-side support to be useful; VARCHAR degradation is honest. |
| **Maintenance: `flush_inlined_data`, `merge_adjacent_files`, `rewrite_data_files`, `expire_snapshots`, `cleanup_old_files`, `cleanup_orphaned_files`** | Yes | No | Already on our roadmap. See "Maintenance operations" section below for scoping notes. |
| **`set_commit_message()`** (author/message on snapshot) | Yes | No (session properties planned) | Trivial once we add session properties for `commit_author`, `commit_message`, `commit_extra_info`. |
| **`freeze()` / export-to-`.ducklake` single-file** | Yes | No | Nice-to-have. |
| **CHECKPOINT (umbrella maintenance)** | Yes | No | Downstream of the individual ops above. |

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
- Direct insert fast path for `INSERT ... SELECT UNNEST($n)`
- Background maintenance worker (PG-style autovacuum-shaped launcher + per-database
  workers). Trino equivalent would be operator-scheduled `CALL` procedures; same
  functions, different driver.

## Maintenance operations — scope notes for roadmap

Each maintenance op is its own snapshot on commit; all are snapshot-isolated from
concurrent DML (append-only catalog semantics, no locks).

- **`flush_inlined_data`** — Reads `ducklake_inlined_data_<tableId>_<schemaVersion>`,
  writes one or more Parquet files, inserts matching `ducklake_data_file` rows, drops
  the inlined rows. Atomic in one snapshot. Must also handle inlined deletes by writing
  a delete file.
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

## Action items surfaced by this comparison

1. **Fix the `snapshot_changes` join.** Either escape values or switch to the upstream
   `created_table:"name"` quoted form. `JdbcDucklakeCatalog.java:1090`. Small, safe, and
   removes a silent correctness bomb.
2. **Verify inlined `list<T>` read path with a real DuckDB-written fixture.** If it fails,
   adjust `DucklakeInlinedValueConverter` to consume PG arrays for list-typed columns.
3. **Map `hugeint`/`uhugeint`.** `hugeint` → `DECIMAL(38, 0)`; `uhugeint` has to be
   VARCHAR (Trino can't represent ≥39-digit integers). Clearer than the current hard
   fail. Update README.
4. **Rename `"linestring z"` → `"linestring_z"`** in `DucklakeTypeConverter`. One line.
5. **Start planning maintenance ops.** Scope above. Earliest wins are probably
   `expire_snapshots` and `cleanup_orphaned_files` — both are catalog-driven and don't
   require a compaction engine.
6. **Add a concurrency isolation test suite.** Two scenarios first: two writers on the
   same table, two writers on different tables. These are the scenarios pg_isolation's
   specs protect against and our code has no tests for.
7. **Consider session properties** for `commit_author`/`commit_message`/`commit_extra_info`.
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
| `uint64`, `hugeint`, `uhugeint` | VARCHAR | unmapped | B3 |
| `date`, `timestamp`, `timestamp_s/ms/ns`, `timestamptz` | VARCHAR | Needs string-parse | Verify converter accepts DuckDB's ISO form |
| `varchar`, `blob` | BYTEA (not text — to allow null bytes) | ? | Verify we read as bytes, not JDBC string |
| `list<T>` | VARCHAR[] (PG array) | Treated as string | B2 |
| `struct`, `map` | VARCHAR (DuckDB serialized form) | Treated as string | Probably broken for anything non-trivial |
| `variant`, geometry family | No inline (Parquet only) | n/a | ✓ |
