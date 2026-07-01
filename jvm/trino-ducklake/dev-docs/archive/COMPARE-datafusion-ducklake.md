# Comparison — `datafusion-ducklake` (Rust) vs this repo (JVM)

> **Freshness (JVM side, 2026-07):** since this snapshot our JVM implementation has SHIPPED the
> full maintenance suite (`remove_orphan_files`, `expire_snapshots`, `cleanup_old_files`,
> `flush_inlined_data`, `rewrite_data_files`/optimize, `ANALYZE`), the **change feed**
> (`table_insertions`/`table_deletions`/`table_changes`), the **local DuckDB catalog backend**, and
> the partial-file read filters. Every "JVM: not implemented / Rust ahead" verdict below on
> **maintenance ops** and **catalog backends** is superseded — read those rows as historical. Note
> on change-feed semantics (datafusion is the closest reference impl of `table_changes`): our
> update-pairing surfaces UPDATEs as `delete`+`insert`, since our `rowid` is `row_id_start + position`
> and doesn't carry cross-file lineage.

Snapshot of the Rust [`datafusion-ducklake`](https://github.com/) project reviewed against
our JVM implementation (`ducklake-catalog` + `trino-ducklake`). The goal is to flag real
spec-level conflicts, call out ideas worth stealing, and record where we're ahead.

Last refreshed against upstream `main` at `f1af7dd` (post-v0.2.1, 2026-05-29). 9 commits
since the previous baseline (`536729a`, v0.2.1) — 1 CI tweak + 8 substantive feature PRs
(row lineage, multicatalog Phase 1, three official maintenance ops, and a writer-side
spec-alignment fix).

## TL;DR

Still no real spec-level conflicts — both honor the same DuckLake catalog schema,
validity ranges, and `path_is_relative` contract. The shape of the comparison shifted
this refresh: the Rust project has stopped being purely read-focused and is now actively
porting the official write/maintenance surface (`expire_snapshots`, `cleanup_old_files`,
`delete_orphaned_files`, `DROP TABLE`, row lineage). It also gained a
project-specific multi-tenant catalog isolation layer (`MulticatalogManager`) that is
**not** a DuckLake spec feature — orthogonal extension for the RuntimeDB use case.

Net: we're still ahead on the depth axes we were ahead on (full DDL/DML/MERGE, temporal
partitioning, views, snapshot-lineage conflict detection, cross-engine compatibility),
but the Rust side has caught up — and in some cases moved ahead — on row lineage and
the official maintenance verbs, both of which we track in `TODO-WRITE-MODE.md §M8` and
`TODO-READ-MODE.md` (Virtual Columns) but have not implemented. Their implementations
are now solid reference points when we pick those up.

## Implementation conflicts (disagreements about DuckLake behavior)

| Area | Rust | JVM (this repo) | Who's right? |
|---|---|---|---|
| Validity filter `? >= begin AND (? < end OR end IS NULL)` | Same | Same | Both ✓ |
| `path_is_relative` honored as authoritative | Yes | Yes | Both ✓ |
| Delete-file cardinality per data file | Row-multiplexes (accepts N rows, merges into one position set) | `DucklakeDataFile` POJO has a single `Optional<String> deletePath` | **JVM, per spec.** Spec `queries.md` says "at most one delete file per file in a single snapshot." Multi-snapshot accumulation is handled by *rewriting* the single delete file (`partial_max` column, new in 1.0), not by stacking concurrent files. Puffin in 1.0 changes the `format` of the single delete file (`parquet` → `puffin`), not the count. See note below on our README wording |
| Interval type | Arrow `IntervalUnit::MonthDayNano` | VARCHAR passthrough | Rust richer; we already admit "degraded" |
| Decimal with `p > 38` | Auto-promotes to `Decimal256` | Capped at 38 | Rust ahead |
| Complex types (struct, map) | Explicitly errors | Full support | JVM ahead |
| Partitioning (identity + temporal year/month/day/hour) | None — no `ducklake_partition_*` reads | Full, calendar (spec-conformant); deprecated epoch path retained for legacy catalogs | JVM way ahead |
| Inlined data (small tables in catalog) | Zero support | Full read + mixed-with-parquet unions | JVM way ahead |
| Time travel (`FOR VERSION`/`FOR TIMESTAMP AS OF`) | Latest-only | Full (query / session / catalog precedence) | JVM way ahead |
| Snapshot conflict detection on commit | None in writer | `ensureSnapshotLineageUnchanged()` — aborts on stale base | JVM way ahead |
| Views | None | Trino dialect supported, cross-dialect filtered | JVM ahead |
| Stats-driven pruning | `TableProvider::statistics()` returns only `total_byte_size` (sum of `file_size_bytes` − `delete_file_size_bytes`), marked `Precision::Inexact` since the catalog tracks compressed parquet bytes while DataFusion's contract is uncompressed Arrow output. No `num_rows`, no per-column stats — record count is noted as a follow-up "when `record_count` is plumbed through `DuckLakeFileData`" (v0.2.1) | Full file + table level via `getTableStatistics`: row count, per-column null fractions, data sizes, ranges; conservative mode (`TableStatistics.empty()`) when delete files or live inlined rows are present | JVM way ahead |
| Parquet footer size hint | Passed via `with_metadata_size_hint()` — saves an S3 round-trip per file | Passed via `FooterPrefetchingParquetDataSource` wrapper (Trino's `MetadataReader.readFooter` has no hint API, so we intercept `readTail`). Covers data files + delete files | At parity |
| Path validation (null-byte, traversal) | `validate_path()` guards | Assumes trusted catalog | Rust more defensive |
| Unsigned range validation on writes | None | `DucklakeUnsignedRangeChecker` rejects out-of-range SMALLINT/INTEGER/BIGINT/DECIMAL(20,0) writes into uint8/uint16/uint32/uint64 columns at page-sink time, before silent Parquet-truncation can corrupt the uint catalog column | JVM ahead |
| Catalog backends | DuckDB (bundled), SQLite, Postgres, MySQL | JDBC — **Postgres + local DuckDB shipped**; SQLite + remote-DuckDB/Quack planned | Rust still wider (MySQL); JVM no longer Postgres-only |
| Row lineage (`rowid` virtual column) | Opt-in via `DuckLakeCatalog::with_row_lineage(true)`; appends BIGINT `rowid` at end of schema. `RowIdExec` injects `row_id_start + position` per file; also detects the embedded `_ducklake_internal_row_id` column (Iceberg-reserved field-id `2_147_483_540`) that UPDATE / compaction writes. Catalog plumbing (`row_id_start`, `record_count`, `ducklake_table_stats(next_row_id)`) is wired in all 4 providers + SQLite & Postgres writers. Multicatalog reader projects the same fields (#124, 2026-05-29). | Tracked in `DESIGN-virtual-columns.md` as `$row_id` and listed in `TODO-READ-MODE.md` under "Add DuckDB-equivalent virtual columns" — **not implemented**. We expose `$row_id` (= `row_id_start + position`) + `$snapshot_id`/`$path`/`$file_row_number`/`$file_size_bytes` as hidden queryable virtual columns, and the change feed consumes `$row_id`. | **Comparable now** — the positional `$row_id` is exposed. What we do NOT capture is DuckLake's *preserved-across-UPDATE* lineage (Rust reads the embedded `_ducklake_internal_row_id`); our `$row_id` is purely positional, which is why the change feed reports UPDATEs as delete+insert. Reading the embedded lineage column is the remaining delta (Rust's `src/row_id.rs` is the reference). |
| Official maintenance ops (`expire_snapshots`, `cleanup_old_files`, `delete_orphaned_files`, `DROP TABLE` tombstone) | All four landed in 2026-05-28/29 (PRs #122, #123). `ExpireCriteria::{Versions, OlderThan}` + `CleanupCriteria::{All, OlderThan}` mirror upstream's two-phase vacuum; `OlderThan` filter is `last_modified` against `object_store::ObjectMeta` (in-flight write protection), not catalog `schedule_start`. Sqlite single-catalog + Postgres multicatalog code paths share `src/maintenance.rs` (290 LOC) with backend-scoped variants. `.parquet`-suffix filter at storage-listing time matches upstream. Side-by-side validated against the official DuckDB+DuckLake extension via `examples/maintenance_demo.sql` + `examples/orphan_cleanup_demo.sql`. | **SHIPPED** — `expire_snapshots`, `cleanup_old_files`, `remove_orphan_files`, `flush_inlined_data`, `rewrite_data_files`/optimize, `ANALYZE` (all `CALL system.*`). Two-phase deletion: expire schedules dead files, `cleanup_old_files` reclaims them age-gated (grace period protects in-flight/cross-engine readers). See DESIGN-maintenance.md. | **At parity** — datafusion's `src/maintenance.rs` was a useful reference; the criteria-enum + two-phase split are mirrored (retention_threshold / explicit snapshot_ids; age-gated cleanup). |
| Multi-tenant catalog isolation (per-tenant `catalog_id` partitioning of a shared metadata DB) | Added in PR #117 (2026-05-25) as `MulticatalogManager` + `PostgresMetadataWriter` + `MulticatalogProvider` + `initialize_multicatalog_schema`. Adds `ducklake_catalog` + `ducklake_catalog_{snapshot,schema}_map` + per-catalog `ducklake_schema_versions` + a `catalog_id` column on `ducklake_files_scheduled_for_deletion` (a documented divergence from the official single-catalog schema). FOR UPDATE on the catalog row (30s `lock_timeout`) serialises concurrent writers; cross-catalog table_id/schema_id rejected at write time. `drop_catalog` and `drop_table_in_catalog` (PR #120) tombstone per catalog. | Single-catalog only. Multi-tenant isolation, if needed, would live on the layer above (one connector instance per tenant, or per-tenant schema namespacing). | **Non-spec extension**, not a gap on our side. Worth noting only because it diverges the Rust schema from the official `ducklake_files_scheduled_for_deletion` shape, which a future cross-engine compatibility check should account for. |

**Rust-side caveat when copying their docs verbatim**: their `CLAUDE.md` describes
`DeleteFilterExec` as "filters rows by global position" — code (`delete_filter.rs`)
is per-file (spec-correct), but the doc phrasing is loose. Read source before copying.

## Ideas worth stealing

1. **Parquet footer size hints.** ~~Worth stealing — Rust passes `footer_size` to
   `ParquetFormat::with_metadata_size_hint()`.~~ **Done.** Trino's `MetadataReader.readFooter`
   has no hint parameter (hardcodes a 48 KB blind tail read with a fallback re-read for
   oversized footers), so the JVM integration wraps `ParquetDataSource` with
   `FooterPrefetchingParquetDataSource`, which pre-fetches exactly `footer_size + 8`
   bytes and short-circuits the first `readTail` call. For typical footers this replaces
   the blind 48 KB read with an exact-size read; for oversized footers it replaces the
   two-round-trip fallback with a single read. Covers `ducklake_data_file.footer_size`
   and `ducklake_delete_file.footer_size`; stale or missing hints degrade silently to
   the default path.
2. **`sqllogictest` harness.** 248 `.test` files across 46 categories under
   `tests/sqllogictests/sql/` (alter / attach / compaction / encryption / merge /
   partitioning / time_travel / tpch / …) — ~18,740 LOC, ~3,699 individual
   `statement`/`query` cases. Note that the upstream `ducklake/` C++ reference has
   **422** `.test` files across 49 categories with ~7,274 cases — the authoritative
   spec-conformance corpus, ~2x what datafusion-ducklake replays. Trino has its own
   product-tests story, but a sqllogictest bridge against `ducklake-catalog` in
   isolation would unlock either corpus for regression coverage.
3. **`hybrid_asyncdb.rs`** — DuckDB writes, DataFusion reads, same `.slt` runner. We do the
   equivalent in `TestDucklakeCrossEngineCompatibility`, but the idea of reusing DuckDB's
   own test suite as an interop oracle is clever and cheap.
4. **Per-backend metadata-provider unit tests** (postgres / mysql / sqlite), each ~1k LOC
   against Testcontainers. We only exercise Postgres today; when the SQLite/DuckDB
   backends land, this is the pattern.
5. **Encryption tests** (Parquet Modular Encryption). Our README does not list PME yet; if
   we target S3-enterprise deployments it's worth tracking.
6. **`src/maintenance.rs` as a reference for our M8 work.** Their `ExpireCriteria` /
   `CleanupCriteria` enum split, the `last_modified` vs catalog-side `schedule_start`
   distinction (in-flight write guard), the storage-listing `.parquet` filter, and the
   side-by-side `examples/maintenance_demo.sql` / `examples/orphan_cleanup_demo.sql`
   parity harness against the official DuckDB extension are all directly applicable.
   When we pick up M8, copy the semantics — the implementation language won't matter.
7. **`src/row_id.rs` as a reference for our `$row_id` virtual column.** The
   `ROW_ID_PARQUET_FIELD_ID = 2_147_483_540` reserved field-id detection (for files
   written by UPDATE / compaction with embedded rowids vs the synthetic
   `row_id_start + offset` path) is the spec-correct way to handle the two cases. Maps
   to our `DESIGN-virtual-columns.md` § 3.2 (`$row_id` as a scalar BIGINT).

## Things we already do better

- Writes: full DDL, DML, MERGE, atomic UPDATE-as-delete+insert.
- Optimistic concurrency with snapshot-lineage check at commit. Rust's writer commits blind.
- Temporal partition encoding handled per the DuckLake 1.0 calendar contract — a concern
  the Rust side has not touched. (We also keep a deprecated epoch path for pre-spec-resolution
  catalogs.)
- Time travel with documented precedence (query clause > session > catalog > current).
- Inlined data + mixed inline/parquet snapshots.
- Cross-engine compatibility harness against DuckDB is production-grade.
- File-level stats → partition pruning, null counts, conservative stats when delete files
  are present.
- Views with dialect filtering (non-Trino views hidden rather than mis-rendered).

## Action items still open

- **Track `partial_max` handling for 1.0.** Puffin deletion-vector reads landed on our
  side (2026-05-20 — `DucklakePuffinDeleteReader`), so the remaining 1.0 delete-file gap
  is `partial_max`. Neither implementation reads it today. When a DuckDB compaction
  rewrites a delete file as partial, we need to not mis-attribute deletes to older
  snapshots.
- **Evaluate the `.slt` corpus** as a portable regression suite for the catalog library.

## Test coverage, at a glance

| | Rust | JVM (this repo) |
|---|---|---|
| Integration test LOC | ~13,425 across 26 files (was ~9,231 / 19 in v0.2.1; +4,200 from multicatalog + maintenance + row-id suites this refresh) | ~10,215 across 24 files |
| Unit tests in source files | 11 files with `#[cfg(test)]` | N/A — separated out |
| sqllogictest suite | 248 `.test` files / 46 categories / ~18,740 LOC / ~3,699 cases (replays a subset of the upstream `ducklake/` corpus, which has 422 files / 49 categories / ~7,274 cases) | none |
| Per-backend metadata-provider tests | Postgres / MySQL / SQLite, Testcontainers | Postgres only (Testcontainers) |
| Concurrent-access tests | `concurrent_tests.rs` + `concurrent_write_tests.rs` (~877 LOC) | Implicit via Trino runtime; no dedicated suite |
| Write/DDL lifecycle | Insert path + DROP TABLE + multicatalog Phase 1 + three official maintenance ops | Full (DDL, DML, MERGE — ~2.4k LOC across 3 classes). M8 maintenance ops still open. |
| Partition pruning tests | None | `TestDucklakePartitionPruning` + matcher tests (~1.4k LOC) |
| Time-travel / snapshot pinning | Not meaningful — latest-only | Dedicated integration test |
| Cross-engine compatibility | Adapter-based (`.slt` replay through DataFusion) | Direct DuckDB↔Trino round-trip tests |
| Encryption (PME) | Yes | No |

**Net:** Rust's suite is broader (SQL surface coverage, multi-backend), ours is deeper
(transaction semantics, query optimization, multi-engine interop).
