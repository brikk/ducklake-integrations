# Comparison — `datafusion-ducklake` (Rust) vs this repo (JVM)

Snapshot of the Rust [`datafusion-ducklake`](https://github.com/) project reviewed against
our JVM implementation (`ducklake-catalog` + `trino-ducklake`). The goal is to flag real
spec-level conflicts, call out ideas worth stealing, and record where we're ahead.

## TL;DR

No real spec-level conflicts — both honor the same DuckLake catalog schema, validity
ranges, and `path_is_relative` contract. The Rust project is a **read-focused DataFusion
extension**; this repo is a **full-lifecycle Trino connector**. Rust is ahead on test
breadth and backend coverage; we're ahead on depth (writes, temporal partitioning, views,
cross-engine compatibility). The Rust project's own `CLAUDE.md` still describes itself as
read-only, but its code has started a write path (`metadata_writer*`, `table_writer.rs`,
`insert_exec.rs`); their docs lag their code.

## Implementation conflicts (disagreements about DuckLake behavior)

| Area | Rust | JVM (this repo) | Who's right? |
|---|---|---|---|
| Validity filter `? >= begin AND (? < end OR end IS NULL)` | Same | Same | Both ✓ |
| `path_is_relative` honored as authoritative | Yes | Yes | Both ✓ |
| Delete-file cardinality per data file | Row-multiplexes (accepts N rows, merges into one position set) | `DucklakeDataFile` POJO has a single `Optional<String> deletePath` | **JVM, per spec.** Spec `queries.md` says "at most one delete file per file in a single snapshot." Multi-snapshot accumulation is handled by *rewriting* the single delete file (`partial_max` column, new in 1.0), not by stacking concurrent files. Puffin in 1.0 changes the `format` of the single delete file (`parquet` → `puffin`), not the count. See note below on our README wording |
| Interval type | Arrow `IntervalUnit::MonthDayNano` | VARCHAR passthrough | Rust richer; we already admit "degraded" |
| Decimal with `p > 38` | Auto-promotes to `Decimal256` | Capped at 38 | Rust ahead |
| Complex types (struct, map) | Explicitly errors | Full support | JVM ahead |
| Partitioning (identity + temporal year/month/day/hour) | None — no `ducklake_partition_*` reads | Full, with calendar/epoch encoding + read leniency | JVM way ahead |
| Inlined data (small tables in catalog) | Zero support | Full read + mixed-with-parquet unions | JVM way ahead |
| Time travel (`FOR VERSION`/`FOR TIMESTAMP AS OF`) | Latest-only | Full (query / session / catalog precedence) | JVM way ahead |
| Snapshot conflict detection on commit | None in writer | `ensureSnapshotLineageUnchanged()` — aborts on stale base | JVM way ahead |
| Views | None | Trino dialect supported, cross-dialect filtered | JVM ahead |
| Stats-driven pruning | Stats not exposed via `MetadataProvider` trait | Full file + table level; conservative mode when deletes present | JVM way ahead |
| Parquet footer size hint | Passed via `with_metadata_size_hint()` — saves an S3 round-trip per file | Not currently used | **Worth stealing** |
| Path validation (null-byte, traversal) | `validate_path()` guards | Assumes trusted catalog | Rust more defensive |
| Unsigned range validation on writes | None | None | Both have the hole |
| Catalog backends | DuckDB (bundled), SQLite, Postgres, MySQL | JDBC (Postgres in prod; SQLite/DuckDB promised) | Rust wider today |

### A doc-vs-code mismatch on the Rust side

Their `CLAUDE.md` says `DeleteFilterExec` "filters rows by global position." DuckLake
positional deletes are **per data file** (`(file_path, pos)` tuples). Reading the code
(`delete_filter.rs`), `row_offset` is per-file (starts at 0 and tracks position within one
file's Parquet stream), so the behavior is actually spec-correct — only the doc phrasing is
loose. Worth a direct source read before copying any ideas verbatim, since a future reader
of their docs could be misled.

### A README-vs-spec mismatch on our side

Our `trino-ducklake/README.md` says:

> Multiple delete files per data file | Yes | Accumulated across snapshots

Read literally against the spec, this is wrong — there is at most one delete file per data
file per snapshot. The feature we almost certainly meant is "our read path correctly sees
the currently-valid delete file for each data file, which may have been rewritten across
snapshots." Worth rewording to avoid implying concurrent stacked delete files. The 1.0
`partial_max` column (where multi-snapshot deletes actually live) is not yet read by
either implementation — see `DUCKLAKE_1_0_IMPACT.md`.

## Ideas worth stealing

1. **Parquet footer size hints.** DuckLake stores `footer_size` in `ducklake_data_file`.
   Rust passes it to `ParquetFormat::with_metadata_size_hint()` so the reader issues a
   single range read instead of two. Low-effort, measurable S3 latency win. Applies to
   both data files and delete files.
2. **`sqllogictest` harness.** 248 `.slt` files across 48 categories (alter / attach /
   compaction / encryption / merge / partitioning / time_travel / tpch / …). Trino has
   its own product-tests story, but an `.slt` bridge could reuse this corpus for regression
   against the catalog library in isolation.
3. **`hybrid_asyncdb.rs`** — DuckDB writes, DataFusion reads, same `.slt` runner. We do the
   equivalent in `TestDucklakeCrossEngineCompatibility`, but the idea of reusing DuckDB's
   own test suite as an interop oracle is clever and cheap.
4. **Per-backend metadata-provider unit tests** (postgres / mysql / sqlite), each ~1k LOC
   against Testcontainers. We only exercise Postgres today; when the SQLite/DuckDB
   backends land, this is the pattern.
5. **Encryption tests** (Parquet Modular Encryption). Our README does not list PME yet; if
   we target S3-enterprise deployments it's worth tracking.

## Things we already do better

- Writes: full DDL, DML, MERGE, atomic UPDATE-as-delete+insert.
- Optimistic concurrency with snapshot-lineage check at commit. Rust's writer commits blind.
- Temporal partition encoding (calendar vs epoch, read leniency) — a DuckLake 1.0 concern
  the Rust side has not touched.
- Time travel with documented precedence (query clause > session > catalog > current).
- Inlined data + mixed inline/parquet snapshots.
- Cross-engine compatibility harness against DuckDB is production-grade.
- File-level stats → partition pruning, null counts, conservative stats when delete files
  are present.
- Views with dialect filtering (non-Trino views hidden rather than mis-rendered).

## Action items surfaced by this comparison

- **Reword the README delete-file row.** "Multiple delete files per data file: Yes |
  Accumulated across snapshots" should probably become something like "Delete files are
  resolved per data file, respecting DuckLake 1.0 partial deletion file semantics (once
  implemented)." The current wording contradicts the spec at face value.
- **Track `partial_max` handling for 1.0.** `DUCKLAKE_1_0_IMPACT.md` already flags Puffin
  format as a medium-priority gap; `partial_max` deserves the same treatment. Neither
  implementation reads it today. When a DuckDB compaction rewrites a delete file as partial,
  we need to not mis-attribute deletes to older snapshots.
- **Consider adopting the footer-size hint** on the read path (passes `ducklake_data_file.footer_size`
  to the Parquet reader, saves an S3 round-trip per file).
- **Evaluate the `.slt` corpus** as a portable regression suite for the catalog library.

## Test coverage, at a glance

| | Rust | JVM (this repo) |
|---|---|---|
| Integration test LOC | ~9,110 across 19 files | ~10,215 across 24 files |
| Unit tests in source files | 11 files with `#[cfg(test)]` | N/A — separated out |
| `.slt` suite | 248 files / 47 categories / ~18,740 LOC | none |
| Per-backend metadata-provider tests | Postgres / MySQL / SQLite, Testcontainers | Postgres only (Testcontainers) |
| Concurrent-access tests | `concurrent_tests.rs` + `concurrent_write_tests.rs` (~877 LOC) | Implicit via Trino runtime; no dedicated suite |
| Write/DDL lifecycle | Partial (insert path started) | Full (DDL, DML, MERGE — ~2.4k LOC across 3 classes) |
| Partition pruning tests | None | `TestDucklakePartitionPruning` + matcher tests (~1.4k LOC) |
| Time-travel / snapshot pinning | Not meaningful — latest-only | Dedicated integration test |
| Cross-engine compatibility | Adapter-based (`.slt` replay through DataFusion) | Direct DuckDB↔Trino round-trip tests |
| Encryption (PME) | Yes | No |

**Net:** Rust's suite is broader (SQL surface coverage, multi-backend), ours is deeper
(transaction semantics, query optimization, multi-engine interop).
