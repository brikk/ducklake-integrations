# DuckLake Read Mode — Open Items

The shipped read surface is summarized in the feature chart in [README](../README.md);
this file tracks what's still open. Reuse strategy when adding to the read path:
lean on Trino's existing Parquet stack, split-pruning infrastructure, and
`ConnectorTableVersion` plumbing; keep custom code limited to DuckLake snapshot
resolution against the SQL catalog, dual-encoding temporal pruning, and inlined-data
fallback semantics.

Spec context for several items below lives in
[DUCKLAKE_1_0_IMPACT.md](DUCKLAKE_1_0_IMPACT.md) (the DuckLake 1.0 spec impact
reference). Gaps surfaced from upstream and sister connectors live in the
`COMPARE-*.md` files.

## Top Priorities

Picked next, in order. Pair this with [TODO-WRITE-MODE.md § Top Priorities](TODO-WRITE-MODE.md#top-priorities)
on the write side.

1. **Puffin deletion-vector reader (long-term)** — lift the existing
   `TestDucklakePuffinDeleteFileGuard` `NOT_SUPPORTED` throw. DuckLake
   writes Roaring-bitmap delete files in puffin format when
   `write_deletion_vectors=true` is set on the writer; we currently refuse
   the read. ~2–3 days (Iceberg's `iceberg-core` puffin lib +
   `org.roaringbitmap:RoaringBitmap`). **See § Puffin Deletion Vector
   Reads § Long-term below.**

Unlocks real cross-engine compatibility for puffin-encoded deletes — any
DuckDB-written catalog becomes readable from Trino regardless of which
delete-encoding the writer chose. After this, the remaining type gap
(`list<blob>` inlined reads) is the next bite-sized correctness item.

(The inlined-deletion reader landed; see § Inlined Deletion Vector Reads
below.)

## Inlined Deletion Vector Reads

DuckLake's deletion inlining stores small deletions
(below `DATA_INLINING_ROW_LIMIT` rows) directly in a per-table metadata
table `ducklake_inlined_delete_<tableId>` rather than as a Parquet delete
file. Schema (per
`vendor/pg_ducklake/third_party/ducklake/src/storage/ducklake_metadata_manager.cpp:2645`
and the
[Data Inlining doc page](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining)):

```sql
ducklake_inlined_delete_<tableId> (
    file_id        BIGINT,
    row_id         BIGINT,
    begin_snapshot BIGINT
)
```

`file_id` is `ducklake_data_file.data_file_id`; `row_id` is the deleted
row's position within that file; `begin_snapshot` is the snapshot that
recorded the deletion. Rows accumulate forever in this table until
compaction rewrites the data file; readers apply all rows with
`begin_snapshot <= currentSnapshotId` as a positional delete mask on the
data file's scan.

- [x] **Short-term guard: refuse to read snapshots that have inlined
  deletions.** Removed once the long-term reader landed. Was implemented
  as `DucklakeSplitManager.validateNoInlinedDeletes` throwing
  `NOT_SUPPORTED`; replaced by the scan-time filter below.
- [x] **Long-term: read inlined deletions and apply them at scan time.**
  Landed 2026-05-11.
  - `DucklakeCatalog.getInlinedDeletes(tableId, snapshotId)` →
    `Map<Long, Set<Long>>` (data_file_id → file-local row positions).
    Query: `SELECT file_id, row_id FROM ducklake_inlined_delete_<tableId>
    WHERE begin_snapshot <= ?`. Empty map when the table doesn't exist.
    Implemented in `JdbcDucklakeCatalog`.
  - `DucklakeSplit` carries `inlinedDeletedRowPositions: Set<Long>`
    (file-local positions for that file's `data_file_id`).
    `DucklakeSplitManager` fetches the grouped map once per scan and
    attaches the matching set to each split (gated on `hasInlinedDeletes`
    to skip the SQL when no deletes are present).
  - `DucklakePageSourceProvider.applyDeleteFile` merges
    `split.inlinedDeletedRowPositions()` into the same `deletedRows` set
    as parquet positional deletes; the existing `DeleteRowFilterTransform`
    checks both global row ids and file-local row offsets per page, so a
    single combined set is correct.
  - `DucklakeMetadata.getTableStatistics` returns `TableStatistics.empty()`
    when `hasInlinedDeletes` is true (same conservative policy as parquet
    delete files).
  - Tests in `TestDucklakeInlinedDeleteHandling` cover: round-trip
    (suppressed rows), per-file granularity (partitioned_table),
    time-travel (`begin_snapshot > snapshotId` → deletion not applied),
    stats invalidation, and the absent-table probe path.

## Inlined-Read Type Gaps

- [ ] **`list<blob>` inlined reads.** DuckDB serializes blobs in lists as
  `'\xNN\xNN'` text. Our parser strips the single quotes but doesn't decode the
  `\xNN` hex escapes, so `VarbinaryType` receives the literal escape characters
  instead of the intended bytes. Pinned by `@Disabled testDuckdbListBlobReadsInTrino`
  in `TestDucklakeCrossEngineTypeAudit` (the test runs both inlined and Parquet
  paths, so lifting `@Disabled` will also confirm whether Trino's Parquet array
  reader survives DuckDB's `ARRAY<BINARY>` layout). Fix in
  `DucklakeInlinedValueConverter`: when the element type is `VarbinaryType`,
  decode `\xNN` sequences before building the Trino value. Compare against
  `ducklake_util.cpp::ToSQLString` BLOB branch and DuckDB's `Blob::ToString` for
  exact escape semantics.
  - Source: `COMPARE-pg_ducklake.md` B2 follow-up

- [x] **`uuid` scalar + `list<uuid>` inlined reads.** Done.
  `DucklakeInlinedValueConverter` now has a `UuidType` branch
  (`toUuidSlice`) that parses the 36-char text via `java.util.UUID.fromString`
  and packs the 16 bytes through `UuidType.javaUuidToTrinoUuid`. Pinned by
  `testDuckdbInlinedUuidReadsInTrino` (scalar) and `testDuckdbListUuidReadsInTrino`
  (list, `@Disabled` lifted) in `TestDucklakeCrossEngineTypeAudit` — both run
  inlined and Parquet paths, so this also confirms Trino's Parquet reader against
  DuckDB's UUID layout.

## Puffin Deletion Vector Reads

- [x] **Short-term guard: refuse to read snapshots that reference puffin delete
  files.** `DucklakeSplitManager.validateDeleteFileFormats` throws
  `TrinoException(NOT_SUPPORTED, ...)` naming the schema.table, the unsupported
  format, and DuckDB's `write_deletion_vectors` setting. Catalog change:
  `DucklakeDataFile` now carries `Optional<String> deleteFileFormat`, populated
  from `ducklake_delete_file.format` in `JdbcDucklakeCatalog.getDataFiles`.
  Pinned by `TestDucklakePuffinDeleteFileGuard` (metadata-only — injects a
  `format='puffin'` row pointing at a non-existent path; the guard runs before
  any IO so a real puffin file isn't required for this test).
- [ ] **Long-term: read DuckLake puffin delete files (Roaring bitmaps).**
  Implement on the Trino connector side (not in `ducklake-catalog`) using
  Iceberg's `iceberg-core` puffin library + `org.roaringbitmap:RoaringBitmap`.
  See
  [DUCKLAKE_1_0_IMPACT.md § Existing Puffin Support in Trino](DUCKLAKE_1_0_IMPACT.md#existing-puffin-support-in-trino-reuse-analysis)
  for reuse analysis. Lift the guard once the reader is in place; add a
  round-trip test with a real puffin file (DuckDB writer with
  `write_deletion_vectors=true` → Trino reader) alongside the existing
  metadata-only guard test.

## Sorted-Table Awareness (Read)

- [ ] **Read `ducklake_sort_info` / `ducklake_sort_expression`.** Today the sort
  metadata is ignored. Sorted tables read fine without it (sorting is a
  write-time optimization), but exposing it enables future planner optimizations
  (skip sort operator when input is pre-sorted, propagate sort properties for
  merge joins, etc.). See
  [DUCKLAKE_1_0_IMPACT.md § Sorted Tables](DUCKLAKE_1_0_IMPACT.md#2-sorted-tables).

## Type-Support Improvements

- [ ] **Variant** — currently degraded to VARCHAR (data accessible as string, no
  variant-specific operators, no shredded field access, no shredded statistics).
  Options: keep degraded; add JSON-style accessor functions over the VARCHAR;
  wait for native Trino Variant type support. See
  [DUCKLAKE_1_0_IMPACT.md § Variant Type](DUCKLAKE_1_0_IMPACT.md#3-variant-type).

- [ ] **Geometry** — currently degraded to VARBINARY (no spatial pruning, no
  bounding-box stats use). Wants integration with a Trino spatial library. See
  [DUCKLAKE_1_0_IMPACT.md § Geometry Type](DUCKLAKE_1_0_IMPACT.md#4-geometry-type).

## Virtual Columns

- [ ] **Add DuckDB-equivalent virtual columns** (`rowid`, `snapshot_id`,
  `filename`, `file_row_number`, `file_index`). Useful for the MERGE story and
  for debug/lineage queries. Deserves its own design doc when prioritized.

## R6: Change Feed and Extended Metadata Parity

- [ ] Evaluate Trino equivalents for:
  - DuckDB `table_changes`, `table_insertions`, `table_deletions`
  - DuckLake-specific metadata surfaces beyond `$files` / `$snapshots`
- [ ] Implement only when there's a clear Trino use-case and a maintainable API
  shape.

## R7: Cross-Backend View Tests

- [ ] Test views across all catalog backends (SQLite, PostgreSQL, DuckDB) — today's
  view tests exercise PostgreSQL only.

## Research / Evaluation

- [ ] **Evaluate adopting the DuckLake `.slt` corpus** as a portable regression
  suite for the catalog library. Source: `COMPARE-datafusion-ducklake.md`.

### Cross-Dialect View Transpilation

Today the connector exposes Trino-dialect views and skips others with logging. The
natural next step is to transpile DuckDB-dialect views to Trino SQL at read time so a
Trino client sees every view in the catalog regardless of which engine created it.

**Architecture sketch**: Rust SQL transpiler → compile to WASM → load via a pure-Java
WASM runtime → call `transpile(sql, from="duckdb", to="trino")` per view access (plan
time, not hot path).

**Transpiler candidates** (need evaluation):
- **sqlglot (Python)**: https://github.com/tobymao/sqlglot — most mature (25+ dialects),
  but Python embedding in JVM is impractical for a Trino plugin
- **sqlglot-rust / sql-glot-rust (Protegrity)**:
  https://github.com/protegrity/sql-glot-rust — Rust port, v0.9.24
- **polyglot-sql**: https://github.com/tobilg/polyglot — Rust, very early stage

Opposite direction (Trino → DuckDB):
- **papera**: https://lib.rs/crates/papera

**WASM-on-JVM runtime**:
- **Chicory**: https://github.com/dylibso/chicory — pure Java WASM interpreter, no
  native dependencies, works in any JVM. Right choice for a Trino plugin (classloader
  isolation, no JNI). Performance is fine for transpiling a single SQL string per view
  access.
- GraalVM WASM and wasmtime-java/wasmer-java are alternatives but require native code
  or a specific JVM, which Trino plugins can't guarantee.

**Configuration**: a catalog property like `ducklake.view-dialect-transpilation=duckdb`
(or a list: `duckdb,spark`) that enables transpilation for those dialects. Default:
disabled (only Trino-dialect views exposed). When enabled, views with matching dialect
are transpiled before being returned to the planner.

**Risks to evaluate**:
- Transpilation correctness — a bug silently changes query semantics
- Rust port maturity vs Python original — DuckDB-specific syntax coverage
- WASM binary as a shipped dependency — versioning and update story
- Error handling — fallback to skip? surface error?

**Status**: research item. Needs someone to evaluate sqlglot-rust / sql-glot-rust
DuckDB→Trino transpilation quality against real DuckLake views.

## Reference: DuckDB Extension Parity Matrix

| DuckDB feature/function | Category | Trino equivalent | Decision |
|---|---|---|---|
| `FROM catalog.last_committed_snapshot()` | Connection-local state | None | Not planned |
| `FROM table_changes(...)` | Change feed | Connector table function/system table | Open (R6) |
| `FROM table_insertions(...)` | Change feed | Connector table function/system table | Open (R6) |
| `FROM table_deletions(...)` | Change feed | Connector table function/system table | Open (R6) |
| `FROM ducklake_list_files(...)` | Metadata utility | `table$files` covers it; standalone table function later | Later |
| `rowid` virtual column | Lineage | Hidden column / metadata table if needed | Open (Virtual Columns) |
| `FROM catalog.options()/settings()` | Config metadata | Session/catalog properties + optional system table | Later |
| `CALL catalog.set_option(...)` | Generic config mutation | Typed Trino properties only | Not planned |

(Items already shipped — `$snapshots`/`$current_snapshot`, `FOR VERSION/TIMESTAMP AS OF`,
session/catalog snapshot pinning — moved to the README feature chart.)

## Reference: Cross-Engine Read Test Matrix

For each supported catalog backend (today: PostgreSQL primary; SQLite/DuckDB deferred
to read-only via the JDBC code path):

| Scenario | DuckDB-created → Trino-read | Trino-created → Trino-read | Trino-created → DuckDB-read |
|---|---|---|---|
| Current snapshot reads | Required | Required | Required |
| Mixed inlined + Parquet snapshot reads | Required | Required | Required |
| `FOR VERSION AS OF` | Required | Required | Required (validate snapshot exists/readable in DuckDB) |
| `FOR TIMESTAMP AS OF` | Required | Required | Required |
| `$files` and snapshot metadata tables | Required | Required | N/A |
