# DuckLake Read Mode — Open Items

The shipped read surface is summarized in the feature chart in [README](../README.md);
this file tracks what's still open. Reuse strategy when adding to the read path:
lean on Trino's existing Parquet stack, split-pruning infrastructure, and
`ConnectorTableVersion` plumbing; keep custom code limited to DuckLake snapshot
resolution against the SQL catalog, dual-encoding temporal pruning, and inlined-data
fallback semantics.

Spec context for several items below lives in
[archive/DUCKLAKE_1_0_IMPACT.md](archive/DUCKLAKE_1_0_IMPACT.md) (the DuckLake 1.0 spec impact
reference). Gaps surfaced from upstream and sister connectors live in the
`COMPARE-*.md` files.

## Top Priorities

Picked next, in order. Pair this with [TODO-WRITE-MODE.md § Top Priorities](TODO-WRITE-MODE.md#top-priorities)
on the write side.

All previously-listed top priorities have landed: the inlined-deletion
reader, the `list<blob>` inlined-read type-gap fix, and the puffin
deletion-vector reader (Roaring bitmaps inside DuckLake's `.puffin`
delete files). See § Inlined Deletion Vector Reads, § Inlined-Read Type
Gaps, and § Puffin Deletion Vector Reads below.

Next bite-sized read items: virtual columns (rowid/file_row_number) and
the `R7` cross-backend view tests. None are correctness blockers — pick
whichever fits the current session. (Sorted-table read awareness landed
2026-05-21; see § Sorted-Table Awareness (Read) below.)

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

- [x] **`list<blob>` inlined reads.** Landed 2026-05-20.
  `DucklakeInlinedValueConverter.decodeBlobText` inverts DuckDB's
  `Blob::ToString` — `\xNN` (upper or lower hex) → byte, other ASCII
  bytes pass through verbatim, bare `\` or non-ASCII chars are rejected
  loudly because DuckDB never emits them in blob text. The `VarbinaryType`
  branch now routes String inputs through it (`byte[]` scalar path
  unchanged). Both serialization shapes work: DuckDB's LIST→VARCHAR cast
  emits unquoted `\xNN\xNN…` for pure-non-printable blobs (since
  `NestedToVarcharCast::LOOKUP_TABLE` doesn't flag `\\`) and quoted
  `'…\\xNN…'` (with `\\` → `\` parser unescape) for blobs whose text form
  contains list-special chars like `,` `[` `]` etc. — both reduce to the
  same `\xNN…` text by the time `decodeBlobText` sees it.
  Pinned by:
  - `TestDucklakeInlinedValueConverter` (9 cases — all-non-printable,
    empty, mixed printable + escape, backslash byte, `'`/`"` bytes,
    lowercase hex, pure-ASCII passthrough, truncated escape rejection,
    non-ASCII char rejection).
  - `TestDucklakeCrossEngineTypeAudit.testDuckdbListBlobReadsInTrino`
    (formerly `@Disabled`; now runs both inlined and Parquet paths via
    `runListRoundTrip`, so it also confirms Trino's Parquet array reader
    survives DuckDB's `ARRAY<BINARY>` layout).

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
- [x] **Long-term: read DuckLake puffin delete files (Roaring bitmaps).**
  Landed 2026-05-20.
  - `DucklakePuffinDeleteReader` (Trino-plugin-side) parses DuckLake's
    bespoke deletion-vector blob format — vector_size (BE u32) + magic
    `0xD1D33964` + bitmap_count (LE u64) + N × (LE i32 high_bits + portable
    Roaring bitmap) + CRC32 (BE u32). The `.puffin` file is *not* a real
    Iceberg Puffin file (no PFA1 framing, no JSON footer); it's just the
    blob bytes, matching `vendor/ducklake/src/storage/ducklake_delete.cpp`
    `WriteDeletionVectorFile`. We do not pull in `iceberg-core` — the
    bespoke format and the absence of an outer puffin container made it
    cheaper to implement the ~150-line decoder directly. Only the
    `org.roaringbitmap:RoaringBitmap` dependency is added.
  - **One library gotcha pinned**: `RoaringBitmap.deserialize(ByteBuffer)`
    slices the input buffer internally and does NOT advance the input's
    position. The reader manually advances by
    `bitmap.serializedSizeInBytes()` after each call. Pinned by
    `TestDucklakePuffinDeleteReader.testMultipleHighGroupsRoundTrip` which
    would loop forever / reject the second bitmap's cookie if regressed.
  - `DucklakePageSourceProvider.applyDeleteFile` dispatches on the file
    extension (`.puffin` → puffin reader, else parquet) and merges the
    resulting positions into the same `deletedRows` set as parquet
    positional deletes. `DucklakeSplit` schema unchanged.
  - `DucklakeSplitManager.validateDeleteFileFormats` now accepts both
    `parquet` and `puffin`; unknown formats still hard-fail with a clear
    message naming the supported formats.
  - Pinned by:
    - `TestDucklakePuffinDeleteReader` (10 cases — empty bitmaps, single
      low-bits group, multi-high-bits groups, sparse 10k-element bitmap,
      negative-high-bits sign handling, CRC mismatch, magic mismatch,
      truncated blob, inconsistent vector_size, helper round-trip
      sanity).
    - `TestDucklakePuffinDeleteFileGuard` (rewritten to assert
      `puffin` is accepted and an unknown format is rejected — replaces
      the previous "puffin not supported" assertion).
    - `TestDucklakeCrossEnginePuffinDeleteRoundTrip` (DuckDB writes with
      `write_deletion_vectors=true`, deletes some rows, Trino reads back
      only the survivors; also covers the no-op delete case where
      DuckDB writes nothing).

## Sorted-Table Awareness (Read)

- [x] **Read `ducklake_sort_info` / `ducklake_sort_expression`.** Landed
  2026-05-21. `JdbcDucklakeCatalog.getSortKeys(tableId, snapshotId)` reads
  the active sort spec (joined + ordered by `sort_key_index`) and returns
  `List<DucklakeSortKey>`. `DucklakeMetadata.getTableProperties` translates
  via `DucklakeSortPropertyMapper.toLocalProperties` and returns a
  `ConnectorTableProperties` carrying the matching
  `SortingProperty<ColumnHandle>` list, which Trino's planner uses to skip
  sort operators when an `ORDER BY` matches the leading prefix.
  - **Safety rule**: prefix-only translation. On the first sort key we
    cannot interpret (unknown dialect, non-column expression, column
    dropped at the snapshot), we stop and emit the safe prefix. Skipping
    a middle key would be a lie (the secondary sort wouldn't actually
    apply to the next-emitted column).
  - **Dialect**: only `duckdb` is honored. Foreign dialects bail to an
    empty list — different identifier/quoting rules can't be guessed.
  - **Column resolution**: case-insensitive name match against top-level
    columns at the snapshot. Quoted DuckDB identifiers (`"name"`,
    `"with""quote"`) and unquoted ASCII identifiers are both accepted.
  - Pinned by:
    - `TestDucklakeSortPropertyMapper` (12 cases — 4 SortOrder
      permutations, multi-key ordering preservation, case-insensitive
      match, quoted-identifier handling including `""` escape, unknown
      dialect, non-column expression, dropped column, empty input, and
      a battery of `parseColumnReference` edge cases).
    - `TestDucklakeCrossEngineSortedTableProperties` (3 cases — DuckDB
      `ALTER TABLE ... SET SORTED BY` two-key spec round-trips with
      mixed ASC/DESC + NULLS_FIRST/NULLS_LAST; unsorted tables yield
      empty properties; `RESET SORTED BY` end-snapshots the row and
      clears the local properties at the next snapshot).

## Type-Support Improvements

- [ ] **Variant** — currently degraded to VARCHAR (data accessible as string, no
  variant-specific operators, no shredded field access, no shredded statistics).
  Options: keep degraded; add JSON-style accessor functions over the VARCHAR;
  wait for native Trino Variant type support. See
  [archive/DUCKLAKE_1_0_IMPACT.md § Variant Type](archive/DUCKLAKE_1_0_IMPACT.md#3-variant-type).

- [ ] **Geometry** — currently degraded to VARBINARY (no spatial pruning, no
  bounding-box stats use). Wants integration with a Trino spatial library. See
  [archive/DUCKLAKE_1_0_IMPACT.md § Geometry Type](archive/DUCKLAKE_1_0_IMPACT.md#4-geometry-type).

## Virtual Columns

- [x] **DuckDB-equivalent virtual columns** — DONE. Ships **five** `$`-prefixed
  hidden columns on the read path (parquet + duckdb + inlined): `$path`,
  `$snapshot_id`, `$file_row_number`, `$row_id`, and `$file_size_bytes`. Hidden
  (out of `SELECT *` / `DESCRIBE`, queryable by name); constant-per-split
  virtuals injected as RLE blocks, row-varying (`$file_row_number`/`$row_id`)
  injected pre-delete-filter so positions reflect true file offsets; inlined
  rows return NULL for file-bound virtuals and per-row `begin_snapshot` for
  `$snapshot_id`; `$path` predicate pushdown prunes data files before the scan.
  Pinned by `TestDucklakeVirtualColumns` (21 cross-engine) + `TestVirtualKind`
  (6 unit). `$file_index`/`$filename` declined (see DESIGN § 8). DuckDB-name
  aliases (`rowid` etc.) deferred to v2. See
  [DESIGN-virtual-columns.md](DESIGN-virtual-columns.md).

## R6: Change Feed and Extended Metadata Parity

- [ ] Evaluate Trino equivalents for:
  - DuckDB `table_changes`, `table_insertions`, `table_deletions`
  - DuckLake-specific metadata surfaces beyond `$files` / `$snapshots`
- [ ] Implement only when there's a clear Trino use-case and a maintainable API
  shape.

## R7: Cross-Backend View Tests

- [ ] Test views across all catalog backends (SQLite, PostgreSQL, DuckDB) — today's
  view tests exercise PostgreSQL only.

## Alternative Columnar File Formats (epics)

DuckLake's `ducklake_data_file.file_format` column already enumerates the
file format per data file (today: `parquet`, `duckdb`). The catalog schema
is format-agnostic; adding a new format is a connector-side concern
(reader plumbing + executor-side ATTACH, where applicable) plus a writer
follow-up.

Two epics tracked here at high level. Neither is scheduled; both are
"when a real workload pushes for them" items.

**DuckDB extension availability (probed 2026-06-06, pinned DuckDB `1.5.3.0`):**
both `lance` and `vortex` are **core** extensions (HTTP 200 at
`extensions.duckdb.org/v1.5.3/{osx_arm64,linux_amd64}`), loadable via plain
`INSTALL lance; LOAD lance;` / `INSTALL vortex; LOAD vortex;` from the
in-process DuckDB executor — no community-repo opt-in. The reader probe for
each epic (write one file via DuckDB → register against a DuckLake table with
the matching `file_format` → attempt a Trino read through the duckdb-format
executor path) is unblocked. See the availability matrix in
[TODO-duckdb-lake-format.md](TODO-duckdb-lake-format.md).

### Lance file support

DuckLake snapshots that reference Lance-format data files. Lance is the
columnar storage shape used by lancedb — vectorized scans, vector-index
secondary structures, and a Trino-native reader path via the
`lance-jni` / `lance-arrow` surface or a Lance table-function pushdown.

Existing design sketch: [RESEARCH-lance-and-pushdown.md](RESEARCH-lance-and-pushdown.md).
Covers: Trino table-function SPI for Lance, route A vs route B trade-offs
(in-process Lance reader vs federation through a Lance service), and how
the existing pushdown layer (translator + macro alias surface in
`TODO-pushdown-duckdb.md`) would extend to Lance's expression syntax.

**Chunked plan: [TODO-lance.md](TODO-lance.md)** — both routes (A: via the
DuckDB `lance` extension; B: direct via `lance-core` JNI / the vendored
`lance-trino` plugin), phased, with the dataset-vs-file risk called out.

### Vortex file support

DuckLake snapshots that reference Vortex-format data files. Vortex is a
newer high-performance columnar storage layer (Spiral DB) with explicit
compression-cascade encodings and lazy decompression; the target use case
is analytic scans where Parquet's row-group encoding is a bottleneck.

**Chunked plan: [TODO-vortex.md](TODO-vortex.md)** — read via the DuckDB
`vortex` extension (no Trino-Vortex project exists to adopt); shares Route A's
file-scan machinery with Lance. Scope summary:

- **Reader path**: most likely route is a `VortexPageSource` paralleling
  the existing `DuckDbFilePageSource` / Trino's standard `ParquetPageSource`.
  Vortex's Java surface — if mature — replaces the Parquet reader for
  files whose `file_format = 'vortex'`. If Java bindings aren't mature,
  fall back to a Vortex-via-DuckDB extension (if one exists) and reuse
  the duckdb-format executor abstraction from step 4 of the pushdown
  program.
- **Pushdown integration**: TupleDomain + function-shape pushdown work
  the same way the parquet path does today; specifics depend on what
  Vortex's filter API surface looks like.
- **Cross-engine compatibility**: catalog-side `file_format = 'vortex'`
  has to be agreed between writer and reader. Check whether the
  upstream DuckLake spec has reserved the string (it should be opaque
  to the spec; the connector decides what it can read).

Open at the epic level — when a workload surfaces, start with a probe:
write one Vortex file outside DuckLake, register it as `add_files` against
a DuckLake table with `file_format='vortex'`, attempt to read via Trino,
record what blows up. That tells us what the first PR scope is.

## Research / Evaluation

- [ ] **Evaluate adopting the DuckLake `.slt` corpus** as a portable regression
  suite for the catalog library. Source: `COMPARE-datafusion-ducklake.md`.

### Open research items (read-path)

Full per-item rationale in [`archive/RESEARCH-TODO.md`](archive/RESEARCH-TODO.md).
When promoted, move into the section above it belongs to (e.g. Type-Support
Improvements, Inlined-Read Type Gaps).

- **uint-type-promotion-audit** — upstream PR #1128 fixed type promotion for
  UINTEGER. Verify it's purely DuckDB-execution (not in `ducklake_column.column_type`
  catalog representation). ~10-min PR-and-test read.
- **schema-evolution-missing-column** — upstream PR #1142 fixed missing-column
  handling under schema evolution. Reproduce their test in our cross-engine harness
  (DuckDB inserts → `ALTER TABLE ADD COLUMN` → DuckDB inserts more → Trino reads):
  confirm old-file rows project NULL/default for the new column. ~1h spike.
- **delete-file-filter-pushdown** — can we hand a `TupleDomain` to the delete-file
  Parquet reader the same way we do for data files? Saves IO on narrow predicates
  (e.g. single `pos` value in a MERGE plan). Scoping spike against Trino's
  `ConnectorPageSource` filter-pushdown surface.
- **partial_max time-travel correctness** — ⚠️→✅ GATED 2026-06-29 (full filter still open).
  `ducklake_data_file` / `ducklake_delete_file` carry `partial_max BIGINT` for **cross-snapshot
  compacted files** (DuckLake's `merge_adjacent_files`): a correct read at snapshot `S` filters the
  file's physical `_ducklake_internal_snapshot_id <= S`, needed only when `partial_max > S`. We
  ignored it → time-travel over-inclusion. `DucklakeSplitManager` now **rejects** a read when an
  active file's `partial_max > snapshotId` (`catalog.hasPartialFilesRequiringSnapshotFilter`) — a
  loud `NOT_SUPPORTED` instead of wrong rows; reads at/above `partial_max` (incl. the latest) pass.
  `TestDucklakePartialFileGuard`. **Still open (follow-up):** the full per-row filter (project
  `_ducklake_internal_snapshot_id`, push `<= S`) so time-travel of compacted tables WORKS rather
  than erroring — and the hard prerequisite before we EMIT compacted files. See
  [DESIGN-maintenance.md § 6](DESIGN-maintenance.md).
- **nested-field-id-top-level-match** — datafusion-ducklake #148 (2026-06) fixed
  List/struct/map columns reading back **all-NULL**: their field-id matcher keyed off
  Parquet *leaf* columns, but a column's field-id is stamped on the *top-level* field
  (the group node for a nested type). We just shipped nested ADD/DROP FIELD (step2-m3);
  verify our nested field-id resolution reads ids off the top-level field, not the leaf,
  with a List/struct write→read roundtrip across schema evolution. ~1h spike.

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
