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

Next bite-sized read items: the `R7` cross-backend view tests (SQLite backend still planned).
None are correctness blockers. (Virtual columns `$snapshot_id`/`$path`/`$row_id`/`$file_row_number`/
`$file_size_bytes` shipped; only the DuckDB `rowid` name-alias is deferred to v2. Sorted-table read
awareness landed 2026-05-21; see § Sorted-Table Awareness (Read) below.)

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

- [x] **Change feed SHIPPED** — `system.table_insertions` / `table_deletions` / `table_changes`
  table functions (scan-rewrite PTFs, all file formats, inclusive snapshot-id/timestamp bounds).
  Reads file data/delete files AND DuckLake **inlined** data (inlined inserts, inlined-row deletes,
  inline file-position deletes). Update pairing (`update_preimage`/`update_postimage`) works via the
  embedded row-lineage column (parquet field-id 2147483540) for lineage-preserving writers — DuckDB
  AND, since 2026-07-06 (F7), Trino's own UPDATE/MERGE under `write_row_lineage = true` (default off
  keeps the delete+insert shape). See README § Change Feed and TODO-jayson-special-list.md § F9.
- [ ] DuckLake-specific metadata surfaces beyond `$files` / `$snapshots` / `$current_snapshot` /
  `$snapshot_changes` — evaluate as use-cases surface.

## R7: Cross-Backend View Tests

- [ ] Test views across all catalog backends (SQLite, PostgreSQL, DuckDB) — today's
  view tests exercise PostgreSQL only.

## Alternative Columnar File Formats (epics)

DuckLake's `ducklake_data_file.file_format` column already enumerates the
file format per data file (today: `parquet`, `duckdb`). The catalog schema
is format-agnostic; adding a new format is a connector-side concern
(reader plumbing + executor-side ATTACH, where applicable) plus a writer
follow-up.

Two epics tracked here at high level. **UPDATE: both have SHIPPED** — Lance (read + write +
add_files + vector/FTS/hybrid search) and Vortex (read + write + add_files) are in the connector;
see TODO-lance.md / TODO-vortex.md. The section below is kept as the original design rationale. The
epics were originally framed as unscheduled "when a workload pushes" items:
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

SHIPPED (read + write + add_files). Original probe-first plan, kept for the record —
write one Vortex file outside DuckLake, register it as `add_files` against
a DuckLake table with `file_format='vortex'`, attempt to read via Trino,
record what blows up. That tells us what the first PR scope is.

## Research / Evaluation

- [ ] **Evaluate adopting the DuckLake `.slt` corpus** as a portable regression
  suite for the catalog library. Source: `COMPARE-datafusion-ducklake.md`.
  Scoped 2026-07-05: corpus is now **492 `.test` files** (was 248 at the 05-29
  COMPARE snapshot — doubling fast; adoption value rising). datafusion's runner =
  `sqllogictest` crate + ~100-line preprocessor (strip directives, skip
  ATTACH/DETACH + DuckDB-only queries), running a curated snapshot.
  **Recommended shape for us: cross-engine oracle REPLAY, not translation** —
  execute each `.test` verbatim through embedded DuckDB (harness already
  attaches it) to build catalog state with zero dialect work, then for each
  plain-SELECT `query` directive run the equivalent read through Trino on the
  same catalog and diff **live result sets** (DuckDB-as-oracle vs Trino, not
  golden text). Turns the corpus into a read-parity fuzzer over our
  hand-written-metadata risk surface; each upstream refresh delivers new cases
  for free. Effort: slt parser ~1d (line-oriented, no Java lib — write our own),
  replay driver 1-2d, skip-list curation a few days → green subset in under a
  week. Statement-translation layer (Trino executes writes) is a possible later
  bolt-on, not v1.
  **Multi-engine/multi-backend scope (decided 2026-07-05):** target Trino AND
  Doris, across all catalog backends. (a) Runner core (parser, replay driver,
  `ReplayReadEngine` interface, DuckDB oracle) lives in
  **`ducklake-catalog/testFixtures`** as a library export — it runs nowhere
  itself. Suite entrypoints run ENGINE-SIDE: Trino adapter
  (`DucklakeQueryRunner`-backed) + its corpus suite in `trino-ducklake/test`;
  Doris adapter (JDBC→FE) + suite in `doris-ducklake` tests; the
  DuckDB-identity control suite in `ducklake-catalog/test` (validates the
  runner, zero engine deps). Engine testing stacks never enter the shared lib.
  (Alternative if testFixtures feels overloaded: standalone
  `jvm/ducklake-corpus-replay` module.) Corpus distribution: **pinned git
  submodule of `duckdb/ducklake`** (vendor/ is git-ignored, unavailable in CI);
  bump the pin during biweekly upstream refreshes. (b) Backend axis via the
  ATTACH-rewrite step (duckdb-local / Postgres / Quack / SQLite-when-shipped) —
  **seed skip lists from upstream's own `test/configs/{postgres,sqlite,quack}.json`**
  (structured skips with reasons; the corpus is already designed for
  backend parameterization — we inherit their curation). (c) Compare
  canonicalized typed values + `rowsort` (upstream's `sort_style`), never text —
  three engines format differently. (d) Tier the matrix: PR = trino ×
  duckdb-local fast subset; nightly = full engines × backends (upstream does the
  same for quack CI). (e) Later: mirror upstream *mode* configs
  (`deletion_vectors.json`, `no_inline.json`, `ducklake_version.json`) to run
  the corpus under our write-mode session properties. Doris agent should
  co-review the `ReplayReadEngine` interface before it's built.

### Corpus-mirror findings (2026-07-06 — REAL BUGS — both ✅ FIXED same day)

Found by `TestTrinoCorpusReplay` (upstream corpus replayed through the DuckDB
oracle on the PG backend axis, lake reads mirrored through Trino live-vs-live).

- [x] **INLINED struct/map reads crash** — ✅ FIXED 2026-07-06 by IMPLEMENTING
  inlined nested reads (retires B2 entirely, not just the crash).
  `NestedTextParser` in `DucklakeInlinedValueConverter` parses DuckDB's
  value-text serialization (struct `{'k': v}` SQL form, map `{k=v}`, list
  `[…]`, quote/escape/NULL rules, arbitrary nesting) into real
  SqlRow/SqlMap/Block. Schema evolution is handled by IDENTITY, not name:
  `InlinedNestedFieldMapper` builds per-schema-version field translations from
  the `ducklake_column` tree (new catalog API `resolveSchemaVersionSnapshot`),
  so RENAMEd fields carry values and REUSEd names (drop `i`, re-add `i`) stay
  NULL — the two cases where name-matching is silently wrong. All 10 corpus
  repro files un-skipped and green (`alter/struct_evolution*`,
  `add/drop_column_nested`, `types/struct`, `struct_in_{list,map}_evolution`).
  Unit coverage in `TestDucklakeInlinedValueConverter`. Note: the change-feed
  inlined path and `flush_inlined_data` share the converter and no longer
  crash, but pass no era mapping yet (name-fallback) — rename/reuse evolution
  through THOSE two paths is a small follow-up.
- [x] **legacy-delete-mapping-after-rename+add_files divergence** — ✅ FIXED
  2026-07-06. Real silent-wrong-results: an add_files parquet with no
  `mapping_id` and no parquet field_ids (legacy v0.3 shape), read after a
  column RENAME, NULL-filled the renamed column (DuckDB still reads it).
  `createParquetPageSource` gained matching step (4): era-name fallback — on a
  miss, resolve the column's name at the file's `begin_snapshot` (reusing the
  cached `resolveFileColumnNames`, same mechanism the DuckDB-executor path
  already used) and match the parquet column by that name. Corpus repro
  un-skipped and green.

### Open research items (read-path)

Full per-item rationale in [`archive/RESEARCH-TODO.md`](archive/RESEARCH-TODO.md).
When promoted, move into the section above it belongs to (e.g. Type-Support
Improvements, Inlined-Read Type Gaps).

Expected-landing tag `[v: …]`: `CURRENT` = DuckLake v1.0 / DuckDB 1.5.x (what we
run today); `NEXT` = DuckLake v1.1 (`V1_1_DEV_1`) / DuckDB 2.x, Fall. **[NOW-n]**
marks the CURRENT-version actionable set — stable ids shared with
`TODO-WRITE-MODE.md` for the parallel agent working this area now (NOW-1/4/5/6 are
write-path; NOW-2/3 below are read-path).

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
- **partial_max time-travel correctness** — ✅ DATA files FILTERED 2026-06-29 (delete files still
  gated). Cross-snapshot compacted files (DuckLake's `merge_adjacent_files`) physically carry per-row
  `_ducklake_internal_snapshot_id`; a correct read at `S` keeps only rows `<= S`, needed when
  `partial_max > S`. The connector now reads the column and drops file-local positions `> S` via the
  `DeleteRowFilterTransform` set (split carries `snapshotFilterMax`); time-travel of a DuckDB-compacted
  table returns correct rows (`TestDucklakePartialFileFilter`, cross-engine). Consolidated **parquet
  DELETE files** are also filtered now (split carries `deleteFileSnapshotFilters`; the delete reader
  keeps only deletions whose `_ducklake_internal_snapshot_id <= S` — `TestDucklakePartialDeleteFilter`,
  cross-engine via `flush_inlined_data` consolidation). Consolidated **PUFFIN** delete files are
  now snapshot-filtered per blob too (`DucklakePuffinDeleteReader` reads each blob's
  `ducklake-snapshot-id`) — `TestDucklakePuffinPartialDelete`. **All partial-file reads (data +
  parquet-delete + puffin-delete) are correct; no read gate remains.** See
  [DESIGN-maintenance.md § 6](DESIGN-maintenance.md).
- ✅ **[NOW-3] nested-field-id-top-level-match** — VERIFIED CLEAN 2026-07. Two-part check:
  (1) STRUCTURAL — `DucklakePageSourceProvider.createParquetPageSource` builds its field-id index
  from `fileSchema.fields` (TOP-LEVEL fields): `fieldIdToColumnIO[field.id] = messageColumnIO.
  getChild(field.name)`, i.e. the top-level group `ColumnIO` for a nested type. The primary
  name-match path also resolves the top-level `ColumnIO`, and `constructField(columnType, columnIO)`
  builds the nested Field from it. So ids are read off the top-level field, not the leaf — the
  opposite of the datafusion #148 bug — on BOTH the name-match and the field-id-fallback paths.
  (2) EMPIRICAL — `TestDucklakeAlterTable.renameNestedColumnsPreserveTheirValues` renames a
  top-level ARRAY, ROW, and MAP column (so the parquet file keeps the OLD name → the read falls
  back to field-id matching, the exact bug analog) and value-asserts the nested contents survive
  (a leaf-keyed matcher would return all-NULL). Passes. (Earlier "PARTIALLY VERIFIED" note was
  right to distrust the scalar-lineage-column proof; this closes it on the general nested path.)
  Original: datafusion-ducklake #148 (2026-06) — nested field-id matcher keyed off parquet leaf
  columns → List/struct/map read back all-NULL.
- ✅ **[NOW-2] inlined-insert-change-vocab** — VERIFIED CLEAN (2026-07), not applicable. Our
  `InterveningChanges.applyEntry` already uses the correct DuckLake-1.0 spelling `inlined_insert:<id>`
  / `inlined_delete:<id>` (not the old `inlined_data_insert` that caused pg_ducklake #216's data
  loss). And it doesn't gate the read path anyway: `$snapshot_changes` returns the raw `changes_made`
  text (no parse) and the change feed queries files/inlined rows by `begin_snapshot` (never parses
  tokens); the parser is used only in write-conflict detection, which round-trips the inlined tokens.
  (Unknown *future* tokens `else -> throw`, i.e. fail-closed — deliberate, left as is.)

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
| `FROM table_changes(...)` | Change feed | `system.table_changes(...)` table function | **Shipped** |
| `FROM table_insertions(...)` | Change feed | `system.table_insertions(...)` table function | **Shipped** |
| `FROM table_deletions(...)` | Change feed | `system.table_deletions(...)` table function | **Shipped** |
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
