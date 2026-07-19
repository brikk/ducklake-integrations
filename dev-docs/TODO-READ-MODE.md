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
- [ ] **Follow-up (perf/heap, review 2026-07-12): keep Roaring, don't explode to
  `HashSet<Long>`.** `decodeBitmaps` (`DucklakePuffinDeleteReader.kt:293-318`)
  materializes every deleted position as a boxed `Long` in a `HashSet` (`:306-307`),
  then those sets are merged (`DucklakePageSourceProvider.kt:989-1022`) and copied
  again in the delete transform (`:1857-1864`). A dense multi-million-position
  vector then costs far more heap than its on-disk Roaring form, with per-split
  allocation churn. Not a correctness bug (size guard at `:121-122`; corruption
  fails loud) — an optimization: retain `RoaringBitmap` (or a primitive
  `long`/bitset membership structure) through the filter path, drop the duplicate
  copies, and consider an explicit resource cap for pathological delete files.
  Profile a dense vector under concurrent scans first. Source:
  [TODO-possible-terra-issues.md § P2 Puffin](TODO-possible-terra-issues.md).

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

- [x] **`json` → native Trino `JsonType`** (driving-list F8). Landed 2026-07-10. DuckLake `json`
  now maps to Trino's native `io.trino.type.JsonType` (was degraded VARCHAR), so JSON path/accessor
  functions (`json_extract`, `json_format`, `->`, etc.) work directly on the column. Since JsonType
  lives in `trino-main` (not the connector's `trino-spi` compile classpath), it is resolved via
  `TypeManager.getType(TypeSignature("json"))` and recognized elsewhere by its signature base string
  through `DucklakeJsonSupport.isJson` (the same reflective-bridge pattern `DuckDbExpressionTranslator`
  uses for `LikePattern`). Physically JSON is a UTF-8 string — exactly how DuckDB stores it — so:
  - **Parquet read**: reconstructs `JsonType` and reads the BINARY column via Trino's
    `AbstractVariableWidthType` reader path (no change needed there).
  - **Parquet write**: `DucklakeJsonSupport.toParquetWriteType` swaps JSON→VARCHAR for the physical
    write schema (the `ParquetSchemaConverter`/`ParquetWriters` library rejects `JsonType`); the
    catalog type stays `json` (top-level only; nested JSON inside ARRAY/MAP/ROW fails loud for v1).
  - **DuckDB-format write + read**: `toDuckDbSqlType` → `JSON`; the appender / arrow-stream writers
    exchange the JSON text as Utf8; the Arrow→page converter wraps it back into a JSON Slice.
  - **Stats**: JSON is excluded from min/max (not orderable in DuckDB) — value/null counts only.
  Pinned by `TestDucklakeCrossEngineJson` (parquet round-trip Trino↔DuckDB, duckdb-format appender +
  arrow-stream Trino round-trip, DuckDB-writes/Trino-reads with a `json_extract_scalar` assertion)
  and `TestDucklakeTypeConverter` (json↔JsonType mapping). Nested JSON, JSON identity-partitioning,
  and JSON predicate pushdown are follow-ups (all currently safe: fail-loud or don't-push).
- [ ] **`interval` / `uint128` — permanent VARCHAR trade** (driving-list F8). Both stay degraded to
  VARCHAR; data round-trips as text. Neither has a lossless native Trino mapping:
  - `uint128` (DuckDB UHUGEINT, `0..3.4e38`) exceeds every Trino type — `DECIMAL(38,0)` tops out
    below it and there is no unsigned-128 type. Permanent.
  - `interval`: DuckDB's `INTERVAL` carries months **and** days **and** micros in one value; Trino
    splits intervals into `INTERVAL YEAR TO MONTH` and `INTERVAL DAY TO SECOND` and cannot hold all
    three components at once, so there is no lossless mapping (the corpus replay already declines
    DuckDB interval literals as a dialect gap — `TrinoReplayEngine.accepts`). Revisit only if Trino
    grows a combined interval type or a workload accepts a lossy/guarded split.

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
  keeps the delete+insert shape). See README § Change Feed and archive/TODO-jayson-special-list-COMPLETED-2026-07.md § F9.
- [x] **change-feed insert side per-row origin-snapshot attribution — FIXED 2026-07-18.** Was:
  `insertUnit` stamped `snapshotId = file.beginSnapshot` for EVERY row, so `table_changes` /
  `table_insertions` over a `merge_adjacent_files` / `rewrite_data_files` output (`partial_max >
  begin_snapshot`) attributed all merged rows to one snapshot and mis-windowed rows at the
  (inclusive) boundary. Now the insert side reads each row's embedded `_ducklake_internal_snapshot_id`
  (`DucklakeDeleteFileReader.readInternalSnapshotIds`, matched by column name — the read-only twin of
  `readSnapshotDropPositions`) for partial files and:
  - **File selection** — `getDataFilesAddedBetween` (`JdbcDucklakeCatalog.kt`) now also admits files
    whose `begin_snapshot < start` but `partial_max >= start` (a partial file can begin before the
    window yet carry in-window rows). Predicate: `begin_snapshot <= end AND (begin_snapshot >= start
    OR partial_max >= start)`.
  - **Per-row attribution** — `insertUnitsForFile` (replaces `insertUnit`) splits a partial file into
    one `ChangeFeedUnit` per distinct origin snapshot in `[start, end]`, each with `keepPositions` =
    that snapshot's file positions and `snapshotId` = that snapshot; rows whose origin is outside the
    window are dropped. Ordinary files still emit one whole-file unit at `begin_snapshot`.
  - **Update pairing** — `computeUpdatedRowids` buckets a partial file's inserted rowids by per-row
    origin snapshot (lineage rowid × origin snapshot, index-aligned) instead of `begin_snapshot`.
  Read/time-travel path was already correct (`DucklakeSplitManager.kt:633-637`); this brings the
  change feed to parity. Mirrors datafusion-ducklake #185 (2026-07-16). Test:
  `TestDucklakeChangeFeedPartialFile` (DuckDB N-snapshot write → `merge_adjacent_files` → Trino
  `table_insertions`/`table_changes` sub-windows assert per-origin attribution). Note: a partial file
  is re-scanned once per in-window origin snapshot (the existing "fully eager" change-feed tradeoff,
  below); acceptable — origin-snapshot count per compacted file is small.
- [x] **change-feed DELETE side per-deletion snapshot attribution for consolidated files — FIXED
  2026-07-18 (delete-side analog of the insert `partial_max` fix).** REACHABILITY CONFIRMED (not
  hypothetical): DuckDB `DELETE` (id=1) → `DELETE` (id=5) → `flush_inlined_data` produces ONE
  consolidated 3-column delete file with `begin_snapshot = MIN` (4), `partial_max = MAX` (5), each
  row carrying its own `_ducklake_internal_snapshot_id` (verified via probe + upstream
  `ducklake_delete.cpp` WriteDeleteFileInternal: `begin_snapshot = min(per-row snapshot)`,
  `WriteNewDeleteFiles`: `partial_max = max_snapshot`). Before the fix the incremental arm
  (`getDeletionsBetween`) reported BOTH deletions at `begin_snapshot=4`: `table_deletions` over
  window `[4,4]` wrongly included id=5, and over `[5,5]` MISSED id=5 entirely (empirically
  reproduced, then fixed). Fix, mirroring the insert side:
  - **File selection** — `incrementalDeletions` (`JdbcDucklakeCatalog.kt`) now admits a consolidated
    delete file when its `[begin_snapshot, partial_max]` span overlaps the window (`begin <= end AND
    partial_max >= start`), via `deleteFileOverlapsWindow`. Ordinary files still filter on
    `begin_snapshot in window`.
  - **Per-deletion attribution** — `DucklakeDeleteFileReader.readPositionsWithSnapshots` reads the
    3-column file into `pos → deletion snapshot`; `newlyDeletedPositionsBySnapshot`
    (`DucklakePageSourceProvider.kt`) groups positions by that snapshot, keeps only in-window
    groups, and emits one `ResolvedChangeFeedDeletion` (new per-event `snapshotId`) per group.
    `previous` is not subtracted for consolidated files (consolidation folds prior deletions INTO
    the file with their own snapshots). Ordinary/full-file deletes unchanged (`current − previous`
    at `begin_snapshot`).
  Test: `TestDucklakeChangeFeedPartialDelete` (DuckDB 2-snapshot delete → `flush_inlined_data` →
  `table_deletions` sub-windows assert per-deletion-snapshot attribution). datafusion-ducklake #185
  fixed the identical case. Found + fixed during the 2026-07-18 upstream run.
- [ ] DuckLake-specific metadata surfaces beyond `$files` / `$snapshots` / `$current_snapshot` /
  `$snapshot_changes` — evaluate as use-cases surface.
- [ ] **Follow-up (perf/latency, review 2026-07-12): change-feed setup is fully
  eager.** `createChangeFeedPageSource` (`DucklakePageSourceProvider.kt:560-618`)
  resolves ALL insert files + deletions, reads each file's entire lineage column
  into a `LongArray` (`readInsertLineage`; collection at
  `DucklakeDeleteFileReader.kt:201-216`), and builds whole-window
  `deletedRowidsBySnapshot` + update-pairing before the first output page — so
  time-to-first-row and retained heap scale with the history window, not the page,
  and input may be re-read (once for lineage, once for output). Correct, just
  eager. **Caveat for any fix:** update-pairing (delete + re-insert on the same
  rowid) inherently needs cross-window state, so a lazy/bounded-batch rewrite may
  need compact keyed/on-disk pairing state — do NOT drop pair semantics to stream.
  Measure a large update/delete window (time-to-first-row + retained heap) first.
  Source: [TODO-possible-terra-issues.md § P2 change-feed](TODO-possible-terra-issues.md).

## R7: Cross-Backend View Tests

- [ ] Test views across all catalog backends (SQLite, PostgreSQL, DuckDB) — today's
  view tests exercise PostgreSQL only.

## Alternative Columnar File Formats (epics) — REMOVED (P6, 2026-07)

The DuckDB `.db` / Lance / Vortex data-file formats were removed from
`trino-ducklake` (now parquet-only) and the DuckDB-engine machinery — including
the Lance/Vortex read surfaces, their design docs, and the pushdown program —
moved to **github.com/brikk/duckbridge** (see that repo's `trino-duckbridge/dev-docs/archive/`).
This connector reads parquet data files only; a non-parquet `file_format` in a
catalog now fails loud (see PLAN-duckdb-parity-moveout.md §7).

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

### Corpus-mirror findings round 2 (2026-07-07, add_files dir — 2 REAL BUGS, diagnosed + tracked)

Both are silent-wrong-results on DuckDB-registered external (add_files) parquet.
Repro files skip-listed in `TestTrinoCorpusReplay` with `BUG:` prefixes.

- [x] **hive-partition columns from DuckDB add_files read as NULL** — ✅ FIXED
  2026-07-07. DuckDB's `ducklake_add_data_files(..., hive_partitioning => true)`
  records NO `ducklake_file_partition_value` rows and NO partition spec; instead
  the partition columns are `ducklake_name_mapping` entries with
  **`is_partition = true`** (`mapping_id`, `source_name=part_key`,
  `target_field_id=<column_id>`), and the VALUE lives in the file PATH
  (`part_key=1/part_key2=10/data.parquet`). The reader used to ignore
  `is_partition`, try to match a parquet column named `part_key`, miss → NULL.
  Fix: new `DucklakeCatalog.getPartitionNameMaps` surfaces the `is_partition`
  entries (`column_id → path key`); `DucklakeSplitManager` parses each file's
  path for the matching `key=value` segment (URL-decode `%20`;
  `__HIVE_DEFAULT_PARTITION__` → NULL, i.e. left unset) and merges the value into
  the split's `partitionValuesByColumnId`, so the existing missing-column
  constant-fill machinery (`buildMissingColumnBlock`) projects the typed value.
   Rename-safe: values are keyed by `column_id` via `target_field_id`. Corpus
   repros un-skipped and green: `add_files/add_files_hive{,_mismatch}.test`.
   **Follow-up completion 2026-07-08** (the full `add_files` corpus dir finally ran
   — earlier "verification" used a malformed `-Ducklake.corpus.dirs` flag that JVM-parsed
   as `-D ucklake.corpus.dirs` and silently fell back to the default dirs): two more
   real gaps surfaced and were fixed, so `add_files_hive_partition_cast.test` is now green:
   (a) **path double-encoding** — the reader's `toLocation` routed the on-disk path through
   `Path.toUri()`, which percent-encodes the literal `%` in DuckDB's URL-encoded hive dirs
   (`category=home%20appliances` → looked-for `%2520`); fixed to prefix `file://` on the raw
   path (Trino's `Location` does not percent-decode). (b) **`DucklakePartitionValueParser`
   lacked DECIMAL + TIMESTAMP(/TZ)** — those partition values fell back to NULL; added.
   The file's two `INTERVAL 1 DAY` (unquoted DuckDB literal) queries are a pure dialect gap
   Trino can't parse, now declined by `TrinoReplayEngine.accepts` (not a connector bug).
- [x] **nested struct-FIELD initial defaults not projected** — ✅ FIXED 2026-07-08.
  Corpus `default/struct_field_default.test`: a FIELD added to a struct with a default
  (`ALTER ... ADD COLUMN s.k ... DEFAULT 42`) must project 42 for old rows; we projected
  NULL. The default lives on the catalog CHILD row's `initial_default` — the 4th path of
  the issue-1135 family. Fixed BOTH named paths: the INLINED nested-text parser
  (`InlinedNestedFieldMapper` threads each era-absent child's parsed `initial_default` into
  `InlinedTextFieldMapping.Struct.defaults`; `NestedTextParser.parseStruct` fills era-absent
  fields with it) and the reshape planner (`StructFieldPlan.initialDefault` →
  `DuckDbSelectSqlBuilder` renders `CAST('<default>' AS type)` instead of `CAST(NULL …)`).
  Corpus repro un-skipped and green; unit-pinned in `TestNestedFieldReshapePlanner`,
  `TestDuckDbSelectSqlBuilder`, and (inlined) the corpus mirror. Top-level defaults were
  fixed 2026-07-07 (parquet missing-column, duckdb-executor CAST, inlined era-defaults).
- [x] **name-map authoritative for MAPPED files** — ✅ FIXED 2026-07-07. The
  parquet matcher now resolves map-carrying files (`mapping_id` set →
  `split.fieldIdToParquetSourceName` non-empty) through target_field_id ONLY
  (map miss = NULL/default), never a bare-name coincidence; unmapped files keep
  name → field-id → era-name. Fixed the mapped re-add case (`my_file4` col2
  reads NULL). [x] **RESIDUAL — ✅ FIXED 2026-07-07.** corpus
  `add_files/add_files.test:170` diverged on the row-100 file: an UNMAPPED
  INSERT-written parquet physically carries a `col2` column written when col2
  FIRST existed, before col2 was dropped + re-added under a new column_id. The
  unmapped bare-name match resurrected it (read `hello`, upstream reads NULL).
  Fix: `resolveColumnIO` now gates the bare-name match on era-aware column
  existence — `eraColumnNames` (already loaded: `column_id → physical name` as of
  the file's `begin_snapshot`) is consulted, and a `column_id` absent from it
  (i.e. not alive when the file was written) is NOT allowed to name-match, so the
  since-dropped identity's bytes stay hidden → NULL. Empty era map (test split /
  no begin_snapshot) keeps the legacy name-first behavior. Corpus repro
  un-skipped and green.

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

## Robustness / Performance Follow-ups (review 2026-07-12)

Triaged from [TODO-possible-terra-issues.md](TODO-possible-terra-issues.md); the
verdicts there hold the counter-evidence for the items that were refuted. The
CONFIRMED read-path items that don't already have a home section:

- [ ] **Fail loud on an unparseable stored partition/default value** (robustness,
  P1 site 2). `buildMissingColumnBlock` (`DucklakePageSourceProvider.kt:1780-1789`)
  catches a parse failure of a `ducklake_file_partition_value` /
  `initial_default` and returns NULL. Unlike the *pruning* path (whose parse
  tolerance safely keeps a file), projecting NULL here emits a **wrong value** and
  the caller can't tell it from a genuine SQL NULL — violates AGENTS.md "fail loud
  over silently wrong". Fix: throw a clear `TrinoException` when a *present*
  catalog value can't be decoded to the column type; keep NULL only for a
  demonstrably absent column. (Note: the sibling concern — a present *primitive*
  parquet column turning into NULL — was REFUTED: `constructField` always builds a
  Field for present primitives and real type mismatches fail loud in
  `ParquetReader`; only malformed nested map/list/row schemas hit the empty path.)
- [ ] **Batch file-stat pruning into one query per plan, not per column** (perf,
  P2). `DucklakeSplitManager.pruneDataFiles` (`:261-291`) calls
  `findDataFileIdsInRange` once per predicate column, and each call
  (`JdbcDucklakeCatalog.kt:960-1014`) fetches ALL active files (LEFT JOIN stats)
  and filters `isWithinBounds` in the JVM — so C prunable columns ≈ C full
  active-file scans + materializations before split construction. The JVM-side
  compare is deliberate (text-stored MIN/MAX need type-aware `parseStatValue`,
  hard to push into generic cross-backend SQL), so keep it; the win is to fetch
  the stats for all predicate columns in a single query and pivot in the JVM
  (C→1). Preserve the "unknown stats → keep the file" LEFT-JOIN semantics.
- [x] **Process-unique `.partial` name in the DuckDB read cache** — ✅ DONE
  2026-07-13. `DucklakeMaterializedFileCache.materialize` now stages each
  download to `<key>.<pid>-<uuid>.partial` (via `uniqueSuffix()`) and cleans it
  up in a `finally`, so two co-located JVMs sharing `${java.io.tmpdir}/
  ducklake-read/` can never write to the same partial file — the worst case is a
  duplicate download, not a corrupt `.db` handed to DuckDB `ATTACH`. The per-key
  lock still serializes downloads within a JVM (avoids redundant fetches). Pinned
  by `TestDucklakeMaterializedFileCache.testTwoJvmsSharingCacheDirDoNotClobber
  EachOther` (two independent cache instances race the same dir+key; both get
  correct bytes, one `.db` remains, no partials leak). (The cache's *no-eviction
  / disk-growth* behavior remains a documented Phase-1 deferral — not tracked
  here.)
