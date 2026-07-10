# DESIGN: Maintenance operations (F6)

**Status:** design note + first procedure (`remove_orphan_files`) — the rest is roadmap.
**Scope:** trino-ducklake connector. **Companion to:** TODO-WRITE-MODE.md § M8, archive/TODO-jayson-special-list-COMPLETED-2026-07.md § F6.

F6 is the biggest single hole on the driving list: `optimize` / `rewrite_data_files` /
`expire_snapshots` / `cleanup_old_files` / `remove_orphan_files` / stats-recalc are all absent, so
operators must run DuckDB against the shared catalog. The one with real operational consequence is
orphan files: **a failed/aborted commit can leave a data file on storage that no snapshot
references, and today Trino has no remedy** (`flush_inlined_data` and `ANALYZE` are the only
maintenance ops that shipped). This note settles the cross-cutting design question the driving list
flagged FIRST — *"the in-place-mutation / snapshot-safety design decided FIRST"* — and then ships
ONE safe procedure built on it.

## 1. The snapshot-safety decision (the load-bearing one)

The danger with every maintenance op is **pulling a file out from under a concurrent reader** —
including a reader on *another engine* (DuckDB, pg_ducklake) pinned to an older snapshot, which the
connector can't see or coordinate with. DuckLake already solved this, and we adopt its model
verbatim for cross-engine compatibility:

1. **File liveness is the half-open interval `[begin_snapshot, end_snapshot)`.** A data/delete file
   is live at snapshot `s` iff `begin_snapshot <= s AND (end_snapshot IS NULL OR s < end_snapshot)`.
   `end_snapshot IS NULL` ⇒ current. This is exactly `SnapshotRange.activeAt(...)`, already used
   throughout the read path.

2. **Catalog mutation NEVER physically deletes a file.** Operations that retire a file (expiry,
   compaction, rewrite) only (a) update catalog rows and (b) **schedule** the now-unreferenced file
   into `ducklake_files_scheduled_for_deletion (data_file_id, path, path_is_relative,
   schedule_start = now())`. The physical `unlink` happens in a *separate*, later step.

3. **Physical deletion is always age-gated by a grace period.** Both the scheduled-file drain
   (`cleanup_old_files`, keyed on `schedule_start`) and the orphan sweep (`remove_orphan_files`,
   keyed on the file's storage mtime) only delete files **older than a retention threshold**. The
   grace period is what makes deletion safe without a global lock: a file young enough to still be
   referenced by an in-flight commit or a just-detached snapshot on another engine is never
   touched. DuckLake defaults this to `2 days`; we default to **`7 days`** (matching Trino's
   Iceberg connector, which operators already know) and enforce a configurable **minimum**
   retention so the op can't be turned into a foot-gun.

This is "two-phase deletion": **logical retirement (catalog) → age-gated physical reclaim
(storage)**. It is the answer to "in-place mutation vs snapshot safety": we never mutate a file in
place and never delete one inside the same commit that retires it.

## 2. Procedure surface (Iceberg-aligned, table-scoped)

Trino operators know the Iceberg maintenance procedures; we mirror that surface so muscle memory
transfers. All are `CALL <catalog>.system.<name>(...)`, table-scoped (DuckLake lays files out under
a per-table data path, so a table scope maps cleanly onto a directory):

| Procedure | Status | Shape |
|---|---|---|
| `remove_orphan_files` | **this PR** | `(schema_name, table_name, retention_threshold => '7d', dry_run => false)` |
| `expire_snapshots` | roadmap | `(schema_name, table_name, retention_threshold \| versions, dry_run)` — catalog mutation + scheduling |
| `cleanup_old_files` | roadmap | drains `ducklake_files_scheduled_for_deletion` past `schedule_start + grace` |
| `optimize` / `rewrite_data_files` | roadmap | compact small files into fewer/larger ones |
| stats-recalc | **done** | shipped as `ANALYZE` |

(DuckLake's upstream surface is catalog-scoped table functions — `ducklake_expire_snapshots`,
`ducklake_delete_orphaned_files`, `ducklake_cleanup_old_files` — with `older_than` / `versions` /
`cleanup_all` / `dry_run` named params. We keep the *semantics* identical but present them as
table-scoped Trino procedures; see § 5 for the mapping.)

## 3. First procedure: `remove_orphan_files` (this PR)

Chosen first because it is **the safest and addresses the named operational hole**:

- **No catalog mutation.** Orphans, by definition, have no catalog row — removing them only touches
  storage. So this procedure needs **no new snapshot, no `WriteChange`, no `ConflictMatrix` /
  `LogicalConflictCheck` changes, no retry loop**. That keeps the first F6 increment small and
  low-risk while still proving the filesystem-maintenance plumbing (list + age-gate + delete) that
  later procedures reuse.
- **Directly fixes "orphans from failed commits have no Trino-side remedy."**

### Algorithm

```
CALL system.remove_orphan_files(schema_name, table_name, retention_threshold => '7d', dry_run => false)
```

1. Resolve `(schema, table)` at `currentSnapshotId`; compute `tableDataPath =
   pathResolver.resolveTableDataPath(schema, table)`.
2. Build the **known-file set** = every path the catalog references for this table, resolved to
   absolute form:
   - all `ducklake_data_file` rows for `table_id` (**any** snapshot, incl. end-snapshotted — those
     physical files are still referenced and must NOT be treated as orphans),
   - all `ducklake_delete_file` rows for `table_id` (any snapshot),
   - all `ducklake_files_scheduled_for_deletion` rows for the table (already owned by the
     two-phase pipeline — not orphans).
   Lance datasets are *directories*; their member files all live under the dataset dir, so a
   registered lance/vortex/duckdb/parquet path is matched by prefix as well as exact path.
3. `fileSystem.listFiles(tableDataPath)` recursively; for each `FileEntry`:
   - skip if its location is in (or under a directory in) the known set,
   - skip if `now - entry.lastModified() < retention_threshold` (the grace period),
   - otherwise it is a deletable orphan.
4. `dry_run = true`: log the orphans (count + paths) at INFO, delete nothing.
   `dry_run = false`: `fileSystem.deleteFiles(orphans)` and log the count.

### Safety rails

- **Minimum retention.** `retention_threshold` is parsed as an airlift `Duration`; a connector
  config `ducklake.remove-orphan-files.min-retention` (default `7d`) sets a floor. A call below the
  floor is rejected with `INVALID_PROCEDURE_ARGUMENT` (Iceberg's exact guard) so a `'0s'` can't
  nuke files an in-flight writer just produced.
- **Directory/prefix match**, not just exact path, so a lance/vortex dataset directory's internal
  files (manifests, `_versions/`, fragment files) are never mistaken for orphans.
- **Read-only on the catalog** — if the listing or a delete fails we surface the error; we never
  leave the catalog inconsistent because we never touched it.

### Why not `expire_snapshots` first?

`expire_snapshots` is the higher-value space reclaimer but it mutates the catalog (deletes
`ducklake_snapshot` + cascades, schedules dead files) and therefore needs new `WriteChange`
variants, `InterveningChanges` parse cases, and exhaustive `ConflictMatrix` /
`LogicalConflictCheck` branches (the sealed `when`s force it) plus a paired `cleanup_old_files` to
actually reclaim space. That is the next increment, built on the model above — but it is not the
*smallest safe* first step, and it is not the op that fixes the un-remediable-orphan hole.

## 4. Roadmap (built on the same two-phase model)

1. ✅ **`remove_orphan_files`** — DONE.
2. ✅ **`expire_snapshots`** — DONE. Deletes `ducklake_snapshot`/`_changes` rows for snapshots
   chosen by `retention_threshold` (floored by `ducklake.maintenance.min-retention`) or an explicit
   `snapshot_ids` array; never the latest. Cascade-deletes dead data/delete files (+ their column
   stats, variant stats, partition values) whose `[begin,end)` window no longer contains a surviving
   snapshot — dropped-table files caught via the dead-table id too — and **schedules** them (stored
   ABSOLUTE) for cleanup. Catalog-wide (DuckLake snapshots are catalog versions). **CORRECTION to
   the earlier assumption that this needs `WriteChange`/`ConflictMatrix` entries:** it does NOT —
   expiry is destructive GC, so it runs as a **plain catalog transaction with no new snapshot**
   (exactly like `ANALYZE`), bypassing the snapshot-mint + conflict machinery entirely. Now also
    GCs the **metadata rows of fully-expired dropped tables/views/macros/schemas**: `ducklake_table`
   + column/stats/partition/sort/mapping rows (reusing validated `deadTableIds`); `ducklake_view`;
   `ducklake_macro`/`_impl`/`_parameters`; `ducklake_schema` (single-PK survivor test + a guard that
   nothing still references the schema_id); and `ducklake_name_mapping` rows orphaned by the table GC.
   Still deferred (harmless, no file leak): dropping the dynamic `ducklake_inlined_data_*` tables.
3. ✅ **`cleanup_old_files`** — DONE. Drains `ducklake_files_scheduled_for_deletion` where
   `schedule_start < now - retention` (floored by `ducklake.maintenance.min-retention`); deletes the
   file then removes the row (a failed delete keeps its row for retry). Resolves connector-written
   ABSOLUTE paths directly and DuckLake-written ROOT-relative paths against the catalog `data_path`.
4. **`optimize` / `rewrite_data_files`** — compact small files. **Read side UNBLOCKED** (§ 6:
   the connector reads partial data + parquet-delete files correctly). ✅ **Non-partial v1 DONE —
   see § 7 for the full contract/decisions** (`DucklakeRewriteDataFilesProcedure` +
   `DucklakeCatalog.rewriteDataFiles`; catalog pins in `TestJdbcDucklakeCatalogRewriteDataFiles`,
   e2e in `TestDucklakeRewriteDataFiles`). Two writer shapes:
   (a) **non-partial / Iceberg-style** — end-snapshot the source files at the current snapshot and
   register one new file (begin = current snapshot, NO `partial_max`); time-travel to older
   snapshots still uses the (end-snapshotted) source files until `expire_snapshots` removes them.
   Simplest; no `partial_max` writing; sources reclaimed only on expiry. **THIS is v1.** Key
   decisions (§ 7): model the commit as `DeletedFromTable`+`InsertedIntoTable` (reuses ALL conflict
   machinery, ZERO spec-locked edits); reuse the real read path (split manager + page source) so the
   merge inherits delete/partial/schema-evolution correctness; row-count-preserving stats. (b)
   ✅ **partial-emitting DONE** (`reclaim_sources_immediately => true`) — writes the merged file with
   `begin = min source begin`, `partial_max = max source begin`, and a per-row
   `_ducklake_internal_snapshot_id` column, then DELETES the sources entirely + schedules them
   (mirrors DuckLake `WriteMergeAdjacent`). The merged file reproduces every historical snapshot on
   its own (read at S keeps rows whose internal id `<= S`), so sources reclaim immediately.
   `DucklakeCatalog.rewriteDataFilesPartial`; round-trip pinned by `TestDucklakeRewriteDataFiles`.
5. **stats-recalc** — already shipped as `ANALYZE`.

## 6. `partial_max` — a standing read-correctness gap (compaction-coupled)

`ducklake_data_file` and `ducklake_delete_file` both carry a `partial_max BIGINT` column that this
connector currently **ignores entirely** (`DucklakeDataFile` doesn't even project it). It is the
row-level snapshot bound for **cross-snapshot compacted files**: when DuckLake's
`merge_adjacent_files` merges rows that began at different snapshots into one physical file, the
file gets an internal `_ducklake_internal_snapshot_id` per row, and a correct read at snapshot `S`
must filter `_ducklake_internal_snapshot_id <= partial_max` (and the symmetric min bound) so
**time-travel doesn't over-include rows that were actually added in a later snapshot**.

Exact mechanics (verified against vendored DuckLake): a partial file physically carries a per-row
`_ducklake_internal_snapshot_id` BIGINT column; a correct read at snapshot `S` filters
`_ducklake_internal_snapshot_id <= S` (the *query* snapshot, NOT `partial_max`), applied **only
when `partial_max > S`** (`begin_snapshot = MIN`, `partial_max = MAX` of that column; the lower
bound is implicit via the catalog's `begin_snapshot <= S` visibility). Partial *delete* files work
the same way (a deletion takes effect at `S` iff its embedded snapshot id `<= S`).

**Status — ALL partial-file read filters SHIPPED (data + parquet-delete + puffin-delete). No gate remains.**
- **Partial data files: filtered (correct).** When a data file's `partial_max > scanSnapshot`,
  `DucklakeSplitManager` sets `DucklakeSplit.snapshotFilterMax = scanSnapshot`; the page source reads
  the file's `_ducklake_internal_snapshot_id` column and drops the file-local positions whose value
  exceeds it, folding them into the same `DeleteRowFilterTransform` position set as deletes (so it
  inherits the contiguous-position invariant — pushdown is disabled for these files). Time-travel of
  a DuckDB-compacted table now returns the CORRECT rows. Pinned cross-engine by
  `TestDucklakePartialFileFilter` (DuckDB `merge_adjacent_files` produces the partial file; Trino
  reads at first-insert → 100 rows, at partial_max / latest → 200).
- **Partial PARQUET delete files: filtered (correct).** A consolidated delete file
  (`file_path, pos, _ducklake_internal_snapshot_id`) with `partial_max > scanSnapshot` is read with
  a snapshot filter: only deletions whose `_ducklake_internal_snapshot_id <= S` are applied (the
  split carries `deleteFileSnapshotFilters`; the reader keeps matching `pos` values). Pinned
  cross-engine by `TestDucklakePartialDeleteFilter` (DuckDB consolidates two deletes via
  `flush_inlined_data`; Trino reads at the first deletion → only that row deleted, at partial_max →
  both).
- **Partial PUFFIN delete files: filtered (correct) — gate LIFTED 2026-06-29.** A consolidated
  deletion-vector file is a real Iceberg PFA1 puffin container (`Magic Blob1…BlobN Footer`) whose
  JSON footer tags each blob with a `ducklake-snapshot-id`. `DucklakePuffinDeleteReader` parses the
  container and, when `partial_max > S`, applies only the blobs whose snapshot id `<= S` (mirrors
  `DuckLakeDeleteFilter::ScanDeletionVectorFile`; a mix of tagged/untagged blobs is rejected as
  corrupt). Wired via the same `deleteFileSnapshotFilters` the parquet path uses. Pinned by
  `TestDucklakePuffinDeleteReader` (PFA1 parse + per-blob filter) and `TestDucklakePuffinPartialDelete`
  (full-Trino: a real consolidated puffin file, reads at different snapshots apply the right
  deletions). `hasPartialDeleteFilesRequiringSnapshotFilter` now only flags an unknown delete-file
  format (already rejected by `validateDeleteFileFormats`).

**Compaction unblocked (read side), fully.** The connector now reads partial data AND partial delete
files (parquet AND puffin) correctly, so an `optimize` / `rewrite_data_files` that emits
cross-snapshot-merged files — including the partial-emitting variant — is read-safe in every format.

## 5. Upstream parity / cross-engine notes

- A file we leave in `ducklake_files_scheduled_for_deletion` is drained equally well by DuckLake's
  own `ducklake_cleanup_old_files`, and vice-versa — the schedule table is the shared contract.
- `remove_orphan_files` ≙ upstream `ducklake_delete_orphaned_files` (filesystem set minus known
  set, mtime-gated). We scope to one table's data path rather than globbing the whole catalog data
  path; running it per table covers the same ground and fits Trino's procedure ergonomics.
- The grace period is the single safety knob shared across engines; keep the default conservative.

## 7. `optimize` / `rewrite_data_files` — non-partial v1 (the compaction WRITER) — ✅ DONE

This is the headline-value remaining F6 op. § 4 #4 settled the *shape* (start with the non-partial /
Iceberg-style writer); this section nails the v1 *contract*, the load-bearing decisions, and the
test plan, built on the two-phase model in § 1. **Status: implemented + green** —
`DucklakeRewriteDataFilesProcedure` (`CALL <catalog>.system.rewrite_data_files(schema_name,
table_name, file_size_threshold => '100MB')`) on top of the `DucklakeCatalog.rewriteDataFiles`
primitive; catalog-layer pins in `TestJdbcDucklakeCatalogRewriteDataFiles` (4) and full-Trino e2e in
`TestDucklakeRewriteDataFiles` (5).

### 7.1 What it does (the catalog state transition)

`optimize` reads the live rows of a set of small source data files, writes them into one (or a few,
size-bounded) larger Parquet file(s), and **atomically**:

1. registers the merged file(s) with `begin_snapshot = <new snapshot>` (NO `partial_max` — these are
   ordinary files; this is the non-partial shape),
2. **end-snapshots** the source data files at the same new snapshot (`end_snapshot = <new>`), and
3. end-snapshots any **active delete file** attached to a retired source (the merged file has those
   deletes already applied to its bytes, so the delete file is no longer needed going forward).

Reads at the new (or a later) snapshot see the single merged file; time-travel reads at older
snapshots still resolve the (now end-snapshotted) source files via the half-open `[begin,end)`
liveness test — exactly as today. The retired source + delete files are physically reclaimed later
by `expire_snapshots` → `cleanup_old_files` (the two-phase pipeline; § 1), never inside this commit.
This matches upstream DuckLake `merge_adjacent_files`'s catalog effect.

### 7.2 The load-bearing decisions

**(D1) Conflict model — reuse `DeletedFromTable` + `InsertedIntoTable`; touch NOTHING spec-locked.**
A compaction is, at the catalog level, exactly "retire these source files' rows + add a file holding
the same rows." So we record `WriteChange.DeletedFromTable(tableId, sourceDataFileIds)` +
`WriteChange.InsertedIntoTable(tableId, mergedColumnIds)`. This gives *correct, complete* conflict
protection with ZERO edits to the spec-locked `ConflictMatrix` / `WriteChange` / `InterveningChanges`
/ `LogicalConflictCheck`:
  - `LogicalConflictCheck.checkDeletedFromTable` already verifies every `sourceDataFileId` is **still
    active** at commit time → if any concurrent commit (DELETE/UPDATE/MERGE, another compaction, or
    DROP) end-snapshotted a source between our read and our commit, we abort **non-retryably** (the
    stale read can't be salvaged by retry). This is precisely the safety a compactor needs.
  - `ConflictMatrix.checkDeletedFromTable` / `checkInsertedIntoTable` additionally abort on a
    concurrent drop/alter of the table.
  - The `changes_made` text we emit is `deleted_from_table:<id>,inserted_into_table:<id>` rather than
    upstream's `merge_adjacent:<id>`. This is **strictly more conservative** for cross-engine
    concurrency (a concurrent reader treats it as a data change) and semantically honest (rows moved
    files). The *physical catalog state* is byte-identical to what `merge_adjacent` produces, so a
    later DuckDB/pg_ducklake read or compaction is unaffected. We deliberately do NOT introduce a
    `MergedAdjacentFiles` `WriteChange` variant in v1: it would force new entries into the
    spec-locked matrix `when`s (high blast radius, "keep in lock-step with upstream") for no
    correctness gain.

**(D2) Read mechanism — reuse the connector's REAL read path, do not reimplement.** The procedure
drives `DucklakeSplitManager.getSplits(...)` + `DucklakePageSourceProvider.createPageSource(...)`
for the target table at the current snapshot, then streams the resulting `Page`s into a
`ParquetFileWriter` (the same writer `flush_inlined_data` and INSERT use). Reusing the real read path
means the merge **inherits every read-side correctness property for free**: positional + parquet
delete-file application, `partial_max` snapshot filtering, schema evolution / name maps, nested
struct reshape. Because the *output* is written by the connector's own `ParquetFileWriter` (column
names == table column names, field_id annotations), it is guaranteed read-compatible. The merged
file's `record_count` is therefore the **live** row count (deletes already applied), which makes the
stats math (D3) trivially correct whether or not the sources carried deletes.

**(D3) Table-stats adjustment — compaction is row-count-preserving.** `ducklake_table_stats`:
  - `record_count`: **unchanged**. `applyInsertFragments` adds the merged file's `record_count`; we
    then subtract the same amount. Net zero — correct because the live row set is unchanged by
    compaction (the merged file holds exactly the live rows of the retired sources).
  - `file_size_bytes`: `+= merged size` (via `applyInsertFragments`) then `-= Σ(retired source
    file_size_bytes)`. Net = the real space delta. `GREATEST(0, …)` guards underflow.
  - `next_row_id`: left as advanced by `applyInsertFragments` (monotonic; retired files keep their
    old `row_id_start`, no reuse).
  - `ducklake_file_column_stats` of retired sources are **left in place** (still needed for
    time-travel reads of those files until `expire_snapshots` removes them). `applyInsertFragments`
    widens `ducklake_table_column_stats` with the merged file — a no-op/correct since merged ⊆
    source value range.

**(D4) The catalog primitives** are `DucklakeCatalog.rewriteDataFiles(...)` (non-partial) and
`rewriteDataFilesPartial(...)` (partial-emitting) — each one `executeWriteTransaction`. Non-partial
does (1)+(2)+(3)+(D3) and end-snapshots sources. Partial-emitting instead **back-dates** the merged
file to `begin = min(source begin)` with `partial_max = max(source begin)`, then **deletes the source
rows entirely** + schedules their paths for cleanup (immediate reclaim — mirrors DuckLake's
`WriteMergeAdjacent`); since sources are deleted (not end-snapshotted) it records only
`InsertedIntoTable` and validates the sources up front (active + no-newer-delete) instead of relying
on `DeletedFromTable`'s post-action active check. The procedure writes the per-row
`_ducklake_internal_snapshot_id` column (each row = its source file's begin_snapshot) as an
un-annotated trailing parquet field, so catalog stats (built over the table columns only) and the
read path (which finds the column by name) are both unaffected. Tested at the catalog layer (no
parity extension needed) + a full-Trino round trip.

### 7.3 Scope (final)

  - **Partitioned tables: supported.** Sources are grouped by partition (a file's `partition_id` +
    its stored partition values); each group with ≥ 2 files is compacted independently and the merged
    files inherit the group's partition values (copied from the catalog, not recomputed — so it works
    for ANY transform, not just identity). A lone file per partition is left alone; cross-partition
    files are never merged.
  - **Size-bounded output: supported.** A `target_file_size` arg (default 512MB) rolls the merged
    rows of a group into a new file once the target is reached (`GroupWriter`).
  - **Source candidate selection**: active **parquet** data files whose `file_size_bytes <
    file_size_threshold` (default 100MB). Mixed-format tables compact only their parquet files;
    lance/vortex/duckdb sources are skipped.
  - **`partial_max` source files**: the NON-partial path folds them in too (read at current = all
    rows live; sources end-snapshotted, still serve time-travel via their own filter). The
    partial-emitting path still requires non-partial sources (re-emitting from an already-partial
    file would need a per-row internal-id read — niche, skipped).
  - **Inlined rows** are left untouched (not file-resident). A partition with < 2 compactable files
    is a no-op.

### 7.4 Tests (every cell proves a real success/failure)

  - **Catalog-level** (`TestJdbcDucklakeCatalogRewriteDataFiles`, postgres-backed, no extension):
    register-merged + retire-sources atomicity; `record_count` unchanged; `file_size_bytes` adjusted;
    `next_row_id` monotonic; source delete files end-snapshotted; **concurrent-delete-on-source
    conflict** is non-retryable (both shapes); partial back-date + `partial_max` + entire-source
    deletion + scheduling.
  - **Full-Trino e2e** (`TestDucklakeRewriteDataFiles`): compaction + time-travel; delete-applying
    (tombstones physically dropped); **partitioned per-partition** compaction (pruning preserved);
    **`target_file_size` rollover** (tiny → many files, large → one); **partial round trip** (sources
    gone, time-travel reproduced from the merged file alone via the per-row snapshot filter);
    non-partial folding an already-partial source; single-file no-op.
