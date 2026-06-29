# DESIGN: Maintenance operations (F6)

**Status:** design note + first procedure (`remove_orphan_files`) — the rest is roadmap.
**Scope:** trino-ducklake connector. **Companion to:** TODO-WRITE-MODE.md § M8, TODO-jayson-special-list.md § F6.

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

1. **`remove_orphan_files`** — this PR.
2. **`expire_snapshots`** — delete `ducklake_snapshot`/`_changes` rows for snapshots chosen by
   `retention_threshold` or explicit `versions`; never the latest. Cascade dead tables/views/macros
   and **schedule** (don't delete) data/delete files whose `[begin,end)` window no longer contains a
   surviving snapshot. New `WriteChange.ExpiredSnapshots` + matching `InterveningChanges` /
   `ConflictMatrix` / `LogicalConflictCheck` entries (conflict against any intervening commit that
   touched the files being retired).
3. **`cleanup_old_files`** — drain `ducklake_files_scheduled_for_deletion` where
   `schedule_start < now - grace`; physical delete + remove the schedule rows.
4. **`optimize` / `rewrite_data_files`** — compact small files; the rewrite registers new files and
   *schedules* the old ones (same two-phase reclaim). This is where the cross-engine concurrency
   matrix matters most; design when scheduled. **HARD PREREQUISITE: `partial_max` (see § 6)** —
   merging rows across snapshots into one file requires populating `partial_max` on write so
   time-travel reads stay correct; do not ship cross-snapshot compaction until the read path honors
   it.
5. **stats-recalc** — already shipped as `ANALYZE`.

## 6. `partial_max` — a standing read-correctness gap (compaction-coupled)

`ducklake_data_file` and `ducklake_delete_file` both carry a `partial_max BIGINT` column that this
connector currently **ignores entirely** (`DucklakeDataFile` doesn't even project it). It is the
row-level snapshot bound for **cross-snapshot compacted files**: when DuckLake's
`merge_adjacent_files` merges rows that began at different snapshots into one physical file, the
file gets an internal `_ducklake_internal_snapshot_id` per row, and a correct read at snapshot `S`
must filter `_ducklake_internal_snapshot_id <= partial_max` (and the symmetric min bound) so
**time-travel doesn't over-include rows that were actually added in a later snapshot**.

Two consequences:

- **Already-live gap (narrow).** A table compacted by *DuckDB* `merge_adjacent_files` on a shared
  catalog can today be **time-travel-read incorrectly** by this connector (over-inclusion of
  later-snapshot rows). Current-snapshot reads are unaffected (every row is live at the latest
  snapshot). Scope: compacted-cross-snapshot tables × time-travel only. Tracked in TODO-READ-MODE.
- **Blocks our own compaction.** We must NOT emit cross-snapshot-merged files from `optimize` /
  `rewrite_data_files` until the read path projects and applies `partial_max` — otherwise we'd
  manufacture the same corruption. (A compaction variant that only merges files *within a single
  snapshot* sidesteps `partial_max` and could ship first.)

This note exists so the read fix and the compaction write are designed together; `remove_orphan_files`
(§ 3) is independent of it.

## 5. Upstream parity / cross-engine notes

- A file we leave in `ducklake_files_scheduled_for_deletion` is drained equally well by DuckLake's
  own `ducklake_cleanup_old_files`, and vice-versa — the schedule table is the shared contract.
- `remove_orphan_files` ≙ upstream `ducklake_delete_orphaned_files` (filesystem set minus known
  set, mtime-gated). We scope to one table's data path rather than globbing the whole catalog data
  path; running it per table covers the same ground and fits Trino's procedure ergonomics.
- The grace period is the single safety knob shared across engines; keep the default conservative.
