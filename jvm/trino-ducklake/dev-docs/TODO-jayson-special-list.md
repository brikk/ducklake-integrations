# TODO — Jayson's driving list (completion, edges filled, test verified)

**This is the driving list.** Ordered by Jayson (2026-06-12) from the completeness sweep of the
trackers + code greps; group labels are his, paragraph text is the sweep verbatim. Mandate:
*"I want completion, edges filled, test verified."* Suggested split: **one agent on the TEST
side, one agent on the FEATURE side**, hygiene items as low-risk fillers for either.

**Status 2026-06-12 (first pass, this branch):** T1 both spikes DONE — and they found real
bugs (see inline ✅ notes): merge-scan pushdown silently tombstoned the WRONG rows on .db
data; the delete filter's dual rowId/offset check phantom-deleted rows; declared
`data_file_format` was lost on empty CREATE TABLE + INSERT; NULL-partition splits leaked
rows through enforced identity predicates; and Trino's legacy `row_id` delete files were
UNREADABLE by DuckDB (now spec-shaped `(file_path, pos)`, cross-engine round-trip tested
both directions). F1 (RENAME TABLE same-schema, RENAME SCHEMA, COMMENT ON TABLE/COLUMN —
cross-engine verified) and F2 (vortex add_files) shipped. H1 archive sweep done. Open from
this list: T2, F3, F4 gate-revisit, F5 interplay item, F6–F11, H2–H3.

---
## ▶ NEXT-SESSION HANDOFF (last worked 2026-06-15, branch `claude/confident-haslett-01cafa`, all FF'd to `main` @ 24ac0e8)

**Repo is green and clean.** `:ducklake-catalog:test` + `:trino-ducklake:test` pass; BOTH
plain `:<module>:detekt` gates green (ReturnCount rule was disabled + baselines refreshed,
24ac0e8). Run detekt for BOTH modules before committing — the gate is the PLAIN `detekt`
task, NOT `detektMain` (type-res, false positives). See memory `project-detekt-gate-mechanics`.

**Done since first pass (each its own commit, all on main):** T2-A schema evolution on
non-parquet (was totally broken — any ALTER → unreadable; now resolves names at file
begin_snapshot); T2-B inlined-row DELETE gate; T2-C short-precision tstz CTAS crash; T2-D
lance MAP write gate; **TRUNCATE TABLE**; **flush_inlined_data** procedure (unblocks the T2-B
gate); **ANALYZE** (2026-06-15, recompute cached table-level stats). F3 (lance index lifecycle)
is PARKED by Jayson — see RESEARCH-lance-index-lifecycle.md; he'll define the application model
+ gauge DuckLake-team interest. Do NOT build F3.

**What's left, with scoping (pick one; all are medium / design-led now):**
- ✅ **ANALYZE** (DDL) — DONE 2026-06-15. Wired `getStatisticsCollectionMetadata` (declares only
  ROW_COUNT) / `beginStatisticsCollection` / `finishStatisticsCollection` →
  `catalog.analyzeTable` → `ducklake_table_stats` + `ducklake_table_column_stats`. The engine
  scans for an authoritative live row count; the per-column aggregates are rebuilt from the
  authoritative per-file stats (NOT decoded from scanned blocks — avoids re-encoding typed
  min/max), which also *tightens* min/max that incremental maintenance never narrows after a
  delete. Plain catalog transaction — no new snapshot, no invented `changes_made` vocab,
  `next_row_id` preserved (stats tables aren't snapshot-versioned). `TestJdbcDucklakeCatalogAnalyze`
  (drift-repair: corrupt → analyze → restored) + `TestDucklakeAnalyze` (e2e × 4: SHOW STATS,
  drift recompute through SQL, empty table, duckdb format). README row flipped to Yes.
- ✅ **Nested ADD/DROP FIELD** (DDL) — DONE 2026-06-15, BOTH steps (Jayson chose "Scope B,
  design-first"). Step 1: SPI `addField`/`dropField` → catalog `addField`/`dropField` (insert one
  child row via insertColumnTree / recursively end-snapshot the subtree; path resolved by
  parent_column walk); parquet self-heals. Step 2 (the non-parquet read path, removes the step-1
  gate): per-file `StructFieldPlan` built by `NestedFieldReshapePlanner` (current-vs-file column
  trees matched by column_id) → `DuckDbSelectSqlBuilder` emits a NULL-guarded `struct_pack`
  normalizing each file's struct to the current shape (added subfields CAST(NULL), drops omitted,
  renames/reorders by column_id, struct-in-struct recursion; NULL structs preserved). Plumbed via
  ExecutionRequest/DuckDbFilePageSource (cached column trees, skipped when no struct projected); the
  Arrow converter is unchanged (SQL normalizes to the current shape). Tests:
  `TestDuckDbSelectSqlBuilder` (+5 SQL), `TestNestedFieldReshapePlanner` (7),
  `TestDucklakeNestedFieldDdl` (parquet, 6), `AbstractDucklakeNestedFieldEvolutionFormatTest` ×
  {duckdb, vortex} (7 each: add NULL-fill, drop-non-trailing-no-misbind, nested-in-nested,
  **NULL-struct guard**, top-level compose, time-travel, delete-interplay). LANCE excluded (ROW
  writes gated upstream). Design: dev-docs/DESIGN-nested-field-evolution.md. `setFieldType`/
  `renameField` stay out. README row flipped to Yes.
- **SET TYPE** (DDL) — DEFER. Same read-path coupling as above but worse (the converter assumes
  column TYPES don't change across snapshots; see `project-schema-evolution-nonparquet`).
- **F6 maintenance** (biggest hole, design-led): optimize / rewrite_data_files /
  expire_snapshots / remove_orphan_files / stats-recalc.
  ✅ DESIGN + THREE procedures DONE 2026-06-29 — **dev-docs/DESIGN-maintenance.md** settles the
  snapshot-safety question: adopt DuckLake's **two-phase deletion** (catalog retirement only ever
  *schedules* files into `ducklake_files_scheduled_for_deletion`; physical unlink is a separate,
  **age-gated** step — the grace period protects in-flight/cross-engine readers; liveness =
  half-open `[begin_snapshot, end_snapshot)`). Shipped:
  • **`remove_orphan_files`** (`TestDucklakeRemoveOrphanFiles`, 5 e2e) — storage-only (orphans have
    no catalog row → no snapshot/WriteChange/ConflictMatrix); fixes the named hole ("orphans from
    failed commits have no Trino-side remedy"). Config `ducklake.remove-orphan-files.min-retention`.
  • **`expire_snapshots`** + **`cleanup_old_files`** (`TestDucklakeExpireSnapshots`, 7 e2e incl. the
    surviving-snapshot safety invariant + root-relative cleanup). Key finding: expire does NOT need
    `WriteChange`/`ConflictMatrix` after all — it's a **plain catalog transaction with no new
    snapshot** (like `ANALYZE`), since expiry is destructive GC. Catalog-wide; retention (floored by
    `ducklake.maintenance.min-retention`) or explicit `snapshot_ids`; never the latest; schedules
    dead files (absolute paths); cleanup drains them age-gated, resolving both absolute (ours) and
    root-relative (DuckLake's) scheduled paths. v1 leaves dead dropped-table/schema/view METADATA
    rows (harmless dangling; no file leak) — a tidy-up follow-up.
  • **partial_max read filter** — cross-snapshot compacted files (DuckDB `merge_adjacent_files`)
    carry per-row `_ducklake_internal_snapshot_id`; correct read at `S` keeps `<= S`, needed when
    `partial_max > S`. ✅ DATA files now FILTERED (2026-06-29): split carries `snapshotFilterMax`,
    page source reads the column + drops positions `> S` via the delete-filter set; time-travel of a
    DuckDB-compacted table is correct (`TestDucklakePartialFileFilter`, cross-engine via real
    `merge_adjacent_files`). Consolidated **parquet DELETE files** also filtered
    (`TestDucklakePartialDeleteFilter`, cross-engine via `flush_inlined_data`); **puffin DELETE files
    now filtered too** (PFA1 container + per-blob `ducklake-snapshot-id`; `TestDucklakePuffinDeleteReader`,
    `TestDucklakePuffinPartialDelete`). **Every partial-file read is now correct — no gate remains;
    compaction read side fully unblocked in all formats.**
  • **dead metadata GC** — expire deletes the metadata rows of fully-expired dropped
    tables/views/macros/schemas (`ducklake_table`+deps, `ducklake_view`, `ducklake_macro`/_impl/
    _parameters, `ducklake_schema`) + name-mapping rows orphaned by the table GC
    (`TestDucklakeExpireSnapshots` +2). Only the dynamic `ducklake_inlined_data_*` tables still
    deferred (harmless dangling, no file leak).
  • **optimize/rewrite_data_files — the compaction WRITER (non-partial v1)** ✅ DONE (uncommitted,
    pending review). `CALL system.rewrite_data_files(schema_name, table_name, file_size_threshold =>
    '100MB')` reads a table's small parquet files through the REAL read path (so delete files /
    partial_max / schema evolution all apply), writes one merged file, and atomically registers it +
    end-snapshots the sources via the new `DucklakeCatalog.rewriteDataFiles` primitive. Key design
    (DESIGN-maintenance.md § 7): modeled as `DeletedFromTable`+`InsertedIntoTable` so ALL conflict
    machinery applies with ZERO spec-locked edits; row-count-preserving stats; a `readSnapshotId`
    guard aborts non-retryably if a concurrent delete lands on a source after the read (the active-
    file check alone wouldn't catch that). Gates: unpartitioned only, parquet + non-partial sources,
    ≥2 candidates. Tests: `TestJdbcDucklakeCatalogRewriteDataFiles` (4, incl. concurrent-delete
    conflict), `TestDucklakeRewriteDataFiles` (5 e2e: compaction+time-travel, delete-applying,
    partitioned reject, single-file no-op).
  • **puffin partial-delete per-blob filter** ✅ DONE — the last partial-file READ gate is lifted.
    `DucklakePuffinDeleteReader` parses the real PFA1 container + per-blob `ducklake-snapshot-id` and
    applies only blobs `<= S` (`TestDucklakePuffinDeleteReader` +5, `TestDucklakePuffinPartialDelete`
    full-Trino). All partial-file reads (data + parquet-delete + puffin-delete) now correct.
  • **partial-emitting compaction variant** ✅ DONE — `rewrite_data_files(..,
    reclaim_sources_immediately => true)` writes the merged file with `_ducklake_internal_snapshot_id`
    per row (= source begin), back-dated begin = min(source begin), partial_max = max(source begin),
    and DELETES the sources entirely + schedules them (mirrors DuckLake WriteMergeAdjacent). Catalog
    primitive `rewriteDataFilesPartial`; `TestJdbcDucklakeCatalogRewriteDataFiles` +2,
    `TestDucklakeRewriteDataFiles` +1 (round trip: sources gone, time-travel AS OF s1/s2 reproduced
    from the merged file alone).
  **F6 DONE (done done).** All maintenance ops shipped: remove_orphan_files, expire_snapshots (+ full
  metadata GC, incl. dropping dynamic `ducklake_inlined_data_*` tables), cleanup_old_files, ANALYZE,
  the partial_max READ filters (all formats), and BOTH compaction WRITER shapes (non-partial +
  partial-emitting). Enhancements also DONE: **partitioned-table compaction** (per-partition groups,
  any transform), **size-bounded multi-file output** (`target_file_size`, default 512MB), and
  **re-compacting already-partial sources** (non-partial path). Only omission: the
  `ALTER TABLE … EXECUTE optimize` *alias* — deliberate non-goal (the connector exposes ALL F6 ops as
  `CALL system.*` procedures; the alias needs the separate TableProcedures SPI for zero capability
  gain). rewrite_data_files tests: `TestJdbcDucklakeCatalogRewriteDataFiles` (6),
  `TestDucklakeRewriteDataFiles` (8).
- **More T2** — ✅ the s3/MinIO cell is now FILLED on amd64 (2026-06-24): the whole
  MinIO+Quack container suite (`TestDucklakeQuackS3InitRace`, `TestDucklakeLanceS3QuackRead`,
  `TestDucklakeDuckDbExecutorBackends`) runs with 0 skips, and the genuine hole — **full-Trino
  parquet data files over s3** (previously ZERO coverage; the old s3 tests were all
   executor-level) — is covered by `TestDucklakeS3ParquetEndToEnd` (11 tests: CTAS / INSERT /
   DELETE / UPDATE / MERGE / schema-evolution / time-travel / partitioned, every data file
   verified as a physical MinIO object; a duckdb-format `.db`-upload-to-s3 write+read cell; and
   **concurrent writers over s3** — 4 barrier-aligned writers, lineage retry, all rows land once,
   the s3-transport variant of the conflict matrix). Only residual: full-Trino vortex/lance reads
   over s3 — a fragile mixed shape (in-process write + Quack-engine read across two containers)
   whose unproven delta is format-identical to the local FileScan path; executor-level
   vortex/lance-s3 reads are already proven. Low-yield leftovers: views × backends, cross-engine
   uint reads go through parquet.
- **H2/H3** (hygiene) — kotlinization candidate list; monolith decomposition PLAN (plan first).

**Env notes:** `:doris-ducklake` does NOT compile (pre-existing ~/.m2 Doris 1.2-SNAPSHOT drift,
unrelated — task chip filed; needs a P-series FE rebuild). lance + vortex DuckDB extensions ARE
available on this box, so those tests run (don't skip). The trino_parity extension binary is
symlinked into the worktree from the main checkout's build dir (tests need it) — on this
**linux-amd64** box `duckdb-trino-parity-extension/build` is a symlink to the main checkout's
build dir, which carries both `build/release/` (host) and `build/linux-amd64/release/`. Build
with JAVA_HOME=java25 (the daemon otherwise picks up 21 and `:doris-ducklake` config fails the
JVM-25 floor). Run the s3 suite with
`-Dducklake.test.parityExtensionPath=<main-checkout>/duckdb-trino-parity-extension/build/release/extension/trino_parity/trino_parity.duckdb_extension`.
**The 2026-06-24 amd64 move retired the "needs infra not on this arm64 box" T2 blocker** — the
full MinIO+Quack container topology runs here.

**Patterns/memory to read first:** MEMORY.md pointers, esp. `project-driving-list-pass2`,
`project-catalog-ddl-constraints` (PK on ducklake_schema, never invent changes_made vocab,
table-scoped ducklake_metadata settings), `project-ducklake-delete-vocabularies`,
`project-schema-evolution-nonparquet`, `project-detekt-gate-mechanics`.

Ground rules carried over from the same conversation:
- Parquet is NOT more important than the other formats — duckdb/vortex/lance coverage and
  capability matter equally.
- Don't break the monolith classes in arbitrary ways — extraction only for concerns that
  matter, careful plan first (SPI groups some things together; delegation is fine).
- Kotlinize wherever the SPI/Jackson isn't forcing Java shapes.
- Every feature lands with tests; every "wired-looking but unverified" claim gets a
  verification test before the README claims it.

---

## TEST SIDE (agent 1)

### T1. All-formats test parity — "they are ALL important"

Two cross-cutting unknowns, flagged because nothing in the trackers names them:

1. **Row-level DELETE/UPDATE/MERGE against tables whose *data* files are .db/vortex/lance.**
   The read path visibly handles splits-with-deletes (it drops pushdown to keep positions
   contiguous), but a grep finds **zero** e2e tests pairing DELETE with a non-parquet data
   format — every delete test is parquet-data. Status: wired-looking, unverified. That's a
   half-day verification spike, and until it runs the README must not claim it.
   ✅ DONE 2026-06-12 — `AbstractDucklakeRowLevelFormatTest` + per-format suites (duckdb/
   vortex/lance, 25 tests). "Wired-looking" was right to distrust: the merge scan kept
   predicate pushdown ON (the contiguity guard ignored the MERGE `$row_id`), so DELETE
   tombstoned the WRONG rows on .db data; and the delete filter's check-both-vocabularies
   set phantom-deleted rows whenever `rowIdStart < recordCount`. Both fixed
   (`DucklakePageSourceProvider`), plus the merge sink now writes DuckLake-spec
   `(file_path, pos)` delete files — DuckDB rejected the legacy `row_id` shape outright
   (`TestDucklakeCrossEngineTrinoDeleteRead` pins both directions). README claims updated.
2. **Partitioned CTAS/INSERT with non-parquet formats.** The writers all accept partition
   values, so plumbing exists — also zero tests.
   ✅ DONE 2026-06-12 — `TestDucklakePartitionedWriteFormats` (identity CTAS+INSERT × 3
   formats + temporal duckdb + NULL partitions). Found: explicit `WITH
   (data_file_format=...)` was lost on empty `CREATE TABLE` + INSERT (now persisted as a
   table-scoped `ducklake_metadata` setting; precedence pinned in
   `TestDucklakeFileFormatPrecedence`), and NULL-partition splits leaked their rows through
   enforced `col = 'x'` predicates (split pruning now drops them for null-excluding domains).

Beyond those two: build the capability × format grid (reads, writes, deletes, partitioning,
time travel, schema evolution, metadata tables, add_files, s3 × engine) and fill every cell
that parquet has and the others don't. Inlined-data interplay with non-parquet splits belongs
in the grid too.

### T2. Fill the left-behind gaps — "zero tests needs to be undone"

The HttpfsS3 (.db-over-s3) path got its FIRST live coverage only this week (the secret-race
test) — that blind spot hid a never-worked credential path for vortex-s3 reads. Hunt for the
remaining never-exercised branches the same way (coverage tooling or grep-the-dispatch-sites).
Known open boxes that belong here: views across all catalog backends (TODO-READ-MODE), the
DuckLake `.slt` corpus evaluation as a portable regression suite (TODO-READ-MODE), and the
concurrent-writer-under-Quack snapshot-lineage test (TODO-WRITE-MODE).

✅ T2-E DONE 2026-06-24 (amd64 box) — **the s3 × engine grid row, full-Trino.** The pre-existing
s3 tests were all *executor*-level (`QuackDuckDbExecutor` reading vortex/lance/.db over s3); the
real hole was **Trino's own ParquetWriter/ParquetPageSource against an s3-resident DuckLake
catalog** — zero coverage. `TestDucklakeS3ParquetEndToEnd` (self-contained: MinIO container + a
**PostgreSQL** catalog ATTACHed with an `s3://` DATA_PATH so the catalog-stored path routes every
new file to the bucket — PG, not a single-file local DuckDB catalog, so the concurrency cell is
real; native S3 filesystem `fs.native-s3.enabled`, no Quack/parity needed for parquet) — 11 tests:
- CTAS / INSERT / DELETE / UPDATE / MERGE / schema-evolution (ADD/RENAME/DROP) / time-travel /
  partitioned writes, each asserting the data file is a **physical MinIO object** (`mc ls`, since
  `$files.path` is the relative name);
- a **duckdb-format** cell (the writer uploads the `.db` to s3 via TrinoFileSystem; read
  materializes back through the parity executor with pushdown);
- **concurrent writers over s3** (4 writers × 3 rows, barrier-aligned): every INSERT is its own
  snapshot commit, and the connector's lineage retry serializes them so all rows land exactly once
  — the s3-transport variant of the (otherwise in-JVM, local-data) catalog conflict matrix. Stable
  across 3 reruns.
Also confirmed the whole MinIO+Quack container suite runs 0-skip on amd64 (the "needs infra not on
this arm64 box" blocker is retired). Doc-drift fixed while here: the Quack `createSchema` smoke is
a live passing test (TODO-WRITE-MODE wrongly called it `@Disabled`), and the type-audit `list<blob>`
comment claiming `@Disabled` was stale (the test is live + passing).
**Only residual in this row:** full-Trino vortex/lance reads over s3 (the *write* path never
consults `execution-engine`, so it would be a fragile mixed shape — in-process vortex/lance write
+ Quack-engine s3 read across a two-container network — and the **executor**-level vortex/lance-s3
read is already proven by `TestDucklakeLanceS3QuackRead` + `TestDucklakeQuackS3InitRace`, so the
only unproven delta is connector plumbing that is format-identical to the local FileScan path).

✅ T2-C DONE 2026-06-14 — type-edge sweep on the thin formats (special floats, empty
strings, all-NULL, empty arrays, bucket partitioning, boundary bigints, decimals,
pushdown, multi-file pruning — all solid). One real bug: **short-precision `TIMESTAMP WITH
TIME ZONE` CTAS crashed** (`LongArrayBlock cannot be cast to Fixed12Block` in the stats
accumulator on vortex/lance; "Failed to close writer" on duckdb). Root cause: DuckLake tstz is
micros-only, so the catalog round-trip always reports precision 6 — but a CTAS streams the
source's actual precision (e.g. default precision-3 short blocks), and beginCreateTable handed
the writer the precision-6 (long) column type. Fixed by giving the CTAS write handle the
ORIGINAL declared tstz type so the writer's isShort branch matches the blocks (value still
widens to micros on read). NB plain CREATE TABLE + INSERT already worked (engine coerces to
micros) — only CTAS broke, so the fix is in beginCreateTable, not a type-level gate (a gate
broke the Tier-C declaration tests). `TestDucklakeTimestampTzPrecision` (CTAS prec 0/3/6 × 3
formats) + unit pin.

✅ T2-B DONE 2026-06-14 — the grid's named inlined-interplay cell: a table with BOTH inlined
rows (DuckDB-written) AND a non-parquet data file (Trino-written). READ composes fine
(inlined split + lance data-file split union correctly — `TestDucklakeInlinedNonParquetInterplay`).
But DELETE/UPDATE/MERGE failed with an opaque "Column not found: $row_id" — the inlined page
source filtered `isVirtual()` columns but not the MERGE `$row_id` (which is `isRowIdColumn()`,
not a VirtualKind), and this connector's merge sink can't tombstone an inlined row anyway (no
data_file_id/position). Now GATED in `beginMerge` with a clear "flush inlined data first"
error. (Pre-existing on ALL tables with inlined rows, not just the non-parquet mix.)

✅ T2-A DONE 2026-06-14 — the branch-hunt's first big find: **schema evolution was totally
broken on non-parquet data**. CTAS into duckdb/vortex/lance + any `ALTER TABLE ADD/RENAME/DROP
COLUMN` made the table UNREADABLE ("column not found") — the DuckDB-engine read projected
current names against a file holding write-time names. Fixed: the provider resolves each
column's name as of the file's begin_snapshot (`catalog.getTableColumns`, memoized) and
`DuckDbSelectSqlBuilder` aliases renames / projects `CAST(NULL AS type)` for columns added
later — matching parquet's behavior. 21 e2e tests (`TestDucklakeSchemaEvolution*Format`) + 4
SQL-builder unit tests. Remaining T2 candidates (ranked, from the dispatch-site audit):
vortex/lance × httpfs/auto read modes (needs s3/minio fixture); metadata-tables/$files +
time-travel on non-parquet (cheap); inlined-rows + DELETE spanning non-parquet splits (the
grid item); add_files name-mapper error paths; views across DUCKDB_LOCAL/QUACK backends;
concurrent-writer snapshot-lineage. Doris note unchanged (module pre-existingly broken).

---

## FEATURE SIDE (agent 2) — in priority order

### F1. DDL gaps

`RENAME TABLE`/`RENAME SCHEMA`/`COMMENT ON TABLE/COLUMN` are small catalog ops (days, not
weeks); `SET TYPE` and nested `ADD/DROP FIELD` are medium. `ANALYZE` medium.
✅ TRUNCATE TABLE shipped 2026-06-15 (`TestDucklakeTruncate` + cross-engine inlined-clear in
`TestDucklakeInlinedNonParquetInterplay`) — catalog bulk-clear (end-snapshot data/delete files
+ inlined rows, keep schema, no schema-version bump, recorded `deleted_from_table`). Clears
inlined rows too, so it works where the T2-B DELETE gate rejects. Still open from F1: SET TYPE
(coupled to the T2-A schema-evolution read path — defer), nested ADD/DROP FIELD.
✅ ANALYZE shipped 2026-06-15 (`TestJdbcDucklakeCatalogAnalyze` + `TestDucklakeAnalyze`) — the
statistics-collection SPI (`getStatisticsCollectionMetadata` declares ROW_COUNT only /
`beginStatisticsCollection` / `finishStatisticsCollection`) → `catalog.analyzeTable`, which
recomputes `ducklake_table_stats` (record_count from the live scan, file_size from active files,
next_row_id preserved) and rebuilds `ducklake_table_column_stats` from the active files'
authoritative per-file stats — tightening min/max that incremental maintenance never narrows
after a delete. Non-snapshot-versioned side-table refresh: plain catalog transaction, no new
snapshot, no invented change vocab. Per-file aggregation (not scanned-block decoding) keeps the
canonical stat-string encoding. Still open from F1: SET TYPE (defer). Nested ADD/DROP FIELD DONE
2026-06-15 (both steps — parquet + non-parquet struct_pack reshaping; per-format e2e on duckdb +
vortex; lance excluded as ROW writes are gated upstream).
✅ PARTIAL 2026-06-12 — the small four shipped (`TestDucklakeDdl` +
`TestDucklakeDdlCrossEngine`): RENAME TABLE (same-schema; cross-schema rejected — table data
paths are schema-relative), RENAME SCHEMA (new schema_id + re-pointed tables/views/macros;
`ducklake_schema` has a PK on schema_id so same-id versioning is impossible; recorded as
dropped+created — upstream's parser has no schema-rename change type), COMMENT ON
TABLE/COLUMN (ducklake_tag/ducklake_column_tag `comment` keys — DuckDB sees them, and
comments survive renames). Still open: SET TYPE, nested ADD/DROP FIELD, ANALYZE.

### F2. add_files for anything

`add_files` accepts parquet + lance only. Vortex is missing — the cheapest real win on this
list, probably a day (single file, count via `read_vortex`, same shape as lance's
registration). Consider `.db` registration too (niche but symmetric). Existing related boxes:
hive_partitioning beyond IDENTITY transforms, and upstream's `allow_missing` recursing into
STRUCT fields (TODO-WRITE-MODE).
✅ vortex DONE 2026-06-12 (`file_format => 'vortex'`, `TestDucklakeVortexAddFiles`; same
opaque shape as lance + real file size; partitioned/hive gates shared). `.db` registration
still open (niche).

### F3. Index lifecycle — lance first, designed for every non-parquet format

**The point of these formats is that we CAN have indexes, which parquet cannot.** Lance now:
search is brute-force unless a dataset arrives pre-indexed via `add_files`; at 200k×384
that's ~30ms, at tens of millions it won't be. The extension exposes `__lance_optimize_index`
etc., so procedures wrapping them is the natural medium-sized step. Then the bigger design
Jayson wants: **index DEFINITION on the table** — "this table always wants that for new
data", with auto-index (or index-on-write/maintenance) semantics — planned generically so the
duckdb format (ART indexes in the .db files) and future formats plug into the same surface,
not a lance one-off. Catalog representation, DDL/procedure surface, and when indexing runs
(synchronous on write vs maintenance op) are the design questions.

⚠️ SCOPED 2026-06-14 — **DECISION NEEDED, see dev-docs/RESEARCH-lance-index-lifecycle.md.**
Live-probed the installed lance extension: **there is NO index-creation function** — indexes
must be built externally (Python lance) and arrive via `add_files`. `__lance_optimize_index`
optimizes a *pre-existing* index only; the genuinely-callable ops are `__lance_compact_files`
+ `__lance_cleanup_old_versions` (dataset maintenance — really F6, and they MUTATE the dataset
in place, raising a snapshot-safety question). All are table functions → run through the
existing executor, no non-SELECT path needed. So a Trino-only "create + maintain an index"
story is NOT deliverable today; the .db ART-index angle doesn't fit DuckLake's many-small-files
model either. Did NOT build speculative procedures (creation blocked + maintenance safety
unresolved). The doc frames the part-2 `ducklake_index` spec-change design + 4 direction
options for Jayson. Re-probe on lance extension bumps (TestLanceExtensionCanary is the
trip-wire).

### F4. Row-level CRUD over non-parquet data files

Verification spike from T1 first; then fix whatever it finds (write-side may need to accept or
reject cleanly; read-side delete filtering over .db/vortex/lance positions must be proven),
then e2e tests per format. Lance search functions currently reject tables with row-level
deletes (v1 gate) — revisit that gate once plain reads are proven.
✅ DONE — gate-revisit concluded 2026-06-29: **KEEP the gate** (deliberate, not a TODO). The lance
search functions return ranked top-k rows (table columns + `_distance`/`_score`) with NO row-position
column, and the search page source bypasses `applyDeleteFile`; DuckLake tombstones key on file-local
positions the DuckDB `lance_*` functions don't surface, so deleted rows can't be filtered out.
Allowing search over a table with active deletes would silently return deleted rows. Lifting needs
the lance extension to expose a delete-aligned row id. The check is snapshot-precise (a search at a
pre-delete snapshot is not gated). Error message + `AbstractLanceSearchTableFunction` class doc now
explain the technical reason + workarounds; `TestDucklakeRowLevelLanceFormat` still pins it.

### F5. Partitioned CTAS/INSERT for non-parquet formats

Plumbing exists (writers take partition values); verify end-to-end per format, fix what
breaks, pin with tests. Interplay to check: lance `add_files` rejects partitioned tables —
decide whether partitioned lance CTAS should work or be gated with a clear error.
✅ DONE 2026-06-29 — decision: partitioned lance/vortex `add_files` LEARNS partition values from the
hive-style `key=value/` path via `hive_partitioning => true` (identity transforms only; reuses the
parquet path's `parseHivePartitions`). Contract: because opaque scan-registered files are
column-projected by the DuckDB engine (no constant-fill like the parquet path), the partition column
must be PRESENT in the file — the path value is recorded so the registered file is PRUNABLE without a
scan. A partitioned lance/vortex `add_files` WITHOUT `hive_partitioning` is rejected with a clear
"requires hive_partitioning => true" error. Tests: `TestDucklakeVortexAddFiles` +1 (partitioned
register + prune), `TestDucklakeLanceAddFiles` +1; gate test updated.

### F6. Maintenance operations

`optimize`, `rewrite_data_files`, `expire_snapshots`, `cleanup_old_files`,
`remove_orphan_files`, `flush_inlined_data`, stats recalc: all absent; users must run DuckDB
against the shared catalog. This is the biggest single hole — a multi-week program (M8 in
TODO-WRITE-MODE), and the one with real operational consequences (orphan files after failed
commits currently have *no* Trino-side remedy).
✅ flush_inlined_data DONE 2026-06-15 — `CALL system.flush_inlined_data(schema_name,
table_name)`: connector-native (reads inlined rows via the catalog, materializes them into a
Parquet file through `ParquetFileWriter` driven by an `InMemoryRecordSet`→Page reuse, then
`catalog.flushInlinedData` registers the file + end-snapshots the inlined rows atomically;
new `WriteChange.FlushedInlinedData` + ConflictMatrix/LogicalConflictCheck entries — conflicts
on any intervening inlined-data/schema change so the read-then-write can't duplicate/drop).
Unblocks the T2-B DELETE gate. v1 gates partitioned tables; handles schema evolution across
inlined versions (readInlinedData NULL-fills). `TestDucklakeFlushInlinedData` (7, cross-engine).
Still absent (the real F6 program): optimize/rewrite_data_files/expire_snapshots/
remove_orphan_files/stats-recalc — design-led, with the in-place-mutation/snapshot-safety
question from RESEARCH-lance-index-lifecycle.md.

### F7. Write-side polish

- ✅ **Puffin deletion-vector WRITES** — DONE 2026-06-29 (commit `722f95b`). New `write_deletion_vectors`
  session property (default off); `DucklakeMergeSink` writes tombstones as DuckLake `.puffin`
  deletion-vector files (`DucklakePuffinDeleteWriter`, byte-exact encode counterpart to the reader),
  prior-position union now reads puffin AND parquet; `DucklakeDeleteFragment` gained a `format` field,
  catalog persists `ducklake_delete_file.format`. Cross-engine: Trino writes puffin deletes, BOTH
  Trino and DuckDB read the survivors (`TestDucklakeTrinoPuffinDeleteWrite`); reader round-trip unit
  (`TestDucklakePuffinDeleteReader.writerEncodeRoundTripsThroughReader`).
- ⏸ **Sorted writes — PARKED, awaiting Jayson's scope decision (2026-06-29).** Today the catalog sort
  spec is read + exposed to the planner only (`DucklakeSortPropertyMapper` → `SortingProperty`);
  nothing sorts on write. Applying it is a **central write-path change** (touches
  `DucklakeWritableTableHandle` (Jackson record + all construction sites), `beginInsert`/
  `beginCreateTable`, `DucklakePageSinkProvider`, and the core `DucklakePageSink` shared by ALL
  INSERT/CTAS across parquet/duckdb/lance/vortex + partitioning + rollover). Scoping facts gathered:
  - **Memory model fork:** `io.trino.spi.PageSorter` (engine-provided, in-memory, bound by
    `ConnectorContextModule`, injectable like `PageIndexerFactory`) vs `io.trino.plugin.hive.Sorting-
    FileWriter` (spill-capable, in trino-hive — but needs a Hive-`FileWriter` adapter + temp-file/
    `TypeOperators` infra; the fragment-producing lifecycle is an impedance mismatch).
  - **Scope fork:** unpartitioned-only first (memory bounded to one buffer ≈ file target; simplest)
    vs partitioned+sorted (memory = partitions × buffer — real OOM risk).
  - **`sorted_by` table property:** NONE exists — Trino can't *create* a sorted table, so sorted
    writes only benefit DuckDB-created sorted tables receiving Trino INSERTs unless we also add it.
  - **Recommended approach (for when unparked):** isolated + GATED — in-memory `PageSorter`, parquet
    + unpartitioned + only when a sort spec exists (every existing write keeps the unchanged path;
    zero risk to the 99% case), per-file sorted output. Buffer pages per file up to the rollover
    target, sort via PageSorter, write; roll by buffered retained-bytes. `sorted_by` property +
    spill (SortingFileWriter) + partitioned-sorted are follow-ups.
- ❌ **Commit-context session props — recommended NON-GOAL (2026-06-29).** Would need new
  `author`/`commit_message` columns on `ducklake_snapshot`, which are NOT in the DuckLake spec →
  cross-engine divergence risk (DuckDB owns that table's schema/migrations). `insertSnapshotRow`
  writes only the 5 spec columns; `operationDescription` is conflict-message-only, never persisted.
  Don't add unless DuckLake upstream defines the columns.

The quack-as-catalog backend group is the active migration thread — its remaining boxes are
mostly verification/CI, not construction. Related standing direction: **retire the in-process
DuckDB engine entirely once quack safety allows** — quack-only is the destination.

### F8. Degraded types (minus variant)

json/interval/geometry/uint128: data round-trips; full typed support is engine-level work and
mostly a permanent trade. Geometry spatial functions / bounding-box pruning and json functions
are the plausible upgrades; uint128 is likely permanent VARCHAR.

### F9. Change feed

`table_changes`/`insertions`/`deletions`: absent. Medium-large — the snapshot machinery and
the table-function pattern (split-based, from the lance searches) both exist, so it's
tractable.
✅ DONE (this branch, `trino-focus`, uncommitted→push). All three functions shipped under
`system.*` as scan-rewrite PTFs (like the lance searches — `ChangeFeedFunctionHandle` →
`applyTableFunction` → `ChangeFeedTableHandle` → single `ChangeFeedSplit` → `ChangeFeedPageSource`).
Catalog side: `getDataFilesAddedBetween` (insert side: `begin_snapshot ∈ [start,end]`) +
`getDeletionsBetween` (delete side: current-minus-previous cumulative delete-file diff + full-file
retire arm, computed in memory → Quack-safe). The page source reuses the ORDINARY data-file read
pipeline per unit (`createDataFilePageSource`, extracted) requesting the table columns + `$row_id`
read AS OF the END-snapshot schema — so schema evolution + every format (parquet/duckdb/vortex/lance)
work for free; delete units keep only the newly-deleted positions and stamp the delete snapshot;
`change_type` classifies pairing. **Bounds** inclusive both ends, snapshot-id OR timestamp
(`getSnapshotAtOrBefore`), start required / end defaults to current. **Two honest edges** (both from
the `row_id_start + position` rowid vocabulary Jayson chose): (1) UPDATEs surface as `delete`+`insert`
— neither Trino nor DuckDB's rewritten row keeps a `rowIdStart+position`-equal rowid (verified live:
DuckDB update allocated a fresh rowid too), so the pre/post-image pairing (implemented + correct) only
fires when a deleted rowid is re-inserted with the same value in a snapshot; (2) inlined data/deletes
are **gated** at analyze time (clear "flush_inlined_data first" error) rather than silently omitted.
Tests: `TestDucklakeChangeFeed` (11 e2e: insertions/deletions/changes, inclusive+scoped bounds,
end-default, timestamp bounds, non-parquet duckdb, projection/COUNT(*), empty window, schema-evolution
as-of-end), `TestDucklakeChangeFeedCrossEngine` (3: DuckDB-written update read as delete+insert,
deletions side, inlined-data gate), `TestChangeFeedPageSource` (5 unit: insert/delete/**update
pre-post pairing**/projection/empty-filter). Both `:module:detekt` gates green. README "Change Feed"
section flipped from roadmap to shipped.

### F10. Variant

Shredded field access (`payload.user`), shredded-subfield statistics for pushdown. The only
big-ticket degraded-type item; engine-level type features.

### F11. Parking lot — "whatever I forgot"

- Pushdown Step 5: DuckDB-exclusive functions via `ConnectorFunctionProvider` (never started,
  optional); arithmetic-operator and `concat`/`position` translation (small, deliberately
  deferred).
- SQLite catalog backend (planned tier).
- Vortex: type audit beyond scalars/ARRAY/ROW; verify the extension actually *exploits* pushed
  predicates (the WHERE renders; whether vortex skips decompression is unmeasured).
- Upstream watches (canary-driven retests on extension bumps): vortex MAP COPY native crash;
  lance arrow-scan NULL-ROW morph; lance MAP needs format 2.2 (now GATED at schema time —
  2026-06-14 — it failed opaquely as "Failed to close writer", not the clean upfront error an
  old comment assumed; `TestDucklakeLanceFormat.mapColumnIsRejectedForLanceWrites`); lance FTS
  k is best-effort.
- `add_files` lance `record_count` full-scan cost on huge datasets.
- Cross-dialect view transpilation (research item).
- DuckDB-equivalent virtual columns (`rowid`, `snapshot_id`) — TODO-READ-MODE box.

---

## HYGIENE (either agent, low-risk fillers)

### H1. dev-docs archive sweep

Move the closed lab-notebook docs (HANDOFF-lance-route-a is explicitly final; finished
TODO/RESEARCH logs) into `dev-docs/archive/`; keep live trackers + this list at top level.
Agents already know to ignore archive. Tick the stale boxes while there: TODO-vortex
"SQL-level read through the catalog" (satisfied by the CTAS work), TODO-lance Route-B boxes
(moot after the A-vs-B decision — REPORT-lance-route-a-vs-b.md is the record).
✅ DONE 2026-06-12 — 10 docs moved (HANDOFF, RESEARCH-lance-and-pushdown, function-mapping ×2,
substrait, COMPARE ×2, REPORT ×3 incl. the A-vs-B record); all path-style references
rewritten to `dev-docs/archive/`; TODO-vortex box ticked (+ add_files note); TODO-lance
Route-B section banner-marked MOOT.

### H2. Kotlinization candidate list

Build the explicit list of remaining Java-accent shapes where SPI/Jackson is NOT forcing them,
then convert: the accessor-method classes (`ExecutionRequest` → data class with properties +
default args is the poster child), remaining `@get:JvmName`/`@JvmRecord` on never-serialized
internal types, telescoping constructors → default args, manual equals/hashCode/toString →
data classes. Excluded by rule: Trino SPI signatures, the `@JvmRecord` Jackson wire DTOs
(load-bearing for Trino's module-less mapper — see project memory), airlift `DucklakeConfig`.

### H3. Decomposition plan for the monoliths

`DucklakeMetadata` and `DucklakePageSourceProvider` are 1,000+ line multi-concern classes, and
the arrow-stream writer mixes lifecycle, schema mapping, stats, and upload. Plan first, not
arbitrary splits: extract the concerns that matter (per-format page-source construction,
metadata-table handling, stats extraction, upload/cleanup) behind the existing SPI entry
points via delegation. First-party connectors have the same disease, so match the genre where
the SPI forces it and delegate where it doesn't.

### H4. LikePattern — the reflection is forced; keep it caged

Investigated 2026-06-12: the engine delivers LIKE as `Call($like, [value, Constant(v)])` where
`v` is an engine-constructed `io.trino.type.LikePattern` **instance** (trino-main). The plugin
classloader exposes only `io.trino.spi.*`, so a compile-time import passes tests (no isolation
there) and throws `NoClassDefFoundError` in a real plugin-dir deployment; porting a copy
in-house can't read the engine's instance (different `Class`). So the reflection stays, caged:
it lives in one guarded accessor (`LikePatternAccessor` — class-name pinned, method cache,
fails soft to "don't push"), with 8 unit canaries (`TestDuckDbExpressionTranslator.testLike*`)
pinning the surface against real engine objects. Revisit on Trino version bumps — if the SPI
ever carries pattern/escape as plain values, delete the accessor. (Long-term: an upstream SPI
ask is the real fix.)

---

## Per-area completeness snapshot (sweep verbatim, 2026-06-12)

**DuckLake core — the real gaps live here.** Maintenance operations (F6) are the biggest
single hole. Change feed (F9) shipped (all three functions; see § F9). DDL gaps (F1) small-to-medium. Write-side
polish (F7). Degraded types (F8/F10) working-as-degraded. Views: only Trino-dialect exposed.
SQLite + Quack catalog backends planned/in-progress.

**duckdb `.db` format — the most complete.** Read modes, both writers, full complex types, the
95-function pushdown catalog, TZ semantics — all shipped and tested. Genuinely open: pushdown
Step 5 (optional), arithmetic-operator and `concat`/`position` translation (small, deferred).

**Lance — functionally complete for its v1 scope, with upstream-bound edges.** Real holes: no
index lifecycle through Trino (F3). Write gates (ROW until the upstream null-struct fix, MAP
until lance format 2.2, scalar-only list elements) are upstream-bound — the canary
(`TestLanceExtensionCanary`) says when to retest. Search v1 gates (s3 quack-only, all-lance
tables, no row-level deletes) are documented choices.

**Vortex — the thinnest, but its holes are small.** `add_files` missing (F2 — cheapest win).
Type audit + pushdown-exploitation verification (F11). MAP write gated on the upstream native
crash. s3 streaming reads are env-channel only (documented).

**Cross-cutting:** the two zero-test unknowns in T1 (row-level CRUD over non-parquet;
partitioned non-parquet writes) gate several feature claims and go first.
