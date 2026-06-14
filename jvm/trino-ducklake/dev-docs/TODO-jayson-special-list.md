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
✅ MOSTLY DONE 2026-06-12 via T1.1 (spike + fixes + per-format e2e all green; plain reads
over deleted lance data proven and the search-gate rejection pinned in
`TestDucklakeRowLevelLanceFormat`). Remaining: the deliberate gate-revisit itself —
loosening lance search over deleted tables needs the search positions to respect tombstones.

### F5. Partitioned CTAS/INSERT for non-parquet formats

Plumbing exists (writers take partition values); verify end-to-end per format, fix what
breaks, pin with tests. Interplay to check: lance `add_files` rejects partitioned tables —
decide whether partitioned lance CTAS should work or be gated with a clear error.
✅ MOSTLY DONE 2026-06-12 via T1.2 (verified + fixed + pinned; partitioned lance CTAS WORKS —
dataset directories nest under `key=value/` partition dirs — so the add_files gate is about
registration, not the format). Remaining: decide whether partitioned-table lance/vortex
`add_files` should learn partition values or stay gated.

### F6. Maintenance operations

`optimize`, `rewrite_data_files`, `expire_snapshots`, `cleanup_old_files`,
`remove_orphan_files`, `flush_inlined_data`, stats recalc: all absent; users must run DuckDB
against the shared catalog. This is the biggest single hole — a multi-week program (M8 in
TODO-WRITE-MODE), and the one with real operational consequences (orphan files after failed
commits currently have *no* Trino-side remedy).

### F7. Write-side polish

Sorted writes (the catalog sort spec is read and exposed to the planner but not applied on
write), Puffin deletion-vector *writes* (reads done), commit-context session props (small).
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
  lance arrow-scan NULL-ROW morph; lance MAP needs format 2.2; lance FTS k is best-effort.
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
single hole. Change feed (F9) absent but tractable. DDL gaps (F1) small-to-medium. Write-side
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
