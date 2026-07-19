# TODO-uhoh — parking lot for concerns without a home yet

Things we cannot place in a feature TODO yet but need to keep worrying about.
Reviewed on every upstream refresh run (see `RESEARCH-upstreams.md` § Per-run
procedure — add a step-7 glance at this file). Items graduate to a real TODO
when they become actionable, or get closed with a dated verdict when the
concern dies.

Seeded 2026-07-05 from the upstream-risk conversation.

## Protections (tests we owe ourselves)

- [x] **cross-engine-cleanup-survival test** — DONE (2026-07,
  `TestDucklakeCrossEngineCleanupSurvival`). Plants ORPHAN files of each format
  (`.parquet`, `.db`, `.vortex`, and a `.lance` dataset dir with member files)
  inside the table data dir, runs stock DuckDB
  `ducklake_delete_orphaned_files('ducklake_db', cleanup_all => true)`, and
  asserts our non-parquet orphans **survive** while the parquet orphan is removed
  (a live control proving the sweep ran — so survival is real protection, not a
  no-op). PINS the `*.parquet`-only filter assumption; the test fails loudly if a
  future DuckLake bump starts deleting our special-format files. (Only orphan
  sweep is exercised — tracked live files are never orphans, and dead-file
  reclaim via `expire_snapshots`/`cleanup_old_files` deleting a genuinely dead
  non-parquet file is correct, not a survival concern.)
  - **UPDATE 2026-07-18 (ducklake main `1e2e74ee`):** the orphan-sweep glob filter
    widened from `suffix(filename,'.parquet')` to
    `(suffix(...,'.parquet') OR suffix(...,'.puffin'))` in
    `ducklake_metadata_manager.cpp` `GetOrphanFilesForCleanup`. Puffin files are
    DuckLake's OWN deletion-vector blobs, so this does **not** endanger our
    `.vortex`/`.lance`/`.db` special-format orphans (still not swept). No action;
    the survival test's premise holds. Confirms upstream is happy to grow the
    swept-suffix set, so keep the test as the trip-wire — a future bump adding a
    broad glob (or our formats) would break it loud. Landing in 1.5.5.

## Ideas we can't commit to yet

- [ ] **corpus content against non-parquet data formats (T3 synergy)** — the
  corpus replay currently tests OUR read path against DuckDB-written parquet.
  Idea (Jayson, 2026-07-07): reuse the corpus's rich table/data shapes to
  exercise the duckdb-format (and vortex/lance) write+read paths too. The
  oracle can't write those formats (upstream is parquet-only), so this needs
  the deferred "statement-translation" runner mode: TRINO executes the
  corpus's translated writes under `data_file_format='duckdb'`, oracle-less,
  comparing against parquet-mode results (self-oracle across formats). Would
  subsume much of the T3 matrix (time travel / system tables / pruning on
  `.db`) with upstream-authored data shapes. Non-trivial (write-side dialect
  translation); consider after the read-mirror axes are done.

- [ ] **union / separated catalog for experimental formats** — keep non-parquet
  tables (and possibly extension tables, e.g. lance-index bookkeeping) in a
  dedicated catalog so a "real" catalog is never polluted with rows a stock
  DuckDB can't understand; a Trino-side union view makes the two catalogs feel
  like one. Liked, but not easy: ATTACH ergonomics, config noise/duplication,
  cross-catalog identity (snapshot ids diverge), and how `USE`/qualified names
  surface it. Think more before speccing.

## Watch (direction-of-travel worries, nothing actionable)

- [ ] **server-side-commit trajectory (Quack)** — upstream `main` added
  `DuckLakeServerSideCommit` (`ducklake_commit` function +
  `quack_metadata_manager` staged-commit flow): a Quack client stages
  transaction rows into the metadata schema, then a single function call *on
  the Quack server* performs snapshot allocation, conflict checks, retries, and
  publish, catalog-side. Today Quack-only; direct-SQL backends still commit
  client-side. Worry: conflict validation migrates server-side and direct-SQL
  writers become second-class. Flip side (opportunity): on the Quack backend we
  could stage + call `ducklake_commit` ourselves and get upstream's conflict
  checking for free instead of maintaining our Kotlin commit protocol for that
  backend. Re-check each refresh.
  - **Re-checked 2026-07-18:** no new server-side-commit machinery this cycle.
    Quack `v1.5-variegata` (→1.5.5) changes were correctness/hardening only —
    reject `INSERT ... RETURNING` (was returning garbage), bind `count(*)` to the
    EMPTY virtual column + forbid `rowid` (structural trip-wire so a future remote
    UPDATE/DELETE can't half-enable via inherited NULL rowid), fetch-tag pushdown
    hardening, `EXTRA_HTTP_HEADERS` secret. Quack `main` is all fetch-ahead /
    read-ahead streaming + server-side client-id hashing (perf/plumbing). Trajectory
    unchanged; still Quack-only, direct-SQL commit stays client-side.
- [ ] **expire-vs-long-read (accepted risk, 2026-07-05)** — another engine's
  `expire_snapshots`/`cleanup_old_files` can delete files mid-read of a
  long-running Trino query. Accepted: our reads resolve the snapshot file list
  up front, so the window is file deletion during the read itself; no
  low-overhead coordination exists in the spec and upstream has no global
  locking story yet. Revisit when upstream grows one.
- [ ] **variant type (v1.1) vs Trino type system** — v1.1 adds
  variant/shredding (`ducklake_file_variant_stats`). Trino has no VARIANT type;
  mapping will be lossy (JSON? ROW?). No action until v1.1 tags; expect an ugly
  design decision.
- [ ] **spec instability horizon (~5-6 months)** — the catalog spec won't
  settle while the ecosystem is still hitting its "oh shit" moments
  (pg_ducklake #215/#216/#217 class). Our mitigation is the biweekly upstream
  research tax — working so far. At some point we need a direct conversation
  with the DuckLake team; they can't call it a standard and ignore outside
  implementors forever.

## Upstream lance/vortex extension canaries (retest on extension bumps)

Collapsed here from the completed driving list (F11). These are extension-side limitations we
gate around; `TestLanceExtensionCanary` (+ the vortex probes) is the trip-wire — retest and lift
the gate when an extension bump fixes the underlying issue.

- [ ] **lance ROW writes** — gated at schema time (null-struct handling upstream). Lift when the
  upstream null-struct fix lands.
- [ ] **lance MAP writes** — need lance format 2.2; currently gated with a clean upfront error
  (`TestDucklakeLanceFormat.mapColumnIsRejectedForLanceWrites`).
- [ ] **vortex MAP COPY** — native crash upstream; MAP writes gated.
- [ ] **lance FTS `k`** — best-effort top-k only.
- [ ] **lance arrow-scan NULL-row morph** — watched.
- [ ] **`add_files` lance `record_count`** — full dataset scan to count rows; costly on huge
  datasets (perf, not correctness).

## Upstream DuckDB `mysql` extension — cross-engine MySQL blocker (retest on DuckDB bumps)

- [ ] **DuckDB's `mysql` extension crashes/flakes reading a DuckLake-on-MySQL catalog.** Verified
  2026-07 on DuckDB 1.5.4 (latest): clean single-shot round-trips fail 5/5 with `Server has gone
  away` / `Got packets out of order`, and it SIGSEGVs the JVM at `ssl3_write_bytes` inside
  `mysql_scanner.duckdb_extension`. Reproduced on MySQL 8.0 + 8.4, with/without `ssl_mode=disabled`.
  DuckLake's MySQL metadata backend itself is fine (a one-shot `ATTACH` reliably bootstraps the
  schema), and OUR connector reads/writes MySQL directly via JDBC (unaffected, shipped + tested —
  see [CATALOG-BACKENDS.md](CATALOG-BACKENDS.md)). But cross-engine (DuckDB reading a MySQL-backed
  catalog) is DEFERRED until this stabilizes. Trip-wire: re-run the standalone MySQL probe on DuckDB
  version bumps; when green, wire MySQL into the cross-engine fixtures. **TODO: file upstream at
  duckdb/ducklake** (sibling to the data-file `file_format` dispatch gap, duckdb/ducklake#1289).
