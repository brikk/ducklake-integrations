# Upstream-Tracking Research Log

Append-only. Newest entry on top. See [`RESEARCH-HOWTO.md`](RESEARCH-HOWTO.md)
for procedure. Action items found during a run live in
[`RESEARCH-TODO.md`](RESEARCH-TODO.md).

---

## 2026-05-19 — refresh run

**Trigger:** user request after fetching updated `datafusion-ducklake` and
`pg_ducklake`. Also a full survey of `ducklake/` since approx. 2026-04-23
(user's last bulk pull date for that repo).

**Surveyed repos and new baselines:**

| Repo | Baseline this run | Surveyed up to | Active branches checked |
|---|---|---|---|
| `ducklake/` | (local HEAD `d897bc5a`, since-date `2026-04-23`) | `origin/main@d897bc5a`, `origin/v1.5-variegata@e6a3bd0a` | both |
| `ducklake-web/` | local HEAD `2bee8779` | `origin/main@2bee8779` | `main` |
| `pg_ducklake/` | local HEAD `377aabf4` | `origin/main@011ab8d5` | `main` |
| `datafusion-ducklake/` | local HEAD `08c6d68` (v0.2.0) | `origin/main@536729a8` (v0.2.1) | `main` |
| `duckdb-web/` | local HEAD `318e0f5f` | `origin/main@318e0f5f` (already at tip) | `main` |

### `datafusion-ducklake` — 4 incoming commits (v0.2.0 → v0.2.1)

- **#112 `TableProvider::statistics()`** — adds `total_byte_size` aggregate
  computed from per-file `file_size_bytes − delete_file_size_bytes`,
  marked `Precision::Inexact` (catalog tracks compressed parquet bytes,
  DataFusion's contract is uncompressed Arrow). No row count, no column
  stats. **Verdict: JVM still way ahead** — our `getTableStatistics`
  exposes row count + per-column null fractions, data sizes, ranges, with
  conservative `TableStatistics.empty()` when deletes or live inlined
  rows are present. Updated `COMPARE-datafusion-ducklake.md` to reflect
  the new Rust capability + refresh-date header + bumped test LOC count.
- README / version / changelog only otherwise.

### `pg_ducklake` — 7 incoming commits, vendored DuckLake **not** bumped

- **#199 `CREATE TABLE ... WITH (ducklake.table_path = '...')`** — new
  per-table data path override at create time. Catalog mechanism
  (`ducklake_table.path` column) is already used by us; Trino-side
  property exposure is missing. **Escalated to working TODO** as
  "Per-Table Storage Path (`location` Table Property)" in
  `TODO-WRITE-MODE.md`.
- **#197/#198 MAX(schema_version) for inlined heap selection** —
  upstream contract clarified. Our `getInlinedDataInfos()` already
  returns all rows ≤ snapshot.schema_version; we don't write to the
  inlined heap. **No JVM analog of the bug.** Documented in
  `COMPARE-pg_ducklake.md` "Known, documented, no action required".
- **#195 streaming `flush_inlined_data`** — buffering caused OOM on
  large heaps. Relevant **when** we implement the maintenance op.
  Added to the maintenance scope notes in `COMPARE-pg_ducklake.md`.
- **#193 SQLSTATE 40001 for direct-insert race** — same hazard our
  `ensureSnapshotLineageUnchanged()` guards against. **At parity.**
- **#187 / #188 / #190 direct-insert fast-path improvements** —
  PG-specific (VALUES detector, STABLE coercion, observability view).
  Mentioned in `COMPARE-pg_ducklake.md` PG-specific add-ons section.

Vendored `third_party/ducklake/` is unchanged; submodule pointer at
`third_party/pg_duckdb/third_party/duckdb` also unchanged.

Updated `COMPARE-pg_ducklake.md` with refresh-date header + new
findings.

### `ducklake/` (upstream reference) — ~30 PRs on `v1.5-variegata`, ~14 on `main` since 2026-04-23

**Quack catalog backend (#1151, merged 2026-05-12, "Experimental"
label):** added `quack_metadata_manager.{hpp,cpp}` so a DuckLake catalog
can live in a remote DuckDB reached over Quack RPC — the obvious answer
to multi-engine shared DuckDB-format catalogs. **Escalated to working
TODO** as "Quack Catalog Backend (DuckDB RPC)" in `TODO-WRITE-MODE.md`,
priority 1.

**Inlined-data lifecycle hardening:**

- #1145 "Drop orphaned inlined tables" + "Also cleanup inlined tables
  if they are superseded and then flushed". Our `getInlinedDataInfos()`
  has `existsAsTable()` defensive filtering (`JdbcDucklakeCatalog.java:658`)
  for this exact case. **Parity** — no action.

**Rename / change-chain semantics:**

- #1154 "Keep change-chain when renaming tables/views". Maps to our
  existing `renameView → altered_view` work. Whether our `renameTable`
  emits the same change-record lineage is unverified. **RESEARCH-TODO
  added** — see [`rename-table-change-chain`](RESEARCH-TODO.md#rename-table-change-chain).
- #1130, #1069, #1106 — incremental DDL fixes (view rename, view rename +
  comment, rename-then-drop-view).
- #1138 case-insensitive column rename — relevant if we support
  case-insensitive identifier paths anywhere; we generally don't.

**Concurrency / retry:**

- #1163 "Fix retrial conflicts" — **write-side** fix in upstream's
  `FlushChanges` retry loop. Bug was reuse of `transaction_changes`
  across retry attempts, which leaked catalog-IDs allocated during a
  failed first commit into the second-attempt conflict check. **Does
  not affect us** today (we have no internal retry — we throw and let
  Trino retry). **RESEARCH-TODO added** — see
  [`internal-retry-strategy`](RESEARCH-TODO.md#internal-retry-strategy) —
  decide whether to add bounded internal retry inside `JdbcDucklakeCatalog`.
- #1150 retry off-by-one — same retry loop, separate edge case.

**Type & schema evolution:**

- #1128 "Fix type promotion for UINTEGER". **RESEARCH-TODO added** — see
  [`uint-type-promotion-audit`](RESEARCH-TODO.md#uint-type-promotion-audit).
- #1142 "Fix missing column in schema evolution". **RESEARCH-TODO
  added** — see [`schema-evolution-missing-column`](RESEARCH-TODO.md#schema-evolution-missing-column).
- #1112 "Disallow dropping sorted columns". Write-side rule. Relevant
  *when* we add sorted-table writes. **RESEARCH-TODO added** — see
  [`disallow-drop-sorted-column`](RESEARCH-TODO.md#disallow-drop-sorted-column).
- #1110 "Fix bucket out of range" — bucket partitioning rule.
  Relevant to our bucket-partition write path.
- #1056 "Fix default stats in transaction".
- #1071 "Fix drop table after change in txn".

**Filter pushdown into deletes (#df1f8dee, #5b2b7f52):** reference is
teaching the delete-file reader to evaluate `BOUND_COMPARISON` constants
and `compare_in` predicates. Potential win on our side if Trino's
`ConnectorPageSource` filter API can be pushed into the delete-file scan
similarly. **RESEARCH-TODO added** — see
[`delete-file-filter-pushdown`](RESEARCH-TODO.md#delete-file-filter-pushdown).

**Stylistic / portability:**

- ANSI `CAST(...)` instead of `::` operator (#1124, #1139). Signals
  upstream caring about catalog-backend portability. We use jOOQ — not
  affected.

**Misc:**

- #1095 merge-adjacent-empty-files fix — maintenance op.
- #1100 / #1081 quote handling in DDL.
- "Dummy scan for partition write rework" + "partitioning tests" — work
  in progress on partition write internals; nothing actionable until it
  lands.

### `ducklake-web` — landing-page Quack mention + Engineering blog rename

- `index.html` catalog-backend list now reads: `DuckDB, in-process` /
  `DuckDB + Quack (beta)`. No standalone Quack docs page yet — matches
  the "Experimental" label upstream.
- "Blog" → "Engineering blog" rename.
- Trademark guidelines added, FAQ entry on Frozen DuckLakes, a
  data-inlining blog post, pg_ducklake mention. None substantive for
  our work.
- No `docs/0.5/` directory; versioned docs go 0.1 → 0.4 + `stable`. The
  v1.5 work shows up under `stable` once cut.

### `duckdb-web`

- Local HEAD already at `origin/main`; no DuckLake-relevant changes
  since user's last refresh.

### Working docs updated this run

- `jvm/trino-ducklake/dev-docs/COMPARE-datafusion-ducklake.md` — refresh
  header + Stats row rewrite + test LOC bump.
- `jvm/trino-ducklake/dev-docs/COMPARE-pg_ducklake.md` — refresh header +
  `table_path` row + inlined MAX(schema_version) confirmation +
  streaming-flush note + direct-insert fast-path expansion.
- `jvm/trino-ducklake/dev-docs/TODO-WRITE-MODE.md` — Top Priorities
  rewritten as ordered list; added §Quack Catalog Backend (DuckDB RPC)
  and §Per-Table Storage Path.

### Items added to RESEARCH-TODO this run

- [`rename-table-change-chain`](RESEARCH-TODO.md#rename-table-change-chain)
- [`internal-retry-strategy`](RESEARCH-TODO.md#internal-retry-strategy)
- [`uint-type-promotion-audit`](RESEARCH-TODO.md#uint-type-promotion-audit)
- [`schema-evolution-missing-column`](RESEARCH-TODO.md#schema-evolution-missing-column)
- [`disallow-drop-sorted-column`](RESEARCH-TODO.md#disallow-drop-sorted-column)
- [`delete-file-filter-pushdown`](RESEARCH-TODO.md#delete-file-filter-pushdown)
- [`quack-hardening-watch`](RESEARCH-TODO.md#quack-hardening-watch)

### Next-run baselines

Diff the next survey against these SHAs (the agent should `git fetch`
and compare `origin/<branch>` to these):

| Repo | Branch | SHA |
|---|---|---|
| `ducklake/` | `main` | `d897bc5a35f887c9087403bc20efd92d99272e69` |
| `ducklake/` | `v1.5-variegata` | `e6a3bd0a8554b74d97cbc7e8acc3e2c9f01a0385` |
| `ducklake-web/` | `main` | `2bee87791ebc2975158e48092d63a6f1580225bd` |
| `pg_ducklake/` | `main` | `011ab8d5033e2d5f97cd57b62d8a0ca5978e9dc0` |
| `datafusion-ducklake/` | `main` | `536729a8394b79478745a79759340a93791adda9` |
| `duckdb-web/` | `main` | `318e0f5fb525d4bc114a066d69ba23a0d123c6cd` |
