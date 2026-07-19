# Upstream-Tracking Research Log

Append-only historical record. Newest entry on top. The procedure (formerly
`RESEARCH-HOWTO.md`) is now [`../RESEARCH-upstreams.md`](../RESEARCH-upstreams.md).
Action items per run live in sibling [`RESEARCH-TODO.md`](RESEARCH-TODO.md);
recent items have been folded as bullets into the working
`../TODO-WRITE-MODE.md` / `../TODO-READ-MODE.md` under "Open Research Items".

---

## 2026-06-29 — full refresh (ducklake, datafusion, ducklake-web, pg_ducklake, quack)

**Trigger:** user asked for a full upstream survey — check logged baselines,
last commit dates/hashes, and whether anything in the DuckLake spec, the
DuckLake DuckDB extension, or the DataFusion plugin would break us. Fresh
blobless clones of all five tracked repos into `vendor/`, fetched and diffed
against the baselines from the 2026-05-22 / 2026-05-29 entries.

**Surveyed repos and new baselines:**

| Repo | Baseline this run | Surveyed up to | Δ (no-merge) |
|---|---|---|---|
| `ducklake/` | `v1.5-variegata@04a91e8e`, `main@d897bc5a` | `v1.5-variegata@c23aca43` (2026-06-17), `main@96a4574d` (2026-06-25) | 126 / 355 |
| `datafusion-ducklake/` | `main@f1af7dd5` (post-v0.2.1) | `main@8abedc97` (2026-06-26, post-v0.3.1) | 21 |
| `ducklake-web/` | `main@58e9ed7e`, `quack@bb393710` | `main@82231c20` (2026-06-28), `quack` unchanged | 23 / 0 |
| `pg_ducklake/` | `main@011ab8d5` | `main@e906c7ae` (2026-06-18, v1.1.0, duckdb v1.5.4) | 716 |
| `duckdb-quack/` | `main@daae4826`, `v1.5-variegata@a3dbe3d5` | `main@ebf88413` (2026-06-19), `v1.5-variegata@40de7bad` (2026-06-10) | 116 / 14 |

`duckdb-web/` and `duckdb/` not refreshed this run.

### Headline: DuckLake metadata format → `1.0` — we are aligned

DuckLake's catalog format bumped through the migration chain V01→V04
(`ducklake_metadata.version` 0.1 → 0.2 → 0.3 → 0.4 → 1.0). Attach now **throws**
if a migration is required and `AUTOMATIC_MIGRATION=false`. The structurally
significant DDL (from `src/storage/ducklake_metadata_manager.cpp`):

- **V03 (0.3→0.4):** drops `ducklake_data_file.partial_file_info`, adds
  `partial_max BIGINT` to both `ducklake_data_file` and `ducklake_delete_file`;
  adds `table_id` to `ducklake_schema_versions` (schema-version tracking is now
  **per-table**, global rows deleted); new tables `ducklake_macro{,_impl,_parameters}`,
  `ducklake_sort_info`, `ducklake_sort_expression`, `ducklake_file_variant_stats`;
  `ducklake_column.default_value_type` + `default_value_dialect`.
- **V04 (0.4→1.0):** version-string bump only; 1.0 == 0.4 structurally.

**Verdict — parity.** Our jOOQ schema (`jvm/ducklake-catalog/generated/...`) is
already generated against a 0.4/1.0 catalog: no `partial_file_info`, `partial_max`
present, `ducklake_schema_versions.table_id` present, and the sort/macro/variant
tables exist. `getSnapshotIdForSchemaVersion` (`JdbcDucklakeCatalog.kt:1071`)
already prefers per-table `schema_versions` with a fallback for older/global
catalogs. We never read a `ducklake_metadata` `version` row and don't migrate —
fine for an attach-as-reader/writer posture, but worth keeping in mind that we
will silently operate on a catalog regardless of declared version.

### Substantive findings

**ducklake `v1.5-variegata` / `main` — deletion vectors + server-side commit.**
- Deletion-vector puffin files (`ducklake_deletion_vector.{hpp,cpp}`, footer-less
  puffin, multiple DVs per puffin, `DeletionVectors.yml` CI, `deletion_vectors.json`
  config). **Parity** — `DucklakePuffinDeleteReader` already decodes DuckLake's
  Roaring-bitmap DV blob and `DucklakeSplitManager.kt:624` validates the `format`
  column (parquet/puffin).
- Server-side / staged commit (`ducklake_server_side_commit.cpp`,
  `ducklake_staged_commit.cpp`, transaction → transaction_state refactor): commit
  id assigned at commit, full metadata written in one txn. Quack-path; feeds the
  open `quack-server-side-txn-lifecycle` write-mode item. **Research.**
- "Hide metadata catalog" option (`hide_metadata_catalog.test`) + answer MIN/MAX
  from catalog metadata when stats are exact (aggregate-pushdown opportunity).
  **Research** (not yet bulleted; low priority).
- `merge_adjacent_files` across schema version + `partial_max` snapshot-bounded
  cross-snapshot compaction (`ducklake_multi_file_reader.cpp:298`). Confirms the
  **already-tracked** `partial_max time-travel correctness` read-mode gap — we
  ignore `partial_max`, so a DuckDB-compacted table read under time-travel
  over-includes later-snapshot rows. Still open; no new bullet (already in
  `TODO-READ-MODE.md` Open research items, linked to DESIGN-maintenance § 6).

**datafusion-ducklake (v0.3.0 / v0.3.1 + unreleased).**
- **#148** — nested List/struct/map columns read back all-NULL because the
  read-schema field-id matcher keyed off Parquet *leaf* columns instead of the
  *top-level* field. **Bug-shaped on our side** given we just shipped nested
  ADD/DROP FIELD. Added `nested-field-id-top-level-match` bullet to
  `TODO-READ-MODE.md` Open research items (~1h verification spike).
- **#149 / #146** — explicit widening-only `promote_column_type`; **reject** silent
  type changes on `Replace`/`Append` writes (ALTER-vs-INSERT separation). Confirms
  our posture; cross-check our write path matches. **Parity (verify).**
- #131 `get_table_row_count` accounting for delete files; #127 staging-file +
  multipart streamed writes; #126/#128 parquet compression + row-group caps;
  #133 nanosecond-tz → `timestamptz_ns`. Mostly Rust-writer-side; informational.

**pg_ducklake — v1.1.0, duckdb v1.5.4, DuckLake v1.5-variegata vendored.** Repo
layout flattened; SQL surface redesigned for 1.0.0; cloud-storage secret mgmt;
ORDER BY/Top-N pushdown into PG scan. Mostly non-portable PG glue. Signal: they
track duckdb **1.5.4** + DuckLake v1.5-variegata. **No action.**

**duckdb-quack.** Added a connection-id scalar function + `quack_active_connections`
view, query-uuid/client-id serialization, cancel-by-name. Directly useful for the
open `quack-jdbc-vs-quack-connid-pinning` / `quack-server-side-txn-lifecycle`
write-mode research items. **Research input** — no new bullet (items exist).

### Documents touched this run

- `../RESEARCH-upstreams.md` — "Latest baselines" table bumped to the SHAs above.
- `../TODO-READ-MODE.md` — added `nested-field-id-top-level-match` bullet under
  Open research items (read-path).
- This log entry.

### Open question for the user

Promote candidates from "Open research items" into real backlog (needs explicit OK):
`nested-field-id-top-level-match` (read), and whether to act on `partial_max`
time-travel correctness now vs. defer to the compaction work.

---

## 2026-05-29 — `datafusion-ducklake` focused refresh

**Trigger:** user pulled fresh `vendor/datafusion-ducklake` and asked for a
scoped survey of what changed since the previous baseline. No refresh of
`ducklake/`, `pg_ducklake/`, `duckdb-quack/`, `ducklake-web/`, or
`duckdb-web/` this run.

**Surveyed repos and new baselines:**

| Repo | Baseline this run | Surveyed up to | Active branches checked |
|---|---|---|---|
| `datafusion-ducklake/` | `origin/main@536729a8` (v0.2.1, 2026-05-19) | `origin/main@f1af7dd5` (post-v0.2.1, 2026-05-29 — no new tag yet) | `main` only |

9 incoming commits, 10,496 LOC added across 32 files. No new release tag —
`v0.2.1` is still the latest tag; everything in this window is unreleased.
1 CI tweak (skip), 1 writer-DDL spec-alignment bug-fix (not portable to us),
and 7 substantive feature PRs.

### Substantive findings

**#115 / #121 / #124 — DuckLake row lineage (`rowid` virtual column).**
Three PRs land the official row-lineage feature end-to-end in Rust:

- #115 (2026-05-20, `83e3972`) plumbs `row_id_start` + `record_count` from
  `ducklake_data_file` through all four metadata providers, defines
  `ROW_ID_PARQUET_FIELD_ID = 2_147_483_540` (the Iceberg-reserved field-id
  DuckLake uses to tag the embedded `_ducklake_internal_row_id` column that
  UPDATE / compaction writes), and adds `RowIdExec` to inject a synthetic
  BIGINT `rowid` column (= `row_id_start + position` per file). Opt-in via
  `DuckLakeCatalog::with_row_lineage(true)`.
- #121 (2026-05-28, `0d3921f`) brings the Postgres writer to parity with
  the SQLite writer: bootstraps `ducklake_table_stats`
  (`record_count, next_row_id, file_size_bytes`), and `register_data_file`
  hands out non-overlapping row-id ranges in-transaction. `Replace`
  clears `record_count`/`file_size_bytes` but leaves `next_row_id`
  untouched — rowids monotonic across the whole table lifetime, not per
  generation.
- #124 (2026-05-29, `f1af7dd`) closes the last gap: the multicatalog
  reader's `get_table_files_for_select` was hard-coding `row_id_start`,
  `snapshot_id`, and `max_row_count` to `None`; PR #124 projects the real
  values, matching the single-catalog Postgres reader verbatim.

**Verdict:** Rust is now ahead of us on this surface. We track `$row_id`
in `DESIGN-virtual-columns.md` and under "Add DuckDB-equivalent virtual
columns" in `TODO-READ-MODE.md`, but the JVM Trino side does not expose a
virtual `rowid` column yet. The Rust `RowIdExec` + `ROW_ID_PARQUET_FIELD_ID`
detection logic is a clean reference impl when we pick this up — copy the
semantics, not the code. **No new RESEARCH-TODO needed** — gap already
tracked in the working TODO and DESIGN.

**#122 / #123 — Official maintenance ops (`expire_snapshots`,
`cleanup_old_files`, `delete_orphaned_files`, single-catalog `DROP TABLE`).**

- #122 (2026-05-28, `4be0758`) ports upstream's two-phase vacuum to both
  SQLite single-catalog and Postgres multicatalog paths. Three-phase
  delete philosophy preserved: DROP TABLE tombstones (soft-delete,
  time-travel preserved), `expire_snapshots` deletes unreachable metadata
  rows and inserts orphaned paths into `ducklake_files_scheduled_for_deletion`,
  `cleanup_old_files` does the actual `object_store.delete()` then deletes
  the bookkeeping rows. `ExpireCriteria::{Versions, OlderThan}` +
  `CleanupCriteria::{All, OlderThan}` mirror upstream's named parameters.
- #123 (2026-05-29, `f8804af`) ports the third official command
  `ducklake_delete_orphaned_files` — storage-listing sweep, subtracts
  every path referenced by the metadata (data files + delete files +
  pending scheduled-for-deletion rows), deletes whatever's left. The
  `OlderThan` filter is applied to `object_store::ObjectMeta.last_modified`
  (in-flight write protection), matching upstream's `last_modified <
  older_than` guard. `.parquet`-suffix filter at listing time also matches
  upstream.

Side-by-side validated against the official DuckDB+DuckLake extension via
`examples/maintenance_demo.sql` + `examples/orphan_cleanup_demo.sql` —
behaviour matches modulo cosmetic differences (upstream's DELETE+INSERT
vs their Replace being one snapshot).

**Verdict:** Rust is now ahead of us on the M8 maintenance surface. We
already track `expire_snapshots`, `cleanup_old_files`, and
`delete_orphaned_files` in `TODO-WRITE-MODE.md §M8`, plus a Commit-Failure
File Cleanup section that explicitly defers to M8. **No new RESEARCH-TODO
needed** — but added a reference pointer for when we pick up M8 (see
[`datafusion-maintenance-ops-reference`](RESEARCH-TODO.md#datafusion-maintenance-ops-reference)).

**#117 / #120 — Multicatalog Phase 1 + `drop_table_in_catalog`.**

Non-spec extension for RuntimeDB's per-tenant catalog isolation. Adds
`MulticatalogManager`, `PostgresMetadataWriter`, `MulticatalogProvider`,
and `initialize_multicatalog_schema`. New tables: `ducklake_catalog`,
`ducklake_catalog_{snapshot,schema}_map`, per-catalog
`ducklake_schema_versions`, plus a `schema_version` column on
`ducklake_snapshot` and a `catalog_id` column on
`ducklake_files_scheduled_for_deletion` (documented divergence from the
official single-catalog schema). FOR UPDATE on the catalog row (30s
`lock_timeout`) serialises concurrent writers. Cross-catalog
`table_id`/`schema_id` rejected at write time.

**Verdict:** Not a DuckLake spec feature, not a gap on our side.
Multi-tenant isolation, if it ever became relevant for us, would more
naturally live on the layer above the connector (one Trino catalog
instance per tenant). Worth noting only because the Rust schema for
`ducklake_files_scheduled_for_deletion` now diverges from upstream —
a future cross-engine compatibility check on that table needs to handle
the optional `catalog_id` column. **No new RESEARCH-TODO** — flagged in
COMPARE.

**#116 — Writer DDL spec-alignment fix.** `SqliteMetadataWriter` had been
DDL-bootstrapping `ducklake_column` without `parent_column`,
`initial_default`, `default_value`, `default_value_type`,
`default_value_dialect`, panicking every read. CI missed it because
`write-sqlite` wasn't on the test step. Not portable to us (we attach to
an existing catalog; our jOOQ-generated `DucklakeColumnRecord` carries
all five columns). Worth being aware of as a sign their backend-CI
matrix is incomplete (encryption / postgres / mysql writer fixtures still
not covered). **Parity — no action.**

**#118 — CI bump (`ubuntu-latest-m` → `ubuntu-latest`).** Skip.

### Documents touched this run

- `jvm/trino-ducklake/dev-docs/archive/COMPARE-datafusion-ducklake.md` —
  refresh-date header; TL;DR rewritten to reflect that Rust is no
  longer purely read-focused; three new capability rows (row lineage,
  maintenance ops, multi-tenant catalog isolation); writer-DDL bug note
  added under "doc-vs-code mismatch"; two new "ideas worth stealing"
  entries pointing at `src/maintenance.rs` + `src/row_id.rs` as
  reference impls; test-LOC counts bumped (~9.2k → ~13.4k).
- `jvm/trino-ducklake/dev-docs/RESEARCH-LOG.md` — this entry.
- `jvm/trino-ducklake/dev-docs/RESEARCH-TODO.md` — appended one new
  reference-pointer item; the substantive gaps (row lineage, M8 ops) are
  already tracked in `DESIGN-virtual-columns.md` / `TODO-READ-MODE.md` /
  `TODO-WRITE-MODE.md §M8` and don't need duplicating.

### Items added to RESEARCH-TODO this run

- [`datafusion-maintenance-ops-reference`](RESEARCH-TODO.md#datafusion-maintenance-ops-reference)

### Next-run baselines

| Repo | Branch | SHA |
|---|---|---|
| `datafusion-ducklake/` | `main` | `f1af7dd5a29482bce40acf8edf28175f48c23753` |

(All other repos: no refresh this run; baselines from the 2026-05-22 entry
still apply.)

---

## 2026-05-22 — Quack catalog blockers, focused pass

**Trigger:** DuckDB bumped to 1.5.3 stable. Re-running
`TestJdbcDucklakeCatalogOnQuackSmoke` against the Quack catalog backend
(testcontainer running DuckDB CLI 1.5.3 with core Quack pre-installed)
reproduces three blockers that gate write support on the Quack-backed
path: the multi-streaming-scan optimizer check, the "Can only update
base table" binder check, and a `NotImplementedException: InMemory not
implemented yet` from `duckdb_databases()` / `SHOW DATABASES`. User
asked whether the upstream C++ DuckLake-on-Quack path uses different
SQL access patterns that side-step these, and what cost we'd pay to
match upstream's envelope.

**Scope:** Quack-specific only. No refresh of `datafusion-ducklake`,
`pg_ducklake`, the non-Quack DuckLake commits, or `duckdb-web`/`ducklake-web`
outside the Quack-relevant branches.

**Surveyed repos and new baselines:**

| Repo | Baseline this run | Surveyed up to | Active branches checked |
|---|---|---|---|
| `ducklake/` | `origin/v1.5-variegata@e6a3bd0a` (2026-05-19) | `origin/v1.5-variegata@04a91e8e`, `origin/main@d897bc5a` (no change since 2026-05-19) | `v1.5-variegata` only (focus on quack files) |
| `ducklake-web/` | `origin/main@2bee8779` (2026-05-19) | `origin/main@58e9ed7e`, `origin/quack@bb393710` | `main` + `quack` branch |
| `duckdb-web/` | `origin/main@318e0f5f` (2026-05-19) | `origin/main@755b8af9`, `origin/carlopi-patch-quack-nightly@b6d59151` | `main` + `carlopi-patch-quack-nightly` branch |
| `duckdb-quack/` | — (first-time read) | `origin/v1.5-variegata@a3dbe3d5`, `origin/main@daae4826` | both; `origin/HEAD → v1.5-variegata` (stable lineage) |
| `duckdb/` | `origin/main@432ebc26` (2026-05-19, informational) | `origin/main@ac9ee657`, `origin/v1.5-variegata@6e9cdf83` | only as source of the "Can only update base table" binder error |

### Blocker ownership table

| # | Error | Owning layer | Source location | In upstream `quack.json` skip list? |
|---|---|---|---|---|
| 1 | `Not implemented Error: Multiple streaming scans or streaming scans + CTAS / insert in the same query are not currently supported` | **duckdb-quack optimizer extension** (registered into local DuckDB's optimizer pipeline) | `vendor/duckdb-quack/src/storage/quack_optimizer.cpp:71-73` — `QuackOptimizer::Optimize` throws when `op_info.scans.size() + op_info.insert_count > 1` per quack connection_id (`quack_optimizer.cpp:66`) | YES — top-level entry "FIXME: Not implemented Error: Multiple streaming scans …" skips **27 tests** (`vendor/ducklake/test/configs/quack.json` lines 144-174). Most-recent addition `0a3b19a6` ("Skip bucket_pruning.test under quack catalog", 2026-05-21) added a 28th. |
| 2 | `Binder Error: Can only update base table` | **DuckDB core binder** (independent of Quack) | `vendor/duckdb/src/planner/binder/statement/bind_update.cpp:130 and :135` — `Binder::BindNode(UpdateQueryNode&)` throws when `bound_table.plan->type != LogicalOperatorType::LOGICAL_GET` OR `bound_table_get.GetTable()` is null. A remote table reached through the Quack catalog binds as a streaming `LOGICAL_GET` on `quack_query` whose `GetTable()` is null — there is no `TableCatalogEntry` behind it because it's a `TableFunction`, not a base table. | YES — there is one explicit entry, line 81-86 of `quack.json`: `"Test issues UPDATE against the metadata catalog directly"` → `test/sql/delete/delete_legacy_missing_mapping_after_rename_add_files.test`. |
| 3 | `Not implemented Error: InMemory not implemented yet` | **duckdb-quack QuackCatalog** | Was `vendor/duckdb-quack/src/storage/quack_catalog.cpp:InMemory()` / `GetDBPath()`. **FIXED upstream** — `669440a` "Implement QuackCatalog::InMemory and GetDBPath" (Carlo Piovesan, 2026-05-19; merged via PR #143 / `893744a`), on `v1.5-variegata`. Now returns `false` and `client_connection->ServerURI().Uri()` respectively (`quack_catalog.cpp:129-134`). | Was `quack.json` line 102-106 entry for `database_size.test` (GetDatabaseSize, separate stub) and the `attach_replace.test` entry on line 175-178 ("FIXME: NotImplementedException: GetDBPath not implemented yet") — that second one is the one #143 closes. |

The "Quack-installed-from-core" environment shipped in DuckDB 1.5.3
contains `duckdb-quack` at the v1.5-variegata tip _just before_ the
#143 InMemory fix landed (the 1.5.3 distribution was cut from `8736cb23`
"Set build for v1.5.3" before #143). The fix is on the branch, so the
**next** core-shipped Quack will not throw blocker #3; the 1.5.3 nightly
or a manual `INSTALL quack FROM 'core_nightly'` would already pick it up.

### Side-by-side SQL-shape comparison

The decisive finding: **upstream's C++ `QuackMetadataManager` does not
issue `ducklake_*` queries against the locally-attached metadata
catalog at all.** Every metadata query is wrapped in a single
`CALL system.main.quack_query_by_name(<catalog_literal>, <sql>)` table
function call, which the local DuckDB binder sees as exactly **one**
`LogicalGet` regardless of how many `ducklake_*` references the inner
SQL contains. The wrapped SQL is prepared and executed server-side
(`vendor/duckdb-quack/src/quack_scan.cpp:95` —
`client.Request<PrepareResponseMessage>(... PrepareRequestMessage(connection_id, query))`).
The remote DuckDB binds it against its **local** base tables, so
UPDATE/DELETE on `ducklake_*` succeeds there as a base-table mutation.

Concrete proof, from `vendor/ducklake/src/metadata_manager/quack_metadata_manager.cpp:14-30`:

```cpp
unique_ptr<QueryResult> QuackMetadataManager::Query(string &query) {
    auto &ducklake_catalog = transaction.GetCatalog();
    auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
    query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
    SubstituteCatalogPlaceholders(query);

    auto metadata_catalog_name_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
    auto wrapper = StringUtil::Format("CALL system.main.quack_query_by_name(%s, %s)", metadata_catalog_name_literal,
                                      SQLString(query));
    auto result = transaction.ExecuteRaw(std::move(wrapper));
    ...
}
```

Note the deliberate override of `{METADATA_CATALOG}`: in the C++ Quack
path it expands to **only the schema identifier** (no catalog prefix),
because the outer `quack_query_by_name(catalog_name, sql)` provides the
remote catalog as a function argument and the inner SQL runs in that
remote's catalog context. This is **different** from the default
`SubstituteCatalogPlaceholders` (`vendor/ducklake/src/storage/ducklake_metadata_manager.cpp:2237-2253`)
which expands `{METADATA_CATALOG}` to `catalog_identifier + "." +
schema_identifier`.

| Access pattern | C++ `QuackMetadataManager` (v1.5-variegata) | `JdbcDucklakeCatalog` (current) |
|---|---|---|
| Latest snapshot lookup | `CALL system.main.quack_query_by_name('lake', 'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM main.ducklake_snapshot WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM main.ducklake_snapshot)')` — see `ducklake_metadata_manager.cpp:3811-3814` (`GetLatestSnapshotQuery()`). Local plan: **1 LogicalGet** (the quack table function). Server-side: same-table multi-scan, fine because it's a normal local DuckDB query. | `SELECT snap.snapshot_id, ... FROM ducklake_snapshot AS snap WHERE snap.snapshot_id = (SELECT max(snap.snapshot_id) FROM ducklake_snapshot snap)` — `JdbcDucklakeCatalog.java:1051-1054`. Local plan via the `ATTACH 'ducklake:quack:...'` path: **2 LogicalGets** of `quack_query` (one for each `FROM ducklake_snapshot`). Trips `QuackOptimizer` at `quack_optimizer.cpp:66-73`. |
| Partition-info read | `SELECT partition_id, part.table_id, partition_key_index, column_id, transform FROM {METADATA_CATALOG}.ducklake_partition_info part JOIN {METADATA_CATALOG}.ducklake_partition_column part_col USING (partition_id) WHERE ...` — `ducklake_metadata_manager.cpp:774-780`. Wrapped in `quack_query_by_name`, executes server-side as a normal join. Local plan: 1 LogicalGet. | `dsl.select(...).from(partinfo).innerJoin(partcol).on(...).where(...)` — `JdbcDucklakeCatalog.java:575-587`. Local plan: 2 LogicalGets of `quack_query`. Same trip-wire. |
| Schema-or-table end-snapshot mutation (drop) | C++ writes `UPDATE {METADATA_CATALOG}.ducklake_schema SET end_snapshot=... WHERE schema_id=...` etc., then sends the entire UPDATE through `quack_query_by_name`. Server-side it's a normal `UPDATE main.ducklake_schema ...` against a base table — passes the binder's "Can only update base table" check trivially. | `tx.dsl().update(sch).set(sch.END_SNAPSHOT, ...).where(sch.SCHEMA_ID.eq(schemaId)).and(sch.END_SNAPSHOT.isNull()).execute()` — `JdbcDucklakeCatalog.java:1571-1575` (and analogous `dropTable` at lines 1708-1743). Issued over JDBC to local DuckDB, which sees `ducklake_schema` as a remote `quack_query`-backed table function (no `TableCatalogEntry`) and throws "Can only update base table" at `bind_update.cpp:135`. |

**Answer to the user's question — do they use different access
patterns to avoid our blockers?** Yes, fundamentally. The upstream C++
path sends the entire metadata SQL as an opaque string to the remote
DuckDB via the `quack_query_by_name` table function and executes it
remotely against base tables. Our JDBC path uses `ATTACH
'ducklake:quack:...'` and then issues the same SQL **locally** against
the attached catalog, where the binder/optimizer sees `ducklake_*`
references as multiple distinct streaming `quack_query` scans (and
non-base tables for UPDATE/DELETE). Both blockers 1 and 2 disappear on
the upstream path because the local DuckDB never plans the
multi-`ducklake_*` query — it plans a single `CALL` of a table
function with the whole SQL packed inside as a `VARCHAR` argument.

### Upstream skip-list quote (verbatim, `vendor/ducklake/test/configs/quack.json`, v1.5-variegata tip `04a91e8e`)

The two entries that map directly to our blockers:

```
{
  "reason": "Test issues UPDATE against the metadata catalog directly",
  "paths": [
    "test/sql/delete/delete_legacy_missing_mapping_after_rename_add_files.test"
  ]
},
{
  "reason": "FIXME: Not implemented Error: Multiple streaming scans or streaming scans + CTAS / insert in the same query are not currently supported",
  "paths": [
    "test/sql/add_files/add_files_hive_many_columns.test",
    "test/sql/add_files/add_files_hive_partition_cast.test",
    "test/sql/add_files/add_files_nested_list_struct_nulls.test",
    "test/sql/alter/add_column_default_stats.test",
    "test/sql/compaction/compaction_partitioned_non_adjacent.test",
    "test/sql/compaction/compaction_partitioned_table.test",
    "test/sql/compaction/merge_rewrite_partial_file_info.test",
    "test/sql/default/all_types_column_default_stats.test",
    "test/sql/metadata/appender_data_files.test",
    "test/sql/metadata/appender_partition_values.test",
    "test/sql/metadata/appender_variant_stats.test",
    "test/sql/partitioning/bucket_partitioning.test",
    "test/sql/partitioning/bucket_pruning.test",
    "test/sql/partitioning/merge_adjacent_null_partition.test",
    "test/sql/partitioning/partition_rename_in_transaction.test",
    "test/sql/rewrite_data_files/test_rewrite_partitioning.test",
    "test/sql/sorted_table/merge_adjacent_sorted_basic.test",
    "test/sql/sorted_table/merge_adjacent_sorted_drop_recreate.test",
    "test/sql/sorted_table/merge_adjacent_sorted_expression.test",
    "test/sql/sorted_table/merge_adjacent_sorted_macro_expression.test",
    "test/sql/sorted_table/merge_adjacent_sorted_macro_expression_transaction.test",
    "test/sql/sorted_table/merge_adjacent_sorted_nested_expression.test",
    "test/sql/sorted_table/merge_adjacent_sorted_repeated.test",
    "test/sql/sorted_table/merge_adjacent_sorted_reset.test",
    "test/sql/sorted_table/merge_adjacent_sorted_transaction_alter_table_unrelated.test",
    "test/sql/sorted_table/merge_adjacent_sorted_transaction_renamed.test",
    "test/sql/sorted_table/schema_version_same_transaction.test",
    "test/sql/sorted_table/set_sorted_by_rollback_basic.test",
    "test/sql/sorted_table/set_sorted_by_rollback_mixed.test"
  ]
}
```

Plus one more transaction-mode block (lines 53-65) listing 9
transaction-conflict / concurrent tests skipped with `"FIXME: Uses SET
immediate_transaction_mode=true with concurrent connections"`, and a
detailed-narrative one on the inlining-rollback case (line 110-114):

```
"ROLLBACK after ducklake_flush_inlined_data does not restore
ducklake_inlined_delete_* rows on the quack-backed metadata catalog
(line 162 expects 15 rows after rollback, gets 0). Each `CALL
quack_query(uri, sql)` opens a fresh server-side connection that
auto-commits independently of the outer DuckLake transaction. Proper
fix needs metadata operations routed through a transaction-scoped
server-side connection (e.g., reuse the local QuackCatalog's
connection_id via quack_query_by_name and have the catalog's
QuackTransaction lifecycle drive server-side BEGIN/COMMIT/ROLLBACK)."
```

This last note is independently important to us: even on the upstream
path, server-side transactionality is **not** automatic. Our JDBC path
has its own transaction semantics (JDBC `setAutoCommit(false)` /
`commit()` / `rollback()` on a HikariCP pool connection), and the
remote DuckDB sees each `quack_query` table function call as a
separate auto-committed connection unless server-side BEGIN/COMMIT is
wired through.

### Recent changes worth noting

**`vendor/duckdb-quack`** — 28 commits on `v1.5-variegata` since the
extension was first stood up; only one substantive change since the
introduction of QuackMetadataManager is the InMemory/GetDBPath stub
fix (`669440a` 2026-05-19, PR #143). The 2026-05-22 PR #149
"bump_duckdb_153" (`a3dbe3d5`) is a pure CI bump (submodule pointer +
2 lines of `MainDistributionPipeline.yml`); no logic change. The `main`
branch sits at `daae4826` and is only the "make it compile on duckdb
main" patch on top of `v1.5-variegata` — no behavior divergence
relevant to this research.

**`vendor/ducklake/v1.5-variegata`** — 28 commits since baseline; only
`0a3b19a6` touches Quack (adds another skip). `quack_metadata_manager.{hpp,cpp}`
unchanged.

**`vendor/ducklake-web/origin/quack`** branch (`bb393710`, 2026-05-11)
is a 1-commit diff against `main` — adds DuckDB+Quack as a catalog
option to `index.html`'s landing-page deployment-diagram selector. No
standalone docs page yet. Label: `(beta)`.

**`vendor/duckdb-web/origin/carlopi-patch-quack-nightly`** (`b6d59151`,
2026-05-21) is the **DuckDB-side** Quack docs, not yet merged to
`main`. Posture quote, `docs/current/quack/troubleshooting.md`:

> Quack is currently available as a beta release. It is not ready for
> production and is subject to breaking changes until the release of
> DuckDB v2.0.

`docs/current/quack/overview.md` advertises only two client access
patterns: `quack_query(uri, query)` for stateless query, and `ATTACH
'quack:host'` for full-catalog attach with `query` table macro
`⟨catalog⟩.query('SELECT ...')`. The branch confirms upstream is happy
with the table-function approach as a primary client API.

### Recommendation

**Match upstream's SQL shape on the Quack backend path, scoped to
`JdbcDucklakeCatalog`'s Quack-detection branch only. Don't touch the
Postgres or in-process DuckDB paths.**

Concrete: introduce a Quack-aware query executor that wraps every
issued SQL in `CALL system.main.quack_query_by_name('<metadata_catalog>',
<sql_literal>)` before sending it over JDBC. Reads return result rows
naturally (the table function streams them); writes (`UPDATE`/`DELETE`/`INSERT`)
return an empty result set but execute server-side. This neutralises
blockers 1 and 2 by sending an opaque single-LogicalGet plan to the
local binder.

Files / methods that would change:

- `jvm/ducklake-catalog/src/dev/brikk/ducklake/catalog/QuackBackedDuckDbCatalogUrl.java`
  — `connectionInitSql()` (`QuackBackedDuckDbCatalogUrl.java:139-158`):
  drop the `ATTACH 'ducklake:quack:...' AS lake (...)` line and the
  `USE <metadata_catalog>.main`. The wrapper-based approach does not
  need a local attached catalog; it just needs `LOAD quack` and the
  `CREATE SECRET`.
- `jvm/ducklake-catalog/src/dev/brikk/ducklake/catalog/JdbcDucklakeCatalog.java`:
  - Constructor (`:124-185`): on the Quack branch, build a custom jOOQ
    `ExecuteListener` / `VisitListener` that rewrites every rendered
    SQL string into `CALL system.main.quack_query_by_name(<lit>, <sql>)`
    before it hits the JDBC layer. Apply only when
    `QuackBackedDuckDbCatalogUrl.matches(configuredUrl)`.
  - All `executeWriteTransaction` paths (`:1020-1144`, plus every
    `update(...)`/`deleteFrom(...)` in `dropTable`, `dropSchema`,
    `renameTable`, etc.): no per-method changes needed if the wrapper
    is applied centrally at SQL-render time. The end-snapshot UPDATEs
    will then run server-side as base-table mutations.
- The `{METADATA_CATALOG}` substitution rule needs to match upstream's
  Quack-specific version: just the schema identifier, no catalog
  prefix. Since jOOQ already renders without schema (`withRenderSchema(false)`
  at `:179`) this is already correct for the SELECT/INSERT/UPDATE/DELETE
  paths — but verify there are no places we hand-build SQL with a
  catalog prefix.
- DDL on the metadata catalog (`InitializeDuckLake`-style if we do it,
  schema migrations): wrap the same way.
- Transaction semantics: this is the **hard** part. JDBC's
  `setAutoCommit(false)` won't carry through `quack_query_by_name` —
  each call opens a fresh server-side connection that auto-commits.
  Upstream's skip-list note on `test_deletion_inlining_transaction.test`
  identifies this same gap and proposes server-side
  `BEGIN/COMMIT/ROLLBACK` via the `QuackTransaction` lifecycle. Our
  port would need to: (a) issue `BEGIN TRANSACTION` as its own wrapped
  call at the start of `attemptWriteTransaction`, (b) issue `COMMIT`
  on success and `ROLLBACK` on failure, **and** (c) somehow guarantee
  all three calls hit the same server-side connection ID. The Quack
  client connection is keyed by `connection_id`, which is local —
  per-pooled-JDBC-connection. If we keep one HikariCP connection per
  transaction (we already do, via `dataSource.getConnection()` inside
  `attemptWriteTransaction`), and Quack maps that JDBC connection to a
  single server-side connection ID, we're probably OK. Needs a
  verification spike against a live Quack server.

Tradeoff:
- **Pro**: closes all three blockers (blocker 3 is already fixed
  upstream; blockers 1 and 2 disappear via the wrapper). Brings us to
  parity with the upstream C++ test envelope. Keeps the jOOQ DSL
  intact — the rewrite is at SQL-render time, not in the DSL.
- **Con**: every SQL string is now interpolated as a string literal
  inside a function call. jOOQ-generated parameterised SQL with `?`
  placeholders needs to be either (a) materialised before wrapping
  (jOOQ supports `getSQL(ParamType.INLINED)`) or (b) the placeholders
  passed through `quack_query_by_name`'s second argument, which is
  `VARCHAR` — placeholders inside a string literal are placeholders to
  the *remote* prepared statement, not the local one. Easiest path: use
  `ParamType.INLINED` to render parameter values into the SQL before
  wrapping (loses prepared-statement caching, gains correctness).
  Smallish performance loss; tolerable for metadata operations.
- **Con**: server-side transaction lifecycle is non-trivial (see
  bullet above). Implementing it wrong = silent data corruption under
  retry / rollback.
- **Con**: Quack is officially **beta**, "not ready for production,
  subject to breaking changes until DuckDB v2.0" (quoted upstream).
  We'd be coupling our integration to an explicit-beta protocol.

The alternatives:
1. **Wait for Quack to lift the restrictions** — viable but slow.
   `QuackOptimizer`'s multi-streaming-scan check is structural, not a
   stub; lifting it requires server-side support for multiplexing
   streaming scans on one connection. Not on any visible roadmap.
   "Can only update base table" is a generic DuckDB binder check that
   pre-dates Quack and won't change for Quack's sake.
2. **Ship read-only Quack backend** — fastest. The read path mostly
   uses single-table reads (`getCurrentSnapshotId`, `getSchema`,
   `getTable`, `listSnapshots` — all single LogicalGets), and would
   sidestep blocker 1 for everything except `attemptWriteTransaction`'s
   snapshot-read (which is on the write path and we'd not exercise).
   Blocker 2 wouldn't fire on reads. Blocker 3 is fixed upstream.
   Lets us ship the Quack backend as the documented "beta, read-only"
   to match upstream's "experimental" label, and lets users **write**
   via DuckDB's own DuckLake extension while Trino reads. Tradeoff: no
   write-side parity, no schema mutations via Trino on Quack backend.

The user's call. The "match upstream" route is the only one that fully
unblocks write parity. Recommendation: **adopt the wrapper approach,
but feature-flag it behind a config setting; ship read-only as the
default Quack posture, with write enabled when the user opts in and
acknowledges the beta upstream label.**

### Documents touched this run

- `jvm/trino-ducklake/dev-docs/RESEARCH-LOG.md` — this entry.
- `jvm/trino-ducklake/dev-docs/RESEARCH-TODO.md` — appended Quack
  blocker-related research items (see below).

### Items added to RESEARCH-TODO this run

- [`quack-wrapper-rewrite-spike`](RESEARCH-TODO.md#quack-wrapper-rewrite-spike)
- [`quack-server-side-txn-lifecycle`](RESEARCH-TODO.md#quack-server-side-txn-lifecycle)
- [`quack-read-only-fallback`](RESEARCH-TODO.md#quack-read-only-fallback)
- [`quack-jdbc-vs-quack-connid-pinning`](RESEARCH-TODO.md#quack-jdbc-vs-quack-connid-pinning)

### Next-run baselines

| Repo | Branch | SHA |
|---|---|---|
| `ducklake/` | `main` | `d897bc5a35f887c9087403bc20efd92d99272e69` |
| `ducklake/` | `v1.5-variegata` | `04a91e8eccba24c5b6d7d04f8be4d7d33c2c3a30` |
| `ducklake-web/` | `main` | `58e9ed7e` |
| `ducklake-web/` | `quack` | `bb3937104ced27093c14fe3bbe0a2651c7bd802e` |
| `pg_ducklake/` | `main` | `011ab8d5033e2d5f97cd57b62d8a0ca5978e9dc0` (unchanged from 2026-05-19) |
| `datafusion-ducklake/` | `main` | `f53a82ee` |
| `duckdb-web/` | `main` | `755b8af9` |
| `duckdb-web/` | `carlopi-patch-quack-nightly` | `b6d59151462189ed70dc0e755bdbcefae38f401d` |
| `duckdb-quack/` | `v1.5-variegata` | `a3dbe3d54d6504a8a9206cb7c46f252f807a93aa` (origin/HEAD) |
| `duckdb-quack/` | `main` | `daae482605cc1ec43e0fb56df72c5fdf85ad0d27` |
| `duckdb/` | `main` | `ac9ee657` (informational) |
| `duckdb/` | `v1.5-variegata` | `6e9cdf83` (1.5.3 backports) |

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

- `jvm/trino-ducklake/dev-docs/archive/COMPARE-datafusion-ducklake.md` — refresh
  header + Stats row rewrite + test LOC bump.
- `jvm/trino-ducklake/dev-docs/archive/COMPARE-pg_ducklake.md` — refresh header +
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
