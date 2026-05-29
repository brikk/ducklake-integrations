# Upstream-Tracking Research TODO

Open research questions and proposed action items surfaced by upstream
surveys. Each entry has a stable anchor so the log can link to it.

When the user escalates an item to a real working TODO, move it into
`jvm/trino-ducklake/dev-docs/TODO-WRITE-MODE.md` or `TODO-READ-MODE.md`
and strike it through here with a back-reference.

See [`RESEARCH-HOWTO.md`](RESEARCH-HOWTO.md) for the workflow and
[`RESEARCH-LOG.md`](RESEARCH-LOG.md) for the surveys that produced
these items.

---

## Open

### rename-table-change-chain

**Source:** `ducklake/` PR #1154 ("Keep change-chain when renaming
tables/views"), merged to `v1.5-variegata` 2026-05-13.
**Kind:** research → likely gap
**Impact:** write path; specifically `JdbcDucklakeCatalog.renameTable`.
**Our state:** We already do this correctly for **views** —
`renameView` emits `altered_view:<viewId>` (the spec change-type), and
the per-table `ducklake_schema_versions` row is bumped via
`incrementSchemaVersion()`. See `COMPARE-pg_ducklake.md` C1/C2 closed
items. Whether `renameTable` emits the analogous `altered_table:<id>`
change-record and preserves the `snapshot_changes` chain across the
rename is unverified.
**Proposed next step:** spike (30 min): read `renameTable` in
`JdbcDucklakeCatalog.java`, confirm it writes `altered_table:<id>` to
`ducklake_snapshot.snapshot_changes`, and that `ducklake_table.table_id`
is preserved (not re-allocated). If yes — record as parity in the log
and remove this entry. If no — escalate to working TODO with a test:
DuckDB renames a Trino-created table and reads `table_changes()` on it
without `InvalidInputException` (mirroring the existing view test
`testDuckdbParsesTrinoWrittenViewAndSchemaDdlChanges`).

### internal-retry-strategy

**Source:** `ducklake/` PR #1163 ("Fix retrial conflicts") merged
2026-05-15, plus #1150 ("Fix off-by-one error in retry attempts").
**Kind:** research / design
**Impact:** write path concurrency; `JdbcDucklakeCatalog` commit path.
**Our state:** No internal retry loop. On lineage conflict we throw
`DUCKLAKE_TRANSACTION_CONFLICT` and Trino's engine retries the whole
query. Upstream maintains a bounded retry inside `FlushChanges` and
just shipped two bug-fixes against it. The hazards their fixes
address (catalog-ID reuse across attempts, off-by-one on the retry
counter) are real, and would apply to us *if* we ever added internal
retry.
**Proposed next step:** decision spike. Two options:

1. Stay query-level retry — write a short doc note in
   `dev-docs/REPORT_CROSS_ENGINE_WRITE.md` (or similar) explaining
   why we deliberately don't have an internal retry loop, and what
   the implications are (each query retry re-pays planning, but
   avoids the upstream class of bugs).
2. Add bounded internal retry — would shrink wasted planning work on
   conflict-heavy workloads, but requires correctly re-allocating
   catalog-IDs per attempt (the upstream pitfall) and re-checking
   conflict on fresh state. Non-trivial.

User decides which posture is the right one before any code lands.

### uint-type-promotion-audit

**Source:** `ducklake/` PR #1128 ("Fix type promotion for UINTEGER"),
merged 2026-05-07.
**Kind:** research → potential read-side gap
**Impact:** read path; `DucklakeTypeConverter`.
**Our state:** We already map `uint8` → SMALLINT, `uint16` → INTEGER,
`uint32` → BIGINT, `uint64` → DECIMAL(20, 0) — wider-on-read, with the
new `DucklakeUnsignedRangeChecker` validating writes. Upstream just
fixed a type-promotion bug specifically for UINTEGER. Is the bug
something *they* could expose in catalog output we read (e.g. a column
that gets promoted *in the catalog representation* under schema
evolution, which we then mis-decode)? Or is it purely an in-engine
DuckDB execution issue?
**Proposed next step:** read the PR #1128 description + the test
(`vendor/ducklake/test/sql/alter/...`) for ~10 min. If the fix is purely
in DuckDB's expression execution (not in the catalog schema), record
parity. If the fix is in how `ducklake_column.column_type` is written
or how `ducklake_data_file` schema annotations resolve, we need a
corresponding fix on our read side.

### schema-evolution-missing-column

**Source:** `ducklake/` PR #1142 ("Fix missing column in schema
evolution"), merged 2026-05-11.
**Kind:** research → potential read-side gap
**Impact:** read path; how we project newly-added columns over old
data files.
**Our state:** We resolve columns by ID via `ducklake_column` and
project against the active snapshot's schema. A column added *after*
a data file was written should read as NULL (or the configured
default) for that file. Whether we currently do this correctly for
all the cases #1142 covers is unverified.
**Proposed next step:** spike (1h). Read PR #1142 + its test case.
Build the same scenario in our cross-engine test
(`TestDucklakeCrossEngineCompatibility`): DuckDB inserts, `ALTER
TABLE ADD COLUMN`, DuckDB inserts more, Trino reads — verify the
old-file rows project the new column as NULL/default and the
new-file rows project the inserted value.

### disallow-drop-sorted-column

**Source:** `ducklake/` PR #1112 ("Disallow dropping sorted columns"),
merged 2026-05-05.
**Kind:** gap, pending — only relevant after sorted-table writes land
**Impact:** write path; DDL.
**Our state:** We don't support sorted-table writes yet (it's in the
TODO-WRITE-MODE.md priority list). When we do, the DDL path needs to
reject `DROP COLUMN` for any column that's part of the sort key.
**Proposed next step:** track. When sorted-table writes are picked up,
add a checklist item to that section: "reject DROP COLUMN if column
is in `ducklake_table_column_sort` (or whichever sort-key catalog
table the spec defines)".

### delete-file-filter-pushdown

**Source:** `ducklake/` commits `df1f8dee` ("Add expression comparison
filter in delete") + `5b2b7f52` ("If an expression is a
BOUND_COMPARISON with a constant or a BOUND_OPERATOR with a compare_in
we push it down").
**Kind:** research → possible optimization
**Impact:** read path; specifically delete-file scan.
**Our state:** We read delete files as `(file_path, pos)` tuples,
build a per-data-file position set, and filter in the Parquet reader.
We don't push predicates *into* the delete-file scan itself.
**Proposed next step:** scoping spike. Look at Trino's
`ConnectorPageSource` filter-pushdown surface to see if we can hand a
`TupleDomain` to the delete-file Parquet reader the same way we do for
data files. If yes, the win is reading less of the delete-file when
the query has a narrow filter (e.g. a single `pos` value in a MERGE
plan). If no, record as "not actionable on Trino's API surface" and
keep an eye on Doris's analogous API.

### quack-hardening-watch

**Source:** `ducklake/` PR #1151 ("Experimental"), plus #1159
("Fix ducklake option segfault" on the quack branch), 2026-05-12 →
2026-05-15.
**Kind:** watch
**Impact:** catalog backend; cross-cutting.
**Our state:** Quack listed as priority 1 in `TODO-WRITE-MODE.md`.
Upstream label is still "Experimental" and there is at least one
post-merge bug fix on the quack branch.
**Proposed next step:** check on each refresh: (1) is the
"Experimental" label dropped from upstream docs / PRs? (2) is there a
DuckDB version cut where Quack is documented as stable on
`ducklake-web/docs/stable/`? (3) does the DuckDB JDBC driver
documentation describe how to point a local DuckDB at a Quack server?
Only commit to a hard cross-engine CI dependency once these three are
green; before then, keep the work in a development branch.

### quack-wrapper-rewrite-spike

**Source:** `duckdb-quack`, `vendor/ducklake/src/metadata_manager/quack_metadata_manager.cpp:14-30`,
dated 2026-05-22 (research pass on Quack catalog blockers; see
`RESEARCH-LOG.md` entry 2026-05-22).
**Kind:** research → likely large action item
**Impact:** write path on the Quack-backed catalog only;
`JdbcDucklakeCatalog`'s constructor + `attemptWriteTransaction` SQL
emission. Does **not** affect the Postgres or in-process DuckDB paths.
**Our state:** On the Quack backend we today `ATTACH 'ducklake:quack:...'`
and let the local DuckDB binder plan our jOOQ DSL against the attached
catalog. This hits `QuackOptimizer`'s "Multiple streaming scans" check
(`vendor/duckdb-quack/src/storage/quack_optimizer.cpp:71`) on any query
with two or more `ducklake_*` references, including the
`attemptWriteTransaction` snapshot read (`JdbcDucklakeCatalog.java:1051-1054`)
and every partition-info JOIN (`:575-587`). It also hits the binder's
`Can only update base table` check (`vendor/duckdb/src/planner/binder/statement/bind_update.cpp:130/135`)
on every UPDATE/DELETE.
**Proposed next step:** spike (1-2 days). Build a jOOQ
`ExecuteListener` (or similar) that, on the Quack branch only:

1. Renders the SQL with `ParamType.INLINED` (no `?` placeholders).
2. Wraps the rendered SQL in
   `CALL system.main.quack_query_by_name('<metadata_catalog>', '<inlined-sql>')`.
3. Sends the wrapper string over JDBC.

Drop the `ATTACH 'ducklake:quack:...'` line from
`QuackBackedDuckDbCatalogUrl.connectionInitSql()` (`:139-158`); just keep
`LOAD quack` + `CREATE SECRET`. Verify against
`TestJdbcDucklakeCatalogOnQuackSmoke`:

- `createSchemaCommitsAndListSchemasSeesIt` — exercises snapshot-read
  via `attemptWriteTransaction`.
- `dropTable` / `dropSchema` paths — exercise UPDATE on `ducklake_*`.

Out-of-scope for the spike: server-side transactionality (see
`quack-server-side-txn-lifecycle`). Spike should succeed at single-op
correctness; multi-op transactional correctness gates on that separate
item.

### quack-server-side-txn-lifecycle

**Source:** `vendor/ducklake/test/configs/quack.json` skip-list note on
`test_deletion_inlining_transaction.test`, dated 2026-05-22.
**Kind:** research → blocker on full write parity
**Impact:** write path on Quack-backed catalog;
`JdbcDucklakeCatalog.attemptWriteTransaction` transaction lifecycle.
**Our state:** Even on the upstream C++ Quack path, server-side
transactionality is **not** automatic. Per upstream's own skip note:
"Each `CALL quack_query(uri, sql)` opens a fresh server-side connection
that auto-commits independently of the outer DuckLake transaction.
Proper fix needs metadata operations routed through a transaction-scoped
server-side connection ... and have the catalog's QuackTransaction
lifecycle drive server-side BEGIN/COMMIT/ROLLBACK." Our JDBC pool
already pins one connection per write transaction
(`dataSource.getConnection()` inside `attemptWriteTransaction` at
`JdbcDucklakeCatalog.java:1045`), but it is unverified whether DuckDB's
Quack catalog pins one server-side connection ID per local JDBC
connection.
**Proposed next step:** measurement spike. Stand up a Quack server with
a known token, open one local DuckDB JDBC connection, run a sequence of
`quack_query_by_name(catalog, sql)` calls, and inspect the
`quack_connection_id` field of `duckdb_logs_parsed('Quack')` (see
`vendor/duckdb-web/docs/current/quack/reference.md`, "Quack Log"
section) — confirm all server-side calls share one `connection_id` as
long as the local DuckDB session is the same. If yes, server-side
`BEGIN`/`COMMIT`/`ROLLBACK` should ride that pinned connection
correctly. If no, we'd need to introduce a per-write-transaction
"opaque server-side session handle" — non-trivial.

### quack-read-only-fallback

**Source:** Quack research pass 2026-05-22; combined fallback if
`quack-wrapper-rewrite-spike` proves expensive or
`quack-server-side-txn-lifecycle` blocks multi-op writes.
**Kind:** gap → potential alternate posture
**Impact:** Trino reads on Quack-backed DuckLake, writes deferred to
DuckDB-native clients.
**Our state:** The current read path uses single-table queries for
most metadata accesses (`getCurrentSnapshotId`, `getSchema`,
`getTable`, `listSchemas`, `listTables`, `listSnapshots`). These are
single LogicalGets and do **not** trip `QuackOptimizer`'s multi-scan
check. `getPartitionSpecs` and `getSortKeys` do (`JdbcDucklakeCatalog.java:575-587`,
`:614-627`) — they JOIN two `ducklake_*` tables. UPDATE/DELETE never
fires on the read path.
**Proposed next step:** decision item. If we adopt a read-only Quack
posture, replace the 2-table JOINs in `getPartitionSpecs` /
`getSortKeys` with sequential per-table fetches joined in-JVM (one
`SELECT FROM partinfo WHERE ...` followed by one `SELECT FROM partcol
WHERE partition_id IN (...)`), or move them behind the wrapper from
`quack-wrapper-rewrite-spike` (which works for reads too — the wrapper
also handles JOIN-spanning queries). Verify the full read suite passes
on Quack without writes. Document the posture in `TODO-WRITE-MODE.md`
and `TODO-READ-MODE.md` as "Quack: read parity, write deferred".

### quack-jdbc-vs-quack-connid-pinning

**Source:** Quack research pass 2026-05-22.
**Kind:** research, unblocks `quack-server-side-txn-lifecycle`
**Impact:** transactional write path on Quack; potentially also
session-state cross-talk if pinning is per-JDBC.
**Our state:** Unknown whether the DuckLake C++ extension's Quack
catalog binds one server-side connection ID per local DuckDB session,
per local DuckLake transaction, or per local query. Quack's protocol
docs (`vendor/duckdb-web/docs/current/quack/overview.md`,
`vendor/duckdb-web/docs/current/quack/reference.md`) describe
"connection_id (server-issued, stable across requests in one ATTACH)"
but don't spell out the JDBC-pool case.
**Proposed next step:** read `vendor/ducklake/src/storage/ducklake_initializer.cpp`
+ `ducklake_transaction.cpp` and `vendor/duckdb-quack/src/quack_client.cpp`
for the connection-ID lifecycle. Cross-reference with the
"Connection Request" handshake in
`vendor/duckdb-quack/src/quack_message.cpp`. ~30 min read. If clear,
record finding in this entry; if not, escalate to a code-level spike
against a live Quack server.

### datafusion-maintenance-ops-reference

**Source:** `datafusion-ducklake/` PRs #122 (`4be0758`, 2026-05-28) and
#123 (`f8804af`, 2026-05-29), plus `examples/maintenance_demo.{rs,sql}`
and `examples/orphan_cleanup_demo.{rs,sql}`.
**Kind:** reference pointer (not a new gap — gap already tracked in
`TODO-WRITE-MODE.md §M8`).
**Impact:** write-path maintenance ops; specifically the future M8 work.
**Our state:** `expire_snapshots`, `cleanup_old_files`, and
`delete_orphaned_files` listed in `TODO-WRITE-MODE.md §M8 Maintenance
Operations` — not implemented. Commit-Failure File Cleanup section
defers explicitly to M8.
**Proposed next step:** when M8 is picked up, lean on the Rust impl as
a reference for non-obvious semantics:

1. **Three-phase split**: tombstone (DROP TABLE; soft-delete preserves
   time travel) → expire (DELETEs unreachable metadata + INSERTs
   orphaned paths into `ducklake_files_scheduled_for_deletion`) →
   cleanup (`object_store.delete()` then deletes bookkeeping rows).
   Mirrors upstream DuckDB's `FileSystem::RemoveFiles` semantics; only
   the storage backend differs.
2. **Criteria enums**: `ExpireCriteria::{Versions(Vec<i64>),
   OlderThan(DateTime<Utc>)}` for expire; `CleanupCriteria::{All,
   OlderThan(DateTime<Utc>)}` for cleanup + orphan-sweep. Mirrors
   upstream's named-parameter shapes.
3. **In-flight write guard**: `delete_orphaned_files` `OlderThan` filter
   applies to `object_store::ObjectMeta.last_modified`, **not** to
   catalog `schedule_start`. This protects in-flight writes that haven't
   registered metadata yet. Upstream does the same — `last_modified <
   older_than`.
4. **Suffix filter**: `.parquet` filter at storage-listing time.
   Matches upstream; avoids deleting `_SUCCESS` markers, logs, etc.
5. **Orphan-sweep referenced-set**: UNION ALL across data files, delete
   files, and pending scheduled-for-deletion rows. The third one is
   non-obvious — a file that's been expired but not yet cleaned up is
   still "referenced" for the purpose of orphan reclamation.
6. **Parity harness**: `examples/maintenance_demo.sql` and
   `examples/orphan_cleanup_demo.sql` drive the same lifecycle through
   the official DuckDB+DuckLake extension. Our cross-engine compatibility
   suite (`TestDucklakeCrossEngineCompatibility`) should add equivalent
   side-by-side coverage when M8 lands — the upstream extension is the
   oracle.

Out of scope for the pointer: their multicatalog `catalog_id` schema
extension on `ducklake_files_scheduled_for_deletion`. We are single-catalog
per connector instance; the official schema applies as-is.

## Closed (escalated to working TODO)

*(none yet)*
