# DuckLake Write Mode — Open Items

The shipped write surface is summarized in the feature chart in [README](../README.md);
this file tracks what's still open. Reuse strategy when adding to the write path:
lean on Trino's `ParquetWriter` and merge-on-read (`ConnectorMergeSink`) plumbing,
keep DuckLake-specific code limited to catalog semantics and snapshot logic.

Spec context for several items below lives in
[DUCKLAKE_1_0_IMPACT.md](DUCKLAKE_1_0_IMPACT.md) (the DuckLake 1.0 spec impact
reference). Spec issues we filed upstream live in
[REPORT_CROSS_ENGINE_WRITE.md](REPORT_CROSS_ENGINE_WRITE.md).

## Bucket Partitioning

- [ ] **Bucket partitioning (full implementation).** Add BUCKET to
  `DucklakePartitionTransform` with arity, implement Murmur3 in
  `DucklakePartitionComputer` (Guava `Hashing.murmur3_32_fixed()`), accept
  `bucket(N, col)` syntax in `DucklakeTableProperties`, and parse the `bucket(N)`
  transform string from `ducklake_partition_column.transform` on read. Formula:
  `(murmur3_32(v) & INT_MAX) % N`, Iceberg-compatible. ~100 lines across 4–5
  files. See
  [DUCKLAKE_1_0_IMPACT.md § Bucket Partitioning](DUCKLAKE_1_0_IMPACT.md#1-bucket-partitioning).

## Sorted Table Writes

- [ ] **Apply table sort spec during Parquet writes** in `DucklakePageSink`.
  Trino-written files would then be pre-sorted for DuckDB compaction. Medium-high
  effort — requires Trino `ParquetWriter` sort integration. See
  [DUCKLAKE_1_0_IMPACT.md § Sorted Tables](DUCKLAKE_1_0_IMPACT.md#2-sorted-tables).

## Schema Evolution Gaps

- [ ] `ALTER TABLE SET TYPE` (type promotion)
- [ ] `ALTER TABLE ADD/DROP FIELD` (nested struct field manipulation)

## `default_value_dialect = 'trino'` for User-Defined DEFAULTs

When user-defined `DEFAULT` expressions ship for Trino-written tables, write the
literal `'trino'` to `ducklake_column.default_value_dialect` (not `'brikk-trino'`
— the dialect names the SQL syntax, which is plain Trino SQL; brikk metadata
lives only in our view rows). Today every column gets `default_value = 'NULL'`
(the "no default" sentinel) and `default_value_dialect` SQL NULL — safe and
honest, since the field is informational and only meaningful when there's a real
literal or expression to interpret. Call sites:
`JdbcDucklakeCatalog.insertColumnTree`, `JdbcDucklakeCatalog.renameColumn`.
Pinned today by
`TestDucklakeCrossEngineCatalogMetadata.testDuckdbReadsTrinoTableWithNullDefaultValueDialect`
(asserts the SQL NULL contract). See [REPORT_CROSS_ENGINE_WRITE.md] Issue 1.

## Commit Context (DuckDB `set_commit_message` equivalent)

Surface user-supplied author/message/extra-info fields onto
`ducklake_snapshot.{author, commit_message, commit_extra_info}`.

Session properties:
- `ducklake.commit_author`
- `ducklake.commit_message`
- `ducklake.commit_extra_info`

Optional convenience procedures:
- `CALL ducklake.system.set_commit_context(author => 'Pedro', message => 'Inserting myself', extra_info => '{"foo":7}')`
- `CALL ducklake.system.clear_commit_context()`

Source: pg_ducklake exposes `set_commit_message()` and it's a commonly-wanted
feature.

## Logical Conflict Checking + Concurrency Test Coverage

Tracked together because the same engineering session should land both the
matrix and the harness that exercises it. Engine-agnostic — all changes land in
`jvm/ducklake-catalog`, reusable by Doris/Spark/etc. The Trino plugin already
just calls `commitInsert` / `commitDelete` / `commitMerge` / DDL methods on the
catalog and translates the resulting exceptions, so no plugin work is required
beyond confirming the existing `translateCatalogExceptions` wrapper still
maps the new conflict messages cleanly.

**Status (2026-05-10): Steps 1–4 landed.** Conflict-detection now matches
upstream's commit-time semantics. The summary below preserves the original
plan; "landed" notes inline call out where the implementation deviates.
Earlier framing of Step 4 as "relax the strict lineage check" was wrong:
review of the vendored upstream source
(`temp/pg_ducklake/third_party/ducklake/src/storage/ducklake_transaction.cpp:2460–2477`)
showed our `ensureSnapshotLineageUnchanged` is functionally equivalent
to upstream's `ducklake_snapshot.snapshot_id` PK fence on attempt 1.
The actual remaining gap is the matrix upstream runs on retry, which
catches dueling-name commits the state-based check in Step 3 misses.

Files added in Steps 1–3: `ConcurrentWriterHarness`, `WriteChange`,
`LogicalConflictCheck`, `LogicalConflictException`; new tests
`TestConcurrentInsertSameTable`, `TestConcurrentInsertDifferentTables`,
`TestConcurrentInsertVsDropColumn`, `TestConcurrentInsertVsDropTable`,
`TestConcurrentAlteredTableVsAlteredTable`. The
`TransactionConflictException.retryable()` flag is what
`WriteTransactionRetry` consults to decide whether to retry —
`LogicalConflictException` returns `false` and short-circuits the retry
loop.

**Current behavior to be aware of**: `JdbcDucklakeCatalog.attemptWriteTransaction`
(`JdbcDucklakeCatalog.java:905`) calls `ensureSnapshotLineageUnchanged`
(`JdbcDucklakeCatalog.java:985`) which throws `TransactionConflictException`
on **any** intervening commit. `WriteTransactionRetry` then retries with
exponential backoff (`MAX_RETRY_COUNT` attempts) and the action re-runs against
a fresh snapshot read. So today's gap is *not* "concurrent commits silently
land" — it's "the retry's action re-runs with stale per-call arguments
(`tableId`, column-stats column_ids, delete-target data_file_ids) that may
reference entities the winner committed away during the original window."

The acceptance scenario:
1. T2 calls `commitInsert(tableId=42, fragments[col_stats column_id=99])`
2. T1 commits `dropColumn(42, 99)`
3. T2's first attempt: lineage check fails → retry
4. T2's retry's action runs to completion (it doesn't re-validate the
   `column_id`s in the fragment) → `applyInsertFragments`
   (`JdbcDucklakeCatalog.java:1621`) inserts `ducklake_file_column_stats` rows
   pointing at column 99, which is end-snapshotted → catalog corruption.

### Step 1 — Concurrency test harness + pinning tests

- [x] **Extract the latch-pause pattern from
  `TestJdbcDucklakeCatalogConcurrentCommit`** (`jvm/ducklake-catalog/test/.../TestJdbcDucklakeCatalogConcurrentCommit.java`)
  into a reusable fixture (e.g. `ConcurrentWriterHarness`) exposing
  `parkOneAttempt(threadName)` and `runConcurrently(winnerOp, loserOp)`. Uses
  the existing `JdbcDucklakeCatalog.beforeWriteTransactionAction` test seam
  (`JdbcDucklakeCatalog.java:879`). Refactor the existing
  `concurrentCommitTriggersRetryAndBothSchemasLand` test to use the fixture so
  the abstraction is proven against the live behavior it already validates.
- [x] **`TestConcurrentInsertSameTable.concurrentInsertsBothCommit`**: two
  threads INSERT into the same table; loser parks before mutation, winner
  commits, loser releases, retries, and commits. Pin today's behavior. Source:
  pg_ducklake's `pg_isolation` spec for same-table concurrent INSERTs.
- [x] **`TestConcurrentInsertDifferentTables.concurrentInsertsBothCommit`**:
  two threads, each inserting into a different table; both commits must
  succeed (loser still retries because lineage advanced — that's fine; it
  must produce a clean second snapshot). Catches snapshot-id cross-talk.
  Source: pg_ducklake's `concurrent_cross_table_writes` spec.

  Both tests must pass against unmodified production code — they pin behavior,
  they don't drive the matrix work.

### Step 2 — Structured change tracking inside `DucklakeWriteTransaction`

- [x] **Add a typed `WriteChange` channel alongside today's `addChange(String)`.**
  `DucklakeWriteTransaction.changes` is a `List<String>` formatted for
  `ducklake_snapshot_changes.changes_made` (see `formatChangesMade` at
  `JdbcDucklakeCatalog.java:817`); great for human readability, awkward for a
  conflict matrix. Add a parallel `List<WriteChange>` where `WriteChange` is a
  sealed interface with variants:
  - `CreatedSchema(String name)`
  - `DroppedSchema(long schemaId)`
  - `CreatedTable(String schemaName, String tableName)`
  - `DroppedTable(long tableId)`
  - `AlteredTable(long tableId)` — covers add/drop/rename column
  - `InsertedIntoTable(long tableId, Set<Long> referencedColumnIds)` — column ids
    drawn from `DucklakeWriteFragment.columnStats()`
  - `DeletedFromTable(long tableId, Set<Long> referencedDataFileIds)` — file ids
    drawn from `DucklakeDeleteFragment`
  - `CreatedView(String schemaName, String viewName)`
  - `DroppedView(long viewId)`
  - `AlteredView(long viewId)` — covers replace/rename
- [x] Convert the `addChange(...)` call sites in `JdbcDucklakeCatalog`
  (`addColumn`, `dropColumn`, `renameColumn`, `commitInsert`, `commitDelete`,
  `commitMerge`, `dropTable`, `dropSchema`, `createSchema`, `createTable`,
  `createView`, `dropView`, `replaceViewMetadata`, `renameView`) to record
  the typed change *and* the existing string in one helper —
  `tx.recordChange(WriteChange.alteredTable(tableId))` etc. This is mechanical
  but touches every write path; do it as one PR.

### Step 3 — `LogicalConflictCheck` + acceptance test

- [x] **Add `LogicalConflictCheck` invoked between `action.execute(tx)` and
  `insertSnapshotRow` in `attemptWriteTransaction`** (insertion point:
  `JdbcDucklakeCatalog.java:938`–`944`, after the existing
  `ensureSnapshotLineageUnchanged` call). For each typed `WriteChange` the
  action recorded, query the catalog at `tx.getCurrentSnapshotId()` to confirm
  referenced entities are still in the expected state:
  - `InsertedIntoTable(tableId, columnIds)` → assert `tableId` is active
    AND every `columnId` in `columnIds` is active at `currentSnapshotId`
    (use the `activeAt` helper already in `JdbcDucklakeCatalog`).
  - `DeletedFromTable(tableId, dataFileIds)` → assert `tableId` is active
    AND every `dataFileId` is active.
  - `AlteredTable(tableId)` / `DroppedTable(tableId)` / column ops →
    assert `tableId` is active.
  - `Created*` ops are PK-protected on the underlying catalog row INSERTs
    today, so they need no extra check (a duplicate-name race surfaces as
    `isMetadataPrimaryKeyConflict` in the existing exception path).
- [x] On mismatch, throw `TransactionConflictException` reusing the existing
  intervening-changes summary helper `getInterveningChangesSummary`
  (`JdbcDucklakeCatalog.java:1030`) and adding the specific stale entity to
  the message (e.g. `"column 99 was end-snapshotted (likely by an intervening
  ALTER TABLE DROP COLUMN) during INSERT against table 42"`). The thrown
  exception must propagate cleanly through `WriteTransactionRetry` —
  `hasTransactionConflict` (`JdbcDucklakeCatalog.java:1069`) already detects
  it. **Decide explicitly per case whether the new conflict should be
  retryable**: stale-column INSERT is *not* retryable (the fragment metadata
  is already wrong; retrying re-fails). If non-retryable, the check should
  rethrow as a distinct subclass or carry a flag so the retry loop bails
  out — otherwise we burn `MAX_RETRY_COUNT` attempts on a guaranteed-fail
  scenario. See the `Sleeper`-keyed retry policy in
  `WriteTransactionRetry.java:46`.
- [x] **`TestConcurrentInsertVsDropColumn`**: T2 plans a `commitInsert` with
  column-stats for column 99; harness parks T2's first attempt at
  `beforeWriteTransactionAction`; T1 commits `dropColumn(table, 99)`; T2
  releases, retries (lineage check fires on first attempt), retry's action
  runs to completion, **logical check** rejects the commit with a
  message that names column 99 specifically. This is the gold acceptance
  test — it's the case lineage-only checking misses.
- [x] **`TestConcurrentInsertVsDropTable`**: same pattern with a
  `dropTable`. Confirms `tableId`-validity branch of the matrix.
- [x] **`TestConcurrentAlteredTableVsAlteredTable`**: two concurrent
  `addColumn` calls on the same table. Both should not silently land —
  expected behavior is that the loser's retry re-runs and the second
  `addColumn` succeeds against the post-winner schema (column_order
  collision is the failure mode if it doesn't). Pin whichever behavior is
  correct after Step 3 lands.

### Step 4 — Port upstream's change-vs-change conflict matrix — DONE (2026-05-10)

**Landed**: `InterveningChanges` (parser + aggregator), `ConflictMatrix`
(direct port of `ducklake_transaction.cpp:1184–1314`), wiring in
`attemptWriteTransaction` (`runConflictMatrix` + finer-grained
delete-vs-delete file-overlap query mirroring upstream's
`GetFilesDeletedOrDroppedAfterSnapshot` path at upstream `:1259–1283`).
The matrix runs only when intervening commits exist (i.e. on retry,
matching upstream's `i > 0` gate). `WriteChange.{CreatedTable,
CreatedView, DroppedSchema}` gained additional fields (schemaId / name)
not serialized to changes_made but needed by the matrix. Acceptance
tests: `TestConcurrentCreateSchemaSameName`,
`TestConcurrentCreateTableSameName`,
`TestConcurrentCreateTableInDroppedSchema`,
`TestConcurrentDeleteVsDelete`. `TestInterveningChangesParser` covers
quoting / escape / all-upstream-kinds round-trip. The behavior pinned
in Step 3's `TestConcurrentAlteredTableVsAlteredTable` was flipped to
match upstream's `altered_table × altered_table` conflict policy
(`:1307–1310`).

After reading the upstream source (vendored at
`temp/pg_ducklake/third_party/ducklake/src/storage/`), my earlier framing of
this step ("relax the strict lineage check") was wrong. The remaining gap to
upstream parity is the **change-vs-change matrix**, not the lineage fence.

**Upstream's actual flow** (`ducklake_transaction.cpp:2460–2477`):
```cpp
for (idx_t i = 0; i < max_retry_count + 1; i++) {
    can_retry = false;
    if (i > 0) {
        // we failed our first commit due to another transaction committing
        // retry - but first check for conflicts
        commit_stats_snapshot = CheckForConflicts(transaction_snapshot, transaction_changes);
    } else {
        commit_stats_snapshot.snapshot = GetSnapshot();
    }
    commit_snapshot.snapshot_id++;          // bump and let PG fence on PK
    ...
    can_retry = true;                       // matrix passed — INSERT below may PK-collide
```

- **First attempt**: just bump `snapshot_id` and INSERT. The
  `ducklake_snapshot` PK collision is the fence; PG rejects on duplicate.
- **Retry attempts**: parse intervening commits' `changes_made` text into a
  typed `SnapshotChangeInformation` struct, run the matrix in
  `CheckForConflicts` (`ducklake_transaction.cpp:1184–1314`). If matrix
  throws, `can_retry` is still `false` (it only flips true after the matrix
  passes), so the retry loop bails — that's their non-retryable mechanism.

Our `ensureSnapshotLineageUnchanged` is **functionally equivalent** to
upstream's PK-on-snapshot-id fence — both fail attempt 1 when intervening
commits exist. There's nothing to "relax". The only difference is: upstream
runs the matrix on retry to catch interleavings where the *retry's action*
itself would commit semantically incompatible state.

**Why the matrix is essential** — the upstream metadata DDL
(`ducklake_metadata_manager.cpp:196–217`) deliberately omits PKs on
`ducklake_table.table_id`, `ducklake_view.view_id`, and stat tables, because
rows are snapshot-versioned (same `table_id` recurs across rename / column
ops). There's no DDL constraint that would catch dueling
`createSchema("foo")` / `createSchema("foo")` or
`createTable("S", "T")` / `createTable("S", "T")`. The matrix is the
**only** safety net. We currently lack it.

**Pre-existing bug exposed by the matrix audit**: two concurrent
`createSchema(name)` (or `createTable(schema, name)`) calls with the same
name today both land active rows. The lineage check fences attempt 1 →
retry → on retry our `createSchema` blindly INSERTs without re-checking
for an active conflicting name. State-based `LogicalConflictCheck` skips
`Created*` variants. `PublicDbKeys.java` confirms only single-column
PKs — no `(schema_id, table_name)` UNIQUE. Upstream catches this in the
matrix at `ducklake_transaction.cpp:1212–1242`; we don't.

**Step 4 work items** (port mechanically — upstream is the spec):

- [x] **Add `ChangesMadeParser`** — inverse of `WriteChange.formatChangesMade`.
  Parse the `ducklake_snapshot_changes.changes_made` text from intervening
  snapshots back into `List<WriteChange>`. Upstream's `ParseChangesList` /
  `ParseChangeEntry` / `ParseChangeValue` (in
  `ducklake_transaction_changes.cpp:34–129`) is the spec; track quoting
  with a 1-pass state machine, splitting on unquoted commas. ~80 lines +
  parser tests covering quote-escaping and embedded-comma cases (mirror our
  existing format tests in `TestJdbcDucklakeCatalogChangesMadeFormat`).
- [x] **Aggregate intervening changes** into a structured equivalent of
  upstream's `SnapshotChangeInformation` (sets keyed by `(schema_id,
  name)` for create-by-name forms, sets of `tableId` for the rest). Add
  alongside `WriteChange` — call it e.g. `InterveningChanges`.
- [x] **Port `CheckForConflicts(my_changes, intervening_changes)`** — direct
  translation of `ducklake_transaction.cpp:1184–1314`. Each `for` loop in
  upstream maps to one Java loop checking the same set membership. Throw
  `LogicalConflictException` (already exists, already non-retryable) with
  upstream's error-message shape ("attempting to X — but another
  transaction Y'd it"). The macro pairs to translate, in order:
  - `dropped_tables × dropped_tables`
  - `dropped_views × dropped_views`
  - `dropped_scalar_macros × dropped_scalar_macros` (skip — we don't
    support macros yet)
  - `dropped_table_macros × dropped_table_macros` (skip — same)
  - `dropped_schemas × dropped_schemas`, `dropped_schemas.name ×
    created_tables[in that schema]`
  - `created_schemas × created_schemas` (the dueling-name bug)
  - `created_tables × dropped_schemas`, `created_tables × created_tables`
    (the other dueling-name bug)
  - `tables_inserted_into × dropped_tables`, `× altered_tables`
  - `tables_deleted_from × dropped_tables`, `× altered_tables`,
    `× tables_merge_adjacent`, `× tables_rewrite_delete`
  - `tables_deleted_from × tables_deleted_from` — finer-grained: only
    conflict if the same `data_file_id` is in both (upstream calls
    `metadata_manager->GetFilesDeletedOrDroppedAfterSnapshot` and
    intersects). We already capture `referencedDataFileIds` on
    `WriteChange.DeletedFromTable`, so this is a Set intersection.
  - `altered_tables × dropped_tables`, `× altered_tables`
  - `altered_views × altered_views`
  - Compaction / inline-flush rows are roadmap (see M8); add the matrix
    entries when those changes are emitted.
- [x] **Wire the matrix into `attemptWriteTransaction`**. Run it on retry
  attempts only, mirroring upstream — when `attemptCount > 0`, fetch
  intervening `changes_made` rows between `baseSnapshotId` and
  `currentSnapshotId`, parse, run matrix. State-based
  `LogicalConflictCheck` stays — it's a strictly stronger check on
  per-call args (column / data-file IDs) that upstream doesn't have, and
  we already pass tests against it.
- [x] **Acceptance tests** (one per matrix gap closed):
  - `TestConcurrentCreateSchemaSameName`: two `createSchema("foo")` →
    loser fails non-retryable matrix conflict naming the schema.
  - `TestConcurrentCreateTableSameName`: two `createTable("S", "T")` →
    same.
  - `TestConcurrentCreateTableInDroppedSchema`: T1 drops schema S
    (empty); T2 creates table S.foo → T2 fails matrix conflict.
  - `TestConcurrentDeleteVsDelete`: two deletes targeting the same
    `data_file_id` → loser fails matrix conflict (other delete-vs-delete
    pairs on different files commit cleanly).
- [x] **Reuse existing test harness**. `ConcurrentWriterHarness` works as-is
  for all four; the only diff vs Step 3 acceptance tests is that the
  matrix throws, not the state-based check.

**Estimate**: ~3–4 days end-to-end. The parser is a half-day; the matrix
translation is a day; tests are a day; review-and-polish is a day.
Mostly mechanical because upstream is the source of truth — no design
decisions, just translation.

## M8: Maintenance Operations

- [ ] Stats maintenance utilities:
  - `recalc stats` procedure(s) to rescan active data files and recompute
    table/column stats into catalog metadata.
  - Decoupled from rewrite/merge; callable independently as maintenance.
  - Regression tests for recompute-after-delete and recompute-after-schema-evolution
    snapshots.
- [ ] Maintenance verbs that map to Trino conventions:
  - `ALTER TABLE ... EXECUTE optimize` (DuckDB `merge_adjacent_files` equivalent)
  - `ALTER TABLE ... EXECUTE rewrite_data_files`
- [ ] Connector procedures in `ducklake.system`:
  - `expire_snapshots`
  - `cleanup_old_files`
  - `remove_orphan_files`
  - `flush_inlined_data`
- [ ] Result tables from procedures (rows affected / files deleted / bytes reclaimed).

Suggested kickoff: `expire_snapshots` + `remove_orphan_files` (both catalog-driven,
don't require a compaction engine).

## Commit-Failure File Cleanup

Files written before a failed commit become orphans. Today this is delegated to
DuckLake's `ducklake_delete_orphaned_files()` maintenance procedure rather than
handled inline. Once the M8 maintenance verbs land, this becomes self-served from
the connector.

## Design Decisions

### Strict stats invalidation, no threshold heuristics

Stats policy is intentionally conservative ("don't be wrong"):

- Snapshots with delete files return unknown table/column stats.
- Snapshots with mixed inline + Parquet rows keep row count but suppress file-derived
  column stats.
- Schema-evolved columns whose file-level `value_count + null_count` doesn't cover all
  active data-file rows have column stats suppressed.

Why not a `% changed > N` threshold like Iceberg-style engines that keep stale
manifest stats and rely on `OPTIMIZE`/`ANALYZE` to refresh? Cross-engine delete
semantics and stale catalog metadata can make threshold-based stats unsafe — for
DuckLake interoperability, prefer "unknown" over "potentially wrong" at read time.
Refresh path is the planned `recalc stats` utility under M8 above.

## Reality Check: Spec vs Actual Catalog Shape

Known differences between the markdown spec and DuckDB-generated catalogs (all
handled by the connector):

- `ducklake_schema_versions` has `table_id` in practice.
- `ducklake_column` has extra columns (`default_value_type`, `default_value_dialect`)
  in practice.
- `ducklake_snapshot_changes.changes_made` values include forms like
  `inlined_insert:...`, `inline_flush:...`, `merge_adjacent:...`.
- Temporal partition values follow the DuckLake 1.0 calendar contract (resolved by
  spec PR [duckdb/ducklake-web#349](https://github.com/duckdb/ducklake-web/pull/349)).
  A deprecated epoch path is kept behind `ducklake.temporal-partition-encoding=epoch`
  for legacy catalogs; see
  [REPORT_DUCKLAKE_PARTITION_PROB.md](REPORT_DUCKLAKE_PARTITION_PROB.md).

See [REPORT_CROSS_ENGINE_WRITE.md](REPORT_CROSS_ENGINE_WRITE.md) for spec issues filed
with the DuckDB team.
