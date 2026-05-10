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

**Status (2026-05-10): Steps 1–3 landed.** Step 4 (relaxing the strict
lineage check) remains open and is scoped below. Files added:
`ConcurrentWriterHarness`, `WriteChange`, `LogicalConflictCheck`,
`LogicalConflictException`; new tests
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

### Step 4 — (deferred follow-up) Relax the strict lineage check

Out of scope for the conflict-matrix work above; tracked here so it's not
forgotten. Today `ensureSnapshotLineageUnchanged` rejects every intervening
commit, which forces a retry even when interleavings are semantically
compatible (e.g. concurrent INSERTs into different tables). Upstream DuckLake
allows compatible interleavings to commit without retry. To match:

- [ ] Remove or weaken `ensureSnapshotLineageUnchanged`; rely on the logical
  check from Step 3 plus the PK collision on the new snapshot row insert
  for atomicity.
- [ ] **Solve the ID-allocation race first**: `nextCatalogId` and `nextFileId`
  are read at attempt-start (`JdbcDucklakeCatalog.java:925`–`927`) and
  allocated lazily by `tx.allocateCatalogId() / allocateFileId()`. If we
  let intervening commits land, their ID consumption isn't visible to the
  in-flight transaction → PK collisions on `ducklake_table.table_id`,
  `ducklake_data_file.data_file_id`, etc. Options: (a) re-read max IDs
  before the snapshot insert and rewrite mutations to use offsets, (b)
  switch to DB-side sequences, (c) keep optimistic lineage check but
  scope it to "lineage advanced *and* affects me" via the typed
  `WriteChange` log on the intervening snapshot. Option (c) is the closest
  match to upstream behavior.
- [ ] Re-validate Step 1's pinning tests against the relaxed behavior:
  `concurrentInsertsBothCommit` on different tables should now commit
  *without* either side retrying.
- [ ] Performance check: under low contention, this should reduce snapshot
  churn and DB round-trips. Under high contention on the same hot table,
  retry counts should be unchanged.

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
