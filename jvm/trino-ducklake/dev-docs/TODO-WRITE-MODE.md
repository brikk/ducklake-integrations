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

## Concurrency Test Coverage

- [ ] **Writer-vs-writer isolation test on the same table.** Two Trino sessions
  doing concurrent INSERTs; one must abort with `TransactionConflictException`,
  the other must commit cleanly. pg_ducklake has this as a `pg_isolation` spec
  and we have zero coverage for it today.
- [ ] **Writer-vs-writer isolation test across two tables.** Two sessions, each
  inserting into a different table; both commits must succeed. Catches
  snapshot-id cross-talk. Source: pg_ducklake's
  `concurrent_cross_table_writes` spec.

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
