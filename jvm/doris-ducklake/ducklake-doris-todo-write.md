# DuckLake-on-Doris — WRITE-path TODO

Sibling todos:
- 📖 [`ducklake-doris-todo.md`](./ducklake-doris-todo.md) — READ path (v1 essentially complete)
- 🔬 [`ducklake-doris-todo-research.md`](./ducklake-doris-todo-research.md) — research

## Where we stand (updated 2026-06-08)

**W0 (the write-feasibility spike) is DONE**, and the INSERT **commit path is built
and tested** (gate-closed). The picture changed materially when
[apache/doris#64253](https://github.com/apache/doris/pull/64253) (P4, "maxcompute
connector full adoption + live cutover") landed on `branch-catalog-spi`:

- **The BE→FE fragment-delivery gap is closed.** Pre-P4, a plugin INSERT would
  write orphaned Parquet that never reached the connector to commit. P4 added
  `ConnectorTransaction.addCommitData(byte[])` plus generic routing
  (`CommitDataSerializer.feed` → the txn looked up by id in
  `GlobalExternalTransactionInfoMgr`), so the BE's per-file commit fragments now
  reach the connector. **MaxCompute is the first live file-writing plugin** and the
  template; the P2 trino-connector migration is the read template.
- **Two enablers were already in place:** the `DucklakeCatalog` commit primitives
  (`commitInsert`/`commitAddFiles`/`commitDelete`/`commitMerge`, proven by
  `trino-ducklake`), and the SPI write surface (`ConnectorMetadata extends
  ConnectorWriteOps`).

See the `doris-write-insert-feasibility` project memory for the full eval.

## How a Doris plugin writes now (the P4 model)

INSERT into a plugin-catalog table flows:

1. **Capability + txn** — `ConnectorMetadata.supportsInsert()=true`,
   `usesConnectorTransaction()=true`, `beginTransaction(session)` returns a
   `ConnectorTransaction` (engine registers it by txn id).
2. **Sink plan** — `Connector.getWritePlanProvider().planWrite(session, handle)`
   returns a `ConnectorSinkPlan(TDataSink)`. For DuckLake that's a
   **`TIcebergTableSink`** (`TDataSinkType.ICEBERG_TABLE_SINK`) — chosen because the
   BE's Iceberg file-writer computes **per-field-id column stats from the Parquet
   footer** and reports them in `TIcebergCommitData` (the Hive sink's
   `THivePartitionUpdate` carries no stats). The plan also binds the target table
   onto the transaction.
3. **BE writes Parquet** + reports one `TIcebergCommitData` per file on the
   report-status RPC (`iceberg_commit_datas` channel).
4. **Fragment → connector** — routed generically to
   `ConnectorTransaction.addCommitData(byte[])`.
5. **Commit** — `commit()` maps the accumulated fragments to
   `DucklakeWriteFragment`s and calls `catalog.commitInsert` (new snapshot).

## Built + tested this session — GATE-CLOSED

**Testing reality (from the connector-SPI migration's own tests, RFC §15):** the
write path is almost entirely **headless FE-unit-testable** — txn open + global
registration, fragment routing, the commit-fragment `TBinaryProtocol` golden, and
**even the `TIcebergTableSink` field population** (built FE-side in `planWrite`,
asserted with `getXxx()`, no BE). Only the SQL `INSERT` round-trip through a real
backend needs a cluster (gated `regression-test` + a skip-by-default live test).
So the build principle here: **build (with tests) whatever has an independent
oracle headless; defer what only the BE can validate.**

Built with independent oracles:
- [x] **`DuckLakeConnectorTransaction`** (`ConnectorTransaction`): `addCommitData`
  decodes `TIcebergCommitData` (TBinaryProtocol); `commit()` maps + calls
  `catalog.commitInsert`; `getUpdateCnt`/`rollback`/`close`. Mirrors
  `MaxComputeConnectorTransaction`. *Oracle: the real Postgres catalog.*
- [x] **`DuckLakeIcebergCommitMapper`**: `TIcebergCommitData` → `DucklakeWriteFragment`.
  Stats keyed by field-id (== DuckLake `column_id`); `value_count` = Iceberg
  total − null; **min/max decoded only for spec-locked types** — `int8/16/32/64`
  (LE) and `varchar` (UTF-8), `null` otherwise (a wrong bound could wrongly prune a
  file → correctness bug; a null bound is safe).
- [x] **`DuckLakeIcebergSchema`**: DuckLake columns → `org.apache.iceberg.Schema`
  with `field_id == column_id` → `schema_json` for the sink. Scalar types only;
  nested/lossy types throw. *Oracle: iceberg's own `SchemaParser` round-trip.*
- [x] **Tests (+10)**: commit-mapper unit, the commit→catalog integration test
  (proves decoded stats drive read-path pruning end-to-end), a multi-fragment
  commit, and the schema round-trip.
- **GATE:** `DuckLakeConnectorMetadata.supportsInsert()` stays **false** — nothing
  routes a real INSERT here yet (exactly how Doris staged MaxCompute pre-cutover).

## Remaining for INSERT — needs a live BE + FE to validate

### W2a — the sink + engine wiring
- [x] **`schema_json`** — `DuckLakeIcebergSchema` (built + tested; see above).
- [ ] **`DuckLakeWritePlanProvider`** (`ConnectorWritePlanProvider.planWrite`):
  assemble the `TIcebergTableSink` around the schema — `db_name`/`tb_name`,
  `setSchemaJson(SchemaParser.toJson(DuckLakeIcebergSchema.of(cols)))`,
  `file_format=parquet`, `overwrite`, `output_path` = resolved table data dir
  (`DuckLakePathResolver`), `hadoop_config` = storage creds (mirror the read path's
  `getScanNodeProperties` location handling), `file_type` (S3 vs local), and bind
  the target onto the txn via `session.getCurrentTransaction()`. Mirror
  `fe/.../planner/IcebergTableSink.java:115-194`. **No independent headless oracle**
  for `output_path`/`hadoop_config`/`file_type` (their format is BE-defined) — so
  this is deferred to build alongside the W2b smoke rather than self-asserted blind.
  (`org.apache.iceberg` is on the classpath now — added compileOnly.)
- [ ] Wire `DuckLakeConnector.getWritePlanProvider()` + metadata `beginTransaction`
  (`= DuckLakeConnectorTransaction(session.allocateTransactionId(), catalog)`) /
  `usesConnectorTransaction=true` / **flip `supportsInsert()=true`** — behind the
  W2b gate.

### W2b — BE round-trip smoke (the actual validation; needs a running cluster)
- [ ] **Parquet field-ids**: confirm the BE Iceberg writer stamps `field_id ==
  DuckLake column_id`. If it assigns its own ids, the written file won't read back
  in DuckLake (columns "cannot be resolved") — fall back to a `nameMap` (the
  `commitAddFiles` mechanism) on the fragment.
- [ ] **footer_size**: `TIcebergCommitData` carries none → we pass `0` (catalog
  stores NULL). Verify the DuckDB/DuckLake reader tolerates a NULL footer size; if
  it crashes ("invalid footer length"), the BE must be extended to report it.
- [ ] **min/max format**: confirm the BE encodes bounds per Iceberg single-value
  spec (so our LE-int/UTF-8 decode is right), then **extend the decoder** to
  `date`/`float64`/`decimal`/`timestamp` — but only once each type's *DuckLake
  stat-string* format is pinned (DuckLake stores stats as strings the catalog
  parses; getting the string form wrong corrupts pruning).
- [ ] **path**: relative vs absolute from the BE; relativize against the data dir if
  the BE returns absolute (`DucklakeWriteFragment.pathIsRelative`).

### W2c — partitioned / BUCKET writes
- [ ] Verify the BE's Iceberg partition transforms match DuckLake's — esp. that the
  BE bucket (Iceberg `murmur3 % N`) equals `DuckLakeBucketTransform`, and that
  `partition_values` (positional) + `partition_spec_id` map to DuckLake's
  `partition_key_index` + `partition_id`. Unpartitioned INSERT is unaffected.

## Phased plan

- [ ] **W1 — DDL** (`CREATE/DROP SCHEMA`, `CREATE/DROP TABLE`): pure catalog
  metadata, no BE. Likely the cheapest first *live* write (no fragment round-trip).
- [~] **W2 — INSERT (append):** commit path ✅ built+tested; **W2a sink + W2b/W2c
  smoke remain.**
- [ ] **W3 — CTAS** = W1 DDL + W2 INSERT composed.
- [ ] **W4 — DELETE / UPDATE (merge-on-read):** position-delete files +
  `catalog.commitDelete`/`commitMerge`. **Gated** on the read-side delete blocker
  (READ todo Step 7, BE OPTIONAL-column position-delete rejection).
- [ ] **W5 — MERGE:** full upsert via the delete+insert fragment path.
- [ ] **Cross-engine round-trip:** Doris-written table read back by DuckDB (and
  vice-versa) on the same metadata DB — the integrity check.

## Reference

- **Execution template (live):** `MaxComputeConnectorTransaction` +
  `MaxComputeWritePlanProvider` + `MaxComputeConnectorMetadata` write methods
  (`fe/fe-connector/fe-connector-maxcompute/`).
- **Semantic template:** `trino-ducklake`'s `DucklakePageSink` /
  `DucklakeMergeSink` and its `catalog.commit*` calls — Doris reuses the **same**
  `catalog.commit*`; only the data-file-production half differs (Doris BE vs in-JVM).
- **Native sink to mirror:** `fe/fe-core/.../planner/IcebergTableSink.java`,
  `IcebergScanNode.java:772` (`SchemaParser.toJson`).
