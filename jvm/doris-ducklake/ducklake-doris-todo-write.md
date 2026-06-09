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

## Built + tested this session (commit path — steps 4 & 5) — GATE-CLOSED

- [x] **`DuckLakeConnectorTransaction`** (`ConnectorTransaction`): `addCommitData`
  decodes `TIcebergCommitData` (TBinaryProtocol); `commit()` maps + calls
  `catalog.commitInsert`; `getUpdateCnt`/`rollback`/`close`. Mirrors
  `MaxComputeConnectorTransaction`.
- [x] **`DuckLakeIcebergCommitMapper`**: `TIcebergCommitData` → `DucklakeWriteFragment`.
  Stats keyed by field-id (== DuckLake `column_id`); `value_count` = Iceberg
  total − null; **min/max decoded only for spec-locked types** — `int8/16/32/64`
  (LE) and `varchar` (UTF-8), `null` otherwise (a wrong bound could wrongly prune a
  file → correctness bug; a null bound is safe).
- [x] **Tests (+7)**: mapper unit (decode + count math + unknown-column drop +
  partition values) and an integration test committing a synthetic fragment to the
  real Postgres catalog, proving the new snapshot's decoded stats drive **read-path
  pruning** end-to-end (`findDataFileIdsInRange`).
- **GATE:** `DuckLakeConnectorMetadata.supportsInsert()` stays **false** — nothing
  routes a real INSERT here yet (exactly how Doris staged MaxCompute pre-cutover).

## Remaining for INSERT — needs a live BE + FE to validate

### W2a — the sink + engine wiring (codeable now, but un-runnable without a BE)
- [ ] **`DuckLakeWritePlanProvider`** (`ConnectorWritePlanProvider.planWrite`): build
  the `TIcebergTableSink`. The crux is `schema_json` — Doris's native
  `IcebergTableSink` does `SchemaParser.toJson(icebergTable.schema())`, i.e. it needs
  an `org.apache.iceberg.Schema`. We must **construct an `org.apache.iceberg.Schema`
  from the DuckLake columns with `field_id == column_id`** (and matching nested
  ids), then `SchemaParser.toJson`. Also set `db_name`/`tb_name`, `output_path` =
  resolved table data dir (`DuckLakePathResolver`), `file_format=parquet`,
  `overwrite`, `partition_specs_json`/`partition_spec_id`. Mirror
  `fe/fe-core/.../planner/IcebergTableSink.java`. (Check whether `org.apache.iceberg`
  is on the plugin classpath; if not, add it or hand-serialize the schema JSON.)
- [ ] Wire `DuckLakeConnector.getWritePlanProvider()` + metadata `beginTransaction`
  / `usesConnectorTransaction` / **flip `supportsInsert()=true`** — all behind the
  smoke-test gate below.

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
