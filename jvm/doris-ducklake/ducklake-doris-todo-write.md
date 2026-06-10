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
- [x] **`DuckLakeWritePlanProvider`** + metadata `beginTransaction` /
  `usesConnectorTransaction` / `supportsInsert=true` + `getWritePlanProvider` +
  `SUPPORTS_INSERT` cap — the full FE INSERT path is wired and the gate is flipped.
  planWrite builds the `TIcebergTableSink` (schema_json + db/table/parquet/output_path/
  hadoop_config/file_type/overwrite) and binds the target onto the txn. FE-tested
  (sink fields + schema round-trip); **end-to-end is the compose smoke**.
- **GATE now:** the real INSERT route requires fe-core's
  `CatalogFactory.SPI_READY_TYPES` to include `"ducklake"` (FE-build change, not ours)
  — see [[doris-fe-build-macos]]. The connector side is complete.

## Remaining for INSERT — run the smoke, then fix what it surfaces

### W2a — the sink + engine wiring ✅ DONE
- [x] `DuckLakeIcebergSchema` (schema_json), `DuckLakeWritePlanProvider` (sink),
  metadata write methods, `getWritePlanProvider`, `SUPPORTS_INSERT`, gate flipped.

### W2b — end-to-end smoke ✅ VALIDATED GREEN (2026-06-09, live FE+BE)
Ran `compose/smoke.sh` end-to-end on a real cluster: Doris `INSERT INTO
dl.tpch.doris_w VALUES (…)` → BE Iceberg sink wrote Parquet → connector committed a
DuckLake snapshot → **read back 3 rows through Doris AND through DuckDB+DuckLake**
(cross-engine). Every worried-about unknown held:
- [x] **Parquet field-ids** — DuckDB read it back, so the BE stamps `field_id ==
  column_id`. No `nameMap` needed.
- [x] **footer_size** — passing `0` (→ NULL) is tolerated by the reader.
- [x] **path** — the BE returns an absolute `s3://` path; `DuckLakeIcebergCommitMapper`
  now relativizes it against the table data dir (fixed the "doubled path" failure).
- [x] **compression** — set `ZSTD` (the BE rejects `UNKNOWN`).
- [x] **sink-prep caps** — `SUPPORTS_INSERT` alone sufficed; no schema-order issue.
- [ ] **min/max stat decode** — still `int8/16/32/64` + `varchar` only; extend to
  `date`/`float64`/`decimal`/`timestamp` once each type's DuckLake stat-string form
  is pinned (only affects write-side *pruning* coverage, not correctness).

**Standing the remote smoke up** (an Apple-Silicon dev box differs): see the
`doris-compose-smoke-remote` memory — FE built from P-series + `SPI_READY_TYPES +=
"ducklake"`, imaged via `docker/runtime/doris-fe-overlay`, BE forced amd64
(`DORIS_BE_PLATFORM`), `docker`→`podman` shim.

### W2c — partitioned / BUCKET writes ✅ VALIDATED GREEN end-to-end (2026-06-09)
FE half built + unit-tested against independent oracles, **and the live BE
bucket-equivalence is now confirmed** on a real cluster.
- [x] **`DuckLakeIcebergPartitionSpec`** — the table's active DuckLake partition spec
  → `org.apache.iceberg.PartitionSpec` (IDENTITY / YEAR / MONTH / DAY / HOUR /
  BUCKET(arity); fields added in `partition_key_index` order; source columns named by
  `column_id == field_id`). *Oracle: iceberg's own `PartitionSpecParser` round-trip.*
- [x] **Sink wiring** — `DuckLakeWritePlanProvider` sets `partition_specs_json`
  (`{specId: PartitionSpecParser.toJson}`) + `partition_spec_id` for partitioned
  tables, unset for unpartitioned — exactly like native `IcebergTableSink`. Tested
  against the real catalog's `by_region` (IDENTITY) and `by_name_bucket` (BUCKET(4)).
- [x] **Commit side** — the FE binds the DuckLake `partition_id` onto the txn;
  `DuckLakeIcebergCommitMapper` stamps it on each fragment and maps the BE's
  positional `partition_values` → `partition_key_index`. A partitioned commit to the
  real catalog (`by_region`) is exercised end-to-end (headless).
- [x] **LIVE bucket-equivalence — GREEN.** Ran the compose smoke against a
  `bucket(4, name)` target: Doris `INSERT` alice/bob/charlie → the BE tagged the files
  with buckets **exactly `{1,2,3}`** (== DuckLake's murmur3 / `DuckLakeBucketTransform`),
  DuckDB+DuckLake read them back cross-engine, and Doris's own `WHERE name='alice'`
  bucket-prune found the row. Repeatable via the `smoke.sh` **W2c** step (bucketed
  INSERT + catalog `ducklake_file_partition_value` assertion).

### W1 — DDL (CREATE/DROP DATABASE + TABLE) — connector side ✅ BUILT + headless-tested
Pure catalog metadata, no BE. The SPI exposes DDL as `ConnectorSchemaOps` /
`ConnectorTableOps` default methods, the FE routes them via `PluginDrivenExternalCatalog`
(resolving `IF [NOT] EXISTS` before the call), and the catalog already has the
`createSchema`/`dropSchema`/`createTable`/`dropTable` primitives (trino-ducklake drives
the same ones).
- [x] **`DuckLakeConnectorMetadata`** overrides `supportsCreateDatabase`/`createDatabase`/
  `dropDatabase`/`createTable(ConnectorCreateTableRequest)`/`dropTable` → catalog
  primitives; `SUPPORTS_CREATE_TABLE` capability added.
- [x] **`DuckLakeCreateTableMapper`** — Doris `ConnectorType` → DuckLake type string (the
  write-side inverse of `DuckLakeTypeMapping`; conservative — unsupported/nested types
  throw). *Oracle: round-trip against `DuckLakeTypeMapping.fromDucklakeType`.* Caught a
  real bug: DATETIMEV2's fractional-second resolution rides in `precision`, not `scale`.
- [x] **Tests (+8)** — type-mapper (forward + round-trip + unsupported-throws) and a
  catalog-backed DDL lifecycle (CREATE DATABASE → CREATE TABLE with typed columns →
  DROP TABLE → DROP DATABASE), plus partitioned-CREATE-TABLE + unsupported-type
  rejection. Doris suite 79→87 green.
#### W1b — partitioned CREATE TABLE + live DDL smoke (TURNKEY: shapes pre-discovered)

**(a) Partitioned CREATE TABLE — connector mapping. ✅ DONE + headless-green (2026-06-09).**
`DuckLakeConnectorMetadata.createTable` now maps `partitionSpec`/`bucketSpec` →
`catalog.createTable(db, table, columns, partitionFields, null)` via the new
**`DuckLakeCreatePartitionMapper`** (write-side inverse of `DuckLakeIcebergPartitionSpec`).
- **`request.partitionSpec` is the real partition path.** The FE
  (`CreateTableInfoToConnectorRequestConverter`) lowercases the transform fn name, so
  `PARTITIONED BY (bucket(16,c), year(d), region)` arrives as per-field
  `transform="bucket"/[16]`, `"year"`, `"identity"`. Map identity/year/month/day/hour/bucket →
  `DucklakePartitionTransform` (bucket arity = `transformArgs[0]`). **Reject LIST/RANGE styles and
  any other transform** (e.g. `truncate`) — conservative-throw, like the type mapper.
- **`request.bucketSpec` (Doris `DISTRIBUTED BY`) is NOT DuckLake bucketing.** Discovered while
  wiring: the FE only ever stamps its algorithm `"doris_default"` (CRC32) / `"doris_random"` —
  *never* murmur3. So mapping it to a DuckLake BUCKET would silently bucket by the wrong hash; we
  **reject any non-murmur3 algorithm** (pointing users at `PARTITIONED BY (bucket(N,col))`) and only
  accept an explicit `murmur3`/`iceberg_bucket` algorithm should a future FE pass one. The W2c
  murmur3-equivalence only holds for the Iceberg-transform path above.
- *Oracles (both green):* `DuckLakeCreatePartitionMapperTest` (pure-logic, every transform +
  rejections) and `DuckLakeDdlTest` (CREATE → `getPartitionSpecs` round-trip for bucket / identity+
  temporal, + DISTRIBUTED-BY / LIST / truncate rejection). **Doris suite 87→96 green.**

**(b) Live DDL smoke. ✅ DONE + VALIDATED GREEN end-to-end (2026-06-10).**
`compose/smoke.sh` now runs a live **W1 DDL** step: Doris `CREATE DATABASE` →
`CREATE TABLE doris_ddl (id INT, name STRING)` → cross-verify (DESC + `ducklake_column`
catalog + DuckDB+DuckLake) → `INSERT` + cross-engine read-back → **partitioned**
`CREATE TABLE … PARTITION BY LIST (bucket(4, name)) ()` (catalog records `bucket(4)`) →
`DROP TABLE`/`DROP DATABASE`. The **DuckDB-create crutch is removed**: W2/W2c targets
are now stood up by Doris `CREATE TABLE` (`doris_w` plain, `doris_wb` bucket-partitioned),
and both still round-trip GREEN cross-engine.

Two FE-route gaps surfaced and were fixed to get here (both in
[`ducklake-doris-friction.md`](./ducklake-doris-friction.md), 2026-06-10; FE patches in
[`fe-patches/`](./fe-patches/FE-PATCHES.md)):
- **FE engine-padding:** `CreateTableInfo.pluginCatalogTypeToEngine` only mapped
  `"max_compute"`, so plugin `CREATE TABLE` threw *"Current catalog does not support
  create table"* before reaching the connector. Fixed by padding `ENGINE_ICEBERG` for
  `"ducklake"` (DB-level DDL was already routed). Read path untouched.
- **Partition style:** live Doris stamps external partitioned `CREATE TABLE` as
  `Style.LIST`/`RANGE` (transform in the field), not `Style.TRANSFORM`. The connector
  (`DuckLakeCreatePartitionMapper`) now maps by per-field transform regardless of style;
  tests updated (`DuckLakeCreatePartitionMapperTest`, `DuckLakeDdlTest`).

**Env recipe (so a fresh agent is turnkey):** all compose images are cached → cluster bring-up ~2–3 min.
- *Headless tests:* `PATH=/opt/podman/bin:$PATH DOCKER_HOST=unix:///var/run/docker.sock
  TESTCONTAINERS_RYUK_DISABLED=true ./gradlew :doris-ducklake:test` (Testcontainers Postgres).
- *Smoke:* `/tmp/dockershim/docker` → podman shim, `DORIS_BE_PLATFORM=linux/amd64` (the arm-under-emulation
  BE crash), then `./smoke.sh`. See [[doris-compose-smoke-remote]] + `compose/README.md`.

## Phased plan

- [x] **W1 — DDL** (`CREATE/DROP DATABASE/TABLE`): ✅ **VALIDATED GREEN end-to-end** on a
  live FE+BE (W1b(b)). CREATE/DROP DATABASE + CREATE/DROP TABLE (unpartitioned +
  **bucket-partitioned**, scalar columns) route FE→connector→DuckLake and cross-verify via
  DuckDB+DuckLake and the catalog tables. Needed two FE patches —
  `pluginCatalogTypeToEngine` += `"ducklake"→ENGINE_ICEBERG` and a connector-side relax to
  accept the live `Style.LIST`/`RANGE` partition tagging (see W1b(b) + friction log 2026-06-10).
- [x] **W2 — INSERT (append, unpartitioned):** ✅ **VALIDATED GREEN end-to-end** on a
  live FE+BE — Doris writes a DuckLake Parquet file, reads back through Doris + DuckDB.
- [x] **W2c — INSERT (partitioned / BUCKET):** ✅ **VALIDATED GREEN end-to-end** — iceberg
  PartitionSpec → sink `partition_specs_json`/`partition_spec_id`, `partition_id` on the
  commit fragment; the live smoke confirmed the BE's bucket transform == DuckLake's
  (recorded buckets `{1,2,3}`). Only the date/decimal/float **stat-decode** extension
  remains (write-side pruning coverage, not correctness).
- [x] **W3 — CTAS** (`CREATE TABLE … AS SELECT`) = W1 DDL + W2 INSERT composed: ✅
  **VALIDATED GREEN end-to-end** on a live FE+BE — Doris creates the table from the
  SELECT schema + the BE writes the rows in one statement; round-trips through Doris
  and DuckDB+DuckLake (compose `smoke.sh` W3 step, INT32/VARCHAR source). **Caveat:**
  CTAS that infers a NARROW int (literal `1`→TINYINT) crashes the BE Iceberg writer —
  a BE serde/arrow-builder bug, not CTAS-specific (see friction log 2026-06-10); use
  INT/BIGINT or `CAST(… AS INT)`.
- [ ] **W4 — DELETE / UPDATE (merge-on-read):** position-delete files +
  `catalog.commitDelete`/`commitMerge`. **Gated** on the read-side delete blocker
  (READ todo Step 7, BE OPTIONAL-column position-delete rejection).
- [ ] **W5 — MERGE:** full upsert via the delete+insert fragment path.
- [x] **Cross-engine round-trip:** ✅ Doris-written `tpch.doris_w` read back by
  DuckDB+DuckLake on the same metadata DB (W2 smoke). The reverse (DuckDB-written
  read by Doris) is the existing read smoke.

## Reference

- **Execution template (live):** `MaxComputeConnectorTransaction` +
  `MaxComputeWritePlanProvider` + `MaxComputeConnectorMetadata` write methods
  (`fe/fe-connector/fe-connector-maxcompute/`).
- **Semantic template:** `trino-ducklake`'s `DucklakePageSink` /
  `DucklakeMergeSink` and its `catalog.commit*` calls — Doris reuses the **same**
  `catalog.commit*`; only the data-file-production half differs (Doris BE vs in-JVM).
- **Native sink to mirror:** `fe/fe-core/.../planner/IcebergTableSink.java`,
  `IcebergScanNode.java:772` (`SchemaParser.toJson`).
