# DuckLake-on-Doris — WRITE-path TODO

**Phase W of [`PLAN.md`](./PLAN.md)** ("write side Parquet") — collect
write-path items here. Sibling todos:
- 📖 [`TODO-read.md`](./TODO-read.md) — READ path (phase R)
- 🔬 [`TODO-research.md`](./TODO-research.md) — research

> **P6 baseline note (2026-07-05).** The write-framework unification on the P6
> baseline (`8b391c7`) rewrote the write SPI described below: `supportsInsert()`
> / `usesConnectorTransaction()` / `getWriteConfig()` and the per-op
> begin/finish/abort model are **gone**; `beginTransaction()` is the single
> mandatory entry, admission is `ConnectorWritePlanProvider.supportedOperations()`,
> and row-level DML / OVERWRITE / sorted writes / procedures are now generic
> capability-keyed seams. Read
> [`REPORT-doris-p6-iceberg-spi-cutover.md`](./REPORT-doris-p6-iceberg-spi-cutover.md)
> §1–2 before acting on P4-era mechanics in the sections below — the
> DELETE/UPDATE/MERGE track especially got dramatically cheaper (the iceberg MoR
> synthesis admits any connector declaring the ops).

## Where we stand (updated 2026-06-08, mechanics partially superseded — see note above)

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
  (recorded buckets `{1,2,3}`).
  - [x] **stat-decode extension (2026-07-07).** `DuckLakeIcebergCommitMapper.decodeBound`
    now decodes `float32`/`float64` (LE IEEE-754 → BigDecimal-parseable string) and `date`
    (LE int days → ISO-8601 `yyyy-MM-dd`) min/max in addition to int/varchar; non-finite
    floats (`NaN`/±Inf) and `decimal`/`timestamp`/unsigned/`int128` stay `null` (safe: no
    pruning). Oracle: `DucklakeStatTypes.parseStat`, the same parser the READ-path range
    prune calls on the stored strings. +2 headless tests. Extends write-side pruning
    coverage (a null bound was already correct, just non-pruning). Decimal deferred (needs
    the big-endian unscaled + scale, out of the fragment's reach here).
- [x] **W3 — CTAS** (`CREATE TABLE … AS SELECT`) = W1 DDL + W2 INSERT composed: ✅
  **VALIDATED GREEN end-to-end** on a live FE+BE — Doris creates the table from the
  SELECT schema + the BE writes the rows in one statement; round-trips through Doris
  and DuckDB+DuckLake (compose `smoke.sh` W3 step, INT32/VARCHAR source). **Caveat:**
  CTAS that infers a NARROW int (literal `1`→TINYINT) crashes the BE Iceberg writer —
  a BE serde/arrow-builder bug, not CTAS-specific (see friction log 2026-06-10); use
  INT/BIGINT or `CAST(… AS INT)`.
- [ ] **W2d — INSERT OVERWRITE — ⛔ TODO: pin the semantics before building.**
  The sink already carries `setOverwrite(handle.isOverwrite)`; the *missing* pieces
  are (a) admission — declare `OVERWRITE` in
  `DuckLakeWritePlanProvider.supportedOperations()` (default is `{INSERT}` only, so
  the engine rejects INSERT OVERWRITE before reaching us), and (b) the **catalog
  commit semantics**, which are NOT a blanket truncate.
  **Doris `INSERT OVERWRITE` is dynamic-partition-overwrite, not table-truncate:**
  - **Unpartitioned** → replace the whole table.
  - **Partitioned, no `PARTITION (...)` clause** → overwrite ONLY the partitions
    *touched by the incoming rows*; other partitions are untouched (dynamic
    partition overwrite).
  - **`PARTITION (p=…)` clause** → overwrite exactly the named partitions
    (`requiresMaterializeStaticPartitionValues()` / `validateStaticPartitionColumns()`
    are the P6 seams for the static-partition form).
  A truncate-then-insert would **over-delete** (wipe untouched partitions) — do NOT
  do it. The right primitive is a **single-snapshot** catalog op that end-snapshots
  only the live data/delete/inlined rows belonging to the *target partition set*
  and registers the new fragments atomically (mirrors `commitMerge`'s one-snapshot
  pattern; `truncateTable`'s clear logic is the unpartitioned special case). Blocked
  on: (1) documenting the exact target-partition-set derivation (dynamic from
  fragment `partition_id`s vs static from the clause), (2) a new conflict-matrix
  entry, (3) how the BE reports the touched partitions on the overwrite sink.
  Validation is the compose smoke + a headless catalog-commit test (the corpus /
  DuckDB oracle has no INSERT OVERWRITE — it's a Doris/Hive-ism).
- [ ] **W4 — DELETE / UPDATE (merge-on-read):** position-delete files +
  `catalog.commitDelete`/`commitMerge`. **De-risked 2026-06-24 (native amd64 probe).**
  Earlier this was marked "gated on the read-side delete blocker (Step 7, BE
  OPTIONAL-column rejection)" — the live probe shows that gate is **narrower than
  thought**: it only blocks reading *DuckDB/DuckLake-written* delete files (which DuckDB
  emits with OPTIONAL `(file_path, pos)`). **The Doris BE's own position-delete writer
  emits REQUIRED columns** — `be/src/exec/sink/writer/iceberg/viceberg_delete_file_writer.cpp`
  `build_position_delete_schema()` constructs both reserved-id fields with
  `NestedField(optional=false, …)`, i.e. spec-compliant REQUIRED. So a **Doris-issued
  DELETE writes a REQUIRED delete file the BE reads back fine** → W4 (Doris write +
  read-back) is end-to-end viable on this box **without** the BE read-strictness fix.
  That fix (widen `ScalarColumnReader<false,false>` to accept OPTIONAL-with-no-nulls,
  `be/src/format/parquet/vparquet_column_reader.cpp`) remains valuable for the
  *cross-engine* case (reading DuckDB-written deletes) but is **not a W4 blocker**.
  Probe specifics: full smoke (read + W1 DDL + W2/W2c INSERT + W3 CTAS) is GREEN
  end-to-end on native amd64; the Step-7 read of a DuckDB-written OPTIONAL delete still
  fails `[CORRUPTION]Not nullable column has null values` from `ScalarColumnReader<false,
  false>` (confirmed real, not an arm-emulation artifact). Remaining W4 work is the
  connector mapping: `supportsDelete`/`supportsMerge` + `beginDelete`/`finishDelete` →
  `catalog.commitDelete`, and `DuckLakeWritePlanProvider` emitting the BE iceberg
  delete/merge sink — headless-unit-testable (oracle: `catalog.commitDelete`, proven by
  trino), then a live smoke DELETE step.

  **⛔ CORRECTION — the real W4 gate is UPSTREAM fe-core, not the connector (probed
  2026-06-24, decisive).** Implementing the connector SPI delete methods would be
  **dead code**: fe-core has no plugin-driven delete/merge executor. Evidence:
  - `PluginDrivenInsertExecutor` is **insert-only** (`ConnectorInsertHandle` +
    `PluginDrivenTableSink`); there is **no `PluginDrivenDeleteExecutor`/`MergeExecutor`**
    anywhere in `fe-core/src/main`.
  - The SPI `ConnectorWriteOps.beginDelete`/`finishDelete`/`beginMerge`/`finishMerge` are
    invoked **only** by the **native** `IcebergDeleteExecutor`/`IcebergMergeExecutor`,
    which cast to `IcebergExternalTable` and call the native `IcebergTransaction`, not the
    connector SPI. `IcebergDeleteCommand.java:111` guards `if (!(table instanceof
    IcebergExternalTable))` → a `PluginDrivenExternalTable` (DuckLake) never routes there.
  - **Live confirmation:** `DELETE FROM dl.tpch.doris_w WHERE id=1` on the running cluster
    → `errCode=2 … delete command could be only used on olap table` (rejected at the FE
    before any connector call).
  This is the exact pre-P4 INSERT situation: INSERT only worked once
  [#64253 (P4)] landed `PluginDrivenInsertExecutor` + generic commit-fragment delivery.
  **W4 needs the analogous upstream contribution** — a `PluginDrivenDeleteExecutor`
  (+ Merge), command-dispatch routing plugin catalogs to it, and a plugin delete/merge
  sink — in apache/doris fe-core, either landed on `branch-catalog-spi` or carried as a
  (large, non-trivial) FE patch. Until then W4 is **upstream-blocked**, not connector
  work. The BE-side facts above (BE writes REQUIRED deletes; read-strictness only affects
  cross-engine OPTIONAL deletes) remain valid and become relevant once the executor exists.

  **⛔⛔ DEEPER GATE (probed 2026-06-24) — it is NOT just a missing executor; it is missing
  row-identity infrastructure.** Merge-on-read DELETE requires the scan to emit
  `(file_path, position)` per matched row so the BE can write position-delete files. The
  native path gets this from the **Iceberg `$row_id` metadata column**, which is
  `IcebergScanNode`-specific (`dataFile.firstRowId()`; `Column.ICEBERG_ROWID_COL` /
  `GLOBAL_ROWID_COL`; `IcebergNereidsUtils.injectRowIdColumn`). For plugin catalogs this
  **does not exist anywhere**: `PluginDrivenScanNode` has no row-id/position column, and
  the connector SPI has no row-identity concept. So a plugin-driven DELETE needs, in
  addition to the FE executor/command/sink classes:
    1. **SPI**: a way for the connector to declare data-file row identity and for the scan
       to project a `(file_path, ordinal-position)` reserved column.
    2. **FE `PluginDrivenScanNode`**: produce/propagate that reserved column + per-split
       first-row-id, mirroring `IcebergScanNode`.
    3. **BE**: the plugin/external scanner must emit file_path + ordinal position per row,
       and the plugin delete-sink path must be wired (today only `IcebergScanNode` feeds
       the position-delete sink).
  This is a **major multi-component effort spanning FE planner + connector SPI + BE
  scanner/sink** — effectively building merge-on-read row-identity for plugin catalogs,
  not a localized fe-core change. Architecture map + the ~7 new FE classes / ~7 modified
  dispatch sites are catalogued in the 2026-06-24 session notes, but they all sit on top
  of this absent row-identity layer. **Recommendation: treat W4 as an upstream design RFC
  (row-identity for plugin/external merge-on-read), coordinated with the Doris team, not
  a connector-side task.** EQUALITY_DELETES (predicate-based, no positions) was considered
  as a position-free alternative but does not map to DuckLake's position-based delete model
  (`ducklake_delete_file` = file_path + pos).
- [ ] **W5 — MERGE:** full upsert via the delete+insert fragment path.
- [x] **Cross-engine round-trip:** ✅ Doris-written `tpch.doris_w` read back by
  DuckDB+DuckLake on the same metadata DB (W2 smoke). The reverse (DuckDB-written
  read by Doris) is the existing read smoke.

## Maintenance procedures (P6 `getProcedureOps` seam)

- [x] **`expire_snapshots` — connector side BUILT + headless-tested (2026-07-07).**
  `ALTER TABLE <t> EXECUTE expire_snapshots(...)` routes generically through the P6
  `Connector.getProcedureOps()` → [`DuckLakeProcedureOps`] (`SINGLE_CALL` mode; no
  distributed rewrite driver needed — it's a metadata-only catalog transaction).
  Drives the existing catalog primitives `listExpirableSnapshots(olderThan, versions)`
  + `expireSnapshots(ids) → ExpireSnapshotsResult` (which only *schedules* dead files
  for a later age-gated physical cleanup). Semantic template: trino-ducklake's
  `DucklakeExpireSnapshotsProcedure`.
  - **Arg contract** (Doris passes procedure args as `Map<String,String>`, so no real
    array type): `retention_threshold` (varchar, default `7d`), `snapshot_ids`
    (comma-separated bigints, mutually exclusive with retention), `dry_run` (bool,
    default false). Keys case-insensitive. Retention floored by catalog prop
    `maintenance.min-retention` (default `7d`); explicit `snapshot_ids` bypass the floor.
  - **Catalog-wide vs table-scoped (documented + surfaced, not faked):** DuckLake
    snapshots are catalog-wide, but Doris only offers `ALTER TABLE t EXECUTE`. Runs
    catalog-wide, ignores the named table for selection, and the result's `scope`
    column says so. A WHERE/PARTITION clause is rejected loudly.
  - **Result:** one-row table `(dry_run, scope, expired_snapshot_count,
    scheduled_file_count, snapshot_ids)`.
  - **Tests:** +17 headless (`DuckLakeProcedureOpsTest`) — arg contract, duration
    parser, floor rejection before touching the catalog, dry-run-never-expires,
    mutual exclusion, conflict wrapping. Fake catalog is a throwing `Proxy` so an
    unexpected catalog call fails loud.
  - **⚠️ NOT yet live-smoked.** Headless-only; needs a compose `smoke.sh` step
    (`ALTER TABLE ... EXECUTE expire_snapshots(dry_run => true/false)` on a real FE+BE,
    cross-verify via the catalog's snapshot tables). Also note `expireSnapshots` is
    "not supported on the Quack backend yet" per the catalog contract — irrelevant to
    Doris (we use `JdbcDucklakeCatalog`/Postgres), but keep in mind for any Quack path.
- [ ] **`rewrite_data_files` (compaction) procedure** — the DISTRIBUTED counterpart
  (uses P6's free `ConnectorRewriteDriver` + `planRewrite`/`ConnectorRewriteGroup`).
  Bigger: needs the rewrite sink + partial-compaction back-dating
  (`rewriteDataFilesPartial`/`PartialMergedFile`). Trino template exists
  (`DucklakeRewriteDataFilesProcedure`).
- [ ] **`cleanup_old_files` / `remove_orphan_files`** — physical GC of the files
  `expire_snapshots` scheduled (`listFilesScheduledForDeletion` + `removeScheduledFileRows`)
  and orphan detection (`listReferencedFilePaths`). Needs BE/FS file deletion wiring.

## Reference

- **Execution template (live):** `MaxComputeConnectorTransaction` +
  `MaxComputeWritePlanProvider` + `MaxComputeConnectorMetadata` write methods
  (`fe/fe-connector/fe-connector-maxcompute/`).
- **Semantic template:** `trino-ducklake`'s `DucklakePageSink` /
  `DucklakeMergeSink` and its `catalog.commit*` calls — Doris reuses the **same**
  `catalog.commit*`; only the data-file-production half differs (Doris BE vs in-JVM).
- **Native sink to mirror:** `fe/fe-core/.../planner/IcebergTableSink.java`,
  `IcebergScanNode.java:772` (`SchemaParser.toJson`).
