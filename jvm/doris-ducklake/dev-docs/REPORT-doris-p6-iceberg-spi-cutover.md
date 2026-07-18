# REPORT: Doris P6 iceberg SPI cutover (`branch-catalog-spi` @ `8b391c7459d`) — what it unlocks for ducklake

Date: 2026-07-05. Analyzed commit: `8b391c7459d2358f187577fb5571313255815230`
"[refactor](catalog) P6 iceberg: migrate to catalog SPI + cutover + remove legacy
fe-core subsystem (#64688)" — tip of `origin/branch-catalog-spi`, parent
`494ec177d58` (P3b). ~685 files, +79,738/−23,744. The FE worktree
(`~/DEV/OSS/doris-catalog-spi`) was reset from the stale rebased P3b line to this
tip; `fe-patches/ducklake-fe.patch` was regenerated against it.

**TL;DR:** this is the commit we've been waiting for. The generic PluginDriven
bridge is no longer "jdbc/maxcompute-grade" — it is now the **full Iceberg-grade
path**: capability-keyed merge-on-read row-level DML, `WriteOperation`-dispatched
sinks, WRITE ORDERED BY, snapshot-pinned writes, branch writes, distributed
rewrite/compaction procedures, system tables, SHOW CREATE, ALTER
column/branch/tag/partition-field DDL — all reachable by an out-of-tree plugin
without any fe-core `instanceof` coupling. Since ducklake deliberately rides the
Iceberg shape (TIcebergTableSink, iceberg transforms, position deletes), nearly
every feature the Doris team built for their iceberg connector is now a seam we
can adopt by declaring it. The cost: `fe-connector-api` broke compatibility
(capability enum gutted, write-ops model rewritten) — 3 compile-break sites in
our connector, all fixed.

---

## 1. The gold: what the generic bridge can now do for an Iceberg-shaped plugin

These are **generic seams** (capability/provider-keyed, not `instanceof`-keyed).
Everything below is available to ducklake by implementing/declaring it — no FE
patch needed beyond our existing 2-liner.

### Write path (P6.3 write-framework unification)
- **Legacy fe-core iceberg sink stack deleted** (`IcebergTableSink`,
  `IcebergDeleteSink`, `IcebergMergeSink`, `BindSink.bindIcebergTableSink`, the
  nereids Logical/Physical iceberg sinks). `TIcebergTableSink` is now built
  *inside* `fe-connector-iceberg/IcebergWritePlanProvider.planWrite` and returned
  as an opaque `TDataSink` — exactly our model. We are on the surviving path.
- **`WriteOperation` enum** (INSERT/OVERWRITE/DELETE/UPDATE/MERGE/REWRITE)
  threaded through `ConnectorWriteHandle.getWriteOperation()`; one `planWrite`
  dispatches to `TIcebergTableSink` vs `TIcebergDeleteSink` vs
  `TIcebergMergeSink`.
- **Write admission = `Connector.supportedWriteOperations()`** (defaults to the
  write provider's `supportedOperations()`, default `{INSERT}`). INSERT OVERWRITE
  requires `OVERWRITE` in the set; DML requires `DELETE`/`MERGE`. Reference
  in-tree iceberg declares `{INSERT, OVERWRITE, DELETE, UPDATE, MERGE, REWRITE}`.
- **Row-level DML is capability-keyed, not type-keyed** (P6.3-T07):
  `RowLevelDmlRegistry` → `IcebergRowLevelDmlTransform.handles()` admits **any**
  `PluginDrivenExternalTable` whose connector declares DELETE/MERGE — a ducklake
  connector declaring those ops is *automatically* admitted into the iceberg
  MoR synthesis (position-delete/DV RowDelta-shaped plans). Per-table/mode
  rejection via `ConnectorWriteOps.validateRowLevelDmlMode()` — connector-authored
  errors. The DML shell requests synthetic columns via
  `ConnectorWritePlanProvider.getSyntheticWriteColumns()` (the
  `__DORIS_ICEBERG_ROWID_COL__` STRUCT) — this is our on-ramp for W-series
  DELETE/UPDATE work.
- **WRITE ORDERED BY**: `getWriteSortColumns()` → engine builds `TSortInfo` →
  handed back via `ConnectorWriteHandle.getSortInfo()` to stamp on the sink.
  (Our "sorted writes PARKED" item now has a first-class seam.)
- **Write distribution is provider-keyed**: `requiresParallelWrite()`,
  `requiresPartitionLocalSort()`, `requiresFullSchemaWriteOrder()`,
  `getWritePartitioning()` (`ConnectorWritePartitionSpec` — transform name +
  param + source column, i.e. our bucket/year/day transforms expressed
  neutrally).
- **MVCC-pinned writes**: the translator applies the scan's snapshot pin to the
  *write* handle so DML writes at the snapshot it read — free correctness for a
  MoR connector once `applySnapshot` semantics exist on the handle (ours do).
- **Static-partition INSERT**: `requiresMaterializeStaticPartitionValues()` +
  `validateStaticPartitionColumns()` — `PARTITION (col=val)` literals get
  materialized into data columns (Iceberg-style hidden partitioning) instead of
  NULL-filled.
- **Branch writes**: `supportsWriteBranch()` + `ConnectorWriteHandle.getBranchName()`.
- **Transactions unified**: `beginTransaction()` is the single mandatory write
  entry (default now **throws**); `NoOpConnectorTransaction` exists for
  auto-commit sinks. New defaults on `ConnectorTransaction`:
  `applyWriteConstraint()` (OCC write constraints via `WriteConstraintExtractor`),
  `profileLabel()`, `registerRewriteSourceFiles()`.

### Procedures + system tables (P6.4 / P6.5)
- `ALTER TABLE EXECUTE` routes any plugin table through generic
  `ConnectorExecuteAction` → `Connector.getProcedureOps()`
  (`ConnectorProcedureOps`, `ProcedureExecutionMode`, `ConnectorRewriteGroup`),
  with fe-core supplying a **distributed rewrite driver**
  (`ConnectorRewriteDriver`/`ConnectorRewriteExecutor`) — i.e. compaction /
  `rewrite_data_files` / expire-snapshots procedures are now a first-class
  generic seam. This maps directly onto our ducklake `expire_snapshots` /
  `rewrite` maintenance ambitions (F6-equivalents on the Doris side).
- System tables (`$snapshots` etc.) + time-travel on them gated by
  `ConnectorScanPlanProvider.supportsSystemTableTimeTravel()` (default false).

### Scan path
- **TCCL pinning**: every scan/write/DDL/commit crossing into the plugin is
  wrapped `onPluginClassLoader(...)`; paimon/iceberg add a connector-side
  `TcclPinningConnectorContext`. If our Postgres driver / jOOQ ever hits
  TCCL-sensitive failures in the FE, this is the sanctioned pattern to copy.
- **`ConnectorScanRange.isPartitionBearing()`** (default false): declare true +
  return metadata partition values and the BE stops hive-path-parsing our
  non-`key=value` file layout. We currently return `emptyMap()` — worth adopting
  for partitioned tables (iceberg does).
- **Streaming split source** (`streamingSplitEstimate()`/`streamSplits()` +
  `ConnectorSplitSource`) with backpressure — opt-in for big tables.
- **TopN lazy materialization** (`SUPPORTS_TOPN_LAZY_MATERIALIZE`) and **nested
  column pruning** (`SUPPORTS_NESTED_COLUMN_PRUNE`, requires per-field ids via
  `ConnectorColumn.withUniqueId` / `ConnectorType` child field-ids — DuckLake
  *has* stable column ids, so this is genuinely adoptable later).
- **Version-aware MVCC pins**: a statement mixing `t` and `t@branch(x)` gets
  distinct pins; `FOR VERSION AS OF '<name>'` (non-numeric) now arrives as new
  `ConnectorTimeTravelSpec.Kind.VERSION_REF` (our `else -> null` degrades it to a
  clean not-found; could map to DuckLake named refs someday).
- New handle-mutation hooks: `applyRewriteFileScope()` (distributed rewrite),
  `applyTopnLazyMaterialization()` — default no-ops.

### DDL / metadata
- **Full ALTER SPI** (P6.6-C5): rename table, add/drop/rename/modify/reorder
  columns, create/drop branch+tag, add/drop/replace partition field — all routed
  `PluginDrivenExternalCatalog` → `ConnectorTableOps` (default-throwing; adopt
  incrementally).
- **SHOW CREATE TABLE** gated by new `SUPPORTS_SHOW_CREATE_DDL` capability
  (replaces an engine-name whitelist); pre-render via reserved
  `ConnectorTableSchema` keys `show.location` / `show.partition-clause` /
  `show.sort-clause`. Don't declare it until we're sure `getTableProperties()`
  leaks no credentials (ours can include the PG metastore DSN — audit first).
- **Views** (`SUPPORTS_VIEW` + `ConnectorViewDefinition` + view ops).
- `CREATE TABLE ... ORDER BY (...)` sort order now flows into
  `ConnectorCreateTableRequest.getSortOrder()` (new `ConnectorSortField`).
- **`REFRESH CATALOG` now calls `Connector.invalidateAll()`** — we should make
  sure our snapshot/schema caches implement it (connector-side).
- **`Connector.deriveStorageProperties(rawProps)`**: connector-derived storage
  defaults folded into `CatalogProperty` before FE filesystem bind + BE storage
  map — the new seam if we ever derive `fs.defaultFS`-style props.
- **Auth flip**: fe-core no longer runs a pre-execution authenticator for plugin
  catalogs — Kerberos/doAs is the connector's job (fe-kerberos consolidation).
- New **`fe-connector-cache`** module (CacheFactory/CacheSpec/MetaCacheEntry,
  `meta.cache.*` catalog props) — opt-in library paimon/iceberg use; nothing
  forces adoption. Worth evaluating to replace hand-rolled caching.

## 2. Breaking changes we had to absorb

`fe-connector-api` is **not backward compatible** at this commit:

1. **`ConnectorCapability` gutted** (capability-unification): deleted
   `SUPPORTS_FILTER/PROJECTION/LIMIT_PUSHDOWN`, `SUPPORTS_PARTITION_PRUNING`,
   `SUPPORTS_INSERT/DELETE/UPDATE/MERGE`, `SUPPORTS_CREATE_TABLE`,
   `SUPPORTS_STATISTICS`, `SUPPORTS_TIME_TRAVEL`, `SUPPORTS_PARALLEL_WRITE`,
   `SINK_REQUIRE_*`, etc. Verified at the parent commit: **fe-core never consumed
   the deleted constants** — declarative-only, so removal is a compile break,
   not a behavior loss. Pushdown is attempted unconditionally now. Kept:
   `SUPPORTS_MVCC_SNAPSHOT` (still gates MVCC table creation — the one we need),
   `SUPPORTS_PASSTHROUGH_QUERY`, `SUPPORTS_PARTITION_STATS`.
2. **`ConnectorWriteOps` rewritten**: `supportsInsert()`,
   `usesConnectorTransaction()`, `getWriteConfig()`, the whole
   begin/finish/abort-per-op handle model, `ConnectorWriteConfig`,
   `ConnectorWriteType`, `ConnectorInsertHandle` — deleted.
   `beginTransaction()` is the single mandatory entry.
3. Write capability declaration moved to `ConnectorWritePlanProvider`
   (all default methods — additive for us).

**Our fixes (3 sites, all in `doris-ducklake`):**
- `DuckLakeConnector.getCapabilities()` → now declares only
  `SUPPORTS_MVCC_SNAPSHOT`.
- `DuckLakeConnectorMetadata.supportsInsert()` override → deleted (INSERT
  admission = write provider default `{INSERT}`).
- `DuckLakeConnectorMetadata.usesConnectorTransaction()` override → deleted
  (`beginTransaction()` unconditional; we already return
  `DuckLakeConnectorTransaction`).

Everything else we import (43 `org.apache.doris.connector.*` types + 12 thrift
types incl. `TIcebergTableSink`/`TIcebergCommitData`) is unchanged or
additive-only. **gensrc/thrift: zero changes.** Plugin discovery, classloader,
`plugin.conf`, `META-INF/services` mechanism: untouched.

## 3. Our FE patch: still needed, still 2 lines, regenerated

- `CatalogFactory.SPI_READY_TYPES` is now
  `{jdbc, es, trino-connector, max_compute, paimon, iceberg}` — still a
  hard-coded set, no connector-declared registration seam yet. Patch line 1
  (add `"ducklake"`) survives unchanged in shape.
- `CreateTableInfo.pluginCatalogTypeToEngine` gained `case "iceberg" →
  ENGINE_ICEBERG`; our `case "ducklake" → ENGINE_ICEBERG` still the right seam
  (javadoc explicitly says the switch must stay in sync — upstream ask stands).
- **New behavior our ENGINE_ICEBERG mapping now triggers**: ducklake CREATE
  TABLE consults catalog-level `table-default/override.format-version` props +
  `validateIcebergRowLineageColumns`, and `ORDER BY (...)` is now **accepted**
  and carried into the create request. Watch for iceberg-specific validation
  semantics that don't fit DuckLake; acceptable for now.
- No new Config/SessionVariable gates in the commit. The only gate remains
  `SPI_READY_TYPES` itself.

`ducklake-fe.patch` regenerated against `8b391c7` (verified
`git apply --check --reverse`). FE-PATCHES.md updated with the new baseline.

## 4. Build/publish pipeline consequence

`doris-ducklake/build.gradle.kts` resolves `org.apache.doris:fe-connector-api /
fe-connector-spi / fe-thrift` `1.2-SNAPSHOT` as `compileOnly` from
**`mavenLocal()`**. The jars in `~/.m2` predated this commit → after every FE
rebuild at a new SPI baseline we must re-`mvn install` those modules before
recompiling the connector, or we compile against a stale SPI and fail at FE load
with `NoSuchMethodError`. (Done as part of this rebuild.)

## 5. Rebuild + validation outcome (2026-07-05)

Everything re-validated live on the new baseline:

- FE worktree reset to `8b391c7`, 2-line patch re-applied,
  `ducklake-fe.patch` regenerated. `build.sh --fe` green (note: stale
  `fe-grpc/target` generated-sources from the pre-rebase line had to be
  cleaned first — proto classes moved).
- `fe-connector-api` / `fe-connector-spi` / `fe-thrift` re-installed to
  `~/.m2` — **must use `-P flatten`**, otherwise the installed POMs keep the
  literal `${revision}` parent version and Gradle can't parse them.
- Connector: 3 compile-break fixes applied (capabilities → only
  `SUPPORTS_MVCC_SNAPSHOT`; `supportsInsert()` / `usesConnectorTransaction()`
  overrides deleted), capabilities test rewritten to pin the new declaration.
  Build + full test suite + detekt green; plugin zip rebuilt.
- `doris-fe:pr62767-local` overlay image rebuilt from the new `output/fe`.
  (The `Build hash` the FE logs at boot still says the old commit — the
  generated `Version` class is stale metadata, not stale code; P6-only classes
  like `RowLevelDmlRegistry` are in the jar.)
- **Full compose smoke GREEN end-to-end**: reads, step-7 delete-file plumbing
  (reaches the documented BE parquet-nullability gap, unchanged), W1 DDL
  (CREATE/DROP DATABASE + TABLE, unpartitioned + bucket-partitioned), W2
  INSERT, W2c bucketed INSERT (murmur3 bucket equivalence 1/2/3), W3 CTAS.
- Two environment fixes discovered on the way, both folded into `smoke.sh`:
  (1) new FE plans `LOCAL_EXCHANGE_NODE` (thrift 38) which the 4.1.0 BE
  rejects — shimmed with `SET GLOBAL enable_local_shuffle_planner = false`
  (see friction log 2026-07-05); (2) BE platform now auto-detects amd64
  hosts instead of defaulting to arm64.

## 6. Recommended adoption roadmap (in rough order of value)

1. **Row-level DML (DELETE/UPDATE/MERGE)**: declare ops + implement
   `getSyntheticWriteColumns` (row-id STRUCT) + sink dispatch on
   `WriteOperation` + `validateRowLevelDmlMode`. This rides the same MoR
   machinery in-tree iceberg uses — our position-delete write path (F7 learnings)
   plugs straight in.
2. **`isPartitionBearing` + metadata partition values** on scan ranges
   (correctness hardening for partitioned tables).
3. **WRITE ORDERED BY** via `getWriteSortColumns` (unparks sorted writes).
4. **Procedures** (`getProcedureOps`): expire_snapshots / rewrite_data_files
   with the free distributed rewrite driver.
5. **`invalidateAll()`** on REFRESH CATALOG; evaluate `fe-connector-cache`.
6. Later: SHOW CREATE (`SUPPORTS_SHOW_CREATE_DDL` — after credential audit),
   nested column pruning (we have stable field ids), TopN lazy materialization,
   system tables + time travel, views, static-partition INSERT materialization.

---

## 2026-07-15 upstream re-check — where we last diffed `branch-catalog-spi`

`branch-catalog-spi` **rebases frequently, so the SHA below WILL go stale** —
match on the commit *message* (subject line), not the hash.

- **Last-reviewed tip:** `c0865b021b0`
  "[doc](catalog) HANDOFF: $position_deletes port done; e2e is the only
  remaining flip gate"
- **Baseline we diffed from (previous known-good reference point):**
  `c215c4c77c5`
  "[fix](build) fix parallel -T shade race in fe-connector-paimon-hive-shade"
- **Range reviewed:** `c215c4c77c5..c0865b021b0` — 5 commits, of which **4 are
  doc/HANDOFF** and exactly **one carries code**:
  - `2e49827ecdd` "[feat](catalog) P6.6: support the iceberg $position_deletes
    system table on the connector SPI" (ports upstream #65135 / `0814e49bea7`).

### Impact on our plugin: none that breaks us; one invariant we already satisfy

1. **SPI + thrift UNCHANGED.** Verified the whole range touches ZERO files in
   `fe-connector-api`, `gensrc`, or any `*.thrift`; every changed `.java` lives
   under `connector/iceberg/`. We compile against the SPI + thrift, so nothing
   forces a code change or a rebuild break on us.
2. **New load-bearing invariant — we already comply.** The feature makes the
   **top-level** `TIcebergFileDesc.content ∈ {1,3}` (+ PARQUET/ORC range) BE's
   sole routing key into the native `$position_deletes` reader, so "the data
   path must never set top-level content to 1 or 3." Our
   `DuckLakeScanRange.populateRangeParams` sets `content` only on the
   per-delete-file descriptors (`toThrift`), never on the top-level `fileDesc`.
   Recorded in CRUTCHES-AND-SHORTCUTS.md §2 as a do-not-violate constraint.
3. **System tables — N/A to us.** The metadata change drops the old
   "position_deletes not supported" filter in `IcebergConnectorMetadata`. Our
   `DuckLakeConnectorMetadata` implements no `SysTable`/`listSupportedSysTables`
   logic at all; those SPI methods are `default`, so we inherit the no-op and
   `t$position_deletes` against a DuckLake table just hits the generic
   not-found path. No regression, no adoption required.

**Verdict:** no action needed. Next re-check: diff from `c0865b021b0` (or, once
it rebases, from the commit whose subject is the "$position_deletes port done"
HANDOFF) forward, and re-confirm the top-level-`content` invariant still holds.

---

## 2026-07-18 upstream re-check — new SPI seams since `$position_deletes`

**Anchor (SHAs churn — match on subject):** last-reviewed = the *"$position_deletes
port done; e2e is the only remaining flip gate"* HANDOFF (was `c0865b021b0`, now
`4d46863b7f1` after a rebase). New tip = `b2dff681aad`. **7 new commits** in
`4d46863b7f1..b2dff681aad` (943 files — dominated by the Hive P11 migration).

New commits (by subject):
1. `[refactor](catalog) Catalog spi 11 hive (#65473)` — 791 files (hive → SPI, parallels P6 iceberg)
2. `[refactor](catalog) fe-connector: replace fat hive-catalog-shade with slim HMS metastore-client shade (#65721)`
3. `[refactor](catalog) fe-core: remove dead Trino-ported DirectoryLister cache (#65734)`
4. `[refactor](catalog) fe-core: remove dead ExternalMetadataOps layer, LakeSoul, odbc/statistics stubs (#65736)`
5. `[refactor](catalog) fe-core: remove 6 dead datasource classes orphaned by the SPI migration (#65740)`
6. `[refactor](catalog) drop ReauthenticatingRestSessionCatalog test orphaned by master rebase`
7. `[feat](catalog) fe-connector-iceberg: port #64966 REST 401 re-auth to the connector`

### Impact on us: SAFE — additive only

- **SPI api grew ~1,884 lines but is 100% additive.** Every new method on
  interfaces we implement (`ConnectorTableOps`, `ConnectorScanPlanProvider`,
  `ConnectorWritePlanProvider`, `ConnectorWriteOps`, `ConnectorMetadata`) is a
  `default`. **Zero removed / signature-changed methods** across the whole api.
  → our plugin compiles unchanged.
- **Wire format stable** — no `gensrc` / `*.thrift` changes.
- **FE patch = trivial re-diff (NOT a rewrite).** Both anchors survive:
  `CatalogFactory.SPI_READY_TYPES` still exists (upstream added `"hms"`; we still
  just append `"ducklake"`); `CreateTableInfo.pluginCatalogTypeToEngine()` switch
  survives and moved, gained a `case "hms"` — our `case "ducklake" -> ENGINE_ICEBERG`
  still slots in. **[TRACKED: TODO-read Upstream coordination / build hygiene.]**
- **Dead-code removals (#65734/#65736/#65740)** touch no class our plugin or FE
  patch references — no fallout.

### New doors (opt-in `default`s / new optional classes) — TRACKED as TODOs

| Seam (new SPI) | DuckLake opportunity | Prio |
| --- | --- | --- |
| `ConnectorMetadata.getTableFreshness()` → `mvcc.ConnectorTableFreshness` | **MTMV / materialized views over DuckLake** — DuckLake snapshot id+ts maps ~1:1 | HIGH |
| `ConnectorScanPlanProvider.ignorePartitionPruneShortCircuit()` | **correctness**: if we ever render a genuine-null partition as a non-null sentinel, must be `true` or `col IS NULL` drops null-partition rows | HIGH |
| `beginQuerySnapshot()` / `resolveTimeTravel()` → `mvcc.ConnectorMvccSnapshot` | formalize DuckLake time-travel (`FOR VERSION/TIME AS OF`), query-scoped snapshot pin serialized to BE | MED |
| `getMvccPartitionView()` → `mvcc.ConnectorMvccPartitionView` | range-aware partition MVCC for partition-aware MV refresh (after freshness) | MED |
| `ConnectorScanPlanProvider.collectScanProfiles()` → `scan.ConnectorScanProfile` | DuckLake scan metrics into Doris query profile | LOW |
| `ConnectorTableOps.truncateTable()` | `TRUNCATE TABLE` on DuckLake | LOW |
| `ConnectorTableOps.renderShowCreateTableDdl()` | `SHOW CREATE TABLE` for DuckLake | LOW |
| `ConnectorScanPlanProvider.supportsTableSample()` | `TABLESAMPLE` | LOW |
| `event.ConnectorEventSource` / `MetastoreChangeDescriptor` | (medium lift, HMS-shaped) poll DuckLake `ducklake_snapshot` commit log → cache invalidation / MV auto-refresh | LATER |

**Next re-check:** diff forward from the *"Catalog spi 11 hive"* / `b2dff681aad`
tip; watch for P7+ connectors and any first NON-`default` SPI method (would be our
first forced compile change).
