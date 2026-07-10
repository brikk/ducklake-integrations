# RESEARCH: can a Doris connector make out-of-band Hive/HMS calls (à la Trino `sync_partition_metadata`)?

Date: 2026-07-08. Question raised while comparing Doris to Trino's
`CALL hive.system.sync_partition_metadata('db','table','FULL')` — which reconciles
the Hive Metastore's partition list against what's physically on storage
(ADD/DROP/FULL), i.e. it picks up partitions written out-of-band (raw files dropped
into `.../key=value/` dirs) without a manual `ALTER TABLE ... ADD PARTITION`.

This note records the finding so we don't re-derive it. Two eras answer differently.

## TL;DR

- **Pre-SPI (released 2.x / 3.x / 4.0.x / 4.1.x) Hive catalog:** **NO.** No equivalent
  command, no plugin seam, and the HMS/storage credentials live in fe-core internals
  with no sanctioned accessor. You cannot add this without forking fe-core.
- **Post-SPI (branch-catalog-spi / the upcoming release we vendor):** **YES.** The
  connector SPI's `ConnectorProcedureOps.execute()` is exactly the seam, the connector
  owns its catalog properties/credentials, and `ConnectorContext` even vends Hive auth
  + storage credentials. This is the same mechanism we already shipped for
  `expire_snapshots` (see `DuckLakeProcedureOps`).

## Pre-SPI: why it's a hard no (verified 2026-07-08 against apache/doris)

1. **No filesystem-reconciling command exists.** No `MSCK REPAIR TABLE` keyword in
   fe-core; no `sync_partition_metadata`. `RECOVER PARTITION` exists but is OLAP
   internal-table-only (`RecoverPartitionCommand` calls `Util.prohibitExternalCatalog`)
   and recovers from the recycle bin, never touching HMS or storage.
2. **`REFRESH` is cache-invalidation, not storage reconciliation.** `REFRESH
   TABLE/CATALOG` invalidates Doris's cache and re-fetches **from HMS** — it only ever
   sees the partition list HMS already knows. Doris docs, verbatim: "If you bypass HMS
   and directly manipulate the file system, HMS will not generate corresponding events,
   and Doris will not detect metadata changes." So a partition on disk but not in HMS is
   invisible no matter how often you REFRESH.
3. **No plugin/extension point.** External catalogs (`HMSExternalCatalog`,
   `HiveMetadataOps`) are compiled into fe-core, not pluggable. The `CALL` mechanism is a
   **hardcoded switch** (`CallFunc.getFunc()` → only `EXECUTE_STMT`, `FLUSH_AUDIT_LOG`),
   not a per-connector procedure registry. UDFs are per-row query functions, not FE admin
   ops. `INSTALL PLUGIN` is for audit plugins. Adding the command = editing that switch +
   the reconcile logic **inside a forked fe-core**.
4. **Credentials aren't reachable.** The HMS URI, Kerberos/keytab, S3/HDFS creds live in
   fe-core objects (`HMSExternalCatalog`/`HiveConf`) with no public accessor for third
   parties. Not encrypted, just no sanctioned door — only fork or brittle reflection.

**Pre-SPI workaround (no Doris-side code):** register the out-of-band partitions in HMS
first with a tool that has HMS access — Hive/Spark `MSCK REPAIR TABLE db.t` (or explicit
`ALTER TABLE ... ADD PARTITION`) — then `REFRESH TABLE hive_cat.db.t` in Doris. Note:
when **Doris itself** writes partitions (INSERT/CTAS), it *does* register them in HMS via
`add_partition` (`HiveMetadataOps.addPartitions`); the gap is only for files dropped by
some other tool without calling HMS. (`enable_hms_events_incremental_sync` auto-updates
Doris's cache from HMS **events**, but still not from raw filesystem drops.)

## Post-SPI: yes — and this is the interesting part

A Hive-shaped **SPI connector** (a sibling of our DuckLake connector) can offer a
`sync_partition_metadata`-style maintenance procedure. Verified against the vendored SPI
jars (`fe-connector-api` / `fe-connector-spi` @ our re-vendor `3ba75b7cf8a`):

### The seam: `ConnectorProcedureOps`
```
List<String> getSupportedProcedures()
ProcedureExecutionMode getExecutionMode(String)   // SINGLE_CALL for HMS-only work
ConnectorProcedureResult execute(
    ConnectorSession session,
    ConnectorTableHandle table,      // target of ALTER TABLE t EXECUTE ...
    String procedureName,            // dispatch on this
    Map<String,String> properties,   // procedure args, e.g. mode => 'FULL'
    ConnectorPredicate where,
    List<String> orderBy)
// planRewrite(...) is only for DISTRIBUTED data-rewrite procedures — leave defaulted.
```
`execute()`'s body can do anything: open an HMS client, list storage, add/drop
partitions out of band. Return a `ConnectorProcedureResult` (small result table)
summarizing what changed. Exactly how `DuckLakeProcedureOps.expire_snapshots` works.

### Credentials — "pass in all info needed": YES, two ways
1. **You own the catalog properties.** An SPI connector is handed its properties at
   `create()` and keeps them (same way `DuckLakeConnector` holds the Postgres DSN). Parse
   the HMS URI / auth / warehouse from there and build your own HMS client.
2. **`ConnectorContext` even vends Hive-specific helpers** (handed to every connector):
   - `executeAuthenticated(Callable<T>)` — run your HMS call inside the catalog's auth
     context (Kerberos/doAs).
   - `loadHiveConfResources(...)` — load core-site/hdfs-site/hive-site conf.
   - `vendStorageCredentials(...)` / `getStorageProperties()` /
     `getBackendStorageProperties()` — S3/HDFS storage creds for the storage scan.
   - `getMetaInvalidator()` — invalidate Doris's cache after your out-of-band mutation so
     the change shows up immediately.

### Caveats
- Use `ProcedureExecutionMode.SINGLE_CALL` (FE-synchronous, metadata-only) for an
  HMS+storage-list procedure; `DISTRIBUTED`/`planRewrite` is for data rewrites.
- This is a **new Hive-shaped SPI connector we'd build** (or extend), NOT a retrofit onto
  the in-tree `HMSExternalCatalog`. Scope = "stand up a Hive SPI connector with a
  `sync_partition_metadata` procedure", reusing the `DuckLakeProcedureOps` pattern.
- Requires the SPI baseline (branch-catalog-spi / upcoming release), not released pre-SPI.

## If we ever build it
Sketch: `getSupportedProcedures() = ["sync_partition_metadata"]`, `SINGLE_CALL`; in
`execute()` resolve the table (from the handle / args), `executeAuthenticated { ... }`
around: (1) list `key=value/` dirs under the table location via `getStorageProperties()`,
(2) fetch HMS's current partition list, (3) diff per `mode` (ADD/DROP/FULL), (4)
`add_partitions`/`drop_partition` on the HMS client, (5) `getMetaInvalidator()` to refresh
Doris's cache, (6) return a result table of what changed. Mirror `DuckLakeProcedureOps`
for arg parsing / result shaping / fail-loud errors.

## Sources
- apache/doris fe-core (verified `master`, 2026-07-08): `CallFunc.java` (hardcoded CALL
  switch), `RecoverPartitionCommand.java` (`prohibitExternalCatalog`),
  `HiveMetadataOps.java`/`HMSTransaction.java` (INSERT registers partitions in HMS); no
  `MSCK`/`REPAIR TABLE`/`sync_partition_metadata` tokens anywhere in `fe/`.
- Doris docs: Hive Catalog (`/lakehouse/catalogs/hive-catalog/`), Metadata Cache
  (`/lakehouse/meta-cache/`, "synchronized from Hive Metastore" + bypass-HMS note),
  REFRESH statement (`/sql-manual/sql-statements/catalog/REFRESH/`, cache invalidation).
- SPI surface: `fe-connector-api`/`fe-connector-spi` @ vendor `3ba75b7cf8a` —
  `ConnectorProcedureOps`, `ConnectorContext` (`executeAuthenticated`,
  `loadHiveConfResources`, `vendStorageCredentials`, `getStorageProperties`,
  `getMetaInvalidator`). Our own precedent: `DuckLakeProcedureOps` (`expire_snapshots`).
