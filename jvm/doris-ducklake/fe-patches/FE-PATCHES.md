# FE patches needed for the DuckLake plugin connector

The DuckLake connector is a Doris **plugin (SPI) connector**. The Doris FE
(`~/DEV/OSS/db/doris`, branch `branch-catalog-spi`, the P-series connector-SPI
migration) carries a couple of generic guards that were written before any
non-MaxCompute full-adopter existed, so they don't yet know about the
`"ducklake"` catalog type. Until these land upstream we apply them as
working-tree patches to the local FE checkout, build the FE, and overlay it into
the `doris-fe:pr62767-local` image used by `compose/docker-compose.yml`.

These are **not** committed to the OSS Doris checkout — they live here as a
reapplyable patch (`ducklake-fe.patch`) so the FE build is reproducible and the
upstream asks are tracked. See [[doris-fe-build-macos]] + [[doris-compose-smoke-remote]].

## Apply + rebuild

```bash
cd ~/DEV/OSS/db/doris            # branch-catalog-spi
git apply /path/to/jvm/doris-ducklake/fe-patches/ducklake-fe.patch   # or keep as WT changes
JAVA_HOME=<jdk17> DISABLE_BUILD_UI=ON ./build.sh --fe                 # ~2 min incremental
# re-image the overlay (FROM apache/doris:fe-4.1.0, COPY ./output/fe):
podman build -f docker/runtime/doris-fe-overlay/Dockerfile \
  -t doris-fe:pr62767-local \
  --build-arg BASE_IMAGE=apache/doris:fe-4.1.0 --build-arg OUTPUT_PATH=./output <staging>
# then tear the cluster down (-v) and rerun compose/smoke.sh so the fresh FE loads.
```

## The patches (`ducklake-fe.patch`)

### 1. `CatalogFactory.SPI_READY_TYPES` += `"ducklake"`  — the route/write gate
`fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java`

Whitelists `type=ducklake` as an SPI-driven catalog. Without it
`CREATE CATALOG ... type=ducklake` → "Unknown catalog type", and INSERT/DDL are
never routed to the connector. This is the gate the W2/W2c INSERT smokes already
depend on. (Tracked in `ducklake-doris-friction.md`, 2026-05-19 "SPI_READY_TYPES
whitelist silently drops unknown ConnectorProviders".)

### 2. `CreateTableInfo.pluginCatalogTypeToEngine` += `case "ducklake" → ENGINE_ICEBERG`  — the CREATE TABLE gate
`fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/CreateTableInfo.java`

`paddingEngineName()` pads a legacy engine name for a no-ENGINE `CREATE TABLE` on
a plugin catalog; `pluginCatalogTypeToEngine()` only mapped `"max_compute"`, so
every other plugin type (including `"ducklake"`) fell to `default → null` and the
else-branch threw **"Current catalog does not support create table"**
(`CreateTableInfo.java:928`) — *before* the connector was ever consulted. This is
purely an FE engine-padding gap; `PluginDrivenExternalCatalog.createTable()` is
generic (it converts the request and calls `metadata.createTable`), and the
connector mapping is headless-green (`DuckLakeDdlTest`, 96 tests).

Padding **`ENGINE_ICEBERG`** is the correct fix, not just a non-null placeholder:
- DuckLake is Iceberg-shaped — the BE sink is a `TIcebergTableSink` and
  partitioning uses the Iceberg transform family (`bucket`/`year`/`day`/…).
- The iceberg engine path is the one that **accepts `PARTITIONED BY (bucket(N, col))`**
  and **rejects `DISTRIBUTE BY`** (`CreateTableInfo.java:792`), which exactly matches
  the connector's own `DuckLakeCreatePartitionMapper` contract (murmur3 bucket only
  via the iceberg-transform path; CRC32 `DISTRIBUTED BY` rejected).
- `checkEngineName()` accepts `ENGINE_ICEBERG` and marks the table external; the
  catalog-engine consistency check (`checkEngineWithCatalog`, line 396) calls the
  same `pluginCatalogTypeToEngine`, so it stays consistent automatically.
- Routing is by catalog **instance** (a `PluginDrivenExternalCatalog`), not by the
  engine string, so the padded name never diverts CREATE TABLE to the native
  Iceberg DDL handler — it stays on the generic connector path.

Read-side engine display (`PluginDrivenExternalTable.getEngine()/
getEngineTableTypeName()`) is intentionally **left generic** for ducklake: the read
path is already shipped/green and some BE dispatch keys on the literal engine
string, so we don't perturb it for a write-DDL fix.

**Upstream ask:** generalize `pluginCatalogTypeToEngine` (and the read-side
switches) to consult the connector's declared capabilities/engine rather than a
hardcoded per-type switch, so a new SPI full-adopter doesn't need an FE edit.
