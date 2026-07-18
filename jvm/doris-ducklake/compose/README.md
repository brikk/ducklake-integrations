# DuckLake-on-Doris — Live FE+BE smoke

The slow, real-cluster counterpart to `:doris-ducklake:test`. `smoke.sh` brings up
a full Doris cluster (FE hosting our plugin + a stock BE) against the DuckLake
substrate (Postgres + MinIO + seeded TPC-H) and exercises the plugin end-to-end:

- **read** — `CREATE CATALOG`, `SHOW/DESC`, `SELECT * FROM dl.tpch.orders`
- **Step 7** — position-delete plumbing (FE-side; BE-side gated on a known
  parquet-nullability gap)
- **W1 DDL** — live Doris `CREATE DATABASE` + `CREATE TABLE` (unpartitioned and
  `PARTITION BY LIST (bucket(4, name)) ()`) routed FE→connector→DuckLake, cross-verified
  via DuckDB+DuckLake and the catalog tables, then `INSERT` + `DROP`. ✅ GREEN 2026-06-10
  (needs the FE engine-padding patch — see *Building the FE image*).
- **W2 INSERT** — `INSERT INTO dl.tpch.doris_w VALUES (…)` via Doris (target now created
  by Doris `CREATE TABLE`, not DuckDB), then reads it back through **both Doris and
  DuckDB+DuckLake** (cross-engine). ✅ GREEN 2026-06-09.
- **W2c BUCKET INSERT** — `INSERT … INTO dl.tpch.doris_wb` (a `bucket(4, name)` table)
  via Doris, then asserts the bucket each file was tagged with in the catalog is exactly
  `{1,2,3}` — i.e. the BE's Iceberg murmur3 matches DuckLake's (alice→1, bob→2,
  charlie→3). ✅ GREEN 2026-06-09.

## Quick start

```bash
cd jvm/doris-ducklake/compose
./smoke.sh                 # build plugin → up substrate+cluster → drive read/delete/INSERT
./smoke.sh --no-build      # skip the gradle plugin rebuild (zip already current)
./smoke.sh --down          # tear down the Doris cluster (substrate left running)
```

One-time `~20 min` cluster build (FE image, image pulls); `~2–3 min` per re-run.

### Prereqs
- The **FE image** `doris-fe:pr62767-local` must exist locally (see *Building the FE
  image* below). It must be built from a P-series FE (`branch-catalog-spi`) **with
  `"ducklake"` in `CatalogFactory.SPI_READY_TYPES`** — that's the gate that routes
  `type=ducklake` (and INSERT) to our connector. Without it: `Unknown catalog type:
  ducklake`.
- The stock BE image `apache/doris:be-4.1.0` (pulled automatically).
- Docker **or** podman (see *Running under podman* ).

## Running under podman (x86_64 remote box)

The compose was authored for an **Apple-Silicon (arm64) + Docker** dev box. On an
**x86_64 + podman** host, three things differ:

1. **`docker`→`podman` shim** — `smoke.sh` calls `docker …`. Make a shim and prepend it:
   ```bash
   mkdir -p /tmp/dockershim
   printf '#!/usr/bin/env bash\nexec /opt/podman/bin/podman "$@"\n' > /tmp/dockershim/docker
   chmod +x /tmp/dockershim/docker
   export PATH="/tmp/dockershim:/opt/podman/bin:$PATH"
   export DOCKER_HOST=unix:///var/run/docker.sock
   ```
   `podman compose` works (it delegates to `docker-compose`).
2. **BE architecture** — the `doris-be` service pins `platform: ${DORIS_BE_PLATFORM:-linux/arm64}`.
   On x86_64 you **must** select amd64, else the arm64 BE runs under emulation and
   crashes on startup (`F … elf.cpp:76] The ELF '/proc/self/exe' is truncated`):
   ```bash
   export DORIS_BE_PLATFORM=linux/amd64
   podman pull --platform linux/amd64 apache/doris:be-4.1.0   # ensure the local tag is amd64
   ```
3. **FE image arch** — build the overlay on the amd64 box so `doris-fe:pr62767-local`
   is amd64 (the overlay's `FROM apache/doris:fe-4.1.0` resolves to the host arch).

Then: `DORIS_BE_PLATFORM=linux/amd64 ./smoke.sh`. (Full remote recipe also in the
`doris-compose-smoke-remote` project memory.)

## Building the FE image (`doris-fe:pr62767-local`)

The compose uses a locally-built FE image overlaying our P-series `output/fe` onto a
stock Doris base, via the in-repo [`fe-overlay/Dockerfile`](./fe-overlay/Dockerfile)
(`FROM apache/doris:fe-4.1.0`, wipes + COPYs `output/fe/{bin,lib,conf,plugins,webroot}`).
(Originally a local-only file in the Doris checkout; now tracked here so it can't be lost.)

# ⚠️ PINNED COMMIT — build from the SAME commit we last researched + re-diffed the patch against.
#   `branch-catalog-spi` REBASES CONSTANTLY, so do NOT just `git pull` the branch tip: a newer
#   tip may move the patch context or add a non-default SPI method that breaks the plugin.
#   Pin (2026-07-18): b2dff681aad
#     subject: "[feat](catalog) fe-connector-iceberg: port #64966 REST 401 re-auth to the connector"
#   SHAs churn on rebase — if that SHA is GC'd/gone, check out the commit with that exact SUBJECT,
#   then RE-VALIDATE before building: re-run the diff in ../dev-docs/REPORT-doris-p6-iceberg-spi-cutover.md
#   §"upstream re-check" and re-diff ../fe-patches/ducklake-fe.patch (git apply --check must be clean).
#   The current pin is recorded in ../fe-patches/FE-PATCHES.md → "Re-vendor log" (keep all three in sync).

```bash
# 1. Build the P-series FE (JDK 17). Apply BOTH FE patches first — see
#    ../fe-patches/FE-PATCHES.md (reapplyable: git apply ../fe-patches/ducklake-fe.patch):
cd ~/DEV/OSS/db/doris && git checkout b2dff681aad   # branch-catalog-spi @ pinned commit (see note above)
#   • CatalogFactory.java         : add "ducklake" to SPI_READY_TYPES        (catalog/INSERT route gate)
#   • CreateTableInfo.java        : pluginCatalogTypeToEngine += "ducklake"→ENGINE_ICEBERG  (CREATE TABLE gate)
JAVA_HOME=<jdk17> DISABLE_BUILD_UI=ON ./build.sh --fe        # → output/fe  (see doris-fe-build-macos memory)

# 2. Image it (stage a minimal context so podman/docker isn't sent the multi-GB repo):
S=/tmp/feimg; rm -rf $S; mkdir -p $S/output $S/docker/runtime/doris-fe-overlay
cp -r output/fe $S/output/fe
cp docker/runtime/doris-fe-overlay/*.txt $S/docker/runtime/doris-fe-overlay/
podman build -f docker/runtime/doris-fe-overlay/Dockerfile \
  -t doris-fe:pr62767-local \
  --build-arg BASE_IMAGE=apache/doris:fe-4.1.0 --build-arg OUTPUT_PATH=./output  $S
```

Rebuild the image only when the **FE itself** changes. Plugin-only changes don't need
it — `smoke.sh` rebuilds the plugin zip and reinstalls it into the FE plugin volume.

## Shutdown / cleanup

```bash
./smoke.sh --down                                                   # Doris FE+BE + volumes
docker compose -f ../../trino-ducklake/compose/docker-compose.yml down -v   # the substrate
podman ps -a | grep -E 'doris|ducklake'                             # confirm nothing lingering
```

`down -v` wipes the FE metadata + plugin volumes — exactly what you want between FE
or plugin changes so the fresh FE reloads everything.

## Troubleshooting (things we actually hit)

| Symptom | Cause / fix |
|---|---|
| `FE never came up` but FE log shows SQL | The FE *is* up; the health-check `SELECT 1` needs a **live BE** (Nereids assigns even constant queries to a backend). Check `SHOW BACKENDS\G` → `Alive`. |
| BE `Exited (0)`, `ErrMsg: NoRouteToHost`, FE `No backend available as scan node` | BE crashed. On x86_64 this is the **arm64-under-emulation** crash — set `DORIS_BE_PLATFORM=linux/amd64` + re-pull the amd64 BE. |
| FE exits 255; `fe.log` shows BDB JE `NoClassDefFoundError: …JVMSystemUtils` → `NullPointerException … CgroupV2Subsystem.getInstance … anyController is null` | The base FE image's bundled **JDK 17 is too old for this host's cgroup v2** (NPEs during container-resource detection). Native-Linux/cgroup-v2 boxes hit this; the old Docker-Desktop VM didn't. Fix: `fe.conf` `JAVA_OPTS_FOR_JDK_17` carries `-XX:-UseContainerSupport` (safe — heap is pinned `-Xmx2g`). |
| `Unknown catalog type: ducklake` on `CREATE CATALOG` | FE missing `"ducklake"` in `SPI_READY_TYPES`. Rebuild the FE image from a patched FE (`../fe-patches/`). |
| `Current catalog does not support create table: dl` on `CREATE TABLE` | FE missing the `pluginCatalogTypeToEngine` `"ducklake"→ENGINE_ICEBERG` patch. Reapply `../fe-patches/ducklake-fe.patch`, rebuild + re-image the FE. |
| INSERT: `Unsupported compress type UNKNOWN with parquet` | (fixed) sink must set a compression — we use `ZSTD`. |
| Read-back path doubled (`…/doris_w/s3%3A//…`) | (fixed) the BE returns an absolute path; the connector relativizes it against the table data dir. |

## Reference / gotchas

- **Plugin dir is `${DORIS_HOME}/plugins/connector/<name>/` (singular `connector`).**
  There's also a plural `plugins/connectors/` for the legacy Trino-JNI bridge — **don't**
  drop our zip there or `rm -rf` it. `smoke.sh` installs into a named volume mounted at
  `…/plugins/connector/ducklake` (podman-on-macOS bind mounts don't propagate reliably,
  hence the volume + `docker cp` helper).
- **FE↔BE wire-compat**: the P-series FE pairs cleanly with stock `apache/doris:be-4.1.0`
  for our scope (Thrift drops unknown fields). Avoid binlog/CCR table options on this
  hybrid cluster. `fe.conf` here drops the heap to 2g and is mounted over the image's.
- **Network**: the substrate compose project is `trino-ducklake-dev`, so its bridge
  network is `trino-ducklake-dev_default` — the one-shot DuckDB helper containers join it
  by that name. Keep the two `name:` fields in sync if you rename projects.
- **The FE-source patches are applied to your `~/DEV/OSS/db/doris` checkout, not built
  from this repo** — but they ARE tracked here as [`../fe-patches/ducklake-fe.patch`](../fe-patches/FE-PATCHES.md)
  (`SPI_READY_TYPES` + `pluginCatalogTypeToEngine`). Reapply with `git apply` if you
  re-pull the branch head, then rebuild + re-image the FE.
