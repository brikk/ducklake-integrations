# DuckLake-on-Doris — Live FE+BE smoke

The slow, real-cluster counterpart to `:doris-ducklake:test`. `smoke.sh` brings up
a full Doris cluster (FE hosting our plugin + a stock BE) against the DuckLake
substrate (Postgres + MinIO + seeded TPC-H) and exercises the plugin end-to-end:

- **read** — `CREATE CATALOG`, `SHOW/DESC`, `SELECT * FROM dl.tpch.orders`
- **Step 7** — position-delete plumbing (FE-side; BE-side gated on a known
  parquet-nullability gap)
- **W2 INSERT** — `INSERT INTO dl.tpch.doris_w VALUES (…)` via Doris, then reads it
  back through **both Doris and DuckDB+DuckLake** (cross-engine). ✅ GREEN 2026-06-09.

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
stock Doris base, via Doris's own `docker/runtime/doris-fe-overlay/Dockerfile`
(`FROM apache/doris:fe-4.1.0`, wipes + COPYs `output/fe/{bin,lib,conf,plugins,webroot}`).

```bash
# 1. Build the P-series FE (JDK 17). Patch SPI_READY_TYPES first (the route gate):
cd ~/DEV/OSS/db/doris   # branch-catalog-spi
#   fe/.../datasource/CatalogFactory.java: add "ducklake" to SPI_READY_TYPES
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
| `Unknown catalog type: ducklake` on `CREATE CATALOG` | FE missing `"ducklake"` in `SPI_READY_TYPES`. Rebuild the FE image from a patched FE. |
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
- **`SPI_READY_TYPES` + the FE-source patch are NOT in this repo** — they live in your
  `~/DEV/OSS/db/doris` checkout. Re-apply if you re-pull the branch head.
