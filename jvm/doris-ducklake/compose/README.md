# DuckLake-on-Doris — Live-FE Smoke Test Recipe

Walks through getting `SHOW DATABASES IN dl` to return real DuckLake schemas
from a running Doris FE that has loaded our plugin. This is the slow,
hands-on equivalent of `:doris-ducklake:test` — used to confirm the SPI
plumbing and ServiceLoader path actually work end-to-end. Expect a one-time
~20 min cluster build and ~2 min per re-run.

Three boxes need to exist:

| Box                          | Provides                                     | Source                                       |
|------------------------------|----------------------------------------------|----------------------------------------------|
| **DuckLake substrate**       | Postgres metadata DB + MinIO + seeded TPC-H  | `jvm/trino-ducklake/compose/`                |
| **Doris cluster**            | FE that hosts our plugin                     | `~/DEV/OSS/db/doris-pr-62767/` (PR #62767)   |
| **Plugin zip**               | `doris-ducklake-<ver>-plugin.zip`            | `./gradlew :doris-ducklake:assemble`         |

## Prereqs (one-time)

1. **Build the Doris FE** from the PR worktree. Requires JDK 17 and the
   doris-thirdparty bundle per `ducklake-doris-integration-spi-plan.md`
   §1.1–1.2.

   ```bash
   cd ~/DEV/OSS/db/doris-pr-62767
   JAVA_HOME=/path/to/jdk17 JDK_17=/path/to/jdk17 sh generated-source.sh
   cd fe
   JAVA_HOME=/path/to/jdk17 \
     DORIS_THIRDPARTY=/path/to/doris-pr-62767/thirdparty \
     mvn install -DskipTests -Dskip.doc=true \
       -pl '!fe-core,!hive-udf,!be-java-extensions' -T 1C
   ```

   Note that this worktree carries the `SPI_READY_TYPES += "ducklake"` patch
   in `CatalogFactory.java`. If you re-pull the PR head, re-apply (sanity-
   check §3.5).

2. **Produce a deployable FE tarball**. The above only installs jars to
   `~/.m2`; for a runnable FE we need `output/fe/`:

   ```bash
   cd ~/DEV/OSS/db/doris-pr-62767
   sh build.sh --fe        # ~10–20 min first run
   ```

   Result lives in `output/fe/` with `bin/start_fe.sh`, `lib/`, `conf/`,
   `plugins/`.

## Workflow per change

### 1. Build the plugin zip

```bash
cd ~/DEV/brikk/repos/ducklake-integrations/jvm
./gradlew :doris-ducklake:assemble
ls -lh doris-ducklake/build/distributions/doris-ducklake-*-plugin.zip
```

### 2. Bring up the DuckLake substrate

```bash
cd ~/DEV/brikk/repos/ducklake-integrations/jvm/trino-ducklake/compose
docker compose up -d postgres minio create-bucket bootstrap
# wait for bootstrap to complete (it CHECKPOINTs and exits)
docker compose ps   # bootstrap should be 'exited (0)'
```

Postgres is now on `localhost:9432`, MinIO on `localhost:9000`. The
metadata DB is `ducklake`, user/password `ducklake`/`ducklake`, with
TPC-H tables seeded under schema `tpch`.

### 3. Drop the plugin into Doris

The FE loads connector plugins from `${DORIS_HOME}/plugins/connector/<name>/`
(singular `connector`, not `connectors`). The existing PR-branch image
ships `plugins/connector/{iceberg,jdbc,paimon,hive,...}` — we drop a
sibling `ducklake/` containing the unzipped plugin contents.

```bash
PLUGIN_DIR=~/DEV/OSS/db/doris-pr-62767/output/fe/plugins/connector/ducklake
mkdir -p $PLUGIN_DIR
rm -rf $PLUGIN_DIR/lib
unzip -d $PLUGIN_DIR ~/DEV/brikk/repos/ducklake-integrations/jvm/doris-ducklake/build/distributions/doris-ducklake-0.0.1-plugin.zip
ls $PLUGIN_DIR/lib | head
```

No `fe.conf` patch needed — `connector_plugin_root` defaults to
`${DORIS_HOME}/plugins/connector` via a hardcoded Java default
(`fe-core/src/main/java/.../common/Config.java:3541`). `Env.java:2134-2145`
runs both `loadBuiltins()` (classpath `ServiceLoader`) and
`loadPlugins(rootPath)` (file-system scan) on startup, so plugins
dropped into the directory get picked up. The default is invisible
from `fe.conf` itself, which is a discoverability bug worth raising
upstream (see "Upstream nits" below), not a functional gap.

### 4. Start the FE (no BE needed for SHOW commands)

```bash
cd ~/DEV/OSS/db/doris-pr-62767/output/fe
JAVA_HOME=/path/to/jdk17 ./bin/start_fe.sh --daemon
tail -F log/fe.log | grep -E "ConnectorProvider|ducklake|ERROR"
```

You're looking for a line like
`Loaded ConnectorProvider: dev.brikk.ducklake.doris.plugin.DuckLakeConnectorProvider, type=ducklake`.
If the plugin isn't discovered, the most common causes are:
- `META-INF/services/...` not in the jar (verify with `unzip -p plugin.zip lib/doris-ducklake-*.jar META-INF/services/org.apache.doris.connector.spi.ConnectorProvider`)
- `SPI_READY_TYPES` whitelist not patched in the FE you actually started (`grep -r SPI_READY_TYPES output/fe/lib/`)
- The plugin dir layout doesn't match what `DirectoryPluginRuntimeManager` expects — peek at how `fe-connector-iceberg` got laid out in the same `plugins/connectors/` tree to compare

### 5. Sanity-gate the cluster before touching the plugin

Wire-compat between PR-branch FE and stock `apache/doris:be-4.1.0` is
mostly clean (Thrift drops unknown fields; PR-branch deletions are
dead CCR/binlog metadata; new RPCs are BE→FE direction and stock BE
doesn't initiate them). Three checks confirm:

```bash
mysql -h 127.0.0.1 -P 9030 -u root
```

```sql
SHOW BACKENDS;     -- IsAlive must be true for at least one BE
SHOW FRONTENDS;
SELECT 1;          -- proves FE↔BE plan dispatch is intact
```

If `SELECT 1` returns a row, the wire is good. If `SHOW BACKENDS` is
empty or BEs flap, that's the thrift-drift smell — look in `fe.log` for
repeating `setBackendStateChange` errors.

**Caveat**: do **not** create tables with binlog or CCR config against
this hybrid cluster. `TBinlogFormat`, `binlog_format`, `binlog_size`,
`row_binlog_schema`, etc. were removed in the PR branch; the FE will
not understand them in `CreateTabletReq` paths. Sticking to DuckLake
SQL keeps us clear.

### 6. Drive the plugin

```sql
CREATE CATALOG dl PROPERTIES (
  'type' = 'ducklake',
  'metadata.url' = 'jdbc:postgresql://host.docker.internal:9432/ducklake',
  'metadata.user' = 'ducklake',
  'metadata.password' = 'ducklake',
  'storage.warehouse' = 's3://ducklake/data/'
);

SHOW CATALOGS;            -- should list 'dl'
SHOW DATABASES IN dl;     -- should list 'tpch' (seeded by the trino-ducklake bootstrap)
SHOW TABLES IN dl.tpch;   -- should list 'orders', 'lineitem', 'customer', ...
```

If FE is on the host (not in Docker), use `localhost:9432` instead of
`host.docker.internal`. The `storage.warehouse` value is unused for the
"hello tables" path — the FE only consults metadata for these commands —
but the property is required by `validateProperties`, so populate it with
a syntactically valid URL.

### 7. Iterate

```bash
# After editing plugin code:
cd ~/DEV/brikk/repos/ducklake-integrations/jvm
./gradlew :doris-ducklake:assemble
rm -rf ~/DEV/OSS/db/doris-pr-62767/output/fe/plugins/connector/ducklake/lib
unzip -d ~/DEV/OSS/db/doris-pr-62767/output/fe/plugins/connector/ducklake \
  doris-ducklake/build/distributions/doris-ducklake-0.0.1-plugin.zip

# Restart FE (plugin classes are cached by classloader at startup):
cd ~/DEV/OSS/db/doris-pr-62767/output/fe
./bin/stop_fe.sh
./bin/start_fe.sh --daemon
```

## Plugin dirs — `connector/` vs `connectors/`

The FE has **two independent plugin loaders** with confusably similar
directory names:

| Path                                       | Loader                                  | Purpose                              |
|--------------------------------------------|-----------------------------------------|--------------------------------------|
| `${DORIS_HOME}/plugins/connector/`         | `DirectoryPluginRuntimeManager` (new SPI) | PR #62767 fe-connector plugins. **Drop our zip contents here.** |
| `${DORIS_HOME}/plugins/connectors/`        | `TrinoConnectorPluginLoader` (`:92`)    | Legacy Trino-connector JNI bridge (e.g. the BigQuery-via-Trino path). |

Both directories ship in the PR-branch image and **both loaders are
live**. Don't `rm -rf` the plural-`connectors/` even if it looks empty —
it's the load path for any user mixing the Trino bridge with the new
SPI. Drop into the wrong one and the plugin silently doesn't load.

## Upstream nits (low-stakes, bundle with the other asks)

These don't block development but are worth raising alongside the
`SPI_READY_TYPES` whitelist and Option-B BE dispatch asks
(see `ducklake-doris-todo.md` → Upstream coordination):

- **`connector_plugin_root` should appear in `fe.conf`** as a commented-
  out `# connector_plugin_root = ${DORIS_HOME}/plugins/connector` line.
  Today it's only the hardcoded Java default at `Config.java:3541` —
  invisible to operators editing `fe.conf`. Zero behavior change, big
  discoverability win.

## Confirmed (from the worktree agent — 2026-05-18)

- **Plugin dir is `${DORIS_HOME}/plugins/connector/<name>/` (singular)** —
  PR-branch ships sibling dirs `iceberg/jdbc/paimon/hive/...` under
  `plugins/connector/`. There's an empty `plugins/connectors/` (plural,
  legacy) — do not use.
- **`fe.conf` knob is `connector_plugin_root`** — set to
  `${DORIS_HOME}/plugins/connector/` if not already.
- **BE is stock**: `apache/doris:be-4.1.0` (arm64) loads cleanly against
  the PR-branch FE — no Doris-side BE changes required for our scope.
- **fe.conf default ports**: http_port=8030, rpc_port=9020,
  query_port=9030, edit_log_port=9010, arrow_flight_sql_port=8070.
- **`init_fe.sh` (from the base image)** wires `priority_networks` from
  an env var at container start — no manual `fe.conf` patching needed.
- **macOS host build gotchas**:
  - `build.sh --fe` silently no-ops without `gnu-getopt` (`brew install gnu-getopt`).
  - The stock `docker/runtime/doris-compose/Dockerfile` unconditionally
    `sed`s `be/bin/start_be.sh` and fails on FE-only output. Use
    `docker/runtime/doris-fe-overlay/Dockerfile` for FE-only image
    builds (added in the worktree).

## Optional: full compose integration (deferred)

We *could* add a Doris service to `trino-ducklake/compose/docker-compose.yml`
that runs `start_fe.sh` inside a container with the plugin mounted in.
That would make this a single `docker compose up` workflow. Two reasons
we haven't done it yet:

1. The Doris image with the SPI doesn't exist publicly — we'd have to
   `docker build` from the PR worktree's `output/`, which adds a layer of
   "is the image fresh?" friction every iteration.
2. The host-FE recipe above is faster to iterate on (restart in ~5s vs.
   rebuild image + restart container).

Revisit once the SPI lands in a Doris release.
