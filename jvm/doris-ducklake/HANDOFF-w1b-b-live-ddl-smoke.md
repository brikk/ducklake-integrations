# HANDOFF — W1b(b): live DDL smoke for Doris→DuckLake CREATE TABLE

## Where to stand (set these for the next agent)

- **Worktree directory:** `/Users/jminard/DEV/brikk/ducklake-integrations/.claude/worktrees/bridge-cse_01EuTesU9ses8UvLH4AgKicK`
- **Branch:** `doris-catalog-spi`  (pushed; remote `origin/doris-catalog-spi`)
- **HEAD at handoff:** `88fd59d` — *feat(doris-write): W1b(a) — partitioned CREATE TABLE connector mapping*
- This is a **separate worktree from `main`**; do not confuse it with the main repo checkout at `/Users/jminard/DEV/brikk/ducklake-integrations` (which sits on `main`/`facccd1`). Work *here*.
- Commit/push autonomy: free to commit & push to `doris-catalog-spi` at checkpoints; merging to `main` needs the user's approval. Commit as `Jayson Minard <jayson.minard@gmail.com>` (global gitconfig already set).

## What's already done (W1b(a) — connector side, headless-green)

The connector-side partitioned CREATE TABLE mapping is **complete, tested, committed, pushed**:
- `DuckLakeCreatePartitionMapper` maps the SPI `partitionSpec`/`bucketSpec` → DuckLake `PartitionFieldSpec`s; `DuckLakeConnectorMetadata.createTable` calls `catalog.createTable(db, table, columns, partitionFields, null)`.
- Tests: `DuckLakeCreatePartitionMapperTest` (pure-logic) + extended `DuckLakeDdlTest` (CREATE → `getPartitionSpecs` round-trip). **Doris suite 87→96 green, 0 failures.**

**Critical fact you must carry into the DDL you write (it shapes the SQL):**
- A real DuckLake (murmur3) bucket only comes from the **Iceberg-transform path**: `PARTITIONED BY (bucket(N, col))`, `year(col)`, etc. The FE lowercases these into `ConnectorPartitionSpec` transforms.
- Doris's `DISTRIBUTED BY HASH(...) BUCKETS n` arrives as a `ConnectorBucketSpec` with algorithm `"doris_default"` (CRC32) — **NOT** murmur3 — and the connector **rejects it** (it would silently bucket by the wrong hash). So in the smoke's CREATE TABLE statements, use `PARTITION BY` iceberg-transform syntax for bucketing, **never** `DISTRIBUTED BY`.

## Your task — W1b(b): add a live DDL step to the smoke + drop the DuckDB-create crutch

Goal: prove the **live FE→connector DDL route** works end-to-end, and stop relying on DuckDB to pre-create the INSERT targets.

**Files (all under `jvm/doris-ducklake/compose/`):**
- `smoke.sh` — the driver. Add a new step (suggest a "W1 DDL" step *before* the W2 INSERT block, ~line 283). Mirror the existing step structure (`log`, `docker exec ... mysql`, `set +e`/status capture, `refresh_dl_catalog`).
- `w2-insert.py` — has a `create` mode (DuckDB `CREATE TABLE … [SET PARTITIONED BY …]`) and a `verify` mode (DuckDB read-back). Keep `verify`; the **`create` mode is the crutch to remove** once Doris `CREATE TABLE` works.

**Concretely:**
1. **New DDL step** — via `docker exec doris-ducklake-fe mysql … -uroot`:
   - `SWITCH dl;` then `CREATE DATABASE`/`CREATE TABLE dl.tpch.doris_ddl (id INT, name STRING)` (unpartitioned first — lowest risk).
   - Verify the table exists with the right columns: easiest is `DESC dl.tpch.doris_ddl` (Doris-side) **and** a DuckDB+DuckLake cross-check (add a small `verify`-style mode, or reuse a psql query against `ducklake_table`/`ducklake_column`).
   - Then `INSERT INTO dl.tpch.doris_ddl VALUES (…)` (reuses the W2 sink) and read back; finally `DROP TABLE` / `DROP DATABASE`.
2. **Partitioned DDL** — once unpartitioned works, add a `CREATE TABLE … PARTITION BY (bucket(4, name))` (or `year(d)`) and confirm via the catalog (`ducklake_partition_column.transform`) that the partition spec landed — the live analogue of the W1b(a) headless round-trip.
3. **Replace the crutch** — swap the `w2_helper create` calls (lines ~329-330 and ~382-383) for Doris `CREATE TABLE` (the partitioned one carries `PARTITION BY (bucket(4, name))` for the W2c target `doris_wb`). Keep `w2_helper verify`.

## How to run the smoke (env recipe — turnkey)

This box is Apple-Silicon; the FE is built from the P-series `branch-catalog-spi` and the BE runs amd64 under emulation. All compose images are cached → bring-up ~2-3 min.

- **Docker→podman shim + amd64 BE:**
  ```
  export PATH=/opt/podman/bin:$PATH
  export DORIS_BE_PLATFORM=linux/amd64        # arm-under-emulation BE crashes without this
  # docker→podman shim at /tmp/dockershim/docker (see doris-compose-smoke-remote memory)
  ```
- **Run:** `cd jvm/doris-ducklake/compose && ./smoke.sh`  (use `--no-build` if the plugin zip is current; `--down` to tear down).
- See `compose/README.md` and the **`doris-compose-smoke-remote`** + **`doris-fe-build-macos`** memories for the FE overlay image, `SPI_READY_TYPES`, and shim details.

## The gate + the main risk to check FIRST

- **`SPI_READY_TYPES` gate:** the live INSERT/DDL route requires fe-core's `CatalogFactory.SPI_READY_TYPES` to include `"ducklake"`. The smoke FE image is already built with this (W2/W2c ran green through it). If DDL is rejected as "unsupported", that gate (FE build) is the first suspect.
- **⚠️ Unverified until you run it:** that the FE actually **routes plugin-catalog `CREATE TABLE` to the connector**. INSERT is proven live (W2); DDL on a plugin catalog goes through `PluginDrivenExternalCatalog` resolving `IF [NOT] EXISTS` then calling `ConnectorMetadata.createTable`. The connector advertises `SUPPORTS_CREATE_TABLE` (added in W1). But whether this specific FE build wires the DDL route — and whether Doris's parser accepts iceberg-style `PARTITION BY (bucket(N,col))` for an external/plugin catalog — is the **live-only unknown this step exists to settle**. If `CREATE TABLE` errors at the FE before reaching the connector, that's an FE-build/route gap to document in `ducklake-doris-friction.md` (not a connector bug — the connector side is headless-green).

## Definition of done

- `smoke.sh` runs a Doris `CREATE DATABASE`/`CREATE TABLE` (unpartitioned + at least one partitioned) that the connector commits to DuckLake, cross-verified by DuckDB+DuckLake and/or the catalog tables, then `INSERT` + `DROP`.
- The DuckDB `create` crutch in `w2-insert.py` is removed from the W2/W2c flow (targets created by Doris).
- Update `ducklake-doris-todo-write.md` W1b(b) + the phased-plan W1 line to reflect the result (GREEN, or BLOCKED-with-reason if an FE-route gap surfaces).
- Commit + push to `doris-catalog-spi`.

## Reference pointers

- TODO/spec: `jvm/doris-ducklake/ducklake-doris-todo-write.md` (§ W1b).
- Connector DDL: `DuckLakeConnectorMetadata.kt` (createDatabase/createTable/dropTable), `DuckLakeCreatePartitionMapper.kt`, `DuckLakeCreateTableMapper.kt`.
- Live templates: `MaxComputeConnectorMetadata` (write/DDL), and the existing W2/W2c steps in `smoke.sh` are the structural pattern to copy.
- Project memories: `doris-write-insert-feasibility`, `doris-compose-smoke-remote`, `doris-fe-build-macos`, `doris-spi-targeting`.
