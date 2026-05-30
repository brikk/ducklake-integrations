# TODO: DuckDB as a first-class data file format

Living tracker for DuckDB-as-DuckLake-data-file-format work.
Detail on completed work lives in the **Status log** at the bottom; the top of
this file is what's *open*. If chat context is lost, this file is the recovery
point.

---

## Phases 1 + 1.5 + N1 + N2 — Done ✅

End-to-end working: write `.db` files via Trino CTAS / INSERT, read them back
through Trino, mix with parquet tables in the same query, predicate +
projection pushdown into DuckDB SQL, column stats persisted to the DuckLake
catalog (cross-engine introspection works), Arrow-stream writer (default) for
columnar bulk loads, INSERT inherits format from existing data files, and
DuckDB httpfs read path for cold one-shot reads of large remote files.

**User-facing surface:**

- `SET SESSION ducklake.data_file_format = 'duckdb' | 'parquet'` (default: unset → inherits from latest existing data file in the table; falls through to `'parquet'` for empty tables)
- `CREATE TABLE foo (...) WITH (data_file_format = 'duckdb')` overrides per-CREATE
- `SET SESSION ducklake.duckdb_writer_mode = 'arrow_stream' | 'appender'` (default `'arrow_stream'`)
- `SET SESSION ducklake.duckdb_read_mode = 'auto' | 'materialize' | 'httpfs'` (default `'auto'`; threshold via `ducklake.duckdb.auto-httpfs-threshold` connector config, default 64 MiB)
- Partitioned tables work on the duckdb path (`partitioned_by` table property, identity transforms).
- File rollover by size: writer rolls to a new `.db` file when approximate logical input bytes exceed `ducklake.duckdb.target-write-bytes` connector config (default 512 MB). Applies per-writer, so partitioned tables roll over per partition independently.

**Practical perf shape (verified on real OVH-backed bench, sf=0.5 scale, post-fix):**

| Workload | parquet | duckdb materialize (warm) | duckdb httpfs |
|----------|---------|---------------------------|---------------|
| TPC-H Q1-shape lineitem scan | ~5 s | ~1–2.5 s | ~7–10 s |
| lineitem ⨝ orders aggregate join | ~5–11 s | ~2–10 s | ~8–15 s |

Materialize wins repeat reads (DuckDB's columnar reads from local disk are the floor). httpfs is currently a *cold one-shot* mode only — every query opens a fresh in-process DuckDB instance, so there's no cross-query block cache. Per-JVM instance pooling (N4) would change that calculus; until then, `auto`'s 64 MiB threshold keeps typical workloads on the materialize path.

**Known runtime requirements (production deploys):**

- Trino worker JVMs need `--add-opens=java.base/java.nio=ALL-UNNAMED` and `--add-opens=java.base/java.lang=ALL-UNNAMED` for Apache Arrow's C-data interface. Documented in `compose/README.md`; injected via `JAVA_TOOL_OPTIONS` in the dev `docker-compose.yml`.
- For the httpfs read path the connector reuses the same `s3.endpoint` / `s3.region` / `s3.aws-access-key` / `s3.aws-secret-key` / `s3.path-style-access` keys the FileSystemModule already consumes — no separate S3 config to maintain.

See the **Status log** for per-step detail.

---

## Phase NEXT — open work

### Test-gap categories — T1..T4

These came out of the 2026-05-06 audit: most of the connector's operations are
format-agnostic in code, but the existing UPDATE/DELETE/MERGE/ALTER/time-travel
test suites only run with the parquet default. The duckdb path is exercised
correctly only by tests that explicitly opt in via `data_file_format = 'duckdb'`
or `arrowStreamSession()`. We've already been bitten twice by this (the
arrow-stream writer's in-place reset, and UUID being silently broken in both
writer + reader). The categories below are the cheap-mostly-mechanical sweep to
close the matrix — each is "take the existing parquet test, parameterize or
fork it for duckdb format, assert the same shape works."

**T1. UPDATE / DELETE / MERGE on duckdb-format tables.** New data files written by
the merge insert leg should be duckdb (per N1's precedence chain). Position-delete
files stay parquet (DuckLake spec). End-to-end: rows actually disappear / change
on subsequent reads. Cover both `arrow_stream` and `appender` writer modes.

**T2. Schema evolution on duckdb-format tables.** Pre-existing `.db` file written
under one schema version, then ADD/DROP/RENAME COLUMN, then SELECT — does the
read path still produce sensible results? Critical for DuckLake's
schema-version-on-data-file model. Also cover ADD COLUMN followed by INSERT —
new files should have the new column populated; old files should null-fill.

**T3. Partition pruning + time travel + system tables on duckdb-format tables.**
- Partition pruning: identity + temporal transforms, both with all-duckdb files
  and with mixed parquet + duckdb partitions in the same table.
- Time travel: `FOR VERSION AS OF` against a snapshot where the table had
  duckdb files; same against a snapshot where ADD COLUMN happened.
- `$snapshots`, `$partitions`, `$file_modifications` system tables — verify
  they return the right rows when the underlying table has duckdb files.

**T4. Type round-trip matrix on duckdb-format reads + writes.** Covers the gap
that hid the UUID bug for so long. One test per supported scalar type that does
the full round trip (CTAS via arrow_stream → SELECT, CTAS via appender → SELECT,
INSERT into pre-existing table via both writers → SELECT). Includes NULL
handling per type. Excludes complex types (ROW/ARRAY/MAP — separate epic).

T4 is the most likely to surface another UUID-shaped silent bug, so probably
worth doing first.

### N3. Out-of-process DuckDB workers via Swanlake (Arrow Flight)

**Why.** Move DuckDB out of the Trino JVM into separately-supervised
worker processes that the connector talks to over Arrow Flight. The current
in-process model means a DuckDB OOM, native crash, or stuck thread takes the
Trino node down with it; resource accounting is fuzzy (DuckDB and Trino share
heap, off-heap, and JNI scratch); cancellation is best-effort. Out-of-process
fixes all of that.

**Reference.** [https://swanlake.io/](https://swanlake.io/) — Arrow Flight
server wrapping DuckDB; gives us a network-addressable DuckDB endpoint we can
talk to via a standard Flight client.

**Shape.**

- **Process per task/split.** Launch a Swanlake process for the lifetime of a
  Trino task or split. Run it under OS-level resource constraints (cgroups for
  memory + CPU) so a runaway query can't blow past its budget. `ATTACH` the
  duckdb file(s) the task needs read-only, run the query, return Arrow batches
  back to the connector.
- **Cancellation = SIGKILL.** Read-only ATTACH + per-task process means killing
  is safe — no torn writes, no shared state to corrupt. No graceful-shutdown
  protocol needed; the controller just terminates and reaps. (This is the main
  argument *against* sharing a Swanlake worker across queries.)
- **Small reuse pool with TTL.** Cold-start cost is real (process fork +
  DuckDB instance init + extension loads). Maintain a small per-Trino-node
  pool of pre-warmed workers; hand them out to incoming tasks. TTL them out
  after some idle period or after N reuses to keep memory clean. Pool sizing
  follows the worker's task-slot count.
- **Wire protocol: Arrow Flight, not JDBC.** Switch the connector's read +
  write paths from `org.duckdb.DuckDBConnection`/JDBC to an Arrow Flight client
  (`flight-sql-jdbc-driver` or the lower-level Flight client). The Flight
  channel naturally produces Arrow record batches, so the existing
  `DucklakeArrowToPageConverter` can be reused as-is on the read side. On the
  write side, `INSERT INTO ... SELECT * FROM <flight stream>` becomes a Flight
  `DoPut` call to the worker. Zero-copy when possible.
- **Transport: TCP first, UDS later.** Initial implementation uses
  `localhost:<port>` per worker (simpler portability across deploy modes).
  Switch to Unix domain sockets once we want to drop syscall + TCP overhead
  for same-host workers; trivial change at the Flight layer.
- **K8s deployment: Swanlake as a sidecar.** Each Trino worker pod gets one
  or more Swanlake sidecar containers; communication over the pod-internal
  shared network namespace (TCP localhost or UDS via shared volume).
  Strongest isolation, easiest resource budgeting (Kubernetes already does
  cgroup enforcement at the container level). Same pattern works in
  docker-compose for the dev stack.
- **Supervisor.** A small connector-side controller keeps the pool sized,
  health-checks workers, restarts ones that died (OOM, native fault, hang).
  In k8s the supervisor is just kubelet plus a restart policy on the sidecar
  container; in compose it's `restart: on-failure` on the service. The
  connector only needs to retry-with-fresh-worker when a Flight call fails on
  a dead endpoint.

**What this changes inside the connector code.**

- `DuckDbFilePageSource` and the two writer impls become Flight clients
  instead of holding a `DuckDBConnection`. Same external behavior (per-split
  page source, columnar Arrow on the wire); just the connection target moves
  from in-JVM to localhost / sidecar.
- `DucklakeMaterializedFileCache` becomes optional: Swanlake can run
  DuckDB's httpfs and ATTACH the remote `.db` directly. (Same decision matrix
  as N2; but the materialized cache, if any, lives on the worker side, not the
  Trino JVM side.)
- The `--add-opens` JVM flags requirement on the Trino server JVM
  *disappears* — the Arrow C-data interface plumbing all moves into the
  Swanlake worker. Trino-side just speaks Flight (regular Arrow over the
  network, no DirectByteBuffer reflection tricks needed).
- New connector config: pool sizing, TTL, worker launcher (path to Swanlake
  binary, or sidecar mode), transport (TCP/UDS), per-worker resource limits.

**Sequencing.** Land N4 first (per-JVM instance pool) — closes the perf gap
that's part of the justification for N3 and is a much smaller change. Then N3
if isolation/cancellation/resource-accounting drive the case for going
out-of-process. Write N3 as an additive layer (Flight worker mode behind a
session/connector flag) so we can A/B against the in-process path the same way
we did with the Arrow-stream writer.

### N4. Per-JVM DuckDB instance pool

**Why this is load-bearing.** With N2 landed, httpfs is functionally "cold one-shot" only — the live OVH benchmark (in the table at the top) shows httpfs is consistently 2–5× slower than materialize on warm reads, and never improves across queries because each query opens a fresh in-process DuckDB instance. There's no cross-query httpfs block cache, no reused secret/extension load, no warm catalog metadata.

**Proposal.**
- Per-JVM `DuckDbInstanceRegistry` (singleton). Holds a small pool of `DuckDBConnection` instances, each long-lived (`INSTALL httpfs; LOAD httpfs; CREATE SECRET ...` runs once per instance).
- `DuckDbFilePageSource.initialize()` borrows a connection via `connection.duplicate()` (DuckDB-native concept that gives an independent connection sharing the same instance), runs `ATTACH '<path>' AS s_<uuid> (READ_ONLY)` against it, runs the query, `DETACH`-es and returns the connection.
- For the writer, same pattern with READ_WRITE attach against a temp file.
- Pool sizing follows `node-scheduler.max-splits-per-node` or similar; TTL after N reuses to keep memory predictable.
- Critical: enable DuckDB's `enable_external_file_cache=true` (or current equivalent) so per-instance httpfs block reads are cached across queries on that instance.

**What we need to verify before/while building this.**
- DuckDB JDBC's `DuckDBConnection.duplicate()` semantics for safety across query lifecycles (especially with concurrent ATTACH/DETACH from different splits).
- That `ATTACH ... (READ_ONLY)` and DETACH are cheap relative to instance creation (cold instance setup is ~hundreds of ms; ATTACH is supposed to be milliseconds).
- Memory accounting: a long-lived DuckDB instance consumes a baseline of native memory; pool size × baseline must fit alongside Trino heap inside the container.

**What this gets us.** httpfs becomes useful for repeat queries on the same large remote file. Materialize stays the right call for hot small-to-medium files. Cold-start cost amortizes across queries. Removes the only real argument for picking N3 over the simpler in-process model for many deployments.

**Sequencing.** Less invasive than N3 (no out-of-process boundary, no Flight client refactor, no sidecar deployment changes). Worth landing before N3 because it likely closes the perf gap that N3 was partly justified by.

### N5. Diagnostic logging around read mode + materialize cache

**Why.** The first thing we needed when debugging the join hang was "what mode did this query actually pick, and was the materialize cache hit?" Today the answer requires `docker exec ... ls /tmp/ducklake-read/` plus inferring from query timing. Trivial code, big debug-time win.

**Proposal.**
- INFO-level log line per duckdb-format split: chosen mode (materialize/httpfs), file path, file size bytes, threshold value (when mode was `auto`), cache hit/miss (materialize), ATTACH ms, total read ms.
- A `$duckdb_read_mode` system table or column on `$files` so users can inspect mode-decision history retrospectively.

Tiny scope. Pair with N4 if we add metrics on instance pool checkout times.

### N6. (Stretch, do after N4) Per-table or SQL-hint override of read mode

Once httpfs is competitive for repeated reads (after N4), the table-property or SQL-hint override becomes meaningful:

- `SET SESSION` override per query (already have via `duckdb_read_mode`).
- `CREATE TABLE foo WITH (duckdb_read_mode = 'materialize')` for hot tables.
- A future cache manager could populate this automatically based on access patterns.

---

## Verified facts (relevant to future work)

- `fileFormat` is a plain `String` column on `ducklake_data_file` — no enum to extend; the spec's `data_file_format` field accepts arbitrary strings.
- `ducklake_schema` and `ducklake_table` rows have no extensible properties column. Persisting schema/table-level metadata requires connector-private extension tables.
- DuckDB writer SQL: `ATTACH 'new.db' AS new (READ_WRITE); CREATE TABLE new.t AS SELECT...; DETACH new` (the original concept doc's `COPY (...) TO 'file.db' (FORMAT DUCKDB)` is *not* the path).
- DuckDB reader SQL: `ATTACH '/path/foo.db' AS s (READ_ONLY); SELECT * FROM s.<table>` works for local files. httpfs extends this to remote S3 paths.
- `DuckDBResultSet.arrowExportStream(allocator, batchSize)` and `DuckDBConnection.registerArrowStream(name, ArrowArrayStream)` are the canonical DuckDB-Java entry points for the Arrow C-data interface (read and write directions respectively). `registerArrowStream` strictly requires a real `org.apache.arrow.c.ArrowArrayStream` instance — no shape-compatible wrappers.
- Apache Arrow's `MemoryUtil.directBuffer()` reaches the private `DirectByteBuffer(long, int)` constructor; production JVMs need `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED`.

---

## Open questions still on the table

**F. Footer size.** Set to 0 in the fragment / catalog row for `.db` files (parquet-specific concept). Confirm DuckLake spec doesn't reject 0 here, and decide whether to populate something meaningful (e.g. DuckDB's metadata block size).

**G. `data_file_format` cross-engine compatibility.** Setting `'duckdb'` in the spec field makes tables Trino-DuckLake-only. For production: gate behind a feature flag, document the tradeoff visibly. Intentionally accepted in Phase 1 (we own both ends in the experiment).

**H. `column_size_bytes`.** Set to 0 on duckdb fragments. Parquet uses real compressed-bytes-per-row-group from the footer. Could affect DuckLake compaction heuristics if/when they ship. DuckDB exposes per-block storage info via `pragma_storage_info` if we want to populate it.

---

## Phase 2+ (out of scope for this TODO file)

The original concept (`archive/CONCEPT-duckdb-as-parquet-file-cache.md`) goes beyond
what's tracked here:

- Auto-promotion of hot parquet tables to a duckdb-format cache (the original "DuckDB as a Parquet cache layer" thesis)
- Soft affinity for splits (Phase 0 in the concept doc — ConsistentHashingHostAddressProvider)
- Partition-scoped `.db` files (open question #7 in the concept doc — collapse a partition's worth of parquet into one `.db` for cross-table joins inside DuckDB)
- High-cardinality partitioned writes (current implementation works for low-cardinality; many-partition workloads may need a writer-side DuckDB instance pool)

These belong in their own design + tracking document when work starts.

## OTHER RESEARCH

is this duckdb cache extension useful? https://duckdb.org/community_extensions/extensions/cache_httpfs
and this https://duckdb.org/community_extensions/extensions/cache_prewarm

and other file formats
arrow - https://duckdb.org/community_extensions/extensions/nanoarrow
lance - https://duckdb.org/docs/current/core_extensions/lance
vortex - https://duckdb.org/docs/current/core_extensions/vortex

