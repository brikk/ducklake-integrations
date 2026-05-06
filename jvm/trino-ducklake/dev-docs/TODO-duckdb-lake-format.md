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

**Practical perf shape (verified on real OVH-backed bench, sf=0.5 scale, post-fix):**

| Workload | parquet | duckdb materialize (warm) | duckdb httpfs |
|----------|---------|---------------------------|---------------|
| TPC-H Q1-shape lineitem scan | ~5 s | ~1–2.5 s | ~7–10 s |
| lineitem ⨝ orders aggregate join | ~5–11 s | ~2–10 s | ~8–15 s |

Materialize wins repeat reads (DuckDB's columnar reads from local disk are the floor). httpfs is currently a *cold one-shot* mode only — every query opens a fresh in-process DuckDB instance, so there's no cross-query block cache. Per-JVM instance pooling (open question B) would change that calculus; until then, `auto`'s 64 MiB threshold keeps typical workloads on the materialize path.

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
  and with mixed parquet + duckdb (rule 26 from N1's matrix lives here too).
- Time travel: `FOR VERSION AS OF` against a snapshot where the table had
  duckdb files; same against a snapshot where ADD COLUMN happened.
- `$snapshots`, `$partitions`, `$file_modifications` system tables — verify
  they return the right rows when the underlying table has duckdb files.

**T4. Type round-trip matrix on duckdb-format reads + writes.** Covers the gap
that hid the UUID bug for so long. One test per supported scalar type that does
the full round trip (CTAS via arrow_stream → SELECT, CTAS via appender → SELECT,
INSERT into pre-existing table via both writers → SELECT). Includes NULL
handling per type. Excludes complex types (ROW/ARRAY/MAP — those are N9 below).

These are the test-gap items. None are blockers in code; all are blockers for
"we know it works." Sequence them however; T4 is the most likely to surface
another UUID-shaped silent bug, so probably worth doing first.

### N3. Out-of-process DuckDB workers via Swanlake (Arrow Flight)

(Still open — see below. The big architectural shift; design first, then a separate epic.)

### N4. Per-JVM DuckDB instance pool (was open question B; now load-bearing)

**Why this got promoted.** With N2 landed, httpfs is functionally "cold one-shot" only — the live OVH benchmark (in the table at the top) shows httpfs is consistently 2–5× slower than materialize on warm reads, and never improves across queries because each query opens a fresh in-process DuckDB instance. There's no cross-query httpfs block cache, no reused secret/extension load, no warm catalog metadata.

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

Originally listed in the N2 proposal; deferred so we have N4's instance-pooled httpfs first. Once httpfs is competitive for repeated reads, the table-property or SQL-hint override becomes meaningful:

- `SET SESSION` override per query (already have via `duckdb_read_mode`).
- `CREATE TABLE foo WITH (duckdb_read_mode = 'materialize')` for hot tables.
- A future cache manager could populate this automatically based on access patterns.

---

## Done in Phase NEXT (kept here briefly; full detail in Status log)

### N1 — DONE (committed `9c4c595`, 2026-05-05)

INSERT inherits format from existing data files. Precedence chain:
1. CTAS-only `WITH (data_file_format = ...)` clause
2. Session property `ducklake.data_file_format` if explicitly set
3. Format of the most recent active data file in the table
4. Connector default (`parquet`)

Implementation: session-property default flipped to `null`, getter returns `Optional<String>`; new `DucklakeCatalog.getLatestDataFileFormat(tableId, snapshotId)`; `resolveWriteFormat` helper in `DucklakeMetadata` runs the chain for `beginInsert` and `beginMerge` (CTAS keeps its WITH-clause-first chain since rule 3 doesn't apply to a fresh table).

Test: `TestDucklakeFileFormatPrecedence` (17 tests covering the matrix + cross-table isolation + invalid-value rejection).

### N2 — DONE (uncommitted; 2026-05-06)

httpfs read path + auto/materialize/httpfs read mode. Implementation: new `DuckDbS3Config` reads same `s3.*` keys the FileSystemModule consumes; new `DuckDbAttachTarget` sealed interface (LocalPath | HttpfsS3); `DuckDbFilePageSource.initialize()` switches on it and runs `INSTALL httpfs; LOAD httpfs; CREATE SECRET ...; ATTACH 's3://...'` for the httpfs branch; `DucklakePageSourceProvider` decides per-split based on `duckdb_read_mode` session property + `ducklake.duckdb.auto-httpfs-threshold` connector config (default 64 MiB).

Tests: `TestDuckDbS3Config` (8 unit tests for SQL rendering / parsing); `TestDucklakeConfig` extended with threshold defaults + parsing; `TestDucklakeDuckDbReadMode` (5 integration tests for routing — explicit-httpfs on local FS hits a clean error, auto with low threshold routes to httpfs, parquet tables ignore the property).

Out of scope (deferred): real httpfs e2e against MinIO testcontainers (manual verification works via the compose stack); per-table / SQL-hint override (now N6 above).

### Arrow-stream writer correctness fix — DONE (uncommitted; 2026-05-06)

**The bug.** `DuckDbArrowStreamFileWriter.populateRoot()` was calling `vector.reset()` per batch. Arrow Java's `BaseFixedWidthVector.reset()` does `setZero(0, capacity)` on the existing buffer's memory in place. The previous batch's exported Arrow C-data still held pointers to those buffers (Apache Arrow Java's retain/release refcount keeps them alive); DuckDB's INSERT pipeline can be reading batch N when we begin populating batch N+1. Result: in-place memory zero corrupted batch N's data DuckDB was still reading. Symptom: non-deterministic `count(DISTINCT)` reduction (~5–60% of values zeroed), `min(custkey) = 0` even when source had no zeros, `count(*)` correct (queued atomically before reads happen).

**The fix.** Drop `vector.reset()`. `populateVector` already calls `allocateNew(rowCount)` which calls `clear()` (decrement buffer refs without touching the underlying memory) and then allocates fresh buffers. Old buffers stay readable for DuckDB until DuckDB calls release. That's the lifecycle Apache Arrow's C-data interface is built for.

Test: `TestDucklakeDuckDbArrowStreamWriter.testArrowStreamPreservesAllDistinctValuesFromConnectorSource`. Reproduces by CTAS-from-parquet with 100K distinct BIGINTs and asserting all distinct values survive. Failed reliably pre-fix (88174, 82157, 80481, 38999 across runs); passes 3 runs in a row post-fix. Also caught by the production OVH bench query.

---

## Historical N1/N2 design notes (kept for reference; superseded by Done sections above)

### N1. INSERT inherits format from existing data files

**Problem.** Today the `data_file_format` value lives only on individual
`ducklake_data_file` rows. The session property and the `WITH (data_file_format = 'duckdb')`
clause both drive the *current* write — neither persists on the table. So:

```sql
-- Default session is parquet
CREATE TABLE foo WITH (data_file_format = 'duckdb') AS SELECT 1 AS id;
-- Produces foo's first .db file (correct: WITH clause used).
INSERT INTO foo VALUES (2);
-- Currently writes a parquet file alongside the .db (wrong: WITH was lost).
```

**Proposal — no schema changes, no extension tables.** Resolve the format at
`beginInsert` time with this precedence:

1. Explicit `WITH (data_file_format = ...)` on the statement (only valid on CTAS, since INSERT doesn't accept `WITH`).
2. Session property `ducklake.data_file_format` if the user explicitly set it.
3. **Match the format of the most recent data file already in the table** (look up `ducklake_data_file` for this `table_id` in the current snapshot, take the latest by `data_file_id` descending). This is the workflow case: CTAS writes `.db`, later INSERTs without explicit overrides automatically continue with `.db`.
4. Connector default (`parquet`) — only hits when (a) no session prop, no WITH, and (b) the table has no data files yet (`CREATE TABLE foo (a int)` followed by `INSERT INTO foo VALUES (1)` with no other context). Edge case; user can avoid by using CTAS or setting the session prop on first insert.

**No schema-level property yet.** Wait for a DuckLake spec standard around schema/table extensible properties before adding extension tables. The "match latest file" approach above gets us most of the way without it.

**What this gets us:** the natural CTAS-then-INSERT workflow keeps a consistent format per table, with no extra ceremony. `CREATE TABLE foo WITH (data_file_format = 'duckdb') AS SELECT ...` followed by any number of `INSERT INTO foo` statements all produce `.db` files — because the first one set the format and subsequent inserts match.

**What it doesn't get us:** schema-wide defaults (`CREATE SCHEMA s WITH (data_file_format = 'duckdb')` would need either a Trino schema property + spec extension, or a session property). And empty-table CREATE then first-INSERT can't infer intent — defaults to parquet.

**Implementation sketch.**
- *Prerequisite:* change the `data_file_format` session property default from `"parquet"` to `null`, and `getDataFileFormat(session)` from `String` to `Optional<String>`. Today there's no way to distinguish "user set the session prop to parquet explicitly" from "user didn't set it; default is parquet" — both come back as `"parquet"`. With null-default we get that distinction at zero cost (validator already only runs on explicit SETs, so an absent property is just absent). Required for the precedence chain to actually behave differently when the user *did* set the session prop vs. when they didn't.
- `DucklakeMetadata.beginInsert`: query catalog for `ducklake_data_file` rows where `table_id = handle.tableId() AND end_snapshot IS NULL`, ordered by `data_file_id DESC LIMIT 1`. If found, use its `file_format`. If not, fall through to session property → connector default.
- One method on `DucklakeCatalog` interface: `Optional<String> getLatestDataFileFormat(long tableId, long snapshotId)`. Cheap query; runs once per INSERT statement (not per row).
- `beginMerge` follows the same pattern (it does an internal insert).
- No change to `beginCreateTable` — the WITH clause + session property already govern that. Same null-default change applies to the empty-CTAS case naturally.

**Test coverage (precedence matrix — must be exhaustive; correctness here is load-bearing for every later phase).**

The four precedence inputs are: (a) explicit `WITH (data_file_format = ...)` on CTAS, (b) session property explicitly set (parquet | duckdb), (c) latest existing data file format in the table (parquet | duckdb | none), (d) connector default (parquet). Each test below asserts both the *written* file extension/format and the row in `ducklake_data_file.file_format`.

*CTAS resolution (no existing files; only WITH + session + default apply):*

1. Session unset, no WITH → parquet file. (Connector default; baseline.)
2. Session unset, `WITH (duckdb)` → `.db` file.
3. Session unset, `WITH (parquet)` → parquet file. (Explicit WITH = default; behaves identically but exercises WITH plumbing for the explicit-parquet case.)
4. Session = parquet, no WITH → parquet.
5. Session = duckdb, no WITH → `.db`.
6. Session = parquet, `WITH (duckdb)` → `.db`. (WITH overrides session.)
7. Session = duckdb, `WITH (parquet)` → parquet. (WITH overrides session — both directions matter.)
8. Empty CTAS (`AS SELECT * FROM src WHERE 1=0`) writes zero data files; subsequent INSERT must fall to session/default since rule (c) has no input. Cover with: empty CTAS WITH duckdb, then INSERT with no session/no WITH → parquet (edge case called out in the proposal — assert and document).

*INSERT resolution (the meat of N1; rule (c) "match latest data file" is the new behavior):*

9. CTAS duckdb (rule a), then plain INSERT with session unset → `.db`. (Rule c picks up duckdb.)
10. CTAS parquet, plain INSERT, session unset → parquet. (Rule c picks up parquet; verifies the new code path is symmetric.)
11. CTAS duckdb, INSERT with session = parquet → parquet. (Rule b beats rule c; this is the override-the-table case.)
12. CTAS parquet, INSERT with session = duckdb → `.db`. (Same, opposite direction.)
13. CTAS duckdb, two consecutive plain INSERTs, session unset for both → both `.db`. (Rule c stays stable across multiple inserts.)
14. CTAS duckdb, INSERT session = parquet (now table has 1×.db + 1×parquet), then plain INSERT session unset → must pick **most recent** (parquet). Asserts the "latest by `data_file_id` desc" tie-break is what's specified. **Then** plain INSERT again, session unset → parquet again (compounds: once flipped, stays flipped until the user flips back).
15. Inverse of 14: CTAS parquet, INSERT session = duckdb, plain INSERT → `.db` (latest).
16. CREATE TABLE (no AS), then plain INSERT with no session, no WITH → parquet. (Empty-table fallback to default; edge case from proposal.)
17. Same as 16 but session = duckdb on first INSERT → `.db`. (Establishes format from session when no files exist yet.)
18. CREATE TABLE empty, INSERT session = duckdb (writes `.db`), then plain INSERT session unset → `.db` (rule c took over once the first file existed).

*MERGE / UPDATE / DELETE resolution (`beginMerge` path; same precedence chain expected):*

19. MERGE on a duckdb-only table with session unset → new files `.db`.
20. MERGE on a parquet-only table with session unset → new files parquet.
21. MERGE on a duckdb-only table with session = parquet → new files parquet (rule b beats c on merge too).
22. UPDATE/DELETE that produces new data files (not just position deletes) follows the same chain — repeat 19–21 with UPDATE.

*Cross-table isolation (no leakage between tables in same session):*

23. Two tables `t_duck` (CTAS duckdb) and `t_parq` (CTAS parquet) coexist; plain INSERTs into each with session unset → each table keeps its own format independently. (Guards against accidental "global last format" state.)
24. Same session, INSERT into `t_duck` then into `t_parq` (no overrides on either) — neither contaminates the other.

*Partitioned tables (rule c needs to look across all partitions for "latest"):*

25. CTAS partitioned duckdb, INSERT new partition with no overrides → `.db` in the new partition.
26. Mixed-format on a partitioned table (CTAS duckdb, INSERT WITH session = parquet adds parquet files in some partitions), then plain INSERT — picks up format of the globally-latest data file regardless of partition. Document this; if we want per-partition inheritance later, it's a separate enhancement.

*Catalog-row assertions (cross-engine introspection — DuckDB extension reads these):*

For every test above that writes a file, additionally assert the `ducklake_data_file.file_format` value in the catalog matches the on-disk format. A bug where the fragment says `duckdb` but the writer wrote parquet (or vice versa) would break the read path silently — these assertions are the early-warning canary.

*Negative / validation tests:*

27. `SET SESSION ducklake.data_file_format = 'orc'` → session-property validator rejects (already in place; pin the behavior).
28. `WITH (data_file_format = 'orc')` on CTAS → table-property validator rejects.
29. `WITH (data_file_format = 'duckdb')` on a non-CTAS statement (if Trino allows it syntactically anywhere we don't expect) → rejected or ignored cleanly; do not silently apply to subsequent INSERTs.

*Read-side smoke (mixed-format tables must still SELECT correctly — exercises that rule c didn't break the reader):*

30. After test 14 (mixed `.db` + parquet in one table), `SELECT *` returns the union with correct row counts; predicates push down correctly into both file types; `COUNT(*)` matches. Already covered by existing read tests for homogeneous tables — extend one to the heterogeneous case.

Tests live alongside `TestDucklakeDuckDbFormatWrite` (write-side precedence) and `TestDucklakeDuckDbFormatRead` (mixed-format read smoke). Use distinct table names per test to avoid catalog state leakage; don't rely on `@BeforeEach` cleanup alone for these — the bugs we're guarding against are exactly the ones that mis-attribute state across tables.

### N2. httpfs read path + writer-time choice

**Problem.** Cold reads materialize the entire `.db` file to local tmp before
DuckDB can `ATTACH` it. For one-off queries against large rarely-touched files
this is wasteful — DuckDB's httpfs extension can stream block-level reads from
S3 directly, fetching only the blocks the query actually touches.

**Proposal.**

1. Plumb DuckDB's httpfs extension on the read connection: at `DuckDbFilePageSource.initialize()` time, run `INSTALL httpfs; LOAD httpfs; CREATE SECRET ...` from the same S3 credentials Trino's `ducklake.properties` already has, then `ATTACH 's3://bucket/foo.db' AS s_<id> (READ_ONLY)` directly — skipping the local materialize step.
2. Add session property `ducklake.duckdb_read_mode` with values:
    - `materialize` (current; downloads file once, all subsequent reads local)
    - `httpfs` (DuckDB streams blocks from S3 per query; no local copy)
    - `auto` (default) — heuristic: small files materialize, large files httpfs; or based on a future cache-promotion signal.
3. Per-query/per-table override via a Trino SQL hint or table property — the eventual cache manager (Phase 2 of original concept doc) would set this based on access patterns: hot tables → materialize, cold tables → httpfs.

**Tradeoffs already documented** in the table in the previous conversation: materialize wins on warm reads; httpfs wins on cold one-shots. Auto mode is the production answer; manual selection is for benchmarking and explicit control.

### N3. Out-of-process DuckDB workers via Swanlake (Arrow Flight)

**Important.** Move DuckDB out of the Trino JVM into separately-supervised
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
  DuckDB's httpfs and ATTACH the remote `.db` directly. (See N2 — the same
  decision matrix applies, but now the materialized cache, if any, lives on
  the worker side, not the Trino JVM side.)
- The `--add-opens` JVM flags requirement on the Trino server JVM
  *disappears* — the Arrow C-data interface plumbing all moves into the
  Swanlake worker. Trino-side just speaks Flight (regular Arrow over the
  network, no DirectByteBuffer reflection tricks needed).
- New connector config: pool sizing, TTL, worker launcher (path to Swanlake
  binary, or sidecar mode), transport (TCP/UDS), per-worker resource limits.

**Sequencing.** N1 and N2 are landed. **N4 first** (per-JVM instance pool) — closes
the perf gap that's part of the justification for N3 and is a much smaller change.
Then N3 if isolation/cancellation/resource-accounting drive the case for going
out-of-process. Write N3 as an additive layer (Flight worker mode behind a
session/connector flag) so we can A/B against the in-process path the same way
we did with the Arrow-stream writer.

---

## Verified facts (relevant to future work)

- `fileFormat` is a plain `String` column on `ducklake_data_file` — no enum to extend; the spec's `data_file_format` field accepts arbitrary strings.
- `ducklake_schema` and `ducklake_table` rows have no extensible properties column. Persisting schema/table-level metadata requires connector-private extension tables (the same pattern as `ducklake_brikk_*` proposed above).
- DuckDB writer SQL: `ATTACH 'new.db' AS new (READ_WRITE); CREATE TABLE new.t AS SELECT...; DETACH new` (the original concept doc's `COPY (...) TO 'file.db' (FORMAT DUCKDB)` is *not* the path).
- DuckDB reader SQL: `ATTACH '/path/foo.db' AS s (READ_ONLY); SELECT * FROM s.<table>` works for local files. httpfs extends this to remote S3 paths.
- `DuckDBResultSet.arrowExportStream(allocator, batchSize)` and `DuckDBConnection.registerArrowStream(name, ArrowArrayStream)` are the canonical DuckDB-Java entry points for the Arrow C-data interface (read and write directions respectively). `registerArrowStream` strictly requires a real `org.apache.arrow.c.ArrowArrayStream` instance — no shape-compatible wrappers.
- Apache Arrow's `MemoryUtil.directBuffer()` reaches the private `DirectByteBuffer(long, int)` constructor; production JVMs need `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED`.

---

## Open questions still on the table

**B. ~~Per-JVM DuckDB instance scope.~~** **Promoted to N4 (above)** after the OVH bench made the perf cost concrete: httpfs has no shared state across queries because each query gets a fresh in-process DuckDB.

**F. Footer size.** Set to 0 in the fragment / catalog row for `.db` files (parquet-specific concept). Confirm DuckLake spec doesn't reject 0 here, and decide whether to populate something meaningful (e.g. DuckDB's metadata block size).

**G. `data_file_format` cross-engine compatibility.** Setting `'duckdb'` in the spec field makes tables Trino-DuckLake-only. For production: gate behind a feature flag, document the tradeoff visibly. Intentionally accepted in Phase 1 (we own both ends in the experiment).

**H. `column_size_bytes`.** Set to 0 on duckdb fragments. Parquet uses real compressed-bytes-per-row-group from the footer. Could affect DuckLake compaction heuristics if/when they ship. DuckDB exposes per-block storage info via `pragma_storage_info` if we want to populate it.

---

## Phase 2+ (out of scope for this TODO file)

The original concept (`CONCEPT-duckdb-as-parquet-file-cache.md`) goes beyond
what's tracked here:

- Auto-promotion of hot parquet tables to a duckdb-format cache (the original "DuckDB as a Parquet cache layer" thesis)
- Soft affinity for splits (Phase 0 in the concept doc — ConsistentHashingHostAddressProvider)
- Partition-scoped `.db` files (open question #7 in the concept doc — collapse a partition's worth of parquet into one `.db` for cross-table joins inside DuckDB)
- High-cardinality partitioned writes (current implementation works for low-cardinality; many-partition workloads may need a writer-side DuckDB instance pool)

These belong in their own design + tracking document when work starts.

---

## Status log

- **2026-05-03** — Plan agreed. Concept doc updated with revised approach (per-conversation-mode option 1 vs option 2 for read, partition-level future idea, brutal-Phase-1 framing). TODO file created. Started Step 1.
- **2026-05-03** — Step 1 (skeleton plumbing) done. Session property `ducklake.data_file_format` plumbed end-to-end: `parquet` (default) keeps existing writer/reader paths untouched, `duckdb` throws `NOT_SUPPORTED` at both ends. `ducklake_data_file.file_format` row sourced from the fragment instead of hardcoded. 5/5 skeleton tests green. Side fix: `@Deprecated` getter/setter pair mismatch in `DucklakeConfig` was blocking all integration tests on main.
- **2026-05-04** — Step 2 (Appender writer) done. CTAS / INSERT with the duckdb session property produces `.db` files via the DuckDB Appender API and uploads through `TrinoFileSystem`. `DucklakeFileWriter` interface extracted; `ParquetFileWriter` and `DuckDbFileWriter` are the two impls. Type coverage: BOOLEAN/integer family/REAL/DOUBLE/DECIMAL/DATE/TIMESTAMP/TIMESTAMPTZ/VARCHAR/VARBINARY/UUID. Per-JVM `DuckDbInstanceRegistry` skipped — one connection per writer is simpler.
- **2026-05-04** — Step 3 (reader) done. `DucklakeMaterializedFileCache` (per-JVM, SHA-256-keyed atomic writes) downloads `.db` to local tmp; `DuckDbFilePageSource` opens a per-split DuckDB JDBC instance, ATTACHes READ_ONLY, runs `arrowExportStream` for the Arrow C-data export; `DucklakeArrowToPageConverter` materializes batches into Trino `Page`s. Delete files and `$row_id` injection reuse parquet-path wrappers. Side fixes: test JVM `--add-opens=java.base/java.nio,java.lang=ALL-UNNAMED`; test postgres `max_connections=500`.
- **2026-05-04** — Step 4 (predicate pushdown) done. `DuckDbWhereClauseTranslator` translates `TupleDomain` → DuckDB SQL `WHERE` (eq, IN, ranges, IS NULL/NOT NULL, value-or-null, multi-column AND, range unions). Best-effort: anything not safely translatable falls through to Trino's filter operator. Literal formatting for BOOLEAN/integer/REAL/DOUBLE/DECIMAL/DATE/short-TIMESTAMP/VARCHAR. Identifier quoting handles `"` in column names. Projection pushdown was already in Step 3.
- **2026-05-04** — Empty-projection (`COUNT(*)`) handled via synthetic `SELECT 1 FROM t [WHERE ...]` discarded in the converter; emits empty-block `Page(rowCount)` per batch so counts and delete-file filtering still get the right positions.
- **2026-05-04** — Compose JVM args added (`JAVA_TOOL_OPTIONS` in `docker-compose.yml`); the test JVM had had the flags but the production server JVM didn't, masking the empty-projection error. Documented as production heads-up in `compose/README.md`.
- **2026-05-04** — Bootstrap defensive against missing `column_stats`. With duckdb-format tables in the lake, the DuckDB extension's `duckdb_tables()` / `TransformGlobalStats` paths crashed on `ducklake_file_column_stats` rows our writer didn't yet emit. Patched `compose/bootstrap/init_ducklake.py` (try/except + `information_schema.tables` fallback). Documented `down -v` recovery path. Real fix landed next day.
- **2026-05-05** — Bootstrap script generalized for non-MinIO S3 endpoints (`S3_USE_SSL`, `S3_URL_STYLE`, `S3_REGION` env vars; SECRET renamed `lake_s3`). User added a second OVH-backed catalog (`ducklake_ovh.properties`); compose docker-compose mounts updated to per-file mounts so the image's built-in catalogs (tpch, etc.) aren't hidden.
- **2026-05-05** — Phase 1.5: column-stats extraction. `DuckDbFileWriter.extractColumnStats()` runs a single aggregate query against the freshly-written DuckDB table (after appender flush, before DETACH) and persists per-column `value_count`/`null_count`/`min_value`/`max_value` in the same string format the parquet path uses. DuckDB extension's introspection no longer chokes on missing rows.
- **2026-05-05** — `data_file_format` table property added (`CREATE TABLE foo (...) WITH (data_file_format = 'duckdb')`). Overrides session-level default for that one statement. Per-table persistence is Phase NEXT N1.
- **2026-05-05** — Per-Page `appender.flush()` added to bound memory; without it Appender buffered the entire CTAS until close, OOMing on large loads.
- **2026-05-05** — Arrow-stream writer landed. New session property `ducklake.duckdb_writer_mode` chooses `appender` or `arrow_stream` (now the default). `arrow_stream` runs `INSERT INTO target SELECT * FROM <registered_arrow_stream>` on a worker thread; the page-sink thread feeds Pages through a bounded queue (`ArrayBlockingQueue` cap 2); a custom `ArrowReader` populates a reused `VectorSchemaRoot` per `loadNextBatch()`. `Data.exportArrayStream` wires the reader into a real `ArrowArrayStream` (which is the only thing DuckDB's `registerArrowStream` accepts). End-of-stream signaled via a sentinel `Page`. Memory bounded; per-cell JNI replaced by one C-data round-trip per Page.
- **2026-05-05** — Default writer mode flipped from `appender` to `arrow_stream` after live TPC-H sf1-half benchmark: arrow_stream lineitem CTAS ~12s vs appender 2-min-ish then crash. Reads on duckdb-format tables 40–75% faster than parquet warm. `ducklake.duckdb_writer_mode = 'appender'` still available as opt-in fallback.
- **2026-05-05** — Partitioned writes on the duckdb path verified. `TestDucklakeDuckDbFormatWrite.testPartitionedCtasInDuckDbFormat`: CTAS with `partitioned_by = ARRAY['region']` produces one `.db` file per partition (`region=us/...`, `region=eu/...`, `region=jp/...`); SELECT round-trips correctly; partition pruning works; cross-partition aggregation works. Code path was format-agnostic; just needed the test. (Open question D from the original list: closed.)
- **2026-05-05** — N1 landed and committed (`9c4c595`). INSERT/MERGE inherit format from latest existing data file when no session/WITH override is set. Session getter changed to `Optional<String>`; new `DucklakeCatalog.getLatestDataFileFormat`. 17 precedence-matrix tests in `TestDucklakeFileFormatPrecedence` covering CTAS resolution, INSERT inheritance (incl. "flip stays flipped"), UPDATE/MERGE, cross-table isolation, negative validation.
- **2026-05-06** — N2 implemented (uncommitted). `duckdb_read_mode` session property + `ducklake.duckdb.auto-httpfs-threshold` connector config (default 64 MiB). `DuckDbS3Config` reads same `s3.*` keys the FileSystemModule consumes; `DuckDbAttachTarget` sealed interface dispatches in `DuckDbFilePageSource.initialize()`. Routing tests in `TestDucklakeDuckDbReadMode` (5 tests pinning the decision matrix using local-FS clean errors as the negative-path signal).
- **2026-05-06** — Arrow-stream writer correctness fix (uncommitted). Removed in-place `vector.reset()` zeroing in `populateRoot`; was corrupting the previous batch's data DuckDB still held C-data pointers to. Reproduced with non-deterministic distinct counts (88174 / 82157 / 80481 / 38999 across runs of `TestDucklakeDuckDbArrowStreamWriter.testArrowStreamPreservesAllDistinctValuesFromConnectorSource`); 3 runs in a row green post-fix; full sweep 605 tests / 0 failures.
- **2026-05-06** — Compose Trino container `mem_limit: 3g` set explicitly (was using daemon defaults); JVM heap continues to auto-size via image's `MaxRAMPercentage` (~2.4 GiB), leaving headroom for native (DuckDB JNI, Arrow off-heap), metaspace, and code cache.
- **2026-05-06** — OVH-backed perf characterization captured in the table at the top: materialize wins repeat reads (1–2.5s); parquet flat ~5s; httpfs 7–11s and never improves due to no cross-query DuckDB state. Promoted "open question B" to N4 as a result.
- **2026-05-06** — UUID end-to-end fix on the duckdb path. Three problems compounded: (a) `DuckDbArrowStreamFileWriter` had no UUID branch in `toArrowType`/`populateVector` (silent regression vs the appender writer when arrow_stream became default); (b) `DucklakeArrowToPageConverter` had no UUID converter case at all (so even appender-written UUID files were unreadable through Trino); (c) `DucklakePageSink` was building the parquet schema unconditionally in its constructor, so `ParquetSchemaConverter` rejected UUID before our writer dispatch ran — fix: build parquet schema only when `fileFormat == parquet`. Empirically discovered while testing that DuckDB's Arrow exchange for UUID uses Utf8 (the printed hex form), not FixedSizeBinary(16) — both directions now use Utf8 and parse via `UuidType.javaUuidToTrinoUuid`/`trinoUuidToJavaUuid`. Tests: `TestDucklakeDuckDbArrowStreamWriter.testUuidRoundTripThroughArrowStream` and `TestDucklakeDuckDbFormatWrite.testUuidRoundTripThroughAppender`. Full sweep 607 / 0 / 0.
