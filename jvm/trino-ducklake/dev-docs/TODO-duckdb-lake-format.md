# TODO: DuckDB as a first-class data file format

Living tracker for DuckDB-as-DuckLake-data-file-format work.
Detail on completed work lives in the **Status log** at the bottom; the top of
this file is what's *open*. If chat context is lost, this file is the recovery
point.

---

## Phase 1 + 1.5 — Done ✅

End-to-end working: write `.db` files via Trino CTAS / INSERT, read them back
through Trino, mix with parquet tables in the same query, predicate +
projection pushdown into DuckDB SQL, column stats persisted to the DuckLake
catalog (cross-engine introspection works), Arrow-stream writer (default) for
columnar bulk loads.

**User-facing surface:**

- `SET SESSION ducklake.data_file_format = 'duckdb'` (default `'parquet'`)
- `CREATE TABLE foo (...) WITH (data_file_format = 'duckdb')` overrides per-CREATE
- `SET SESSION ducklake.duckdb_writer_mode = 'arrow_stream'` (default; `'appender'` available as fallback)
- Partitioned tables work on the duckdb path (`partitioned_by` table property, identity transforms).

**Known runtime requirement (production deploys):**

- Trino worker JVMs need `--add-opens=java.base/java.nio=ALL-UNNAMED` and `--add-opens=java.base/java.lang=ALL-UNNAMED` for Apache Arrow's C-data interface. Documented in `compose/README.md`; injected via `JAVA_TOOL_OPTIONS` in the dev `docker-compose.yml`.

See the **Status log** for per-step detail.

---

## Phase NEXT — open work, not yet started

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

**Sequencing.** Land N1 and N2 first (they're cheap and pay off independently),
then take this on. N3 is a real architectural shift — probably its own design
document and a separate epic. Write it as an additive layer (Flight worker
mode behind a session/connector flag) so we can A/B against the in-process
path the same way we did with the Arrow-stream writer.

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

**B. Per-JVM DuckDB instance scope.** Step 3 ships per-split independent DuckDB instances (one `jdbc:duckdb:` per page source). Trade-off: highest spinup cost but trivial lifecycle and full isolation. If profiling later shows native-instance init is hot, introduce a per-JVM `DuckDbInstanceRegistry` that hands out duplicated connections via `DuckDBConnection.duplicate()`. Same trade-off applies to the writer side. No interface change required.

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
