# DESIGN: DuckDB Files as a Parquet Cache Layer

**Status:** Exploratory design sketch. No code yet.
**Scope:** trino-ducklake connector only. Does not and cannot apply to Hive/Iceberg/Delta without patching them — we control the full stack here.

## 1. The idea in one paragraph

Watch which parquet data files are read hot (either individually or in co-read groups). On a best-guess "home" node for each hot file, transparently convert the parquet into a DuckDB database file. Publish a pointer to that `.db` in the DuckLake catalog alongside the original parquet data-file entry. When splits are generated, if a cached `.db` exists, the split carries the `.db` path instead of the parquet path, plus a node affinity hint pointing at the node that has it local. The coordinator routes the split to that node under SOFT_AFFINITY. A new `DuckDBFilePageSource` on the worker opens the `.db` and streams rows. If the cache is missing, absent, or the affinity target is loaded, the split falls back to the parquet path and everything works as it does today.

The whole feature is additive — remove the cache table and it's a no-op.

---

## 2. What Trino gives us for free

### 2.1 Consistent hashing of file path → preferred node

**Class:** `io.trino.filesystem.cache.ConsistentHashingHostAddressProvider`
**Location:** `lib/trino-filesystem/src/main/java/io/trino/filesystem/cache/ConsistentHashingHostAddressProvider.java`

- **Hash function:** MetroHash (`org.ishugaliy.allgood.consistent.hash`, `DefaultHasher.METRO_HASH`). Not Murmur3.
- **Ring identity:** `node.getNodeIdentifier()` — the stable `node.id`, not hostname or IP. Survives worker restarts and re-addressing.
- **Ring rebuild:** every 5 seconds, reflects node churn. Consistent hashing means node add/remove only reshuffles a fraction of the keyspace.
- **Split key format:** `path:offset:length` (via the static helper `CachingHostAddressProvider.getSplitKey(path, offset, length)`). This means different byte ranges of the same parquet can go to different nodes — useful for large files, but for our use case where we convert the *whole* file to a `.db`, we'd key on `path:0:totalLength` or a stable file-level key.
- **Config:** `fs.cache.preferred-hosts-count` (default 2). The provider returns the top-N nodes; primary + a secondary for failover warmth.
- **API:** `getHosts(String splitKey, List<HostAddress> defaultAddresses)` → `List<HostAddress>` in preference order.

**How Iceberg/Hive/Delta use it today:** inject `CachingHostAddressProvider`, call `getHosts(splitKey, allWorkers)` when generating splits, stuff the result into `ConnectorSplit.getAddresses()`. Trino then treats a non-empty `getAddresses()` as SOFT_AFFINITY — coordinator prefers the node, falls back under load.

**What this buys us:**
- A deterministic "home node" for any given file that both the conversion worker and the split manager can agree on without any extra coordination.
- Survives restarts: node IDs are stable, so the home node for a file is the same on Monday as it was on Friday.
- No JMX or system table exposes this externally. To compute it outside Trino, pull in the same `consistent-hash` library and feed it the live node IDs.

### 2.2 Trino's local filesystem cache (page-based, transparent)

**Interface:** `io.trino.filesystem.cache.TrinoFileSystemCache`
**Impl:** `io.trino.filesystem.alluxio.AlluxioFileSystemCache` (Alluxio page-store under the hood, wrapped by Trino)
**Plumbing:** `CacheFileSystem` wraps a delegate `TrinoFileSystem`. Installed in `FileSystemModule` when `fs.cache` is enabled — applies to *all* filesystem operations in that catalog.

Behavioral facts, verified from the source:

| Behavior | Answer | Citation |
|----------|--------|----------|
| Granularity | **Page-based**, 1 MB default pages (range 64 KB – 15 MB). NOT whole-file. | `AlluxioFileSystemCacheConfig.java:41` |
| On-demand? | **Yes, fetch-through.** Reading bytes `[offset, offset+len]` fetches and caches only the overlapping 1 MB pages. | `AlluxioInputHelper.java:138-159`, `AlluxioInputStream.java:128-135` |
| Random access / seek | **Fully supported.** Seek updates position; subsequent reads align to pages. DuckDB's random `.db` access works correctly and efficiently. | `AlluxioInputStream.java:209-222` |
| Read-ahead / prefetch | **None.** Only pages covering the actual read range are fetched. | `AlluxioInputHelper.java:189-196` |
| Eviction | **TTL-primary (default 7 days), LRU under space pressure.** TTL configurable via `fs.cache.ttl`. Size caps via `fs.cache.max-sizes` or `fs.cache.max-disk-usage-percentages`. | `AlluxioFileSystemCacheConfig.java:39,62-63,95` |
| Writes populate the cache? | **No.** `CacheFileSystem.newOutputFile()` calls `cache.expire(location)`; the Alluxio impl's `expire()` is currently a no-op, but writes do not self-populate pages either way. A subsequent read fetches from origin. | `CacheFileSystem.java:65-75`, `AlluxioFileSystemCache.java:83-84` |
| Invalidation on delete/rename | `expire()` called; currently a no-op in Alluxio impl. Effective invalidation relies on TTL. | `CacheFileSystem.java:78-100` |
| Pinning | **None.** Purely reactive — populated on read, evicted by TTL or space. | N/A |
| Scope | **Per-catalog global** — applied to every read/write through the wrapped `TrinoFileSystemFactory`. | `FileSystemModule.java:130-132,163-164` |
| Observability | **Aggregate JMX only** via `AlluxioCacheStats`: `externalReads`, `cacheReads` as `DistributionStat`. Per-stream `bytesReadFromCache` / `bytesReadExternally` in `AlluxioInput`. **No per-file or per-path hit-rate. No way to ask "is this specific path fully cached?"** | `AlluxioCacheStats.java:22-50` |

**So — to answer directly: yes, the cache does "smart hot page" stuff.** It caches individual 1 MB pages of a file on demand. If DuckDB seeks into a `.db` file and only touches 30 MB of a 2 GB file for a query, only those 30 pages get cached. On the next query touching overlapping pages, those are hits; the rest are still fetched on demand. This is exactly right for DuckDB's access pattern.

**What it does *not* do:**
- Prefetch ahead. No background page warming.
- Whole-file staging. If you want the whole thing local after one access, you have to read it all.
- Report per-path state. You cannot ask the cache "is `s3://.../foo.db` already here?"
- Pin things so they don't evict under pressure.

### 2.3 Summary: how Iceberg uses both together

Iceberg's `IcebergSplitManager` (for reference):
1. Injects `CachingHostAddressProvider` as an optional dep.
2. When generating a split for file `path` at range `[offset, len]`, calls `cachingHostAddressProvider.getHosts(getSplitKey(path, offset, len), defaultAddresses)`.
3. Puts that list into the split's `getAddresses()`.
4. When the split runs, it opens the path via `TrinoFileSystem` — which, if fs-cache is enabled for the catalog, is actually a `CacheFileSystem` and transparently reads from local page store when warm.

That's the entire integration. Everything else is automatic: SOFT_AFFINITY scheduling, page-level caching, eviction. Iceberg does not maintain its own cache, its own hash, or its own placement.

---

## 3. What we're building on top

Two independent features, which together create the end-user behavior.

### 3.1 Soft affinity for DuckLake splits (free win, do this first)

**Completely standalone of the DuckDB cache idea** — worth doing on its own. Today, `DucklakeSplit.getAddresses()` returns an empty list. Fix: inject `CachingHostAddressProvider` into `DucklakeSplitManager`, compute `getHosts()` for each split's parquet path, populate `getAddresses()`. Parquet files now get a stable home node, the existing Trino fs cache becomes effective for ducklake, hit rates go up.

This is a ~20-line change and is the precondition for everything below.

### 3.2 DuckDB-file cache register

**New catalog table** (in the DuckLake metadata DB, alongside `ducklake_data_file`):

```
ducklake_cached_duckdb_files (
  table_id        bigint,
  data_file_id    bigint,      -- original parquet file ID
  snapshot_id     bigint,      -- for MVCC / invalidation
  remote_path     varchar,     -- authoritative copy, e.g. s3://bucket/cache/<hash>.db
  local_subpath   varchar,     -- relative path under each worker's local cache dir
  duckdb_size     bigint,
  home_node_id    varchar,     -- node.id at conversion time, hint only
  status          varchar,     -- pending | ready | failed
  created_at      timestamp,
  last_used_at    timestamp,
  hit_count       bigint
)
```

- Keyed per `(data_file_id, snapshot_id)` so new snapshots simply miss until conversion catches up — MVCC invalidation is free.
- `remote_path` is the durable copy; `local_subpath` tells any worker where it would *find* a local copy if it has one (resolved against that worker's `ducklake.local-cache-dir` config).
- `home_node_id` is the node.id where conversion ran. It's a hint for debugging and for the conversion lock; actual split affinity at query time still uses `ConsistentHashingHostAddressProvider.getHosts()` so a node-id shuffle doesn't strand the cache row.
- `status = pending` reserves the slot during conversion (acts as a cross-node lock — see Section 5 question 6). Splits ignore `pending` and `failed` rows; only `ready` rows route to the DuckDB path.

The table fits in the existing DuckLake JDBC catalog (PostgreSQL or DuckDB itself, same as the main metadata). No new infrastructure.

### 3.3 Split flow

In `DucklakeSplitManager`:
1. Resolve data files for the query from the DuckLake catalog.
2. Batch-lookup `ducklake_cached_duckdb_files` for the `(data_file_id, snapshot_id)` pairs, filtering to `status = 'ready'`.
3. For each data file:
    - If a `ready` cache row exists → split carries `remote_path`, `local_subpath`, and the `data_file_id`. `getAddresses()` derived from `ConsistentHashingHostAddressProvider.getHosts(remote_path, workers)` — same hash space as the parquet path so home node selection is stable.
    - Else → existing parquet split path. `getAddresses()` derived from the same provider on the parquet path. (This is the Phase 0 win, independent of the cache feature.)
4. Split record gains optional fields: `Optional<String> duckDBRemotePath`, `Optional<String> duckDBLocalSubpath`. If both absent, behavior is unchanged.

The page source on the worker decides at execution time whether the local file actually exists; the split doesn't promise anything beyond "there is a cache row registered." This avoids any TOCTOU races between split planning and execution.

### 3.4 Page source routing

In `DucklakePageSourceProvider.createPageSource()`:
- If `split.duckDBLocalSubpath().isPresent()` and the local file actually exists at `<localCacheDir>/<localSubpath>` → `new DuckDBFilePageSource(localPath, projectedColumns, predicate, deleteFiles)`.
- Else → existing `createParquetPageSource()`.

The "cache row exists in catalog but local file is missing on this worker" case (which is the common case on non-home nodes, plus the always case after a worker disk wipe) silently falls back to parquet. No error, no fetch. This keeps the design strictly additive — broken cache state can never make a query fail.

`DuckDBFilePageSource` opens the `.db` via an embedded DuckDB instance pointed at a **local file path** (e.g. `ATTACH '/var/cache/ducklake/<id>.db' AS s (READ_ONLY)`). DuckDB needs a real local file for random access — its on-disk format is mmap-friendly and assumes a regular filesystem path. Delete files are applied the same way as the parquet path — the existing delete-merge wrapper pattern wraps either source.

**Data path: DuckDB → Arrow → Trino Page (NOT JDBC row API).**

Going through standard JDBC `ResultSet` (`getInt`, `getString` per cell) would dominate execution time and discard the columnar layout DuckDB already has. Instead:

1. The page source still uses the DuckDB Java driver to issue the query (it's the simplest way to manage the session, transaction, and prepared statements).
2. After execution, instead of iterating the `ResultSet` row-by-row, call **`DuckDBResultSet.arrowExportStream(allocator, batchSize)`** (JNI-backed). This returns an Arrow IPC stream populated directly from DuckDB's internal vectors — effectively zero-copy from DuckDB's vector buffers into Arrow `FieldVector`s.
3. For each Arrow `RecordBatch`, run an **Arrow → Trino Page converter** patterned on `plugin/trino-bigquery/src/main/java/io/trino/plugin/bigquery/BigQueryArrowToPageConverter.java`. That class is column-schema-agnostic, dispatches per Trino `Type`, and already handles nested arrays/structs. We adapt only the type-mapping (DuckDB column types → Trino `Type`) and reuse the converter mechanics.
4. Hand the resulting `Page` out of `ConnectorPageSource.getNextSourcePage()`.

This is the Arrow path. A fully native columnar path also exists — DuckDB Java exposes `DuckDBDataChunkReader` and `DuckDBReadableVector` (via `duckdb_jdbc_fetch()`) which give direct access to DuckDB's internal vectors without an Arrow round-trip — but it requires writing per-type vector→Block converters from scratch and the speedup over Arrow is marginal. Defer until measurements justify it.

**Arrow dependency:** Trino already pulls Apache Arrow Java (used by the BigQuery storage API plugin among others), so we don't add a new top-level dep — we add Arrow to the trino-ducklake module if it isn't there already.

**Pushdown opportunity (later):** because we control the SQL we send to DuckDB, projection and predicate pushdown are essentially free — just translate the Trino `TupleDomain` into a `WHERE` clause and only `SELECT` the projected columns. DuckDB then prunes pages before they ever materialize. Worth wiring in early because it's where the DuckDB path most clearly wins over the parquet path.

**Key consequence:** the Trino page cache is **not directly useful for the `.db` file itself**, because the cache stores opaque Alluxio page chunks on disk that we can't hand to DuckDB as a regular file path. We can't `ATTACH` a Trino-cached file. The page cache *is* still the win for parquet reads (Phase 0); for `.db` files we need a different approach.

**The natural shape:** the home node has the `.db` materialized as a real file on its local filesystem, written there by the conversion worker (which runs *on* that node). Other nodes don't have the `.db` and don't try to use it — they fall back to the parquet split path. This is consistent with how SOFT_AFFINITY actually behaves: most splits go to the home node; occasional spillover to other nodes degrades to parquet, which always works.

This is why we don't need to solve "how does DuckDB read through `TrinoFileSystem`" — the question is moot if the file is local. Embedded DuckDB extensions to teach it about `TrinoFileSystem` would be possible (custom C++ extension implementing DuckDB's `FileSystem` interface against a JNI bridge to `TrinoFileSystem`) but the value-to-effort ratio is poor compared to just keeping a local copy on the home node.

### 3.5 Writing the cache file (conversion path)

Because DuckDB needs a real local file (Section 3.4), the write path is dual-target:

1. **Local file on the home node.** The conversion worker, which runs on the home node by construction, writes `/var/cache/ducklake/<table_id>/<data_file_id>.db` (path configurable). This is what the page source `ATTACH`es. Writes here are direct file I/O, not through `TrinoFileSystem`.
2. **Remote copy to S3 (or whatever object store backs the catalog).** Written via `TrinoFileSystem` to a known path. Purpose: durability so we don't lose the cache on a worker disk failure; and a place from which a different node could re-fetch and re-materialize locally if the home node moves (consistent-hash ring change).

The cache row in `ducklake_cached_duckdb_files` records both `local_hint_path` (best-effort, may be missing on other nodes) and `remote_path` (authoritative).

**On read:**
- Split runs on the home node → page source opens the local file. Fast.
- Split runs on a non-home node (rare under SOFT_AFFINITY) → no local file present → page source falls back to the **parquet path**. We do not try to fetch the `.db` over the network for an opportunistic split. Parquet always works, and the page cache for the parquet file may already be warm on the non-home node anyway from prior queries.
- Worker restarts and the local file is gone (e.g. the cache directory is on tmpfs or has been cleaned) → page source detects missing local file → falls back to parquet → the next conversion pass re-materializes from S3.

**Why not fetch the `.db` from S3 on a non-home node?** Three reasons:
- We'd have to materialize the whole file locally before DuckDB can `ATTACH` it (DuckDB needs a real file path). That's expensive for what is supposed to be a fallback path.
- The Trino page cache stores Alluxio's internal page format, not a regular file, so we can't hand its on-disk pages to DuckDB.
- Parquet fallback is simpler, cheap, and exercised continuously.

**Cleanup:** local files are owned by the connector and should be GC'd when the corresponding row in `ducklake_cached_duckdb_files` is deleted (or its `last_used_at` ages out). A periodic sweep on each worker reads the table, deletes any local files not referenced. S3 copies are GC'd by the same mechanism, but with a delay (e.g. 24h) to absorb in-flight queries that have already started using the path.

**Future variant — non-home nodes pull and pin a local copy.** If telemetry shows a meaningful fraction of splits running on non-home nodes (e.g. due to skew or affinity overrides), we could add a worker-side daemon that fetches "near-hot" `.db` files from S3 and pins them locally. Out of scope for the first cut.

### 3.6 Hot-file detection

The DuckLake catalog already knows which files are queried — but only because *we* instrument it. Two places to emit access events:

- **`DucklakePageSourceProvider.createPageSource()`** — single chokepoint per-file per-split. Emit `(data_file_id, snapshot_id, node_id, query_id, ts)` to a lightweight append-only table (or buffered in-memory with periodic flush). This is clean and gives us everything.
- **JMX `AlluxioCacheStats`** — aggregate only. Useful for dashboards, not for per-file decisions. Worth exporting but not the primary signal.

A background selector reads the access log and picks hot files + co-accessed groups for conversion. Conversion is scheduled onto the current home node for that file (per `ConsistentHashingHostAddressProvider`). The worker runs the conversion (embedded DuckDB: `COPY (SELECT * FROM read_parquet('…')) TO '…db' (FORMAT DUCKDB)` or `CREATE TABLE s.t AS SELECT …`), writes the `.db` per Section 3.5, and registers the cache row.

Open question: one-parquet-per-db-file vs grouped (N parquets → 1 db with N tables or 1 unioned table). Grouped is more valuable — DuckDB's local scans benefit from colocating co-accessed data — but it changes the split shape: a split must encode "rows X–Y from table T inside this .db" rather than file+byte-range. Recommend 1:1 for the first cut and ship grouping as a follow-up.

---

## 4. What this does NOT require

- No changes to Trino core.
- No patches to Hive/Iceberg/Delta. The user was right — we can only do this where we own the split + page source + catalog, i.e. in trino-ducklake.
- No new local-disk bookkeeping if we go with Option A. Trino's fs cache manages the local disk.
- No coordinator changes. SOFT_AFFINITY is inferred from non-empty `getAddresses()`.
- No API to Trino's page cache internals. We use it as a black box via `TrinoFileSystem`.

---

## 5. Open questions worth resolving before coding

1. **Cost of conversion vs. benefit.** If DuckDB's parquet reader is already fast on the home node with a warm Trino page cache, converting to `.db` may only win for specific access patterns (point lookups, aggregations over sorted/indexed data, repeated scans of the same hot table). Worth a benchmark before generalizing — pick a representative workload and measure parquet-with-warm-cache vs. local-`.db` on the same node.
2. **Local cache directory lifecycle.** We're introducing connector-managed local disk state for the first time. Need to decide: configurable path per worker (`ducklake.local-cache-dir`), max size cap, what happens on disk full (refuse new conversions vs evict LRU), what happens on worker restart (assume persistent vs assume ephemeral). The simplest answer is "persistent, capped, LRU evict, missing-file falls back to parquet" — all four pieces straightforward but each needs explicit code.
3. **Invalidation on DuckLake snapshot change.** The design keys cache rows by `(data_file_id, snapshot_id)`. UPDATE/DELETE on DuckLake produces *new* data file IDs anyway (data files are immutable once written), so old rows become unreferenced and can be TTL-swept rather than actively invalidated. Confirm this matches current DuckLake write semantics — particularly anything around compaction or rewrite that might re-emit the same `data_file_id` with new content.
4. **Grouped vs 1:1 `.db`.** See Section 3.6. Probably defer, but worth deciding the split schema to be forward-compatible.
5. **Where conversion actually runs.** A scheduled job on the coordinator? A worker-side daemon triggered by access-log thresholds? An ad-hoc user-invoked procedure for explicit pinning? All three can coexist; cheapest first-cut is an explicit procedure (`CALL ducklake.system.cache_data_file(...)`) so we can validate the split + page-source plumbing without the hot-detection loop.
6. **Cross-node coordination of conversion.** If two splits on two different nodes both decide to convert the same parquet file at roughly the same time, we want exactly one to win. The catalog table can serve as a lock (insert-or-fail on `(data_file_id, snapshot_id)` with a `pending` status). Not hard, but it needs to be designed in from the start, not bolted on.

---

## 6. Rollout plan sketch

1. **Phase 0 — Soft affinity for parquet splits.** Inject `CachingHostAddressProvider`, populate `DucklakeSplit.getAddresses()`. Standalone win; improves Trino fs cache hit rate for DuckLake parquet reads today. No new catalog table, no new code paths.
2. **Phase 1 — DuckDB as a first-class data-file format (read + write).** See Section 7. Make the connector treat `.db` as a valid `data_file_format` value alongside `parquet`. Wire the page source (DuckDB → Arrow → Trino Page) and the writer (`COPY ... TO 'file.db' (FORMAT DUCKDB)`). No caching machinery, no auto-conversion. Tables are created with one format or the other and stay that way. **This is what builds and proves the read path before any caching exists.**
3. **Phase 2 — Cache table + manual pinning.** Add `ducklake_cached_duckdb_files`. Add a `cache_data_file(table, file)` procedure that runs conversion synchronously and registers the row. Split manager reads the table and routes appropriately between the parquet original and the `.db` cache. No hot-detection yet.
4. **Phase 3 — Access log + auto-selection.** Instrument page source, background selector, async conversion.
5. **Phase 4 — Grouped `.db` files (if measurements justify it).**

Everything through Phase 2 is additive and reversible: drop the table, remove the injection, fall back to parquet. Phase 3+ introduces background state we'd need to design for observability and ops.

---

## 7. Phase 1: DuckDB as a first-class data file format

This phase is the precondition for everything in Sections 3–5. It is also independently useful: tables that benefit from DuckDB's native on-disk format (point lookups, repeated scans of small hot working sets, indexed columns) can opt in directly without any caching layer.

### 7.1 What changes in the connector

- **File format enum:** add `DUCKDB_FILE` (or similar) alongside `PARQUET` in whatever enum the connector uses to discriminate `data_file_format`.
- **Page source routing:** `DucklakePageSourceProvider.createPageSource()` already needs to branch on file format for any future caching work. In Phase 1 it dispatches purely on the format value of the data file row in the catalog: `parquet` → existing `createParquetPageSource()`, `duckdb` → new `DuckDBFilePageSource`. The cache-table logic from Section 3 doesn't exist yet; routing is a one-line check.
- **Writer:** add a `DuckDBFileWriter` that, on INSERT/CTAS into a table whose `data_file_format = 'duckdb'`, runs an embedded DuckDB statement of the shape `COPY (SELECT ... FROM <input pages>) TO '<remote path>' (FORMAT DUCKDB)`. Or `CREATE TABLE ... AS SELECT` into a fresh `.db` file. Either way, the writer materializes a `.db` instead of a parquet file. The write-side schema → DuckDB type mapping is the same one we already use for the JDBC catalog metadata path.
- **CREATE TABLE syntax:** an optional table property `data_file_format = 'duckdb'` (default `'parquet'`). Stays put for the lifetime of the table — we do not mix formats within a table in Phase 1.

### 7.2 Read path mechanics

DuckDB needs a real local file (Section 3.4). In Phase 1 the data file lives on remote storage (e.g. S3) like parquet does — no special placement. The page source therefore has to materialize the file locally before opening it:

1. Open the remote path via `TrinoFileSystem.newInputFile(remotePath)` — going through `CacheFileSystem` if the catalog has fs cache enabled.
2. Stream the bytes to a temp file under a configurable local directory (e.g. `ducklake.tmp-attach-dir`, default `${java.io.tmpdir}/ducklake-attach/`). Use a stable filename derived from `(remote_path, etag/size/mtime)` so concurrent splits hitting the same file converge on one local copy.
3. `ATTACH '<temp file>' AS s (READ_ONLY)` in the embedded DuckDB session.
4. Issue `SELECT <projected cols> FROM s.<table> WHERE <pushed predicates>` and pull results via `arrowExportStream()` → BigQuery-converter-style Arrow → Trino Page.
5. On close, decrement a refcount on the temp file. A periodic sweep deletes temp files that haven't been used recently (e.g. > 1 hour idle), capped by total directory size.

**This is where the user's "let Trino's VFS cache do its job" idea pays off.** Step 1's read goes through `CacheFileSystem`. The first split to materialize the file fetches all 1 MB pages from S3 and they land in the local Alluxio cache. Subsequent splits on the same worker requesting the same file get cache hits during the materialization step — the byte-by-byte streaming to the temp file becomes a local-disk-to-local-disk copy, not an S3 fetch. We never hand the cache pages to DuckDB directly (we can't); we just benefit from the cache speeding up the materialization.

The cost we accept in Phase 1: every worker that reads a `.db` materializes it to local temp first. Phase 2 (the cache layer) is exactly the optimization that skips this — by writing the `.db` to a known local path on the home node at conversion time and avoiding the materialization round-trip on splits that route there.

### 7.3 What this experiment tells us

Three measurements decide whether Phases 2–4 are worth building:

1. **Steady-state read perf, `.db` vs parquet, on the same data, both with warm fs cache.** If parquet wins by enough that DuckDB's native format is a wash, the caching feature is unjustified and we stop here.
2. **DuckDB pushdown wins.** Predicate pushdown into DuckDB SQL should beat parquet row-group skipping for selective queries (especially on indexed columns or sorted data). If the gap is large, the caching feature has clear motivation.
3. **First-read penalty.** How bad is the materialization step on a cold cache? If it's tolerable (small files, fast S3, generous cache disk), Phase 1 might be enough on its own — users who want DuckDB-format tables write them as such and the connector just deals.

If (1) and (2) come out in DuckDB's favor and (3) is the main pain point, then Phase 2 is precisely the right next step (skip materialization on the home node by keeping a real local copy).

### 7.4 Cross-engine compatibility caveat

DuckLake 1.0's `data_file_format` field is part of the spec. `'parquet'` is recognized; `'duckdb'` is not. Implications:

- **Other DuckLake readers (DuckDB itself, Spark, future engines) will not understand `data_file_format = 'duckdb'`.** They will likely error or skip the data file, depending on their strictness. Tables created with `.db` data files become Trino-DuckLake-only.
- **This is opt-in per-table.** Users explicitly set the property at CREATE TABLE; there is no surprise. Tables with the default `parquet` format remain fully cross-engine compatible.
- **Worth proposing upstream.** DuckDB *can* attach `.db` files natively, so adding `'duckdb'` as a recognized `data_file_format` value in a future DuckLake spec revision is plausible. We should write up the case (predicate pushdown wins, native indexes, smaller working sets for hot tables) and float it before this becomes load-bearing for production users.
- **For the cache feature (Phases 2+), this caveat does not apply.** The cache table is a connector-private extension — the canonical `data_file` rows still record `parquet`, so other engines see a normal DuckLake table. Only Trino-DuckLake knows about the cached sidecar `.db`s.

### 7.5 What Phase 1 is not

- **Not a caching feature.** No hot-file detection, no auto-conversion, no soft-affinity-driven local placement. A `.db` file is just where the data lives, by user choice at CREATE TABLE time.
- **Not a mixed-format table.** A table is parquet or `.db` for its lifetime in Phase 1. Mixing formats within a table is what Phases 2+ do (a `.db` cache sitting alongside the canonical parquet).
- **Not a way to avoid the cross-engine concern.** It surfaces it directly. That's a feature: we want users who opt in to know they're choosing connector-private storage.
