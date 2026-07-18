# TODO — DuckDB-format read paths & the "silent" DuckDB-side cache

Research note (2026-07-18, updated after review). How the connector reads DuckDB-format
(`data_file_format = 'duckdb'`, `.db`) files, which paths go through Trino's `TrinoFileSystem`
vs DuckDB's own storage layer, and where DuckDB keeps caches that Trino neither controls nor
observes.

**Why this matters now:** we are heading into transparent conversion + caching of Parquet
files AS DuckDB database files. That makes the DuckDB read path (and its caches) load-bearing,
not a side format — so cache ownership, bounds, staleness, and reuse-of-Trino's-cache all need
to be deliberate, not incidental.

Scope: the DuckDB **read** path only. Parquet reads use Trino's native Parquet reader +
`TrinoFileSystem`.

---

## The dispatch, top to bottom

1. **Format dispatch** — `DucklakePageSourceProvider.createPageSource` routes `.db`/vortex/
   lance splits to `createDuckDbPageSource` (`DucklakePageSourceProvider.kt:328`, `:1451`).
2. **Execution engine** — `ducklake.execution-engine` (`DucklakeExecutionEngine`): `DUCKDB_LOCAL`
   (default, embedded DuckDB in the worker JVM), `QUACK` (out-of-process DuckDB **sidecar** over
   Quack RPC), `SWANLAKE` (reserved). `DucklakeConfig.kt:223`, `DucklakeExecutionEngine.kt`,
   `DucklakeDuckDbExecutorFactory`.
3. **Read target** — `resolveDuckDbAttachTarget` (`DucklakePageSourceProvider.kt:1693`), driven by
   the `duckdb_read_mode` **session property** (`DucklakeSessionProperties.kt:64`, default
   **`httpfs`**): `httpfs` → stream (`HttpfsS3`), `materialize` → local copy (`LocalPath`), `auto`
   → per-file by size vs `ducklake.duckdb.auto-httpfs-threshold` (default **64 MB**). httpfs vs a
   non-s3 URL degrades to materialize (`:1715`). **This target resolution runs on the Trino
   worker for ALL engines**, including Quack.

---

## Per-strategy map

| Strategy | Selected at | Bytes-from-storage via | DuckDB cache in play? | Cleanup / lifecycle |
|---|---|---|---|---|
| **materialize (copy-first-local)** | `resolveDuckDbAttachTarget` → `LocalPath` (`DucklakePageSourceProvider.kt:1717`); `DucklakeMaterializedFileCache.materialize` | **TrinoFileSystem** — `fileSystem.newInputFile(remotePath).newStream()` → local tmp (`DucklakeMaterializedFileCache.kt:145-152`). DuckDB then ATTACHes the LOCAL path. **This read IS behind Trino's file cache when `fs.cache.enabled=true` (see § Trino cache).** | External file cache caches the local `.db` bytes in DuckDB memory too, but the pull is local. | **Our cache**: `${java.io.tmpdir}/ducklake-read/<sha256(path:size)>.db`. **No eviction, TTL, or cap.** Keyed by `(remotePath, fileSize)`. `DucklakeMaterializedFileCache.kt:31-52`. |
| **httpfs (stream remote)** — DEFAULT | `resolveDuckDbAttachTarget` → `HttpfsS3` (`:1712`) | **DuckDB's own httpfs/S3** — `INSTALL httpfs; LOAD httpfs; CREATE SECRET ducklake_s3; ATTACH 's3://…'` (`InProcessDuckDbExecutor.kt:194-207`). **Trino FS is bypassed** — Trino's file cache never sees these bytes. | **YES** — DuckDB `enable_external_file_cache` (default ON; see § DuckDB cache) + `enable_http_metadata_cache` (default OFF) + http keep-alive/retries. | In-process: cache lives in the per-split embedded instance → dies at split close (`InProcessDuckDbExecutor.kt:141-156`). So the in-process httpfs cache is effectively **per-split, cold every split** today. Quack: long-lived server connection → cache persists across queries (see below). |
| **direct remote** | = the httpfs path (no separate one exists) | DuckDB httpfs | YES | as httpfs |
| **Quack (out-of-process sidecar)** | `QuackDuckDbExecutor`; ATTACH happens **server-side** via `quack_query_by_name` (`QuackDuckDbExecutor.kt:80-137`, `:159-201`) | **Depends on the target the Trino worker resolved:** `LocalPath` → the sidecar reads a **shared/bind-mounted local path** that the Trino worker's materialize cache wrote; `HttpfsS3` → DuckDB httpfs **on the sidecar**. | YES, and **instance-wide + long-lived** on the sidecar (one shared DuckDB across sessions; tuning SETs travel via the wrapper) | Server-side ATTACH shared across sessions (`IF NOT EXISTS`, alias = `ducklake_cache_<sha16(path)>`, `QuackDuckDbExecutor.kt:302-318`). External file cache persists for the sidecar's lifetime. Invisible to Trino. |
| **FileScan (vortex / lance)** | `resolveDuckDbFileScanTarget` (`DucklakePageSourceProvider.kt:1642`) | vortex/lance bind through **Rust object_store**, ignoring DuckDB secrets, reading via `AWS_*` env (`DuckDbAttachTarget.kt:44-49`, `DuckDbS3Config.toObjectStoreEnv`). | DuckDB external file cache N/A to the Rust layer. | Reuses the materialize-vs-stream decision; materialized single files use our no-eviction cache. |

---

## Q1 — Quack: shared dir with the Trino worker + reads Trino's file OR its own cache. CONFIRMED.

Yes, both, and here's the exact mechanism (corrects the earlier vaguer note):

- The Quack backend is a **sidecar** co-located with the Trino worker (README §
  `ducklake.execution-engine`; `DucklakeConfig.kt:224` — *"for 'quack' … a shared local path
  (multi-container pod) or s3:// access"*).
- **`resolveDuckDbAttachTarget` runs on the Trino worker regardless of engine**
  (`DucklakePageSourceProvider.kt:1491` → `:1693`). So under `materialize`/`auto`-small the
  **Trino worker downloads the `.db` into its own materialize cache**
  (`${tmpdir}/ducklake-read/…`, via `TrinoFileSystem`), and then hands that **local path** to the
  sidecar's server-side `ATTACH '<path>' (READ_ONLY)` (`QuackDuckDbExecutor.kt:160-161`). This
  only works if the worker's cache dir is a **shared bind mount** visible to the sidecar (the
  intended pod topology). → **The sidecar reads the file Trino produced.**
- Under `httpfs`, the worker hands the sidecar the `s3://` URL and the sidecar's DuckDB fetches
  + caches it itself (`QuackDuckDbExecutor.kt:162-163`, `:181-189`). → **The sidecar uses its
  own internal cache.**
- Either way the sidecar's DuckDB `enable_external_file_cache` (in-memory, default ON) can also
  cache on top — long-lived because the server connection is long-lived.
- **Open:** the shared-mount requirement for Quack+LocalPath is implicit. If tmpdir isn't the
  shared mount, the sidecar ATTACH fails (file not found server-side). We should (a) make the
  materialize cache dir explicitly configurable (see § materialize-cache), and (b) document the
  "cache dir must be the shared mount" contract for the Quack topology. Also: two materialize
  copies could exist (worker tmpdir vs whatever the sidecar image expects) if misconfigured.

---

## Q2 — Can we reuse the SAME cache Trino uses for Parquet? PARTIALLY YES (and we already do, on one path).

Trino **does** expose a filesystem cache primitive, and our connector already sits behind it:

- `io.trino.filesystem.cache` package (in `trino-filesystem`): `TrinoFileSystemCache` (the
  cache interface: `cacheInput`/`cacheStream`/`cacheLength`/`expire`), `CacheFileSystem` +
  `CacheFileSystemFactory` (wrap ANY `TrinoFileSystemFactory`), `CacheKeyProvider`
  (`DefaultCacheKeyProvider`), `CacheInputFile`, and `CacheSplitAffinityProvider`. Backends:
  **Alluxio** (`trino-filesystem-cache-alluxio`, on-disk) and **Memory**
  (`MemoryFileSystemCache`).
- **`FileSystemModule.createFileSystemFactory` wraps the connector's `TrinoFileSystemFactory`
  in `CacheFileSystemFactory` when `FileSystemConfig.isCacheEnabled()` is true** (i.e. the
  `fs.cache.enabled` catalog property). Verified by decompiling `FileSystemModule` (483).
- Our factory chain: `DucklakeFileSystemFactory` → `DefaultDucklakeFileSystemFactory` delegates
  straight to the **injected `TrinoFileSystemFactory`** (`DefaultDucklakeFileSystemFactory.kt:22-27`),
  which IS the one `FileSystemModule` provides. So when `fs.cache.enabled=true`,
  `fileSystemFactory.create(session).newInputFile(...)` reads THROUGH Trino's file cache —
  the exact same cache the Parquet path uses.

**Implication (important):**
- The **materialize** path already reads via `newInputFile(...)`, so a materialize read is
  **already covered by Trino's file cache** (Alluxio/Memory) when enabled — same as Parquet.
  The subsequent DuckDB ATTACH then reads a LOCAL file (our materialize cache), so there are
  two layers: Trino FS cache on the remote pull, our on-disk copy for ATTACH.
- The **httpfs** path (the DEFAULT) does NOT use `TrinoFileSystem` at all → **Trino's cache
  cannot see it.** DuckDB owns those bytes.
- There is **no lower-level primitive that lets DuckDB's own httpfs reader pull through
  `CacheInputFile`** — DuckDB fetches over its own C++ httpfs. To put DuckDB reads behind
  Trino's cache you must route the bytes through `TrinoFileSystem` first (i.e. the materialize
  path), which is exactly what materialize does. `TrinoFileSystemCache.cacheInput/cacheStream`
  operate on `TrinoInputFile`, so any DuckDB read we want cached by Trino must be materialized
  (or block-served) through a `TrinoInputFile` — DuckDB can't call back into it.

**Design levers this opens (for the transparent-conversion work):**
- Prefer/deploy `materialize` mode for `.db`/converted files so all reads ride Trino's cache
  (Alluxio on-disk cache reused across queries AND workers, with split affinity) — this is the
  cleanest way to get "the same cache as Parquet."
- Feed the materialize download through the cache-wrapped `fileSystemFactory` (it already is).
  Consider replacing our bespoke `DucklakeMaterializedFileCache` with a thin use of
  `CacheFileSystem` + a local staging dir, so we inherit Trino's eviction/affinity/metrics
  instead of maintaining our own unbounded cache. Needs a spike: `CacheInputFile` serves
  block reads, but DuckDB ATTACH needs a real path — so we'd still stage a local file; the win
  is that the *remote pull* + its cache policy become Trino's, not ours.
- `CacheSplitAffinityProvider` / `CacheKeyProvider` are the hooks to make the same `.db` land on
  the same worker across queries (warm-cache locality) — relevant once conversion produces
  stable, reused `.db` artifacts.

---

## Q3 — DuckDB built-in cache: constraint knobs we could expose. RESEARCHED (against our bundled DuckDB 1.5.4).

**Correction to the earlier note:** `enable_object_cache` is now a **legacy PLACEHOLDER that
DOES NOTHING** (confirmed live on duckdb_jdbc 1.5.4.0: description = *"[PLACEHOLDER] Legacy
setting - does nothing"*, default `false`). **Our `SET enable_object_cache = …`
(`DuckDbTuningSql.kt:32`) is a NO-OP** — accepted without error, zero effect. The
`ducklake.duckdb.enable-object-cache` config property (`DucklakeConfig.kt:311`) therefore
controls nothing today. Two fixes: (a) drop it / mark deprecated, (b) if we want the behavior,
target the real settings below.

The real DuckDB caches + their knobs (all `SET`-able; all confirmed present on 1.5.4):

| DuckDB setting | Default | What it controls | Expose as? |
|---|---|---|---|
| `enable_external_file_cache` | **`true`** | The actual cache — external file bytes (Parquet AND `.db`) held **in memory**. This is the "silent cache" for httpfs and for the Quack sidecar. We never set it → it's ON by default. | `ducklake.duckdb.enable-external-file-cache` (replace the dead object-cache one). Lets operators turn it OFF for strict-freshness or memory-tight deployments. |
| `validate_external_file_cache` | `VALIDATE_ALL` | Staleness mode: `VALIDATE_ALL` / `VALIDATE_REMOTE` / `NO_VALIDATION`. Governs whether cached entries are re-checked before reuse. | `ducklake.duckdb.validate-external-file-cache`. For immutable DuckLake `.db` (path+size unique) `VALIDATE_REMOTE`/`NO_VALIDATION` could speed warm reads; risky if a path is ever reused. |
| `memory_limit` (`max_memory`) | 80% RAM | Buffer manager cap; the external file cache lives inside it. **This is the real bound on the DuckDB cache size** — there is no separate "external file cache size" knob; you cap it via `memory_limit`. | Already exposed: `ducklake.duckdb.memory-limit` (`DucklakeConfig.kt:267`). Document that it also bounds the file cache. |
| `enable_http_metadata_cache` | `false` | Global cache of HTTP metadata (HEAD results) across queries. | `ducklake.duckdb.enable-http-metadata-cache`. Cheap win for many-small-file httpfs; safe for immutable files. |
| `http_keep_alive` / `http_retries` / `http_timeout` | true / 3 / 30s | httpfs connection reuse + resilience. We already SET keep-alive + retries (`DuckDbTuningSql.kt:30-31`); `http_timeout` is not exposed. | Consider `ducklake.duckdb.http-timeout`. |
| `temp_directory` / `max_temp_directory_size` | `<db>.tmp` / 90% disk | Spill location + cap (NOT the read cache; spill for large ops). | Already exposed (`DucklakeConfig.kt:289,300`). |
| inspection: `duckdb_external_file_cache()` / `duckdb_settings()` | — | Table function listing cached files (since 1.3.0) + settings view. | Useful for an ops/debug PTF or a `$duckdb_cache` system table later. |

**How to show/apply:** the same channel we already use — add to `DuckDbTuning` +
`DuckDbTuningSql.statements()` so each SET flows to the in-process instance (`applyDirect`) and
to the Quack sidecar (via `quack_query_by_name`). One caveat on Quack: these are **instance-wide
on the shared sidecar**, so a per-catalog SET is really per-server — fine if one catalog per
sidecar, needs thought for shared sidecars.

---

## Q4 — Credential drift: RETRACTED / re-scoped.

Correct: Trino has **no runtime S3 credential rotation** — `s3.*` FS config and our
`DuckDbS3Config` are both fixed at catalog load and only change on **restart** OR on a
**dynamic-catalog `DROP` + `CREATE CATALOG`** (which rebuilds the connector, re-deriving both).
So they can't silently diverge at runtime. The only real residue of the two-channel design is
**config-time correctness**: `DuckDbS3Config` must be derived consistently from the same
resolved FS config so a catalog is never created with FS creds that work for Parquet but a
mismatched DuckDB secret that fails httpfs. Keep as a config-construction consistency check, not
a drift concern. (`DuckDbS3Config.kt`, built in `DucklakePageSourceProviderFactory`.)

---

## Risks / open questions (updated)

- [ ] **Dead `enable_object_cache` no-op.** `DuckDbTuningSql.kt:32` + `ducklake.duckdb.enable-object-cache`
  do nothing on modern DuckDB. Replace with `enable_external_file_cache` (real) or remove. **Low
  effort, high clarity.**
- [ ] **The real silent cache is `enable_external_file_cache` (default ON), which we never
  configure.** On the Quack sidecar (long-lived) it persists across queries and is bounded only
  by `memory_limit`. Decide defaults + expose the knob (Q3 table). Add a Quack staleness test:
  file at a reused path replaced → does the sidecar's external file cache serve stale bytes under
  `VALIDATE_ALL`? (Immutable-path design says no, but prove it before the conversion work leans
  on it.)
- [ ] **Default path bypasses Trino's cache entirely.** `duckdb_read_mode=httpfs` (default) →
  no `TrinoFileSystem`, no Alluxio/Memory cache, no split affinity. For the transparent
  Parquet→`.db` conversion work, decide whether **`materialize` should be the default** so
  converted files ride Trino's cache exactly like Parquet (Q2). Big lever.
- [ ] **Unbounded materialize cache.** `DucklakeMaterializedFileCache` has no eviction/TTL/cap and
  a **hardcoded** dir (`${tmpdir}/ducklake-read/`, `DucklakeMaterializedFileCache.kt:127-129`).
  Make the dir configurable (needed for the Quack shared-mount contract), and either add
  eviction/cap OR replace with Trino's `CacheFileSystem` (Q2).
- [ ] **In-process httpfs cache is per-split (cold every split).** Because each split opens a
  fresh `jdbc:duckdb:` instance (`InProcessDuckDbExecutor.kt:61`), the external file cache never
  warms across splits/queries for `DUCKDB_LOCAL`. That's safe but slow for repeated remote reads
  — another argument for materialize (Trino cache) or a pooled/long-lived in-process DuckDB.
- [ ] **`getCompletedBytes()` always 0** on the DuckDB path (`DuckDbFilePageSource.kt:87-92`).
  Byte accounting blind while DuckDB owns the fetch. Materialize-through-Trino-FS would restore
  it (the pull becomes a `TrinoInput`).
- [ ] **Observability.** No metric for warm-vs-cold on either cache. `duckdb_external_file_cache()`
  + Trino's Alluxio cache stats could feed a debug surface.
- [ ] **Quack shared-mount contract implicit.** Document + validate that the materialize cache dir
  is the sidecar-visible mount when engine=quack and mode≠httpfs.

---

## Key files (for the next agent)

- `DucklakePageSourceProvider.kt` — `createDuckDbPageSource` (:1451), `resolveDuckDbAttachTarget`
  (:1693), `resolveDuckDbReadTarget`/`resolveDuckDbFileScanTarget` (:1597, :1642).
- `DucklakeMaterializedFileCache.kt` — connector-managed local copy cache (no eviction; hardcoded dir).
- `InProcessDuckDbExecutor.kt` — in-process ATTACH: local vs httpfs `CREATE SECRET`/ATTACH (:184-220);
  fresh per-split instance (:61).
- `QuackDuckDbExecutor.kt` — server-side ATTACH via `quack_query_by_name` (:80-137, :159-201);
  shared alias (:302-318).
- `DuckDbTuning.kt` / `DuckDbTuningSql.kt` — the SET channel; **`enable_object_cache` no-op** (:32),
  temp_directory, http keep-alive/retries.
- `DuckDbS3Config.kt` — DuckDB `CREATE SECRET` (the separate, load-time-fixed cred channel).
- `DucklakeConfig.kt` — engine, `auto-httpfs-threshold` (64MB), `enable-object-cache` (dead),
  memory-limit, temp-directory*.
- `DucklakeSessionProperties.kt` — `duckdb_read_mode` (default `httpfs`), :64-66, :220.
- `DefaultDucklakeFileSystemFactory.kt` — delegates to the (cache-wrapped) injected FS factory.
- Trino cache primitive: `io.trino.filesystem.cache.*` (`CacheFileSystem`, `TrinoFileSystemCache`,
  `CacheKeyProvider`, `CacheSplitAffinityProvider`), enabled by `fs.cache.enabled`
  (`FileSystemModule` / `FileSystemConfig`).
