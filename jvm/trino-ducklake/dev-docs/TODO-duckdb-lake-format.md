# TODO: DuckDB as a first-class data file format (Phase 1)

Living tracker for the Phase 1 work described in `CONCEPT-duckdb-as-parquet-file-cache.md` §7.
Update as you go. If chat context is lost, this file is the recovery point.

**Phase 1 scope:** brutal and basic. Make `'duckdb'` a valid `data_file_format` value alongside `'parquet'`, write it on insert, read it on query. No caching, no auto-conversion, no soft affinity, no mixed formats per table. We control writer and reader, so the experiment is self-contained.

---

## Verified facts (2026-05-03)

- `fileFormat` is a plain `String` column on `ducklake_data_file` — no enum to extend.
- Page source format branch already exists at `DucklakePageSourceProvider.java:176` (currently throws on non-parquet).
- Page sink (`DucklakePageSink.java`) is hardcoded to parquet; needs format-aware writer factory.
- Format hardcode at `JdbcDucklakeCatalog.java:1578` — `dataFile.setFileFormat("parquet")`. Needs to read from `DucklakeWriteFragment`.
- DuckDB writer SQL confirmed in CLI: `ATTACH 'new.db' AS new (READ_WRITE); CREATE TABLE new.t AS SELECT...; DETACH new`. The doc's old `COPY (...) TO 'file.db' (FORMAT DUCKDB)` is **not** the path.
- DuckDB reader SQL confirmed in CLI: `ATTACH '/tmp/foo.db' AS s (READ_ONLY); SELECT * FROM s.<table>`.
- `DuckDBResultSet.arrowExportStream(allocator, batchSize)` exists in 1.5.2.0; returns `ArrowReader` (typed as `Object`).
- `org.duckdb:duckdb_jdbc:1.5.2.0` is in `libs.versions.toml`, currently `testImplementation` only — must move to `implementation`.
- Apache Arrow 18.3.0 is in `libs.versions.toml`, but trino-ducklake module does not yet depend on it.
- No existing `org.duckdb.*` Java imports anywhere — clean greenfield.
- Test scaffold ready: `AbstractDucklakeIntegrationTest`, `DucklakeQueryRunner`, isolated PG catalog per suite.

---

## Phase 1 step plan (one commit per step)

### Step 1: Skeleton plumbing — NO DuckDB code yet ✅ DONE 2026-05-03
Goal: thread the format string from `SET SESSION` through to the catalog row, with both branches present but the duckdb branch throws "not yet implemented." Tests prove the threading works.

**Decision (2026-05-03):** use a *session* property, not a table property, for Phase 1. Reasons: no per-table catalog persistence question; no INSERT-into-existing-table format-inference question; simplest test fixture. Promote to a table property when Phase 1 is proven.

- [x] Create this TODO file
- [x] Add `data_file_format` session property to `DucklakeSessionProperties` (default `"parquet"`, allowed: `"parquet" | "duckdb"`).
- [x] Add `String fileFormat` field to `DucklakeWriteFragment` (record), thread through serialization. Added 5-arg and 7-arg convenience overloads (default `"parquet"`) so existing call sites and tests didn't churn.
- [x] Add `String fileFormat` field to `DucklakeWritableTableHandle`.
- [x] In `DucklakeMetadata.beginCreateTable`, `beginInsert`, and `beginMerge`, read session property → set on handle.
- [x] `DucklakePageSinkProvider` already passes the handle through; sink now reads `handle.fileFormat()` directly, no constructor change needed.
- [x] In `DucklakePageSink.openNewWriter()`, throw `NOT_SUPPORTED` for `"duckdb"`; reject anything other than `"parquet"`/`"duckdb"`. Fragment construction passes `this.fileFormat`.
- [x] In `JdbcDucklakeCatalog:1578`, replaced `setFileFormat("parquet")` with `setFileFormat(fragment.fileFormat())`.
- [x] In `DucklakePageSourceProvider:176`, switch on format: parquet → existing path; duckdb → throw `NOT_SUPPORTED`; anything else → throw.
- [x] Smoke test `TestDucklakeDuckDbFormatSkeleton` (5 cases): empty CREATE works, CTAS/INSERT throw, invalid format rejected, default parquet path unchanged. All green.
- [x] Full `:trino-ducklake:test` and `:ducklake-catalog:test` suites green — no regressions on parquet path.
- [x] **Side fix:** `DucklakeConfig` had `@Deprecated` on the setters but not the matching getters for `temporalPartitionEncoding` and `temporalPartitionEncodingReadLeniency`. Airlift's config validation rejects this and was failing all integration tests on main with `ApplicationConfigurationException` (the unused-property errors were a downstream symptom of fail-fast in the config phase). Added `@Deprecated` to both getters. Pre-existing main bug; would have blocked any test run regardless.

**Files changed in Step 1:**
- `jvm/ducklake-catalog/src/.../DucklakeWriteFragment.java`
- `jvm/ducklake-catalog/src/.../JdbcDucklakeCatalog.java`
- `jvm/trino-ducklake/src/.../DucklakeConfig.java` (side fix)
- `jvm/trino-ducklake/src/.../DucklakeMetadata.java`
- `jvm/trino-ducklake/src/.../DucklakePageSink.java`
- `jvm/trino-ducklake/src/.../DucklakePageSourceProvider.java`
- `jvm/trino-ducklake/src/.../DucklakeSessionProperties.java`
- `jvm/trino-ducklake/src/.../DucklakeWritableTableHandle.java`
- `jvm/trino-ducklake/test/src/.../TestDucklakeDuckDbFormatSkeleton.java` (new)

**Exit criteria for step 1:** Plumbing passes, no DuckDB code yet, CI green. ✅

### Step 2: Writer (Appender API) ✅ DONE 2026-05-04
Goal: CTAS into a duckdb-format table actually produces a `.db` file with the right contents.

- [x] Move `duckdb_jdbc` from `testImplementation` to `implementation` in `build.gradle.kts`.
- [~] **Skipped:** `DuckDbInstanceRegistry`. The simpler shape ended up being one DuckDB JDBC connection per writer (created in the constructor, closed in `finishAndBuildFragment` / `abort`). Per-JVM connection pool can be added later if profiling shows the connection-spinup cost matters; for Phase 1 it's negligible compared to the actual row writes.
- [x] Added `DuckDbFileWriter`:
    - Open: `${java.io.tmpdir}/ducklake-write/<uuid>.db`, `ATTACH ... AS ducklake_out (READ_WRITE)`, `CREATE TABLE ducklake_out.main.t (...)` from Trino column types via inline mapping.
    - `write(Page)`: per-row `appender.beginRow() ... append(...) ... endRow()`. Block dispatch covers BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, DECIMAL (short and long), DATE, TIMESTAMP (short + long via LongTimestamp), TIMESTAMPTZ, VARCHAR, VARBINARY, UUID. Nested types throw `NOT_SUPPORTED`.
    - `finishAndBuildFragment`: close appender, `DETACH`, close connection, upload local file via `TrinoFileSystem.newOutputFile().create()`, delete local temp, build fragment with `footerSize = 0` and empty `columnStats` (DuckDB stats can come later).
- [x] Extracted `DucklakeFileWriter` interface with `write/getApproximateWrittenBytes/finishAndBuildFragment/abort`. Moved the parquet inner class out to `ParquetFileWriter` implementing the same interface. `DucklakePageSink.openNewWriter` now branches `parquet` → `ParquetFileWriter`, `duckdb` → `DuckDbFileWriter`. `closeWriter` is format-agnostic — each writer builds its own fragment.
- [x] `TestDucklakeDuckDbFormatWrite` (5 cases):
    - CTAS produces a valid `.db` file readable by direct DuckDB JDBC, with correct row count and values.
    - NULLs in VARCHAR columns round-trip correctly.
    - INSERT with the duckdb session into a parquet-created table also produces a `.db`.
    - SELECT through Trino still throws `NOT_SUPPORTED` for the read side (Step 3 not yet done).
    - Default session still produces parquet (regression check).
- [x] `TestDucklakeDuckDbFormatSkeleton` retained for the cross-cutting cases (empty CREATE, invalid format string).
- [x] Full `:trino-ducklake:test` and `:ducklake-catalog:test` suites green.

**Exit criteria for step 2:** Files written via Trino-DuckLake are readable by DuckDB CLI directly. Tests green. Reader still throws. ✅

**Files changed in Step 2:**
- `jvm/trino-ducklake/build.gradle.kts` — `duckdb_jdbc` promoted to `implementation`
- `jvm/trino-ducklake/src/.../DucklakeFileWriter.java` (new interface)
- `jvm/trino-ducklake/src/.../ParquetFileWriter.java` (new — extracted from inner class)
- `jvm/trino-ducklake/src/.../DuckDbFileWriter.java` (new)
- `jvm/trino-ducklake/src/.../DucklakePageSink.java` (refactored to use the interface)
- `jvm/trino-ducklake/test/src/.../TestDucklakeDuckDbFormatWrite.java` (new)
- `jvm/trino-ducklake/test/src/.../TestDucklakeDuckDbFormatSkeleton.java` (trimmed — obsolete throwing tests removed)

### Step 3: Reader ✅ DONE 2026-05-04
Goal: SELECT from a duckdb-format table works end-to-end.

**Decision (2026-05-04):** Per-split DuckDB instance instead of the per-JVM-shared model in the original plan. Reasons: simpler lifecycle (open in `initialize()`, close in `close()` — no registry, no refcount), and READ_ONLY ATTACHes don't conflict so independent instances reading the same physical file is correct, just duplicated. Migration path to a per-JVM `DuckDbInstanceRegistry` is mechanical if profiling later shows native-instance spinup matters.

- [x] Added Arrow deps (`arrow-vector`, `arrow-memory-core`, `arrow-c-data`, runtime `arrow-memory-netty`) to `trino-ducklake/build.gradle.kts`. Also added `--add-opens=java.base/java.nio=ALL-UNNAMED` and `--add-opens=java.base/java.lang=ALL-UNNAMED` to test JVM args; Arrow's `MemoryUtil.directBuffer()` reaches into the private `DirectByteBuffer(long, int)` constructor for off-heap interop with DuckDB's Arrow C-data export.
- [x] Added `DucklakeMaterializedFileCache` — per-JVM, keyed by SHA-256 of `(remotePath, fileSize)`, atomic write via `<name>.partial` → rename, per-key locks for concurrent calls. No eviction in Phase 1 — files live under `${java.io.tmpdir}/ducklake-read/` and rely on OS tmpdir cleanup.
- [x] Added `DucklakeArrowToPageConverter` covering BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, DECIMAL (short and long), DATE, TIMESTAMP (S/MS/US/NS), TIMESTAMPTZ (S/MS/US/NS), VARCHAR, VARBINARY. Nested types and unrecognized vector classes throw `NOT_SUPPORTED`.
- [x] Added `DuckDbFilePageSource implements ConnectorPageSource`. Per-split `DriverManager.getConnection("jdbc:duckdb:")`, `ATTACH ... READ_ONLY`, `executeQuery(SELECT ...)`, `arrowExportStream(allocator, 1024)` → `ArrowReader.loadNextBatch()` loop → converter → `SourcePage.create(page)`. `close()` releases reader, ResultSet, Statement, DETACH, connection, allocator.
- [x] In `DucklakePageSourceProvider`, replaced the duckdb-throw branch with `createDuckDbPageSource(...)`. Filters out `$row_id` from the SELECT projection, materializes the file via the cache, wraps with `RowIdInjectingPageSource` if `$row_id` was requested, then with `applyDeleteFile` for delete-file handling. Empty projection (e.g. `COUNT(*)` with only `$row_id`) currently throws `NOT_SUPPORTED` — a row-counting fast path can come later.
- [x] `DucklakeMaterializedFileCache` bound as a singleton in `DucklakeModule`; injected through `DucklakePageSourceProviderFactory` → `DucklakePageSourceProvider`. Two pre-existing tests (`TestDucklakeDeleteFileHandling`, `TestDucklakePageSourceProvider`) updated to pass a `new DucklakeMaterializedFileCache()` directly.
- [x] **Side fix:** the test PostgreSQL container was at default `max_connections=100`. With the new read tests adding parallel postgres-extension connections (each DuckDB instance opening its own), the suite hit "FATAL: sorry, too many clients already" intermittently. Bumped to 500 via `withCommand("postgres", "-c", "max_connections=500")` on the testcontainer.
- [x] `TestDucklakeDuckDbFormatRead` (7 cases): scalar round-trip, NULLs, temporal+decimal, projection, Trino-side WHERE filter, GROUP BY aggregation, mixed parquet+duckdb table join.
- [x] `TestDucklakeDuckDbFormatWrite.testReadbackThroughTrino` updated — used to assert `NOT_SUPPORTED`, now asserts the round-trip succeeds.
- [x] Full `:trino-ducklake:test` (480 tests) and `:ducklake-catalog:test` suites green.

**Files changed in Step 3:**
- `jvm/trino-ducklake/build.gradle.kts` — Arrow deps, JVM `--add-opens` args
- `jvm/trino-ducklake/src/.../DucklakeMaterializedFileCache.java` (new)
- `jvm/trino-ducklake/src/.../DucklakeArrowToPageConverter.java` (new)
- `jvm/trino-ducklake/src/.../DuckDbFilePageSource.java` (new)
- `jvm/trino-ducklake/src/.../DucklakeModule.java` — bind cache singleton
- `jvm/trino-ducklake/src/.../DucklakePageSourceProvider.java` — wire `createDuckDbPageSource`
- `jvm/trino-ducklake/src/.../DucklakePageSourceProviderFactory.java` — pass cache through
- `jvm/trino-ducklake/test/src/.../TestDucklakeDuckDbFormatRead.java` (new)
- `jvm/trino-ducklake/test/src/.../TestDucklakeDuckDbFormatWrite.java` — read-back test flipped
- `jvm/trino-ducklake/test/src/.../TestDucklakeDeleteFileHandling.java` — pass cache to direct ctor
- `jvm/trino-ducklake/test/src/.../TestDucklakePageSourceProvider.java` — pass cache to direct ctor
- `jvm/ducklake-catalog/testFixtures/src/.../TestingDucklakePostgreSqlCatalogServer.java` — `max_connections=500`

**Exit criteria for step 3:** Round-trip CTAS + SELECT works for a simple TPC-H-shaped table. ✅

### Step 4: Pushdown
Goal: project list and predicates pushed into the SQL we issue to DuckDB.

- [ ] Translate Trino `TupleDomain<DucklakeColumnHandle>` → DuckDB-compatible `WHERE` clause for the dispatched query.
- [ ] Restrict `SELECT` to projected column names only.
- [ ] Test: a query with a selective predicate against a 1M-row table touches fewer DuckDB blocks than the no-pushdown version (measure via DuckDB's `EXPLAIN ANALYZE` against the local file).

**Exit criteria for step 4:** Selective queries on duckdb-format tables show measurable improvement over parquet on the same data.

---

## Open questions to revisit during implementation

A. **Appender vs INSERT INTO ... SELECT FROM arrow_stream.**
   Phase 1 uses the DuckDB Appender API (one row at a time). Future optimization: build an Arrow representation of the input `Page` on the Trino side and have DuckDB consume it via `INSERT INTO t SELECT * FROM <arrow-mounted-table>` (DuckDB JDBC supports mounting Arrow streams via `arrow_array_stream` extension; the BigQuery plugin has shown the pattern in reverse for read). User has working example. Defer until Appender perf is measured.

B. **DuckDB instance scope.** Step 3 ships with **per-split independent DuckDB instances** (one `jdbc:duckdb:` per page source, closed in `close()`). Trade-off: highest spinup cost per query, but trivial lifecycle and full isolation; READ_ONLY ATTACHes don't conflict so duplicate ATTACHes of the same physical file are correct, just wasted work. Migration path if profiling later shows the native init is hot: introduce a per-JVM `DuckDbInstanceRegistry` that hands out duplicated connections via `DuckDBConnection.duplicate()`; the page source releases its connection but not the instance. No interface change required. Mirrors the writer's deferred-registry decision.

C. **Materialization vs httpfs.** Phase 1: download `.db` via `TrinoFileSystem`, then DuckDB ATTACHes the local file. Phase 2+ may use DuckDB's httpfs for direct S3 attach on the cold-read path (per design conversation 2026-05-03). Not relevant in Phase 1 because we're filesystem-only for testing.

D. **Partitioned tables.** Phase 1 supports unpartitioned only. Partitioned writes need per-partition `.db` files; mostly mechanical extension of the Appender writer but worth a separate step.

E. **Stats extraction.** Parquet writer fills `DucklakeFileColumnStats` from parquet footer. DuckDB has stats internally too — can we extract them via PRAGMA / system tables? Phase 1 fine to skip (split-time predicate filtering still works on data, just less efficiently). Add later.

F. **Footer size.** Parquet-specific concept. For duckdb files, set to 0 in the fragment / catalog row. Confirm DuckLake spec doesn't reject 0 here.

G. **`data_file_format` cross-engine compatibility.** Setting `'duckdb'` in the spec field will make tables Trino-DuckLake-only. For Phase 1 this is intentional (we own both ends). Gate behind a feature flag for any release.

---

## Status log

- **2026-05-03** — Plan agreed with user. Doc `CONCEPT-duckdb-as-parquet-file-cache.md` updated with revised approach (per-conversation-mode option 1 vs option 2 for read, partition-level future idea, brutal-Phase-1 framing). This TODO file created. Starting Step 1.
- **2026-05-03** — Step 1 (skeleton) done. Session property `ducklake.data_file_format` plumbed end-to-end: `parquet` (default) keeps the existing writer/reader paths untouched, `duckdb` throws `NOT_SUPPORTED` at both ends. `ducklake_data_file.file_format` row now sourced from the fragment instead of hardcoded. 5/5 skeleton tests green; full trino-ducklake + ducklake-catalog suites green. Side fix: `@Deprecated` getter/setter pair mismatch in `DucklakeConfig` was blocking all integration tests on main. Ready for Step 2 (writer via DuckDB Appender API).
- **2026-05-04** — Step 2 (writer) done. CTAS / INSERT with the duckdb session property now produces a real `.db` file via the DuckDB Appender API and uploads it through `TrinoFileSystem`. `DucklakeFileWriter` interface now sits between the page sink and the format-specific writers (`ParquetFileWriter` extracted; `DuckDbFileWriter` new). Phase 1 type coverage: BOOLEAN/TINYINT/SMALLINT/INT/BIGINT/REAL/DOUBLE/DECIMAL/DATE/TIMESTAMP/TIMESTAMPTZ/VARCHAR/VARBINARY/UUID. Nested types throw. New write tests round-trip via direct DuckDB JDBC; the read path still throws `NOT_SUPPORTED` (Step 3). Both module suites green. Skipped the per-JVM `DuckDbInstanceRegistry` — current shape is one connection per writer, simpler and fast enough; can be revisited if profiling justifies it.
- **2026-05-04** — Step 3 (reader) done. SELECT through Trino against duckdb-format tables works end-to-end. Pipeline: `DucklakeMaterializedFileCache` (per-JVM, SHA-256-keyed atomic writes) downloads the `.db` to local tmp; `DuckDbFilePageSource` opens a per-split DuckDB JDBC instance, ATTACHes READ_ONLY, runs `SELECT <projected> FROM ...`, calls `arrowExportStream()` for the Arrow C-data export; `DucklakeArrowToPageConverter` materializes each batch into a Trino `Page`. Delete files and `$row_id` injection reuse the same wrappers as the parquet path. Empty projections (COUNT-only) currently throw `NOT_SUPPORTED`. New `TestDucklakeDuckDbFormatRead` (7 cases) covers scalar round-trip, NULLs, temporal+decimal, projection, predicate, aggregation, and a mixed parquet+duckdb join. `TestDucklakeDuckDbFormatWrite.testReadbackThroughTrino` flipped from "expect throw" to "expect success". Side fixes: `--add-opens=java.base/java.nio=ALL-UNNAMED` and `--add-opens=java.base/java.lang=ALL-UNNAMED` on the test JVM (Arrow's `MemoryUtil` requires private `DirectByteBuffer` access); test postgres `max_connections=500` (default 100 was hit by parallel test classes each opening DuckDB-postgres-extension connections). Both suites green (480 trino-ducklake tests).
