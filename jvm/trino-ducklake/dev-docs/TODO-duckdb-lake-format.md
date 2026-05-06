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
- [x] In `DucklakePageSourceProvider`, replaced the duckdb-throw branch with `createDuckDbPageSource(...)`. Filters out `$row_id` from the SELECT projection, materializes the file via the cache, wraps with `RowIdInjectingPageSource` if `$row_id` was requested, then with `applyDeleteFile` for delete-file handling.
- [x] Empty projection (`COUNT(*)` and similar) handled via a `SELECT 1 FROM t [WHERE ...]` placeholder column whose value is ignored — the page source emits empty-block `Page(rowCount)` per Arrow batch, so downstream operators (count, delete-file filter) see the right position counts.
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

### Step 4: Pushdown ✅ DONE 2026-05-04
Goal: project list and predicates pushed into the SQL we issue to DuckDB.

- [x] **Projection pushdown** was already done in Step 3 — `DuckDbFilePageSource.buildSelectSql()` only emits the projected columns. No work needed here.
- [x] **Predicate pushdown** via new `DuckDbWhereClauseTranslator` — best-effort `TupleDomain<DucklakeColumnHandle>` → DuckDB SQL `WHERE`. Anything not safely translatable is omitted; Trino's filter operator runs on top, so partial pushdown stays correct (just less selective). Translation covers:
    - `Domain.singleValue` → `col = literal`
    - `Domain.multipleValues` (≥2 discrete) → `col IN (...)`
    - Range bounds (inclusive/exclusive, half-open, two-sided) → `col >=/> a AND col <=/< b`
    - Disjoint range unions → `(term1 OR term2 OR ...)`
    - `Domain.onlyNull` → `col IS NULL`
    - `Domain.notNull` → `col IS NOT NULL`
    - `Domain.create(values, nullAllowed=true)` → `(values_term OR col IS NULL)`
    - Tuple-level `none` → literal `FALSE` (defensive — split manager should already short-circuit)
    - Multi-column → `AND` join of per-column terms
- [x] Literal formatting types: BOOLEAN, integer family, REAL, DOUBLE, DECIMAL (short + long), DATE, short-precision TIMESTAMP, VARCHAR (with `'` escaping). TIMESTAMPTZ, VARBINARY, UUID, and long-precision TIMESTAMP are intentionally not formatted — they're left to Trino's filter operator. Unsupported types in the predicate are silently dropped (no error).
- [x] Identifier quoting: column names wrapped in double quotes with `"` → `""` escaping.
- [x] `DucklakePageSourceProvider.createDuckDbPageSource` now takes the `effectivePredicate` and threads it through. Predicate is restricted to `fileColumns` (excluding the synthetic `$row_id`) before being passed to the page source so we never emit `WHERE` references to a column we're not also projecting.
- [x] Unit test (`TestDuckDbWhereClauseTranslator`, 17 cases): all-domain → no WHERE, none → FALSE, integer equality, IN-list, half-open range, varchar with quote escape, date, timestamp µs, short decimal, long decimal, IS NULL only, IS NOT NULL only, value-or-null, multi-column AND, boolean equality, identifier escape, range union (OR).
- [x] Integration test (added to `TestDucklakeDuckDbFormatRead`, 6 new cases): equality, range, IN-list, IS NULL / IS NOT NULL, date+decimal pushdown, and a VARBINARY predicate that is *not* pushed (translator skips it) but still returns the right rows because Trino's filter operator handles it.
- [x] Full `:trino-ducklake:test` and `:ducklake-catalog:test` suites green.

**Exit criteria for step 4:** Selective queries on duckdb-format tables push the predicate into DuckDB SQL when the column type and predicate shape are supported, with Trino-side filtering as a transparent fallback for anything else. Correctness preserved unconditionally; perf comparison vs. parquet on a real workload is deferred to whenever we run the actual experiment. ✅

**Files changed in Step 4:**
- `jvm/trino-ducklake/src/.../DuckDbWhereClauseTranslator.java` (new)
- `jvm/trino-ducklake/src/.../DuckDbFilePageSource.java` — accepts `TupleDomain`, applies WHERE in `buildSelectSql`
- `jvm/trino-ducklake/src/.../DucklakePageSourceProvider.java` — threads `effectivePredicate` into `createDuckDbPageSource`, restricts to `fileColumns`
- `jvm/trino-ducklake/test/src/.../TestDuckDbWhereClauseTranslator.java` (new)
- `jvm/trino-ducklake/test/src/.../TestDucklakeDuckDbFormatRead.java` — 6 new pushdown cases (one renamed test)

---

## Open questions to revisit during implementation

A. **Appender vs INSERT INTO ... SELECT FROM arrow_stream.**
   Phase 1 uses the DuckDB Appender API (one row at a time, JNI per cell). Per-page `appender.flush()` was added 2026-05-05 to bound memory — without it, Appender buffers the entire CTAS in memory until close, OOMing on large loads. Flush is cheap (Pages are ~1024–65536 rows) so per-Page is a fine cadence. **Real next perf win:** Trino `Page` → Arrow `VectorSchemaRoot` (column-at-a-time, fast), then on the DuckDB side `connection.registerArrowStream("trino_in", ArrowArrayStream)` + `INSERT INTO t SELECT * FROM trino_in`. Per the DuckDB Java client docs that registration explicitly requires a real `org.apache.arrow.c.ArrowArrayStream` instance — passing arbitrary `ArrowReader`-shaped objects throws ClassCastException-style errors at the JNI boundary. So the implementation needs the `arrow-c-data` `Data.exportArrayStream(VectorSchemaRoot, ArrowArrayStream)` shape rather than a homebrew bridge. Mirrors the read side's `arrowExportStream`/`Data.importArrayStream` flow but reversed.

B. **DuckDB instance scope.** Step 3 ships with **per-split independent DuckDB instances** (one `jdbc:duckdb:` per page source, closed in `close()`). Trade-off: highest spinup cost per query, but trivial lifecycle and full isolation; READ_ONLY ATTACHes don't conflict so duplicate ATTACHes of the same physical file are correct, just wasted work. Migration path if profiling later shows the native init is hot: introduce a per-JVM `DuckDbInstanceRegistry` that hands out duplicated connections via `DuckDBConnection.duplicate()`; the page source releases its connection but not the instance. No interface change required. Mirrors the writer's deferred-registry decision.

C. **Materialization vs httpfs.** Phase 1: download `.db` via `TrinoFileSystem`, then DuckDB ATTACHes the local file. Phase 2+ may use DuckDB's httpfs for direct S3 attach on the cold-read path (per design conversation 2026-05-03). Not relevant in Phase 1 because we're filesystem-only for testing.

D. **Partitioned tables.** Phase 1 supports unpartitioned only. Partitioned writes need per-partition `.db` files; mostly mechanical extension of the Appender writer but worth a separate step.

E. **Stats extraction.** ✅ Done 2026-05-05. `DuckDbFileWriter.finishAndBuildFragment()` now runs a single aggregate query against the just-written DuckDB table (after appender flush, before DETACH) — `SELECT COUNT(*), COUNT(col1), MIN(col1), MAX(col1), COUNT(col2), MIN(col2), ...` — and converts the JDBC results to `DucklakeFileColumnStats` using the same string format as the parquet path's `DucklakeStatsExtractor`. Per-column rows are always emitted (even when MIN/MAX is unsupported, e.g. VARBINARY/UUID — empty Optionals there) so the DuckLake DuckDB extension's introspection paths don't choke on missing rows. Test: `TestDucklakeDuckDbFormatWrite.testColumnStatsWrittenForDuckDbFormat` reads `ducklake_file_column_stats` directly via JDBC and asserts value_count / null_count / min / max match the source data for INTEGER, VARCHAR, and INTEGER-with-NULLs columns. Remaining minor: `column_size_bytes` is set to 0 (parquet uses real compressed bytes from the footer); could affect DuckLake compaction heuristics in the future, can be revisited if it bites.

F. **Footer size.** Parquet-specific concept. For duckdb files, set to 0 in the fragment / catalog row. Confirm DuckLake spec doesn't reject 0 here.

G. **`data_file_format` cross-engine compatibility.** Setting `'duckdb'` in the spec field will make tables Trino-DuckLake-only. For Phase 1 this is intentional (we own both ends). Gate behind a feature flag for any release.

---

## Status log

- **2026-05-03** — Plan agreed with user. Doc `CONCEPT-duckdb-as-parquet-file-cache.md` updated with revised approach (per-conversation-mode option 1 vs option 2 for read, partition-level future idea, brutal-Phase-1 framing). This TODO file created. Starting Step 1.
- **2026-05-03** — Step 1 (skeleton) done. Session property `ducklake.data_file_format` plumbed end-to-end: `parquet` (default) keeps the existing writer/reader paths untouched, `duckdb` throws `NOT_SUPPORTED` at both ends. `ducklake_data_file.file_format` row now sourced from the fragment instead of hardcoded. 5/5 skeleton tests green; full trino-ducklake + ducklake-catalog suites green. Side fix: `@Deprecated` getter/setter pair mismatch in `DucklakeConfig` was blocking all integration tests on main. Ready for Step 2 (writer via DuckDB Appender API).
- **2026-05-04** — Step 2 (writer) done. CTAS / INSERT with the duckdb session property now produces a real `.db` file via the DuckDB Appender API and uploads it through `TrinoFileSystem`. `DucklakeFileWriter` interface now sits between the page sink and the format-specific writers (`ParquetFileWriter` extracted; `DuckDbFileWriter` new). Phase 1 type coverage: BOOLEAN/TINYINT/SMALLINT/INT/BIGINT/REAL/DOUBLE/DECIMAL/DATE/TIMESTAMP/TIMESTAMPTZ/VARCHAR/VARBINARY/UUID. Nested types throw. New write tests round-trip via direct DuckDB JDBC; the read path still throws `NOT_SUPPORTED` (Step 3). Both module suites green. Skipped the per-JVM `DuckDbInstanceRegistry` — current shape is one connection per writer, simpler and fast enough; can be revisited if profiling justifies it.
- **2026-05-04** — Step 3 (reader) done. SELECT through Trino against duckdb-format tables works end-to-end. Pipeline: `DucklakeMaterializedFileCache` (per-JVM, SHA-256-keyed atomic writes) downloads the `.db` to local tmp; `DuckDbFilePageSource` opens a per-split DuckDB JDBC instance, ATTACHes READ_ONLY, runs `SELECT <projected> FROM ...`, calls `arrowExportStream()` for the Arrow C-data export; `DucklakeArrowToPageConverter` materializes each batch into a Trino `Page`. Delete files and `$row_id` injection reuse the same wrappers as the parquet path. Empty projections (COUNT-only) currently throw `NOT_SUPPORTED`. New `TestDucklakeDuckDbFormatRead` (7 cases) covers scalar round-trip, NULLs, temporal+decimal, projection, predicate, aggregation, and a mixed parquet+duckdb join. `TestDucklakeDuckDbFormatWrite.testReadbackThroughTrino` flipped from "expect throw" to "expect success". Side fixes: `--add-opens=java.base/java.nio=ALL-UNNAMED` and `--add-opens=java.base/java.lang=ALL-UNNAMED` on the test JVM (Arrow's `MemoryUtil` requires private `DirectByteBuffer` access); test postgres `max_connections=500` (default 100 was hit by parallel test classes each opening DuckDB-postgres-extension connections). Both suites green (480 trino-ducklake tests).
- **2026-05-04** — Step 4 (pushdown) done. Predicate pushdown into the SQL we issue to DuckDB via the new `DuckDbWhereClauseTranslator`. Best-effort: anything we can't translate safely is skipped, and Trino's filter operator runs on top so the result is unconditionally correct. Coverage: equality, IN-list, range bounds (incl. half-open and disjoint unions), IS NULL / IS NOT NULL, value-or-null, multi-column AND, plus literal formatting for BOOLEAN, integer family, REAL, DOUBLE, DECIMAL (short+long), DATE, short-precision TIMESTAMP, and VARCHAR (with `'` escaping). Identifier quoting handles `"` in column names. Projection pushdown was already in Step 3. 17 unit tests on the translator + 6 new integration cases on `TestDucklakeDuckDbFormatRead` (eq, range, IN, IS NULL/IS NOT NULL, date+decimal, and a VARBINARY case that intentionally falls through to Trino-side filtering). Full `:trino-ducklake:test` and `:ducklake-catalog:test` suites green.
- **2026-05-04** — Empty-projection fix follow-up (post-Step 4). `COUNT(*)` (and any zero-column projection) on a duckdb-format table previously threw `NOT_SUPPORTED`. Switched the page source to emit a synthetic `SELECT 1 FROM t [WHERE ...]` and discard the column, returning empty-block `Page(rowCount)` per Arrow batch so counts and delete-file filtering still get the right positions. 2 new tests on `TestDucklakeDuckDbFormatRead` (`COUNT(*)` with and without WHERE). User hit this running the TPC-H side-by-side script — needed for the row-count sanity check in section 4.
- **2026-05-04** — Compose JVM args + production heads-up. The empty-projection error on the live stack was masking a deeper one: the Trino server JVM in `compose/docker-compose.yml` had no `--add-opens` flags, so every duckdb-format read failed Arrow's `MemoryUtil.directBuffer()` with `UnsupportedOperationException: sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available` — independent of the empty-projection issue. Added `JAVA_TOOL_OPTIONS` to the trino service in compose: `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED`. Documented the same requirement for production deployments in `compose/README.md`. Also added 2 explicit test cases to `TestDucklakeDuckDbFormatRead` mirroring the user's failing patterns: `count(<column>)` with an equality predicate, and a TPC-H Q1 shape (multi-aggregate GROUP BY with date predicate). The test JVM already had the flags, which is why the suite never caught this — the gap was test-vs-runtime config divergence. Worth a follow-up: surface the requirement louder in any future connector-install README so external deployers don't hit it cold.
- **2026-05-04** — Bootstrap broken by missing column_stats (DuckLake extension can't introspect). With duckdb-format tables in the lake, the DuckDB bootstrap script's informational `count(*) FROM duckdb_tables() WHERE database_name = 'lake'` errors with `INTERNAL Error: Calling GetValueInternal on a value that is NULL` deep inside `TransformGlobalStats`. Cause: the DuckLake extension v1.5.2 aggregates per-table stats via `GetGlobalTableStats`, which eagerly scans `column_stats` rows for every data file in the lake — and our `DuckDbFileWriter` writes empty `columnStats` (Phase 1 deferral). Net effect: any duckdb-format table existing in the catalog breaks DuckDB-side introspection of the *whole* lake (including bootstrap re-runs). Patched `compose/bootstrap/init_ducklake.py` to be defensive: try/except around the status banner, and an `information_schema.tables` fallback in `seed_tpch()` for the existing-table check. Documented the cross-engine breakage and `down -v` recovery path in `compose/README.md`. The proper fix (extract column stats from the `.db` file during `finishAndBuildFragment`) is now logged as Phase 1.5 follow-up under open question E.
- **2026-05-05** — `data_file_format` table property added. CREATE TABLE / CTAS can now opt in per-statement: `CREATE TABLE foo (...) WITH (data_file_format = 'duckdb')` or `CREATE TABLE foo WITH (data_file_format = 'duckdb') AS SELECT ...`. The property overrides the session-level default for that single CREATE only — INSERTs into existing tables still inherit from the session because there's no per-table format persistence in Phase 1 (the DuckLake catalog tracks per-data-file format, not per-table). Validation matches the session property: must be `'parquet'` or `'duckdb'`. 3 new tests on `TestDucklakeDuckDbFormatWrite`: property forces duckdb on a parquet-default session (CTAS), property forces parquet on a duckdb-default session (override), and an invalid value is rejected at planning time.
- **2026-05-05** — Per-Page `appender.flush()` in `DuckDbFileWriter.write(Page)`. Without this the Appender buffered every row of a CTAS in memory until `close()`, OOMing on larger loads (user hit it loading sf1 partsupp/lineitem to OVH). Flush per-Page bounds memory to ~one Page at a time at negligible cost. Doesn't fix the per-row JNI overhead — that requires the Arrow-stream optimization documented under open question A — but does unblock larger CTAS without bumping the JVM heap.
- **2026-05-05** — Phase 1.5: column-stats extraction landed. `DuckDbFileWriter.extractColumnStats()` issues a single aggregate query against the freshly-written DuckDB table (after appender flush, before DETACH) and persists per-column `value_count` / `null_count` / `min_value` / `max_value` to `ducklake_file_column_stats`. Stats string formatting matches the parquet path's `DucklakeStatsExtractor` for cross-engine consistency. Per-column rows are emitted for every column — even VARBINARY/UUID where MIN/MAX is meaningless (empty Optionals there) — so the DuckLake DuckDB extension's `TransformGlobalStats` no longer chokes on missing rows. New test `TestDucklakeDuckDbFormatWrite.testColumnStatsWrittenForDuckDbFormat` reads `ducklake_file_column_stats` directly via JDBC and asserts the rows are correct for INTEGER, VARCHAR-with-nulls, and INTEGER-with-nulls. Bootstrap defensive fallbacks remain in place for any old data still in the lake from before this fix; new writes will not break introspection. Audit of prior steps for other dangling bugs found nothing critical — `column_size_bytes=0` and `footer_size=0` for duckdb files are noted as minor follow-ups but don't break anything live.
