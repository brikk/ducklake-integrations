# TODO: Lance file format support

**Status:** SHIPPED (Route A). Read + write + `add_files` (incl. partitioned via `hive_partitioning`) + the `lance_vector_search` / `lance_fts` / `lance_hybrid_search` table functions are all in the connector (Phases A0–A4 done). Route B (lance-core JNI) is MOOT — see REPORT-lance-route-a-vs-b.md. Index lifecycle (F3) is PARKED (RESEARCH-lance-index-lifecycle.md). Search v1 gates (all-lance tables, no row-level deletes, s3 quack-only) are deliberate.
**Scope:** trino-ducklake connector — read first, then write. Two implementation routes (A: via the DuckDB `lance` extension; B: direct via `lance-core` JNI / the vendored `lance-trino` plugin). Both are tracked here; they are not mutually exclusive.
**Design basis:** [RESEARCH-lance-and-pushdown.md](RESEARCH-lance-and-pushdown.md) (deep design — Trino pushdown SPI surface, table-function shape for vector/FTS, route A vs B trade-offs, `ScanOptions` surface). This file is the actionable, chunked plan; that file is the rationale.
**Sibling:** [TODO-vortex.md](TODO-vortex.md) shares Route A's "scan a file via a DuckDB format table-function" machinery.
**▶ Pick-up doc:** [HANDOFF-lance-route-a.md](HANDOFF-lance-route-a.md) — read-dispatch wiring (Phase A1) is **done** on branch `worktree-trino-ducklake`; its probe test **skips on the Intel box** (lance ext 404 on osx_amd64) and must be run on an arm64/linux host. The handoff is the step-by-step to resume there.

---

## 0. Catalog / spec angle (same for both routes)

`ducklake_data_file.file_format` is an opaque string; `'lance'` is a new value alongside `'parquet'` / `'duckdb'`, no spec change. Same cross-engine contract as our `'duckdb'` extension: opt-in per table, only readers that understand `'lance'` see those files. Worth proposing upstream alongside `'duckdb'`.

**Platform gap (probed 2026-06-08, v1.5.3):** the `lance` DuckDB extension is published for **osx_arm64 + linux_amd64 only — it is 404 for osx_amd64** (the Intel dev box). So the in-process Route-A probe/read CANNOT run on this Intel machine; it must go through the **linux_amd64 quack container** (extension available there), or run on an arm64 / linux host. Vortex has no such gap (published for all three). Factor this into where Lance read tests run.

**Lance is a dataset, not a file (the central risk).** `COPY … TO 'x.lance' (FORMAT lance)` produces a *directory* (manifest + data + index files), but `ducklake_data_file.path` is per-file. Resolve this BEFORE write work; it also shapes how `$path` / `add_files` behave. Options to evaluate in Phase 0:
- record the dataset directory as the `path` and treat it as opaque (one catalog row per dataset version), or
- record one row per Lance fragment file (closer to the per-file model but more catalog churn).
Read-only probing can sidestep this by registering an externally-written dataset via `add_files` with the directory path.

**RESOLVED — option A (2026-06-09, arm64):** `__lance_scan('<dir>')` accepts the dataset directory directly; one catalog row per dataset version, `path` = dir, opaque. `add_files(..., file_format => 'lance')` now registers such a directory (skips parquet footer, sources `record_count` by scanning via `__lance_scan`, no stats, read-by-name). The SQL-level read works end-to-end (`TestDucklakeLanceAddFiles`). One read-path fix was needed: `createPageSource` must NOT `newInputFile()` a lance directory (trailing-slash location) — only the parquet branch opens a `TrinoInputFile`; the DuckDB-engine branch reads via the path string. The lance writer (Phase A4) will own the dataset-write side; **decided: local-temp-then-upload, mirroring vortex.**

---

## Verification & maintenance status (2026-06-09)

Checked `lance-format/lance-duckdb` (Route A extension) and the maintenance of both:

- **Route A surface is real + tested + actively maintained.** Extension `0.5.1` (semver), Lance
  Rust core pinned to stable `v2.0.0`, **last commit 2026-06-09**, automated dependency bumps on
  new Lance releases. Tests exercise the full surface: `lance_vector_search` / `lance_fts` /
  `lance_hybrid_search` (with `k`, `prefilter`, input validation), scan + predicate pushdown
  (`pushdown_filter_ir_types`, `pushdown_regex`, TPCH-Q1), DML (truncate, `merge_returning`,
  alter/schema-evolution), maintenance (cleanup/metrics), namespace (`ATTACH … TYPE LANCE`, REST),
  S3/MinIO, write via `COPY … (FORMAT lance, mode 'overwrite'|'append')` incl. direct `s3://`.
  `prefilter=true` forces filter pushdown (errors if it can't); `prefilter=false` is best-effort
  with DuckDB fallback. **Vector/FTS/hybrid are available TODAY.**
- **Route B (lance-trino) is also being maintained** (lance-core bumps + incremental work) — so
  "let it bake" is viable.

**Decision lean: when Lance happens, do Route A; let Route B bake.** Rationale:
- Route A needs no code absorption — DuckDB owns the dataset internals; we hand `lance_scan` /
  `lance_vector_search` a **path**. The dataset-vs-file friction is soft: record the dataset
  directory as the `data_file.path` (one catalog row per dataset). Wiring = the vortex-style
  `FileScan` read + three `ConnectorTableFunction`s (vector/FTS/hybrid). Vectors land with modest
  effort, riding a daily-shipping extension.
- Route B is the "absorb-and-rework a dataset-focused plugin" burden (a fork). Since it's actively
  maintained, don't fork now — revisit only if they add vectors, grow a file-focused mode, or
  DuckDB indirection proves a measured bottleneck.
- Caveat unchanged: Route A is `linux_amd64`/`arm64` + `osx_arm64` only (no Intel Mac) → container
  or Apple-Silicon to run.

## Route A — via the DuckDB `lance` extension (recommended first)

Leans on DuckDB as the single execution engine for non-parquet formats, exactly like the `.db` path. `lance` is a **core** DuckDB 1.5.3 extension (plain `INSTALL lance; LOAD lance;`, verified 2026-06-06). Lower risk, shares machinery with Vortex.

**Key difference from the `.db` read path:** `.db` files are read by ATTACHing a database (`createDuckDbPageSource` → `DuckDbFilePageSource` over an attach target). Lance is read by a **table function over a path** — `SELECT <cols> FROM lance_scan('<resolved path>')` — NOT an ATTACH. So Route A's core work is generalizing the DuckDB executor's *source* from "ATTACH a db file" to "scan a file via a format-specific table function." This generalization is shared with Vortex.

### Phase A0 — probe (de-risk first)
- [x] Write one `.lance` dataset out-of-band via DuckDB (`INSTALL lance; COPY … TO 'x.lance' (FORMAT lance)`), register it against a DuckLake table with `file_format='lance'` (via `add_files` once that accepts a format arg, or a hand-seeded catalog row), attempt a Trino read, record what blows up. Captures the dataset-vs-file shape and the `lance_scan` projection/column-mapping behavior. Output: a short findings note appended here.

**Phase A0 findings — run on arm64 (M1 Max, osx_arm64), 2026-06-09, duckdb_jdbc 1.5.3.0:**
1. **Scan function is `__lance_scan`, NOT `lance_scan`.** The probe initially failed with
   `Catalog Error: Table Function with name lance_scan does not exist! Did you mean "lance_fts"?`.
   The shipped extension exposes the dataset scan as `__lance_scan('<dir>', explain_verbose := false)`
   (double-underscore). Read wiring + probe test corrected `lance_scan` → `__lance_scan`; probe is now
   **green (skipped=0, failures=0)**. (HANDOFF O3 / version churn — the old docs said `lance_scan`.)
2. **Dataset-vs-file (Step 2): decision A confirmed.** `__lance_scan('<dataset-dir>')` accepts the
   directory path directly and streams all rows — no per-fragment registration needed. Proceed with
   **one catalog row per dataset version, `path` = the dataset directory, treated as opaque.**
3. **Type audit (Step 3):** round-tripped a rich source table through `COPY (FORMAT lance)` +
   `__lance_scan`. **Scalars all round-trip cleanly** — INTEGER, `DECIMAL(18,3)`, DATE, TIMESTAMP,
   STRUCT, and variable `INTEGER[]` come back with identical DuckDB types. **The embedding column is
   the one transform:** a source `FLOAT[]` is stored and returned by lance as **`FLOAT[3]` — a
   fixed-size list (`FixedSizeList<float>`)**, exactly the RESEARCH §5 prediction → must map to
   Trino `ARRAY(REAL)`. **Converter gap found:** `DucklakeArrowToPageConverter` is scalar-only (no
   `ArrayType`/`RowType`/`MapType`) — any nested column currently throws `NOT_SUPPORTED`. Addressed
   below by adding `ARRAY` support (fixed + variable list of scalar elements); ROW/MAP still deferred.
4. Other lance table functions confirmed present for Phase A3: `lance_fts`, `lance_vector_search`
   (FLOAT[]/DOUBLE[] overloads), `lance_hybrid_search`. Maintenance helpers are `__`-prefixed too.

### Phase A1 — read dispatch + executor scan-source generalization (DONE — wiring; probe gated)
- [x] `DucklakeSessionProperties`: added `FORMAT_LANCE = "lance"`. **Validators left STRICT**
  (lance write rejected; read-only, mirroring vortex V1 — reads dispatch on the catalog
  `file_format`, not the write validators).
- [ ] ~~`DucklakeTableProperties`: accept `'lance'`~~ — deferred to A4 (write). Read doesn't need it.
- [x] `DucklakePageSourceProvider.createPageSource`: `FORMAT_LANCE` routes through
  `createDuckDbPageSource` (with duckdb + vortex), wrapped by `injectConstantVirtuals`.
- [x] DuckDB executor source already generalized (vortex did this). Lance reuses
  `DuckDbAttachTarget.FileScan(path, "lance_scan", "lance", s3Config?)`. **Dataset-dir quirk:**
  `resolveDuckDbReadTarget` gives lance an early return that **bypasses the single-file
  materialize cache** (lance is a directory; `lance_scan` reads the whole dir).
- [x] Column projection + type mapping audit — **done on arm64** (HANDOFF Step 3). Scalars all
  round-trip; embedding `FLOAT[]` → Arrow `FixedSizeList<float>` (`FLOAT[N]`) → `ARRAY(REAL)`.
  `DucklakeArrowToPageConverter` was scalar-only → added `ARRAY` support (fixed + variable list of
  scalar/nested-array elements; ROW/MAP + timestamp/uuid elements still NOT_SUPPORTED). Verified
  end-to-end by `TestDucklakeLanceFileScanRead.fileScanReadsLanceEmbeddingColumnAsArrayOfReal`.
- [x] Probe test `TestDucklakeLanceFileScanRead` — executor-level FileScan read; **skips on
  osx_amd64** (404), runs on arm64/linux. This IS the Phase A0 probe; just needs a capable box.

### Phase A2 — predicate pushdown (standard)
- [x] TupleDomain pushdown flows through the existing `DuckDbWhereClauseTranslator` / `DuckDbSelectSqlBuilder` exactly as the `.db` path does — a `WHERE`-clause predicate renders into the `__lance_scan(...)` query (no ATTACH alias) and reaches DuckDB; verified on arm64 by `TestDucklakeLanceFileScanRead.fileScanPushesPredicateIntoLanceScan` (pushed `id=2` returns only that row). Function-shape (`pushedExpressions`) uses the same channel; lance `prefilter` semantics still matter for the Phase A3 table functions.

### Phase A3 — vector / FTS / hybrid table functions
- [x] **`lance_vector_search` DONE (2026-06-10, arm64).** First `ConnectorTableFunction` in the connector:
  `SELECT ... FROM TABLE(<catalog>.system.lance_vector_search(schema_name, table_name, column_name,
  query_vec, k, prefilter))`. The SPI chain (all greenfield): `LanceVectorSearchTableFunction.analyze`
  (resolves the table's lance dataset dirs from the catalog + builds the output descriptor = table
  columns + `_distance REAL`) → `LanceVectorSearchFunctionHandle` → `DucklakeSplitManager.getSplits(
  ConnectorTableFunctionHandle)` (one `LanceVectorSearchSplit` per dataset dir) →
  `LanceVectorSearchSplitProcessor` (a `TableFunctionSplitProcessor`) runs DuckDB
  `lance_vector_search('<dir>', '<col>', [..]::DOUBLE[], k := …, prefilter := …)` through the
  executor factory and converts Arrow→Page with the shared converter. Wiring: `Multibinder
  <ConnectorTableFunction>` in `DucklakeModule`, `DucklakeConnector.getTableFunctions()` (wrapped
  `ClassLoaderSafeConnectorTableFunction`) + `getFunctionProvider()` → `DucklakeFunctionProvider`.
  Key answers from the spike:
  - **`query_vec` argument**: a plain `ScalarArgumentSpecification` of `ArrayType(DOUBLE)` works —
    the engine evaluates the constant and `ScalarArgument.value` arrives as a `Block`; a bare
    `ARRAY[1.0, 0.0, 0.0]` literal (array(decimal)) coerces to `ARRAY(DOUBLE)` with no caller cast.
    There is no array-argument restriction in the Trino 481 analyzer.
  - **DuckDB signature** (extension June 2026): `lance_vector_search(VARCHAR dir, VARCHAR col,
    FLOAT[]|DOUBLE[] vec, k := BIGINT, nprobs, use_index, refine_factor, prefilter BOOLEAN,
    explain_verbose)`. Output = dataset columns + `_distance FLOAT`, ascending, exactly k rows.
    `prefilter` is a BOOLEAN flag: a WHERE over the function output is post-filtered (can return
    < k) unless DuckDB pushes it into the function with `prefilter := true` (filter-then-search).
  - **Semantics**: one split per dataset fragment, each computes a *local* top-k → a multi-fragment
    table returns up to `k × fragments` rows (superset of global top-k); `ORDER BY _distance LIMIT k`
    recovers exact (pinned by `vectorSearchOverMultipleFragmentsReturnsPerFragmentTopK`).
  - **v1 scope**: local-path datasets only (s3 gated on O1), all data files lance-format, no
    row-level deletes (inlined or file), embedding column must be an array type.
  Tests: `TestDucklakeLanceVectorSearch` (5 e2e: nearest+distance, SELECT * incl. embedding,
  SQL composition, arg validation + empty table, multi-fragment, non-lance rejection) +
  `TestLanceVectorSearchSql` (arg-tail rendering). `FileScan` gained `extraArgsSql` (default "")
  rendered after the quoted path by both executors.
- [x] **`lance_fts` + `lance_hybrid_search` DONE (2026-06-10, arm64).** Mirrored via a shared-
  machinery refactor: `AbstractLanceSearchTableFunction` (catalog resolution + v1 guards + arg
  helpers + score-column constants), marker interface `LanceSearchHandle { datasetPaths,
  outputColumns }` (split manager + function provider dispatch on it), shared `LanceSearchSplit` +
  `LanceSearchSplitProcessor` (scan function + arg tail dispatched per concrete handle).
  Probe facts (extension June 2026):
  - **FTS works WITHOUT an inverted index** (brute force; an index in the dataset accelerates
    transparently). `lance_fts(dir, col, query, k := …, prefilter := …)` returns *matching rows
    only* + `_score FLOAT` descending. **`k` is best-effort for FTS** — the shipped build returned
    2 rows for k:=1 — so the connector documents "up to/around k" and the exact recipe is
    `ORDER BY _score DESC LIMIT k`. `prefilter := true` changes BM25 corpus stats (scores differ
    from post-filter), confirming genuine filter-then-search.
  - **Hybrid** is positional `lance_hybrid_search(dir, vec_col, vec, fts_col, query, k := …,
    alpha := …, prefilter := …)`; output = dataset cols + `_distance` + `_score` (**NULL** for
    rows with no text match — score handles are nullable) + `_hybrid_score`, descending. Trino
    arg `ALPHA DOUBLE` is optional via `defaultValue(null)` (verified working) — omitted ⇒ not
    rendered ⇒ extension default blend; given ⇒ validated to [0,1] and rendered `alpha := x`.
  Trino surface: `lance_fts(schema_name, table_name, column_name, query, k, prefilter)` (column
  must be varchar); `lance_hybrid_search(schema_name, table_name, vector_column, query_vec,
  text_column, query, k, alpha, prefilter)` (array + varchar validation). Tests:
  `TestDucklakeLanceFtsAndHybridSearch` (4 e2e) + `TestLanceSearchSql` (4, tail rendering +
  scan-fn dispatch; replaces TestLanceVectorSearchSql).
- [x] **O2 + applyTopN DONE (2026-06-10, arm64) — `applyTableFunction` scan migration.** The
  split-processor path has NO pushdown hooks in the SPI; the route is
  `ConnectorMetadata.applyTableFunction(handle)` → `TableFunctionApplicationResult(
  LanceSearchTableHandle, outputColumns)` — the engine's `RewriteTableFunctionToTableScan` then
  plans an ordinary scan and the connector hooks compose:
  - **`applyFilter`** intersects the TupleDomain into `LanceSearchTableHandle.pushedPredicate`;
    the page source (`createLanceSearchPageSource` → reused `DuckDbFilePageSource`) renders it
    as the WHERE over the `lance_*` call. All predicates stay in the remaining filter (engine
    re-applies — correct under both prefilter modes). Projection now reaches DuckDB too (the
    processor path always materialized every column).
  - **`applyTopN`** recognizes `ORDER BY <natural score column> <natural direction> LIMIT n`
    (single sort item; `_distance` ASC / `_score` DESC / `_hybrid_score` DESC via
    `LanceSearchHandle.scoreOrderColumn()/scoreOrderAscending()`) and trims the per-fragment
    `k` to `limitK = n` (`topNGuaranteed=false` — engine still sorts+limits; pure row-volume
    reduction; per-fragment top-n stays a superset of global top-n).
  - **prefilter edge (probed live):** with `prefilter := true` lance REQUIRES any WHERE over
    the call to be pushable into the function — DuckDB pushes single-range conjuncts
    (`=`, `>`, `BETWEEN`) but NOT OR-of-ranges or IN-lists, and lance errors
    ("requires filter pushdown for prefilterable columns") on unpushable shapes. No WHERE at
    all + prefilter is fine. So `createLanceSearchPageSource` renders only single-range,
    non-null-allowing domains when `prefilter=true` (`isPrefilterPushable`); unpushable shapes
    (e.g. `<>` = two ranges) stay engine-side and degrade to post-filter semantics instead of
    erroring. Pinned by `vectorSearchPushesPredicateIntoLanceWithPrefilterSemantics`:
    `WHERE id >= 2, prefilter=true` → full k among survivors (filter-then-search observable);
    `WHERE id <> 1, prefilter=true` → no error, post-filter row count.
  - **Jackson gotcha (cost one debug round):** a handle field whose DECLARED type is assignable
    to `ConnectorTableFunctionHandle` gets hijacked by the engine's `AbstractTypedJacksonModule`
    (matches by runtime type for serialization, by exact declared type for deserialization) and
    fails with "Type id handling not implemented" if any `@JsonTypeInfo` is present. Resolution:
    `LanceSearchHandle` is NOT a `ConnectorTableFunctionHandle` subtype (concretes implement
    both), and `LanceSearchTableHandle.searchHandle` is declared as the bare
    `ConnectorTableFunctionHandle` so the engine's own envelope handles nested polymorphism.
  The processor path (`DucklakeFunctionProvider` + `LanceSearchSplitProcessor.process`) stays
  wired as a fallback for engines that skip the rewrite; the same processor companion renders
  the scan-fn + arg tail for both paths.

### Phase A4 — write (DONE — 2026-06-09, arm64)
- [x] `DucklakePageSink.openNewWriter`: added a `FORMAT_LANCE` branch (`openLanceWriter`) reusing the Arrow-stream writer. **Local-temp-then-upload** (decided): `COPY (SELECT * FROM <arrow-stream>) TO '<localtmp>.lance' (FORMAT lance)`, then walk the dataset dir and upload every file under the remote location. In `DuckDbArrowStreamFileWriter`, `isVortex` → `usesCopy = isVortex || isLance` (shared COPY + inline `DucklakeColumnStatsAccumulator`, no ATTACH); lance adds directory-aware size/upload/cleanup + `deleteDirectory` on abort. Both `validateDataFileFormat` validators now accept `'lance'`.
- [x] Catalog rows: the writer emits a `DucklakeWriteFragment(relativePath, "lance", …)` per the Phase-0 dataset model (one row, dir path, relative). Round-trip verified by `TestDucklakeLanceFormat` (CTAS + INSERT + SELECT/predicate).
- [x] **ARRAY/embedding writes DONE (2026-06-10).** The Arrow-stream writer maps `ARRAY(scalar)`
  to Arrow `List<element>` (`toArrowField` child field + `populateListVector` via
  `UnionListWriter`; elements: boolean/tinyint/smallint/int/bigint/real/double/varchar). DuckDB
  stores LIST and the lance COPY writer materializes uniform float lists as FixedSizeList — the
  exact shape `lance_vector_search` consumes, so **embedding CTAS/INSERT now closes the full
  loop**: write embeddings through Trino, search them with the table function (pinned by
  `TestDucklakeLanceFormat.embeddingCtasInsertThenVectorSearchRoundTrip`, incl. a NULL array row
  and the multi-fragment CTAS+INSERT case). Stats: array columns record value/null counts only
  (no min/max — `DucklakeColumnStatsAccumulator.supportsMinMax` skips them). Limits: NULL array
  *elements* rejected (no positional null append on the list sub-writers; embeddings never carry
  them), nested arrays / ROW / MAP elements still fail fast at writer setup. NOTE: the writer is
  shared — ARRAY writes are now also accepted toward `.db`/vortex targets (DuckDB LIST handles
  them; vortex COPY behavior untested).

---

## Route B — direct via `lance-core` JNI (extend the vendored `lance-trino` plugin)

**MOOT (2026-06-12): the A-vs-B benchmark decided for Route A as primary — do NOT fork
lance-trino. See REPORT-lance-route-a-vs-b.md (dev-docs/archive/) for the record. The boxes
below are retained for the historical option only; none are planned.**

Native path, no DuckDB indirection; full vector/FTS/index surface (`nearest`, `fullTextQuery`, `prefilter`, `useScalarIndex`, `substraitAggregate`, `setColumnOrderings`) sits in `org.lance.ipc.ScanOptions.Builder` — the vendored plugin wires only 5 of 16. See RESEARCH §4.2 for the full surface.

**Prereqs:**
- [ ] Run `vendor/clone-related-projects.sh` to materialize `vendor/lance-trino` (clone URL `lance-format/lance-trino`, Apache 2.0). Inspect its current shape (columnar reader: `LanceRuntime`, `LanceArrowToPageScanner`, `SubstraitExpressionBuilder`, `LancePageSink`, `ScannerFactory`/`FragmentScannerFactory`).
- [ ] Confirm `org.lance:lance-core:6.0.0` is resolvable from the local maven repo (RESEARCH §6 says it was present 2026-05-26). Note the `--add-opens` requirement (same as the Arrow C-data path we already document — no new JVM flag cost).

### Phase B1 — wire `data_file_format='lance'` reads through a JNI page source
- [ ] Decide integration: (a) fork/overlay the vendored plugin's reader into this connector, or (b) depend on it as a library and adapt. RESEARCH leans "extend locally — smaller delta than green-field."
- [ ] Add a `createLanceJniPageSource` branch (or make the Phase-A1 dispatch route to JNI when an engine toggle selects it). Reuse the plugin's `LanceArrowToPageScanner`.
- [ ] Storage creds: lance-core takes `Map<String,String> storageOptions`, **separate** from our `DuckDbS3Config`. Add a translation from the connector's S3 config to lance storageOptions.

### Phase B2 — extend `ScannerFactory` for the vector/FTS surface
- [ ] Extend `ScannerFactory.open(...)` with `Optional<Query> nearest`, `Optional<FullTextQuery> fts`, `boolean prefilter`; `FragmentScannerFactory` plumbs them onto `ScanOptions.Builder`. Backward-compatible as optional args (RESEARCH §4.2).
- [ ] The same three `ConnectorTableFunction`s from A3, but building `org.lance.ipc.Query` / `FullTextQuery` and passing them via a table-handle field → split → page source → scanner factory.

### Route A vs B decision — DECIDED: Route A is primary (benchmarked 2026-06-12)
Measured (same dataset/vectors/k, brute-force, cold + warm — full numbers in
[REPORT-lance-route-a-vs-b.md](REPORT-lance-route-a-vs-b.md), harness `BenchLanceRouteAVsB`
re-runnable via `-Dducklake.bench=true`): warm medians at parity at 384 dims, ~10% Route A
overhead at 768 dims, cold start comparable (0.27–0.44 s both — the predicted extension
cold-start penalty did not materialize). B's only consistent edge is tighter tail latency
(single-digit to low-tens of ms — below Trino's own split-scheduling noise). **Not
decision-grade → do NOT fork lance-trino.** The original triggers stand for reopening:
(a) extension lags upstream — watched by the O3 canary; (b) latency — now measured, fine;
(c) JNI-only surface (`substraitAggregate`/`setColumnOrderings`) — no current need.

---

## Risks / open questions
- **Dataset-vs-file** (Phase 0) — gates all write work and `$path`/`add_files` semantics.
  **RESOLVED: option A** — `__lance_scan('<dataset-dir>')`, one catalog row per dataset, path = dir.
- **Extension availability at test time** — `INSTALL lance` may need network (extensions.duckdb.org). Decide whether read-path tests run only when the extension is reachable (tag/skip), or pre-stage the extension like we bundle `trino_parity`. Mirror the JVM-test-env approach.
  **RESOLVED: tag/skip** (`assumeLanceExtensionAvailable()` in every lance test). Pre-staging was
  rejected when O3 established the repo hosts only the latest build per (DuckDB version, platform) —
  vendoring would mean trino_parity-style per-platform bundling for no pin benefit.
- **Extension version churn (HANDOFF O3)** — lance-duckdb ships ~daily and already renamed
  `lance_scan` → `__lance_scan` once. **RESOLVED (2026-06-11): no repo-side pin is possible**
  (`INSTALL lance VERSION '…'` parses on DuckDB 1.5.3 but the versioned URLs 404), so
  `TestLanceExtensionCanary` FORCE-installs the currently served build, asserts every rendered call
  shape (`__lance_scan`, search positional prefixes + `k`/`prefilter`/`alpha` named args,
  `COPY (FORMAT lance)` behavior), then trips if the served build hash ≠ the verified pin
  (`533e0ee`). Bump workflow in the class doc.
- **Embedding type** — `ARRAY(REAL)` vs a future Trino `VECTOR(N)`; schema validation in the meantime (RESEARCH §5).
- **`prefilter` predicate routing** — pushing `WHERE category='x'` into a table-function's `prefilter` arg needs expression pushdown on a table-function input; verify the Trino SPI surface (RESEARCH §5).
  **RESOLVED:** `applyTableFunction` → `LanceSearchTableHandle` scan rewrite; `applyFilter` honors
  the user's `prefilter` flag (§A3 gotchas for the non-pushable WHERE shapes).
- **Two cred paths under Route B** — DuckDbS3Config vs lance storageOptions.

## Test plan (both routes)
- Probe read (Phase 0/A0): small dataset, exact-shape read.
- Standard read: projection, predicates (TupleDomain + function-shape) over `lance_scan`.
- Vector search: `lance_vector_search` returns k rows, ordered by distance; `prefilter` narrows.
- Cross-engine: file written by DuckDB read by the connector and vice-versa.
- Write (Phase A4/B): round-trip a Lance dataset through the connector.
