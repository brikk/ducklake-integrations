# TODO: Lance file format support

**Status:** Planning. No code yet (grep confirms zero `lance` references in connector src/test as of 2026-06-08).
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
- [ ] Add `ConnectorTableFunction`s: `lance_vector_search`, `lance_fts`, `lance_hybrid_search` under `my_catalog.system.*` (parallel to `add_files`). Each resolves Lance files from the catalog, emits splits carrying `(path, column, query_vec, k, prefilter)`, and the page source runs the corresponding `lance_*` DuckDB function. This is the novel SPI work — we have no table functions today (`add_files` is a procedure, different shape). See RESEARCH §2.
- [ ] (Stretch) `applyTopN` so a Trino-side `ORDER BY <distance> LIMIT k` synthesizes the function's `k`.

### Phase A4 — write (after the dataset-vs-file decision in Phase 0)
- [ ] `DucklakePageSink.openNewWriter` (≈:307): add a `FORMAT_LANCE` writer branch using DuckDB `COPY … TO (FORMAT lance)`.
- [ ] Catalog rows for the produced dataset per the Phase-0 decision. `resolveWriteFormat` already plumbs the session/catalog format through.

---

## Route B — direct via `lance-core` JNI (extend the vendored `lance-trino` plugin)

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

### Route A vs B decision
Current lean (RESEARCH §4.3): **A first** (single engine, matches the `.db` story). B becomes the priority if (a) DuckDB's lance extension lags upstream Lance, (b) the indirection shows up in measured vector latency, or (c) we want the `prefilter`/`substraitAggregate`/`setColumnOrderings` surface the JNI exposes cleanly. **Benchmark A vs B** (same dataset/vector/k, cold + warm) before committing to one as primary.

---

## Risks / open questions
- **Dataset-vs-file** (Phase 0) — gates all write work and `$path`/`add_files` semantics.
- **Extension availability at test time** — `INSTALL lance` may need network (extensions.duckdb.org). Decide whether read-path tests run only when the extension is reachable (tag/skip), or pre-stage the extension like we bundle `trino_parity`. Mirror the JVM-test-env approach.
- **Embedding type** — `ARRAY(REAL)` vs a future Trino `VECTOR(N)`; schema validation in the meantime (RESEARCH §5).
- **`prefilter` predicate routing** — pushing `WHERE category='x'` into a table-function's `prefilter` arg needs expression pushdown on a table-function input; verify the Trino SPI surface (RESEARCH §5).
- **Two cred paths under Route B** — DuckDbS3Config vs lance storageOptions.

## Test plan (both routes)
- Probe read (Phase 0/A0): small dataset, exact-shape read.
- Standard read: projection, predicates (TupleDomain + function-shape) over `lance_scan`.
- Vector search: `lance_vector_search` returns k rows, ordered by distance; `prefilter` narrows.
- Cross-engine: file written by DuckDB read by the connector and vice-versa.
- Write (Phase A4/B): round-trip a Lance dataset through the connector.
