# TODO: Lance file format support

**Status:** Planning. No code yet (grep confirms zero `lance` references in connector src/test as of 2026-06-08).
**Scope:** trino-ducklake connector — read first, then write. Two implementation routes (A: via the DuckDB `lance` extension; B: direct via `lance-core` JNI / the vendored `lance-trino` plugin). Both are tracked here; they are not mutually exclusive.
**Design basis:** [RESEARCH-lance-and-pushdown.md](RESEARCH-lance-and-pushdown.md) (deep design — Trino pushdown SPI surface, table-function shape for vector/FTS, route A vs B trade-offs, `ScanOptions` surface). This file is the actionable, chunked plan; that file is the rationale.
**Sibling:** [TODO-vortex.md](TODO-vortex.md) shares Route A's "scan a file via a DuckDB format table-function" machinery.

---

## 0. Catalog / spec angle (same for both routes)

`ducklake_data_file.file_format` is an opaque string; `'lance'` is a new value alongside `'parquet'` / `'duckdb'`, no spec change. Same cross-engine contract as our `'duckdb'` extension: opt-in per table, only readers that understand `'lance'` see those files. Worth proposing upstream alongside `'duckdb'`.

**Platform gap (probed 2026-06-08, v1.5.3):** the `lance` DuckDB extension is published for **osx_arm64 + linux_amd64 only — it is 404 for osx_amd64** (the Intel dev box). So the in-process Route-A probe/read CANNOT run on this Intel machine; it must go through the **linux_amd64 quack container** (extension available there), or run on an arm64 / linux host. Vortex has no such gap (published for all three). Factor this into where Lance read tests run.

**Lance is a dataset, not a file (the central risk).** `COPY … TO 'x.lance' (FORMAT lance)` produces a *directory* (manifest + data + index files), but `ducklake_data_file.path` is per-file. Resolve this BEFORE write work; it also shapes how `$path` / `add_files` behave. Options to evaluate in Phase 0:
- record the dataset directory as the `path` and treat it as opaque (one catalog row per dataset version), or
- record one row per Lance fragment file (closer to the per-file model but more catalog churn).
Read-only probing can sidestep this by registering an externally-written dataset via `add_files` with the directory path.

---

## Route A — via the DuckDB `lance` extension (recommended first)

Leans on DuckDB as the single execution engine for non-parquet formats, exactly like the `.db` path. `lance` is a **core** DuckDB 1.5.3 extension (plain `INSTALL lance; LOAD lance;`, verified 2026-06-06). Lower risk, shares machinery with Vortex.

**Key difference from the `.db` read path:** `.db` files are read by ATTACHing a database (`createDuckDbPageSource` → `DuckDbFilePageSource` over an attach target). Lance is read by a **table function over a path** — `SELECT <cols> FROM lance_scan('<resolved path>')` — NOT an ATTACH. So Route A's core work is generalizing the DuckDB executor's *source* from "ATTACH a db file" to "scan a file via a format-specific table function." This generalization is shared with Vortex.

### Phase A0 — probe (de-risk first)
- [ ] Write one `.lance` dataset out-of-band via DuckDB (`INSTALL lance; COPY … TO 'x.lance' (FORMAT lance)`), register it against a DuckLake table with `file_format='lance'` (via `add_files` once that accepts a format arg, or a hand-seeded catalog row), attempt a Trino read, record what blows up. Captures the dataset-vs-file shape and the `lance_scan` projection/column-mapping behavior. Output: a short findings note appended here.

### Phase A1 — read dispatch + executor scan-source generalization
- [ ] `DucklakeSessionProperties`: add `FORMAT_LANCE = "lance"`; accept it in `validateDataFileFormat` (DucklakeSessionProperties.kt:151).
- [ ] `DucklakeTableProperties`: accept `'lance'` in `validateDataFileFormat` (DucklakeTableProperties.kt:81).
- [ ] `DucklakePageSourceProvider.createPageSource` (≈:166): add a `FORMAT_LANCE` branch routing to a new `createLanceViaDuckDbPageSource` (or a generalized `createFileScanPageSource(format)`), wrapped by `injectConstantVirtuals` like the others.
- [ ] DuckDB executor: generalize the source. Today `DuckDbFilePageSource` + `buildAttachSql` assume an ATTACH target. Add a "file-scan target" that renders `FROM lance_scan('<path>')` and issues `INSTALL lance; LOAD lance;` (hook alongside the httpfs INSTALL/LOAD in `InProcessDuckDbExecutor.kt:189` and the Quack equivalent). S3 path reuses the existing httpfs + secret setup.
- [ ] Column projection + type mapping: Lance → Arrow → Trino reuses the existing `DucklakeArrowToPageConverter`. Verify embedding columns (`FixedSizeList<float>`) land as `ARRAY(REAL)` (see RESEARCH §5 "Embedding column types").
- [ ] Tests: probe-style end-to-end read of a small lance dataset (gate on extension availability — see Risks).

### Phase A2 — predicate pushdown (standard)
- [ ] TupleDomain + function-shape pushdown flows through the existing `DuckDbWhereClauseTranslator` / `applyFilter` exactly as the `.db` path does — `WHERE`-clause predicates render into the `lance_scan` query. Confirm the translator works over a `lance_scan` source (no ATTACH alias).

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
