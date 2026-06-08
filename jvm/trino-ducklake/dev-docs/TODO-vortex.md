# TODO: Vortex file format support

**Status:** Planning. No code yet (grep confirms zero `vortex` references in connector src/test as of 2026-06-08).
**Scope:** trino-ducklake connector — read via the DuckDB `vortex` extension only. There is **no Trino-Vortex project** to adopt (the Vortex team has not published one), so unlike Lance there is no native-JNI route in scope; Vortex is DuckDB-engine-only for now.
**Shared machinery:** Route A in [TODO-lance.md](TODO-lance.md) — the "scan a file via a DuckDB format table-function" generalization of the DuckDB executor is the same work; Vortex is the second consumer. Land that generalization once, parameterize by format.

Vortex (Spiral DB) is a high-performance columnar format with compression-cascade encodings and lazy decompression; target use case is analytic scans where Parquet's row-group encoding is the bottleneck.

---

## 0. Catalog / spec angle

Same as Lance/duckdb: `ducklake_data_file.file_format = 'vortex'` is a new opaque string value, no spec change, opt-in per table, cross-engine only for readers that understand it. Vortex is a single-file format (unlike Lance datasets), so it fits DuckLake's per-file `path` model cleanly — **no dataset-vs-file complication**, which makes Vortex the simpler of the two epics.

**Extension availability:** `vortex` is a **core** DuckDB 1.5.3 extension (plain `INSTALL vortex; LOAD vortex;`, verified 2026-06-06 — HTTP 200 at `extensions.duckdb.org/v1.5.3/{osx_arm64,linux_amd64}`). The read probe is unblocked.

---

## Phase V0 — probe (DONE 2026-06-08)

Ran in-process via the DuckDB JDBC driver on this osx_amd64 box (vortex extension IS published for osx_amd64, unlike lance — see below). Findings:
- [x] **Write:** `COPY t TO 'x.vortex' (FORMAT vortex)` works (`FORMAT 'vortex'` quoted also works). Single ~4 KB file for 3 rows. Single-file output confirmed (no dataset/directory complication).
- [x] **Read:** all three forms work and return correct row counts:
  - `SELECT … FROM read_vortex('path')`  ← **use this** (explicit, parameterizable)
  - `SELECT … FROM 'path.vortex'`  (replacement scan on the extension)
  - `SELECT … FROM vortex_scan('path')`
- [x] **Types round-trip cleanly:** `(INTEGER, VARCHAR, DECIMAL(2,1))` came back exactly. No surprising encodings surfaced through Arrow for basic types (richer encodings still to test in V1).

> **Platform availability (probed 2026-06-08, v1.5.3):** `vortex` extension is published for **osx_amd64, osx_arm64, linux_amd64** (all 200). So the in-process read path works on this Intel dev box AND the linux quack container. (Contrast: `lance` is 404 on osx_amd64 — see TODO-lance.md.)

Probe ran as a temporary spike test (`ProbeVortexViaDuckDb`, since deleted — it needs network for `INSTALL vortex`).

## Phase V1 — read dispatch (reuse Lance Route-A machinery)
- [ ] **Depends on** the DuckDB executor's file-scan-target generalization from TODO-lance Phase A1. If that's landed, Vortex is mostly parameterization.
- [ ] `DucklakeSessionProperties`: add `FORMAT_VORTEX = "vortex"`; accept in `validateDataFileFormat` (DucklakeSessionProperties.kt:151).
- [ ] `DucklakeTableProperties`: accept `'vortex'` in `validateDataFileFormat` (DucklakeTableProperties.kt:81).
- [ ] `DucklakePageSourceProvider.createPageSource` (≈:166): add a `FORMAT_VORTEX` branch routing to the generalized file-scan page source with the vortex read function.
- [ ] DuckDB executor: render `FROM read_vortex('<path>')` (exact function name confirmed in V0) and `INSTALL vortex; LOAD vortex;` alongside the existing httpfs INSTALL/LOAD (`InProcessDuckDbExecutor.kt:189` + Quack equivalent). S3 reuses httpfs + secret setup.
- [ ] Type mapping: Vortex → Arrow → Trino via the existing `DucklakeArrowToPageConverter`. Note any Vortex encodings that don't round-trip to a Trino type.
- [ ] Tests: probe-style end-to-end read (gate on extension availability — same concern as Lance).

## Phase V2 — predicate pushdown
- [ ] TupleDomain + function-shape pushdown via the existing `DuckDbWhereClauseTranslator` / `applyFilter`, same as the `.db` and lance paths — predicates render into the `read_vortex` query. Vortex's compression-cascade should make pushdown especially valuable (skip decompression of pruned columns/ranges); verify the extension honors pushed filters.

## Phase V3 — write (optional, when a workload pushes)
- [ ] `DucklakePageSink.openNewWriter` (≈:307): add a `FORMAT_VORTEX` writer branch using DuckDB `COPY … TO (FORMAT vortex)`. `resolveWriteFormat` already plumbs the format through. Single-file output → straightforward catalog rows (no dataset complication).

---

## Risks / open questions
- **Exact scan/write syntax** — confirm `read_vortex(...)` / `COPY … (FORMAT vortex)` against the extension docs in V0; the extension may use a replacement scan on `.vortex` paths instead of an explicit function.
- **Extension availability at test time** — `INSTALL vortex` may need network; same decision as Lance (tag/skip vs pre-stage). Mirror the JVM-test-env approach.
- **Encoding coverage** — Vortex's cascade encodings are richer than Parquet's; verify all map cleanly through Arrow to Trino types, and that the extension's reader exposes them as standard Arrow.
- **Maturity** — Vortex and its DuckDB extension are newer than Lance's; pin the extension version and watch for API churn.

## Test plan
- Probe read (V0): small file, exact-shape read.
- Standard read: projection + predicate pushdown over `read_vortex`.
- Cross-engine: file written by DuckDB read by the connector.
- Write round-trip (V3).
