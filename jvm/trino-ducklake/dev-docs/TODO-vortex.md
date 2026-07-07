# TODO: Vortex file format support

**Status:** SHIPPED incl. verified predicate pushdown (read + write + `add_files`, incl. partitioned via `hive_partitioning`), via the DuckDB `vortex` extension (V0 probe, V1 read dispatch, V3 write all done). Remaining: audit richer Vortex encodings beyond scalars/ARRAY/ROW, appender-mode + direct-to-s3 write. Predicate-pushdown exploitation VERIFIED 2026-07-07 (filter binds inside the READ_VORTEX operator — see V2). MAP writes gated on the upstream native crash.
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

## Phase V1 — read dispatch (IN PROGRESS, 2026-06-08)
- [x] DuckDB executor file-scan-target generalization (the shared Route-A machinery): new
  `DuckDbAttachTarget.FileScan(path, scanFunction, extension, s3Config?)` sealed variant;
  `InProcessDuckDbExecutor.prepareSource` returns the FROM ref (`read_vortex('path')` for FileScan)
  and INSTALL/LOADs the extension instead of ATTACHing. Commit `d9d9896`.
- [x] `DucklakeSessionProperties`: `FORMAT_VORTEX` constant added. **Write validators left STRICT**
  (vortex is read-only for now — `data_file_format='vortex'` rejected as a write format until V3).
- [x] `DucklakePageSourceProvider.createPageSource`: routes `FORMAT_VORTEX` through the DuckDB
  engine; `resolveDuckDbReadTarget` reuses the materialize-vs-httpfs decision and wraps it as a
  FileScan. Positional virtuals + delete filtering reused unchanged.
  **s3-streaming credential correction (2026-06-11):** `read_vortex` binds through Rust
  object_store and NEVER reads the DuckDB httpfs secret (probed: a secret-only single-threaded
  s3 read fails to the EC2-IMDS fallback; only the vortex COPY *write* honors the secret). The
  streaming FileScan therefore no longer carries `DuckDbS3Config` — credentials are the lance-O1
  `AWS_*` env channel (`DuckDbS3Config.toObjectStoreEnv()` on the Quack sidecar / Trino JVM env
  in-process). Materialized (sub-threshold) s3 reads are unaffected — Trino's own filesystem
  downloads those. Read e2e: `TestDucklakeQuackS3InitRace` scenario 1 reads vortex-over-s3
  through the env-credentialed Quack sidecar.
- [x] DuckDB executor renders `read_vortex('path')`. **Quack FileScan now supported** (commit
  `68fd99b`): server-side `INSTALL/LOAD vortex` + `FROM read_vortex('/data/x.vortex')`, no ATTACH
  alias. Verified end-to-end against the linux quack container by
  `TestDucklakeDuckDbExecutorBackends.quackBackendReadsVortexViaFileScan` (host writes the
  `.vortex` into the bind-mounted shared dir; container scans `/data/scan.vortex`). This is the
  exact FileScan-via-Quack mechanism Lance Route A reuses.
- [x] Executor-level end-to-end test (`TestDucklakeVortexFileScanRead`): write a `.vortex` file,
  read it back via a FileScan target through the real in-process executor; network-gated
  (`assumeTrue`) so offline/unsupported-platform CI skips. Passes on osx_amd64. Commit `ded8c9e`.
- [x] **Full SQL-level read through the catalog.** Satisfied by the V3 CTAS work (the writer
  produces the catalog row a `SELECT` needs — see the V3 note below). And the `add_files`
  route now exists too (2026-06-12): `CALL system.add_files(..., file_format => 'vortex')`
  registers an externally-written `.vortex` file opaquely (row count scanned via
  `read_vortex`, real file size, no stats/name map — `TestDucklakeVortexAddFiles`), the
  exact same shape as lance registration.
- [ ] Type mapping: probe confirmed INTEGER/VARCHAR/DECIMAL round-trip; audit richer Vortex
  encodings through `DucklakeArrowToPageConverter` once the SQL-level read path is testable.
- [x] Quack-engine vortex — server-side INSTALL/LOAD + `FROM read_vortex` without an ATTACH alias;
  verified against the linux quack container (see above).

## Phase V2 — predicate pushdown — ✅ DONE 2026-07-07
- [x] TupleDomain + function-shape pushdown via the existing `DuckDbWhereClauseTranslator` /
  `applyFilter`, same as the `.db` and lance paths — the rendering plumbing turned out to be
  format-blind and was ALREADY firing (`createDuckDbPageSource` never gated by format); what V2
  actually delivered is proof + a canary: (a)
  `TestDucklakeVortexFileScanRead.fileScanPushesPredicateIntoVortexScan` pins that a pushed
  `id=2` reaches DuckDB and filters (lance-A2 transplant); (b) the **exploitation question is
  now MEASURED**: `vortexScanFilterPushdownProbe` EXPLAINs `read_vortex(...) WHERE id=42` and
  the plan shows the filter **bound INSIDE the READ_VORTEX operator** (`Filters: id=42`, no
  separate FILTER node) — the extension binds DuckDB's optimizer filter pushdown, so predicates
  reach the vortex scan itself (pruned decoding), not a post-filter. The probe doubles as a
  canary against a future extension update losing the binding.

## Phase V3 — write (DONE 2026-06-08)
- [x] `DucklakePageSink.openVortexWriter` → `DuckDbArrowStreamFileWriter` parameterized with
  `outputFormat=vortex`: streams Trino pages as an Arrow stream and the consumer thread runs
  `COPY (SELECT * FROM <stream>) TO 'local.vortex' (FORMAT vortex)` — single pass, no scratch
  table, reusing the exact `.db`-path Arrow machinery (one source of truth for the concurrency).
- [x] **Inline stats** via the reusable `DucklakeColumnStatsAccumulator` (commit `fafd3b0`), fed
  from the producer thread as pages flow — no read-back, no MIN/MAX SQL. Works for any
  manually-written format (the user's point: future formats reuse it). VARCHAR min/max uses
  UTF-8 byte order (Slice), NaN excluded, VARBINARY/UUID counts-only — unit-tested.
- [x] **Local-temp-then-upload** via `TrinoFileSystem` (portable across all storage backends;
  matches parquet/duckdb writers). Direct `COPY TO s3://` is a deliberate future opt-in perf
  mode (S3-scoped), NOT the default — see the design discussion.
- [x] Validators (session + table property) accept `'vortex'` as a write format.
- [x] CTAS round-trip integration test (`TestDucklakeVortexFormat`): writes a vortex file,
  catalog records `file_format='vortex'`, SELECT reads it back + predicate. Network-gated.
- This **also closed the V1 "full SQL-level read through the catalog" gap** — the writer
  produces the catalog row a `SELECT` needs, so the read dispatch is now exercised end-to-end.

Follow-up (not blocking): a vortex arrow-stream writer is the only writer mode; the appender
mode isn't wired for vortex (no need — the stream path is the production one). Direct-to-s3
write mode + richer-encoding type audit remain open.

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
