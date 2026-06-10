# HANDOFF: Lance Route A — pick up on an arm64 / lance-capable box

**Why this file exists:** Lance Route A can't be verified on the Intel dev box — the `lance`
DuckDB extension is **404 for `osx_amd64`** (`INSTALL lance` fails). So the read-dispatch wiring
was written here but its tests **skip locally** and must be run on a host where the extension is
published: **`osx_arm64`** (Apple Silicon), `linux_amd64`, `linux_arm64`, or `windows_amd64`.

**Branch:** `worktree-trino-ducklake` (pushed). Pull it, then start at "Step 1" below.

---

## PROGRESS — arm64 run (M1 Max, osx_arm64), 2026-06-09

Picked up on a lance-capable box. **Steps 1–6 done and green; Step 7 + O1 still open.**

- **Step 1 (Phase A0 probe) — DONE, green.** The probe ran (no skip) once one bug was fixed:
  the scan function is **`__lance_scan`** (double-underscore), NOT `lance_scan`. The shipped
  extension errored `Catalog Error: Table Function with name lance_scan does not exist!`. Renamed in
  the read wiring (`DucklakePageSourceProvider.resolveDuckDbReadTarget`), `DuckDbAttachTarget`/
  `DucklakeSessionProperties` comments, and the probe test. `TestDucklakeLanceFileScanRead` now
  `tests=2 skipped=0 failures=0`.
- **Step 2 (dataset-vs-file) — DECIDED: option A.** `__lance_scan('<dataset-dir>')` accepts the
  directory path directly and streams all rows. One catalog row per dataset version, `path` = dir,
  opaque. Recorded in TODO-lance Phase 0 findings.
- **Step 3 (type audit) — DONE + converter gap fixed.** Scalars (INTEGER, DECIMAL, DATE, TIMESTAMP,
  STRUCT, variable `INTEGER[]`) all round-trip. The embedding column `FLOAT[]` comes back as
  `FLOAT[3]` = Arrow `FixedSizeList<float>` → must map to `ARRAY(REAL)`. **`DucklakeArrowToPageConverter`
  was scalar-only** (any nested type threw `NOT_SUPPORTED`). Added `ARRAY` support (fixed + variable
  list of scalar / nested-array elements; ROW/MAP + timestamp/uuid elements still deferred). New test
  `fileScanReadsLanceEmbeddingColumnAsArrayOfReal` verifies the `ARRAY(REAL)` path end-to-end through
  the real executor + converter. NOTE: this converter is **shared** with the duckdb/vortex read paths
  — the change is purely additive (no scalar-path edits), so those are unaffected.
- **Step 4 (pushdown) — DONE, green.** A `TupleDomain` on `id` renders into the `WHERE` of the
  `__lance_scan(...)` query via `DuckDbWhereClauseTranslator`/`DuckDbSelectSqlBuilder` (no ATTACH
  alias — bare WHERE over the scan source) and reaches DuckDB: `__lance_scan` returns only the
  matching row. New test `fileScanPushesPredicateIntoLanceScan`. Pushdown comes for free, as the
  handoff predicted. (lance's `prefilter` semantics still matter for the Step-7 table functions.)
- **Step 5 (SQL-level read via add_files) — DONE, green.** Extended `add_files` with an optional
  `FILE_FORMAT` arg (default `'parquet'`, so existing calls are unchanged). `file_format => 'lance'`
  registers a dataset *directory* as one catalog row: skips the parquet footer, sources `record_count`
  by scanning via `__lance_scan` through the read executor (also a readability check), best-effort
  directory size, no stats, read-by-name (no nameMap). Guards reject hive-partitioning + partitioned
  tables for lance v1. New CTAS-free integration test `TestDucklakeLanceAddFiles` registers an
  externally-written `.lance` dir and SELECTs it (catalog records `file_format='lance'` + `record_count=3`;
  count/order/predicate all correct). **Fixed a latent read-path bug:** `createPageSource` opened a
  `TrinoInputFile` eagerly for every format, which throws on a lance *directory* location (trailing
  slash) — moved that `newInputFile` into the parquet-only branch (the DuckDB-engine branch reads via
  the path string, never a `TrinoInputFile`). `__lance_scan` accepts bare / trailing-slash / `file://`
  path forms (verified), so the catalog's directory URI is fine for the scan. Targeted regression batch
  (parquet/vortex/duckdb reads + parquet add_files) stays green.
- **Environment caveat (NOT lance) — RESOLVED with graceful skip.** 3 `TestDucklakeDuckDbExecutorBackends`
  parity tests used to hard-fail on Apple Silicon: the Quack testcontainer's duckdb is **amd64** while
  the host bundles the **arm64** `trino_parity.duckdb_extension` (`os.arch`-based selection in the
  test's `@BeforeAll`), so the container rejects the arm64 extension at `LOAD`. Added a shared
  `assumeQuackParityExtensionLoadable()` guard (mirrors the vortex sibling's existing skip) so the 3
  now **skip cleanly** off-platform instead of failing — full `:trino-ducklake:test` is green on arm64,
  and full parity coverage still runs on a matching-arch host / CI. The deeper fix (make the bundled
  extension match the *container* platform, not the JVM host arch — option 1 in the task note) remains
  a follow-up. Pre-existing, orthogonal to lance.
- **Quality gates:** detekt gate (`:trino-ducklake:detekt`) is green for all the above changes —
  validators de-duped via `SUPPORTED_DATA_FILE_FORMATS`, the array-element dispatch split into
  `appendArrayElement`/`appendScalarElement`, `finishAndBuildFragment` trimmed via
  `releaseEngineResources()`, and `countLanceRows` catches `SQLException`/`IOException` specifically.
  Two pre-existing `DucklakeColumnStatsAccumulator` complexity findings (from commit `fafd3b0`, not
  these changes) were added to `detekt-baseline.xml` to restore a green gate.
- **Step 6 (writer A4) — DONE, green.** `DucklakePageSink.openNewWriter` gets a `FORMAT_LANCE` branch
  reusing the Arrow-stream writer with **local-temp-then-upload** (the decided approach). In
  `DuckDbArrowStreamFileWriter`, `isVortex` generalized to `usesCopy = isVortex || isLance`: both
  `COPY … (FORMAT <fmt>)` to local temp with inline `DucklakeColumnStatsAccumulator` (no ATTACH).
  Lance-specific: the COPY target is a *directory*, so size = sum of the tree, upload walks the dir
  and uploads each file under the remote dataset location (`uploadDirectoryToRemote`), cleanup deletes
  recursively, and abort uses `deleteDirectory`. Flipped both `validateDataFileFormat` validators
  (session + table props) to accept `'lance'`. New round-trip test `TestDucklakeLanceFormat` (CTAS +
  INSERT → two dataset dirs → SELECT/predicate). **Scalar columns only** — the Arrow-stream writer's
  `toArrowType` is scalar-only, so embedding/ARRAY *writes* fail fast; register embedding datasets via
  `add_files(file_format => 'lance')` (Step 5) instead. Writer regression batch (vortex CTAS, duckdb
  arrow-stream writer, rollover) stays green.

**Still open:** Step 7 (table functions A3), the O1 fix (lance s3 cred channel), and a follow-up
(ARRAY/embedding *write* support). Ordering + scope below.

---

## NEXT — ordering & scope (decided 2026-06-10; handoff happens in a fresh session)

Three independent workstreams remain. **They have no hard dependency on each other**; recommended
order is by value + risk:

### 1. Step 7 — vector / FTS / hybrid table functions (Phase A3) — DO FIRST
The big novel-SPI chunk. **Scope the first session to `lance_vector_search` end-to-end ONLY**, then
mirror the proven shape for `lance_fts` + `lance_hybrid_search` in a follow-up. Plan:
- **Spike the hard part first: the `query_vec` argument.** Trino `ConnectorTableFunction` args are
  scalar / descriptor / table only — there is no native array-argument kind. Figure out how a caller
  passes a `FLOAT[]`/`DOUBLE[]` query vector (scalar `array`-typed arg? a VARCHAR the function parses?
  a `DESCRIPTOR`?) BEFORE building the rest. This is the main unknown; everything else mirrors the
  `add_files` procedure + the existing FileScan split→page-source flow. We have **zero** table
  functions today (`add_files` is a *procedure* — different SPI shape), so this is greenfield.
- Wire: register the `ConnectorTableFunction` in `DucklakeModule` (new `Multibinder` for
  `ConnectorTableFunction`), resolve lance files from the catalog, emit splits carrying
  `(path, column, query_vec, k, prefilter)`, and have the page source run
  `lance_vector_search(dir, column, query_vec, k := …, prefilter := …)` (confirmed present;
  two overloads — `FLOAT[]` and `DOUBLE[]` query vecs; see the duckdb_functions() dump in
  [[project_lance_scan_function_name]]). Stretch: `applyTopN` so `ORDER BY <distance> LIMIT k`
  synthesizes `k`.
- **Target LOCAL-path lance datasets only.** s3 vector search would hit the same missing cred
  channel as O1 — so treat "table functions over s3" as gated behind the O1 fix, not part of this
  session. Verifiable end-to-end on arm64 (PostgreSQL catalog testcontainer + in-process executor).
- Register the fixture via the Step-5 `add_files(file_format => 'lance')` path (or the Step-6 writer),
  then call `SELECT * FROM TABLE(<catalog>.system.lance_vector_search(...))`.

### 2. O1 fix — lance s3 credential channel — INDEPENDENT, schedule after/with Step 7
Confirmed (see §O1): lance ignores the DuckDB secret and reads object_store's `AWS_*` **process-global**
env. The connector's `DuckDbS3Config → CREATE SECRET` is a no-op for lance s3. Fix is a design choice,
NOT more SPI:
- **Favored: the Quack sidecar path** — set `AWS_*` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  `AWS_ENDPOINT`/`AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`, `AWS_REGION`) in the container env at launch
  from catalog `s3.*` config. Clean because it's a separate process (no global-env collision with
  Trino's parquet s3 client). Mirror the `s3.* → AWS_*` aliasing the Doris BE already does
  ([[project_doris_be_aws_keys]]).
- In-process executor: setting `AWS_*` per-query is unsafe (global). Only viable as a once-at-startup
  global for a single s3 identity — document the limitation, or restrict in-process lance-s3.
- Keep it OUT of the Step-7 PR — it touches execution-engine/sidecar config, not the table-function SPI.

### 3. Follow-up — ARRAY/embedding WRITE support (lower priority)
The Arrow-stream writer is scalar-only (`toArrowType`/`populateVector`), so embedding columns can't be
written via CTAS — only registered via `add_files`. Add `ARRAY`(`FixedSizeList`) support to the writer
+ stats to enable embedding CTAS. Not blocking; embeddings are usually produced out-of-band and
registered.

**Design context (read first):**
- [TODO-lance.md](TODO-lance.md) — the chunked plan (Phases A0–A4) + the Route A vs B decision.
- [RESEARCH-lance-and-pushdown.md](RESEARCH-lance-and-pushdown.md) — the deep design rationale.
- [TODO-vortex.md](TODO-vortex.md) — the sibling format; Lance reuses its FileScan machinery
  verbatim. Read the vortex commits to see the exact shape Lance is mirroring.

---

## What's already done on this branch (read-dispatch wiring, Phase A1 partial)

Lance reads dispatch through the **same DuckDB FileScan path vortex uses** — the executor was
already generalized for `DuckDbAttachTarget.FileScan`, so Lance needed only format plumbing + one
dataset-aware quirk. Commits land on `worktree-trino-ducklake`:

1. **`FORMAT_LANCE = "lance"` constant** — `DucklakeSessionProperties.kt`. Declared but **NOT**
   added to the write validators (`validateDataFileFormat` in `DucklakeSessionProperties.kt` and
   `DucklakeTableProperties.kt`). Lance is **read-only** for now, exactly like vortex was at its
   Phase V1 — a `data_file_format='lance'` *write* is still rejected. Reads dispatch on the
   catalog row's `file_format`, which the validators don't gate, so reads work without touching
   them.

2. **Read dispatch** — `DucklakePageSourceProvider.createPageSource` routes `FORMAT_LANCE`
   through `createDuckDbPageSource` (alongside `duckdb` + `vortex`).

3. **Dataset-directory handling** (the one real Lance-specific quirk) —
   `DucklakePageSourceProvider.resolveDuckDbReadTarget` gives lance an **early return that
   bypasses the single-file materialize cache**. Lance is a *directory* (manifest + data + index
   files); `lance_scan('<dir>')` reads the whole dataset. Routing it through
   `resolveDuckDbAttachTarget` would `materialize()` a single file to tmp and hand DuckDB a broken
   path. So lance hands the catalog `dataFileLocation` straight to a
   `FileScan(url, "lance_scan", "lance", s3Config?)`. For `s3://` it passes `DuckDbS3Config`
   (httpfs + secret) — **see open question O1, this may not be how lance reads s3.**

4. **Gated probe test** — `test/.../TestDucklakeLanceFileScanRead.kt`. Writes a `.lance` dataset
   via raw DuckDB, reads it back through the real `InProcessDuckDbExecutor` via
   `FileScan(lance_scan)`, asserts 3 rows. **Skips on osx_amd64** (`assumeTrue` on `INSTALL lance`
   failure). This is the Phase A0 probe — it just needs a capable box to actually execute.

Nothing else is wired: no pushdown verification, no table functions, no writer. The full suite is
green here (lance test skips, everything else runs).

---

## Test environment setup (do this once on the new box)

The lance **probe test itself** (`TestDucklakeLanceFileScanRead`) needs only network (to
`INSTALL lance`) — no Docker, no parity extension. But the **full suite** and any **Quack-container**
work need the two things below. (This recipe was Intel-Mac specific in the original dev memory;
generalized here for arm64/linux. Substitute your host platform where noted.)

**1. Docker runtime for Testcontainers.** Need a Docker-compatible daemon. With **podman** (the
setup on the dev boxes):
```sh
export PATH="/opt/podman/bin:$PATH"          # podman CLI location on the Mac boxes; skip on linux if podman/docker is already on PATH
export DOCKER_HOST=unix:///var/run/docker.sock
export TESTCONTAINERS_RYUK_DISABLED=true      # Ryuk reaper is flaky on podman
```
With Docker Desktop / native docker, none of the above is needed. First suite run builds the
`brikk-ducklake-quack-server` image from
`ducklake-catalog/testFixtures/resources/docker/quack-server/Dockerfile` (~few min, then cached).

**2. trino_parity DuckDB extension** (needed by the `.db` and Quack tests, NOT by the lance probe).
Fetch the prebuilt CI artifact instead of building it (`gh` must be authed):
```sh
cd duckdb-trino-parity-extension && ./scripts/fetch-from-ci-artifacts.sh
```
**Gotcha:** the fetch writes the *host* binary to `build/<host-platform>/release/...` but
`jvm/trino-ducklake/build.gradle.kts` reads the host source from the bare `build/release/...` path.
The `linux-*` paths line up; the host one does NOT. Copy it into place (substitute your host dir —
`darwin-arm64` on Apple Silicon, `darwin-amd64` on Intel Mac, `linux-amd64`/`linux-arm64` on linux):
```sh
cp build/<host-platform>/release/extension/trino_parity/trino_parity.duckdb_extension \
   build/release/extension/trino_parity/trino_parity.duckdb_extension
```
Then re-run gradle; `bundleParityExtension` re-bundles (its inputs changed). To build from source
instead: `cd duckdb-trino-parity-extension && GEN=ninja make` (needs `ninja ccache cmake`); the host
`make` writes to `build/release/` directly, so no copy needed when building locally.

---

## Step 1 — confirm the probe runs (Phase A0)

```sh
cd jvm
# The probe test alone needs no Docker/parity-extension — just network for INSTALL lance:
./gradlew :trino-ducklake:test --tests "dev.brikk.ducklake.trino.plugin.TestDucklakeLanceFileScanRead"
```

**Expected on a capable box:** `tests="1" skipped="0"`, green. Confirm in the XML report
(`trino-ducklake/build/test-results/test/TEST-*LanceFileScanRead.xml`) that it did NOT skip.

If it fails instead of passing, the most likely causes and what they tell you:
- `lance_scan` is the wrong function name / arity → check the lance-duckdb version's actual
  function (`lance_scan` was the documented one; verify against the installed extension).
- `COPY … TO 'x.lance' (FORMAT lance)` syntax drift → adjust the fixture write.
- Arrow type surprise (esp. embedding columns) → see Step 3.

**Also run the Quack-container variant idea:** the vortex equivalent
(`TestDucklakeDuckDbExecutorBackends.quackBackendReadsVortexViaFileScan`) proves FileScan-over-Quack
works. For lance you can't write the fixture on the host (no osx_amd64 ext) — so to test lance
through Quack you must create the dataset **server-side in the container**. That harness doesn't
exist yet; if you want Quack coverage, add a helper that runs `COPY … (FORMAT lance)` via the Quack
RPC before the scan. On a native arm64/linux box the in-process test above is sufficient to
de-risk; Quack coverage is a nice-to-have, not a blocker.

---

## Step 2 — the dataset-vs-file decision (gates all write work)

`COPY … TO 'x.lance' (FORMAT lance)` produces a **directory**, but `ducklake_data_file.path` is
per-file. Two modeling options (TODO-lance §0/§Phase 0):
- **(A) one catalog row per dataset version**, `path` = the dataset directory, treated as opaque.
  This is what the read wiring already assumes (it hands the dir to `lance_scan`). Simplest;
  `$path` returns the dir. **Recommended unless probing surfaces a problem.**
- **(B) one row per Lance fragment file** — closer to the per-file model, much more catalog churn,
  and `lance_scan` wants the dataset dir anyway. Likely wrong for Route A.

**Decide A here.** Gather the evidence the probe gives you: does `lance_scan` accept the directory?
Does it need the manifest path specifically? Record the answer in TODO-lance Phase 0 and proceed
with A for the writer.

---

## Step 3 — type mapping audit (columnar depth)

Read a lance dataset with richer types and confirm the Arrow→Trino mapping through
`DucklakeArrowToPageConverter`:
- **Embedding columns** — `FixedSizeList<float>` should land as `ARRAY(REAL)` (RESEARCH §5). This
  is the one most likely to surprise. Add a fixture with a vector column.
- DECIMAL, DATE/TIMESTAMP/TIMESTAMP_TZ, nested ROW/ARRAY — confirm round-trip.
- Note anything that needs converter work as a follow-up chunk.

---

## Step 4 — predicate pushdown (Phase A2)

Pushdown *should* come for free: `DuckDbWhereClauseTranslator` / `applyFilter` render predicates
into the `lance_scan(...)` query exactly as for the `.db` and vortex paths — there's no ATTACH
alias, but the translator emits a bare `WHERE` over the source, which is what FileScan already
produces. **Verify, don't assume:** run a SELECT with a pushed predicate against a lance file and
confirm (a) correct rows and (b) the predicate actually reaches DuckDB (check the rendered SQL /
row counts). The lance extension's `prefilter` semantics (TODO-lance §A2) matter most for the
table-function phase, not basic column predicates.

---

## Step 5 — full SQL-level read through the catalog

The probe (Step 1) is executor-level. To exercise `createPageSource` dispatch end-to-end you need
a `file_format='lance'` row in a DuckLake catalog. Same gap vortex had at V1. Cheapest enabler:
extend `add_files` to (a) accept a `file_format` arg, (b) skip parquet-footer validation for
non-parquet, (c) source row-count/path from the catalog. Then a CTAS-free integration test can
register an externally-written `.lance` dataset and `SELECT` it. (Vortex closed this gap with its
writer instead — but the lance writer is Phase A4, so `add_files` is the cheaper path to a
SQL-level read test first.)

---

## Step 6 — write (Phase A4, after Step 2)

`DucklakePageSink.openNewWriter` gets a `FORMAT_LANCE` branch using DuckDB
`COPY … TO (FORMAT lance)`. Then **flip the write validators** (the two `validateDataFileFormat`s)
to accept `'lance'`. Key decisions:
- **Direct-to-s3 vs local-temp-then-upload — DECIDED (Jayson, 2026-06-09): local-temp-then-upload,
  mirroring vortex.** Write the lance dir to local temp via `COPY … (FORMAT lance)`, then walk the
  directory and upload every file to the destination filesystem. Keeps portability + the
  inline-stats hook (below); accept the dir-walk/upload friction. (Direct `COPY … TO 's3://…'` was
  the alternative — simpler for lance's directory shape but loses inline stats and diverges from
  vortex.)
- **Stats** — reuse `DucklakeColumnStatsAccumulator` (the same inline single-pass accumulator the
  vortex writer uses), if you go through the Arrow-stream writer. If you `COPY` directly from a
  server-side source you lose the inline-stats hook — decide deliberately.
- Catalog rows per the Step-2 dataset model (one row, dir path).

---

## Step 7 — table functions (after columnar depth — Phase A3)

The novel SPI work. Three `ConnectorTableFunction`s under `<catalog>.system.*`:
`lance_vector_search`, `lance_fts`, `lance_hybrid_search` (parallel to the `add_files` procedure,
but procedures and table functions are different SPI shapes — we have no table functions today).
Each resolves lance files from the catalog, emits splits carrying `(path, column, query_vec, k,
prefilter)`, and the page source runs the matching `lance_*` DuckDB function. See TODO-lance §A3
and RESEARCH §2. Stretch: `applyTopN` so `ORDER BY <distance> LIMIT k` synthesizes `k`.

---

## Open questions to resolve on the capable box

- **O1 — s3 creds for lance — ANSWERED (2026-06-10, arm64 + MinIO): lance does NOT honor the DuckDB
  secret. It uses object_store's own `AWS_*` env-var channel.** Probed against MinIO with the exact
  connector setup (INSTALL/LOAD lance + httpfs + `CREATE OR REPLACE SECRET ducklake_s3 (TYPE S3,
  ENDPOINT 'localhost:9000', KEY_ID/SECRET, URL_STYLE 'path', USE_SSL false)`), then `COPY t TO
  's3://…' (FORMAT lance)` + `__lance_scan('s3://…')`:
  - **With the DuckDB secret only (no AWS_* env): FAILED.** Both write and read ignored the secret and
    hit **`https://s3.eu-west-1.amazonaws.com`** (real AWS, https, wrong region) → 403. lance's Rust
    object_store does its OWN credential/endpoint resolution; the DuckDB httpfs secret is invisible to it.
  - **With object_store env (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`/
    `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP=true`, `AWS_REGION`), same secret present: SUCCEEDED** — write
    and read both worked against MinIO (count=3).
  **Implication:** `DucklakePageSourceProvider.resolveDuckDbReadTarget` passing `DuckDbS3Config` →
  `CREATE SECRET` is a **no-op for lance s3 paths** (works for `.db`/vortex httpfs, not lance). The
  lance path needs a **separate cred channel that feeds object_store's `AWS_*` env**. Hard part: env
  vars are **process-global** and can't be set per-query safely in the in-process executor (would
  collide with Trino's own parquet s3 config and other sessions). Design options for the fix:
  (a) **Quack sidecar path** — set `AWS_*` in the container env at launch; natural fit since it's a
  separate process. (b) In-process — set `AWS_*` once at JVM/plugin startup from catalog config
  (global; only viable for a single s3 identity). (c) Check whether lance-duckdb exposes a storage-
  options channel (a lance-specific secret TYPE, a `SET`, or named params) — `__lance_scan` itself
  takes only `(path, explain_verbose)`, so none via the scan function. Compare with
  [[project_doris_be_aws_keys]] — the codebase already aliases `s3.*` → `AWS_*` for a native s3 client
  (Doris BE); lance is the same shape. **Local-path lance reads/writes are unaffected** (no creds).
- **O2 — `prefilter` pushdown** into the table functions (Phase A3) — needs expression pushdown on
  a table-function input; verify the Trino SPI surface.
- **O3 — extension version pinning.** lance-duckdb ships ~daily; pin a version and watch for
  `lance_scan` / `COPY` API churn (TODO-lance Risks).

## Substrait side-quest (deferred, independent)

Unrelated to running this: [RESEARCH-substrait-opportunities.md](RESEARCH-substrait-opportunities.md)
captures the "one Substrait IR for all engines" idea + the `SubstraitExpressionBuilder` deep-dive
(LIGHT-ADAPT, ~75% liftable, 2 correctness bugs to fix on the way out). The decisive experiment is
a `from_substrait` round-trip through DuckDB — pick that up only after the columnar path is real.
