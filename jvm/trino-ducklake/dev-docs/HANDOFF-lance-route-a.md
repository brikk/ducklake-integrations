# HANDOFF: Lance Route A — pick up on an arm64 / lance-capable box

**Why this file exists:** Lance Route A can't be verified on the Intel dev box — the `lance`
DuckDB extension is **404 for `osx_amd64`** (`INSTALL lance` fails). So the read-dispatch wiring
was written here but its tests **skip locally** and must be run on a host where the extension is
published: **`osx_arm64`** (Apple Silicon), `linux_amd64`, `linux_arm64`, or `windows_amd64`.

**Branch:** `worktree-trino-ducklake` (pushed). Pull it, then start at "Step 1" below.

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

## Step 1 — confirm the probe runs (Phase A0)

Env (mirror the local test env — see memory `jvm-test-env.md`; on linux adjust DOCKER_HOST):

```sh
cd jvm
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
- **Direct-to-s3 vs local-temp-then-upload.** Vortex chose local-temp-then-upload for portability.
  Lance is a *directory*, so local-temp means writing a dir then walking + uploading every file —
  more friction. Direct `COPY … TO 's3://…/x.lance' (FORMAT lance)` is more attractive here (the
  lance-duckdb extension supports direct s3 writes per TODO-lance verification). Weigh portability
  vs the dir-walk-upload cost; this is a real fork, flag it for Jayson.
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

- **O1 — s3 creds for lance.** The read wiring passes `DuckDbS3Config` (DuckDB httpfs secret) for
  `s3://` lance paths. But lance's object_store may want its **own `storageOptions`** map, not a
  DuckDB secret (this is the Route-B two-cred-paths problem leaking into Route A). **Test an
  `s3://` lance read** and confirm whether the DuckDB secret is honored by `lance_scan`. If not,
  the FileScan target needs a lance-specific cred channel. (Local-path reads sidestep this.)
- **O2 — `prefilter` pushdown** into the table functions (Phase A3) — needs expression pushdown on
  a table-function input; verify the Trino SPI surface.
- **O3 — extension version pinning.** lance-duckdb ships ~daily; pin a version and watch for
  `lance_scan` / `COPY` API churn (TODO-lance Risks).

## Substrait side-quest (deferred, independent)

Unrelated to running this: [RESEARCH-substrait-opportunities.md](RESEARCH-substrait-opportunities.md)
captures the "one Substrait IR for all engines" idea + the `SubstraitExpressionBuilder` deep-dive
(LIGHT-ADAPT, ~75% liftable, 2 correctness bugs to fix on the way out). The decisive experiment is
a `from_substrait` round-trip through DuckDB — pick that up only after the columnar path is real.
