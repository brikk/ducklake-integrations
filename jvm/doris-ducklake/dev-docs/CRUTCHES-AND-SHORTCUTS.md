# doris-ducklake — crutches, shortcuts & honest caveats

A single audited inventory of everything in this module that is NOT
production-clean: dev/compose-only paths, masquerades, silent-degradation
risks (and how each is actually handled), gated/opt-in features, skipped tests,
and test-only workarounds. Written 2026-07-07 after the inlined-data review.
Keep this current — if you add a shortcut, add it here.

Verdict up front: **one feature is a genuine dev-only crutch (now hard-blocked
by default); the rest are either deliberate documented degradations that fail
LOUD, or compose test-harness config, not connector behavior.**

## 1. Genuine crutch — inlined-data reads (HARD-BLOCKED by default)

- **What:** reading DuckLake inlined rows (`ducklake_inlined_data_*`) works by
  the FE synthesizing a temp Parquet the BE scans by path.
- **Why it's a crutch:** the BE opens the file by path, so it only works when
  FE and BE share warehouse storage as a local filesystem (a compose bind
  mount). A distributed FE/BE cluster cannot use it.
- **How it's contained:** OFF by default — a table with live inlined rows
  throws a loud, actionable error unless the catalog property
  `experimental.inlined.reads=true` is set. So it can't be used by accident.
- **Follow-up (blocking for production):** an object-store temp write + GC, or
  an SPI channel that streams the rows to the BE without shared storage. See
  friction log 2026-07-07 "No way to hand a small FE-built payload to the BE
  without shared storage" and TODO-read.
- **Secondary smell:** even opted-in, the temp file lives on shared warehouse
  storage, so DuckLake's own `GLOB('<data_path>/**')` file-count queries count
  it (3 corpus files skip-listed for this, honestly). No temp-file GC yet.

## 2. Masquerade — `table_format_type = "iceberg"` (deliberate, load-bearing)

- **What:** every scan range reports `table_format_type = "iceberg"` and packs
  the file shape into `iceberg_params`, so the BE dispatches its native Iceberg
  reader. Position deletes ride `iceberg_params.delete_files`.
- **Is it a hack?** No — DuckLake is genuinely Iceberg-shaped (positional
  deletes are iceberg-spec-compatible; field_id == column_id), so the iceberg
  reader reads DuckLake files correctly. This is the whole plugin's read/write
  mechanism, not a shortcut, and it's how paimon/hudi ride shared BE readers
  too. Not silent-wrong; validated by the corpus + live smoke.
- **Upstream ask (nice-to-have, not a fix):** `DuckLakeScanRange.TABLE_FORMAT_TYPE`
  has a `TODO(option-B)` to flip to `"ducklake"` once the BE adds a 5-line
  dispatch synonym. The thrift bytes are identical; purely cosmetic.

## 3. Deliberate type degradations (fail loud OR documented-lossy, never silent-wrong)

All in `DuckLakeTypeMapping`, mirrored from the trino plugin's choices, pinned
by `DuckLakeTypeMappingTest` + `DuckLakeTemporalTypeAuditTest`:

- **`timestamptz` → naive `DATETIMEV2(6)`** — BE-gated: the 4.1.0 BE can't read
  a UTC-micros parquet column into a `TimeStampTz` slot. Values are correct
  UTC; only zone-aware typing is lost. Restore `TIMESTAMPTZ` when the BE
  supports it (friction 2026-07-07).
- **`timestamp_ns` → `DATETIMEV2(6)`** — Doris caps datetime scale at 6; nanos
  clamp to micros (trino does the same).
- **`time`/`timetz` → STRING, `json`/`variant`/`interval` → STRING,
  `uuid` → VARBINARY, `uint64` → DECIMALV3(20,0), `uint128` → STRING,
  `int128` → DECIMALV3(38,0), geometry family → VARBINARY** — no first-class
  Doris type; data round-trips, type-specific ops unavailable.
- **Write-side (`DuckLakeCreateTableMapper`, `DuckLakeCreatePartitionMapper`)
  THROW on unmappable types / on `DISTRIBUTED BY` (CRC32 ≠ murmur3)** rather
  than silently corrupt — the correct-by-loud posture.

These are the honest kind: a user sees a degraded type or a clear error, never
a wrong value.

## 4. Loud correctness gates (fail rather than silently over/under-return)

In `DuckLakeScanPlanProvider`, all throwing `DorisConnectorException`:

- **Inlined DELETEs** (`failOnInlinedDeletes`) — can't apply catalog-side
  positional tombstones; would over-return. Stage 2.
- **Time travel over a compacted (partial) file** (`failOnUnfilterablePartialFile`)
  — `partial_max > snapshot` needs a per-row hidden-column filter the BE can't
  apply; would over-return. Latest-snapshot reads unaffected.
- **COUNT(\*) pushdown** refuses on any filter / delete / inlined / non-latest
  / partial file (serves the `-1` sentinel → BE counts by reading).
- **BE delete-file nullability** — a known BE gap (OPTIONAL-vs-REQUIRED
  position-delete columns); classified as an engine-skip, never wrong rows.

## 5. Known upstream-blocked gaps (documented, not worked around)

- **BE position-delete nullability** (merge-on-read delete reads) — BE fix.
- **BE timestamptz→TimeStampTz** conversion — BE fix (see §3).
- **Per-file column mapping** (add_files DROP+re-ADD name collision over id-less
  files) — the scan-node-level schema dictionary can't express per-file maps;
  needs per-range schema info or a BE hook. 2 corpus files skipped.
- **Column DEFAULT values** (ADD COLUMN … DEFAULT n backfill) — DuckLake stores
  it; our read path returns NULL. Needs the iceberg default-column seam;
  possibly BE-gated.
All in the friction log with pickable upstream fixes.

## 6. Test-harness shortcuts (test-only; no product impact)

- **`DuckLakeInlinedParquetWriterTest` assume-skips** the round-trip cases when
  the JDK-25 test toolchain hits the parquet-format shaded-thrift ABI break.
  The FE runtime is JDK 17 (unaffected); the writer is validated live by the
  `data_inlining` corpus. The value-conversion logic isn't otherwise unit-
  covered here — a real (if bounded) coverage gap.
- **Corpus `assumeTrue` gates** — skip the mirror when there's no live FE/PG or
  the duckdb extensions aren't installable. Correct (the mirror needs a
  cluster); plain `:doris-ducklake:test` never depends on them.
- **One `@Disabled` test** — the Step-7.5 inline-delete fixture in
  `DuckLakeScanPlanProviderTest` (needs a direct `ducklake_inlined_delete_*`
  PG seed). Tracks the Stage-2 inlined-delete work.
- **Corpus skip-list** — every entry has a documented reason; categories:
  dialect (DuckDB-only SQL), oracle-local (non-lake reference tables),
  harness-unmirrorable (fault injection), and the real gaps above. After the
  inlined-data review, the earlier false "harness variance" GLOB skips were
  corrected to the true cause (our temp file on shared storage).

## 7. Compose / cluster config (not connector behavior)

- **`enable_local_shuffle_planner=false` shim** in `smoke.sh` — the P-series FE
  plans `LOCAL_EXCHANGE_NODE` (thrift 38) the stock 4.1.0 BE rejects. Pure
  FE/BE version skew in the dev cluster; drop when a matching BE image exists.
- **BE platform auto-detect** (arm64 default ↔ amd64 host) — dev ergonomics.
- **FE `SPI_READY_TYPES` + engine-padding patches** (`fe-patches/`) — the two
  documented, reapplyable FE guards the plugin needs until upstream generalizes
  them; tracked as upstream asks. Not a hack, a pending-upstream dependency.
