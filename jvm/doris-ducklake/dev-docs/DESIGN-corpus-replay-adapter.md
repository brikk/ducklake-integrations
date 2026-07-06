# DESIGN: Doris adapter for the DuckLake corpus replay runner

> Created 2026-07-05. Status: **LIVE** (2026-07-06) — `DorisReplayEngine` +
> `DorisCorpusReplayTest` mirror the corpus against a compose FE+BE via
> `./gradlew :doris-ducklake:corpusReplayTest` (starter dirs; add
> `-Dducklake.corpus.dirs=all` for the full corpus). Starter-dirs result:
> **477 passed, 0 failed** (skips = documented gaps). Backend axis landed in
> runner commit `8c0ed16` (`OracleAttachment` + `ReplayEngineSkip` +
> `metadataRewriter`); corpus = duckdb/ducklake pinned at `c23aca43`
> (v1.5-variegata ↔ DuckDB 1.5.4).
>
> **First-contact findings (2026-07-06):**
> 1. **REAL BUG: silent wrong rows on inlined state.** DuckLake inlines small
>    INSERTs/DELETEs into `ducklake_inlined_*` metadata rows by default on PG
>    backends; our read path served file rows only → empty/stale results with
>    no error. Fixed: `DuckLakeScanPlanProvider.failOnLiveInlinedState` throws
>    a documented-gap `DorisConnectorException` at plan time ("never silently
>    wrong"). Serving inlined rows is now a TODO-read item.
> 2. FE **catalog-cache staleness** vs an external writer: the adapter issues
>    `REFRESH CATALOG` before every mirrored query (the documented Doris
>    freshness contract for externally-written catalogs).
> 3. DuckDB inline time travel (`AT (VERSION => …)`) reaches the FE parser as
>    an empty-detail error — denied in `DorisCorpusDialect` (`\bAT\s*\(`).
> 4. `general/metadata_cache.test` file-skipped: the known BE
>    parquet-nullability delete gap (friction 2026-05-19).

## The contract (what we implement)

`dev.brikk.ducklake.corpus.ReplayReadEngine` — engine-side adapter, invoked
only to **mirror read-only queries against catalog state the embedded DuckDB
oracle already built**. We never execute DuckDB-dialect writes, never see
test-env/loops/directives, never parse or match sqllogictest golden text
(oracle-only identity control). Comparison is live-vs-live rows, canonicalized
(`null`→NULL, `""`→`(empty)`), order-insensitive in v1.

```kotlin
interface ReplayReadEngine : AutoCloseable {
    val name: String
    fun connect(catalogUri: String, dataPath: String)  // once per corpus file
    fun accepts(sql: String): Boolean = true            // dialect gate → skip, never fail
    fun executeQuery(sql: String): List<List<String?>>  // rows of cells; null = SQL NULL
}
```

Skip levels: per-file curated skip-list (`CorpusRunner(skipList)`, reasons
printed in the report) + per-record `accepts()` (engine-skip, never failure).
Report yields `passed/failed/skipped` per engine — our bring-up metric.

## Doris adapter shape (`DorisReplayReadEngine`, lives in this module)

Transport: **mysql-protocol JDBC against a live compose FE+BE** — inherently
heavier than Trino's in-JVM runner, but `connect` is per-file and queries
batch within it.

- `connect(catalogUri, dataPath)`:
  `DROP CATALOG IF EXISTS corpus; CREATE CATALOG corpus PROPERTIES(...)` on
  the FE with `metadata.url = <catalogUri>` (requires the Postgres form —
  this is exactly the backend-axis gate; today's oracle attaches a
  duckdb-local `.db` metadata file our FE cannot read) and
  `storage.warehouse`/s3 props derived from `dataPath`. DROP/CREATE per file
  is cheap and gives clean cache state (equivalent to `REFRESH CATALOG`,
  stronger). The FE+BE cluster itself is brought up ONCE per run, not per
  file.
- `accepts(sql)` v1: bare scalar `SELECT`s only — reject non-SELECT, DuckDB
  extension syntax/functions, and types our read surface degrades
  (TIME/TIMETZ/INTERVAL → STRING, JSON/UUID renderings). Grow incrementally;
  the green-count climb is the read-path progress metric.
- `executeQuery(sql)`: plain JDBC; render each cell per the normalization
  below, `null` stays Kotlin `null`.

## The real work: value normalization (Doris JDBC → DuckDB canonical)

Bounded, pure, testable in isolation (unit tests need no cluster):

| Logical value | Doris mysql-protocol rendering | DuckDB canonical | Action |
|---|---|---|---|
| BOOLEAN | `1` / `0` (TINYINT-ish) | `true` / `false` | map via ResultSetMetaData type |
| DATETIMEV2(6) | trailing zeros vary by scale | micro-trimmed (`GoldenComparator` rules) | trim to DuckDB's trimming |
| DECIMALV3 | scale-padded (`1.50`) | trailing-zero behavior differs | normalize via BigDecimal |
| DATE/DATEV2 | `yyyy-MM-dd` | same | pass-through |
| FLOAT/DOUBLE | Java rendering | DuckDB rendering (esp. `inf`/`nan`) | map specials, format check |
| VARBINARY/blob | driver-dependent | `\xHH` escapes | encode to DuckDB form |
| NULL | JDBC null | cell = Kotlin null | pass-through |

Known risk (flagged for co-review, does NOT block freezing the seam):
decimals + timestamps are where string canonicalization is most likely to go
brittle across mysql-protocol driver versions. **Position: string
canonicalization is right for v1; keep `GoldenComparator` internally evolvable
to typed comparison** (parse both sides to typed values when both parse) —
adapter interface unchanged either way, so freeze `ReplayReadEngine` after
Trino contact as planned.

## Prep checklist (do-able before the backend axis lands)

- [x] **Corpus-mode compose profile** — `compose/smoke.sh --up-only`
  (2026-07-05, validated live): substrate + plugin install + FE/BE health +
  `enable_local_shuffle_planner` shim, exits at the driver boundary; FE on
  `127.0.0.1:9030` (root, no password). Our stack already matches the
  backend-axis shape — Postgres (`trino-ducklake-postgres`) + MinIO
  (`trino-ducklake-minio`) on a shared network — so the only new requirement
  is that the RUNNER's oracle attaches to that same Postgres + writes the
  same MinIO bucket.
- [x] **`DorisValueNormalizer`** + unit tests —
  `src/dev/brikk/ducklake/doris/corpus/DorisValueNormalizer.kt` (2026-07-05):
  targets the oracle's `renderCell` forms (Java `toString` numerics incl.
  `Infinity`/`NaN`, micro-trimmed timestamps, UTC `+00`, `\xHH` blobs,
  `toPlainString` decimals, TINYINT(1)→boolean via metadata flag).
- [x] **`accepts()` v1 predicate** + unit tests —
  `src/dev/brikk/ducklake/doris/corpus/DorisCorpusDialect.kt` (2026-07-05):
  SELECT-only (comment-tolerant), deny-tiers for DuckDB syntax (`::`, `[`,
  `{`, lambdas), catalog/file table-functions, degraded type families
  (INTERVAL/UNNEST/sampling), word-boundary precise. WITH-CTEs rejected in
  v1 (Doris supports them — first widening candidate).
- [x] **Module wiring** (2026-07-06): `testImplementation(project(":ducklake-corpus-replay"))`
  + mysql-connector-j; test code compiles at JVM 25 (main stays 17 — the FE
  ABI) via classpath-attribute overrides in build.gradle.kts. Adapter =
  `test/src/.../corpus/DorisReplayEngine.kt`; harness =
  `DorisCorpusReplayTest` driven by the dedicated `corpusReplayTest` task
  (pins `java.io.tmpdir` into the compose bind mount; excluded from plain
  `test`).
- [x] **Skip-list seed** (2026-07-06): metadata_parameters (props not
  threaded), ducklake_settings (backend-type assert), metadata_cache (known
  BE nullability gap).
- [ ] **Full-corpus green**: run `-Dducklake.corpus.dirs=all`, triage the
  remaining failure classes into fixes vs documented skips (in progress).

## What this replaces

Hand-porting read-parity tests from trino-ducklake (paused per
[`TODO-read.md`](./TODO-read.md)). The five audit files already ported stay —
they pin FE-metadata semantics (type surfaces, snapshot pinning, identifiers)
the corpus can't see, since the corpus exercises full SQL through a live BE.
