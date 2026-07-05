# DESIGN: Doris adapter for the DuckLake corpus replay runner

> Created 2026-07-05. Status: **prep — blocked on the runner's backend axis**
> (Postgres metadata catalog + shared/MinIO data path), which is the runner
> side's next milestone after Trino-adapter contact. Runner: module
> `jvm/ducklake-corpus-replay`, branch `ducklake-corpus-test` @ `5be8131`
> (full upstream corpus green through the DuckDB oracle: 466 files, 426
> executing, 7,681/7,681 records; corpus = duckdb/ducklake pinned at
> `c23aca43`, v1.5-variegata ↔ DuckDB 1.5.4).

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

- [ ] **Corpus-mode compose profile**: headless FE+BE bring-up (today's
  `compose/smoke.sh` couples cluster bring-up to the smoke driver). Factor a
  `--up-only` mode (cluster + plugin install + `enable_local_shuffle_planner`
  shim, no driver). Our stack already matches the backend-axis shape —
  Postgres (`trino-ducklake-postgres`) + MinIO (`trino-ducklake-minio`) on a
  shared network — so the only new requirement is that the RUNNER's oracle
  attaches to that same Postgres + writes to the same MinIO bucket.
- [ ] **`DorisValueNormalizer`** + unit tests (table above) — no cluster
  needed, can build now against sample renderings captured from the smoke
  cluster.
- [ ] **`accepts()` v1 predicate** + unit tests (SELECT-only lexer-level
  check + deny-list seeded from upstream `test/configs/*.json` where
  applicable).
- [ ] **Module wiring**: the adapter needs `ducklake-corpus-replay` on the
  test classpath (or a small `doris-corpus-adapter` source set) — decide with
  the runner owner whether adapters compile into the engine module (their
  stated plan: "Doris adapter in doris-ducklake") and add the gradle
  dependency once the branch merges.
- [ ] **Skip-list seed** for Doris: start empty at the file level; rely on
  `accepts()` until real runs show file-level pathologies.

## What this replaces

Hand-porting read-parity tests from trino-ducklake (paused per
[`TODO-read.md`](./TODO-read.md)). The five audit files already ported stay —
they pin FE-metadata semantics (type surfaces, snapshot pinning, identifiers)
the corpus can't see, since the corpus exercises full SQL through a live BE.
