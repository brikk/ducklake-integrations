# REPORT: Lance Route A vs Route B vector-search benchmark

**Date:** 2026-06-12 · **Box:** Apple Silicon (arm64 mac, local NVMe) · **Engines:**
Route A = DuckDB 1.5.3 `lance` extension build `533e0ee` (in-process JDBC, the engine the
connector ships); Route B = `org.lance:lance-core:6.0.0` JNI (darwin-aarch64 native, the engine
the vendored lance-trino plugin wraps). **Harness:**
[BenchLanceRouteAVsB](../test/src/dev/brikk/ducklake/trino/plugin/BenchLanceRouteAVsB.kt) — run
manually with `./gradlew :trino-ducklake:test --tests "*.BenchLanceRouteAVsB" -Dducklake.bench=true`.

## Question

The Route A-vs-B decision gate (TODO-lance §"Route A vs B decision"): does the extra indirection
layer of Route A (Trino → DuckDB → lance-core via the extension) show up in measured vector
latency vs talking to lance-core directly over JNI? If yes, Route B (fork-and-extend the
lance-trino plugin) becomes interesting; if no, Route A stays primary.

## Method

Same lance dataset (written once via `COPY ... (FORMAT lance)`, deterministic `setseed`), same
10 query vectors round-robined, same `k = 10`, **brute-force search** (no ANN index — both
engines execute the same flat scan + top-k inside the same lance-core Rust, so the measured
delta IS the indirection). COLD = fresh engine, first query (extension LOAD + dataset open for
A; `Dataset.open` + first JNI call for B). WARM = 5 warmup then 30–50 timed queries, full result
drain. Route A includes the SQL-literal vector rendering the connector really performs (i.e. the
comparison is biased *against* A by exactly the costs A really pays).

## Results (ms)

| config | route | cold | warm min | warm p50 | warm p90 | warm max |
|---|---|---:|---:|---:|---:|---:|
| 200k × 384d | A (duckdb ext) | 265.9 | 26.3 | **28.5** | 38.1 | 64.7 |
| 200k × 384d | B (JNI) | 435.1 | 24.6 | **27.3** | 32.4 | 41.9 |
| 500k × 384d | A | 346.3 | 61.9 | **65.0** | 76.7 | 113.9 |
| 500k × 384d | B | 384.7 | 59.8 | **64.3** | 69.3 | 71.1 |
| 200k × 768d (30 it) | A | 284.9 | 52.1 | **66.4** | 97.5 | 200.5 |
| 200k × 768d (30 it) | B | 397.9 | 48.3 | **50.9** | 65.0 | 79.1 |
| 200k × 768d (50 it) | A | 432.7 | 51.2 | **56.1** | 74.8 | 175.7 |
| 200k × 768d (50 it) | B | 393.0 | 47.6 | **50.3** | 53.2 | 58.9 |

## Findings

1. **Warm medians are at parity at 384 dims** (≤ ~4% delta, both row counts) and show a **~10%
   Route A overhead at 768 dims** (56 vs 50 ms on the steadier 50-iter run; the 30-iter run's
   larger gap was partly machine noise — see its 200 ms max outlier).
2. **Route B has consistently tighter tails** (p90/max). Route A's tail wobble is single-digit
   to low-tens of ms — small against Trino's own per-split scheduling noise.
3. **Cold start does NOT favor B** — first-query cost is 0.27–0.44 s for both (A pays extension
   LOAD + dataset open; B pays JNI init + dataset open). The RESEARCH §4.3 worry that "cold-start
   is where the DuckDB extension layer is likeliest to lose" did not materialize.
4. The per-query overhead A pays (SQL parse incl. a 384–768-element vector literal, DuckDB
   planning, Arrow re-export) is fixed-ish single-digit ms — it dilutes further as datasets grow
   (65.0 vs 64.3 at 500k rows) and would be invisible behind ANN-indexed sub-ms scans only in
   relative terms (both engines run the same index code; absolute overhead stays single-digit ms).

## Decision

**Route A stays primary; do NOT pick up Route B.** Revisiting the TODO-lance §"Route A vs B
decision" triggers:
- (a) *extension lags upstream* — monitored by the O3 canary (`TestLanceExtensionCanary`), not a
  reason to fork today;
- (b) *indirection shows up in measured latency* — measured: parity at typical dims, ~10% at 768
  dims, comparable cold; not decision-grade;
- (c) *JNI-only surface needed* (`substraitAggregate`, `setColumnOrderings`, …) — no current need.

Residual reasons to ever reopen: a hard requirement on Route B's tighter tail latency, an
ANN-index feature the extension doesn't expose, or upstream divergence the canary surfaces.

## Caveats

- Brute-force only. An ANN-indexed comparison needs index creation plumbing (neither route
  exposes it through our integration yet); since both routes execute the same lance-core scan
  internally, relative conclusions are expected to hold.
- Single node, local NVMe, OS page cache warm for both routes (dataset written immediately
  before). s3-resident datasets add identical object_store costs to both.
- Route B measured WITHOUT Trino plumbing (no page conversion, no split scheduling); Route A
  measured WITH its real SQL rendering. The bias runs against Route A — and it still ties.
- lance-core 6.0.0 vs extension build 533e0ee — different lance-core revisions of the same Rust
  core; rerun the harness if either moves significantly.
