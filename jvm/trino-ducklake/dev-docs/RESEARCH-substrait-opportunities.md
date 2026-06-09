# RESEARCH: Substrait as a unified predicate-pushdown IR

**Status:** Exploratory. No code. Surfaced 2026-06-08 while evaluating the vendored
`lance-trino` connector for the Lance file-format work.
**One-line:** The Lance connector translates Trino's filter to **Substrait** (a standard,
engine-neutral expression IR) rather than to anything Lance-specific — which suggests a
single Trino→Substrait pushdown front-end could serve *all* our non-parquet backends
(DuckDB, Lance, DataFusion, Vortex), in place of the per-engine SQL-string translators we
maintain today.

---

## 1. The finding

`vendor/lance-trino/plugin/trino-lance/src/main/java/io/trino/plugin/lance/SubstraitExpressionBuilder.java`
(~1,273 LOC, Apache 2.0) converts Trino predicates to a Substrait `ExtendedExpression`
protobuf, which Lance's scanner consumes via `ScanOptions.Builder.substraitFilter(ByteBuffer)`.

It is wired into `LanceMetadata.applyFilter` (`LanceMetadata.java:574`) and powers **only the
Filter node** of the plan. The connector's other pushdown hooks — `applyProjection` (:461),
`applyLimit` (:497), `applyAggregation` (:511), `applyDelete` (:1052) — do not go through
Substrait (projection/limit ride `ScanOptions.columns`/`limit`).

Crucially, the converter imports `io.substrait.*` only — **zero Lance dependency** in the
translation itself. Lance is just the first consumer of the bytes.

## 2. What it covers (the Filter surface)

`applyFilter` feeds the builder **both** halves of Trino's predicate surface:

| Source | Shapes handled | Builder entry |
|---|---|---|
| `constraint.getSummary()` — `TupleDomain<ColumnHandle>` | equality, ranges (`< <= > >=`), `IN`-lists, `IS NULL` | `build(tupleDomain, …)` (~:105/:126) |
| `constraint.getExpression()` — `ConnectorExpression` tree | `LIKE`, `AND`, `OR`, `NOT`, `IS NULL`/`IS NOT NULL`, the six comparisons (`equal/not_equal/gt/gte/lt/lte:any_any`), `IN` (Substrait `SingleOrList`) | `extractPushableExpressions(expr, assignments, fieldIds)` (~:734) |

Substrait function refs used: `is_null:any`, `is_not_null:any`, `equal:any_any`,
`gt/gte/lt/lte:any_any`, plus `SingleOrList` for `IN`, recursive `AND/OR/NOT`, and `LIKE`.

Partial pushdown is **lossless**: whatever it can't translate is returned as
`remainingExpression`, which Trino re-applies above the scan. Type coverage in the filter:
BOOLEAN, all ints, REAL/DOUBLE, VARCHAR, DATE, TIMESTAMP, TIMESTAMP_TZ, VARBINARY, plus
ARRAY/ROW field refs.

What it does **not** do: arithmetic/function-of-column expressions beyond the above,
aggregates, projection-side computed columns, TopN/ordering.

## 3. Why this is interesting for us

Today the connector pushes predicates to DuckDB by rendering **DuckDB SQL strings**
(`DuckDbWhereClauseTranslator`, `DuckDbExpressionTranslator` — see
[TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md)). That is engine-specific and string-shaped.

A Trino→**Substrait** layer is the general version of the same idea, and Substrait is a
documented IR that multiple of our (current and planned) backends already speak:

| Backend | Substrait consumption | Notes |
|---|---|---|
| **Lance** | native — `ScanOptions.substraitFilter(ByteBuffer)` | proven here |
| **DuckDB** | `substrait` / `substrait_json` extension (`from_substrait`) | would let `duckdb`/`vortex`-format reads push filters via IR instead of SQL strings |
| **DataFusion** | native Substrait consumer | the `datafusion-ducklake` sibling we track in `vendor/` |
| **Vortex** | via DuckDB (Route A) today; native TBD | inherits DuckDB-substrait if we go that way |
| **Parquet (Trino native)** | n/a — Trino's own reader | unaffected; this is for the non-parquet engines |

So instead of one predicate translator **per engine**, a single
`(TupleDomain + ConnectorExpression) → Substrait` front-end could feed all of them. That is a
bigger architectural lever than Lance alone.

## 4. Honest trade-offs (Substrait vs. our DuckDB SQL translator)

This is **not** a free swap — the two approaches have different strengths:

- **Breadth of functions.** Our DuckDB pushdown is *deep*: Steps 0–4 shipped ~96 function
  mappings (LIKE, 25 numeric macros, 15 string + regex + encoding macros, the full date/time
  Tier A–C surface — see TODO-pushdown-duckdb.md). The Lance Substrait builder covers a much
  *narrower* core (comparisons, LIKE, boolean connectives, IN, IS NULL). Substrait's strength
  is portability of a **standard core**; the long tail of engine-specific functions still
  needs per-engine handling (or Substrait extension functions the target actually implements).
- **Per-backend semantics.** A Substrait function must mean the same thing on every consumer.
  The hard-won correctness work in the DuckDB program (timezone handling, Unicode case/trim,
  NULL-propagation differences, NaN) doesn't vanish — it moves into "does engine X implement
  this Substrait function with Trino-compatible semantics?" Each backend needs a parity audit,
  same as we did for DuckDB.
- **Maturity / version coupling.** The builder pins a Substrait proto version (≈0.70) and an
  extension catalog; each consumer pins its own Substrait support level. Mismatch = the filter
  silently doesn't push (or errors). Needs a compatibility matrix.
- **It only covers Filter.** Projection, aggregation, limit, TopN still go through each
  engine's own hooks. Substrait *can* express those, but lance-trino doesn't, and wiring them
  is separate work.

Net: Substrait is attractive as a **shared front-end for the common predicate core** across
engines, layered *under* (not replacing) the engine-specific deep-function translators.

## 5. Open questions

- Does DuckDB's `substrait` extension accept the same `ExtendedExpression` shape lance-trino
  emits, with matching extension URIs? (Probe: serialize a Trino filter → `from_substrait` in
  DuckDB → confirm it scans correctly.) Note the `substrait` extension's own platform-build
  availability (we already hit `osx_amd64` gaps with lance/vortex).
- Which Substrait function set does each target actually implement with Trino-compatible
  semantics? (Per-backend parity audit, like the DuckDB one.)
- Could the existing `DuckDbExpressionTranslator` and a Substrait builder share a single
  Trino-expression *walker*, differing only in the emit backend (SQL string vs Substrait
  proto)? That would avoid two predicate-tree traversals to maintain.
- Lossless-remainder bookkeeping: our `applyFilter` already splits enforced/unenforced — does
  a Substrait path slot into that cleanly?

## 6. Suggested evaluation (when picked up)

1. **Spike:** lift `SubstraitExpressionBuilder` (Apache 2.0) into a throwaway test, feed it a
   handful of Trino `TupleDomain`/`ConnectorExpression` filters, and round-trip the bytes
   through DuckDB's `from_substrait` — confirm DuckDB applies the same predicate. This single
   experiment decides whether the "one IR, many engines" idea holds for our DuckDB path.
2. If it holds: prototype a `duckdb_read_mode`-style toggle that pushes the **common core**
   (comparisons/LIKE/IN/IS NULL/boolean) via Substrait while keeping the deep function
   translator for the long tail.
3. Build the per-backend Substrait-function parity matrix before trusting it for pruning.

## 6a. Builder deep-dive (2026-06-08) — reuse verdict: LIGHT-ADAPT (~75% liftable)

Line-by-line review of `SubstraitExpressionBuilder.java` on eight axes. It lifts cleanly in
shape but carries two correctness bugs and one robustness gap to fix on the way out.

| Axis | Finding |
|---|---|
| **Type/literal coverage** | Handles BOOLEAN, all ints, REAL/DOUBLE, VARCHAR, VARBINARY, DATE, TIMESTAMP, TIMESTAMP_TZ. **No DECIMAL / ARRAY / ROW / UUID**, and the literal path (`toLiteral`, ~:470) **throws `UnsupportedOperationException`** on the gap instead of declining to push. The *expression* path is graceful (unmapped → `Optional.empty` → remainder); the *TupleDomain/literal* path is NOT — a `decimal > 5` domain could fail the query. VARCHAR length not carried. |
| **Function mapping** | Hardcoded Substrait anchors (`equal:any_any`, `gt/gte/lt/lte:any_any`, `like:str_str`, `and/or/not:bool`, `is_null:any`) against `DefaultExtensionCatalog` via `SimpleExtension.loadDefaults()` (fixed set, no dynamic registration). Unmapped Trino function → remainder. Small, frozen set. |
| **`remainingExpression`** | **Generic and reusable verbatim.** Pure Trino-expression-tree recursion (`extractExpressionsRecursive`), zero Lance assumptions, rebuilds `AND` from surviving conjuncts. Swap the inner emit for a DuckDB target and it works. |
| **TupleDomain × ConnectorExpression** | **No dedup.** Same predicate can be pushed from both the domain and the expression and AND'd (`(x>5) AND (x>5)`). Idempotent (correct) but redundant — a real perf cost on large `IN`-lists / complex predicates. |
| **NULL semantics** | `IS NULL`/`IS NOT NULL` explicit; null literal → `typedNull`; `IN`/`NOT` rely on Substrait's three-valued logic — **not verified in code** (esp. `NOT IN (…, NULL)`). |
| **LIKE** | 🔴 **ESCAPE silently dropped** — parses the 3-arg `LIKE … ESCAPE` but passes only column+pattern to Substrait; `LIKE 'a\_%' ESCAPE '\'` misbehaves. Only constant patterns push (dynamic correctly filtered out). |
| **Timestamp/TZ** | DATE (days) and TIMESTAMP (micros + precision) clean. 🔴 **TIMESTAMP_TZ** unpacks `millisUtc → micros` and **assumes UTC, passes no zone** — same naive-TZ class that bit the DuckDB date-time pushdown; needs a per-consumer parity check. |
| **Coupling** | Imports are pure `io.trino.spi.*` + `io.substrait.*`; only Lance dep is `LanceColumnHandle` via `.name()`/`.fieldId()`/`.trinoType()`. `final` class, static methods, no runtime state → clean lift. |

**Must-fix when extracting:** (a) make the literal/type path graceful (unsupported → remainder,
not throw); (b) honor `LIKE ESCAPE`; (c) verify/handle `TIMESTAMP_TZ` semantics per target
engine; (d) extend types (DECIMAL at minimum); (e) optionally dedup domain-vs-expression.
**Top correctness risks: LIKE ESCAPE (HIGH), TIMESTAMP_TZ UTC assumption (HIGH).** These are the
same kinds of silent-wrong-results bugs our DuckDB parity work already had to chase — i.e. a
Substrait lift inherits the parity-audit burden, it doesn't skip it.

## 7. References

- Lance builder: `vendor/lance-trino/plugin/trino-lance/src/main/java/io/trino/plugin/lance/SubstraitExpressionBuilder.java`
- Lance applyFilter: `…/LanceMetadata.java:574` (`getSummary()` + `getExpression()` →
  `extractPushableExpressions`)
- Lance scanner hook: `ScanOptions.Builder.substraitFilter(ByteBuffer)` (lance-core 6.0.0)
- Our current pushdown: `DuckDbWhereClauseTranslator`, `DuckDbExpressionTranslator`,
  [TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md)
- Lance evaluation context: [TODO-lance.md](TODO-lance.md),
  [RESEARCH-lance-and-pushdown.md](RESEARCH-lance-and-pushdown.md)
- Substrait: https://substrait.io ; DuckDB substrait extension:
  https://duckdb.org/docs/extensions/substrait
