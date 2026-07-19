# DuckBridge P2 — parity pushdown port notes

## DuckDbWhereClauseTranslator — DEFERRED (not ported)

The DuckLake connector's `DuckDbWhereClauseTranslator` renders a Trino `TupleDomain`
into a DuckDB `WHERE` clause (IN-lists, ranges, per-type literals). In this base-jdbc
connector that job is already done by base-jdbc's own `QueryBuilder`: `applyFilter`
splits the constraint into a `TupleDomain` (handled by the framework's domain
compaction + `QueryBuilder.toConjuncts`) and a `ConnectorExpression` (handled by our
`convertPredicate`/rewriter). Porting `DuckDbWhereClauseTranslator` would duplicate the
framework's domain path and risk divergence.

Verified at runtime: `TestDuckBridgeUnicodeStringRoundTrip.equalityAndInPushdownPreserveNastyStringConstants`
and the arithmetic tests confirm equality / IN / range predicates push correctly through
the base-jdbc domain path with byte-faithful Unicode constants — no custom translator needed.

**P5 caveat:** the `query(...)` PTF and any future custom page-source scan bypass the
JDBC `QueryBuilder`. If P5 adds a scan path that needs to render a `TupleDomain` to
DuckDB SQL itself, port `DuckDbWhereClauseTranslator` then (it's a clean, self-contained
object keyed on the column handle's `columnType` + `columnName`).

## Translator port — what changed vs the DuckLake original

Semantics preserved verbatim (the whole `PUSHABLE_FUNCTIONS` set, the Tier A/B/C type
gates, the `pushdown_timestamp_with_timezone` gate, the concat→`||` NULL rewrite, LIKE
reflection, divide/modulo refusal). Only the seam changed:

- `translateVariable` resolves against base-jdbc `JdbcColumnHandle.getColumnName()` (was
  DuckLake's own handle + a `isRowIdColumn()` check that has no analogue here).
- Session-property lookup goes through the new `DuckBridgeSessionProperties`.

## Integration seam

- `convertPredicate` (overridden on the client) delegates to a
  `ConnectorExpressionRewriter<ParameterizedExpression>` built via
  `JdbcConnectorExpressionRewriterBuilder`, whose single rule
  (`DuckBridgeParityExpressionRule`) runs the whole-expression translator and returns a
  parameter-less `ParameterizedExpression` (the translator inlines only losslessly-
  renderable constants). `DefaultJdbcMetadata.applyFilter` already splits conjuncts and
  calls `convertPredicate` per conjunct → per-conjunct partial pushdown is free.
- `duckbridge.parity.enabled=false` builds the rewriter with NO rules, so function-shape
  pushdown is fully off (domain + LIMIT/TopN still push).

## LIMIT / TopN

Both are pushed onto the JdbcTableHandle but reported NOT guaranteed
(`isLimitGuaranteed=false`, `isTopNGuaranteed=false`) because row order / TopN ties are
non-deterministic without a total order — Trino keeps its own Limit/TopN on top. So the
"provable pushdown" test asserts `limit=`/`sortOrder=` on the TableScan in the EXPLAIN
plan, NOT `isFullyPushedDown()` (which would fail for a non-guaranteed push).

## Parity alias drift

`TestTrinoFunctionAliases.testJavaPushableSetMatchesDuckDbMeta` ran against the freshly
built `trino_parity.duckdb_extension` (linux-amd64, 44 MB, bundled in the plugin jar at
`dev/brikk/duckbridge/trino/plugin/duckdb-extensions/linux-amd64/`). **No drift found** —
`PUSHABLE_FUNCTIONS` equals `trino_meta()` exactly (92 entries).

## Notes for P3 (Quack / remote server-side DuckDB)

The P2 parity init bakes in two T1-embedded assumptions that P3 must generalize:

1. **LOAD is in-process.** `DuckBridgeParity.ensureInitialised` + `TrinoFunctionAliases.loadInProcess`
   issue `LOAD '<local-path>'` over the same JDBC connection the client hands out. For a
   remote DuckDB, the LOAD must be forwarded server-side and the binary must already live
   on the server. The DuckLake original had a `loadServerSide(QuackStatementForwarder, path)`
   helper for exactly this — it was intentionally NOT ported (no Quack here yet). Re-add a
   forwarder abstraction in P3.
2. **The extension path is a Trino-worker-local filesystem path.** `TrinoParityExtensionResolver`
   extracts the bundled binary to the worker's `java.io.tmpdir`. A remote server can't read
   that path. P3 must let `duckbridge.parity-extension-path` mean "a path the server can
   resolve" (or ship the binary to the server out of band) and skip the local extraction.
3. **`getConnection(session)` does SET TimeZone + LOAD + probe per connection.** For a
   pooled remote connection this is fine (idempotent), but the probe cost and the
   fail-loud behavior should be revisited so a transient server hiccup doesn't turn every
   query into a hard `NOT_SUPPORTED` failure — P3 may want a one-time init latch per
   physical connection instead.
