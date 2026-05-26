# RESEARCH: Lance support and Trino pushdown architecture

**Status:** Exploratory. No code yet.
**Triggered by:** DuckDB's Lance extension going GA ([announcement](https://duckdb.org/2026/05/21/test-driving-lance), [docs](https://duckdb.org/docs/current/core_extensions/lance)). The question is how Lance's vector-search / FTS / hybrid-search shape — which isn't expressible as standard SQL `WHERE` — fits into Trino's pushdown surface, and what we'd add to our connector to support `data_file_format = 'lance'` as a third value alongside `parquet` and `duckdb`.

---

## 1. The Trino pushdown surface, by shape of the thing you're pushing

Trino exposes several `ConnectorMetadata` hooks and one declarative SPI. The trick is knowing which one fits which kind of operation.

### 1.1 Standard column predicates — `applyFilter` with TupleDomain

`WHERE col = 5` style. `ConnectorMetadata.applyFilter(handle, constraint)` is called with a `TupleDomain<ColumnHandle>` (ranges, equality, IN, IS NULL). This is what our existing `DuckDbWhereClauseTranslator` consumes. Limited to ranges over column values; no function calls.

### 1.2 Arbitrary expressions in predicates — `applyFilter` with ConnectorExpression

Same `applyFilter`, but you read `constraint.getExpression()` which is a tree of `ConnectorExpression` (`Call` / `Variable` / `Constant`). This is where `WHERE myfunc(s, col) = 4` shows up. You walk the tree, recognize calls into your catalog's function namespace, translate to whatever the backend understands, and return a `ConstraintApplicationResult` with:

- the new `tableHandle` (carries the pushed-down state — e.g., "this scan now has predicate X attached")
- the `remainingFilter` (TupleDomain part Trino must still apply above)
- the `remainingExpression` (sub-tree you couldn't push)

Anything you can't translate, you leave; Trino re-applies it above the scan. Lossless.

### 1.3 ORDER BY + LIMIT — `applyTopN`

`applyTopN(handle, topNCount, sortItems)`. Critical for vector search: the source has the index, only the source can produce top-K efficiently. You return a new handle that carries `(sortColumn, k, direction)`, then the splits / page source honor it.

### 1.4 Projection — `applyProjection`

`applyProjection(handle, projections, assignments)`. Lets you push column pruning into the source; also lets you push computed-column expressions (e.g., `cosine_distance(vec, ?) AS _distance`) into the source so the synthetic column is computed there, not in Trino above the scan.

### 1.5 Table-shaped operations — `ConnectorTableFunction`

The canonical Trino mechanism for things like `lance_vector_search` that aren't predicates but produce table-shaped results. Plugin declares a function with named args + a return schema; planner treats it as a leaf source; the plugin's split manager + page source see the function args attached to the split and produce rows.

### 1.6 Custom scalar functions per catalog — `ConnectorFunctionProvider`

`ConnectorFunctionProvider` / `ConnectorFunctionMetadata`. Lets you register `my_catalog.system.cosine_distance(...)` so Trino's parser knows it exists. Otherwise users have to call it through table-function syntax or it gets analyzer-rejected.

### 1.7 Other pushdown hooks worth knowing about

- `applyAggregation(...)` — partial-aggregation pushdown (COUNT/MIN/MAX on indexes etc.)
- `applyLimit(...)` — LIMIT pushdown without sort
- `applyJoin(...)` — join pushdown (used heavily by JDBC connector, less common elsewhere)

---

## 2. How Lance fits

The Lance SQL the post shows:

```sql
SELECT id, label, _distance
FROM lance_vector_search(
    'path/to/dataset.lance', 'vec',
    [0.1, 0.2, 0.3, 0.4]::FLOAT[4],
    k = 5,
    prefilter = true)
ORDER BY _distance ASC;
```

is **table-function-shaped** in Trino's world, not predicate-shaped. The "predicate" — `ORDER BY _distance ASC LIMIT k` — gets baked into the function arguments, not into a SQL `WHERE`.

The natural Trino expression is:

```sql
SELECT id, label, _distance
FROM TABLE(my_catalog.system.lance_vector_search(
    table => TABLE(my_catalog.schema.t),
    column => 'vec',
    query => CAST(ARRAY[0.1, 0.2, 0.3, 0.4] AS ARRAY(REAL)),
    k => 5,
    prefilter => true))
ORDER BY _distance ASC;
```

Plugin shape:

- `ConnectorTableFunction.analyze(...)` declares the output schema `(id BIGINT, label VARCHAR, _distance DOUBLE)` and captures the args
- `getSplits` for the function returns splits that carry `(file_path, column, query_vec, k, prefilter)` — likely one split per Lance file
- `createPageSource` opens an embedded DuckDB (or our Quack sidecar), loads the lance extension, runs the actual `SELECT … FROM lance_vector_search(...)`, streams Arrow batches back to Trino

For the `ORDER BY _distance ASC LIMIT k` clause that Trino sees ABOVE the table-function call: if you've already passed `k => 5` to the function, the source returns exactly 5 rows per file, and Trino's TopN above merges them into the final top-K. Could also push the outer TopN into the function call via `applyTopN` if you want to be slick.

`lance_fts` and `lance_hybrid_search` are exactly the same pattern, different function args.

---

## 3. How custom predicates push down

The general pattern, illustrated with a hypothetical `WHERE myfunc(s, col) = 4`:

1. **Register** `myfunc` as a connector function via `ConnectorFunctionProvider`. Trino's analyzer now accepts `my_catalog.system.myfunc(s, col)` as a valid call.
2. **User writes** `WHERE my_catalog.system.myfunc(s, col) = 4`.
3. **Trino plans**, calls `applyFilter` on your table handle with a `Constraint` carrying both the (empty) TupleDomain summary AND a `ConnectorExpression` tree: `Call(=, Call(myfunc, [Var(s), Var(col)]), Constant(4))`.
4. **Your `applyFilter`** walks the tree. Recognizes `myfunc` by its catalog-qualified name. Translates to backend SQL: `WHERE backend_myfunc(s_col, col_col) = 4`. Stashes that into the new handle's "pushed-down predicates" list.
5. **Returns** `ConstraintApplicationResult(newHandle, remainingFilter=TupleDomain.all(), remainingExpression=Optional.empty())` — Trino now knows nothing's left to apply above the scan.
6. **When `createPageSource` runs**, the new handle's pushed-down predicate is rendered into the SQL we send to DuckDB.

For functions you can't translate (you don't know the semantics, or backend doesn't support it), you return `remainingExpression` containing the un-translatable piece and Trino re-applies it. Partial pushdown is lossless.

This is exactly how Iceberg / Hive / BigQuery push predicates that aren't pure column ranges — they all read `constraint.getExpression()` in their `applyFilter`. Trino's expression-pushdown SPI was added precisely for this.

---

## 4. Two routes for adding Lance

The DuckLake-spec side is the same either way: `data_file_format = 'lance'` as a third value alongside `parquet` / `duckdb`, recorded in `ducklake_data_file` exactly as today (no spec change needed — `file_format` is an arbitrary string). Same cross-engine caveat as our `'duckdb'` value already has: opt-in per table, only readers that understand `'lance'` will see it. Worth proposing upstream alongside our `'duckdb'` value.

What differs is the **read/write execution engine**.

### 4.1 Route A — through DuckDB's Lance extension

Lean on DuckDB as the single execution engine for non-parquet formats (already the model for `.db` files).

- **Writer:** DuckDB's `lance` extension — `COPY ... TO 'file.lance' (FORMAT lance)`. Same in-process or Quack-sidecar pattern as the `.db` writer.
- **Regular SELECT:** same shape as our existing duckdb-format read — open DuckDB, `INSTALL lance; LOAD lance;` then either `lance_scan('path')` or ATTACH-style read. Predicate pushdown via the existing `DuckDbWhereClauseTranslator`.
- **Vector / FTS / hybrid:** add three `ConnectorTableFunction`s (`lance_vector_search`, `lance_fts`, `lance_hybrid_search`). Each resolves Lance files from the catalog, generates splits carrying function args, the page source runs the corresponding `lance_*` DuckDB function on the sidecar.
- **Function namespace:** `my_catalog.system.lance_vector_search(...)`, parallel to our existing `my_catalog.system.add_files(...)`. Same registration mechanism.
- **Concrete first deliverable:** the `ConnectorTableFunction` skeleton for `lance_vector_search`. We don't have any custom table functions today beyond `add_files` (a procedure — different shape), so this is the novel work.

**Pros:** one execution engine, one S3 cred setup, one set of `--add-opens`, one extension-version matrix to track. Write and read share the toolchain. Quack already exists as our isolation knob if we want a process boundary.

**Cons:** vector hot path goes Trino → DuckDB → lance-core (Rust via DuckDB extension), one more indirection layer than necessary. Bound to DuckDB's release cadence for the Lance extension.

### 4.2 Route B — through `org.lance:lance-core` JNI directly

Newly verified: the Lance Java SDK that the vendored trino-lance plugin already imports exposes the full vector + FTS + index surface; the plugin just doesn't wire it through. Inspected directly via `javap` against `lance-core-6.0.0.jar`.

**`org.lance.ipc.ScanOptions.Builder` surface** (what's there, what the vendored plugin uses, what we'd add):

| Builder method | Purpose | Vendor plugin |
|---|---|---|
| `columns(List<String>)` | column projection | ✓ |
| `batchSize(long)` | batch tuning | ✓ |
| `substraitFilter(ByteBuffer)` | scalar predicate pushdown | ✓ |
| `limit(long)` | row limit | ✓ |
| `withRowAddress(boolean)` | row addresses for MERGE | ✓ |
| `nearest(Query)` | k-NN vector search | — |
| `fullTextQuery(FullTextQuery)` | full-text (Tantivy/BM25) | — |
| `prefilter(boolean)` | scalar-filter-before-ANN (hybrid) | — |
| `useScalarIndex(boolean)` | opt in to btree/bitmap/inverted indexes | — |
| `setColumnOrderings(List<ColumnOrdering>)` | ORDER BY pushdown | — |
| `substraitAggregate(ByteBuffer)` | partial-aggregation pushdown | — |
| `offset(long)` | pagination | — |
| `filter(String)` | string-form predicate (alt to Substrait) | — |
| `batchReadahead(int)` | scan tuning | — |
| `withRowId(boolean)` | row IDs | — |

`org.lance.ipc.Query` (vector query for `.nearest()`): `column` (String), `key` (float[]), `k` (int), `minimumNprobes`/`maximumNprobes`, `ef`, `refineFactor`, `distanceType`, `useIndex`, `queryParallelism`. Full HNSW/IVF tuning surface.

`org.lance.ipc.FullTextQuery`: static factories `match`, `phrase`, `multiMatch`, `boost`, `booleanQuery` — Tantivy-style FTS with BM25.

**What we'd build.** The vendored plugin's infrastructure already gives us: dataset/session caching (`LanceRuntime`), columnar Arrow→Page conversion (`LanceArrowToPageScanner`), Substrait predicate pushdown (`SubstraitExpressionBuilder`), INSERT via `org.lance.WriteFragmentBuilder` (`LancePageSink`), MERGE/UPDATE/DELETE, and time travel. License is Apache 2.0 — we can fork-and-extend or vendor-then-overlay cleanly.

Shape of the fork/extension:

- Extend `ScannerFactory.open(...)` (currently takes columns, storageOptions, substraitFilter, limit, userIdentity, datasetVersion) with `Optional<Query> nearest`, `Optional<FullTextQuery> fts`, `boolean prefilter`. `FragmentScannerFactory` plumbs these onto `ScanOptions.Builder` directly. Backward-compatible if added as optional.
- Add three `ConnectorTableFunction`s (`lance_vector_search`, `lance_fts`, `lance_hybrid_search`) that build the appropriate `Query` / `FullTextQuery` and pass it via a new field on the table handle through split → page source → scanner factory.
- Optional: `applyTopN` pushdown so a Trino-side `ORDER BY <distance_col> LIMIT k` synthesizes `Query(k)` without requiring users to call the table function explicitly. Stretch goal.

**Pros:** native Lance path, no DuckDB indirection. Full surface (`prefilter`, `useScalarIndex`, `substraitAggregate`, `setColumnOrderings`) is sitting there to push down. We already vendor the plugin — extending it locally is a smaller delta than green-field.

**Cons:** two execution engines (DuckDB for `.db` and parquet-side; lance-core JNI for `.lance`). Two storage-cred plumbing paths (lance-core takes `Map<String, String> storageOptions`, separate from our `DuckDbS3Config`). Maintenance burden if upstream vendor plugin evolves and we've forked. JNI imposes the same `--add-opens` requirement we already document for Arrow C-data — no new JVM-flag cost.

### 4.3 Picking between A and B

Current lean: **A** (single execution engine matches the `.db` story; matches the conversation that triggered this research). Route B becomes attractive if (a) DuckDB's Lance extension lags upstream Lance features we want, (b) the extra indirection layer shows up in measured vector-search latency, or (c) we end up wanting the `prefilter` / `substraitAggregate` / `setColumnOrderings` surface that the JNI exposes cleanly and the DuckDB extension may not.

Worth a small benchmark when we pick this up: same dataset, same query vector, same k — Route A (Trino → DuckDB Lance extension) vs Route B (Trino → lance-core JNI) on cold and warm scans. Cold-start cost is where the DuckDB extension layer is likeliest to lose; warm scans probably wash out.

---

## 5. Open questions for when we pick this up

- **Lance file lifecycle on writes** — `COPY TO 'file.lance' (FORMAT lance)` produces a Lance *dataset* (a directory of files), not a single file. Our `ducklake_data_file.path` model is per-file; we'd either record one row per Lance manifest version, or treat the dataset directory as opaque from the catalog's POV. Needs a closer look at DuckDB's Lance writer output shape.
- **Predicate pushdown into `lance_vector_search`'s `prefilter`** — the `prefilter` arg accepts a SQL predicate string. To route `WHERE category = 'x'` from Trino into the prefilter, we'd need expression-pushdown on a table-function input. Trino's table-function SPI supports table arguments with filters; need to verify the surface.
- **`applyTopN` interaction with the table function's own `k`** — if the user writes `... ORDER BY _distance LIMIT 100` and we already baked `k=5` into the function call, the outer LIMIT 100 is no-op-saturated. Conversely if user omits `k` and just writes `ORDER BY _distance LIMIT 100`, can we synthesize `k=100` into the function call via TopN pushdown? Worth designing in.
- **Embedding column types** — Trino has `ARRAY(REAL)` but no first-class `VECTOR(N)` type. Lance distinguishes vectors with a fixed dimensionality. Either we represent as `ARRAY(REAL)` and rely on schema validation, or wait for a Trino-side vector type.
- **Performance shape vs `.db`-format vectors** — DuckDB itself can store vectors in a `.db` file with HNSW indexes via the `vss` extension. Lance is an alternative on-disk format with its own index machinery. Probably worth a head-to-head benchmark before committing to Lance as a first-class format vs treating it as an optional engine for vector-heavy workloads.

---

## 6. References

- Trino SPI: `io.trino.spi.connector.ConnectorMetadata` (`applyFilter`, `applyTopN`, `applyProjection`, etc.)
- Trino SPI: `io.trino.spi.function.table.ConnectorTableFunction`
- Trino SPI: `io.trino.spi.connector.ConnectorFunctionProvider` / `FunctionMetadata`
- Trino SPI: `io.trino.spi.expression.ConnectorExpression`, `Constraint`, `ConstraintApplicationResult`
- DuckDB Lance extension: https://duckdb.org/docs/current/core_extensions/lance
- DuckDB Lance blog: https://duckdb.org/2026/05/21/test-driving-lance
- DuckLake spec: `data_file_format` field (currently `'parquet'` per spec; `'duckdb'` is our extension)
- Lance Java SDK Maven coordinates (Apache 2.0, in our local maven repo as of 2026-05-26):
  - `org.lance:lance-core:6.0.0` — Dataset / Fragment / ScanOptions / LanceScanner / Query / FullTextQuery / VectorIndexParams; JNI binding over the Lance Rust core. Surface in §4.2 verified via `javap` on this jar.
  - `org.lance:lance-namespace-apache-client:0.7.6` and `org.lance:lance-namespace-core:0.7.6` — Lance namespace catalog API (table discovery, declare/describe/list) and HTTP client.
- Vendored Trino-Lance plugin: `vendor/lance-trino/` (Apache 2.0; columnar reader only — uses 5 of 16 `ScanOptions.Builder` methods, no vector / FTS / index pushdown wired through).
