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

## 4. What we'd do if we add Lance

- **`data_file_format = 'lance'`** as a third value alongside `parquet` / `duckdb`. Writer uses DuckDB's `lance` extension (`COPY ... TO 'file.lance' (FORMAT lance)`).
- **Catalog records** the format in `ducklake_data_file` as it already does. No spec changes needed — the spec just calls it a `file_format` string.
- **Regular SELECT** path: same shape as our existing duckdb-format read — open DuckDB (in-process or via Quack), `INSTALL lance; LOAD lance; ATTACH/SELECT FROM lance_scan('path')`. Predicate pushdown still works via the existing `DuckDbWhereClauseTranslator`.
- **Vector / FTS / hybrid** path: add three `ConnectorTableFunction`s. Each one resolves the table's Lance files from the catalog, generates splits carrying the function args, the page source executes the corresponding `lance_*` function on the sidecar.
- **Function namespace**: `my_catalog.system.lance_vector_search(...)`, parallel to our existing `my_catalog.system.add_files(...)`. Same registration mechanism.
- **Cross-engine concern**: `'lance'` isn't a recognized `data_file_format` value in the DuckLake spec yet. Same caveat as `'duckdb'` already has — opt-in per table, only our connector + DuckDB-with-Lance will read it. Worth proposing upstream alongside our `'duckdb'` value.

The most novel work would be the table-function infrastructure — we don't have any custom table functions today beyond `add_files` (which is a procedure, slightly different shape). When scoping this seriously the first concrete deliverable is the `ConnectorTableFunction` skeleton for `lance_vector_search`.

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
