# TODO: Trino → DuckDB pushdown

Bring our duckdb-format read path up to the BigQuery / Snowflake connector shape: a curated translator that turns Trino's `ConnectorExpression` predicates into DuckDB SQL, plus DuckDB-namespaced functions exposed through Trino's function SPI for things Trino doesn't have natively.

Background and architecture: see [RESEARCH-lance-and-pushdown.md](RESEARCH-lance-and-pushdown.md).
Function mapping reference: see [RESEARCH-function-mapping.md](RESEARCH-function-mapping.md).

## Discipline (non-negotiable)

- **Lossless pushdown only.** Anything we can't translate with confidence stays in Trino. Partial pushdown is correct, ambitious pushdown that quietly changes semantics is a bug factory.
- **Curated map, not "translate anything that looks similar."** Each translation is an explicit entry: Trino signature → DuckDB fragment → recorded NULL / Unicode / edge semantics.
- **Cross-engine semantic test per entry.** For each mapped function, a test that runs the same input through "Trino does the filter" vs "pushdown does the filter" and asserts identical results. Edge cases (NULL, empty string, multi-byte, leap-day, etc.) where the function spec calls them out.
- **Conservative defaults.** When in doubt, don't push. Adding translations later is cheap; un-pushing a wrong one is expensive.

## Priority order

1. **`LIKE` / `NOT LIKE`** — extremely common, perfectly defined in both engines, near-zero risk. Likely single biggest win for selective queries.
2. **Numeric / comparison expressions in predicates** — `col + 1 > 5`, `col % 10 = 0`, etc. Same shape, mostly trivial. Pin overflow + decimal semantics.
3. **String basics** — `SUBSTRING`, `LENGTH`, `LOWER`, `UPPER`, `TRIM`, `CONCAT`, `POSITION`. Watch the Unicode caveat — pin behavior, test against non-ASCII.
4. **Date / time** — risky because of timezone, locale, calendar. Do one function at a time, with cross-engine semantic tests. Skip anything that touches session timezone interpretation until we've verified the alignment.
5. **DuckDB-namespaced exclusives** — vector / JSON / list / struct / regex variants that exist in DuckDB but not in Trino. Register through `ConnectorFunctionProvider`, route through the same translator. Lower-risk than #4 because we own both ends of the semantic contract.
6. **Lance table functions** when we get there. Sits on top of the infrastructure above.

## Infrastructure to land before any specific mapping

- Extend `DuckDbWhereClauseTranslator` (or build a new sibling) to read `Constraint.getExpression()` — the `ConnectorExpression` tree — not just `TupleDomain`.
- `applyFilter` returns `ConstraintApplicationResult` with `newTableHandle` (carries pushed-down state), `remainingFilter`, and `remainingExpression`.
- A `DucklakeFunctionProvider` (new) for DuckDB-namespaced functions registered via `ConnectorFunctionProvider`. Used for step 5 onward.
- The handle's pushed-down state surfaces in the SQL we send to the executor at read time. Same shape regardless of in-process / Quack / Swanlake executor.

## When pushdown is allowed to fire (mixed-format tables)

DuckLake tables can carry mixed file formats — parquet and duckdb side by side, snapshot by snapshot. Pushdown gates per format.

**Regime 1 — common-SQL functions** (`LIKE`, `UPPER`, `SUBSTRING`, basic math/date — anything in both engines' built-in set).
- Push as a hint AND return as `remainingFilter`.
- `.db` splits: DuckDB pre-filters server-side, fast path.
- `.parquet` splits: Trino re-applies the same predicate above the scan, correct result.
- No homogeneity check needed. Double-evaluation on `.db` splits is trivial — DuckDB already removed most rows.

**Regime 2 — DuckDB-namespaced functions** (`cosine_distance`, `list_*`, `struct_extract`, …).
- Trino has no implementation, so a parquet split has no way to evaluate the function above the scan.
- `applyFilter` checks `ducklake_data_file` at plan time: every file for this scan must have `file_format = 'duckdb'`. If not, reject with a clear error pointing at the offending format mix.
- If homogeneous: push to the executor's SQL AND remove from `remainingExpression` — we own the evaluation end-to-end.

Skip the "route parquet through DuckDB too" alternative. Loses Trino's native parquet reader + Alluxio page-cache + soft-affinity work; undoes the layered design from `CONCEPT-duckdb-as-parquet-file-cache.md`.

## What's out of scope

- Aggregate / window pushdown (`applyAggregation`). Different SPI, different design problem. Tracked separately.
- Join pushdown (`applyJoin`). JDBC connector territory; not relevant for us.
- Trino → backend pushdown for non-DuckDB executors (e.g., direct-to-S3 parquet reads). The parquet path stays as it is.

## Status

- Step 0 (this doc, mapping table) — drafted.
- Steps 1–6 — not started.
