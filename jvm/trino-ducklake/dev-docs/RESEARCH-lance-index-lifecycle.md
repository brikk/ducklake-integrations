# RESEARCH — Lance index lifecycle (F3): what the extension actually exposes

**Status:** decision-needed. Probed the installed `lance` DuckDB extension live (2026-06-14,
osx_arm64) to scope F3 ("index lifecycle — lance first"). Headline: **index *creation* is not
available through the extension**, so F3's core thesis ("the point of these formats is that we
CAN have indexes") cannot be delivered through Trino with this extension version. What *is*
callable is index *optimization* of an already-created index, plus dataset maintenance
(compaction, version cleanup).

This doc records the probed reality so nobody re-discovers it, states what is/isn't buildable,
and frames the open design questions for the F3 "index DEFINITION on the table" part — which is
Jayson's call.

## Probed function surface (`duckdb_functions() WHERE function_name LIKE '%lance%'`)

All are **table functions** (invoked `SELECT * FROM fn(...)`), positional VARCHAR args (the
catalog shows them as `col0, col1, col2` — named-arg syntax does NOT bind). This matters: our
`DucklakeDuckDbExecutor` is SELECT/Arrow-stream only, and these return a result table, so they
run through the **existing** executor with no new non-SELECT statement path needed.

| Function | Signature (probed) | Works against a plain dataset dir? | Returns |
|---|---|---|---|
| `__lance_scan` | `(VARCHAR path, BOOLEAN explain_verbose)` | yes (this is our read path) | the data |
| `__lance_compact_files` | `(VARCHAR path, VARCHAR optionsJson)` | **yes** — `('<dir>', '')` or `('<dir>', '{}')` | `(Operation, Target, MetricsJSON)` |
| `__lance_cleanup_old_versions` | `(VARCHAR path, VARCHAR optionsJson)` | **yes** | `(Operation, Target, MetricsJSON)` |
| `__lance_optimize_index` | `(VARCHAR path, VARCHAR indexName, VARCHAR optionsJson)` | only if the index already exists — `''` indexName errors `index_name must be non-empty` | `(Operation, Target, MetricsJSON)` |
| `__lance_set_auto_cleanup` | `(VARCHAR, BIGINT, VARCHAR, BIGINT, BOOLEAN)` | yes | status |
| `__lance_show_auto_cleanup` | `(VARCHAR path)` | yes — reports `enabled`, `interval`, ... | `(Key, Value)` |
| `__lance_truncate_table` | `(VARCHAR, VARCHAR, VARCHAR)` | (namespace API; not probed deeply) | status |
| `__lance_namespace_scan` | `(VARCHAR, VARCHAR, VARCHAR, api_key, bearer_token, token, BOOLEAN)` | (lance-namespace catalog API) | rows |
| `__lance_exec` | `(VARCHAR, BLOB)` | takes a serialized op BLOB — not constructible from Trino without the lance client | — |
| `lance_vector_search` / `lance_fts` / `lance_hybrid_search` | (already wired — see README-lance-format.md) | yes | search hits |

**There is no `__lance_create_index` / `CREATE INDEX` surface.** `__lance_exec(path, BLOB)`
could in principle carry a create-index op, but the BLOB is a serialized lance operation that
requires the lance client library to construct — impractical to build in the connector.

## What this means for F3

1. **Index creation through Trino: BLOCKED (upstream).** A lance dataset must arrive already
   indexed (built by Python/Rust lance tooling) and registered via `add_files`, exactly as the
   driving list's "search is brute-force unless a dataset arrives pre-indexed via add_files"
   already describes. Wrapping `__lance_optimize_index` only helps datasets that *already* have
   an index — so a Trino-only "create + maintain an index" story is not deliverable today.

2. **Dataset maintenance IS callable** — `__lance_compact_files` and
   `__lance_cleanup_old_versions` work against the dataset directory and return a metrics table.
   These belong to **F6 (maintenance operations)** more than to "index lifecycle." They are
   wrappable as `CALL system.<proc>(schema, table)` that resolves the table's `file_format =
   'lance'` data file paths (catalog.getDataFiles + DucklakePathResolver) and runs the function
   through the existing executor.

   **Open safety question before shipping these:** they MUTATE the dataset directory in place
   (compaction rewrites internal files; cleanup deletes old lance versions). DuckLake registers
   a lance dataset as one immutable logical unit (path + record_count + file_format). Compaction
   keeps the logical content and path, so catalog rows stay valid — but if more than one DuckLake
   snapshot references the same dataset dir, an in-place mutation affects them all, and there is
   no DuckLake snapshot bump to record it. This needs a deliberate decision (gate to
   single-reference datasets? require a fresh snapshot? document as best-effort?) — it is the
   same transactionality question F6 raises generally.

3. **duckdb-format (.db) ART indexes** (the other half of F3's "designed for every non-parquet
   format"): DuckDB's ART indexes are *within-file* and created by `CREATE INDEX` on an attached
   database. DuckLake's model is many small immutable data files per table, so a per-file ART
   index does not fit — there is no single mutable table to index. So the .db side does not give
   a natural index surface either, without a different model (e.g. a dedicated index sidecar).

## F3 part 2 — "index DEFINITION on the table" (Jayson's design call)

The generic design ("this table always wants that index for new data", planned so duckdb and
future formats plug into the same surface) is unaffected by the part-1 blocker in its *catalog
& DDL shape*, but its *execution* depends on a creation primitive that lance doesn't expose
today. The open questions Jayson named, with what the probe constrains:

- **Catalog representation.** A `ducklake_index` table (index_id, table_id, begin/end_snapshot,
  column_id, index_type, params) is the natural shape — but it's a DuckLake **spec** change
  (cross-engine visible; DuckDB's metadata reader would need to tolerate the new table, the way
  it tolerates unknown table-scoped settings — see [[project-catalog-ddl-constraints]]). Worth
  confirming upstream appetite before authoring.
- **DDL/procedure surface.** `ALTER TABLE ... SET INDEX (col) USING <type>` (definition) vs
  `CALL system.optimize_index(...)` (maintenance). Definition is recordable now; *applying* it
  (actually building the index) is the blocked step for lance.
- **When indexing runs.** Synchronous-on-write needs a creation primitive (blocked for lance);
  maintenance-op is what's callable (`optimize_index` on an existing index, `compact_files`).

## Recommendation

- **Do not build index-lifecycle procedures speculatively now.** The creation primitive is
  missing, so a Trino-only index story is not deliverable, and the maintenance wrappers carry an
  unresolved in-place-mutation safety question that belongs to the F6 design.
- **Decide direction (Jayson):** (a) treat lance indexing as external-only (Python builds the
  index, `add_files` registers it; Trino just searches — the status quo, documented); (b) wait
  for / request a lance extension that exposes index creation, then revisit; (c) scope the
  `ducklake_index` table-definition spec change independently of the (blocked) lance execution,
  so the *definition* lands and *application* follows when a primitive exists; (d) build the
  `compact_files` / `cleanup_old_versions` maintenance procedures as part of **F6**, once the
  in-place-mutation/snapshot semantics are decided.
- The `TestLanceExtensionCanary` ([[project-lance-scan-function-name]]) is the trip-wire: if a
  future extension bump adds a creation function, re-probe `duckdb_functions()` and reopen F3.
