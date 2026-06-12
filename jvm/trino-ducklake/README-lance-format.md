# Lance Data Files & Vector Search — Reference

Companion to the [README's Lance section](README.md#lance-data-files--vector-search-experimental).
Everything here is shipped and tested; the lance path as a whole is **EXPERIMENTAL** (surface may
evolve). Engine: the DuckDB `lance` extension (Route A — the
[A-vs-B benchmark](dev-docs/REPORT-lance-route-a-vs-b.md) validated this architecture against
direct lance-core JNI: warm-latency parity, comparable cold start).

## Storage model

A lance "file" is a **dataset directory** (manifest + data + index files), not a single file.
Each Trino write (CTAS or INSERT) produces one dataset directory, recorded as one
`ducklake_data_file` row with `file_format='lance'` whose `path` is the directory. Reads hand
that directory to `__lance_scan('<dir>')` through the DuckDB execution engine — the same
FileScan machinery the vortex format uses.

## Reads

- `SELECT` over lance tables dispatches per data file through the DuckDB engine
  (in-process by default, or the Quack sidecar with `ducklake.execution-engine=quack`).
- **Predicate pushdown:** TupleDomain predicates render into the `WHERE` of the
  `__lance_scan(...)` query and are evaluated by DuckDB/lance.
- **Type round-trips:** scalars, `ARRAY` (lance materialises uniform float lists as
  `FixedSizeList` — the embedding shape), `ROW`/`MAP` and nested combinations read through the
  shared Arrow→Page converter. Positional virtual columns and delete filtering reuse the
  standard machinery.

## Writes

| Capability | Status |
|---|---|
| CTAS / INSERT with `data_file_format='lance'` | Yes — scalar columns + `ARRAY` of scalar (embeddings: `ARRAY(REAL)` round-trips through `lance_vector_search`) |
| NULL array *rows* | Yes (NULL array *elements* rejected) |
| `ROW` columns | **Rejected at schema time.** The lance COPY of an Arrow-streamed source silently morphs NULL ROW values into ROW-of-NULLs (probed 2026-06-11; a VALUES-sourced lance COPY preserves struct nulls, so this is an upstream arrow-scan interplay). Datasets with structs written *outside* Trino register and read fine via `add_files`. |
| `MAP` columns | Rejected by lance itself ("Map ... only supported in Lance file format 2.2+") |
| DELETE / UPDATE / MERGE | Standard DuckLake parquet positional delete files apply to the table; the *search functions* reject tables with row-level deletes (v1 gate below) |

Writes go local-temp-then-upload: DuckDB `COPY ... (FORMAT lance)` produces the dataset
directory in local tmp, then the writer walks and uploads the tree to the table location.
Array columns get value/null-count stats only (no min/max).

## Registering external datasets — `add_files(file_format => 'lance')`

```sql
CALL ducklake.system.add_files(
    schema_name => 'vectors',
    table_name  => 'docs',
    files       => ARRAY['s3://bucket/datasets/docs.lance'],
    file_format => 'lance')
```

Registers a lance dataset **directory** as one data-file row: no parquet footer probing,
`record_count` sourced by scanning the dataset (doubles as a readability check), best-effort
directory size, no column stats, read-by-name. Guards reject hive-partitioning and partitioned
tables for lance. This is the intended route for embedding datasets produced out-of-band
(and currently the only route for lance datasets containing `ROW` columns).

## Vector / full-text / hybrid search table functions

Three table functions under `<catalog>.system.*`. All take `schema_name`/`table_name` (the
DuckLake table whose lance data files to search), default `k => 10`, and
`prefilter => false`. They return the table's columns **plus** score column(s):

| Function | Extra arguments | Appended columns | Ordering |
|---|---|---|---|
| `lance_vector_search` | `column_name` (the embedding column), `query_vec ARRAY(DOUBLE)`, `k`, `prefilter` | `_distance REAL` | ascending distance |
| `lance_fts` | `column_name` (the text column), `query VARCHAR`, `k`, `prefilter` | `_score REAL` | descending BM25 score |
| `lance_hybrid_search` | `vector_column`, `query_vec`, `text_column`, `query`, `k`, `alpha DOUBLE` (optional), `prefilter` | `_distance`, `_score` (NULL when no text match), `_hybrid_score` | descending hybrid score |

```sql
SELECT id, title, _distance
FROM TABLE(ducklake.system.lance_vector_search(
        schema_name => 'vectors',
        table_name  => 'docs',
        column_name => 'emb',
        query_vec   => ARRAY[0.12, 0.85, ...],
        k           => 10))
ORDER BY _distance
LIMIT 10;
```

**Exact top-k across fragments.** Each dataset directory (one per CTAS/INSERT) is searched
independently with local top-k, so the raw function output is a *superset* of the global top-k.
Wrap with `ORDER BY _distance LIMIT k` (or `_score DESC` / `_hybrid_score DESC`) for the exact
global answer — when you do, `applyTopN` also trims the per-fragment `k` to your limit. FTS
`k` is additionally best-effort inside lance itself ("around k"); the same `ORDER BY ... LIMIT`
recipe makes it exact.

**Pushdown.** The searches execute as ordinary table scans (`applyTableFunction` rewrite), so
`WHERE` over the function output, `ORDER BY <score> LIMIT n`, and column projection all push
down. The `prefilter` flag selects lance's filter-then-search semantics:

- `prefilter => false` (default): search first, then filter — a `WHERE` can return fewer than
  `k` rows.
- `prefilter => true`: the predicate is pushed *into* lance, and `k` survivors come back (for
  FTS this also changes the BM25 corpus statistics). Only single-range, non-null conjuncts
  (`=`, `<`, `BETWEEN`, ...) are pushable into the function; `OR`-of-ranges and `IN`-lists
  degrade to post-filtering above the scan.

**FTS needs no index** — brute-force matching works on unindexed text columns.

**v1 analyze-time gates:** every data file of the table must be lance-format; tables with
row-level deletes are rejected; s3-resident datasets are accepted only on the Quack engine
(see below).

## S3 datasets — the `AWS_*` env credential channel

The lance extension's Rust `object_store` does **not** read DuckDB httpfs secrets (probed —
this applies to `read_vortex` too). Credentials for `s3://` lance datasets come from
process-global `AWS_*` environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_REGION`, `AWS_ENDPOINT`/`AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`):

- **Quack engine** (`ducklake.execution-engine=quack`): set the `AWS_*` env on the sidecar
  container at launch. The dev compose stack derives it from the catalog's `s3.*` settings
  (`DuckDbS3Config.toObjectStoreEnv()`).
- **In-process engine:** the channel is the Trino JVM's own environment — operator-set, one
  s3 identity per JVM. The search functions *reject* s3 datasets in-process (unverifiable);
  plain reads attempt the scan and fail with an object_store error if the env is absent.

Local-filesystem datasets need none of this.

## Upstream version pinning — the canary

The lance extension ships ~daily and has renamed functions before. There is no repo-side
INSTALL pin (extensions.duckdb.org serves only the latest build per DuckDB version), so
`TestLanceExtensionCanary` force-installs the currently served build, verifies every call
shape this connector renders plus a live write/scan/search round-trip, and fails when the
served build drifts from the verified pin — see the class doc for the bump workflow.

## More detail

- [dev-docs/TODO-lance.md](dev-docs/TODO-lance.md) — feature tracker, probe findings, Route A/B decision record
- [dev-docs/REPORT-lance-route-a-vs-b.md](dev-docs/REPORT-lance-route-a-vs-b.md) — the benchmark behind "Route A is primary"
- [dev-docs/HANDOFF-lance-route-a.md](dev-docs/HANDOFF-lance-route-a.md) — the full build log (closed)
