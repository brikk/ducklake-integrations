# Lance & Vortex Scan + Search — DuckBridge

Path-based table functions that scan lance datasets / vortex files and run lance vector / full-text /
hybrid search, executed by the DuckDB `lance` / `vortex` extensions. **EXPERIMENTAL** — surface may
evolve with the upstream extensions.

Unlike the DuckLake connector these took over from, DuckBridge has no catalog table→file mapping:
**the PTFs take the dataset PATH directly.** There is no DuckLake-format storage model, no
schema-evolution, no snapshot/delete gating here — you name a path, the connector scans it.

## Enabling

Off by default (a plain DuckDB/parity deployment pays nothing). Enable per extension on the catalog:

```properties
duckbridge.lance.enabled=true
duckbridge.vortex.enabled=true
```

When enabled, the connector `INSTALL`s (floating latest) + `LOAD`s the extension on every DuckDB
connection so the PTFs' synthetic scan queries resolve. If the extension can't be installed/loaded,
PTF use **fails loud** with install instructions — never a silent empty result.

## Extension availability — probed, never managed

The `lance` / `vortex` extensions come from DuckDB's extension repositories (floating latest). A hard
version pin isn't possible (extensions.duckdb.org serves only the latest build per DuckDB version +
platform). `TestLanceVortexExtensionCanary` force-installs the served build and verifies every call
shape this connector renders (`__lance_scan`, `lance_vector_search` + `k`/`prefilter`, `read_vortex`)
plus a live write/scan/search round-trip, failing loud when upstream drifts.

On a **remote (Quack) server** the extension is a server-side concern: the connector issues
INSTALL/LOAD over the pass-through connection, but the server must have repo access (or the extension
pre-installed). There is no bundled binary for these (unlike `trino_parity`).

## Scan table functions

```sql
SELECT id, txt FROM TABLE(duckbridge.system.lance_scan(path => '/data/docs.lance')) ORDER BY id;
SELECT * FROM TABLE(duckbridge.system.vortex_scan(path => '/data/events.vortex'));
```

| Function | Arguments | Returns |
|---|---|---|
| `lance_scan` | `path` (dataset directory) | the dataset's columns as-is (`__lance_scan`) |
| `vortex_scan` | `path` (`.vortex` file) | the file's columns as-is (`read_vortex`) |

`ARRAY` columns (lance embeddings `FLOAT[n]`, `VARCHAR[]` tags, ...) read through a dedicated array
column mapping; scalars, arrays and unicode content round-trip.

## Search table functions (lance)

Three functions under `duckbridge.system.*`, all path-based with `k => 10`, `prefilter => false`
defaults. They return the dataset's columns **plus** score column(s):

| Function | Extra arguments | Appended columns | Ordering |
|---|---|---|---|
| `lance_vector_search` | `column` (embedding), `query_vector ARRAY(DOUBLE)`, `k`, `prefilter` | `_distance REAL` | ascending distance |
| `lance_fts` | `column` (text), `query VARCHAR`, `k`, `prefilter` | `_score REAL` | descending BM25 score |
| `lance_hybrid_search` | `vector_column`, `query_vector`, `text_column`, `query`, `k`, `alpha DOUBLE` (optional), `prefilter` | `_distance`, `_score` (NULL when no text match), `_hybrid_score` | descending hybrid score |

```sql
SELECT id, title, _distance
FROM TABLE(duckbridge.system.lance_vector_search(
        path         => '/data/docs.lance',
        column       => 'emb',
        query_vector => ARRAY[0.12, 0.85, 0.03],
        k            => 10))
ORDER BY _distance
LIMIT 10;
```

**Multiple fragments.** A lance dataset may hold multiple fragments; the DuckDB function searches
them and returns up to `k` (best-effort for FTS — "around k"). Wrap with
`ORDER BY _distance LIMIT k` (or `_score DESC` / `_hybrid_score DESC`) for the exact global answer.

**FTS needs no index** — brute-force matching works on unindexed text columns.

## Filtering & pushdown — be truthful

These PTFs run through base-jdbc's `query`-PTF pattern: the function builds the DuckDB scan/search
SQL, resolves its output columns via `getTableHandle`, and the result flows through the standard scan
path. Consequences:

- **A `WHERE` / `ORDER BY ... LIMIT` over the PTF is applied by Trino ABOVE the function**, not pushed
  into the generated `__lance_scan` / `lance_*` SQL. (The DuckLake `LanceSearchTableHandle` +
  `applyFilter`/`applyTopN` machinery that pushed domains into the scan and trimmed per-fragment `k`
  needs a custom `ConnectorMetadata`, which the base-jdbc `JdbcPlugin` frame doesn't expose for PTFs.
  `DuckDbWhereClauseTranslator` was therefore NOT ported — see `dev-docs/P5-NOTES.md`.)
- Results are correct either way — Trino re-applies the predicate/limit above the scan.
- The `prefilter` flag is forwarded to the DuckDB function (which changes lance's filter-then-search
  semantics + FTS corpus stats), but the connector does not push any Trino `WHERE` *into* it.

## Not ported (dropped)

- **Writes** (`COPY ... FORMAT lance/vortex`, CTAS/INSERT into lance/vortex, `add_files`
  registration). DuckBridge exposes read/search only. Write a lance dataset / vortex file with DuckDB
  (or any lance/vortex writer) out of band, then scan/search it by path.
- **DuckLake-format storage model, schema-evolution, snapshot/time-travel, delete-file gating** — all
  DuckLake-catalog concerns with no analogue in a plain path-based scan.
- **S3 `AWS_*` credential channel** for `s3://` datasets — the ported `DuckDbS3Config` renders the
  DuckDB S3 secret, but the lance/vortex scan PTFs are exercised on local paths only in this phase; an
  `s3://` path is passed through to DuckDB as-is (the extension's object_store reads process-global
  `AWS_*` env, which the operator must set — see `dev-docs/P3-NOTES.md` "S3 credential story").
