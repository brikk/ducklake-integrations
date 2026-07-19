# DuckBridge P5 — lance/vortex scan + search PTFs

## What landed

Path-based table functions under `duckbridge.system.*`, gated by `duckbridge.lance.enabled` /
`duckbridge.vortex.enabled` (both default off):

| PTF | Signature (named args) | DuckDB call |
|---|---|---|
| `lance_scan` | `path` | `__lance_scan('<dir>')` |
| `vortex_scan` | `path` | `read_vortex('<file>')` |
| `lance_vector_search` | `path`, `column`, `query_vector ARRAY(DOUBLE)`, `k`=10, `prefilter`=false | `lance_vector_search('<dir>','<col>',[...]::DOUBLE[], k:=, prefilter:=)` |
| `lance_fts` | `path`, `column`, `query`, `k`=10, `prefilter`=false | `lance_fts('<dir>','<col>','<q>', k:=, prefilter:=)` |
| `lance_hybrid_search` | `path`, `vector_column`, `query_vector`, `text_column`, `query`, `k`=10, `alpha DOUBLE`=NULL, `prefilter`=false | `lance_hybrid_search(...)` |

Vector search appends `_distance REAL` (asc); FTS `_score REAL` (desc); hybrid `_distance`, `_score`,
`_hybrid_score` (desc).

## The self-contained seam (key architectural decision)

The DuckLake connector runs these through a custom `FunctionProvider` + `TableFunctionProcessorProvider`
+ `LanceSearchSplit` + a `LanceSearchTableHandle` scan-shaped handle so `applyFilter`/`applyTopN` push
domains into the scan SQL. **None of that is available in the base-jdbc `JdbcPlugin` frame** —
`JdbcConnector` doesn't expose `getFunctionProvider` or a custom split manager, and base-jdbc's PTFs
(the `query` PTF) don't use the processor-split path at all.

So DuckBridge mirrors base-jdbc's own `query` PTF pattern instead: the PTF's `analyze()` builds the
DuckDB scan/search SQL, wraps it in a `PreparedQuery`, calls `JdbcMetadata.getTableHandle` to resolve
the output columns, and returns a `Query.QueryFunctionHandle(jdbcTableHandle)`. base-jdbc's existing
`applyTableFunction` → scan → split → page-source path executes it. This reuses ALL existing machinery
(including the T2 Arrow engine) and keeps the module self-contained for the transplant.

### Where the extension gets loaded

`getTableHandle`/`getColumns` call `ConnectionFactory.openConnection` **directly**, bypassing
`DuckBridgeClient.getConnection`. So extension loading moved into a `ConnectionFactory` decorator
(`DuckBridgeExtensionConnectionFactory`) that loads parity + lance/vortex on EVERY connection —
otherwise the PTF metadata probe sees `__lance_scan` as an unknown function. Session-specific
`SET TimeZone` stays in `getConnection` (metadata probes don't need it).

### ARRAY column support

Lance datasets have embedding columns (`FLOAT[3]`) and tag columns (`VARCHAR[]`). base-jdbc's default
`toColumnMapping` rejects `Types.ARRAY`, which failed metadata resolution for `lance_scan`. Added
`DuckBridgeArrayColumnMapping` — a read-only ARRAY column mapping (parses the DuckDB `<elem>[n]` type
name, reads `java.sql.Array`, builds a Trino array Block; pushdown disabled, writes unsupported). The
T2 Arrow converter already handled arrays; this is the JDBC record-set + metadata counterpart.

## Domain pushdown into scan SQL — NOT landed (truthful)

`DuckDbWhereClauseTranslator` was **NOT ported.** A `WHERE`/`ORDER BY ... LIMIT` over these PTFs is
applied by Trino **above** the function, not pushed into the `__lance_scan`/`lance_*` SQL. Pushing it
would need the DuckLake `LanceSearchTableHandle` + `applyFilter`/`applyTopN` custom-`ConnectorMetadata`
machinery, which the JdbcPlugin frame doesn't expose for PTFs. Results are correct (Trino re-applies);
`prefilter` is forwarded to DuckDB but the connector pushes no Trino predicate into it. Tests assert
this (`lanceScanDomainFilterAppliedAbovePtf`, `vortexScanProjectionAndFilter`) — correctness over the
filter is verified; the plan-shape push is explicitly not claimed.

## Dropped (per the G3 sign-off)

- **Writes**: `COPY ... FORMAT lance/vortex`, CTAS/INSERT into lance/vortex, and `add_files`
  registration. DuckBridge is read/search-only for these formats. Fixtures write datasets with DuckDB
  directly, then the PTFs scan/search by path.
- The DuckLake catalog resolution + v1-scope gates (all-lance-format, no-deletes, s3-only-on-Quack)
  in `AbstractLanceSearchTableFunction.resolveSearchTable` — there is no catalog table to resolve; the
  user names a path. DuckDB validates the column types.
- `LanceSearchTableHandle` / `LanceSearchSplit` / `LanceSearchProcessor` / `LanceSearchHandle` /
  `LanceVectorSearchFunctionHandle` / `DucklakeFunctionProvider` — replaced by the `Query.QueryFunctionHandle`
  reuse above. The Jackson-serialized handle shapes and the processor-split path are unnecessary.

## Extension provisioning in tests

`INSTALL <ext>` (floating latest, default repo) + `LOAD <ext>` — same mechanism as the DuckLake
`TestLanceExtensionCanary`. Verified downloadable in this environment: lance `3500606`, vortex
`275ac23` (both for DuckDB 1.5.4 / linux_amd64). Fixtures write a small dataset via a direct DuckDB
connection, then query through the duckbridge PTFs. Tests `assumeTrue(available)` and skip cleanly if
an extension can't be downloaded. `TestLanceVortexExtensionCanary` is the version-drift tripwire.

Note: vortex is NOT in the *community* repo for 1.5.4/linux_amd64 (404), but IS available via the
default `INSTALL vortex` (already cached from a prior default-repo install). If a fresh machine lacks
it, the vortex tests skip.

## For the transplant

- All P5 files are self-contained (no `project(":...")`, no DuckLake imports). Ready for `git mv`.
- The `Query.QueryFunctionHandle` reuse depends on `io.trino.plugin.jdbc.ptf.Query` being public in
  base-jdbc (it is at 483). If a future base-jdbc makes it package-private, fall back to a small
  local `ConnectorTableFunctionHandle` wrapping a `JdbcTableHandle` + a matching `applyTableFunction`
  override in a custom `DefaultJdbcMetadata` subclass.
- If domain pushdown into the scan SQL is wanted later, that's the point to port
  `DuckDbWhereClauseTranslator` + a custom metadata — a larger change than P5's scope.
