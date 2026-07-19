# DuckBridge P3 — Quack data plane notes

## Transport matrix status

| Transport | connection-url | Driver | Data plane | Status |
|-----------|----------------|--------|-----------|--------|
| T1 embedded | `jdbc:duckdb:...` | `org.duckdb.DuckDBDriver` | JDBC record set (default) OR T2 Arrow | ✅ works |
| T3 remote (pass-through) | `jdbc:quack://host:port` | `com.gizmodata.quack.jdbc.sql.QuackDriver` | JDBC record set | ✅ works, integration-tested |
| T2 Arrow (DUCKDB_LOCAL) | `jdbc:duckdb:...` + `execution-engine=DUCKDB_LOCAL` | DuckDB JDBC | Arrow page source | ✅ works, integration-tested |
| T2 Arrow (QUACK) | — + `execution-engine=QUACK` | DuckDB JDBC + `quack` ext + `quack_query_by_name` | Arrow page source | ⚠️ ported + unit-tested, NOT live-wired (pool gate) |

Transport is chosen by the **connection-url prefix** (`DuckBridgeTransport`), URL-first per the plan.
Auth/tuning come from `duckbridge.quack.*` config (kept out of the copy-pasteable URL).

### Why quack-jdbc (T3) can't do the Arrow (T2) path

`quack-jdbc` is a pure-JVM pass-through driver with **no Arrow surface** (`javap` confirms its
`QuackResultSet` has no `arrowExportStream`). The T2 Arrow data plane needs
`org.duckdb.DuckDBResultSet.arrowExportStream`, which only the **DuckDB JDBC driver** exposes. So:

- T3 (quack-jdbc) is the interim remote default and uses base-jdbc's row-by-row `JdbcRecordSetProvider`.
- T2-over-Quack must use the DuckDB driver + `quack` DuckDB extension + `quack_query_by_name` wrapper
  (the DuckLake approach), NOT quack-jdbc. That's `QuackDuckBridgeExecutor`.

## Auth mechanism

`duckbridge.quack.token` → quack-jdbc `token` property; also `duckbridge.quack.token-env` (`tokenEnv`),
`duckbridge.quack.token-file` (`tokenFile`), `duckbridge.quack.tls` (`tls`). Host/port from the URL.
Token resolution order is the driver's (token → tokenEnv → tokenFile). Verified end-to-end against a
real server in `TestDuckBridgeQuackTransport`.

## Parity over Quack — VERIFIED

`DuckBridgeParity` is transport-aware:
- EMBEDDED: resolve a worker-local binary (bundled/extracted) and `LOAD '<local-path>'`.
- QUACK (T3): the worker can't extract for the remote server, so `duckbridge.parity-extension-path` is
  treated as a **server-side** path and LOADed over the pass-through connection; if unset, we probe
  `trino_meta()` assuming the server pre-loaded it. Either way, failure throws with server-side install
  instructions (fail loud).

`TestDuckBridgeQuackTransport.parityFunctionPushdownOverQuack` / `parityUnicodeCaseFoldOverQuack` run
GREEN against the real built extension LOADed server-side (`upper('straße')` → `STRASSE`,
`length` pushdown), so the full `trino_*` pushdown path works over Quack.

## SET TimeZone over Quack — VERIFIED

`SET TimeZone = '...'` executes fine over quack-jdbc (probed directly + the client's `getConnection`
applies it on every connection). No gating needed — the T3 path sets it like the embedded path.

## GLIBC gotcha (test-image base)

The DuckLake test fixture uses `debian:bookworm-slim` (GLIBC 2.36). The locally-built
`trino_parity.duckdb_extension` links against **GLIBC 2.38**, so it FAILS to LOAD on bookworm
("GLIBC_2.38 not found"). The duckbridge test image (`test/resources/docker/quack-server/Dockerfile`)
uses `debian:trixie-slim` (GLIBC 2.41) instead. If the extension's glibc floor rises again, bump the
base image. (In-process P2/P1 tests were unaffected — the host has GLIBC 2.43.)

## T2 status — what runs, what's gated

- **DUCKDB_LOCAL: LIVE.** `DuckBridgePageSourceProvider` overrides base-jdbc's
  `ConnectorPageSourceProvider` (via `OptionalBinder.setBinding`) and, for this engine, runs the split's
  SQL — rendered by base-jdbc's own `JdbcClient.buildSql` on the executor's connection (so
  projection/predicate/limit/parity pushdown are identical to the default path) — through
  `InProcessDuckBridgeExecutor`, decoding `arrowExportStream` batches via
  `DuckBridgeArrowToPageConverter`. Integration-tested end-to-end (`TestDuckBridgeArrowEngine`:
  full scan, projection, domain + parity pushdown, count(*)).
- **QUACK T2: GATED.** `QuackDuckBridgeExecutor` is ported and unit-tested, but the page-source provider
  does NOT divert to it — selecting `execution-engine=QUACK` falls back to the JDBC record-set path with
  a one-shot warning. **Known upstream gate (do not fight it): Quack 1.5.4's fixed server-side connection
  pool exhausts under per-split churn, so T2-over-Quack is a benchmark channel, not the default, until
  the pool rework lands.** Also, `buildSql` can't produce the `quack_query_by_name`-wrapped SQL the Quack
  executor needs, so live wiring requires a small SQL-string extraction step deferred with the pool gate.
- Default is `execution-engine=JDBC` (production).

## Dropped from the ported executor surface

The DuckLake executor is built around a file-scan / schema-evolution model that a base-jdbc connector
doesn't have. Dropped (stay in trino-ducklake):

- `DucklakeDuckDbExecutor.ExecutionRequest` (projectedColumns/pushedPredicate/fileColumnNamesById/
  structReshapePlans/promotedColumnIds) — base-jdbc's `JdbcTableHandle` + `buildSql` render the SQL.
  Replaced by a trivial `DuckBridgeExecutor.ExecutionRequest(sql, duckDbTimeZone)`.
- `DuckDbAttachTarget` (LocalPath/HttpfsS3/FileScan) + `DuckDbSelectSqlBuilder` — DuckLake's ATTACH-a-.db
  / read-via-scan-function machinery. Not applicable: our SQL already names its tables.
- `DucklakeExecutionEngine.SWANLAKE` (Arrow Flight SQL) — not in scope.
- `DucklakeJsonSupport` — only its trivial `isJson(type)` was needed; inlined into
  `DuckBridgeArrowToPageConverter`.

Ported (renamed, DuckLake-stripped): the executor interface, `InProcessDuckBridgeExecutor`,
`QuackDuckBridgeExecutor`, `DuckBridgeExecutorFactory`, `DuckBridgeExecutionEngine`, `DuckDbTuning`,
`DuckDbTuningSql`, `DuckDbS3Config`, `DuckDbCatalogWriteRetry`, `DuckBridgeArrowToPageConverter`.

## S3 credential story — OPEN

The current test env has no MinIO/S3, so `DuckDbS3Config` ships **ported but unexercised**. It renders a
`CREATE SECRET IF NOT EXISTS duckbridge_s3 (TYPE S3, ...)` from the standard `s3.*` catalog keys, ready
for the T2 executors to attach `s3://` URLs, but there is no integration test wiring an object store.
If/when S3 is needed, add a MinIO container to the T2 integration test and thread the secret SQL into the
executor's connection init (the DuckLake `InProcessDuckDbExecutor.prepareSource` HttpfsS3 branch is the
reference). The `renderCreateSecretSql()` is unit-tested.

## For P4+/P5

- `DuckBridgeArrowToPageConverter` is public and handles the full scalar + ARRAY/ROW/MAP/JSON surface;
  P5's lance PTFs can reuse it directly.
- The `openPreparedConnection` seam on the in-process executor (tuning+parity+TZ applied, then
  `client.buildSql`) is the clean pattern for any future custom scan that needs base-jdbc's SQL rendering
  on a DuckDB Arrow connection.
