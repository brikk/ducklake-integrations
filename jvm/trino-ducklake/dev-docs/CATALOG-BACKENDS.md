# DuckLake Catalog Backends

The connector talks to the DuckLake metadata catalog over JDBC (HikariCP + jOOQ) in
`JdbcDucklakeCatalog`. The backend dialect is inferred from the JDBC URL scheme
(`org.jooq.tools.jdbc.JDBCUtils.dialect`), and the SQL is kept to a portable subset
(`withRenderSchema(false)`, unquoted identifiers, no `ON CONFLICT`/`RETURNING`/sequences —
IDs are allocated app-side from the snapshot row). So adding a SQL backend is mostly a driver +
fixture exercise, not a rewrite.

## Support matrix

| Backend | Connector read/write | Bootstrap (schema create) | Cross-engine (DuckDB reads same catalog) | Status |
|---|---|---|---|---|
| **PostgreSQL** | Yes | DuckDB `ATTACH ducklake:postgres:` | Yes | Primary / fully tested |
| **DuckDB (local `.db`)** | Yes | DuckDB `ATTACH ducklake:<file>` | N/A (single-host file) | Fully tested |
| **DuckDB (Quack RPC)** | Experimental | Quack sidecar | Partial (upstream Quack limits) | Gated; see TODO-WRITE-MODE |
| **MySQL 8+** | **Yes** (verified) | DuckDB `ATTACH ducklake:mysql:` (one-shot) | **Deferred** — upstream DuckDB `mysql` extension is unstable | Connector-verified; cross-engine deferred |
| **SQLite** | Not yet | — | — | Deferred (low priority) |
| **Turso (libSQL)** | Not yet | — | — | Blocked on a formal JDBC driver in Maven Central (asked upstream) |

## MySQL 8+ (added 2026-07)

**What works and is tested:** our connector reads/writes a MySQL-backed DuckLake catalog through
the MySQL Connector/J driver + jOOQ (dialect auto-detected as `SQLDialect.MYSQL` from the
`jdbc:mysql:` URL). Pinned by `TestJdbcDucklakeCatalogOnMySqlSmoke` (schema + table CRUD round-trip
against a real MySQL 8.4 Testcontainer). To use it: set `ducklake.catalog.database-url=jdbc:mysql://host:3306/db`
plus `database-user`/`database-password`. The MySQL driver ships on the connector's runtime
classpath; no other config change is needed.

**Backend-specific handling that landed:**
- `JdbcDucklakeCatalog.isDuplicateKeyViolation` now recognizes MySQL's duplicate-key signal
  (vendor error code `1062` + SQLState `23000`, and the `"Duplicate entry"` message), so the
  optimistic-concurrency retry (PK collision on `ducklake_snapshot.snapshot_id`) fires correctly.
- `snapshot_time` / `schedule_start` are written via a bound app-clock `OffsetDateTime` value
  (`nowUtc()`) instead of `DSL.currentOffsetDateTime()`. jOOQ renders the latter as
  `cast(current_timestamp() as timestamp with time zone)`, which MySQL rejects (no
  `TIMESTAMP WITH TIME ZONE`). The bound value is dialect-portable and verified to not regress
  PG/DuckDB (full `ducklake-catalog` suite green).
- jOOQ bindings for MySQL's physical types are covered by existing codegen guards: booleans are
  `tinyint(1)` (jOOQ MYSQL maps to `Boolean`), and `*_uuid` columns are `text` — exactly the case
  the `*_uuid` `forcedType` UUID binding was written for.

**Why cross-engine is deferred (verified upstream blocker).** DuckLake itself supports MySQL as a
metadata backend — a one-shot DuckDB `ATTACH 'ducklake:mysql:...'` reliably bootstraps all 28
`ducklake_*` tables + the initial snapshot (5/5 clean runs). But DuckDB 1.5.4's `mysql` extension
(the latest release as of 2026-06-17) is **unstable reading a DuckLake-on-MySQL catalog**: clean
single-shot round-trips failed 5/5 with `Server has gone away` / `Got packets out of order`, and it
SIGSEGVs the JVM inside `mysql_scanner.duckdb_extension` at `ssl3_write_bytes`. Reproduced on MySQL
8.0 and 8.4, with and without `ssl_mode=disabled`. This is an upstream DuckDB `mysql`-extension bug,
not ours — our connector never uses that extension (it talks to MySQL directly). Consequently:
- We bootstrap the test schema via the reliable one-shot `ATTACH`, then drive all CRUD through our
  own JDBC path (`TestJdbcDucklakeCatalogOnMySqlSmoke`).
- We do NOT wire MySQL into the DuckDB-driven cross-engine test fixtures
  (`DucklakeCatalogGenerator` / `DucklakeQueryRunner` / `DucklakeTestCatalogBackend`), because those
  generate/read data through DuckDB, which crashes on MySQL.
- Revisit cross-engine MySQL when DuckDB's `mysql` extension stabilizes (trip-wire: re-run the
  standalone probe on DuckDB version bumps). **TODO: file this upstream at duckdb/ducklake** (sibling
  to the data-file `file_format` dispatch gap reported as duckdb/ducklake#1289).

## Turso (libSQL)

Deferred pending a formal libSQL/Turso JDBC driver in Maven Central (requested upstream). Turso is
preferred over plain SQLite for our distributed shape (Turso cloud). When the driver lands, it
should slot in like MySQL: a driver dependency + a fixture + the dialect inference (libSQL is
SQLite-wire, so `SQLDialect.SQLITE` handling — including the existing SQLite duplicate-key detection
in `isDuplicateKeyViolation` — already applies).

## SQLite

Not yet wired; low priority relative to Turso (which supersedes it for our use case). The catalog
code already recognizes SQLite's duplicate-key error (`errorCode == 19` / `UNIQUE constraint
failed`), so the main gap is a fixture + test wiring.
