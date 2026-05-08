# TODO: jOOQ Follow-ups (Runtime + Tests)

Date: 2026-05-07
Status: open
Owner: unassigned

## Current state

The catalog migration is complete:

- `JdbcDucklakeCatalog` query construction is jOOQ-based.
- `DucklakeWriteTransaction` and write paths use jOOQ-backed transaction SQL.
- Raw JDBC in catalog runtime is now mostly connection lifecycle/transaction boundary handling.

This doc now tracks only **remaining work**.

## Open runtime items

### 1) Add upstream-style logical conflict checking in commit flow

Why:
- Current conflict detection is primarily snapshot-lineage based.
- Upstream DuckLake additionally checks semantic incompatibilities in intervening snapshot changes.

TODO:
- Add a `checkForLogicalConflicts(...)` pass in `executeWriteTransaction` after winning snapshot advancement but before final commit.
- Define conflict matrix (schema/table/view/data-file operations) and reject incompatible interleavings.
- Return conflict errors with useful intervening change summaries (same quality as current retry conflict text).

Acceptance:
- Concurrent write tests include at least one semantic-conflict case that lineage-only checking would miss.

### 2) Decide on Kotlinization timing for catalog internals

Context:
- jOOQ conversion is done in Java.
- Kotlinization is still optional follow-up; no blocker for correctness.

TODO:
- Either:
  - keep Java for now and remove Kotlinization from near-term roadmap, or
  - schedule a contained Kotlin pass and use `org.jooq.kotlin` idioms consistently.

Acceptance:
- Explicit decision captured in this doc (target release or deferred).

## Test-side jOOQ adoption

## Problem statement

Tests currently use a lot of raw SQL/JDBC against catalog tables. There is effectively no jOOQ usage in:

- `jvm/ducklake-catalog/test/**`
- `jvm/trino-ducklake/test/**`

Result:
- Runtime code gets compile-time schema drift detection.
- Many catalog-introspection tests fail only at runtime on schema changes.

## Goal

Use jOOQ for **catalog-table introspection** in tests where practical, without rewriting Trino SQL behavior tests.

## Scope boundaries

In scope:
- Postgres catalog-table checks in tests (`ducklake_snapshot`, `ducklake_table`, `ducklake_data_file`, etc.).
- Reusable test helpers that centralize jOOQ DSL for common assertions.

Out of scope:
- Replacing `computeActual(...)` Trino SQL tests.
- Replacing DuckDB SQL tests (`jdbc:duckdb:` paths).

In scope but with caveats:
- Dynamic inlined-data table reads (`ducklake_inlined_data_{tableId}_{schemaVersion}`) — codegen cannot model their names, but they can still be expressed in jOOQ via `DSL.table(DSL.name(...))` with unqualified field references. This pattern is encapsulated in `CatalogQueries.inlinedDataTable(...)` and `CatalogQueries.activeInlinedRowCountForSchemaVersion(...)`.

## Phase T1: Shared jOOQ test helper — DONE (2026-05-07)

Helpers live in `ducklake-catalog`'s `testFixtures` source set so both `ducklake-catalog/test` and `trino-ducklake/test` share them:

- **`CatalogTestSupport.dsl(Connection)`** — wraps a caller-managed JDBC connection in a `DSLContext` configured to match the runtime catalog's `Settings` (lowercase unquoted identifiers). Connection lifecycle stays with the caller.
- **`CatalogPredicates`** — composable `Condition` builders:
  - `currentlyActive(endSnapshot)` — `end_snapshot IS NULL`
  - `activeAt(beginSnapshot, endSnapshot, snapshotId)` — point-in-time visibility
  - `activeAt(Table<?>, snapshotId)` — convenience overload off a generated table
  - `activeTableNamed(tableName)` — `table_name = ? AND end_snapshot IS NULL`
- **`CatalogQueries`** — named lookups against the generated schema:
  - `latestSnapshotId`, `latestSnapshotIdOrEmpty`, `latestSnapshotIdSubquery`
  - `currentSchemaVersion`, `snapshotSchemaVersion(snapshotId)`
  - `activeTableId(tableName)`, `activeTableIdOrEmpty`
  - `activeDataFileCount(tableId)`
  - `schemaVersionsByTable(tableId)` → `List<SchemaVersionRow>`
  - `inlinedDataSchemaVersions(tableId)`, `inlinedDataTable(tableId, sv)`
  - `activeInlinedRowCount{,ForSchemaVersion,sBySchemaVersion}(...)`

Build wiring: `testFixturesApi(libs.bundles.jooq)` in `ducklake-catalog/build.gradle.kts` so consumers (e.g. `trino-ducklake/test`) get the jOOQ types transitively.

## Phase T2: Migrations

### Done (2026-05-07)
- `AbstractDucklakeCrossEngineTest` — `assertRowsStayedInlined` / `assertRowsWrittenToParquet` helpers; deleted the bespoke `queryLong(...)` polymorphic helper. File shrank ~60 lines (219 → 158).
- `TestDucklakeCrossEngineTypeAudit` — both inlined-then-Parquet ALTER tests (`testDuckdbInlineAlterThenInlineMatchesTrino`, `testDuckdbNineAlterNineStaysInlinedAndMatchesTrino`) using `activeInlinedRowCountsBySchemaVersion(...)`.
- `TestDucklakeCrossEngineCatalogMetadata` — `readCurrentSchemaVersion` (uses `currentSchemaVersion`), `countSchemaVersionRowsWithNullTableId` (composed inline against `DUCKLAKE_SCHEMA_VERSIONS` to demonstrate the no-canned-helper case).
- `AbstractDucklakeIntegrationTest.getCurrentSnapshotIdFromCatalog` — one-liner using `latestSnapshotId(dsl)`.
- `TestDucklakeSnapshotAndSchemaVersion` — `getActiveTableId`, `getSchemaVersionRows`, `getCurrentSchemaVersion`, `assertSchemaVersionRowConsistent`. The local `SchemaVersionRow` record was removed in favor of `CatalogQueries.SchemaVersionRow`.

### Remaining high-value migrations
- `TestDucklakeDuckDbFormatWrite` — file-format/column-stats joins against `ducklake_file_column_stats` + `ducklake_column` + `ducklake_data_file`. No canned helper today; the predicates compose cleanly inline (see `countSchemaVersionRowsWithNullTableId` for the pattern).

### Phase T3: Expand opportunistically

As tests are touched for other reasons, migrate additional catalog raw SQL to helper methods/DSL.

Do not force a broad mechanical rewrite if signal is low.

## Risks / tradeoffs

- Test code can become harder to read if jOOQ DSL is overused inline.
  - Mitigation: keep short helper methods with intention-revealing names; reach for inline DSL only when the predicate is genuinely one-off.
- Some low-level diagnostics are easier in plain SQL.
  - Mitigation: allow selective raw SQL where it clearly improves troubleshooting.
- Dynamic inlined-data tables need unqualified column references — `table.field("begin_snapshot", Long.class)` returns `null` because jOOQ codegen never saw the table. The fix is to build `Field<Long>` directly via `DSL.field(DSL.name(...))`, mirroring what the runtime's `JdbcDucklakeCatalog$InlinedDataTable` already does. `CatalogQueries.activeInlinedRowCountForSchemaVersion` encapsulates this so callers don't have to.

## Acceptance criteria (test side)

- Core catalog-introspection tests use jOOQ helpers instead of ad hoc raw SQL.
- Schema changes that affect referenced catalog columns fail fast at compile time in migrated tests.
- No regression in test clarity or debuggability.

## Notes

- This doc intentionally removed completed migration steps from the prior version and now tracks only open work.
