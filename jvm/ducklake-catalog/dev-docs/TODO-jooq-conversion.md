# Convert SQL Access in catalog to use jOOQ

Goal: replace all hand-written JDBC + raw SQL in `JdbcDucklakeCatalog` (and
`DucklakeWriteTransaction`) with jOOQ DSL built against the generated
`dev.brikk.ducklake.catalog.schema.*` tables. End state: a single catalog
implementation with compile-time-checked queries, no raw SQL string concat,
consistent transaction semantics.

## Prereqs (done)

- [x] jOOQ codegen wired into the module — `./gradlew :ducklake-catalog:jooqCodegenDucklake`
  emits `generated/dev/brikk/ducklake/catalog/schema/**` from a DuckLake-bootstrapped
  Postgres testcontainer.
- [x] Custom naming strategy in `:jooq-custom-naming` (`JooqCustomNaming`).
- [x] `DSLContext` constructed in `JdbcDucklakeCatalog` over the Hikari `DataSource`,
  dialect inferred via `JDBCUtils.dialect(url)`. No consumers yet — existing raw JDBC
  paths are untouched.

## Known query-surface inventory

JDBC call sites in `JdbcDucklakeCatalog.java` (~60 SQL statements) break down as:

- ~20 simple SELECTs (single table, snapshot-range predicate)
- ~5 INNER / LEFT joins (data_file ↔ delete_file, partition_info ↔ partition_column,
  file_column_stats ↔ data_file)
- ~3 batch INSERTs (file partition values, per-file column stats, per-table column stats)
- 1 UPSERT with CASE-based typed min/max merge (`ducklake_table_column_stats`)
- 2 dynamic-table-name queries (inlined data: `ducklake_inlined_data_{tableId}_{schemaVersion}`)
- 1 optimistic-locking loop around snapshot PK conflicts

Reusable helpers today: `setUuid` (Postgres-specific UUID binding), `setNullableString`,
`readSnapshot/readView`, `getLongOptional/getStringOptional/getBooleanOptional`,
`AggregatedColumnStats`, `hasTransactionConflict` / `isMetadataPrimaryKeyConflict`,
`DucklakeWriteTransaction`.

## Plan

The work is phased so each step lands on green tests and the catalog stays shippable
between steps. Each phase migrates a coherent surface area; the raw-JDBC implementation
is deleted only after its jOOQ replacement is covered by the existing integration
tests.

### Phase 1 — Shared jOOQ utilities

Lay down the helpers every later phase will call. No behavior change yet.

- [x] `SnapshotRange` helper — a `Condition` builder for the
  `begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)` predicate that
  appears in ~30 queries. Takes a generated `Table<?>` and snapshot id, returns a
  `Condition`. Lives at `src/dev/brikk/ducklake/catalog/SnapshotRange.java`; also
  exposes a `(Field<Long>, Field<Long>, long)` overload for the dynamic
  inlined-data tables (Phase 4).
- [x] Transaction bridge — `DSLContext forConnection(Connection c)` that returns a
  connection-scoped `DSLContext` using the same `dialect` + `Settings` as the field
  `dsl`. Used from inside `DucklakeWriteTransaction` so writes flow through the
  transaction's `Connection`, not a fresh pool checkout.
- [x] UUID binding — added a `forcedTypes` entry for `.*\.(?:schema|table|view)_uuid`
  in `ducklake-catalog/build.gradle.kts`; regenerated and confirmed the three
  UUID columns (`SCHEMA_UUID`, `TABLE_UUID`, `VIEW_UUID`) expose `UUID` getters.
  See "Resolved decisions → UUID columns" below for the rationale and call-site
  conversion strategy.
- [ ] `jooq-kotlin` usage — `JdbcDucklakeCatalog` is being Kotlinized in a parallel
  branch (not yet pushed) before this migration finishes. Plan around that: once the
  Kotlin port lands, use jOOQ's Kotlin extensions (`org.jooq.kotlin.*`) — destructuring
  on `Record`, `fetchInto<T>()`, `DSL.select { ... }` DSL blocks — rather than Java-
  style call chains. The `jooq-kotlin` dep is already on the classpath via the bundle,
  so no extra wiring needed. If a jOOQ-phase lands before the Kotlin port, write it in
  Java with the expectation that Kotlin conversion will happen in the subsequent
  refactor pass.

### Phase 2 — Tier 1 reads (pure SELECTs, no joins)

Straight port of read-only methods, one method at a time, each landed as its own
commit + run of `:ducklake-catalog:test`.

- [x] Snapshot reads: `getCurrentSnapshotId`, `getSnapshot`, `getSnapshotAtOrBefore`,
  `listSnapshots`, `listSnapshotChanges`.
- [x] Metadata key read: `getDataPath`.
- [x] Schema reads: `listSchemas`, `getSchema`.
- [x] Table reads: `listTables`, `getTable`, `getTableById`.
- [x] Table stats read: `getTableStats`.
- [x] View reads: `listViews`, `getView`.
- [x] Retire the old `readSnapshot` / `readView` helpers and the
  `parseSnapshotTime` parser (jOOQ returns `OffsetDateTime` via the Postgres
  driver — the SQLite/DuckDB text-format normalization is no longer needed).
  Deleted the reflective pin test
  `TestJdbcDucklakeCatalogSnapshotTimeParsing` alongside. `getLongOptional` /
  `getStringOptional` / `getBooleanOptional` stay until Phase 3 finishes (still
  called by `getTableColumns`, `getAllColumnsWithParentage`, `getDataFiles`,
  etc.).

### Phase 3 — Tier 2 reads (joins + aggregation)

- [x] `getTableColumns` + `getAllColumnsWithParentage` — SELECT ducklake_column with
  snapshot range + ORDER BY; keep the parent-link graph walk in Java (jOOQ doesn't
  simplify struct/list/map reconstruction). Retired `getColumnType` private helper
  (its only caller, `findDataFileIdsInRange`, now inlines the SELECT).
- [x] `getDataFiles` — LEFT JOIN `ducklake_data_file` with `ducklake_delete_file`
  (aliased `del`) on `data_file_id` + `activeAt(del, snapshotId)` in the ON
  clause; Java-side `fetch(...)` mapper folds into `DucklakeDataFile` with
  optional delete info.
- [x] `getPartitionSpecs` — INNER JOIN `ducklake_partition_info` with
  `ducklake_partition_column`; Java-side `forEach` groups by `partition_id`.
- [x] `getFilePartitionValues` — INNER JOIN `ducklake_file_partition_value` with
  `ducklake_data_file` for snapshot range; Java-side `forEach` groups by
  `data_file_id`.
- [x] `findDataFileIdsInRange` — INNER JOIN `ducklake_file_column_stats` with
  `ducklake_data_file`; typed min/max comparison (`parseStatValue`,
  `isWithinBounds`) stays in Java as a post-fetch stream filter — column-type-aware
  logic doesn't belong in SQL.
- [x] `getColumnStats` — INNER JOIN + Java-side typed min/max merge via `forEach`
  accumulator. Still a candidate for follow-up cleanup to centralize
  `AggregatedColumnStats.merge()` — out of scope for this phase.

**Null-safety note:** DuckLake's schema leaves most BIGINT columns nullable at the
database layer, so jOOQ's generated accessors return `Long`. The original
`ResultSet.getLong()` path silently returned `0L` for SQL NULL — callers (and the
test data generator!) rely on that for fields like `file_order`. Added a
package-private `orZero(Long)` helper in `JdbcDucklakeCatalog` and threaded it
through every `Long → long` unbox in the mapper helpers and inline join
projections. If we ever tighten the schema (DuckLake upstream or our own
migrations), `orZero` becomes a no-op we can delete.

### Phase 4 — Inlined data (dynamic table names)

- [x] `getInlinedDataInfos` — outer SELECT on `ducklake_inlined_data_tables` with
  the sub-query `SCHEMA_VERSION.le(dsl.select(DUCKLAKE_SNAPSHOT.SCHEMA_VERSION).where(SNAPSHOT_ID.eq(?)))`;
  per-row existence probe targets the dynamic table via
  `InlinedDataTable.existsAsTable(dsl)` (`selectOne().from(table).where(falseCondition()).fetch()`,
  catching `DataAccessException` for non-materialized tables).
- [x] `hasInlinedRows` — `dsl.fetchExists(DSL.selectOne().from(table).where(activeAt(...)))`.
- [x] `readInlinedData` — dynamic projection built as `List<Field<?>>` with
  `DSL.inline((Object) null).as("cN")` for missing source columns and
  `DSL.field(DSL.name(srcColumn)).as("cN")` for present ones. Runs through
  `dsl.select(projected).from(table).where(...).orderBy(...).fetch()`.
- [x] `getSnapshotIdForSchemaVersion` — 2 SELECTs (table-scoped + fallback) on
  `ducklake_schema_versions` / `ducklake_snapshot`; signature changed to drop the
  `Connection` param now that writes don't need a shared connection.

**Dynamic-table helper:** introduced a private nested `InlinedDataTable` record in
`JdbcDucklakeCatalog` that bundles `{name, Table<?>, Field<Long> beginSnapshot,
Field<Long> endSnapshot}` with `activeAt(snapshotId)` and `existsAsTable(dsl)`
convenience methods. Collapses the 4-line "build table + field refs" dance to a
single `InlinedDataTable.of(tableId, schemaVersion)` call at the top of each
method. Validates the `SnapshotRange.activeAt(Field<Long>, Field<Long>, long)`
overload added in Phase 1 — it gets exercised by every inlined-data path.

**Retired:** `quoteIdentifier(String)` private helper — its only callers were the
inlined-data methods, and `DSL.name(...)` handles identifier quoting in a
dialect-aware way. `writeQuotedValue` stays — still used by
`changeCreatedTable/View/Schema` for the DuckLake-specific `changes_made` text
column format.

### Phase 5 — Transaction framework

Bring `DucklakeWriteTransaction` onto jOOQ before porting writes — otherwise each
write-path port duplicates transaction plumbing.

- [x] Reworked `DucklakeWriteTransaction` to hold a connection-scoped `DSLContext`
  alongside the existing `Connection`. New `dsl()` accessor exposed; the raw
  `getConnection()` accessor and the `resolveSchemaId` / `resolveTableId` /
  `hasTablesInSchema` raw-JDBC helpers stay so Phase 6+ write paths still
  compile. Constructor now takes `(Connection, DSLContext, long, long, long, long)`.
- [x] `executeWriteTransaction` — ported snapshot-lineage reads
  (`readLatestSnapshotId`, `ensureSnapshotLineageUnchanged`, `insertSnapshotRow`,
  `getInterveningChangesSummary`, `transactionConflictException`) to jOOQ.
  Signatures changed from `(Connection, ...) throws SQLException` to
  `(DSLContext, ...)` (unchecked). The method also ports the initial state read,
  the `ducklake_schema_versions` insert (nullable `table_id`), and the
  `ducklake_snapshot_changes` insert — all three now use `txDsl.insertInto(...)`.
  `DSL.currentOffsetDateTime()` replaces the raw `CURRENT_TIMESTAMP` literal for
  the snapshot-time column.
- [x] PK-conflict detection — no changes needed. `findSqlException` already walks
  the cause chain, so jOOQ's `DataAccessException(cause=SQLException)` wrapping
  is transparent to `isMetadataPrimaryKeyConflict` / `isDuplicateKeyViolation`.
- [x] Decision: kept the explicit `Connection.setAutoCommit(false)` +
  commit/rollback pattern rather than `ctx.transaction(...)`. Rationale in
  "Resolved decisions → Transaction retry" below (unchanged).

### Phase 6 — Schema + table DDL writes

Simple INSERT/UPDATE ports; the soft-delete (`end_snapshot`) pattern repeats.

- [x] `createSchema`, `dropSchema`.
- [x] `createTable` — INSERT `ducklake_table` + recursive column INSERT (keep
  recursion in Java) + optional partition spec (INSERT `ducklake_partition_info` +
  multi-row INSERT `ducklake_partition_column`). `insertColumnTree` lost its
  `throws SQLException` — jOOQ wraps everything in unchecked
  `DataAccessException`.
- [x] `dropTable` — fan-out UPDATEs for `ducklake_table` / `ducklake_column` /
  `ducklake_data_file` / `ducklake_delete_file` / `ducklake_partition_info`. The
  `ducklake_delete_file` update uses `DSL.select(DATA_FILE_ID).from(DUCKLAKE_DATA_FILE).where(...)`
  inside an `.in(...)` clause — same shape as the original SQL subquery.
- [x] `addColumn`, `dropColumn`, `renameColumn` — recursive column handling
  reuses `insertColumnTree`. `addColumn` uses `DSL.max(COLUMN_ORDER) → orZero`
  rather than a SQL `COALESCE`, since jOOQ returns `Long` and the empty-table
  case is rare enough that one Java unbox beats an extra SQL function.

**No `setUuid` / `setNullableString` calls remain in the Phase 6 paths.** The
catalog UUID round-trips via `UUID.fromString(newCatalogUuid())` at the
`.set(SCHEMA_UUID, ...)` / `.set(TABLE_UUID, ...)` call sites — generated UUID
columns auto-bind via the `forcedTypes` entry from Phase 1. Nullable-string
columns (`initial_default`, `default_value_dialect`) are left out of the
`insertInto(...)` builder entirely; jOOQ omits them from the INSERT and the
column defaults to SQL NULL, matching the original explicit-NULL semantics.

**Verified by:** `./gradlew :ducklake-catalog:test` (24 tests, 0 failures) and
`./gradlew :trino-ducklake:test` (473 tests, 0 failures). The catalog test
fixture populates state via DuckDB's ducklake extension rather than our DDL
methods, so the trino-ducklake suite is the load-bearing coverage for Phase 6
paths (CREATE/DROP SCHEMA, CREATE/DROP TABLE, ADD/DROP/RENAME COLUMN all flow
through here).

### Phase 7 — View writes

- [x] `createView`, `dropView`, `renameView`, `replaceViewMetadata` — all four
  converge on the four jOOQ helpers below; the public methods themselves are
  now tiny callers.
- [x] `insertViewRow` — takes `UUID` directly (was `String`); generated
  `DUCKLAKE_VIEW.VIEW_UUID` binds to `java.util.UUID` via Phase 1's
  `forcedTypes`, so no string round-trip inside jOOQ. Callers convert at
  the boundary: `UUID.fromString(newCatalogUuid())` in `createView`,
  passthrough from `ActiveViewRow.viewUuid()` in `renameView` /
  `replaceViewMetadata`.
- [x] `resolveActiveViewRow` — jOOQ select on `DUCKLAKE_VIEW` with
  `activeAt(DUCKLAKE_VIEW, snapshotId)`; `ActiveViewRow.viewUuid` is now
  `UUID` (internal-only record, flipped off `String`).
- [x] `endSnapshotActiveView` — jOOQ `update(...).set(END_SNAPSHOT, ...).where(...)`;
  rowcount check preserved via `.execute()` return value.
- [x] `hasActiveView` / `hasActiveTable` — `dsl.fetchExists(DSL.selectOne().from(...))`
  replaces the `SELECT 1 ... LIMIT 1 → rs.next()` idiom. Terser and
  dialect-agnostic.

**No shared soft-delete helper extracted.** Phase 6's `dropTable` fan-out
(5 different tables) and Phase 7's single-view `endSnapshotActiveView` don't
share a uniform "get active row + soft-delete" shape — the Phase 6 updates are
keyed on `table_id` across heterogeneous tables with different column
projections. With jOOQ the 4-line
`update(T).set(T.END_SNAPSHOT, newSnapshotId).where(T.XXX_ID.eq(id)).and(T.END_SNAPSHOT.isNull()).execute()`
form is readable enough inline; a helper would either hard-code the id column
or require passing a `Field<Long>` plus the table, buying little over the
literal.

**`setUuid` / `isPostgresql` are now dead in the catalog** — last calls were in
the view writes above. Kept in place (along with `setNullableString`, still
used in Phase 8 commit paths) so Phase 9 owns the deletion diff cleanly.

**Verified by:** `./gradlew :ducklake-catalog:test` (24 tests, 0 failures) and
`./gradlew :trino-ducklake:test` (473 tests, 0 failures). View lifecycle
coverage rides on trino-ducklake's CREATE/DROP/ALTER VIEW and rename paths.

### Phase 8 — Commit paths (hard)

Biggest surface area; save for last so the earlier phases have shaken out the helper
APIs.

- [x] `commitInsert`:
  - [x] Multi-row INSERT for `ducklake_data_file` — used
    `ctx.batchInsert(List<DucklakeDataFileRecord>)`. Records are built once per
    fragment in a single pass (allocating `data_file_id`, accumulating
    `runningRowId`/totals), then batched in one wire call. Equivalent to the
    multi-row-`values` form for our workload and keeps the record-based shape
    consistent with the two tables below.
  - [x] Multi-row INSERT for `ducklake_file_partition_value` +
    `ducklake_file_column_stats` — `ctx.batchInsert(List<Record>)` on unsaved
    records, exactly as the TODO recommended.
  - [x] Table stats UPSERT — **not** `ON CONFLICT DO UPDATE`. DuckLake's
    `ducklake_table_stats` has no PK or UNIQUE on `table_id`, so
    Postgres rejects the `ON CONFLICT (table_id)` clause with "there is no unique
    or exclusion constraint matching the ON CONFLICT specification". Fell back
    to the explicit existence-probe + INSERT/UPDATE split — `selectFrom(...)`
    returns the existing `DucklakeTableStatsRecord` (or null), the branch
    chooses UPDATE-with-delta or INSERT-fresh. Same semantics as the original;
    the existence read is free since we need `next_row_id` anyway for the
    per-fragment `row_id_start`. TODO Phase 9 could explore widening the
    DuckLake schema with a PK, but that's cross-engine territory.
  - [x] `ducklake_table_column_stats` merge — `DSL.when(...)` chain for the
    min/max CASE, value parsing stays in `AggregatedColumnStats.merge`. The
    `contains_null`/`contains_nan` `col = (col OR ?)` shape collapsed to "only
    emit SET when the flag is true" since `Field<Boolean>.or(Field<Boolean>)`
    isn't in the jOOQ API and OR-with-false is a no-op in Postgres
    (FALSE→FALSE, TRUE→TRUE, NULL→NULL). Same on-wire effect, cleaner code.
    Insert path uses `ctx.batchInsert(List<DucklakeTableColumnStatsRecord>)`.
  - [x] Dynamic IN-list for existing column IDs — `COL.COLUMN_ID.in(candidateColumnIds)`;
    `fetchSet(COL.COLUMN_ID)` returns the `Set<Long>` directly, replacing the
    hand-rolled placeholder string + manual parameter binding.
- [x] `commitDelete` — `ctx.batchInsert(List<DucklakeDeleteFileRecord>)` + UPDATE
  `DUCKLAKE_TABLE_STATS.RECORD_COUNT` via
  `DSL.greatest(DSL.inline(0L), RECORD_COUNT.minus(total))`.
- [x] `commitMerge` — untouched; still a trivial composer over
  `applyDeleteFragments` + `applyInsertFragments`.

**Batching model chosen.** `ctx.batchInsert(records)` sends one JDBC prepared-
statement batch per call site (one wire round-trip, N executes). Equivalent to
a multi-row `INSERT ... VALUES (...)` for perf at our scale, but the
record-centric API plays nicer with jOOQ's type-safe column setters and the
in-memory Record → UPDATE conversion we already use elsewhere.

**`setUuid` / `setNullableString` / `isPostgresql` all dead now.** Phase 8 was
the last holdout for `setNullableString`. All three can die in Phase 9 without
any call-site follow-up.

**Verified by:** `./gradlew :ducklake-catalog:test` (24 tests, 0 failures) and
`./gradlew :trino-ducklake:test` (473 tests, 0 failures). INSERT, DELETE, and
MERGE query paths all get real coverage from trino-ducklake's write integration
suite (`TestDucklakeWriteIntegration`, `TestDucklakeDeleteIntegration`).

### Phase 9 — Cleanup

- [x] Deleted `isPostgresql` + `setUuid`. jOOQ handles Postgres `UUID` binding
  via the generated column type (`SQLDataType.UUID` via Phase 1's `forcedTypes`);
  no dialect branch needed at call sites.
- [x] Deleted `setNullableString`, `getLongOptional`, `getStringOptional`,
  `getBooleanOptional`. No callers remained after Phase 8.
- [x] **Kept** `writeQuotedValue` / `changeCreatedTable` / `changeCreatedView` /
  `changeCreatedSchema`. These are *not* raw-SQL helpers — they format the text
  written into `ducklake_snapshot_changes.changes_made`, which is a freeform
  `String` column that upstream DuckLake's `ducklake_snapshots()` parser reads.
  `TestJdbcDucklakeCatalogChangesMadeFormat` pins the output grammar. The
  earlier `quoteIdentifier` helper was retired in Phase 4 (its only callers
  were the inlined-data methods, now using `DSL.name(...)`).
- [x] Dead imports dropped: `java.sql.PreparedStatement`,
  `java.sql.ResultSet`, `java.util.HashSet`. `java.sql.Connection` /
  `java.sql.SQLException` stay — `executeWriteTransaction` still opens a
  pool `Connection`, wraps commit/rollback manually, and the
  `WriteTransactionAction` interface declares `throws SQLException` for any
  raw-JDBC helpers future code might add inside a transaction callback (none
  today, but the surface is there).
- [x] Regression pin: `./gradlew :ducklake-catalog:test` (24 tests, 0 failures)
  and `./gradlew :trino-ducklake:test` (473 tests, 0 failures) both green
  after the cleanup.

**Migration complete.** All query construction in `JdbcDucklakeCatalog` and
`DucklakeWriteTransaction` goes through jOOQ; the only raw JDBC that remains
is the pool-connection lifecycle in `executeWriteTransaction` (intentional,
per Phase 5's "Transaction retry: `ctx.transaction(...)` vs. bespoke loop"
decision). The follow-ups in "Resolved decisions → Transaction retry"
(internal retry with backoff, logical `CheckForConflicts` pass) remain open
as orthogonal correctness work; they don't block shipping the migration.

## Non-goals

- Porting the `trino-ducklake` plugin's SQL — it uses this catalog via the
  `DucklakeCatalog` interface and shouldn't care that the impl now uses jOOQ.
- Adding a second backend (DuckDB-as-catalog, MySQL, etc.). Keep the work scoped to
  Postgres; dialect abstraction falls out of jOOQ for free once we want it.
- Rewriting `AggregatedColumnStats` / typed min-max parsing — it's orthogonal to SQL
  construction and the logic is correct today.

## Resolved decisions

### UUID columns

**Decision: use `forcedTypes` in codegen to bind every `*_uuid` column to
`java.util.UUID`.** DuckLake writes `schema_uuid` / `table_uuid` / `view_uuid` as
Postgres `uuid` (so jOOQ OSS's built-in `UUID` binding is usually enough on its own —
`DefaultBinding<UUID>` maps `java.util.UUID ↔ java.sql.Types.OTHER` on Postgres). Pin
it anyway via `forcedTypes` so the binding doesn't drift if DuckLake's schema ever
widens to `text`/`varchar` on a non-Postgres backend.

- [x] Add a `forcedTypes` entry in `ducklake-catalog/build.gradle.kts`:
  ```kotlin
  forcedTypes {
      forcedType {
          name = "UUID"
          includeExpression = ".*\\.(?:schema|table|view)_uuid"
          includeTypes = ".*"
      }
  }
  ```
- [x] Regen + confirm the generated record's getter returns `UUID` (not `String`).
- [x] Our internal call sites use `String` (`JdbcDucklakeCatalog.newCatalogUuid()`
  returns a `String` representation of a UUIDv7). On the boundary between our code and
  jOOQ, convert with `UUID.fromString(str)` / `uuid.toString()`. Since every catalog
  UUID we mint is v7 and every one we read back is persisted as Postgres `uuid`, a
  round-trip through `java.util.UUID` preserves the value bit-for-bit (v7 layout fits
  in `UUID`'s 128-bit storage — the `version` nibble is `0x7`, nothing jOOQ cares
  about). **Done in Phases 6–8** — all `UUID.fromString(newCatalogUuid())`
  conversions wired at the write-path call sites; read-path records hold
  `java.util.UUID` directly where they weren't already `String` for
  legacy-compat reasons.
- [x] **Resolved: write v7, read version-agnostic.** A `Converter<UUID, UuidV7>` was
  attempted and reverted — the catalog is realistically mixed-version, so binding-layer
  enforcement breaks cross-engine reads. Findings from upstream:
    - **DuckLake spec** (`docs/stable/specification/data_types.md:39`): `uuid` is just
      "Universally unique identifier", no version mandate. Example happens to be v4.
    - **pg_ducklake's vendored DuckDB ducklake extension (HEAD):** `schema_uuid` and
      `table_uuid` use `GenerateUUIDv7()` (`ducklake_transaction.cpp:3107`), but
      `view_uuid` uses plain `UUID::GenerateRandomUUID()` (`ducklake_schema_entry.cpp:159`)
      → v4. Inconsistent within upstream itself.
    - **DuckDB 1.5.2 shipped extension** (what our test container uses): writes v4
      across the board — pre-dates the v7 switch. Verified empirically: a
      `schema_uuid` minted by the test setup came back as
      `aa71b8bd-82d0-43dc-b177-f02cccab056d` (version nibble = 4).
    - **datafusion-ducklake:** doesn't use UUID columns at all — uses integer
      auto-increment IDs. Irrelevant to this decision.
    - **Read-side validation:** none of the implementations check the version
      nibble. Postgres treats `uuid` as opaque 128 bits.
  Our policy: continue minting v7 via `Generators.timeBasedEpochGenerator()` for the
  catalog-identity UUIDs we write (modest B-tree locality benefit on the subset we
  own, even though it's diluted by other writers' v4 rows). On read, the
  `name = "UUID"` `forcedType` surfaces a plain `java.util.UUID` with no version
  check, so any catalog the spec accepts, we accept.

### Inlined data dynamic tables

**Decision: keep them out of codegen; use `DSL.table(DSL.name(...))` +
`DSL.field(DSL.name(...), type)` + jOOQ parameter binding for the dynamic projection.**
These tables don't exist at codegen time — `ducklake_inlined_data_{tableId}_{schemaVersion}`
is created on demand by DuckLake for tables we may never have seen. Synthesising them
in codegen would require a meta-model of DuckLake's inlining rules that we'd have to
keep in sync. Dynamic jOOQ gives us parameter binding and a real `Query` object (no
string concat, no manual `PreparedStatement`) without the sync cost.

Call-site pattern (sketch):
```java
Table<?> inlined = DSL.table(DSL.name("ducklake_inlined_data_" + tableId + "_" + schemaVersion));
Field<Long> beginSnap = DSL.field(DSL.name("begin_snapshot"), Long.class);
dsl.selectOne().from(inlined).where(snapshotRange(beginSnap, endSnap, snapshotId)).limit(1);
```

### Transaction retry: `ctx.transaction(...)` vs. bespoke loop

**Context — what upstream does.** Confirmed by reading the C++ ducklake extension
at `duckdb/ducklake@main`:

- **Optimistic, app-level, same shape as ours.** Default isolation, compute
  `snapshot_id = MAX(snapshot_id) + 1` in the app, INSERT into `ducklake_snapshot`
  (PK on `snapshot_id`), catch PK/unique/"conflict"/"concurrent" errors, retry.
  Source: `src/storage/ducklake_transaction.cpp:FlushChanges()` (lines 2515–2615)
  and `RetryOnError()` (2498–2513). No `SET TRANSACTION ISOLATION`, no `FOR
  UPDATE`, no `pg_advisory_lock`, no `ON CONFLICT DO UPDATE`.
- **Postgres catalog path is identical to DuckDB / SQLite.**
  `src/metadata_manager/postgres_metadata_manager.cpp` dispatches through DuckDB's
  `postgres_execute` CALL with zero isolation tweaks. Postgres-specific code is
  limited to type mapping / BLOB reinterpretation.
- **Internal retry loop.** Up to `ducklake_max_retry_count` (default 10) with
  exponential backoff (`retry_wait_ms=100`, `retry_backoff=1.5`), lines 2520–2533
  and 2597–2604. Surfaces `"Failed to commit DuckLake transaction. Exceeded the
  maximum retry count..."` only after exhaustion.
- **Logical conflict check after winning the PK race.** Even once the INSERT
  succeeds, upstream runs `CheckForConflicts()`
  (`ducklake_transaction.cpp:1194–1334`, called from line 2547) to reject
  semantically incompatible interleavings — e.g. "I dropped column X in my
  snapshot; someone else altered it in the intervening snapshot." The PK gives
  mutual exclusion on `snapshot_id`; the logical check catches everything else.

**Decision: keep the bespoke loop** — it's the same model upstream uses, not a
custom invention. But note two behaviors we don't yet implement and should track:

- [x] **Follow-up: internal retry with backoff.** ~~We surface
  `TransactionConflictException` on the first PK collision.~~ Now retries up to
  10× with exponential backoff (100ms × 1.5^n), matching upstream's
  `ducklake_max_retry_count` / `retry_wait_ms` / `retry_backoff` defaults. Both
  PK conflicts and lineage-advance detections feed the same retry loop. After
  exhaustion, the latest `TransactionConflictException` is rewrapped with an
  "exceeded the maximum retry count" message. See
  `JdbcDucklakeCatalog.executeWriteTransaction`.
- [ ] **Follow-up: logical `CheckForConflicts` pass.** Our current conflict check
  is "did `max(snapshot_id)` advance while I was working" — purely lineage-based.
  Upstream additionally scans `ducklake_snapshot_changes` for incompatible
  change pairs. Without this pass we accept some interleavings that upstream
  would reject (e.g. two writers both altering the same column's type in
  different snapshots — our PK check passes for one of them, but the other's
  schema is now stale). Worth a defect-report entry before we cut the next
  release.

**Why not `ctx.transaction(...)`.** Reasons below — written up so future-us
doesn't relitigate.

`ctx.transaction(TransactionalRunnable)` wins on:
- **Boilerplate.** It does `setAutoCommit(false)` / commit / rollback / restore for
  you, and unwraps into `DataAccessException` consistently.
- **Savepoint support.** Nested `ctx.transaction` calls turn into savepoints for free,
  which we don't currently use but might want if a partial mutation inside
  `commitInsert` ever needs to roll back independently.
- **Unification.** Every read path would already be on jOOQ; using jOOQ for the
  transaction boundary too removes the "why does this one file hold raw
  `Connection`?" discoverability wart.

The bespoke loop wins on:
- **Retry semantics are ours.** Our control flow is "open txn → read max(snapshot_id)
  → do work → try INSERT into `ducklake_snapshot` → on PK conflict, *re-read*
  max(snapshot_id), compute the intervening-changes summary for the error message,
  throw `TransactionConflictException`." jOOQ's `transaction(...)` has no
  first-class retry — you'd still need to catch `DataAccessException`, inspect the
  SQL state, rebuild the txn context, and re-run the closure. At that point the jOOQ
  wrapper isn't saving anything and is obscuring the lineage check.
- **Intervening-change summary is read-after-failure.** The helpful part of
  `TransactionConflictException` is the list of snapshot IDs + change summaries that
  got committed between our read and our write. We need a *fresh* read on the
  *original* pool connection (the failed txn's connection is poisoned and about to
  roll back). `ctx.transaction(...)` would need us to bounce back out to the outer
  `DSLContext` anyway — same code, more indirection.
- **`SQLException` inspection.** `isMetadataPrimaryKeyConflict` reads
  `SQLState = 23505` / `23000` plus dialect-specific message fragments. jOOQ wraps
  into `DataAccessException(cause = SQLException)` — one extra `.getCause()` unwrap.
  Not a dealbreaker but nothing to gain either.
- **Cost of the port.** The loop is ~60 lines, well-tested, and has no known bugs.
  Porting is a net zero on LOC, a net negative on readability (the retry logic is
  already flat), and a net positive only on "everything's jOOQ." Not worth the risk
  right now.

Revisit when: (a) we adopt savepoints anywhere, (b) we need to coordinate with a
second resource (e.g. a file-system rollback), or (c) jOOQ gains built-in
optimistic-retry primitives that cover our shape. Until then, the loop stays; only
the individual `PreparedStatement` calls inside it move to jOOQ.
