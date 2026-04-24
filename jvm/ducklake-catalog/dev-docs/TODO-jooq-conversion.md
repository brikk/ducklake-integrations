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

- [ ] `SnapshotRange` helper — a `Condition` builder for the
  `begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)` predicate that
  appears in ~30 queries. Takes a generated `Table<?>` and snapshot id, returns a
  `Condition`.
- [ ] Transaction bridge — `DSLContext forConnection(Connection c)` that returns a
  connection-scoped `DSLContext` using the same `dialect` + `Settings` as the field
  `dsl`. Used from inside `DucklakeWriteTransaction` so writes flow through the
  transaction's `Connection`, not a fresh pool checkout.
- [ ] UUID binding — add a `forcedTypes { forcedType { name = "UUID"; includeExpression
  = ".*\\.(?:schema|table|view)_uuid" } }` to the codegen config, regen, and confirm
  records expose `UUID` getters. See "Resolved decisions → UUID columns" below for
  the rationale and call-site conversion strategy.
- [ ] `jooq-kotlin` usage audit — `JdbcDucklakeCatalog` is Java, but generated types
  are Java too; Kotlin extensions are only relevant if we add Kotlin code. Note here
  so later phases can opt in to `coroutines`/`kotlin` extensions if we ever Kotlinize.

### Phase 2 — Tier 1 reads (pure SELECTs, no joins)

Straight port of read-only methods, one method at a time, each landed as its own
commit + run of `:ducklake-catalog:test`.

- [ ] Snapshot reads: `getCurrentSnapshotId`, `getSnapshot`, `getSnapshotAtOrBefore`,
  `listSnapshots`, `listSnapshotChanges`.
- [ ] Metadata key read: `getDataPath`.
- [ ] Schema reads: `listSchemas`, `getSchema`.
- [ ] Table reads: `listTables`, `getTable`, `getTableById`.
- [ ] Table stats read: `getTableStats`.
- [ ] View reads: `listViews`, `getView`.
- [ ] Retire the old `readSnapshot` / `readView` / `getXxxOptional` helpers once all
  callers migrate (jOOQ records replace them).

### Phase 3 — Tier 2 reads (joins + aggregation)

- [ ] `getTableColumns` + `getAllColumnsWithParentage` — SELECT ducklake_column with
  snapshot range + ORDER BY; keep the parent-link graph walk in Java (jOOQ doesn't
  simplify struct/list/map reconstruction). Retire `getColumnType` private helper.
- [ ] `getDataFiles` — LEFT JOIN `ducklake_data_file` with `ducklake_delete_file` on
  `data_file_id` + snapshot range on both sides; Java-side fold into
  `DucklakeDataFile` with optional delete info.
- [ ] `getPartitionSpecs` — INNER JOIN `ducklake_partition_info` with
  `ducklake_partition_column`; Java-side group by `partition_id`.
- [ ] `getFilePartitionValues` — INNER JOIN `ducklake_file_partition_value` with
  `ducklake_data_file` for snapshot range; Java-side group by `data_file_id`.
- [ ] `findDataFileIdsInRange` — INNER JOIN `ducklake_file_column_stats` with
  `ducklake_data_file`; keep typed min/max comparison (`parseStatValue`,
  `isWithinBounds`) in Java — that logic is column-type-aware and shouldn't live in
  SQL.
- [ ] `getColumnStats` — INNER JOIN + Java-side typed min/max merge. Candidate for a
  follow-up cleanup to centralize `AggregatedColumnStats.merge()`.

### Phase 4 — Inlined data (dynamic table names)

- [ ] `getInlinedDataInfos` — the outer query is a straight SELECT on
  `ducklake_inlined_data_tables`; the per-row existence probe targets a dynamic table
  name (`ducklake_inlined_data_{tableId}_{schemaVersion}`) that jOOQ codegen does not
  know about. Use `DSL.table(DSL.name(...))` and
  `selectOne().from(...).where(falseCondition()).limit(1)`.
- [ ] `hasInlinedRows` — same pattern; snapshot range on the dynamic table. Build the
  `Field<?>` refs via `DSL.field(DSL.name(...))`.
- [ ] `readInlinedData` — dynamic projection (NULL-aliasing missing columns) against
  the dynamic table. This one stays closest to hand-written SQL; goal is to eliminate
  `Statement`/`PreparedStatement` construction and route through
  `dsl.resultQuery(...)`.
- [ ] `getSnapshotIdForSchemaVersion` — 2 SELECTs (table-scoped + fallback) on
  `ducklake_schema_versions` / `ducklake_snapshot`; trivial ports, just sequencing
  matters.

### Phase 5 — Transaction framework

Bring `DucklakeWriteTransaction` onto jOOQ before porting writes — otherwise each
write-path port duplicates transaction plumbing.

- [ ] Rework `DucklakeWriteTransaction` to hold a connection-scoped `DSLContext`
  alongside the existing `Connection`. Expose `dsl()` so write methods call jOOQ; keep
  raw `Connection` accessor temporarily so un-migrated paths still compile.
- [ ] `executeWriteTransaction` — port snapshot-lineage reads
  (`readLatestSnapshotId`, `ensureSnapshotLineageUnchanged`, `insertSnapshotRow`,
  `getInterveningChangesSummary`) to jOOQ. Keep PK-conflict detection
  (`isMetadataPrimaryKeyConflict`) at the `SQLException` layer — jOOQ surfaces
  `DataAccessException` wrapping the same cause, so the inspection logic just unwraps
  one level.
- [ ] Decide: use jOOQ's `TransactionProvider` (wraps `ctx.transaction { ... }`) or
  keep the explicit `Connection.setAutoCommit(false)` + commit/rollback pattern.
  Recommendation: keep explicit for now — the conflict/retry handling is bespoke and
  doesn't map cleanly to jOOQ's `TransactionalRunnable`.

### Phase 6 — Schema + table DDL writes

Simple INSERT/UPDATE ports; the soft-delete (`end_snapshot`) pattern repeats.

- [ ] `createSchema`, `dropSchema`.
- [ ] `createTable` — INSERT `ducklake_table` + recursive column INSERT (keep
  recursion in Java) + optional partition spec (INSERT `ducklake_partition_info` +
  multi-row INSERT `ducklake_partition_column`).
- [ ] `dropTable` — fan-out UPDATEs for `ducklake_table` / `ducklake_column` /
  `ducklake_data_file` / `ducklake_delete_file` / `ducklake_partition_info`. The
  `ducklake_delete_file` update uses a subquery over `ducklake_data_file` — jOOQ
  handles this with a `select(...)` in the `IN` clause.
- [ ] `addColumn`, `dropColumn`, `renameColumn` — recursive column handling; port
  alongside `createTable` since they share the recursion helper.

### Phase 7 — View writes

- [ ] `createView`, `dropView`, `renameView`, `replaceViewMetadata`. The end-snapshot +
  re-insert pattern is shared; extract a `soft-delete + reinsert` helper if the same
  shape appears again in table DDL phase.

### Phase 8 — Commit paths (hard)

Biggest surface area; save for last so the earlier phases have shaken out the helper
APIs.

- [ ] `commitInsert`:
  - [ ] Multi-row INSERT for `ducklake_data_file` —
    `insertInto(...).values(...).values(...)` or a single-statement bulk insert built
    from a Stream.
  - [ ] Multi-row INSERT for `ducklake_file_partition_value` +
    `ducklake_file_column_stats` — use jOOQ's `batchInsert(List<Record>)` on unsaved
    records for batched wire traffic.
  - [ ] Table stats UPSERT — port the conditional INSERT vs. UPDATE to
    `insertInto(...).onConflict(TABLE_STATS.TABLE_ID).doUpdate().set(...)` (Postgres)
    or an explicit `select-then-write` if we need portability.
  - [ ] `ducklake_table_column_stats` merge — the CASE-based typed min/max.
    Recommendation: express the CASE via jOOQ's `DSL.case_()` builder but keep value
    parsing (`parseStatValue`, `typedMin`, `typedMax`) in Java. The alternative —
    aggregating in SQL — doesn't play well with the column's string-encoded values.
  - [ ] Dynamic IN-list for existing column IDs — `.where(COL.COLUMN_ID.in(ids))`.
- [ ] `commitDelete` — INSERT `ducklake_delete_file` (batched) + UPDATE
  `ducklake_table_stats SET record_count = GREATEST(0, record_count - ?)`. `GREATEST`
  is supported by jOOQ across dialects.
- [ ] `commitMerge` — composes `applyDeleteFragments` + `applyInsertFragments` under
  the same write transaction; no new patterns beyond the two above.

### Phase 9 — Cleanup

- [ ] Delete the `isPostgresql` flag and `setUuid` helper (jOOQ handles dialect +
  UUID binding via the `SQLDialect` + column types).
- [ ] Delete `setNullableString`, `getLongOptional`, `getStringOptional`,
  `getBooleanOptional` once no caller remains.
- [ ] Delete `quoteIdentifier` / `writeQuotedValue` / `changeCreatedTable*` string
  builders if their only caller was the raw-SQL change-log emission path (the change
  text is a freeform `String` stored in `changes_made`; likely stays as-is).
- [ ] Run `./gradlew :ducklake-catalog:test` and `./gradlew :trino-ducklake:test` to
  pin regressions.

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

- [ ] Add a `forcedTypes` entry in `ducklake-catalog/build.gradle.kts`:
  ```kotlin
  forcedTypes {
      forcedType {
          name = "UUID"
          includeExpression = ".*\\.(?:schema|table|view)_uuid"
          includeTypes = ".*"
      }
  }
  ```
- [ ] Regen + confirm the generated record's getter returns `UUID` (not `String`).
- [ ] Our internal call sites use `String` (`JdbcDucklakeCatalog.newCatalogUuid()`
  returns a `String` representation of a UUIDv7). On the boundary between our code and
  jOOQ, convert with `UUID.fromString(str)` / `uuid.toString()`. Since every catalog
  UUID we mint is v7 and every one we read back is persisted as Postgres `uuid`, a
  round-trip through `java.util.UUID` preserves the value bit-for-bit (v7 layout fits
  in `UUID`'s 128-bit storage — the `version` nibble is `0x7`, nothing jOOQ cares
  about).
- [ ] If we ever want a UUIDv7-typed wrapper (e.g. to enforce "only v7 goes into these
  columns"), write a custom `Converter<UUID, UuidV7>` and add a `converter` entry
  alongside the `forcedType`. Not needed today — `UUID.version() == 7` is a one-line
  runtime assertion at the `newCatalogUuid` call site.
- [ ] With UUIDs auto-bound, `setUuid` + the `isPostgresql` flag that guards it can
  die in Phase 9.

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

**Decision: keep the bespoke loop.** Reasons below — written up so future-us
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
