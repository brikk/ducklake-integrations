# DuckLake Write Mode — Open Items

The shipped write surface is summarized in the feature chart in [README](../README.md);
this file tracks what's still open. Reuse strategy when adding to the write path:
lean on Trino's `ParquetWriter` and merge-on-read (`ConnectorMergeSink`) plumbing,
keep DuckLake-specific code limited to catalog semantics and snapshot logic.

Spec context for several items below lives in
[archive/DUCKLAKE_1_0_IMPACT.md](archive/DUCKLAKE_1_0_IMPACT.md) (the DuckLake 1.0 spec impact
reference). Spec issues we filed upstream live in
[archive/REPORT_CROSS_ENGINE_WRITE.md](archive/REPORT_CROSS_ENGINE_WRITE.md).

## Top Priorities

Picked next, in order. Pair this with [TODO-READ-MODE.md § Top Priorities](TODO-READ-MODE.md#top-priorities)
on the read side.

Next up:

1. **Quack/remote-DuckDB catalog backend** — a new DuckLake catalog
   option upstream just added (merged 2026-05-12) that we MUST support
   and test against. See § Quack Catalog Backend (DuckDB RPC) below.
   Cross-cutting concern — affects both read and write paths.
2. Sorted-table writes (PARKED, awaiting a scope decision). The M8 maintenance
   procedures are all DONE (see § M8).

(The `add_files` procedure, bucket partitioning, nested-leaf file
column stats, and per-table `location` have all landed; see § Adopt
Existing Parquet Files (`add_files`), § Bucket Partitioning, § Nested-
Leaf File Column Stats, and § Per-Table Storage Path below.)

## DuckDB-as-Catalog Backend (Local + Quack)

**Status: local-DuckDB backend ready; Quack backend experimental, gated on upstream maturity.**

Two DuckDB-flavoured backend variants are wired alongside the default PostgreSQL
backend, selected via `-Dducklake.test.catalog-backend={POSTGRES,DUCKDB_LOCAL,DUCKDB_QUACK}`
(default POSTGRES). The local-DuckDB variant is the "SQL + type compatibility
gate" for the Quack work — if our jOOQ-generated SQL round-trips against
DuckDB-as-catalog on a local file, then when upstream Quack RPC matures past
its multi-table-query and UPDATE/DELETE limitations the connector will pick up
Quack without a second refactor. We deliberately do *not* compromise our SQL
patterns to work around Quack's current limitations — local DuckDB and PG both
handle the efficient single-query forms.

What landed (2026-05-19):

**Backend selector + fixtures**:
- `DucklakeTestCatalogBackend { POSTGRES, DUCKDB_LOCAL, DUCKDB_QUACK }` plus
  system-property forwarding through Gradle into the test JVM (see
  `jvm/trino-ducklake/build.gradle.kts`).
- `TestingDucklakeLocalDuckDbCatalogFixture` — a managed temp dir; one
  `lake.db` + `data/` subdir per isolated catalog. No Testcontainer required.
- `TestingDucklakeDuckDbQuackCatalogServer` — Testcontainer running a DuckDB CLI
  sidecar with the Quack extension preloaded; Dockerfile + entrypoint at
  `jvm/ducklake-catalog/testFixtures/resources/docker/quack-server/`.
  Out-of-process by construction — required because in-process DuckLake-on-Quack
  + `quack_serve` deadlock on connection-id self-RPC.
**JdbcDucklakeCatalog changes**:
- `Settings.withRenderSchema(false)` — jOOQ codegen runs against PG so generated
  Table refs hard-code `public.`. PG keeps working via its default `search_path`;
  DuckDB-as-catalog (`jdbc:duckdb:/path/lake.db`) needs the prefix dropped so
  bare table refs resolve against the current default schema.
- **Synthetic URL parser** for Quack: `QuackBackedDuckDbCatalogUrl` recognises
  `jdbc:duckdb:quack://host:port[?metadata_catalog=name]`. Token rides in
  `ducklake.catalog.database-password`. `JdbcDucklakeCatalog` swaps the URL for
  a real `jdbc:duckdb:` (in-memory) connection and uses HikariCP's
  `connectionInitSql` to install/load Quack + DuckLake (both core extensions
  as of DuckDB 1.5.3), create the Quack secret, ATTACH `ducklake:quack:...` with a unique
  `METADATA_CATALOG` name, and `USE <metadata_catalog>.main`. Bare references
  to `ducklake_*` then resolve via the metadata-catalog sibling that
  DuckLake-on-Quack publishes alongside the user catalog.
- **SQL kept efficient**: the same-table-multi-scan in `attemptWriteTransaction`
  (`SELECT … WHERE id = (SELECT max(id) FROM same_table)`) is kept as a single
  query. PG and local DuckDB run it efficiently; Quack rejects it today, but
  we don't compromise the SQL for that — Quack tests skip.

**Smoke tests**:
- `TestQuackJdbcProbeTest` — bare Quack RPC + DuckLake-on-Quack subprocess
  round-trip; no Docker required (diagnostic).
- `TestDuckDbQuackCatalogServerSmoke` — containerized DuckLake-on-Quack
  round-trip through the test fixture.
- `TestJdbcDucklakeCatalogOnQuackSmoke` — `JdbcDucklakeCatalog` against the
  Quack fixture; both `listSchemas` AND `createSchemaCommitsAndListSchemasSeesIt`
  are live `@Test`s and PASS (re-verified 2026-06-24 on amd64). The earlier
  `@Disabled`-pending-multi-scan note is stale — the `createSchema` commit path
  round-trips against Quack today.
- `TestJdbcDucklakeCatalogOnLocalDuckDbSmoke` — full schema + table CRUD round
  trip against a local DuckDB `.db` file, including `dropTable`/`dropSchema`
  (which Quack can't do yet). This is the "SQL/type-compat gate" for Quack —
  if it stays green, Quack should Just Work once upstream lifts its
  limitations.
- `TestDucklakeBackendDispatchSmoke` — end-to-end Trino connector against the
  selected backend; runs unmodified under POSTGRES and DUCKDB_LOCAL, skips
  cleanly under DUCKDB_QUACK with a documented assumption message.

What's already validated end-to-end on DUCKDB_LOCAL:

- Schema CRUD (createSchema / listSchemas / dropSchema)
- Table CRUD (createTable with mixed types / listTables / getTable / dropTable)
- Trino CREATE SCHEMA / CREATE TABLE / INSERT / SELECT / DROP TABLE round trip
- jOOQ + DuckDB JDBC type round trip for INTEGER / VARCHAR / BOOLEAN / DATE
- HikariCP pool against a single `.db` file, multiple connections sharing one
  `DatabaseInstance` inside the JVM

The catalog-lib full PG suite (30 classes, 0 failures) keeps passing alongside,
confirming nothing in the DuckDB-friendly changes (`renderSchema(false)`, URL
parser dispatch, fixture additions) regresses the PG path.

What's blocked on upstream Quack:

- **Multi-table queries**: Quack RPC rejects any query that scans more than
  one remote table — both same-table multi-scan (subqueries on the same table)
  and cross-table JOINs (e.g. `ducklake_partition_info` ⋈
  `ducklake_partition_column` during Trino plan building) fail with
  `Not implemented Error: Multiple streaming scans or streaming scans + CTAS
  / insert in the same query are not currently supported`. JdbcDucklakeCatalog's
  read path uses JOINs throughout (partition info, column lineage, file lists,
  snapshot lookups), so virtually every Trino query against a Quack-backed
  catalog blocks on this.
- **UPDATE / DELETE on remote ducklake_* tables**: DuckLake-on-Quack exposes
  the metadata catalog as a *read-only* sibling — UPDATE fails with
  `Binder Error: Can only update base table` and DELETE with the analogous
  delete error. Raw Quack ATTACH (no `ducklake:` prefix) exhibits the same
  restriction. This blocks every metadata mutation that end-snapshots existing
  rows: `dropTable`, `dropSchema`, schema evolution, compaction, expire-snapshots,
  the entire `attemptWriteTransaction` end-snapshot path for delete commits, etc.
- **`SHOW DATABASES` / `duckdb_databases()` on Quack-attached remotes** error
  with `Not implemented Error: InMemory not implemented yet`. Surfaces in
  catalog-introspection code paths and any test that does ad-hoc topology
  queries. Workaround: query the specific catalog name directly (`SELECT FROM
  smoke_meta.main.ducklake_*`).

What works under DUCKDB_QUACK today:

- Single-table reads against `ducklake_*` metadata (`listSchemas`)
- INSERT-only metadata writes that don't end-snapshot existing rows

How to revisit when upstream Quack matures:

- Watch `duckdb/ducklake` `test/configs/quack.json` for shrinking FIXME skip
  lists. When "Multiple streaming scans" disappears, our read path becomes
  usable. Same for "Can only update / delete base table" — that one likely
  needs an upstream Quack-extension change rather than a DuckLake change.
- When the Quack-side blockers lift, lift the `assumeTrue` skip in
  `TestDucklakeBackendDispatchSmoke` and extend
  `DucklakeCatalogGenerator.generateIsolatedDuckDbQuackCatalog` with the
  full test-data bootstrap so the cross-engine suite can run under it. (The
  `createSchema` smoke test is already enabled and passing — see the smoke-test
  list above; no `@Disabled` to lift there.)

(Pre-2026-05-19 scoping notes follow.)

Upstream `duckdb/ducklake#1151` (merged 2026-05-12, title "Experimental
Support For Quack (DuckDB RPC Client/Server Protocol) as a catalog for
Ducklake") added a new metadata-manager backend that lets a DuckLake
catalog live in a remote DuckDB accessed over the Quack RPC protocol.
Files added in that PR:

- `src/include/metadata_manager/quack_metadata_manager.hpp`
- `src/metadata_manager/quack_metadata_manager.cpp`
- `test/configs/quack.json` (test config wiring)
- `scripts/run_quack_tests.py` (runner)

This becomes a first-class catalog backend alongside DuckDB-local /
SQLite / Postgres / MySQL, and is the natural deployment shape for
"shared DuckDB-as-catalog without giving every reader filesystem access
to the .db file." We need to be in front of it, not chasing it.

**Target architecture for our JVM stack**:

```
Trino → DuckLake plugin → JDBC → local DuckDB (driver) ──Quack RPC──▶ remote DuckDB (catalog)
```

The local DuckDB acts as a Quack *client*; our existing JDBC-based
catalog plumbing (`JdbcDucklakeCatalog`) keeps working unchanged because
from Java's POV it still talks JDBC to a DuckDB driver — just one
configured to delegate storage to a remote DuckDB over Quack. This is
the same "DuckDB as a thin client" pattern DuckDB itself uses for the
Quack tests.

Scope (read + write — this is cross-cutting):

- [ ] Verify the DuckDB JDBC driver exposes the connection-string knobs
  needed to point a local DuckDB at a Quack server (host/port/auth/TLS).
  Pin the minimum driver version that supports it.
- [ ] Confirm `JdbcDucklakeCatalog` works unmodified against the
  Quack-backed local DuckDB. If the catalog-schema bootstrap or any of
  our SPI queries break under the remote-delegation surface, file those
  as bugs and fix close to the catalog layer (not in the Trino plugin).
- [ ] Add a Quack profile to the cross-engine compatibility harness
  (`TestDucklakeCrossEngineCompatibility`): same scenarios as the
  existing DuckDB run, but with both sides (Trino-side local DuckDB and
  the DuckDB CLI write fixture) pointed at a shared remote-DuckDB
  catalog over Quack. This is the test that proves "Trino's catalog
  writes and DuckDB's catalog writes converge under Quack".
- [ ] Snapshot-lineage / concurrent-writer test under Quack: two Trino
  sessions plus one direct DuckDB writer, all hitting the same remote
  catalog. The PR's own concern was correctness of the catalog manager
  under RPC; our concern is `ensureSnapshotLineageUnchanged()` still
  fires correctly when the catalog reads/writes go through Quack
  round-trips.
- [ ] Document the Quack catalog config in the connector README
  alongside the existing JDBC / Postgres examples. Include the
  delegating-DuckDB JDBC URL pattern and the remote-DuckDB endpoint
  config.
- [ ] CI: stand up a remote-DuckDB Quack server in Testcontainers (or
  a shell-spawned DuckDB CLI with Quack server enabled) and run a
  reduced regression suite against it on every PR. The full
  cross-engine matrix can run nightly if startup cost is too high for
  per-PR.

Open questions to resolve before scoping the work:

- Does the upstream DuckDB JDBC driver already ship with Quack-client
  support enabled, or does it require an explicit extension load
  (`LOAD quack`) the way `httpfs` does? If extension-load, our catalog
  bootstrap needs the right `INSTALL` / `LOAD` sequence.
- Authentication model: token, TLS client cert, or neither yet? The PR
  is labeled "Experimental" — auth may not be finalized. We need to
  pick a posture that doesn't regress when upstream tightens it.
- Wire-protocol stability guarantees. "Experimental" in the PR title
  means we should pin a specific DuckDB version per release and not
  assume forward compatibility across DuckDB minor bumps.

Why this is "MUST support" and not optional: Quack is the obvious answer
to the question "how do I share one DuckDB-format DuckLake catalog
between multiple engines/clusters without NFS-mounting the .db file."
Every DuckDB-as-catalog deployment that goes past a single host will
end up wanting this. If we ship a Trino connector that can't read a
Quack-backed catalog, we cede the multi-engine-shared-catalog story
back to pg_ducklake (Postgres) and to native DuckDB. Owning Quack
parity keeps us aligned with the reference implementation's deployment
shape, not just its on-disk format.

Reference: <https://github.com/duckdb/ducklake/pull/1151>

## Per-Table Storage Path (`location` Table Property)

- [x] **`location` table property.** Landed 2026-05-20.
  - `TableLocationSpec(path, isRelative)` added to `ducklake-catalog`;
    `DucklakeCatalog.createTable` signature carries an
    `Optional<TableLocationSpec>` so callers (Trino plugin today, future
    Doris/Spark) opt in per-table. Empty falls back to the historical
    `<tableName>/` relative default.
  - `DucklakeTableProperties.LOCATION_PROPERTY = "location"` (matches
    Hive/Iceberg/Delta convention) with `getLocation` doing scheme
    detection and trailing-slash normalization, and a validator that
    rejects blank input and `..` path-traversal segments (both `/` and
    `\` separators).
  - URI-scheme prefix (matched by `^[a-zA-Z][a-zA-Z0-9+\-.]*://`) →
    `path_is_relative=false`; otherwise relative. Covers
    `s3://`, `gs://`, `file://`, `abfss://`, `hdfs://`, `gcs://`, etc.
    without enumeration.
  - `DucklakeMetadata.createTable` and `beginCreateTable` (CTAS) both
    plumb the property through. `JdbcDucklakeCatalog.createTable` uses
    the provided value (or default) when writing `ducklake_table.path` /
    `path_is_relative`. The existing `DucklakePathResolver` handles
    relative-vs-absolute resolution at INSERT time unchanged.
  - Pinned by:
    - `TestDucklakeTablePropertiesLocation` (10 cases — scheme
      detection per backend, trailing-slash normalization, missing /
      blank, validator rejection of `..` traversal, segment-internal
      `..` allowed).
    - `TestDucklakeCrossEngineTableLocation` (6 cases — relative
      round-trip including Trino INSERT + DuckDB read + on-disk parquet
      assertion, absolute s3-style catalog row, trailing-slash
      normalization, traversal rejection at CREATE TABLE, default
      `<tableName>/` fallback unchanged).

Out of scope for v1: per-table data inlining row limit, per-table sort
keys, and other knobs that route through `ducklake_set_option`.
pg_ducklake deferred those too (#199 PR notes) because the new table is
transaction-local inside the create-table path. Same reasoning for us.

## Adopt Existing Parquet Files (`add_files`)

DuckLake exposes `CALL ducklake_add_files(table_name, files => [...], ...)`
to register pre-existing parquet files as data files of a DuckLake table.
The procedure reads the footer of each file (record count, footer offset,
column min/max/null stats) and inserts matching `ducklake_data_file` +
`ducklake_file_column_stats` rows. No file rewriting — the bytes already
on storage become live data of the table. Upstream tests this exhaustively
under `vendor/pg_ducklake/third_party/ducklake/test/sql/add_files/` (33
files; footer validation, statistics recovery, column-reordering, malformed
schemas).

**Why this is high value**: closes the most-asked-for catalog migration
path ("I have existing parquet on S3 — how do I make it a DuckLake table?")
without forcing a full rewrite. The plumbing also lays groundwork for
future maintenance ops (`rewrite_data_files`, `merge_adjacent_files`) that
generate new parquet files and need a similar metadata-insert path.

**Engine-agnostic placement**: most of the work lands in
`jvm/ducklake-catalog`. The Trino plugin exposes the procedure surface.

- [x] **Footer reader** — landed in
  `DucklakeAddFilesProcedure.buildFragment` (uses Trino's
  `MetadataReader.readFooter` + a thrift adapter to reuse
  `DucklakeStatsExtractor`).
- [x] **Catalog write path** — `DucklakeCatalog.commitAddFiles` /
  `JdbcDucklakeCatalog.commitAddFiles` delegate to
  `applyInsertFragments` and record `WriteChange.InsertedIntoTable`, so
  the existing conflict matrix covers `add_files × dropTable / × alter`
  for free (pinned by `TestConcurrentAddFilesVsDropColumn` /
  `TestConcurrentAddFilesVsDropTable`).
- [x] **Schema validation** — `DucklakeAddFilesNameMapper` walks each
  field by name, validating types recursively (struct / list / map).
  Top-level missing columns honored by `allow_missing`; extra columns
  honored by `ignore_extra_columns`.
- [x] **Trino procedure surface** — `DucklakeAddFilesProcedure` exposes
  `CALL ducklake.system.add_files(schema_name, table_name, files,
  allow_missing, ignore_extra_columns, hive_partitioning)`. Iterates
  files, builds fragments, single `commitAddFiles` call per invocation.
- [x] **Tests** — `TestDucklakeAddFiles` (28 methods),
  `TestDucklakeAddFilesCrossEngine`, and the concurrent-conflict tests
  cover round-trip, column reorder, missing/extra columns, mapping_id
  dedup, hive partitioning (IDENTITY), and `add_files × dropColumn /
  dropTable` conflicts.

**References**:
- Spec: upstream's `data_inlining.md` and `add_files`-related docs on
  the website.
- Upstream tests: `vendor/pg_ducklake/third_party/ducklake/test/sql/add_files/`
- Procedure surface in Trino: existing connectors expose
  `system.register_table()`-style procedures via
  `io.trino.spi.procedure.Procedure`.

## `add_files` Follow-Ups

- [ ] **`allow_missing` recurses into STRUCT fields.** Upstream's
  `add_files_missing_fields.test` exercises a `ROW(a INT)` parquet against a
  `ROW(a INT, b INT)` table column. Our `DucklakeAddFilesNameMapper.mapStruct`
  rejects unconditionally when a child field is absent — only top-level columns
  honor the `allow_missing` flag today. Port the same semantics for struct
  children (return a name-map entry without a child for the missing field, and
  the reader produces NULL via the existing missing-column path). ~half-day.
- [x] **Compute `footer_size` for `add_files`-registered files.** Landed
  2026-05-12. `DucklakeAddFilesProcedure.readFooterLengthFromPostScript`
  reads the last 8 bytes via `ParquetDataSource.readTail(8)`, validates
  the trailing `PAR1`/`PARE` magic, and decodes the 4-byte little-endian
  Thrift FileMetaData length. The value is stored in the
  `DucklakeWriteFragment.footerSize` field (and persisted to
  `ducklake_data_file.footer_size`), letting subsequent reads through
  `FooterPrefetchingParquetDataSource` skip the blind 48 KB tail read.
  Best-effort: IO error / short read / non-magic trailer falls back to 0
  (the read path tolerates 0 by doing its default blind read).
  Pinned by `TestDucklakeAddFiles.testAddFilesPopulatesFooterSize`.

## Nested-Leaf File Column Stats

- [x] **Emit `ducklake_file_column_stats` rows for nested-leaf columns.**
  Landed 2026-05-13. `DucklakeStatsExtractor` no longer skips
  ARRAY/MAP/ROW types — its input changed from `List<DucklakeColumnHandle>`
  to a flat `List<LeafStatsTarget>` whose entries pair each parquet leaf
  with the matching catalog field_id. Two builders feed it:
  - `DucklakeStatsLeafProjector.projectFromCatalogTree(handles, allCatalogColumns)`
    for the INSERT / CTAS / MERGE writer path, walking Trino types in
    declaration order and consulting the catalog's parent→child tree.
  - `DucklakeAddFilesNameMapper` now produces `leafStatsTargets` during
    the same parquet-schema walk used to build the name map. Skipped /
    hive-overridden parquet leaves advance the parquet-column-index
    counter (via `countParquetLeaves`) so emitted indices stay aligned
    with `RowGroup.columns`.

  `extractStats` now looks up each row-group chunk by the explicit
  `parquetColumnIndex` on its target, which also fixes a pre-existing
  latent bug where flat columns following any nested column got
  misaligned stats.

  Pinned by:
  - `TestDucklakeStatsLeafProjector` (8 cases — flat / ROW / ARRAY / MAP
    / ARRAY<ROW> / nested ROW + error path).
  - `TestDucklakeStatsExtractor` (synthetic thrift FileMetaData verifies
    field_id keying, struct leaves, non-contiguous indices, multi-row-group
    merge).
  - `TestDucklakeNestedLeafStats` (end-to-end INSERT + add_files: asserts
    one stats row per leaf, dotted-path lookup via `ducklake_column` tree,
    correct min/max strings; covers ROW, ARRAY, MAP, ARRAY<ROW>, nested
    ROW, and add_files with a ROW column).

## Bucket Partitioning

- [x] **Bucket partitioning (full implementation).** Landed 2026-05-12.
  - `DucklakePartitionTransform` gained a `BUCKET` variant plus
    `parseCatalogTransform(String)` / `toCatalogString(OptionalInt)` helpers
    that round-trip the upstream `bucket(N)` text form stored in
    `ducklake_partition_column.transform`.
  - `PartitionFieldSpec` and `DucklakePartitionField` carry an
    `OptionalInt arity` (empty for non-bucket transforms, populated with
    the N for BUCKET).
  - `DucklakePartitionComputer.computeBucket` implements
    `(murmur3_32(v) & INT_MAX) % N` via Guava's `Hashing.murmur3_32_fixed()`
    (Iceberg-compatible: int widened to long, UTF-8 for VARCHAR, raw
    bytes for VARBINARY/UUID, μs for TIMESTAMP/TIMESTAMPTZ). REAL /
    DOUBLE / BOOLEAN are rejected per Iceberg spec.
  - `DucklakeTableProperties` accepts `bucket(N, col)` syntax
    (`partitioned_by = ARRAY['bucket(4, name)']`).
  - `DucklakePagePartitioner` routes BUCKET rows through the INTEGER
    page-indexer path (same as temporal transforms).
  - `DucklakeBucketPartitionMatcher` prunes files for equality
    predicates by hashing the predicate constant and comparing against
    the file's stored bucket index; ranges aren't pruned (bucketing
    scrambles ordering).
  - Unit tests in `TestDucklakePartitionComputer` cover Iceberg spec
    reference values (int 34 → bucket 79, "iceberg" → 89, date
    2017-11-16 → 26), determinism, non-negativity, and the unsupported-
    type guard. Integration tests in `TestDucklakePartitionedWrite`
    cover round-trip insert+read on VARCHAR and BIGINT columns, plus
    equality-predicate pruning.

## Sorted Table Writes — ⏸ PARKED (awaiting scope decision, 2026-06-29)

- [ ] **Apply table sort spec during Parquet writes** in `DucklakePageSink`. Today the spec is read
  + exposed to the planner only (`DucklakeSortPropertyMapper` → `SortingProperty`); nothing sorts on
  write. Central write-path change (handle + `beginInsert`/`beginCreateTable` + sink provider + core
  sink). Scoping done — see TODO-jayson-special-list.md § F7 for the full notes. Forks: in-memory
  `io.trino.spi.PageSorter` (injectable) vs spill `io.trino.plugin.hive.SortingFileWriter`;
  unpartitioned-only vs partitioned+sorted (OOM); no `sorted_by` table property exists yet (so only
  DuckDB-sorted tables + Trino INSERT benefit). **Recommended when unparked:** gated in-memory
  PageSorter, parquet + unpartitioned + sort-spec-present only (existing writes unchanged), per-file
  sorted output. See [archive/DUCKLAKE_1_0_IMPACT.md § Sorted Tables](archive/DUCKLAKE_1_0_IMPACT.md#2-sorted-tables).

## Puffin deletion-vector writes — ✅ DONE 2026-06-29

- [x] `write_deletion_vectors` session property (default off) → `DucklakeMergeSink` writes tombstones
  as DuckLake `.puffin` deletion-vector files (`DucklakePuffinDeleteWriter`); `DucklakeDeleteFragment`
  carries `format`, catalog persists `ducklake_delete_file.format`; cross-engine round-trip tested
  (`TestDucklakeTrinoPuffinDeleteWrite`). Completes the puffin read/write symmetry.

## Commit-context session props — ❌ recommended NON-GOAL

- Snapshot author/commit-message would need columns NOT in the DuckLake spec on `ducklake_snapshot`
  → cross-engine divergence risk (DuckDB owns that table). Don't add unless upstream defines them.

## Schema Evolution Gaps

- [ ] `ALTER TABLE SET TYPE` (type promotion)
- [x] `ALTER TABLE ADD/DROP FIELD` (nested struct field manipulation) — DONE (parquet self-heals;
  non-parquet via per-file struct_pack reshaping; e2e on duckdb + vortex). See § F9/nested in
  TODO-jayson-special-list.md and DESIGN-nested-field-evolution.md.

## `default_value_dialect = 'trino'` for User-Defined DEFAULTs

When user-defined `DEFAULT` expressions ship for Trino-written tables, write the
literal `'trino'` to `ducklake_column.default_value_dialect` (not `'brikk-trino'`
— the dialect names the SQL syntax, which is plain Trino SQL; brikk metadata
lives only in our view rows). Today every column gets `default_value = 'NULL'`
(the "no default" sentinel) and `default_value_dialect` SQL NULL — safe and
honest, since the field is informational and only meaningful when there's a real
literal or expression to interpret. Call sites:
`JdbcDucklakeCatalog.insertColumnTree`, `JdbcDucklakeCatalog.renameColumn`.
Pinned today by
`TestDucklakeCrossEngineCatalogMetadata.testDuckdbReadsTrinoTableWithNullDefaultValueDialect`
(asserts the SQL NULL contract). See [archive/REPORT_CROSS_ENGINE_WRITE.md] Issue 1.

## Commit Context (DuckDB `set_commit_message` equivalent)

Surface user-supplied author/message/extra-info fields onto
`ducklake_snapshot.{author, commit_message, commit_extra_info}`.

Session properties:
- `ducklake.commit_author`
- `ducklake.commit_message`
- `ducklake.commit_extra_info`

Optional convenience procedures:
- `CALL ducklake.system.set_commit_context(author => 'Pedro', message => 'Inserting myself', extra_info => '{"foo":7}')`
- `CALL ducklake.system.clear_commit_context()`

Source: pg_ducklake exposes `set_commit_message()` and it's a commonly-wanted
feature.

## Logical Conflict Checking + Concurrency Test Coverage

Tracked together because the same engineering session should land both the
matrix and the harness that exercises it. Engine-agnostic — all changes land in
`jvm/ducklake-catalog`, reusable by Doris/Spark/etc. The Trino plugin already
just calls `commitInsert` / `commitDelete` / `commitMerge` / DDL methods on the
catalog and translates the resulting exceptions, so no plugin work is required
beyond confirming the existing `translateCatalogExceptions` wrapper still
maps the new conflict messages cleanly.

**Status (2026-05-10): Steps 1–4 landed.** Conflict-detection now matches
upstream's commit-time semantics. The summary below preserves the original
plan; "landed" notes inline call out where the implementation deviates.
Earlier framing of Step 4 as "relax the strict lineage check" was wrong:
review of the vendored upstream source
(`vendor/pg_ducklake/third_party/ducklake/src/storage/ducklake_transaction.cpp:2460–2477`)
showed our `ensureSnapshotLineageUnchanged` is functionally equivalent
to upstream's `ducklake_snapshot.snapshot_id` PK fence on attempt 1.
The actual remaining gap is the matrix upstream runs on retry, which
catches dueling-name commits the state-based check in Step 3 misses.

Files added in Steps 1–3: `ConcurrentWriterHarness`, `WriteChange`,
`LogicalConflictCheck`, `LogicalConflictException`; new tests
`TestConcurrentInsertSameTable`, `TestConcurrentInsertDifferentTables`,
`TestConcurrentInsertVsDropColumn`, `TestConcurrentInsertVsDropTable`,
`TestConcurrentAlteredTableVsAlteredTable`. The
`TransactionConflictException.retryable()` flag is what
`WriteTransactionRetry` consults to decide whether to retry —
`LogicalConflictException` returns `false` and short-circuits the retry
loop.

**Current behavior to be aware of**: `JdbcDucklakeCatalog.attemptWriteTransaction`
(`JdbcDucklakeCatalog.java:905`) calls `ensureSnapshotLineageUnchanged`
(`JdbcDucklakeCatalog.java:985`) which throws `TransactionConflictException`
on **any** intervening commit. `WriteTransactionRetry` then retries with
exponential backoff (`MAX_RETRY_COUNT` attempts) and the action re-runs against
a fresh snapshot read. So today's gap is *not* "concurrent commits silently
land" — it's "the retry's action re-runs with stale per-call arguments
(`tableId`, column-stats column_ids, delete-target data_file_ids) that may
reference entities the winner committed away during the original window."

The acceptance scenario:
1. T2 calls `commitInsert(tableId=42, fragments[col_stats column_id=99])`
2. T1 commits `dropColumn(42, 99)`
3. T2's first attempt: lineage check fails → retry
4. T2's retry's action runs to completion (it doesn't re-validate the
   `column_id`s in the fragment) → `applyInsertFragments`
   (`JdbcDucklakeCatalog.java:1621`) inserts `ducklake_file_column_stats` rows
   pointing at column 99, which is end-snapshotted → catalog corruption.

### Step 1 — Concurrency test harness + pinning tests

- [x] **Extract the latch-pause pattern from
  `TestJdbcDucklakeCatalogConcurrentCommit`** (`jvm/ducklake-catalog/test/.../TestJdbcDucklakeCatalogConcurrentCommit.java`)
  into a reusable fixture (e.g. `ConcurrentWriterHarness`) exposing
  `parkOneAttempt(threadName)` and `runConcurrently(winnerOp, loserOp)`. Uses
  the existing `JdbcDucklakeCatalog.beforeWriteTransactionAction` test seam
  (`JdbcDucklakeCatalog.java:879`). Refactor the existing
  `concurrentCommitTriggersRetryAndBothSchemasLand` test to use the fixture so
  the abstraction is proven against the live behavior it already validates.
- [x] **`TestConcurrentInsertSameTable.concurrentInsertsBothCommit`**: two
  threads INSERT into the same table; loser parks before mutation, winner
  commits, loser releases, retries, and commits. Pin today's behavior. Source:
  pg_ducklake's `pg_isolation` spec for same-table concurrent INSERTs.
- [x] **`TestConcurrentInsertDifferentTables.concurrentInsertsBothCommit`**:
  two threads, each inserting into a different table; both commits must
  succeed (loser still retries because lineage advanced — that's fine; it
  must produce a clean second snapshot). Catches snapshot-id cross-talk.
  Source: pg_ducklake's `concurrent_cross_table_writes` spec.

  Both tests must pass against unmodified production code — they pin behavior,
  they don't drive the matrix work.

### Step 2 — Structured change tracking inside `DucklakeWriteTransaction`

- [x] **Add a typed `WriteChange` channel alongside today's `addChange(String)`.**
  `DucklakeWriteTransaction.changes` is a `List<String>` formatted for
  `ducklake_snapshot_changes.changes_made` (see `formatChangesMade` at
  `JdbcDucklakeCatalog.java:817`); great for human readability, awkward for a
  conflict matrix. Add a parallel `List<WriteChange>` where `WriteChange` is a
  sealed interface with variants:
  - `CreatedSchema(String name)`
  - `DroppedSchema(long schemaId)`
  - `CreatedTable(String schemaName, String tableName)`
  - `DroppedTable(long tableId)`
  - `AlteredTable(long tableId)` — covers add/drop/rename column
  - `InsertedIntoTable(long tableId, Set<Long> referencedColumnIds)` — column ids
    drawn from `DucklakeWriteFragment.columnStats()`
  - `DeletedFromTable(long tableId, Set<Long> referencedDataFileIds)` — file ids
    drawn from `DucklakeDeleteFragment`
  - `CreatedView(String schemaName, String viewName)`
  - `DroppedView(long viewId)`
  - `AlteredView(long viewId)` — covers replace/rename
- [x] Convert the `addChange(...)` call sites in `JdbcDucklakeCatalog`
  (`addColumn`, `dropColumn`, `renameColumn`, `commitInsert`, `commitDelete`,
  `commitMerge`, `dropTable`, `dropSchema`, `createSchema`, `createTable`,
  `createView`, `dropView`, `replaceViewMetadata`, `renameView`) to record
  the typed change *and* the existing string in one helper —
  `tx.recordChange(WriteChange.alteredTable(tableId))` etc. This is mechanical
  but touches every write path; do it as one PR.

### Step 3 — `LogicalConflictCheck` + acceptance test

- [x] **Add `LogicalConflictCheck` invoked between `action.execute(tx)` and
  `insertSnapshotRow` in `attemptWriteTransaction`** (insertion point:
  `JdbcDucklakeCatalog.java:938`–`944`, after the existing
  `ensureSnapshotLineageUnchanged` call). For each typed `WriteChange` the
  action recorded, query the catalog at `tx.getCurrentSnapshotId()` to confirm
  referenced entities are still in the expected state:
  - `InsertedIntoTable(tableId, columnIds)` → assert `tableId` is active
    AND every `columnId` in `columnIds` is active at `currentSnapshotId`
    (use the `activeAt` helper already in `JdbcDucklakeCatalog`).
  - `DeletedFromTable(tableId, dataFileIds)` → assert `tableId` is active
    AND every `dataFileId` is active.
  - `AlteredTable(tableId)` / `DroppedTable(tableId)` / column ops →
    assert `tableId` is active.
  - `Created*` ops are PK-protected on the underlying catalog row INSERTs
    today, so they need no extra check (a duplicate-name race surfaces as
    `isMetadataPrimaryKeyConflict` in the existing exception path).
- [x] On mismatch, throw `TransactionConflictException` reusing the existing
  intervening-changes summary helper `getInterveningChangesSummary`
  (`JdbcDucklakeCatalog.java:1030`) and adding the specific stale entity to
  the message (e.g. `"column 99 was end-snapshotted (likely by an intervening
  ALTER TABLE DROP COLUMN) during INSERT against table 42"`). The thrown
  exception must propagate cleanly through `WriteTransactionRetry` —
  `hasTransactionConflict` (`JdbcDucklakeCatalog.java:1069`) already detects
  it. **Decide explicitly per case whether the new conflict should be
  retryable**: stale-column INSERT is *not* retryable (the fragment metadata
  is already wrong; retrying re-fails). If non-retryable, the check should
  rethrow as a distinct subclass or carry a flag so the retry loop bails
  out — otherwise we burn `MAX_RETRY_COUNT` attempts on a guaranteed-fail
  scenario. See the `Sleeper`-keyed retry policy in
  `WriteTransactionRetry.java:46`.
- [x] **`TestConcurrentInsertVsDropColumn`**: T2 plans a `commitInsert` with
  column-stats for column 99; harness parks T2's first attempt at
  `beforeWriteTransactionAction`; T1 commits `dropColumn(table, 99)`; T2
  releases, retries (lineage check fires on first attempt), retry's action
  runs to completion, **logical check** rejects the commit with a
  message that names column 99 specifically. This is the gold acceptance
  test — it's the case lineage-only checking misses.
- [x] **`TestConcurrentInsertVsDropTable`**: same pattern with a
  `dropTable`. Confirms `tableId`-validity branch of the matrix.
- [x] **`TestConcurrentAlteredTableVsAlteredTable`**: two concurrent
  `addColumn` calls on the same table. Both should not silently land —
  expected behavior is that the loser's retry re-runs and the second
  `addColumn` succeeds against the post-winner schema (column_order
  collision is the failure mode if it doesn't). Pin whichever behavior is
  correct after Step 3 lands.

### Step 4 — Port upstream's change-vs-change conflict matrix — DONE (2026-05-10)

**Landed**: `InterveningChanges` (parser + aggregator), `ConflictMatrix`
(direct port of `ducklake_transaction.cpp:1184–1314`), wiring in
`attemptWriteTransaction` (`runConflictMatrix` + finer-grained
delete-vs-delete file-overlap query mirroring upstream's
`GetFilesDeletedOrDroppedAfterSnapshot` path at upstream `:1259–1283`).
The matrix runs only when intervening commits exist (i.e. on retry,
matching upstream's `i > 0` gate). `WriteChange.{CreatedTable,
CreatedView, DroppedSchema}` gained additional fields (schemaId / name)
not serialized to changes_made but needed by the matrix. Acceptance
tests: `TestConcurrentCreateSchemaSameName`,
`TestConcurrentCreateTableSameName`,
`TestConcurrentCreateTableInDroppedSchema`,
`TestConcurrentDeleteVsDelete`. `TestInterveningChangesParser` covers
quoting / escape / all-upstream-kinds round-trip. The behavior pinned
in Step 3's `TestConcurrentAlteredTableVsAlteredTable` was flipped to
match upstream's `altered_table × altered_table` conflict policy
(`:1307–1310`).

After reading the upstream source (vendored at
`vendor/pg_ducklake/third_party/ducklake/src/storage/`), my earlier framing of
this step ("relax the strict lineage check") was wrong. The remaining gap to
upstream parity is the **change-vs-change matrix**, not the lineage fence.

**Upstream's actual flow** (`ducklake_transaction.cpp:2460–2477`):
```cpp
for (idx_t i = 0; i < max_retry_count + 1; i++) {
    can_retry = false;
    if (i > 0) {
        // we failed our first commit due to another transaction committing
        // retry - but first check for conflicts
        commit_stats_snapshot = CheckForConflicts(transaction_snapshot, transaction_changes);
    } else {
        commit_stats_snapshot.snapshot = GetSnapshot();
    }
    commit_snapshot.snapshot_id++;          // bump and let PG fence on PK
    ...
    can_retry = true;                       // matrix passed — INSERT below may PK-collide
```

- **First attempt**: just bump `snapshot_id` and INSERT. The
  `ducklake_snapshot` PK collision is the fence; PG rejects on duplicate.
- **Retry attempts**: parse intervening commits' `changes_made` text into a
  typed `SnapshotChangeInformation` struct, run the matrix in
  `CheckForConflicts` (`ducklake_transaction.cpp:1184–1314`). If matrix
  throws, `can_retry` is still `false` (it only flips true after the matrix
  passes), so the retry loop bails — that's their non-retryable mechanism.

Our `ensureSnapshotLineageUnchanged` is **functionally equivalent** to
upstream's PK-on-snapshot-id fence — both fail attempt 1 when intervening
commits exist. There's nothing to "relax". The only difference is: upstream
runs the matrix on retry to catch interleavings where the *retry's action*
itself would commit semantically incompatible state.

**Why the matrix is essential** — the upstream metadata DDL
(`ducklake_metadata_manager.cpp:196–217`) deliberately omits PKs on
`ducklake_table.table_id`, `ducklake_view.view_id`, and stat tables, because
rows are snapshot-versioned (same `table_id` recurs across rename / column
ops). There's no DDL constraint that would catch dueling
`createSchema("foo")` / `createSchema("foo")` or
`createTable("S", "T")` / `createTable("S", "T")`. The matrix is the
**only** safety net. We currently lack it.

**Pre-existing bug exposed by the matrix audit**: two concurrent
`createSchema(name)` (or `createTable(schema, name)`) calls with the same
name today both land active rows. The lineage check fences attempt 1 →
retry → on retry our `createSchema` blindly INSERTs without re-checking
for an active conflicting name. State-based `LogicalConflictCheck` skips
`Created*` variants. `PublicDbKeys.java` confirms only single-column
PKs — no `(schema_id, table_name)` UNIQUE. Upstream catches this in the
matrix at `ducklake_transaction.cpp:1212–1242`; we don't.

**Step 4 work items** (port mechanically — upstream is the spec):

- [x] **Add `ChangesMadeParser`** — inverse of `WriteChange.formatChangesMade`.
  Parse the `ducklake_snapshot_changes.changes_made` text from intervening
  snapshots back into `List<WriteChange>`. Upstream's `ParseChangesList` /
  `ParseChangeEntry` / `ParseChangeValue` (in
  `ducklake_transaction_changes.cpp:34–129`) is the spec; track quoting
  with a 1-pass state machine, splitting on unquoted commas. ~80 lines +
  parser tests covering quote-escaping and embedded-comma cases (mirror our
  existing format tests in `TestJdbcDucklakeCatalogChangesMadeFormat`).
- [x] **Aggregate intervening changes** into a structured equivalent of
  upstream's `SnapshotChangeInformation` (sets keyed by `(schema_id,
  name)` for create-by-name forms, sets of `tableId` for the rest). Add
  alongside `WriteChange` — call it e.g. `InterveningChanges`.
- [x] **Port `CheckForConflicts(my_changes, intervening_changes)`** — direct
  translation of `ducklake_transaction.cpp:1184–1314`. Each `for` loop in
  upstream maps to one Java loop checking the same set membership. Throw
  `LogicalConflictException` (already exists, already non-retryable) with
  upstream's error-message shape ("attempting to X — but another
  transaction Y'd it"). The macro pairs to translate, in order:
  - `dropped_tables × dropped_tables`
  - `dropped_views × dropped_views`
  - `dropped_scalar_macros × dropped_scalar_macros` (skip — we don't
    support macros yet)
  - `dropped_table_macros × dropped_table_macros` (skip — same)
  - `dropped_schemas × dropped_schemas`, `dropped_schemas.name ×
    created_tables[in that schema]`
  - `created_schemas × created_schemas` (the dueling-name bug)
  - `created_tables × dropped_schemas`, `created_tables × created_tables`
    (the other dueling-name bug)
  - `tables_inserted_into × dropped_tables`, `× altered_tables`
  - `tables_deleted_from × dropped_tables`, `× altered_tables`,
    `× tables_merge_adjacent`, `× tables_rewrite_delete`
  - `tables_deleted_from × tables_deleted_from` — finer-grained: only
    conflict if the same `data_file_id` is in both (upstream calls
    `metadata_manager->GetFilesDeletedOrDroppedAfterSnapshot` and
    intersects). We already capture `referencedDataFileIds` on
    `WriteChange.DeletedFromTable`, so this is a Set intersection.
  - `altered_tables × dropped_tables`, `× altered_tables`
  - `altered_views × altered_views`
  - Compaction / inline-flush rows are roadmap (see M8); add the matrix
    entries when those changes are emitted.
- [x] **Wire the matrix into `attemptWriteTransaction`**. Run it on retry
  attempts only, mirroring upstream — when `attemptCount > 0`, fetch
  intervening `changes_made` rows between `baseSnapshotId` and
  `currentSnapshotId`, parse, run matrix. State-based
  `LogicalConflictCheck` stays — it's a strictly stronger check on
  per-call args (column / data-file IDs) that upstream doesn't have, and
  we already pass tests against it.
- [x] **Acceptance tests** (one per matrix gap closed):
  - `TestConcurrentCreateSchemaSameName`: two `createSchema("foo")` →
    loser fails non-retryable matrix conflict naming the schema.
  - `TestConcurrentCreateTableSameName`: two `createTable("S", "T")` →
    same.
  - `TestConcurrentCreateTableInDroppedSchema`: T1 drops schema S
    (empty); T2 creates table S.foo → T2 fails matrix conflict.
  - `TestConcurrentDeleteVsDelete`: two deletes targeting the same
    `data_file_id` → loser fails matrix conflict (other delete-vs-delete
    pairs on different files commit cleanly).
- [x] **Reuse existing test harness**. `ConcurrentWriterHarness` works as-is
  for all four; the only diff vs Step 3 acceptance tests is that the
  matrix throws, not the state-based check.

**Estimate**: ~3–4 days end-to-end. The parser is a half-day; the matrix
translation is a day; tests are a day; review-and-polish is a day.
Mostly mechanical because upstream is the source of truth — no design
decisions, just translation.

## M8: Maintenance Operations

**Design + snapshot-safety model: [DESIGN-maintenance.md](DESIGN-maintenance.md)** (two-phase
deletion: catalog retirement only *schedules* files; physical unlink is a separate age-gated step).

- [x] Stats maintenance — shipped as `ANALYZE` (rescans for the live row count, rebuilds column
  stats from per-file stats; recompute-after-delete + drift-repair tested).
- [x] **`rewrite_data_files` (compaction WRITER, non-partial v1)** — DONE (uncommitted, pending
    review). Shipped as the procedure `CALL system.rewrite_data_files(schema_name, table_name,
    file_size_threshold => '100MB')` rather than `ALTER TABLE ... EXECUTE` (procedure surface matches
    the other F6 ops). Reads the table's small parquet files through the REAL read path (delete files
    / partial_max / schema evolution all apply), writes one merged file, and atomically registers it
    + end-snapshots the sources via `DucklakeCatalog.rewriteDataFiles`. Non-partial / Iceberg-style:
    sources stay readable via time-travel until expire/cleanup reclaim them. Modeled as
    `DeletedFromTable`+`InsertedIntoTable` (no spec-locked conflict-matrix edits) + a `readSnapshotId`
    guard for the concurrent-delete-on-source race. Tests: `TestJdbcDucklakeCatalogRewriteDataFiles`
    (4), `TestDucklakeRewriteDataFiles` (5). See DESIGN-maintenance.md § 7.
- [x] **partial-emitting compaction variant** — DONE. `rewrite_data_files(.., reclaim_sources_-
    immediately => true)` writes `partial_max` + the `_ducklake_internal_snapshot_id` column on
    write, back-dates begin = min(source begin), and DELETES the sources entirely + schedules them
    (mirrors DuckLake WriteMergeAdjacent). `DucklakeCatalog.rewriteDataFilesPartial`;
    `TestJdbcDucklakeCatalogRewriteDataFiles` +2, `TestDucklakeRewriteDataFiles` +1 round trip.
- [x] `rewrite_data_files` enhancements — DONE: partitioned tables (per-partition groups, any
    transform), size-bounded multi-file output (`target_file_size`), re-compacting already-partial
    sources (non-partial path). The `ALTER TABLE ... EXECUTE optimize` alias is a deliberate non-goal
    (procedure surface is the connector's convention; the alias needs the separate TableProcedures
    SPI for no capability gain).
- [x] Connector procedures in `ducklake.system` (ALL shipped — incl. `rewrite_data_files`/optimize;
      see the § M8 optimize entry above):
  - [x] `remove_orphan_files` — DONE 2026-06-29 (`TestDucklakeRemoveOrphanFiles`). Storage-only
    (no catalog mutation): deletes files under the table data path with no catalog row, older than
    `retention_threshold` (default 7d, floored by `ducklake.remove-orphan-files.min-retention`).
  - [x] `expire_snapshots` — DONE 2026-06-29 (`TestDucklakeExpireSnapshots`, 7 e2e incl. surviving-
    snapshot safety + root-relative cleanup). Catalog-wide; `retention_threshold` (floored by
    `ducklake.maintenance.min-retention`) or explicit `snapshot_ids`; never the latest. Turned out
    NOT to need `WriteChange`/`ConflictMatrix` — it's a **plain catalog transaction, no new
    snapshot** (like `ANALYZE`), since expiry is destructive GC. Schedules dead files (absolute
     paths); also GCs the metadata of fully-expired dropped tables/views/macros/schemas + orphaned
    name-mapping rows. Still deferred: dynamic inlined-data tables only (harmless dangling).
  - [x] `cleanup_old_files` — DONE 2026-06-29. Drains `ducklake_files_scheduled_for_deletion` past
    the grace period; resolves connector-written absolute + DuckLake-written root-relative paths.
  - [x] `flush_inlined_data` — shipped earlier.
- [ ] Result tables from procedures (rows affected / files deleted / bytes reclaimed). v1 procs log
  counts; Trino's `Procedure` SPI is void, so a richer result surface is a separate item.

**F6 DONE (done done).** `remove_orphan_files` + `expire_snapshots` (+ FULL metadata GC incl. dynamic
inlined-data tables) + `cleanup_old_files` + `ANALYZE` + the partial-file READ filters (data +
parquet-delete + puffin-delete, all gates lifted) + the `rewrite_data_files` compaction WRITER in BOTH
shapes (non-partial + partial-emitting), with partitioned compaction, size-bounded multi-file output,
and partial-source re-compaction. Only omission: the cosmetic `ALTER TABLE … EXECUTE optimize` alias
(deliberate non-goal — procedure surface is the convention).

## Commit-Failure File Cleanup

Files written before a failed commit become orphans. This is now self-served from the connector:
`CALL system.remove_orphan_files(schema_name, table_name)` (shipped 2026-06-29) deletes catalog-
unreferenced files older than the retention grace period. (DuckLake's own
`ducklake_delete_orphaned_files()` remains available cross-engine.)

## Design Decisions

### Strict stats invalidation, no threshold heuristics

Stats policy is intentionally conservative ("don't be wrong"):

- Snapshots with delete files return unknown table/column stats.
- Snapshots with mixed inline + Parquet rows keep row count but suppress file-derived
  column stats.
- Schema-evolved columns whose file-level `value_count + null_count` doesn't cover all
  active data-file rows have column stats suppressed.

Why not a `% changed > N` threshold like Iceberg-style engines that keep stale
manifest stats and rely on `OPTIMIZE`/`ANALYZE` to refresh? Cross-engine delete
semantics and stale catalog metadata can make threshold-based stats unsafe — for
DuckLake interoperability, prefer "unknown" over "potentially wrong" at read time.
Refresh path is `ANALYZE` (shipped — recomputes `ducklake_table_stats` +
`ducklake_table_column_stats`).

## Reality Check: Spec vs Actual Catalog Shape

Known differences between the markdown spec and DuckDB-generated catalogs (all
handled by the connector):

- `ducklake_schema_versions` has `table_id` in practice.
- `ducklake_column` has extra columns (`default_value_type`, `default_value_dialect`)
  in practice.
- `ducklake_snapshot_changes.changes_made` values include forms like
  `inlined_insert:...`, `inline_flush:...`, `merge_adjacent:...`.
- Temporal partition values follow the DuckLake 1.0 calendar contract (resolved by
  spec PR [duckdb/ducklake-web#349](https://github.com/duckdb/ducklake-web/pull/349)).
  A deprecated epoch path is kept behind `ducklake.temporal-partition-encoding=epoch`
  for legacy catalogs; see
  [REPORT-temporal-partition-encoding-resolution.md](REPORT-temporal-partition-encoding-resolution.md).

See [archive/REPORT_CROSS_ENGINE_WRITE.md](archive/REPORT_CROSS_ENGINE_WRITE.md) for spec issues filed
with the DuckDB team.

## Catalog Namespace Hygiene — `brikk-` Prefixes (decided 2026-07-05)

Upstream owns the `file_format` value namespace (spec: "currently, only
`parquet` is allowed"), and the ecosystem signal says Vortex could plausibly be
their next official format (pg_ducklake just shipped `read_vortex` /
`pg_vortex` off the vortex core extension). If upstream later defines
`vortex`/`lance`/`duckdb` with their own conventions (footer_size semantics,
stats encoding, row-id mapping), our bare-named rows become nonconforming
squatters. Same policy as our view dialect (`brikk-trino`): prefix every
catalog-visible identifier we invent, migrate to canonical later if/when
upstream defines one (free while we have no users).

- [ ] Switch catalog-written `file_format` values `duckdb`/`lance`/`vortex` →
  `brikk-duckdb`/`brikk-lance`/`brikk-vortex`
  (`DucklakeSessionProperties.kt:97-106` constants + every write site). Session
  property values (`data_file_format = 'lance'`) can stay user-friendly — the
  prefix is for what lands in `ducklake_data_file.file_format`. Read side
  updates in the same change (it keys off the same constants). Mostly a
  catalog-only rename; no users → no back-compat shim needed.
- [ ] Inventory **all** other catalog-visible namespaces we extend for
  collision risk and prefix the same way: view `dialect` (already
  `brikk-trino` ✅), delete-file `format` values (if we ever write non-standard),
  `ducklake_tag` / `ducklake_column_tag` keys we invent, snapshot
  `changes_made` tokens (we must NOT invent any — upstream vocabulary only),
  table/partition option keys. One sweep, ~1h.
- [ ] Cross-engine sanity: after the rename, confirm stock DuckDB attached to
  the same catalog errors *cleanly* (not corruptly) on a `brikk-*` table scan,
  and skips them for maintenance ops (ties into the
  cross-engine-cleanup-survival test in [TODO-uhoh.md](TODO-uhoh.md)).

## Open Research Items

Pointers; full per-item rationale lives in
[`archive/RESEARCH-TODO.md`](archive/RESEARCH-TODO.md). When an item is picked up,
promote it into a real task in the appropriate section above and prune the bullet.

Each upstream-derived item carries an expected-landing tag `[v: …]`:
`CURRENT` = behavior/format of DuckLake v1.0 on DuckDB 1.5.x (what we run today);
`1.5.5` = coming DuckDB patch (hold until it lands); `NEXT` = DuckLake v1.1
(`V1_1_DEV_1`) / DuckDB 2.x, Fall. Items marked **[NOW-n]** are the CURRENT-version
actionable set — stable ids for the parallel agent working this area now to
cross-reference (see also the read-path list in `TODO-READ-MODE.md`).

- **rename-table-change-chain** — does `renameTable` emit `altered_table:<id>` to
  `snapshot_changes` (analogous to verified `renameView`)? ~30-min spike.
- **internal-retry-strategy** — decide: stay query-level (Trino retries the whole
  query on `DUCKLAKE_TRANSACTION_CONFLICT`) vs. bounded internal retry (upstream
  shape, but has the catalog-ID-reuse + off-by-one pitfalls they just fixed).
- **disallow-drop-sorted-column** — when sorted-table writes ship, reject
  `DROP COLUMN` for any sort-key column. Track as a sub-item of § Sorted Table Writes.
- **quack-hardening-watch** — re-check each refresh: "Experimental" label dropped?
  Stable-docs entry exists? JDBC driver docs describe pointing local DuckDB at Quack?
- **quack-wrapper-rewrite-spike** — jOOQ `ExecuteListener` on Quack branch that
  inlines params and wraps every statement in
  `quack_query_by_name(<metadata_catalog>, <inlined-sql>)`; drops the
  `ATTACH 'ducklake:quack:...'` line. 1-2 day spike; gates multi-op write parity.
- **quack-server-side-txn-lifecycle** — measurement spike: open one local DuckDB
  JDBC connection, run a sequence of `quack_query_by_name` calls, confirm via
  `duckdb_logs_parsed('Quack')` that all share one server-side `connection_id`.
- **quack-read-only-fallback** — alternate posture if wrapper-rewrite proves
  expensive: replace the 2-table JOINs in `getPartitionSpecs`/`getSortKeys` with
  per-table fetches joined in-JVM; ship "Quack: read parity, write deferred."
- **quack-jdbc-vs-quack-connid-pinning** — read DuckLake C++ extension's
  `ducklake_initializer.cpp` + `ducklake_transaction.cpp` + Quack's
  `quack_client.cpp` for connection-ID lifecycle. ~30-min read; unblocks the
  server-side-txn item above.
- **datafusion-maintenance-ops-reference** — pointer for M8 (above). Upstream Rust
  impl carries non-obvious semantics worth lifting: three-phase split (tombstone →
  expire → cleanup), criteria enums, in-flight-write guard on `last_modified`,
  `.parquet` suffix filter at storage listing, orphan-sweep referenced-set as a
  3-way UNION ALL (including pending scheduled-for-deletion). The Rust
  `examples/maintenance_demo.{rs,sql}` is the cross-engine oracle when M8 lands.
- **DuckLake v1.1 / v2.0 spec watch** — upcoming spec features to track as
  upstream firms them up. v1.1: variant inlining (lifts the "variant blocks
  inlining" restriction for non-DuckDB catalogs), multi-deletion-vector puffin
  files (multiple DVs per puffin to preserve time-travel without small-file
  proliferation). v2.0 ideas under consideration: git-like branching for
  DuckLake versions, role-based access control, incremental materialized views.
  Re-check on each upstream refresh — promote to real backlog when the
  spec text lands. Detailed previews in
  [`archive/archive/DUCKLAKE_1_0_IMPACT.md`](archive/archive/DUCKLAKE_1_0_IMPACT.md)
  "DuckLake v1.1 / v2.0 Preview" sections.

### Added 2026-06-23 upstream refresh (see RESEARCH-upstreams.md "Latest baselines")

- **upstream-deletion-vectors** (ducklake `main` + `v1.5-variegata`) — upstream
  landed a full **Puffin deletion-vector WRITE** path (multiple DVs per puffin,
  snapshot-filtered, hooked into `ducklake_delete` and the merge): commits
  `1e805b2d`/`4d16f7a1`/`48ea3aa5`/`16ee5948`/`4cf...`. We READ DVs but always
  WRITE parquet positional deletes (TODO-WRITE-MODE § F7). This narrows the gap
  to a known feature, not a research unknown — promote F7 Puffin-DV-writes when
  picked up. Bug-shaped watch: our DV *reader* must stay correct against the new
  multi-DV-per-puffin layout (canary: cross-engine delete round-trip).
- **upstream-maintenance-ops** (ducklake `v1.5-variegata`) — `ducklake_merge_adjacent_files`
  + `ducklake_rewrite_data_files` now run **server-side** (`5bceaccc`), plus
  `REWRITE_DELETES` honors `target_file_size` (`b32c1577`) and recomputes exact
  global stats after compaction (`4de135fd`/`ed4b5e3a`). Direct reference material
  for our F6/M8 maintenance program. `ducklake_delete_orphaned_files()` got a
  Quack remote-exec fix (`61a0fa1f`). ~½-day read to extract the commit/stats-refresh
  shape before M8 design.
- **upstream-inlined-merge-insert** (ducklake `main`) — inlining now covers the
  `MERGE INTO ... INSERT` case (`b849b051`, fixes #1186) and inline-data inserts +
  `tables_flushed_inlined`/`tables_deleted_inlined` in `ducklake_commit`
  (`2f00f057`/`5e8f73d7`/`4cfc3ca8`). Cross-check our `flush_inlined_data` +
  T2-B inlined-DELETE gate against the new commit vocabulary so a Trino-written
  inlined table round-trips. ~½-day verify spike; bug-shaped if commit fields drift.
- **upstream-commit-retry-partition-ids** (ducklake `main`) — a cluster of
  commit-retry correctness fixes around transaction-local partition ids
  (`4f658a2f`/`6d078382`/`c6a20e92`/`a5a6be4d`) and "preserve externally added
  transaction-local files" (`fcf8e5e8`). Directly relevant to our concurrent-conflict
  / commit-retry path (`DuckDbCatalogWriteRetry`) — verify our partition-id handling
  on retry doesn't have the same off-by-one they fixed. Bug-shaped; ~1-day spike.
- **upstream-stats-on-delete** (ducklake `main` + release) — "Decrement stats when
  deletes drop data files" (`3be1c235`), "Keep column stats accurate on drop-to-empty
  + same-txn insert" (`8f51c142`), MIN/MAX answered from catalog when exact
  (`8c1e97ab`). Our ANALYZE recompute (shipped) overlaps; confirm our incremental
  stats on DELETE match the new upstream decrement semantics. ~½-day cross-check.
- **upstream-change-feed-views** (ducklake `v1.5-variegata`) — "now allow views in
  the data change feed" (`7cefdf4b`). Signal that the change-feed surface (our F9,
  absent) is maturing upstream — re-scope F9 against the current `table_changes`
  shape when picked up.
- **datafusion-maintenance-shipped** (datafusion-ducklake v0.3.0, 2026-06-22) — the
  Rust impl now ships `expire_snapshots`, `cleanup_old_files`, `delete_orphaned_files`
  (#122/#123) and a `rowid` virtual column / row lineage (#115). This is the
  cross-engine oracle the existing `datafusion-maintenance-ops-reference` bullet
  predicted — it has now LANDED in a release. Also: schema-evolution read fixes
  (#140/#141) worth diffing against our T2-A work, and nanosecond-tz → `timestamptz_ns`
   mapping (#133). Promote alongside M8.

<!-- Added by 2026-07-05 upstream refresh (survey window 2026-06-29 → 07-05). -->
- **[NOW-5] max-compacted-files-bound** — ✅ DONE 2026-07-06.
  `rewrite_data_files` gained `max_compacted_files => N` (0 = unlimited):
  caps total SOURCE files consumed per invocation, deterministic group order,
  last group trimmed to budget when >= 2 remain. Test:
  `TestDucklakeRewriteDataFiles.maxCompactedFilesCapsSourcesPerInvocation`.
  Original: upstream `ducklake` main (`8b8e0491`) added a
  `max_compacted_files` named param to `ducklake_rewrite_data_files`, capping how
  many files a single rewrite/merge invocation touches (now applies to
  REWRITE_DELETES too, not just MERGE_ADJACENT). Our F6 compaction has no such
  cap — a large table rewrites everything in one commit. Add a bound. ~30-min.
- **[NOW-6] bucket-partition-name-collision** — ✅ DONE 2026-07-06, and the
  check found a REAL bug: we emitted bare column names for ALL transforms, so
  `bucket(4, id)` + `bucket(8, id)` produced duplicate `id=` hive keys
  (ambiguous layout; add_files hive parsing would silently overwrite), and our
  bucket/temporal key names diverged from upstream convention entirely
  (`bucket=`/`year=` vs our `id=`/`ts=`). `DucklakePagePartitioner` now mirrors
  upstream's `GetPartitionKeyName` incl. the main-branch counter fix: identity →
  column name, temporal → `year`/`month`/`day`/`hour`, bucket → `bucket`,
  collisions → `<prefix>_<column>` → `_2`/`_3`… Path names are cosmetic
  (catalog rows carry partition truth), so no read-path impact. Test:
  `TestDucklakePartitionedWrite.testPartitionPathKeyNamingMatchesUpstreamConvention`.
  Original: upstream `ducklake` (`1add112e`,
  `ducklake_partition_data.cpp`) fixed colliding partition-key *names* for
  repeated bucket transforms on the same column (now disambiguates with a
  `_2`, `_3`… counter suffix). We support `bucket(N, col)` — verify our
  partition-key name generation disambiguates repeated/overlapping bucket
  partitions the same way (else cross-engine name mismatch). ~30-min check.
- **[NOW-4] explicit-widening-type-promotion** `[v: CURRENT — DuckLake v1.0
  ALTER-vs-INSERT semantics; datafusion reference]` — datafusion-ducklake now models a column
  type change as an *explicit* widening `promote_column_type` (retires the live
  `ducklake_column` row, inserts a new version with the **same** `column_id` /
  Parquet field-id, no data rewrite, lossless set only) and now **rejects** a
  silent type change on a data write (`Replace`/`Append`) pointing at the promote
  API — mirroring upstream's ALTER-vs-INSERT split. Compare our write path: do we
  reject vs silently drop/accept a widening type change on INSERT, and can we
  version a column's type keeping the field-id stable? Research; ~1-2h.
- **[NOW-1] next-file-id-signals-change** `[v: CURRENT]` — ✅ **VERIFIED NOT A
  LIVE BUG 2026-07-05** (parallel agent). Both directions clean: (poisoner)
  `next_row_id` only advances on INSERT, which always writes a data file →
  shared `allocateFileId()` (delete files use it too, line 3443 / data 2951) →
  `next_file_id` moves → concurrent DuckDB's `(next_file_id, schema_version,
  table_id)` stats-cache key changes; (victim) we read `next_row_id` fresh from
  `ducklake_table_stats` per transaction, no cache on our side. Residual
  (benign): TRUNCATE changes `record_count` without bumping `next_file_id` —
  stale stats describe a *superset* post-truncate, so concurrent DuckDB filter
  folding stays correct; healed by the next INSERT. Optional ~10-min follow-up:
  confirm upstream's TRUNCATE doesn't bump `next_file_id` either
  (behavior-match). Original concern — pg_ducklake #217 (`d538bf8`): DuckLake's
  table-stats cache is keyed by `(next_file_id, schema_version, table_id)`, so
  upstream bumps `next_file_id` by 1 even on commits that add **no** data file
  (inlined-only) purely to bust that cache; not doing so let a concurrently-open
  DuckDB backend serve a stale `next_row_id` and write **duplicate row ids**.
  Verify every snapshot we commit that changes data — especially delete-only or
  metadata-only commits that add no new file — advances `next_file_id` (or
  otherwise invalidates a concurrent DuckDB reader's cached next_row_id). ~1h.
- **quack-protocol-bump-2026-07** `[v: DuckDB 1.5.5 (expected) — HOLD until 1.5.5
  lands; we already ship duckdb-quack so it applies to us once on our branch]` —
  duckdb-quack `main` (`ebf8841..b9f841c`)
  moved `QUACK_VERSION` into the message header and added a client↔server
  compatibility check, plus a large new async insert data-stream path
  (`quack_send_data` / `quack_data_stream` / client-scan table function). Wire
  protocol version bumped again; still experimental. Because we already use
  duckdb-quack, the protocol bump + compat check will affect our wire path as
  soon as it lands on our branch — deliberately deferred until 1.5.5 ships.
  Re-pin Quack wire assumptions then (feeds `quack-hardening-watch`).

<!-- LONGER-HORIZON scan of `ducklake` main (builds vs duckdb-main; the "Fall" -->
<!-- release train). 176 commits ahead of v1.5-variegata as of 2026-07-05. -->
- **ducklake-v1.1-format-horizon** `[v: NEXT — DuckLake v1.1 (V1_1_DEV_1) /
  DuckDB 2.x, Fall]` — ⚠️ BIG. `ducklake` main is staging a new
  catalog format version `V1_1_DEV_1` (`common/ducklake_version.hpp`,
  `DUCKLAKE_LATEST_VERSION`), with a versioned metadata manager
  (`ducklake_metadata_manager_v1_1.cpp`), a `ducklake_version` ATTACH option, and
  **automatic migration** from v1.0 (`ducklake_initializer.cpp:220`,
  `options.automatic_migration`). Concrete new surface we hand-write/read against:
  (a) **row_group information** persisted in the catalog (per-file row-group
  stats — `cd0581b9`); (b) an **`encryption_key VARCHAR`** column on
  `ducklake_data_file` (per-file encryption at rest); (c) **footer-less puffin**
  for single-file DVs (`4c4aebac`, changes our `DucklakePuffinDeleteReader`
  assumptions); (d) **multiple deletion vectors per puffin** (`4d16f7a1` — the
  item on our v1.1 spec watch, now landing). When this firms up, our hand-written
  metadata layer needs: version detection on ATTACH, a read path for v1.1 tables
  (row_group + mapping_id already present, encryption_key), footer-less/multi-DV
  puffin read, and a decision on whether we ever *write* v1.1. Promote to a real
  epic when `V1_1` (non-dev) tags. Deep-scan spike ~1 day when we act.
- **duckdb-2x-cpp17-abi** `[v: NEXT — DuckDB 2.x, Fall]` — ⚠️ build risk. `ducklake` main moved to **C++17**
  and adapted to DuckDB core API changes (`3f2a7b28`: new
  `GetData`/`GetDataMutable`, reworked `Table`/`Projection` Index types;
  `4ec16074`, `3f2a7b28`). main pins duckdb to an unreleased `duckdb-main` commit
  (the Fall/2.x train), not `v1.5.x`. Our `trino_parity` DuckDB C++ extension
  (`duckdb-trino-parity-extension/`) will need porting + rebuild against 2.x when
  DuckLake follows DuckDB to the next release. No action now (we track v1.5.4);
  flag so the extension rebuild isn't a surprise. Re-check the duckdb-version pin
  each refresh.
- **ducklake-going-GA** `[v: NEXT — DuckLake v1.1 / DuckDB 2.x, Fall]` — `1622bd3b Update README to remove DuckLake experimental
  notice` on main: DuckLake is dropping its "experimental" label in the next
  release. Signal only; expect docs/site (ducklake-web `docs/stable`) to promote
   the full feature set — re-survey ducklake-web when v1.1 tags.
