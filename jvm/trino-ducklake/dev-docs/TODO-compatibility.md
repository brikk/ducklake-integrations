# DuckLake Compatibility TODO

Action items for tightening wire- and behavior-compatibility of this connector with the
DuckLake spec and the reference implementations.

Where this list comes from: reading the upstream DuckDB `ducklake` extension (the
reference implementation, authored by the spec team), reading two independent DuckLake
integrations (`datafusion-ducklake` and `pg_ducklake`), cross-checking observed behavior
against real DuckDB-written catalogs via our cross-engine tests, and spot-reading our own
catalog write path. See `COMPARE-datafusion-ducklake.md` and `COMPARE-pg_ducklake.md` for
the full write-ups.

Items are ordered by rough effort — small correctness fixes up top, feature work below —
and each links back to where the decision came from.

## Small correctness / hygiene fixes

- [x] **Accept `linestring_z` (DuckLake 1.0 name) in the type converter.** Switch now
  matches both `linestring_z` and `linestring z` (kept for backwards compat with pre-1.0
  catalogs). Covered by `TestDucklakeTypeConverter.testLinestringZUnderscoreIsVarbinary`
  and `testLinestringZLegacySpaceFormStillAccepted`.
  - `DucklakeTypeConverter.java:161-163`
  - Source: `COMPARE-pg_ducklake.md` B4

- [x] **Write spec-conformant `ducklake_snapshot_changes.changes_made`.** Done for (a)–(d):
  - (a) Values are quoted via `writeQuotedValue()` (double-quote wrap, `""` escape), matching
    upstream `KeywordHelper::WriteQuoted` / `DuckLakeUtil::ParseQuotedValue` in
    `third_party/ducklake/src/common/ducklake_util.cpp`.
  - (b) `created_table` / `created_view` now emit the fully-qualified `"schema"."name"` form
    (two quoted values separated by `.`) — schema name is threaded through the call sites in
    `createTable` / `createView`. Upstream's `ParseCatalogEntry` round-trips this cleanly.
  - (c) `created_schema` emits the single-value form `created_schema:"schema_name"`.
  - (d) Numeric-id entries (`dropped_*`, `altered_*`, `inserted_into_table`,
    `deleted_from_table`) stay unquoted — already spec-conformant.

  Helpers live on `JdbcDucklakeCatalog` (`changeCreatedSchema` / `changeCreatedTable` /
  `changeCreatedView`, all package-private). `formatChangesMade` remains a plain
  comma-joiner; quoting happens at the entry-building helpers so `addChange` callers don't
  need to know the format.

  Pinned by `TestJdbcDucklakeCatalogChangesMadeFormat` (unit-level quoting + pathological
  names) and two new cross-engine tests in `TestDucklakeCrossEngineCompatibility` —
  `testDuckdbParsesTrinoWrittenChangesMadeAcrossAllChangeKinds` exercises
  {schema/table create, insert, alter, delete, drop} and confirms DuckDB's
  `ducklake_snapshots('ducklake_db')` parses every row without raising; the pathological
  names test passes a comma-bearing schema and table name through the full pipeline.

  - `JdbcDucklakeCatalog.java` (helpers next to `formatChangesMade`; call sites in
    `createTable` / `createView` / `createSchema`)
  - Tests: `TestJdbcDucklakeCatalogChangesMadeFormat`,
    `TestDucklakeCrossEngineCompatibility.testDuckdbParsesTrinoWrittenChangesMadeAcrossAllChangeKinds`,
    `TestDucklakeCrossEngineCompatibility.testDuckdbParsesTrinoWrittenChangesMadeWithPathologicalNames`
  - Source: `COMPARE-pg_ducklake.md` B1

- [x] **`renamed_view` is not a recognized upstream change type — folded into
  `altered_view`.** Upstream's `ParseChangeType` in `ducklake_transaction_changes.cpp`
  lists no `RENAMED_*`; emitting `renamed_view:<viewId>` used to make DuckDB's
  `ducklake_snapshots()` / `table_changes()` throw "Unsupported change type
  renamed_view" for any snapshot that ever renamed a view. `JdbcDucklakeCatalog.renameView`
  now emits `altered_view:<viewId>` instead — semantically correct (a rename is a
  schema/name change) and spec-conformant. Also bumps `schema_version` the same way the
  other `altered_view` paths do. Pinned by
  `TestDucklakeCrossEngineCompatibility.testDuckdbParsesTrinoWrittenViewAndSchemaDdlChanges`,
  which exercises create → rename → replace → drop through DuckDB's parser.
  - `JdbcDucklakeCatalog.renameView`
  - Source: cross-engine gap uncovered while fixing B1

- [x] **Bump `schema_version` on view and schema DDL, not just table DDL.** Done.
  Added a no-arg `DucklakeWriteTransaction.incrementSchemaVersion()` overload that bumps
  the counter but leaves `schema_versions.table_id` as SQL NULL (spec allows nullable —
  see `ducklake_schema_versions.md`, upstream's `SchemaChangesMade()` in
  `third_party/ducklake/src/storage/ducklake_transaction.cpp:718` also flips on
  `new_tables` *including view entries*, `dropped_views`, `new_schemas`, and
  `dropped_schemas`). Wired into `createView`, `dropView`, `renameView`,
  `replaceViewMetadata`, `createSchema`, `dropSchema`. A DuckDB reader that caches the
  catalog keyed on `schema_version` now sees Trino-created/dropped views and schemas
  without waiting on a table DDL to bump the counter. Pinned by
  `TestDucklakeCrossEngineCompatibility.testSchemaVersionBumpsOnViewAndSchemaDdl` (walks
  the full view/schema DDL surface and asserts a bump per step + ≥6 new rows in
  `ducklake_schema_versions` with `table_id IS NULL`).
  - `JdbcDucklakeCatalog.java` (`createView` / `dropView` / `renameView` /
    `replaceViewMetadata` / `createSchema` / `dropSchema`)
  - `DucklakeWriteTransaction.incrementSchemaVersion()` (no-arg overload)
  - Source: cross-engine gap uncovered while fixing B1

- [ ] **When user-defined DEFAULT expressions ship, set `default_value_dialect = 'trino'`.**
  Today every column we write has `default_value = 'NULL'` (the "no default" sentinel) and
  we leave `default_value_dialect` SQL NULL — safe and honest, since the field is
  informational and only meaningful when there's a real literal or expression to interpret
  (spec `ducklake_column.md:36`; upstream migration at
  `ducklake_metadata_manager.cpp:297` adds the column with `DEFAULT NULL`; upstream's
  ducklake extension never reads the field anywhere). When we wire up real `DEFAULT`
  support for Trino-written tables, write the literal `'trino'` (not `'brikk-trino'` —
  the dialect names the SQL syntax of the expression, which is plain Trino SQL; brikk
  metadata lives only in our view rows). Call sites: `JdbcDucklakeCatalog.insertColumnTree`
  and `JdbcDucklakeCatalog.renameColumn`. Pinned today by
  `TestDucklakeCrossEngineCompatibility.testDuckdbReadsTrinoTableWithNullDefaultValueDialect`.
  - Source: this design discussion; `REPORT_CROSS_ENGINE_WRITE.md` Issue 1

- [ ] **Add a write-side range check for unsigned types.** Today we widen on read
  (uint8→SMALLINT etc.) but do nothing on write — a Trino `SMALLINT 300` written to a
  `uint8` column silently wraps to 44. Throw a `TrinoException` with the column and
  range, or clamp, as soon as we detect overflow in the page sink.
  - `DucklakePageSink` write path
  - Source: `COMPARE-pg_ducklake.md` B5 (already in README as acknowledged limitation)

- [x] **UUIDv7 for catalog-identity UUIDs.** Done. Added
  `com.fasterxml.uuid:java-uuid-generator` 5.0.0, introduced a
  `JdbcDucklakeCatalog.newCatalogUuid()` helper backed by
  `Generators.timeBasedEpochGenerator()`, and swapped the three catalog-identity call sites
  (view / schema / table). Transient `DucklakeTransactionHandle` UUID and Parquet filename
  UUIDs intentionally left on v4 (no DB locality value there). Covered by
  `TestJdbcDucklakeCatalogUuidVersion` (pins version bits + verifies embedded timestamp
  matches wall clock, so a swap to v1/v4 would be caught). Cross-engine sanity pass with
  DuckDB reading Trino-written UUIDv7 values.
  - `JdbcDucklakeCatalog.java:57-64` (generator + helper), `:1308, :1490, :1536` (call sites)
  - Source: `COMPARE-pg_ducklake.md` (cosmetic but cheap)

## Verified via fixtures (2026-04-23)

Added to `TestDucklakeCrossEngineCompatibility`: DuckDB writes small rows under
`data_inlining_row_limit=100`, a helper asserts rows stayed in
`ducklake_inlined_data_<tableId>_<schemaVersion>` (no Parquet files emitted), Trino reads.

- [x] **Inlined `date` / `timestamp` reads work.** `testDuckdbInlinedDateAndTimestampReadInTrino`.
  No action needed — `DucklakeInlinedValueConverter` already parses DuckDB's VARCHAR ISO
  encoding correctly for these types.
- [x] **Inlined `blob` reads work, including bytes with 0x00 and 0xFF.**
  `testDuckdbInlinedBlobReadsInTrino`. Confirms our read path handles BYTEA bytes.
- [x] **Inlined `varchar` with an embedded null byte reads correctly.**
  `testDuckdbInlinedVarcharWithEmbeddedNullByteReadsInTrino`. Pleasant surprise — we were
  worried about this (`'ABC' || chr(0) || '123'`); length(s) comes back as 7 in Trino,
  so the null byte survives the round-trip. No action needed.
- [x] **`list<T>` reads work for flat primitive element types, via both the inlined and
  Parquet paths.** Widened `DucklakeInlinedValueConverter` to detect `ArrayType` columns
  and build a Trino element `Block` via `TypeUtils.writeNativeValue`. Upstream
  (`ducklake_util.cpp::ToSQLString` LIST branch) encodes inlined lists for the PG catalog
  as scalar VARCHAR holding DuckDB's `[elem, elem, ...]` text form, so the converter parses
  that syntax (respecting `NULL` elements and single-quoted strings with `\'` / `\\`
  backslash escapes — DuckDB's text form uses C-style escaping inside list elements, not
  SQL-style `''` doubling). A `java.sql.Array` / `Object[]` path is also accepted for
  forward-compat with catalogs that use native PG arrays. Each `TestDucklakeCrossEngineCompatibility.testDuckdbList*ReadsInTrino`
  test runs through the `runListRoundTrip` helper, which writes twice — once with
  `data_inlining_row_limit=100` (inlined) and once with `data_inlining_row_limit=0`
  (Parquet, via Trino's native Parquet array reader) — and applies the same assertions to
  both. Covered element types: `int`, `bigint`, `smallint`, `tinyint`, `boolean`, `real`,
  `double`, `decimal`, `varchar` (incl. apostrophe + embedded comma + empty string),
  `date`, `timestamp`, `timestamptz`. Every test includes a `NULL` element case; the `int`
  and `varchar` tests additionally cover null-valued rows and empty lists. Nested
  array/struct/map element types still throw `UnsupportedOperationException` pointing back
  here — separately tracked.
  - `DucklakeInlinedValueConverter.java`
  - Source: `COMPARE-pg_ducklake.md` B2

- [ ] **`list<blob>` inlined reads return raw `\xNN` text instead of decoded bytes;
  Parquet path not yet validated.** DuckDB serializes a blob inside a list as
  `'\x00\x01\xFF'` text. Our parser strips the single quotes but does not decode the
  `\xNN` hex escapes, so `VarbinaryType` gets the literal string bytes (`\`, `x`, `0`,
  `0`, ...) instead of the intended `0x00 0x01 0xFF`. Pinned by `@Disabled
  testDuckdbListBlobReadsInTrino` — the test runs both inlined and Parquet paths, so
  lifting the `@Disabled` will also confirm whether Trino's Parquet array reader survives
  DuckDB's `ARRAY<BINARY>` layout. Fix: in the array path, when the element type is
  `VarbinaryType` (or at the scalar `VarbinaryType` branch when the input is a `String`
  that starts with `\x`), decode `\xNN` sequences into bytes before building the Trino
  value. Compare against `ducklake_util.cpp::ToSQLString` BLOB branch and DuckDB's
  `Blob::ToString` for exact escape semantics.
  - `DucklakeInlinedValueConverter.java`
  - Test: `testDuckdbListBlobReadsInTrino` (`@Disabled`)

- [ ] **`uuid` scalar + `list<uuid>` inlined reads mis-handle the 36-char text form;
  Parquet path not yet validated.** `DucklakeInlinedValueConverter` has no `UuidType`
  branch, so a scalar UUID column falls through to the VARCHAR fallback and Trino's
  `UuidType.writeSlice` rejects the resulting 36-byte Slice ("Expected entry size to be
  exactly 16"). The list path inherits the same gap. Pinned by `@Disabled
  testDuckdbListUuidReadsInTrino` — the test covers both inlined and Parquet paths, so
  lifting the `@Disabled` will also validate Trino's Parquet UUID logical-type handling
  against DuckDB-written files. Fix: add a `UuidType` branch that parses the string with
  `java.util.UUID.fromString` and packs the 16 bytes as Trino expects (see
  `io.trino.spi.type.UuidType.javaUuidToTrinoUuid`).
  - `DucklakeInlinedValueConverter.java`
  - Test: `testDuckdbListUuidReadsInTrino` (`@Disabled`)

## Feature gaps

- [x] **Map `int128` / `uint128`.** Done. `int128` → `DECIMAL(38, 0)`, `uint128` → VARCHAR.
  README type table and limitations section updated.
  (Spec canonical names are `int128`/`uint128`, not `hugeint`/`uhugeint` — see upstream
  `ducklake_types.cpp`; `hugeint`/`uhugeint` are DuckDB SQL names that never appear in the
  catalog column_type column.) Covered by unit tests
  `TestDucklakeTypeConverter.testInt128MapsToDecimal38` / `testUint128MapsToVarchar` and
  end-to-end tests
  `TestDucklakeCrossEngineCompatibility.testDuckdbHugeintColumnReadsAsDecimalInTrino` /
  `testDuckdbUhugeintColumnReadsAsVarcharInTrino`.
  - `DucklakeTypeConverter.java:130-137`
  - Source: `COMPARE-pg_ducklake.md` B3

- [ ] **Add a Parquet footer size hint on the read path.** Pass
  `ducklake_data_file.footer_size` (and the equivalent on `ducklake_delete_file`) to
  Trino's Parquet reader as a footer-size hint. Saves an S3 range read per file. Low
  effort, measurable S3 latency win. Rust `datafusion-ducklake` already does this.
  - `DucklakePageSourceProvider` / related reader setup
  - Source: `COMPARE-datafusion-ducklake.md` (ideas worth stealing)

- [ ] **Session properties for commit context.** Already on the roadmap, but pg_ducklake
  exposing `set_commit_message()` is a nudge it's commonly wanted. Add:
  `commit_author`, `commit_message`, `commit_extra_info`. Write them to
  `ducklake_snapshot_changes.author` / `commit_message` / `commit_extra_info`.
  - Source: `COMPARE-pg_ducklake.md` (feature gap)

## Concurrency tests (no new code, just tests)

- [ ] **Add a writer-vs-writer isolation test on the same table.** Two Trino sessions,
  each doing an INSERT; one must abort with `TransactionConflictException` and the other
  must commit cleanly. pg_ducklake has this as a pg_isolation spec and we have zero
  coverage for it today.
  - Source: `COMPARE-pg_ducklake.md` (test-coverage deltas)

- [ ] **Add a writer-vs-writer isolation test across two tables.** Same as above but on
  different tables; both commits must succeed. This is the one pg_ducklake's
  `concurrent_cross_table_writes` spec was added to protect against — catches snapshot-ID
  cross-talk.
  - Source: `COMPARE-pg_ducklake.md`

## Track but not now

These are the larger items that deserve their own dev-doc / design pass, not a line on a
short-term list:

- Maintenance operations (`flush_inlined_data`, `merge_adjacent_files`,
  `rewrite_data_files`, `expire_snapshots`, `cleanup_old_files`, `cleanup_orphaned_files`,
  `CHECKPOINT`) — scope notes in `COMPARE-pg_ducklake.md`. Start with
  `expire_snapshots` + `cleanup_orphaned_files` (both are catalog-driven, don't need a
  compaction engine).
- `partial_max` handling for DuckLake 1.0 — already tracked in `DUCKLAKE_1_0_IMPACT.md`.
- Puffin deletion vector format — already tracked in `DUCKLAKE_1_0_IMPACT.md`.
- Bucket partition transform — already in `DUCKLAKE_1_0_IMPACT.md`.
- Data change feed (`table_changes` / `table_insertions` / `table_deletions`) —
  already in our "Not yet implemented" list.
- Virtual columns (`rowid`, `snapshot_id`, `filename`, `file_row_number`,
  `file_index`) — useful for MERGE story; worth its own design doc.
- Sorted writes — lives with the sort-on-write design pass.
- Evaluate adopting the DuckLake `.slt` corpus as a portable regression suite for the
  catalog library (from `COMPARE-datafusion-ducklake.md`).
