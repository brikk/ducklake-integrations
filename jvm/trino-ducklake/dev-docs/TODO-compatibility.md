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

- [ ] **Write spec-conformant `ducklake_snapshot_changes.changes_made`.** Two bugs, one fix:
  - (a) Values aren't quoted. Upstream `DuckLakeUtil::ParseQuotedValue` (in
    `third_party/ducklake/src/common/ducklake_util.cpp`) requires values start with `"`
    and uses `""` to escape embedded quotes.
  - (b) `created_table` / `created_view` values should be **fully qualified** as
    `"schema"."name"` — upstream's `ParseCatalogEntry` expects both parts separately quoted
    with a `.` between. Today we emit just `created_table:tableName` with no schema.
  - (c) `created_schema` is a single quoted value: `created_schema:"schema_name"`.
  - (d) `dropped_*`, `altered_*`, `inserted_into_table`, `deleted_from_table` use numeric
    IDs and are emitted unquoted — those are fine today.
  
  Impact: any DuckDB function that parses `changes_made` (`snapshots()`, `table_changes()`,
  `table_insertions()`, `table_deletions()`) will raise `InvalidInputException` on our
  snapshots today. Regular table reads are unaffected.
  
  The join has been extracted into a package-private
  `JdbcDucklakeCatalog.formatChangesMade(List<String>)` helper and pinned by unit tests;
  the two `@Disabled` tests in `TestJdbcDucklakeCatalogChangesMadeFormat` describe the
  target. Plumbing schema names to the `created_table` / `created_view` call sites is part
  of the fix.
  - `JdbcDucklakeCatalog.java:1090` (call site), `JdbcDucklakeCatalog.java:940` (helper),
    `JdbcDucklakeCatalog.java:1460, 1566, 1282` (addChange sites for created_*)
  - Tests: `TestJdbcDucklakeCatalogChangesMadeFormat`
  - Source: `COMPARE-pg_ducklake.md` B1

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
- [ ] **Inlined `list<T>` reads FAIL.** Bug confirmed:
  `ClassCastException: Slice cannot be cast to Block` at `ArrayType.writeObject`.
  `DucklakeInlinedValueConverter` produces a Slice where Trino's `ArrayType` expects a
  `Block` — we're treating the PG `INTEGER[]` column as a scalar. Pinned by
  `testDuckdbInlinedListIntCurrentlyFailsInTrino`; intended behavior is described by
  `@Disabled` `testDuckdbInlinedListIntReadsInTrino_target`. Fix: widen the converter to
  consume PG arrays and emit an ARRAY `Block` for `ArrayType` columns.
  - `DucklakeInlinedValueConverter.java`
  - Source: `COMPARE-pg_ducklake.md` B2

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
