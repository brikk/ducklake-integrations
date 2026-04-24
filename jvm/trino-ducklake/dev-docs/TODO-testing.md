# TODO Testing Analysis

## Scope Snapshot

- `jvm/trino-ducklake`: 27 test classes, 11,162 LOC, 475 `@Test` methods.
- `jvm/ducklake-catalog`: 3 test classes, 277 LOC, 19 `@Test` methods.
- There are no `@ParameterizedTest`, `@MethodSource`, or `@Nested` usages right now, so file splitting does not need special parameterization migration work.

## Are Tests In The Wrong Place?

### Clearly misplaced (move fully to `ducklake-catalog`)

- `jvm/trino-ducklake/test/src/dev/brikk/ducklake/trino/plugin/TestDucklakeWriteFragment.java`
- Why: it tests `dev.brikk.ducklake.catalog.DucklakeWriteFragment` and `DucklakeFileColumnStats` JSON round-trip only; no Trino behavior under test.
- Action: move to `jvm/ducklake-catalog/test/src/dev/brikk/ducklake/catalog/TestDucklakeWriteFragmentJson.java`.

### Mixed-scope (split between catalog and trino)

- `TestDucklakeCatalog.java` (581 LOC)
- Catalog-level tests and Trino-level tests are mixed in one class.
- Catalog-level portion: snapshot/schema/table listing, data files, table-column type strings, file-id predicates, raw column stats.
- Trino-level portion: `DucklakeMetadata#getTableStatistics`, `getColumnHandles`, `getTableHandle` version semantics.
- Action:
- Create catalog-side file for pure `JdbcDucklakeCatalog` behavior.
- Keep/create trino-side file focused on `DucklakeMetadata` behavior.

- `TestDucklakePartitionPruning.java` (889 LOC)
- First catalog-metadata checks are not Trino-specific (`getPartitionSpecs`, `getFilePartitionValues`).
- Main body is Trino-specific (`applyFilter`, `DucklakeSplitManager`, domain enforcement).
- Action:
- Move the pure catalog partition metadata checks to catalog module.
- Keep pruning logic in Trino module.

- `TestDucklakeDDLIntegration.java` (869 LOC)
- `testConcurrentSchemaCommitsFailWithTransactionConflict` is primarily catalog transaction behavior (`JdbcDucklakeCatalog.createSchema`) with DB-level locking.
- Most other tests are true Trino DDL semantics.
- Action:
- Move that conflict test to catalog module (`TestJdbcDucklakeCatalogConflicts`).
- Keep DDL SQL semantics in Trino module.

### Truly Trino-related (keep in `trino-ducklake`)

- `TestDucklakeIntegration`, `TestDucklakeCrossEngineCompatibility`, `TestDucklakeWriteIntegration`, `TestDucklakeDeleteIntegration`, `TestDucklakeViewIntegration`
- `TestDucklakePageSourceProvider`, `TestDucklakeSplitManager`, `TestDucklakeSnapshotResolver`, `TestDucklakeTemporalPartitionMatcher`, `TestDucklakePartitionComputer`, `TestDucklakeTypeConverter`, `TestDucklakeMetadataTemporalVersionConversion`, `TestDucklakePlugin`, `TestDucklakeConfig`, `TestDucklakeStatsExtractor`, `TestDucklakeUnsignedRangeChecker`, `TestFooterPrefetchingParquetDataSource`, `TestDucklakeDeleteFileHandling`, `TestDucklakeCatalogSnapshotPinningIntegration`
- Reason: they test connector/session/query planning/read/write/split/page-source behavior or classes located in `dev.brikk.ducklake.trino.plugin`.

## Oversized Test Files To Split

## Highest priority

- `TestDucklakeCrossEngineCompatibility.java` (1748 LOC)
- Split by capability:
- `...CrossEngineRoundTripTest` (basic insert/ctas/nulls/show/describe).
- `...CrossEngineTypeAuditTest` (`list<T>`, inlined scalars, hugeint/uhugeint).
- `...CrossEngineCatalogMetadataTest` (`changes_made`, default dialect null handling, schema-version/view+schema DDL assertions).
- `...CrossEngineWriteGuardsTest` (unsigned range enforcement).

- `TestDucklakeIntegration.java` (1562 LOC)
- Split by behavior cluster:
- `...ReadSmokeTest` (metadata queries, info schema, basic reads).
- `...PartitionAndTimeTravelTest` (partitioned/daily/temporal and `FOR VERSION/TIMESTAMP AS OF`).
- `...PlannerAndJoinsTest` (joins, set ops, explain).
- `...InlinedAndDeleteReadTest` (delete-file reads, inlined/mixed-inline handling).

- `TestDucklakePartitionPruning.java` (889 LOC)
- Split by layer:
- `...MetadataApplyFilterTest` (enforced/unenforced/idempotence/none-all).
- `...SplitPruningIdentityTest` (identity partition split pruning).
- `...SplitPruningTemporalTest` (year/month/day and timestamp/timestamptz scenarios).

- `TestDucklakeDDLIntegration.java` (869 LOC)
- Split by operation family:
- `...SchemaAndTableDdlTest` (create/drop schema/table).
- `...AlterTableIntegrationTest` (add/drop/rename and data preservation).
- `...SchemaVersionSemanticsTest` (schema-version changes vs DML).

## Medium priority

- `TestDucklakeDeleteIntegration.java` (637 LOC)
- Split into:
- `...DeleteSemanticsTest`.
- `...UpdateAndMergeSemanticsTest`.

- `TestDucklakeWriteIntegration.java` (614 LOC)
- Split into:
- `...InsertAndCtasTest`.
- `...WriteMetadataAndSnapshotTest`.
- `...PartitionedWriteTest`.

- `TestDucklakeCatalog.java` (581 LOC)
- Split as described in mixed-scope section.

- `TestDucklakePageSourceProvider.java` (569 LOC)
- Split into:
- `...PredicateAndDynamicFilterTest`.
- `...ComplexTypeAndSchemaEvolutionReadTest`.
- `...InlinedReadSourceTest`.

- `TestDucklakeTemporalPartitionMatcher.java` (554 LOC)
- Split into:
- `...DatePartitionMatcherTest`.
- `...TimestampPartitionMatcherTest`.
- `...TimestampWithTimeZonePartitionMatcherTest`.

## Low priority

- `TestDucklakeViewIntegration.java` (453 LOC)
- Split into:
- `...ViewDdlTest`.
- `...ViewSnapshotIsolationTest`.
- `...ViewQuerySemanticsTest`.

## Catalog Module Coverage Gaps (after moves)

- Add direct `JdbcDucklakeCatalog` integration tests for:
- snapshot/schema/table discovery against generated fixtures.
- partition specs and file partition values.
- data file lookup predicates and typed min/max stats.
- transaction conflict behavior on concurrent catalog writes.
- Keep `trino-ducklake` focused on connector semantics, not catalog data-model verification.

## Execution Plan (recommended order)

- [x] Move `TestDucklakeWriteFragment` to `ducklake-catalog`.
- [x] Start split of `TestDucklakeCatalog` by moving snapshot/table/data-file/predicate/stats coverage to `ducklake-catalog` (`TestJdbcDucklakeCatalogIntegration`) and trimming those assertions from Trino-side `TestDucklakeCatalog`.
- [x] Move pure catalog checks for `TestDucklakePartitionPruning` (`partition specs` + `file partition values`) into `ducklake-catalog`.
- [ ] Move `testConcurrentSchemaCommitsFailWithTransactionConflict` from DDL integration to catalog tests.
- [ ] Split the two largest integration files: `TestDucklakeCrossEngineCompatibility` and `TestDucklakeIntegration`.
- [ ] Split `DDL`, `Write`, `Delete`, `PageSourceProvider`, `TemporalPartitionMatcher`.
- [ ] Extract shared test fixture utilities (catalog generator/server/environment) to a shared test-support package to avoid duplication between modules.
