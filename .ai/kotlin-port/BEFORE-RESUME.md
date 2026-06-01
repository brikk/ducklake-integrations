# BEFORE fixes — resume handoff

> Written mid-session because this Claude Code session's tool-output channel went bad
> (Gradle runs returning stale/empty/`EXIT=127` results, fabricated file reads). The repo
> on disk is intact — bad reads only ever caused Edits to fail safely, never corrupted files.
> Pick up from here in a fresh session/agent. Read `.ai/kotlin-port/PLAN.md` first for the full triage.

## Branch
`before-fixes` (off `main`). Pre-existing uncommitted changes from before this work:
`jvm/ducklake-catalog/build.gradle.kts`, `jvm/trino-ducklake/build.gradle.kts`, deleted `jvm/trino-ducklake/ducklake-web`. Leave those alone.

## How to verify (run from `jvm/`)
Never `gradle clean`. Gradle fails the build on any test failure, so BUILD SUCCESSFUL == green.
Trust on-disk JUnit XML over stdout:
`jvm/<module>/build/test-results/test/TEST-<fqcn>.xml` → `tests=… failures=… errors=…`.

---

## B1 — Type vocabulary + stats comparator cluster — DONE, VERIFIED GREEN
One canonical numeric-aware comparator now routes all three drifted paths.

Files:
- NEW `jvm/ducklake-catalog/src/dev/brikk/ducklake/catalog/DucklakeStatTypes.java`
  — canonical vocab + `isNumericType` (now incl. int128/uint128) + `parseStat` + `compare/min/max`
  (numeric → BigDecimal compare; everything else → lexical, which is correct for ISO temporal/bool/text).
- `JdbcDucklakeCatalog.java` — `typedMin/typedMax` delegate to `DucklakeStatTypes`; `parseStatValue`
  delegates to `DucklakeStatTypes.parseStat`; deleted the old `typedCompare`/`isNumericType`/`parseBoolean`.
- `DucklakeStatsExtractor.java` — row-group merge uses `DucklakeStatTypes.min/max(.., numeric)` instead of
  `stringMin/stringMax`; decimal decode now endianness-aware via Parquet physical type
  (`decodeDecimalUnscaled`: INT32/INT64 = little-endian, FIXED_LEN_BYTE_ARRAY/BINARY = big-endian).
- NEW test `jvm/ducklake-catalog/test/.../TestDucklakeStatTypes.java` (12 tests).
  NOTE: ducklake-catalog test module has NO junit-jupiter-params — use plain `@Test` loops, not `@ParameterizedTest`.
- `TestDucklakeStatsExtractor.java` — added 4 decimal-endianness tests; FLIPPED the old
  `extractStatsMergesStatisticsAcrossRowGroups` assertion from max=="8" (lexical bug it had enshrined) to max=="12".

Verified: TestDucklakeStatTypes 12/0/0, TestDucklakeStatsExtractor 20/0/0, TestJdbcDucklakeCatalogIntegration BUILD SUCCESSFUL.

---

## B4 — Temporal partition pruning silent data loss — CODE COMPLETE, NOT YET CONFIRMED GREEN
The confirming test run is the call that glitched (EXIT=127). Code is written; just needs one clean run.

Bug: in `DucklakeTemporalPartitionMatcher.temporalRangeContainsTransformedValue`, CALENDAR sub-year
transforms (MONTH/DAY/HOUR) are only monotonic within one parent period. With an unbounded bound (→
Long.MIN/MAX) or bounds straddling parent periods, the `[low,high]` transformed compare wraps; combined
with the exclusive-high decrement this pruned ALL files (e.g. `ts < 2026-03-01` DAY → high becomes 0,
every day-of-month 1..31 > 0 → everything dropped).

Fix (already applied to `DucklakeTemporalPartitionMatcher.java`):
- Added `nonMonotonicCalendar` flag.
- BEFORE the exclusive-high decrement: if non-monotonic CALENDAR AND (low empty OR high empty OR bounds
  not in same parent period) → `return true` (skip pruning).
- New helper `inSameCalendarParentPeriod(columnType, low, high, transform)`:
  HOUR→same epochDay, DAY→same year+month, MONTH→same year.
- Kept the original `lowTransformed > highTransformed` guard as a defensive fallback after the decrement.
- Verified by reasoning against all 25 pre-existing tests (bounded single-period exclusive-boundary cases
  like testExclusiveHighAtMonthBoundary still prune correctly).

Tests (already added to `TestDucklakeTemporalPartitionMatcher.java`, CONFIRMED RED before the fix —
ran tests=29 failures=4):
- testCalendarHourUnboundedLowAtMidnightDoesNotPruneAll
- testCalendarDayUnboundedLowAtMonthStartDoesNotPruneAll
- testCalendarDayUnboundedHighDoesNotPrune
- testCalendarDayMultiMonthRangeDoesNotPrune
- plus helper `tsMicros(y,m,d,h)` added above `singleDate`.

### >>> FIRST ACTION ON RESUME <<<
From `jvm/`:
`./gradlew :trino-ducklake:test --tests "dev.brikk.ducklake.trino.plugin.TestDucklakeTemporalPartitionMatcher"`
Expect tests=29 failures=0. If green, B4 is done. If any of the 4 new tests still fail, the parent-period
guard logic needs adjusting (check inSameCalendarParentPeriod boundaries).

---

## Remaining (not started) — see PLAN.md for full detail
(none — all five BEFORE items DONE on this branch)

## B2 — DONE (cheap-correct fix; macro alternative deferred)
Re-trace surfaced that the BEFORE-RESUME / PLAN.md framing was slightly off: it called
out integer truncation as the bug, but the actual surface is THREE divergences AND only
on the `.db` read path. Parquet path is unaffected because `DuckDbFilePageSource` is the
only consumer of `pushedExpressions`. On `.db` reads:

- **Integer truncation**: Trino's `/` truncates toward zero (5/2=2), DuckDB's `/`
  returns DOUBLE (5/2=2.5). Pushed `WHERE id/2=2` strips row id=5 from DuckDB's
  output. Trino's above-scan re-evaluation via `remainingExpression` can re-check
  rows DuckDB returned, but it CANNOT restore rows DuckDB stripped at the source.
- **Integer divide-by-zero**: Trino throws `DIVISION_BY_ZERO`; DuckDB silently
  returns `Infinity`; `Infinity = N` is false; DuckDB strips the offending row;
  Trino never sees it, never throws — query silently succeeds with wrong shape.
- **Integer (and float) modulo-by-zero**: same as above, DuckDB returns `NULL`
  instead of throwing.

Fix: `DuckDbExpressionTranslator.arithmeticOperator` returns `null` for
`$divide` and `$modulo` (kept `$add`/`$subtract`/`$multiply` — those align in both
engines including overflow throw semantics). Trino routes the predicate through
its own evaluator above the scan, which throws correctly and uses Trino's
truncated integer division.

Tests:
- NEW `TestDucklakeArithmeticPushdownParity` (5 tests, `.db` format via
  `data_file_format=duckdb` session prop): pinned RED before the fix (truncation
  returned [4] instead of [4,5]; both throw tests returned null instead of an
  exception), GREEN after.
- `TestDuckDbExpressionTranslator.testArithmeticPushedOperators` (renamed from
  `testArithmeticAllOperators`) pins `$subtract`/`$multiply` push as-is.
- NEW `TestDuckDbExpressionTranslator.testDivideAndModuloAreNotPushed` pins the
  intentional non-push of `$divide`/`$modulo` so a future change to
  `arithmeticOperator` has to update both layers.

Deferred macro alternative (for AFTER as a perf optimization): add `trino_divide`
and `trino_modulo` macros in `duckdb-trino-parity-extension/src/macro_definitions.cpp`
that emulate Trino truncation + throw on zero (DuckDB's `error()` function for
the throw; CAST-back to integer for the truncation), register in `trino_meta()`,
route through `translateMacroCall` in `DuckDbExpressionTranslator`. Cost:
extension rebuild, error-propagation verification, and trino_meta/PUSHABLE_FUNCTIONS
parity tests. Not worth it for divide/modulo predicates — they're rare in WHERE
clauses, and the cheap fix is fully correct.

Verification: full ducklake-catalog (33 suites / 108 tests / 0-0-0) +
trino-ducklake (70 suites / 825 tests / 0-0-0), on-disk JUnit XML, no `gradle clean`.

## B3 — DONE (option B coordinated fix landed on this branch)
B3a (coordinated catalog+writer+merge-handle fix) AND B3b (cheap predicate-disable fix) both implemented:

- **B3a parts**:
  - `DucklakeMergeTableHandle.DataFileRange` carries `existingDeleteFilePaths` (resolved absolute paths).
  - `DucklakeMetadata.beginMerge` groups `DucklakeDataFile` rows by `data_file_id` and resolves all active
    delete-file paths via `pathResolver.resolveFilePath(...)`.
  - NEW `DucklakeDeleteFileReader.readPositions(...)` factored from `DucklakePageSourceProvider.readDeletedRowsFromFile`,
    callable from both read and write sides so the union math is consistent.
  - `DucklakeMergeSink.writeDeleteFile` reads prior positions via that helper, unions with the new rowIds in
    a `LinkedHashSet`, writes the UNION to the new parquet file. Fragment reports `deleteCount` (union total)
    and `newDeleteCount` (delta — positions truly new this commit).
  - `DucklakeDeleteFragment` gains `newDeleteCount`.
  - `JdbcDucklakeCatalog.applyDeleteFragments` end-snapshots all prior active delete files for the
    touched `data_file_id`s in the same snapshot, then inserts the new (superseding) files. `record_count`
    decrement uses the DELTA (sum of `newDeleteCount`), not the union total — the prior file's positions
    were already deducted at first commit.
  - `DucklakePageSinkProvider` injects `ParquetReaderConfig` + `FileFormatDataSourceStats` and threads
    them into the merge sink so it can read prior delete files.
  - Red catalog test: `TestApplyDeleteFragmentsEndSnapshotsPrior` — confirmed RED before the fix
    (2 active delete files when 1 expected), GREEN after.
  - All three pre-existing `TestConcurrentDeleteVs*` suites still green (conflict-check semantics unchanged).

- **B3b parts** (chose option B — disable pushdown when deletes present):
  - `DucklakePageSourceProvider.splitHasActiveDeletes(split)` static helper — true iff split has any
    parquet delete files (incl. puffin) or any inlined deletes.
  - Parquet path (~430): `parquetTupleDomain = splitHasActiveDeletes(split) ? TupleDomain.all() :
    toParquetTupleDomain(...)`. That cascades into `getFilteredRowGroups` (no pruning) and the predicate
    arg to `ParquetReader` (Optional.empty()), so pages stream contiguously from row 0.
  - DuckDB path (~603): `filePredicate = ... ? TupleDomain.all() : effectivePredicate.filter(...)`,
    plus `pushedExpressions = splitHasActiveDeletes(split) ? List.of() : pushedExpressions`. Same reason.
  - Trino's filter pipeline still applies the predicate ABOVE the page source — query semantics unchanged;
    we just give up row-group pruning on the files that carry deletes.
  - Structural test: `TestSplitHasActiveDeletes` covers all five "active deletes" shapes (none / parquet /
    puffin / inlined / both).
  - Performant alternative (project `appendRowNumberColumn` / DuckDB `file_row_number` and strip in both
    transforms) is DEFERRED to AFTER as a perf optimization. Stacking it on top of B3a was unjustified
    blast radius; the cheap fix is correct and faithfully portable.

- **Verification**: full ducklake-catalog suite (33 suites, 108 tests, 0/0/0) + full trino-ducklake suite
  (69 suites, 819 tests, 0/0/0), aggregated from on-disk `TEST-*.xml`. No `gradle clean`.
- B5: Arrow writer join-before-close — `DuckDbArrowStreamFileWriter.cleanupAfterFailure` (~624-637):
  `consumerThread.join(bounded)` before freeing allocator (mirror happy path ~446). Reasoned fix, no red-test.

Suggested order (agreed with user): B4 (finish) → B5 → B3 (DONE) → B2 (DONE).
