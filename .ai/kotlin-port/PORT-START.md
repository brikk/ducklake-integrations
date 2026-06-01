# Kotlin port — start here (read order: this → PLAN.md → memory)

> Written by the review/orchestration agent after the BEFORE fixes landed. PLAN.md is the
> canonical port DESIGN (slices, kit model, conventions). This doc adds what the BEFORE work
> taught us that PLAN.md predates, plus the things most likely to bite the port specifically.
> The driving agent owns gradle/verification — it has the reliable channel.

## State at port start
- All 5 BEFORE fixes are DONE, verified green, and (per Jayson) committed + merged to master.
  Baseline is correct: catalog 108/0/0, trino ~825/0/0. The faithful port will NOT enshrine
  the known bugs — that was the entire point of doing BEFORE first.
- Branch the port off the post-merge master (e.g. `port/catalog-test`), NOT off `before-fixes`.
- Deferred-to-AFTER (do NOT pull into the port): performant B3b (file_row_number thread/strip),
  B3b multi-row-group integration test, B2 trino_divide/trino_modulo parity macros. See BEFORE-RESUME.md.

## Files the BEFORE work ADDED/CHANGED — these are now part of the port surface
New files (port them like any other when their slice comes up):
- `ducklake-catalog/.../DucklakeStatTypes.java` (+ test `TestDucklakeStatTypes.java`)
- `trino-ducklake/.../DucklakeDeleteFileReader.java`
- tests: `TestApplyDeleteFragmentsEndSnapshotsPrior.java`, `TestSplitHasActiveDeletes.java`,
  `TestDucklakeArithmeticPushdownParity.java`
Changed (watch these during port — see invariants below):
- `JdbcDucklakeCatalog.java`, `DucklakeStatsExtractor.java`, `DucklakeTemporalPartitionMatcher.java`,
  `DuckDbArrowStreamFileWriter.java`, `DucklakeMergeSink.java` (constructor +2 params),
  `DucklakeMergeTableHandle.java`, `DucklakeMetadata.java`, `DucklakePageSinkProvider.java`,
  `DucklakePageSourceProvider.java`, `DucklakeDeleteFragment.java`, `DuckDbExpressionTranslator.java`.

## INVARIANTS the port must preserve (a faithful port can silently break these — they're behavioral, not structural)
1. **Numeric stats compare numerically, not lexically** (B1). `DucklakeStatTypes.compare` switches on
   numeric-vs-not. If the port "simplifies" min/max merges back to string compare, 12<8 bugs return.
   The flipped assertion in `TestDucklakeStatsExtractor` (max==12 not 8) is the guard — keep it.
2. **Decimal stat decode is endianness-by-physical-type** (B1). INT32/INT64 = little-endian,
   FIXED_LEN_BYTE_ARRAY/BINARY = big-endian. Don't collapse to one path.
3. **$divide / $modulo must NOT push down** (B2). `arithmeticOperator` returns null for them.
   `TestDivideAndModuloAreNotPushed` pins it. A port that "completes the switch" reintroduces silent
   data loss on .db reads.
4. **Splits with active deletes disable predicate pushdown, BOTH engines** (B3b). `splitHasActiveDeletes`
   gates parquetTupleDomain→all() AND drops DuckDB filePredicate+pushedExpressions. There's an INVARIANT
   comment at DucklakePageSourceProvider.java ~441-450 tied to whole-file split granularity — KEEP that
   comment in the Kotlin port verbatim; it's a tripwire, not decoration.
5. **Delete write unions prior positions; record_count decrements by DELTA not total** (B3a).
   DucklakeDeleteFragment carries both deleteCount (union) and newDeleteCount (delta). Don't conflate them.
6. **Temporal CALENDAR sub-year transforms skip pruning when bounds are unbounded or cross parent periods**
   (B4). `inSameCalendarParentPeriod`. The 4 new tests guard it.
7. **Arrow writer joins the consumer before freeing the allocator** (B5). cleanupAfterFailure does
   join(bounded) before close. interrupt() can't stop a native DuckDB INSERT — order matters.

## Port-specific gotchas learned this session
- **ducklake-catalog test module has NO junit-jupiter-params.** Use plain `@Test` loops, not
  `@ParameterizedTest`/`@ValueSource`. (trino-ducklake tests DO have it.) This bit us once.
- **Verify from on-disk JUnit XML, not stdout**: `jvm/<module>/build/test-results/test/TEST-<fqcn>.xml`
  → tests/failures/errors attrs. Gradle fails the build on any test failure, so BUILD SUCCESSFUL == green,
  but the XML is the source of truth when stdout is noisy.
- **Never `gradle clean`** (incremental is fast; clean is slow and unnecessary). Per PLAN.md.
- **`.java` → `.java.old` (never delete)**; `generated/` never ported; no Gradle changes needed
  (Kotlin already wired via build-logic kotlin.srcDirs). Per PLAN.md conventions.

## The one meta-lesson from BEFORE (apply it during the port's equivalence checks)
**A suspicious line is a hypothesis; the read/write path is the finding.** This session, B3a *looked*
buggy but was read-correct (naive fix = data loss); B2 *looked* benign but was silent data loss. Both
were settled only by tracing the full path end-to-end. During the port's per-file equivalence review,
when something looks wrong in isolation, trace where its inputs come from and where its outputs go before
"fixing" it — the faithful port's job is to preserve behavior, and behavior lives in the path, not the line.

## Run
From `jvm/` (driving agent, reliable channel):
```
Workflow({scriptPath:".claude/workflows/kotlin-port.js", args:{slice:"catalog-test"}})
#   → catalog-main → trino-test → trino-main → {slice:"combine"}
# Checkpoints: args.stopAfter:"port" (after translation) | "kit" (before applying kit)
```
Resume across sessions at SLICE boundaries only — completed slices are durable on disk
(.java.old, baseline.json, prekit/, IdiomKit.kt, kit-notes.json). Don't hand off a mid-flight Workflow.
```
