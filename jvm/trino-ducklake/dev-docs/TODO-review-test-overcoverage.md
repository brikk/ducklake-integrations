# TODO: Review Test Over-Coverage (trino-ducklake)

Date: 2026-05-07
Owner: unassigned
Status: open

## Context

Quick audit of the JVM test suites in this repo found one clear duplication area and one likely trim area:

- `jvm/ducklake-catalog/test/src`: ~31 tests (mostly lean and high-signal)
- `jvm/trino-ducklake/test/src`: ~553 tests (many integration tests; some intentional overlap)

The suite is not broadly wasteful, but there are targeted opportunities to reduce maintenance and runtime cost.

## High-confidence findings

### 1) Duplicate view coverage across two classes

`TestDucklakeViewDialectFiltering` duplicates behavior already covered by `TestDucklakeViewIntegration`.

- Hidden DuckDB views check:
  - `.../TestDucklakeViewDialectFiltering.java:42`
  - `.../TestDucklakeViewIntegration.java:52`
- Trino-created view visibility check:
  - `.../TestDucklakeViewDialectFiltering.java:53`
  - `.../TestDucklakeViewIntegration.java:82`

Impact: low incremental signal, extra class setup/maintenance.

Proposed action:
- Merge/remove `TestDucklakeViewDialectFiltering` and keep equivalent assertions in `TestDucklakeViewIntegration`.

### 2) `TestDucklakeReadSmoke` includes several generic SQL engine checks

Some cases in `TestDucklakeReadSmoke` are weakly connector-specific and mostly validate Trino SQL behavior:

- `testFalsePredicateReturnsEmpty` (`.../TestDucklakeReadSmoke.java:298`)
- `testLimitZero` (`.../TestDucklakeReadSmoke.java:304`)
- `testSelectConstant` (`.../TestDucklakeReadSmoke.java:318`)
- `testCaseExpression` (`.../TestDucklakeReadSmoke.java:324`)
- `testCastExpression` (`.../TestDucklakeReadSmoke.java:335`)
- `testWithClause` (`.../TestDucklakeReadSmoke.java:361`)

There is already broader query-shape coverage in dedicated suites, e.g. `TestDucklakePlannerAndJoins`.

Impact: canary value exists, but the set is broader than needed for connector smoke checks.

Proposed action:
- Keep smoke tests that are clearly connector-facing (catalog/schema discovery, metadata tables, basic scans/predicates over fixtures).
- Move/remove generic SQL-semantic tests unless they catch a known connector regression mode.

## Suggested execution plan (for follow-up agent)

1. De-duplicate view tests
   - Remove or fold `TestDucklakeViewDialectFiltering` into `TestDucklakeViewIntegration`.
   - Ensure no behavior loss (same assertions retained once).

2. Trim `ReadSmoke` to connector canaries
   - Create a short keep/remove table per test method.
   - Keep only methods that validate connector contracts (metadata visibility, fixture reads, connector-specific system tables).

3. Validate impact
   - Run `:trino-ducklake:test` and compare:
     - test count delta
     - runtime delta
     - any flakiness change

## Acceptance criteria

- No duplicated view behavior across multiple classes.
- `ReadSmoke` is intentionally scoped and documented as connector canary coverage.
- Full `trino-ducklake` tests still pass.
- A short changelog note explains what was removed and why.

## Notes

- This is a static review note; no execution-time profiling was done in the audit.
- There are also 20 classes marked `@Execution(ExecutionMode.SAME_THREAD)`, which may affect runtime, but that was not classified as over-coverage by itself.
