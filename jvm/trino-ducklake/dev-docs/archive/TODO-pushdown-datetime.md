# TODO: Date/time pushdown — chunk tracker

Pickup-ready task list for step 4 of [TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md). Read this doc, pick a chunk, do the chunk, update the row. The chunks are sequenced — Chunk 2 needs Chunk 1's translator type-gate refactor; Chunk 3 needs Chunk 2's session plumbing.

## Read before any chunk

These three docs together are the full context. The agent doing the work does NOT need to re-derive the empirical results or re-litigate the tier model.

- **[PLAN-pushdown-datetime.md](PLAN-pushdown-datetime.md)** — architecture decisions, tier model, the per-function divergence-pressure-point notes. Source of truth for "why this design."
- **[REPORT-datetime-tz-handling.md](REPORT-datetime-tz-handling.md)** — empirical findings from `ProbeDuckDbTimeZoneHandling`. Validated normaliser table, Quack parity confirmation, the year-boundary smoking gun.
- **[TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md)** — parent context: the discipline ("lossless pushdown only; curated map"), the "Adding a new alias — checklist" at the bottom, the tier/round numbering convention.

## How the pushdown layer is wired today (one-paragraph orientation)

`DucklakeMetadata.applyFilter` calls `DuckDbExpressionTranslator.translateConjuncts(expression, assignments)`, which emits DuckDB SQL fragments for each pushable conjunct. Function calls flow through `translateCall` → if `(name, arity)` is in `PUSHABLE_FUNCTIONS`, emit `trino_<name>(args)`. The `trino_<name>` macros are installed at attach time by `TrinoFunctionAliases.applyDirect(stmt)`, called from both `InProcessDuckDbExecutor.execute` and `QuackDuckDbExecutor.execute`. Lockstep invariants: every macro must have a row in `trino_meta()` (file `trino-function-aliases.sql`), an entry in Java-side `PUSHABLE_FUNCTIONS`, and a semantic fixture in `TestTrinoFunctionAliases#semanticCases()`. Three tests (one each) enforce the invariants.

## Status

- Chunk 1 — ✅ shipped (round 6j). 12 new macros, type-gate registry in translator, 13 unit tests, 3 e2e tests, 775 total tests green. See "Chunk 1 — shipped notes" below.
- Chunk 2 — ✅ shipped. `TrinoTimeZoneNormaliser` in production, threaded through `DucklakePageSourceProvider → DuckDbFilePageSource → ExecutionRequest`, `SET TimeZone` applied at attach in both `InProcessDuckDbExecutor` and `QuackDuckDbExecutor` (via `quack_query_by_name`), 14 normaliser unit tests + 2 executor-level integration tests (positive LA, positive Singapore, parity through Quack, control mismatch) + 1 Trino-runner stability test (Tier A/B results invariant across 4 session zones). 791 total tests green. See "Chunk 2 — shipped notes" below.
- Chunk 3 — ✅ shipped (narrow). Session-property `pushdown_timestamp_with_timezone` (default off), `ConnectorSession` threaded through the translator (overloads preserve test backward compat), gate extension limited to TRULY zone-invariant `to_unixtime`. Zone-dependent extracts (`year`/`month`/`day`/`hour`/`minute`/`second`/`millisecond`/`date_trunc`/`date_diff`) STAY UNPUSHED over WTZ even when property is on — the converter's UTC-hardcoding makes them unsafe (see Chunk 3.5 below). See "Chunk 3 — shipped notes" below.
- Chunk 3.5 — ✅ shipped. `DucklakeArrowToPageConverter` now resolves the Arrow vector's schema TimeZone and constructs WTZ values with it instead of hardcoding `UTC_KEY`. Full Tier C surface (`year`/`month`/`day`/`quarter`/`hour`/`minute`/`second`/`millisecond`/`date_trunc`/`date_diff`) promoted to push over WTZ when property on. **Year-boundary smoking gun fires end-to-end** (`testTierCYearBoundarySmokingGunSingaporeSession`): Singapore session + property on + `WHERE year(ts) = 2025` over a `'2024-12-31 22:00 UTC'` literal matches the row through Trino's full stack. 801 total tests green; zero regressions in the existing WTZ test surface (nothing pinned UTC rendering). See "Chunk 3.5 — shipped notes" below.
- Chunk 4 — ✅ shipped. Added `from_unixtime/1` (DOUBLE → WTZ via DuckDB's `to_timestamp`) and `with_timezone/2` (TIMESTAMP no-TZ + VARCHAR → WTZ via DuckDB's `timezone(zone, t)` — note the arg-order flip). Probed `at_timezone(WTZ, varchar)` and **confirmed not pushable** through this connector: DuckDB's `WTZ AT TIME ZONE 'X'` and `timezone('X', WTZ)` return TIMESTAMP no-TZ, not WTZ — DuckDB's TIMESTAMPTZ has no per-value zone metadata, so the "rezone display" operation is fundamentally not expressible. Documented in the macro SQL and RESEARCH-function-mapping.md. 807 total tests green. Date stuff complete for this iteration — moves to archive next.

### Chunk 1 — shipped notes

The actual implementation differed from the original plan in two places worth recording before Chunk 2 starts:

1. **Trino function names corrected.** The Plan's table mentioned `iso_week`/`iso_year`, but Trino itself does not have those function names — Trino spells them `week`/`week_of_year` (ISO week, both) and `year_of_week`/`yow` (ISO year). Macros shipped under the correct Trino names. RESEARCH-function-mapping.md was already right; the Plan's table was wrong and has been corrected.
2. **DuckDB function names corrected via probe.** The Plan's candidate macro bodies referenced DuckDB functions that do not exist as bare names (`isoweek`, `isoyear`, `last_day_of_month`, `day_of_year`). Probed empirically — DuckDB's `week()` is already ISO-aligned, `last_day` (no `_of_month` suffix) returns the right value, `dayofyear` (single token) works, and there is no bare `isoyear` — reach it via `extract('isoyear' FROM d)::BIGINT`. Macro bodies in `trino-function-aliases.sql` use the actual function names.

Sparse type-gate registry: `TYPE_GATES` is a `Map<NameArity, ArgTypeGate>` that complements `PUSHABLE_FUNCTIONS` (still a `Set<NameArity>` — preserves the existing `testJavaPushableSetMatchesDuckDbMeta` parity guard's contract). Entries without a gate implicitly accept any types (preserves shipped string/numeric/regex behaviour). The Chunk 2 session-property gate for Tier C can layer on top of this same registry — add an `ArgTypeGate` that also consults the session property; no further refactor needed.

### Chunk 2 — shipped notes

Implementation details worth recording for the Chunk 3 agent:

1. **Normaliser API**: `TrinoTimeZoneNormaliser.normalise(String)` returns a string DuckDB *might* accept. Pass-through is used for unknown shapes (fractional bare offsets like `+05:30`); the caller catches DuckDB's `Unknown TimeZone` exception and one-shot-warns. This keeps the "what zones DuckDB knows" knowledge inside DuckDB itself rather than maintaining a list on the Java side. Three-rule contract: `Z` → `UTC`; `±HH:00` → `Etc/GMT∓HH` (POSIX inversion); everything else passes through.
2. **`ExecutionRequest` extended (record)**: added `Optional<String> duckDbTimeZone` as a fifth component, with two new overloaded constructors that default it to `Optional.empty()` so existing test fixtures (`TestDucklakeDuckDbExecutorBackends.bothBackendsHonourEmptyProjection` and similar) continue to compile + pass without modification.
3. **In-process attach hook**: `InProcessDuckDbExecutor.execute` runs `applySessionTimeZone(stmt, request.duckDbTimeZone())` immediately after `TrinoFunctionAliases.applyDirect` and the file ATTACH. One-shot warn per (normalised) zone string on `SQLException`; cached in a `ConcurrentHashMap<String, Boolean>` field — survives across executor instances (a new fresh DuckDB connection per split would otherwise re-fire the warn for every split with a problematic zone).
4. **Quack attach hook**: `QuackDuckDbExecutor.execute` runs `applyServerSideTimeZone(init, request.duckDbTimeZone())` after the server-side `ATTACH`. Uses `drainWrappedQuery(stmt, "SET TimeZone = '...'")` — same `quack_query_by_name`-wrapper pattern the function-aliases loop uses. Persistence across calls in the same client session was the most consequential unknown going in; the executor-level integration test (`sessionTimeZonePropagatesToBothBackends`) pins that persistence is real, with a control case where LA-zone request + Singapore-rendered cast predicate returns zero rows — proves DuckDB consulted the requested zone and not some incidental default.
5. **No connection pool, no key changes**: each split spins a fresh DuckDB connection (Q4 / chunk 0 survey), so per-split `SET TimeZone` carries no cross-session leak risk. The chunk 3 agent can rely on this invariant.
6. **Tier A/B regression check**: `TestDucklakeDuckDbReadMode#testTierABPushdownStableUnderSessionTimeZoneChange` iterates UTC / LA / Berlin / Singapore session zones against the shipped Tier A/B macros (`day_of_week`, `year_of_week`, `hour`) on DATE / TIMESTAMP-no-TZ columns — all four zones produce identical row counts, confirming the chunk 2 plumbing doesn't break wall-clock invariance.

**For chunk 3**: the type-gate registry from chunk 1 is the right hook for the session-property gate. Suggested shape — add an `ArgTypeGate` factory that consults `session.getProperty(PUSHDOWN_TIMESTAMP_WITH_TIMEZONE, Boolean.class)`. The gate sits inside `translateConjuncts`, which today receives `Map<String, ColumnHandle> assignments` but no session. The lightest-touch refactor: extend `translateConjuncts` to also accept the session (or just the boolean property value), thread through `DucklakeMetadata.applyFilter` (which already has the session). Tier C `year/month/day/quarter/hour/minute/second/millisecond/date_trunc/date_diff` then get gates that ALSO accept `TimestampWithTimeZoneType` when the property is on. New Tier C-only entries (`at_timezone`, `with_timezone`, `from_unixtime`) get their own entries with the same conditional gate.

### Chunk 3 — shipped notes

Chunk 3 shipped as a **narrow** Tier C: only `to_unixtime` was promoted to accept `TIMESTAMP WITH TIME ZONE` when the property is on. The reason — discovered while writing the chunk's e2e test — is that the connector's `DucklakeArrowToPageConverter:380-388` hardcodes `UTC_KEY` for every incoming `TIMESTAMPTZ` value:

```java
private static void writeTimestampTz(TimestampWithTimeZoneType type, BlockBuilder builder, long epochMillis, int picosOfMilli)
{
    if (type.isShort()) {
        type.writeLong(builder, packDateTimeWithZone(epochMillis, UTC_KEY));   // ← always UTC
    }
    else {
        type.writeObject(builder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, UTC_KEY));
    }
}
```

That choice means Trino's above-scan `year(timestamptz_col)` always uses **UTC** (per `io.trino.operator.scalar.timestamptz.ExtractYear`, which unpacks the value's zone — which is UTC for everything we hand it). DuckDB's pushed `year(timestamptz_col)` uses **session zone** (per chunk 2's `SET TimeZone` on attach). Whenever the session isn't UTC, those disagree — pushing the predicate would drop or keep rows that Trino's re-eval (which always wins; pushed conjuncts also stay in `remainingExpression`) wouldn't.

`to_unixtime` is the exception because it returns the absolute UTC epoch in both engines regardless of any zone. The chunk-3 gate extension is limited to that one entry.

Files in chunk 3:
- `DucklakeSessionProperties.java` — new `PUSHDOWN_TIMESTAMP_WITH_TIMEZONE` boolean property (default `false`), `isPushdownTimestampWithTimeZone(session)` accessor that tolerates `null` session (test path) as off.
- `DuckDbExpressionTranslator.java` — `ArgTypeGate` signature extended with `ConnectorSession session`; `translateConjuncts/translate` got new overloads taking session (backward-compat overloads delegate with `null`); every internal recursion (`translateOrNull`, `translateCall`, `translateLike`, `translateCast`, `translateStringConcat`, `translateMacroCall`, `joinBinary`) threads session through. The new `argTier(int index)` factory is the WTZ-when-property-on gate, currently bound to `to_unixtime/1`.
- `DucklakeMetadata.applyFilter` — passes `session` into `translateConjuncts`.
- Tests: 7 new translator unit tests covering positive (`to_unixtime` WTZ pushes when property on), negative (off), regression guard (Tier A and Tier B without WTZ stay correct), and explicit non-promotion (`year`/`hour`/`date_trunc`/`date_diff` over WTZ stay unpushed even with property on, with reasoned-out comments). 2 new e2e tests: property-toggle round-trips for `to_unixtime` across UTC / LA / Singapore sessions, and invariance check that `year(WTZ)` results don't shift under the property toggle.

### Parquet-format WTZ asymmetry — known, fix design recorded

**Finding.** Trino's standard `trino-parquet` reader (`io/trino/parquet/reader/ColumnReaderFactory.java:221-235`, `io/trino/parquet/reader/decoders/ValueDecoders.java:1025+`) **hardcodes `UTC_KEY` for all incoming `TIMESTAMP WITH TIME ZONE` values** regardless of parquet's `isAdjustedToUTC` logical-type flag. Same fault mode as our Arrow path pre-3.5. This is an upstream Trino behaviour; we don't own the parquet decoders.

**Practical impact today — modest:**
- `SELECT timestamptz_col FROM parquet_format_table` renders in UTC, has always rendered in UTC; chunk 3.5 didn't regress this.
- Function-shape pushdown does NOT fire on parquet splits (`DucklakePageSourceProvider.createParquetPageSource` ignores `pushedExpressions`), so the chunk 3.5 session property has zero effect on parquet results.
- **New asymmetry introduced by chunk 3.5**: a mixed-format DuckLake table (parquet + duckdb splits in the same logical table) now renders WTZ differently per split. Pre-3.5: both UTC. Post-3.5: parquet UTC, duckdb session-zone. Same WTZ instant, two displays.

**Fix design (deferred to its own chunk):**
Wrap the parquet `ConnectorPageSource` with a `WtzSessionZonePageSource` that rebuilds WTZ blocks with the session zone before yielding pages downstream. Sketch:

```java
// Wraps ParquetPageSource for parquet-format reads. For each output Page, looks at
// each column position; if the column type is TimestampWithTimeZoneType, rebuilds
// the block by reading each packed (epochMillis, UTC_KEY) value and writing
// (epochMillis, sessionZone) back. Short variant: unpack/repack the long.
// Long variant: read LongTimestampWithTimeZone, construct a new one with the
// session zone preserving picosOfMilli.
final class WtzSessionZonePageSource implements ConnectorPageSource {
    private final ConnectorPageSource inner;
    private final TimeZoneKey sessionZone;
    private final int[] wtzColumnIndexes;
    private final TimestampWithTimeZoneType[] wtzColumnTypes;
    // ... iterate pages, rebuild WTZ blocks via BlockBuilder
}
```

Wire it into `DucklakePageSourceProvider.createParquetPageSource` just before returning. Existing tests that don't assert WTZ rendering pass unchanged; new tests pin the round-trip in parquet format.

**Why deferred** — the wrap is ~150-250 lines + dedicated test coverage for both `ShortTimestampWithTimeZone` and `LongTimestampWithTimeZone` rebuild paths. It's also redundant with a future upstream Trino fix (which would honour `isAdjustedToUTC` natively). Either path closes the asymmetry; pick whichever is reachable when prioritized.

**Workaround for users hitting the asymmetry today**: keep DuckLake tables single-format, OR `SET SESSION pushdown_timestamp_with_timezone = false` to keep Tier C predicates above the scan (Trino-side eval is UTC-aligned for parquet AND duckdb in that case, so they agree).

### Chunk 3.5 — converter fix unblocks full Tier C

**Why this chunk.** Chunk 3 shipped only `to_unixtime` for Tier C because the converter blocks the rest. Fixing the converter is the lever that unlocks `year`/`month`/`day`/`hour`/`minute`/`second`/`millisecond`/`date_trunc`/`date_diff` over `TIMESTAMP WITH TIME ZONE`. It's also probably a latent user-visible correctness bug — `SELECT timestamptz_col FROM t` rendering in UTC regardless of session is the outlier behaviour vs. other Trino connectors (Iceberg, Hive, Parquet's `isAdjustedToUtc=true`).

**Empirical foundation.** Probed against the same DuckDB JDBC bundle the connector uses:

```
session=UTC                    schema-field-type=Timestamp(MICROSECOND, UTC)
session=America/Los_Angeles    schema-field-type=Timestamp(MICROSECOND, America/Los_Angeles)
session=Asia/Singapore         schema-field-type=Timestamp(MICROSECOND, Asia/Singapore)
session=Etc/GMT-5              schema-field-type=Timestamp(MICROSECOND, Etc/GMT-5)
session=Etc/GMT+8              schema-field-type=Timestamp(MICROSECOND, Etc/GMT+8)
session=Asia/Kolkata           schema-field-type=Timestamp(MICROSECOND, Asia/Kolkata)
session=Pacific/Chatham        schema-field-type=Timestamp(MICROSECOND, Pacific/Chatham)
```

DuckDB reliably sets the Arrow schema-field TZ to whatever the session `TimeZone` is at export time. Every shape is a valid `TimeZoneKey` string. Chunk 2 already set DuckDB's session TimeZone to match Trino's session zone on attach, so the Arrow schema TZ on every incoming WTZ vector IS the session zone — exactly what we want to propagate into Trino's WTZ value.

### Implementation plan

| File | Change |
|---|---|
| `src/dev/brikk/ducklake/trino/plugin/DucklakeArrowToPageConverter.java` | In `writeTimestampTzColumn(...)`: read the Arrow vector's field metadata (`vector.getField().getType()` is `ArrowType.Timestamp` with a `getTimezone()` string). Parse that string to `TimeZoneKey` via `TimeZoneKey.getTimeZoneKey(...)`. Pass to `writeTimestampTz(...)` instead of the hardcoded `UTC_KEY`. Fallback: `UTC_KEY` when the TZ is null or unparseable (defensive — should not happen on our read path, but it makes the change robust). |
| `src/dev/brikk/ducklake/trino/plugin/DuckDbExpressionTranslator.java` | Promote `year/month/day/quarter/hour/minute/second/millisecond/date_trunc/date_diff` gates to accept `TimestampWithTimeZoneType` when the chunk-3 session property is on (reuse `argTier(...)`). Remove the explicit "no WTZ even with property on" notes added in chunk 3 for these entries. |
| `test/src/dev/brikk/ducklake/trino/plugin/TestDuckDbExpressionTranslator.java` | Flip the chunk-3 `testYearOnTimestampWithTimeZoneDoesNotPushEvenWhenPropertyOn` / hour / date_trunc / date_diff tests to assert the OPPOSITE: predicate DOES push when property on. Pin the SQL. |
| `test/src/dev/brikk/ducklake/trino/plugin/TestDucklakeDuckDbReadMode.java` | The year-boundary smoking gun finally works through Trino: Singapore session + property on + `WHERE year(ts) = 2025` on `'2024-12-31 22:00 UTC'` literal must return that row. Land this test (it was attempted in chunk 3 and removed because the converter blocked it). Also: pin that `SELECT ts FROM t` renders with the session zone (was UTC before this chunk — that's the user-visible behaviour change). |
| Audit | Any existing test that pins a WTZ rendering or compares WTZ Trino-side `year()` results against a UTC expectation. The chunk-3-shipped tests are deliberately invariant-under-toggle (no rendering pins), so the blast radius from chunk 3 is zero. Older tests may pin UTC renderings; those need their expected values shifted to whatever the session TZ produces. |

### Acceptance criteria

- [ ] Converter constructs WTZ values with the Arrow schema TZ as `TimeZoneKey`. Defensive fallback to `UTC_KEY` when missing/unparseable.
- [ ] Promoted Tier C entries push when property on, stay unpushed when off. Unit tests pin both branches.
- [ ] Year-boundary smoking gun e2e: Singapore session + property on + `WHERE year(ts) = 2025` returns the row that's 2024-12-31 22:00 UTC.
- [ ] Same query with property OFF returns the same row (Trino above-scan eval matches because the WTZ value carries the session zone now).
- [ ] No other test regresses; if any existing test pinned a UTC rendering, its expected value gets shifted and the comment explains the behaviour change.
- [ ] `RESEARCH-function-mapping.md` Tier-C-promoted rows updated.
- [ ] `TODO-pushdown-datetime.md` Chunk 3.5 row marked `✅ shipped`. Suggested follow-ups (deferred to their own chunks): genuinely new Tier C entries (`from_unixtime`, `at_timezone`, `with_timezone`).

### Gotchas / pressure points

- **User-visible behaviour change**: `SELECT timestamptz_col FROM duckdb_table` will render in the session zone instead of UTC. Document this clearly in `TODO-pushdown-duckdb.md` Status and in the commit message. Probably not what users were relying on (UTC-only rendering is the surprising bit), but call it out.
- **Type-of vector check**: `vector.getField().getType()` only returns `ArrowType.Timestamp` for actual timestamp vectors. Don't try to read the TZ on a non-timestamp vector — guard with `instanceof ArrowType.Timestamp` before the cast.
- **`TimeZoneKey.getTimeZoneKey(...)` throws** for unknown zones. Catch and fall back to `UTC_KEY` with a one-shot WARN — matches the chunk-2 `SET TimeZone` failure pattern.
- **Parity test**: `TestDucklakeDuckDbExecutorBackends.sessionTimeZonePropagatesToBothBackends` from chunk 2 uses a CAST-to-VARCHAR predicate to observe DuckDB-side zone behaviour. After 3.5, the same predicate works whether through DuckDB or Trino — chunk 2's test should still pass because we don't read the WTZ data, only the cast-string column.

### What does NOT belong in Chunk 3.5

- New Tier C function entries (`from_unixtime`, `at_timezone`, `with_timezone`) — each needs its own probe + semantic fixture before promotion. Track as Chunk 4+ if a need surfaces.
- Promoting Tier C to default-on. Burn-in first; flip the property's default in a follow-up commit.
- Changing the converter for non-WTZ types. Out of scope.

### Chunk 3.5 — shipped notes

Smoking gun confirmed: `testTierCYearBoundarySmokingGunSingaporeSession` runs the full Trino → connector → DuckDB → Arrow → connector → Trino loop with Singapore session + property on + `WHERE year(ts) = 2025` over a `2024-12-31 22:00 UTC` literal. Pre-3.5 the same query returned 0 rows (Trino above-scan year() always UTC-aligned, dropping the row DuckDB pushed). Post-3.5 it returns the row, and property-off returns the same row too (invariance under toggle).

Files changed:
- `src/dev/brikk/ducklake/trino/plugin/DucklakeArrowToPageConverter.java` — `writeTimestampTzColumn` resolves a `TimeZoneKey` once per column from `vector.getField().getType()` cast to `ArrowType.Timestamp.getTimezone()`. Passes through `writeTimestampTz` as a new parameter. Defensive fallback to `UTC_KEY` on null TZ or unparseable string with one-shot WARN per zone string (`UNPARSEABLE_TZ_WARNED` ConcurrentHashMap field, matches the chunk-2 pattern). Added `Logger` field.
- `src/dev/brikk/ducklake/trino/plugin/DuckDbExpressionTranslator.java` — `buildTypeGates()` unified the Tier B and Tier C codepaths into a single `argTier(...)` call for `year/month/day/quarter/hour/minute/second/millisecond/to_unixtime/date_trunc/date_diff`. Pre-3.5 these were split: `to_unixtime` got the conditional gate, the rest were locked to DATE+TIMESTAMP. Post-3.5 all share the same gate. Tier A (`day_of_week`/`week`/`year_of_week`/etc.) still DATE-only because Trino's signature doesn't accept WTZ for those either.
- `test/src/dev/brikk/ducklake/trino/plugin/TestDuckDbExpressionTranslator.java` — 5 chunk-3 unit tests flipped from `assertThat(conjuncts).isEmpty()` to `containsExactly(...)`, asserting the WTZ predicates DO push when property on. New `testYearOnTimestampWithTimeZoneStillBlockedWhenPropertyOff` regression guard.
- `test/src/dev/brikk/ducklake/trino/plugin/TestDucklakeDuckDbReadMode.java` — added `testTierCYearBoundarySmokingGunSingaporeSession`: the canonical year-boundary case Trino + DuckDB now agree on, with property-toggle invariance asserted.

User-visible behaviour change to call out in the commit / release notes: `SELECT timestamptz_col FROM duckdb_table` now renders in the session zone (matches Iceberg / Hive / Parquet `isAdjustedToUtc=true` connectors) instead of always UTC. No test in the suite pinned the previous UTC rendering, so this surfaces only when users observe the changed display. The instant carried by each value is unchanged — the byte content on the wire, comparisons, and predicate semantics are equivalent under the previous UTC interpretation, just rendered differently.

The Quack-server-side path also benefits: `TestDucklakeDuckDbExecutorBackends.sessionTimeZonePropagatesToBothBackends` continues to pass without modification — that test uses CAST-to-VARCHAR predicates that DuckDB computes server-side, and the cast string formatting was always controlled by DuckDB's session TimeZone (chunk 2). The converter change only affects how Trino constructs WTZ values from incoming Arrow, which that test doesn't exercise.

What's left for future chunks:
- ✅ **Default-on flip for `pushdown_timestamp_with_timezone` — DONE (2026-06-06).** Burn-in complete; the property now defaults to `true` in `DucklakeSessionProperties`. README + parent TODO updated. Set to `false` to keep Tier C predicates above the scan.
- **Parquet-format WTZ path — confirmed UTC-hardcoded, fix deferred** (see "Parquet-format WTZ asymmetry" section below for the full audit and the fix design).

---

## Chunk 1 — Translator type gate + Tier A (DATE) + Tier B (TIMESTAMP no-TZ)

**Why this chunk.** Closes a latent bug AND ships the largest tractable surface in one push. The existing `year/month/day/quarter/date_trunc/date_diff` entries push regardless of argument type — feed them a `TIMESTAMP WITH TIME ZONE` column and they emit SQL that DuckDB will execute against whatever the (session-uncontrolled) TimeZone happens to be. The type-gate refactor is the minimal infra change that lets us safely (a) keep the existing entries restricted to safe types and (b) add a meaningful list of new ones in the same PR.

**No new infrastructure required outside the translator.** No session plumbing, no executor changes, no session properties. Just macros + a richer pushable registry.

### Files to touch

| File | What |
|---|---|
| `src/dev/brikk/ducklake/trino/plugin/DuckDbExpressionTranslator.java` | Replace `Set<NameArity>` with a richer per-entry registry that carries an argument-type predicate. Suggested shape: `record PushableEntry(NameArity nameArity, ArgTypeGate gate) {}` + `Map<NameArity, ArgTypeGate>`. `ArgTypeGate` is a `Predicate<List<Type>>` matched against the arg types at translation time. Default for the existing string/numeric entries: accept-anything (preserves shipped behaviour). For new and re-gated date entries: explicit type sets. |
| `resources/dev/brikk/ducklake/trino/plugin/trino-function-aliases.sql` | Add new macros (list below). Add corresponding `trino_meta()` rows. Mirror the round-6g/6i comment style for context. |
| `test/src/dev/brikk/ducklake/trino/plugin/TestTrinoFunctionAliases.java` | Add semantic fixtures for every new macro (one or more `c("...", new NameArity(name, arity), "SELECT trino_<name>(...)", expected)` row per entry). The `testMetaMatchesFixtures` parity guard will fail CI otherwise. Pick fixture inputs from the corpus in `PLAN-pushdown-datetime.md` — DST transitions, calendar boundaries, ISO-year ≠ calendar-year. |
| `test/src/dev/brikk/ducklake/trino/plugin/TestDuckDbExpressionTranslator.java` | New tests pinning the type gate. At minimum: `year(DATE col)` pushes; `year(TIMESTAMP col)` pushes; `year(TIMESTAMP WITH TIME ZONE col)` does NOT push (whole conjunct dropped). Same triad for `date_trunc`, `date_diff`. Plus one positive test per new function shape. |
| `test/src/dev/brikk/ducklake/trino/plugin/TestDucklakeDuckDbReadMode.java` | Add 2–3 end-to-end tests against the duckdb-format read path. Suggested: `WHERE day_of_week(date_col) = 7` (ISO Sunday — pins the `isodow` macro choice); `WHERE iso_year(date_col) = 2025 AND year(date_col) = 2024` (ISO ≠ calendar smoking gun, `2024-12-30`); `WHERE hour(ts_col) = 22 AND year(ts_col) = 2024` on a TIMESTAMP (no TZ) column to pin Tier B. |
| `dev-docs/archive/RESEARCH-function-mapping.md` | Flip the "Done" column for every function shipped. Use the same `yes step 4` flag convention added for LIKE in step 1. |
| `dev-docs/TODO-pushdown-duckdb.md` | Update step-3 line's "Not yet shipped" list. Update the catalog totals in the "Round 6+" header. |
| **This file** | Mark Chunk 1 row in the Status section as `✅ shipped` with PR / commit ref. |

### Macros to add (with macro body candidates)

Tier A — gated to **DATE only**:

| Trino | DuckDB macro body | Fixture inputs to cover |
|---|---|---|
| `day_of_week(date) -> bigint` | `isodow(d)` | `'2024-01-07'` (Sun → 7, NOT 0 which is `dayofweek`); `'2024-01-08'` (Mon → 1) |
| `day_of_year(date) -> bigint` | `dayofyear(d)` | `'2024-02-29'` (leap → 60); `'2024-12-31'` (366 in leap year) |
| `last_day_of_month(date) -> date` | `last_day(d)` | `'2024-02-15'` → `'2024-02-29'` (leap); `'1900-02-15'` → `'1900-02-28'` (non-leap century) |
| `week(date) -> bigint` | `isoweek(d)` | `'2024-01-01'` (Mon → ISO week 1); `'2023-01-01'` (Sun → ISO week 52, of 2022!) |
| `week_of_year(date) -> bigint` | `isoweek(d)` | same as week; this is Trino's alias |
| `iso_week(date) -> bigint` | `isoweek(d)` | `'2024-12-30'` → ISO week 1 (the trap) |
| `iso_year(date) -> bigint` | `isoyear(d)` | `'2024-12-30'` → 2025 (calendar year 2024 but ISO year 2025) |

Tier B — gated to **{DATE, TIMESTAMP}**:

| Trino | DuckDB macro body | Fixture inputs |
|---|---|---|
| `hour(timestamp) -> bigint` | `hour(t)` | `TIMESTAMP '2024-12-31 22:30:45'` → 22 (wall clock) |
| `minute(timestamp) -> bigint` | `minute(t)` | → 30 |
| `second(timestamp) -> bigint` | `second(t)` | → 45 |
| `millisecond(timestamp) -> bigint` | `(epoch_ms(t) % 1000)::BIGINT` or `extract('millisecond' FROM t)::BIGINT` — verify which matches Trino's `millisecond()` semantics (Trino returns the milliseconds-component-of-second, not total millis) | `TIMESTAMP '2024-06-15 12:00:00.123'` → 123 |
| `to_unixtime(timestamp) -> double` | `epoch(t)::DOUBLE` | `TIMESTAMP '1970-01-01 00:00:00'` → 0.0; `TIMESTAMP '1969-12-31 23:59:59'` → -1.0 |

Re-gate (no SQL change, just add the type predicate to the existing entries):

| Function | New gate |
|---|---|
| `year/month/day/quarter/1` | `{DATE, TIMESTAMP}` |
| `date_trunc/2` (unit varchar + date/ts) | arg1 ∈ `{DATE, TIMESTAMP}` |
| `date_diff/3` (unit varchar + 2× date/ts of same kind) | arg1, arg2 ∈ `{DATE, TIMESTAMP}` |

**NOT in this chunk** (kicked to Chunk 3):
- Anything taking or returning `TIMESTAMP WITH TIME ZONE`. Notably: `from_unixtime` (returns WTZ in Trino), `at_timezone`, `with_timezone`, `current_timestamp`. These need the session-TZ plumbing from Chunk 2.

### Type-gate registry design (recommendation, not mandate)

```java
// Sketch — replaces the bare Set<NameArity>.
interface ArgTypeGate {
    boolean accepts(List<Type> argTypes);
    ArgTypeGate ANY = args -> true;
    static ArgTypeGate argOneOf(int index, Class<?>... allowed) { ... }
}

static final Map<NameArity, ArgTypeGate> PUSHABLE_FUNCTIONS = Map.ofEntries(
    Map.entry(new NameArity("length", 1),       ArgTypeGate.ANY),
    // ... all existing string/numeric/regex entries default to ANY (preserves behaviour)
    Map.entry(new NameArity("year", 1),         ArgTypeGate.argOneOf(0, DateType.class, TimestampType.class)),
    Map.entry(new NameArity("day_of_week", 1),  ArgTypeGate.argOneOf(0, DateType.class)),
    Map.entry(new NameArity("hour", 1),         ArgTypeGate.argOneOf(0, DateType.class, TimestampType.class)),
    ...);
```

In `translateCall`, after the `(name, arity)` lookup hit, evaluate `gate.accepts(args.stream().map(ConnectorExpression::getType).toList())` and return null if it doesn't. The `testJavaPushableSetMatchesDuckDbMeta` parity check needs to keep working; it only inspects the keyset, which is unchanged.

### Done when

- [ ] All listed macros land in `trino-function-aliases.sql` AND `trino_meta()` AND `PUSHABLE_FUNCTIONS` AND `TestTrinoFunctionAliases#semanticCases()`. The four lockstep invariants don't break.
- [ ] `testJavaPushableSetMatchesDuckDbMeta`, `testMetaMatchesFixtures`, `testTrinoMacroSemantics` all green.
- [ ] New translator unit tests cover: positive (correct type pushes); negative (`TIMESTAMP WITH TIME ZONE` does NOT push); wrong-arity (still doesn't push). At least three test methods per re-gated function.
- [ ] 2–3 end-to-end tests in `TestDucklakeDuckDbReadMode` exercise the new macros on actual data, mirroring `testFunctionPredicatePushesDownThroughTrinoMacro`'s shape.
- [ ] Full test suite green (750+ tests at the time of writing — grow with the new additions).
- [ ] `RESEARCH-function-mapping.md` "Done" column updated for every shipped function.
- [ ] `TODO-pushdown-duckdb.md` step-3 line + catalog totals updated.
- [ ] This file's Status section: Chunk 1 row marked `✅ shipped` with commit hash.

### Gotchas / pressure points (from the PLAN doc — not exhaustive)

- **Week numbering trap**: DuckDB's `week()` is non-ISO in some versions / settings. Use `isoweek()` explicitly in the macro body. Test fixtures must include `'2023-01-01'` (Sunday → ISO week 52 of 2022, NOT week 1).
- **Day-of-week numbering trap**: DuckDB's `dayofweek()` is 0=Sun, 6=Sat. Trino is 1=Mon, 7=Sun. Use `isodow()` in the macro. Fixture: `'2024-01-07'` Sunday → 7, NOT 0.
- **`millisecond()` ambiguity**: Trino returns the millis-OF-SECOND (0–999), not total millis since epoch. Pin the semantic by probing both engines on `TIMESTAMP '2024-06-15 12:00:00.123'` before locking the macro body.
- **`date_trunc('week', ...)`**: Monday-start in both engines if we use `date_trunc('week', d)` — but verify with `'2024-01-07'` (Sunday) → should return `'2024-01-01'` (the Monday).
- **`date_diff` cross-boundary semantics**: Pin `date_diff('month', '2024-01-31', '2024-02-29')` = 1 and `date_diff('year', TIMESTAMP '1999-12-31 23:59:59', TIMESTAMP '2000-01-01 00:00:01')` = 1.
- **Type-gate fall-through**: anything currently pushable that DIDN'T have an explicit gate must default to `ANY` so behaviour is preserved. The refactor is supposed to be invisible to existing macros.

---

## Chunk 2 — Session-TZ plumbing + normaliser in production

**Why this chunk.** Closes the portability bomb identified in Q3 (DuckDB inherits JVM system TZ — `America/Costa_Rica` on a dev box, UTC on a CI worker, host-default in prod). Even Tier A/B work benefits from a deterministic baseline. Required prereq for Chunk 3 (Tier C).

**No new macros**, no new pushable entries. Pure plumbing + the normaliser. Touches more files than Chunk 1 because the session has to thread through five classes that don't currently know about it.

### Files to touch

| File | What |
|---|---|
| **New: `src/dev/brikk/ducklake/trino/plugin/TrinoTimeZoneNormaliser.java`** | Lift the validated three-rule normaliser out of `ProbeDuckDbTimeZoneHandling#normaliseTrinoZoneForDuckDb` into production. Public static method `String normalise(String trinoZoneId)`. Same rules: `Z` → `UTC`; `±HH:00` → `Etc/GMT∓HH` (POSIX sign inversion); else passthrough. **Do not add an "is acceptable" predicate** — let DuckDB's `SET TimeZone` failure be the actual gate, which we observed is clean and useful (`Unknown TimeZone 'X'!`). |
| **New: `test/src/dev/brikk/ducklake/trino/plugin/TestTrinoTimeZoneNormaliser.java`** | Unit tests covering the 14 representative shapes from REPORT-datetime-tz-handling.md Q3 table. Include the fractional-offset passthrough (so the caller knows DuckDB will reject and the connector can fall back). |
| `src/dev/brikk/ducklake/trino/plugin/DucklakeDuckDbExecutor.java` | Add a field to `ExecutionRequest`: `Optional<String> normalisedDuckDbTimeZone`. (Optional so callers that genuinely have no session — there are none in production but a few test fixtures — can omit it.) |
| `src/dev/brikk/ducklake/trino/plugin/DucklakePageSourceProvider.java` | At `createPageSource` (around line 145–211), read `session.getTimeZoneKey().getId()`, normalise, attach to the `ExecutionRequest`. |
| `src/dev/brikk/ducklake/trino/plugin/DuckDbFilePageSource.java` | Plumb the normalised zone through to `ExecutionRequest` construction (the page source builds it). |
| `src/dev/brikk/ducklake/trino/plugin/InProcessDuckDbExecutor.java` | After `TrinoFunctionAliases.applyDirect(attachStmt)` at line 70, run `SET TimeZone = '<normalised>'` if the request has one. On failure, log a one-shot WARN per (session, zone) tuple and proceed without setting it. Document the implication: Tier C correctness is now compromised for the rest of this attach, but Tier A/B is unaffected. |
| `src/dev/brikk/ducklake/trino/plugin/QuackDuckDbExecutor.java` | Mirror in the server-side init loop around line 113. The `quack_query_by_name` wrapper from the existing `aliasSql` loop is the right pattern — append a `"SET TimeZone = '...'"` statement after `TrinoFunctionAliases.statements()` are shipped. |
| `test/src/dev/brikk/ducklake/trino/plugin/TestDucklakeDuckDbReadMode.java` | New e2e test: session zone explicitly set to `Asia/Singapore`; `TIMESTAMPTZ '2024-12-31 22:00:00 UTC'` literal in a CTAS; assert Trino sees year 2025 (via Trino-side eval), and assert it again with the connector property `ducklake.test.force_pushdown` (or however current tests force pushdown) to confirm DuckDB's pushed extract returns the same. Note: Tier C functions don't push yet — but the SET TimeZone itself can be observed by rendering a TIMESTAMPTZ literal via a non-extract path, or by relying on Chunk 3's tests to find the regression. |
| **This file** | Chunk 2 → `✅ shipped`. |

### Acceptance check — the "is SET TimeZone really firing on Quack" gotcha

The Quack server-side state is shared across sessions on the same DuckDB instance (per `QuackDuckDbExecutor` class-doc comment). `SET TimeZone` is a session-level setting in DuckDB — does it survive across Quack RPC calls within the same client connection, or does each `quack_query_by_name(...)` call land in a fresh server-side session?

The `ProbeDuckDbTimeZoneHandling#probeQ4b_quackContainerParity` empirically established that `SET TimeZone` followed by `SELECT year(...)` in two separate `quack_query_by_name` calls works — the zone persists. **Re-confirm this in production code path**, because that probe used a fresh local client connection per probe; the executor reuses one connection across many wrapper calls. If `SET TimeZone` doesn't persist, fall back to prepending it to every `quack_query_by_name` payload via a multi-statement string.

### Done when

- [ ] `TrinoTimeZoneNormaliser` + unit tests for all 14 REPORT cases.
- [ ] Both executors set `TimeZone` at attach when the request provides one.
- [ ] One e2e test pins that the session zone reaches DuckDB and affects results (even if it's just a TIMESTAMPTZ-render check, not a Tier C pushed function).
- [ ] Quack-path SET TimeZone persistence across `quack_query_by_name` calls verified — either by a probe extension or by passing the e2e test on the QUACK backend (`-Dducklake.test.catalog-backend=DUCKDB_QUACK`).
- [ ] Full suite green.
- [ ] `PLAN-pushdown-datetime.md` "Open questions — answered" stays accurate; if any of the open follow-up probes from REPORT (DST history accuracy, mid-query session-zone change) revealed surprises during this chunk, update both PLAN and REPORT.
- [ ] This file: Chunk 2 row marked `✅ shipped`.

---

## Chunk 3 — Tier C (TIMESTAMP WITH TIME ZONE) + session property + cleanup

**Why this chunk.** Final layer. With Chunk 2's plumbing in place, Tier C is mostly (a) extending type gates to also accept `TimestampWithTimeZoneType`, (b) adding the WTZ-specific functions that take/return WTZ, and (c) adding a session-property gate so we can ship default-off and flip default-on after burn-in.

### Files to touch

| File | What |
|---|---|
| `src/dev/brikk/ducklake/trino/plugin/DucklakeSessionProperties.java` | Add `pushdown_timestamp_with_timezone` boolean session property. Default `false`. |
| `src/dev/brikk/ducklake/trino/plugin/DucklakeMetadata.java` (or `DuckDbExpressionTranslator`) | Wire the session-property check into the translator's gate. Likely cleanest: pass the property value into `translateConjuncts` so the type-gate registry can consult it. WTZ entries become "accept only if session property is on." |
| `resources/dev/brikk/ducklake/trino/plugin/trino-function-aliases.sql` | Add macros that take WTZ args. Re-extend `year/month/day/quarter/date_trunc/date_diff` gates to include `TIMESTAMP WITH TIME ZONE`. Add the WTZ-specific ones: `at_timezone(ts, zone)`, `with_timezone(ts, zone)`, `from_unixtime(double)` (returns WTZ). |
| `test/src/dev/brikk/ducklake/trino/plugin/TestTrinoFunctionAliases.java` | Semantic fixtures for every Tier C entry. **Crucially**: include the year-boundary smoking gun `TIMESTAMPTZ '2024-12-31 22:00:00+00'` extracted in Singapore session → year 2025. |
| `test/src/dev/brikk/ducklake/trino/plugin/TestDuckDbExpressionTranslator.java` | Unit tests pinning the property gate: with property off, `year(timestamptz_col)` does NOT push; with on, it does. |
| `test/src/dev/brikk/ducklake/trino/plugin/TestDucklakeDuckDbReadMode.java` | E2e: TIMESTAMPTZ column, session zone = Singapore, `WHERE year(ts) = 2025` with the literal at `2024-12-31 22:00 UTC`. Run twice — property off (Trino re-evaluates above scan, still returns the right row) and property on (DuckDB pushes, still returns the right row). Same result, different code path. |
| `test/src/dev/brikk/ducklake/trino/plugin/ProbeDuckDbTimeZoneHandling.java` | **Delete this file.** All findings are now baked into shipped tests + REPORT. Matches the `ProbeConcatNullHandling` precedent. |
| `dev-docs/archive/RESEARCH-function-mapping.md` | Flip "Done" column for Tier C entries. |
| `dev-docs/TODO-pushdown-duckdb.md` | Step 4 → ✅ shipped. Update catalog totals. |
| `PLAN-pushdown-datetime.md` (sibling in archive/) | Add a "Shipped" section pointing at the commits / PR. |
| **This file** | Chunk 3 → `✅ shipped`. |

### The session-property flip

Ship `pushdown_timestamp_with_timezone` default-`false`. Let the broader test suite + a real workload run for a release with the property explicitly enabled (via CI matrix or staging env). After that burn-in, flip the default to `true` in a follow-up commit. The cost of the conservative default is one extra session property to set in environments that want Tier C; the benefit is that no Tier C correctness bug can escape to a user unless they opt in.

### Done when

- [ ] All Tier C macros + fixtures land. Lockstep guards still green.
- [ ] Session property toggles pushdown observably (test pinning both code paths).
- [ ] Year-boundary smoking gun green through both backends with property on.
- [ ] `ProbeDuckDbTimeZoneHandling` deleted.
- [ ] All docs (`PLAN`, `RESEARCH`, `TODO-pushdown-duckdb`, this file) reflect shipped state.
- [ ] This file: Chunk 3 row marked `✅ shipped`.
- [ ] **Out of scope**: flipping the property default to `true` — that's a follow-up after burn-in.

---

## Notes for any agent picking this up

- The "Adding a new alias — checklist" at the bottom of [TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md) is the canonical four-step lockstep. Follow it for every new macro.
- The patterns to mirror: LIKE pushdown (translator branch, type-aware, NOT a macro) and concat → `||` (translator rewrite). Both shipped in step 1. Recent commits: `d0cd5c9 like / not like pushing downing`, `5338fdc concat pushdown, needs to use || to avoid difference in null behavior`.
- `TestDucklakeDuckDbReadMode#testFunctionPredicatePushesDownThroughTrinoMacro` is the canonical end-to-end test shape for a duckdb-format read-path pushdown test. Copy its structure.
- If you discover an unexpected semantic divergence while writing fixtures, **stop and write a probe** (mirror `ProbeDuckDbTimeZoneHandling`'s shape). Don't ship a fixture whose expected value you guessed. Add the finding to REPORT-datetime-tz-handling.md or a sibling REPORT file.
- The work is well-scoped; each chunk should land in a single focused commit (or PR), green on the full suite. If it grows, that's a signal to split — not to barrel through.
