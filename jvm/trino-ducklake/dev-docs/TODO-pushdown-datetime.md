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
- Chunk 2 — ⏳ not started (no longer blocked).
- Chunk 3 — ⏳ not started (blocked by Chunk 2).

### Chunk 1 — shipped notes

The actual implementation differed from the original plan in two places worth recording before Chunk 2 starts:

1. **Trino function names corrected.** The Plan's table mentioned `iso_week`/`iso_year`, but Trino itself does not have those function names — Trino spells them `week`/`week_of_year` (ISO week, both) and `year_of_week`/`yow` (ISO year). Macros shipped under the correct Trino names. RESEARCH-function-mapping.md was already right; the Plan's table was wrong and has been corrected.
2. **DuckDB function names corrected via probe.** The Plan's candidate macro bodies referenced DuckDB functions that do not exist as bare names (`isoweek`, `isoyear`, `last_day_of_month`, `day_of_year`). Probed empirically — DuckDB's `week()` is already ISO-aligned, `last_day` (no `_of_month` suffix) returns the right value, `dayofyear` (single token) works, and there is no bare `isoyear` — reach it via `extract('isoyear' FROM d)::BIGINT`. Macro bodies in `trino-function-aliases.sql` use the actual function names.

Sparse type-gate registry: `TYPE_GATES` is a `Map<NameArity, ArgTypeGate>` that complements `PUSHABLE_FUNCTIONS` (still a `Set<NameArity>` — preserves the existing `testJavaPushableSetMatchesDuckDbMeta` parity guard's contract). Entries without a gate implicitly accept any types (preserves shipped string/numeric/regex behaviour). The Chunk 2 session-property gate for Tier C can layer on top of this same registry — add an `ArgTypeGate` that also consults the session property; no further refactor needed.

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
| `dev-docs/RESEARCH-function-mapping.md` | Flip the "Done" column for every function shipped. Use the same `yes step 4` flag convention added for LIKE in step 1. |
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
| `dev-docs/RESEARCH-function-mapping.md` | Flip "Done" column for Tier C entries. |
| `dev-docs/TODO-pushdown-duckdb.md` | Step 4 → ✅ shipped. Update catalog totals. |
| `dev-docs/PLAN-pushdown-datetime.md` | Add a "Shipped" section pointing at the commits / PR. |
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
