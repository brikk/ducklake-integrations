# PLAN: Date/time pushdown

Step 4 of [TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md). The category most likely to silently produce wrong rows on the duckdb-format read path if we skip the alignment work.

## The core hazard

Date/time alignment between Trino and DuckDB is **not** uniformly TZ-sensitive. The hazard concentrates in a narrow band; most of the surface is safe. Get the gating right and we can ship most of the catalog cheaply. Get it wrong once and we return wrong years on a year-boundary timestamp.

Three storage shapes matter:

| Type | Trino interpretation | DuckDB interpretation | TZ-sensitive ops |
|---|---|---|---|
| `DATE` | Calendar day, no time | Calendar day, no time | **None.** Always safe. |
| `TIMESTAMP(p)` (no TZ) | Wall clock — components stored directly | Wall clock — components stored directly | **None.** `year()` etc. read off the wall clock without consulting any zone. Safe in both engines regardless of session TZ. |
| `TIMESTAMP(p) WITH TIME ZONE` | Instant + zone (zone travels with the value) | `TIMESTAMPTZ` — instant only; interpretation uses **session** `TimeZone` | **All extract operations diverge** unless session zones agree. |

The third row is the entire problem. The first two rows are the entire opportunity.

## What's already shipped (status check)

Per the executor survey:

- `year/month/day/quarter` (round 6g), `date_trunc/date_diff` (round 6i) — macros defined, **but the translator does not gate by argument type**. Today `year(timestamp_with_tz_column)` would push and silently diverge if the session TZs disagree. Currently latent because TIMESTAMP WTZ columns are uncommon in the test corpus; not currently latent because the test corpus doesn't include them.
- ICU extension loads best-effort at attach (`TrinoFunctionAliases.applyDirect`).
- No `SET TimeZone` anywhere. DuckDB sessions inherit their default (UTC for the JDBC driver; verify the embedded engine in-process).
- **No connection pool.** Each split spins up a fresh DuckDB connection (`InProcessDuckDbExecutor:62`, `QuackDuckDbExecutor:91`). No cross-session leakage risk. No pool-key re-architecture needed.
- **Trino session does not currently reach the executor or translator.** `DuckDbExpressionTranslator.translateConjuncts` is a static method with no session parameter. `DuckDbFilePageSource` receives no `ConnectorSession`. This is the prerequisite blocker.

## Architecture decisions

### Decision 1 — TZ alignment lives at session-attach, not per-call

Per-call `SET TimeZone` wrapping each pushed predicate would (a) burn round trips and (b) make WHERE clauses unreadable. Per-attach is the right granularity given no pool exists — each split's DuckDB connection is born fresh, used once, discarded. Set `TimeZone` immediately after `ATTACH`, alongside the existing `TrinoFunctionAliases` macro install. One statement per split, zero ambiguity.

For Tier B (TIMESTAMP no TZ — see Tiers below), this still doesn't matter — wall-clock extracts ignore session TZ in both engines. So the per-attach `SET TimeZone` only buys correctness for Tier C, but it's cheap enough to do unconditionally.

### Decision 2 — Match DuckDB's `TimeZone` to Trino's session TZ

`ConnectorSession.getTimeZoneKey()` is the source of truth. On attach:

```sql
SET TimeZone = '<trino_session_zone>';
```

This requires:
1. Threading `ConnectorSession` (or just its `TimeZoneKey`) through `DuckDbFilePageSource` → `ExecutionRequest` → executor's per-split attach hook.
2. Quoting the zone string for SQL injection safety (validate against `ZoneId.of(...)` before emitting; refuse to push and log a warn if invalid).
3. Best-effort: if `SET TimeZone` fails (no ICU loaded, unknown zone name), fall back to refusing Tier C pushdown for that split — `DucklakeTableHandle.pushedExpressions` becomes the gate.

### Decision 3 — Translator gates by argument type

Today `PUSHABLE_FUNCTIONS` matches by `(name, arity)` only. Date/time functions need a **type gate**: only push when the relevant argument's `Type` is one of the safe shapes for that function. New layer:

```java
// Conceptual shape; implementation deferred to step 4 ship-PR.
record PushableSpec(String name, int arity, ArgTypePredicate argGate) {}
```

For `year/month/day/quarter/date_trunc/date_diff`, the gate is `instanceof DateType || instanceof TimestampType` — never `TimestampWithTimeZoneType` for now. Promote to Tier C once the TZ-alignment plumbing lands and tests pass.

Critically: this gate is **a fix for an existing latent bug**, not just a step-4 thing. The current `year/month/day/quarter/date_trunc/date_diff` entries should land their type gates as part of step 4's first commit, before adding new entries.

### Decision 4 — ICU is bundled; gate on `SET TimeZone` success, not on `LOAD icu`

Original assumption: DuckDB without ICU only knows UTC. **Probe overturned this** ([REPORT-datetime-tz-handling.md](REPORT-datetime-tz-handling.md) Q2): the duckdb-jdbc bundle ships with ICU functionality at startup, and named IANA zones (`America/Los_Angeles`, `Asia/Singapore`, etc.) resolve even without an explicit `LOAD icu`. The existing `INSTALL icu; LOAD icu` in `TrinoFunctionAliases` is harmless but not load-bearing for zone resolution.

What DuckDB still rejects, ICU or not: **fixed-offset zone literals** (`+05:00`, `-08:00`). Trino's `TimeZoneKey` covers both shapes. The connector must normalise — likely translating offsets to `Etc/GMT±N` form, with the sign-inversion gotcha that POSIX `Etc/GMT+5` means UTC−5. Sign-table to be pinned in a follow-up probe before shipping. Conservative shortcut: refuse Tier C pushdown when the session zone is a fixed-offset shape; log a one-shot WARN; Trino re-evaluates above the scan.

Tier C correctness gate, in code: did `SET TimeZone = '<normalised>'` succeed? If yes, push. If no, don't.

### Decision 5 — Tier rollout

| Tier | Argument shape | Functions | TZ work needed | Status |
|---|---|---|---|---|
| **A** | `DATE` only | `year/month/day/quarter` (re-gated), `day_of_week`, `day_of_year`, `week`, `week_of_year`, `year_of_week` / `yow`, `last_day_of_month`, `date_trunc` (DATE), `date_diff` (DATE) | None | Ship first (✅ shipped in chunk 1 — note: original plan listed `iso_week`/`iso_year` which are not actual Trino function names; corrected to `week`/`week_of_year`/`year_of_week`/`yow` during implementation) |
| **B** | `DATE` or `TIMESTAMP(p)` (no TZ) | A + `hour`, `minute`, `second`, `millisecond`, `to_unixtime`, `from_unixtime`, `date_trunc` (TS), `date_diff` (TS), `date_add` | None | Ship after A |
| **C** | `TIMESTAMP(p) WITH TIME ZONE` | B + `at_timezone`, `with_timezone`, `year/month/...` on WTZ | Session-TZ propagation + `SET TimeZone` at attach + ICU required | Ship behind a session-flag initially; promote after the cross-engine semantic corpus passes |
| **D** | Format/parse, locale-sensitive | `format_datetime`, `parse_datetime`, `date_format`, `date_parse` with non-ISO patterns | Trino uses Joda format strings; DuckDB uses strftime. Not interchangeable. | **Never push** without a native extension that owns the format spec |

## Implementation order

1. **Type gates on existing date macros.** Fix the latent bug. Lands without ICU work.
2. **Tier A macros + tests.** Cheap. Hits the most common queries (date partitioning, day-of-week reporting).
3. **Tier B macros + tests.** Adds the TIMESTAMP-no-TZ surface.
4. **Session propagation plumbing.** Thread `ConnectorSession` (or `TimeZoneKey`) from `applyFilter` and `createPageSource` down through `DuckDbFilePageSource` → `ExecutionRequest` → executor. Set `SET TimeZone` on attach.
5. **Tier C, gated behind a connector session property** (`ducklake.pushdown.timestamp-tz` default-off). Ship the macros + type gate. Run the cross-engine corpus. Flip default once stable.
6. Tier D — explicitly excluded; document and move on.

Each tier's PR ships its own semantic fixtures in `TestTrinoFunctionAliases` (per-macro Trino-aligned expected values, like the existing string macros) plus end-to-end tests in `TestDucklakeDuckDbReadMode`.

---

## Per-function notes (the divergence pressure points)

Things that look the same on the surface but aren't.

### `day_of_week`

- Trino: 1=Monday, 7=Sunday (ISO).
- DuckDB `dayofweek(x)`: **0=Sunday, 6=Saturday** (non-ISO).
- DuckDB `isodow(x)`: **1=Monday, 7=Sunday** (ISO). ← what we want.
- Macro body: `CREATE OR REPLACE MACRO trino_day_of_week(x) AS isodow(x);`

### `week`, `week_of_year` (Trino has no `iso_week`)

- Trino: `week(x)` and `week_of_year(x)` both return ISO week (1–53).
- DuckDB: bare `week(x)` is already ISO-aligned — verified empirically (probe results: `week('2023-01-01') = 52`, `week('2024-12-30') = 1`). No separate `isoweek` function exists in DuckDB.
- Macro body shipped in chunk 1: `trino_week(d) AS week(d);` (and `trino_week_of_year` identically).

### `year_of_week` / `yow` (Trino has no `iso_year`)

- Trino: `year_of_week(x)` / `yow(x)` — ISO week-numbering year, NOT calendar year. `2024-12-30` → `2025`.
- DuckDB: no bare `isoyear()` function — reach it via `extract('isoyear' FROM x)`. Cast to BIGINT to align Trino's return type.
- Macro body shipped in chunk 1: `trino_year_of_week(d) AS extract('isoyear' FROM d)::BIGINT;` (and `trino_yow` identically).

### `date_trunc(unit, x)` unit strings

Trino units: `'second' | 'minute' | 'hour' | 'day' | 'week' | 'month' | 'quarter' | 'year'`.
DuckDB units: superset (`'microseconds' | 'millisecond' | 'second' | ... | 'decade' | 'century' | 'millennium'`).

Macro can be a direct passthrough for the Trino set; the type system in both engines rejects unknown units identically, so no extra gate needed. But: **`'week'` truncation aligns iff both engines use Monday-start ISO weeks.** Verify with the corpus.

### `date_diff(unit, t1, t2)` unit semantics

Trino: count of whole `unit` boundaries crossed between `t1` and `t2`. `date_diff('month', '2024-01-31', '2024-02-29')` = 1.
DuckDB: same boundary-count semantics.

Macro body is a direct rename, but the **cross-month-end edge cases** are the test pressure point. Pin them in the corpus.

### `from_unixtime` / `to_unixtime`

Trino's `from_unixtime(double)` returns `TIMESTAMP(3) WITH TIME ZONE` (!) — uses session TZ. So actually this is **Tier C**, not B. Move it.

DuckDB's analogue is `to_timestamp(numeric)` (different name) and returns `TIMESTAMPTZ`. Same semantics; macro renames.

`to_unixtime(timestamp)` in Trino returns a `double` in seconds. DuckDB `epoch(timestamp)` returns `double` in seconds. Tier B because the input is TIMESTAMP — wall-clock interpretation aligns.

### `date_format` / `date_parse` (Trino) vs. `strftime` / `strptime` (DuckDB)

Format strings are **incompatible**. Trino uses Joda-Time format (`yyyy-MM-dd HH:mm:ss`); DuckDB uses C strftime (`%Y-%m-%d %H:%M:%S`). No safe rewrite without owning the format-string translation layer. **Tier D — never push.**

### Trino-only / DuckDB-only — explicit do-not-push

- Trino `format_datetime` / `parse_datetime`: Joda format. Do not push.
- Trino `human_readable_seconds`: do not push.
- DuckDB `time_bucket(interval, timestamp)`: no Trino equivalent. Route via `ConnectorFunctionProvider` in step 5, not step 4.

---

## Test corpus — designed for divergence pressure

The general principle: pick inputs where engines would disagree if they're using different rules. A test that passes on `2024-06-15 12:00:00 UTC` proves nothing.

Tests run in two modes per case: **Trino-side filter** (no pushdown — `non_existent_column_macro` trick from existing pattern, or the connector-property switch) vs. **pushdown** (default), then asserted equal. This is the same shape as `TestTrinoFunctionAliases#semanticCases` and would extend it.

### DST transition pressure

The Trino test runner sets a session TZ. Pick `America/Los_Angeles` for these:

1. **Spring-forward gap:** `TIMESTAMP '2024-03-10 02:30:00'` — wall clock doesn't exist in LA. What does `hour()` return? Both engines: 2 (read from wall clock). `date_trunc('day', ts)` → `2024-03-10 00:00:00`.
2. **Fall-back ambiguity:** `TIMESTAMP '2024-11-03 01:30:00'` — exists twice in LA. For TIMESTAMP no-TZ: irrelevant, wall clock reads 1. For TIMESTAMP WTZ: depends on which 01:30 the original instant was. Pin first-occurrence and second-occurrence variants.
3. **Cross-zone year boundary** (Tier C, gated): `TIMESTAMP '2024-12-31 22:00:00' AT TIME ZONE 'UTC'` viewed in Asia/Singapore session → `year()` is **2025**, not 2024. The single most likely test to catch a missing `SET TimeZone`.

### Calendar boundary pressure

4. **Leap day (divisible-by-400):** `DATE '2000-02-29'`. `month/day_of_month` agree trivially; `date_diff('day', '2000-02-28', '2000-03-01')` = 2 in both.
5. **Non-leap century:** `DATE '1900-02-28'`. `date_add('day', 1, ...)` → `1900-03-01` (not `1900-02-29`).
6. **Leap day 2024:** `DATE '2024-02-29'`. `date_diff('month', '2024-01-31', '2024-02-29')` = 1; this catches engines that disagree on "did we cross a month boundary."
7. **Month-end add:** `date_add('month', 1, DATE '2024-01-31')` → Trino: `2024-02-29`. DuckDB: `2024-02-29`. Aligned (clamps to month end). But verify — common divergence point in date libraries.

### Year-numbering edge cases

8. `DATE '2024-01-01'` (Monday) — ISO week 1 of 2024. `year_of_week = 2024`.
9. `DATE '2024-12-30'` (Monday) — ISO week 1 of **2025**. `year_of_week = 2025`. The classic "ISO year ≠ calendar year" trap.
10. `DATE '2023-01-01'` (Sunday) — ISO week 52 of **2022**. `year_of_week = 2022`.
11. `DATE '2024-12-31'` (Tuesday) — ISO week 1 of 2025 still. `year = 2024`, `year_of_week = 2025`.

### Day-of-week numbering

12. `DATE '2024-01-07'` (Sunday). Trino `day_of_week = 7`. DuckDB `dayofweek = 0` (wrong!); `isodow = 7` (right). Pins the macro choice.

### Extreme dates

13. `DATE '0001-01-01'`, `DATE '9999-12-31'`. Smoke test for overflow / format handling.
14. `TIMESTAMP '1970-01-01 00:00:00'`. Epoch. `to_unixtime = 0` in both.
15. `TIMESTAMP '1969-12-31 23:59:59'`. Negative epoch. `to_unixtime = -1`.

### Precision

16. `TIMESTAMP '2024-06-15 12:00:00.123456'` (microseconds) — Trino's default precision is 3 (millis), DuckDB is 6 (micros). Mismatched precision is a common divergence source. Verify `date_trunc('millisecond', ts)` rounds the same way.

### Interval / `date_diff` semantics

17. `date_diff('month', '2024-01-31', '2024-02-29')` = 1 (cross one month boundary).
18. `date_diff('day', '2024-02-29', '2024-03-01')` = 1.
19. `date_diff('year', TIMESTAMP '1999-12-31 23:59:59', TIMESTAMP '2000-01-01 00:00:01')` = 1.
20. `date_diff('year', TIMESTAMP '1999-12-31 23:59:59', TIMESTAMP '1999-12-31 23:59:59.999')` = 0 (within same year).
21. `date_diff('hour', t1, t2)` across a DST spring-forward in TIMESTAMP WTZ — does the "lost hour" count? Trino: yes (real elapsed time). DuckDB: yes via `epoch_diff`. Verify; if it doesn't, **do not push** `date_diff` on WTZ inputs.

### Week-truncation pressure

22. `date_trunc('week', DATE '2024-01-07')` — Sunday. Both engines (Monday-start ISO): `2024-01-01`. If DuckDB returns `2024-01-07`, the engine is using Sunday-start, and we need an explicit `isoweek` truncation path.
23. `date_trunc('week', DATE '2024-01-01')` — Monday. Both: `2024-01-01`.
24. `date_trunc('week', DATE '2024-01-08')` — next Monday. Both: `2024-01-08`.

### Negative pressure tests (what should NOT push)

25. `year(TIMESTAMP '2024-06-15 12:00:00.000 UTC' AT TIME ZONE 'America/Los_Angeles')` — TIMESTAMP WTZ input, no Tier C plumbing yet. **Predicate must not push.** Assert SQL emitted by translator contains no `trino_year` call; assert correct rows returned (Trino re-applies the predicate).
26. `date_format(TIMESTAMP '2024-06-15 12:00:00', '%Y')` — Trino doesn't have this signature; `format_datetime(ts, 'yyyy')` — must not push, even when Tier B is on. Wrong format-string language.

### Trino's TZ defaults in the test runner

`DucklakeQueryRunner` likely defaults the session TZ to UTC (or the system default). Pin the session TZ explicitly per test case where the answer depends on it; don't let the harness default leak.

---

## Open questions — answered

All four questions probed via `ProbeDuckDbTimeZoneHandling`; findings written into [REPORT-datetime-tz-handling.md](REPORT-datetime-tz-handling.md). Summary:

1. **DuckLake TIMESTAMPTZ storage shape** — **instant only**; writer's session zone is dropped at storage. Round-trip confirmed identical for plain DuckDB AND full DuckLake-format. Reader's `TimeZone` controls all rendering and extraction. → Tier C correctness lives entirely on the session-`TimeZone` knob.
2. **`SET TimeZone` without ICU + `Etc/GMT±N`** — named IANA zones work without `LOAD icu` (ICU is bundled in the JDBC driver). Bare fixed-offset shapes (`+05:00`, `-08:00`) FAIL, but **`Etc/GMT±N` translation works** with the documented POSIX sign inversion (`+05:00` → `Etc/GMT-5`); empirically pinned across 11 shapes including `Etc/GMT-14`. Integer hours only — fractional offsets (`Etc/GMT-5:30`) fail; those need their named IANA zone instead. → Three-rule normaliser validated in Q3 below.
3. **Embedded `jdbc:duckdb:` default `TimeZone`** — **inherits the JVM's system zone**, not UTC (probed on Costa Rica → DuckDB reported `America/Costa_Rica`). Silent portability bomb across CI/dev/prod. → `SET TimeZone` on attach is non-optional even for Tier A/B work. **Trino→DuckDB propagation mechanism validated end-to-end** for 12 of 14 representative `TimeZoneKey` shapes — every named IANA and every integer-hour offset matches `java.time` ground truth exactly. The 2 fractional bare-offset cases fail cleanly with a DuckDB error (refusal, not silent divergence). Production normaliser is the three-rule function from the probe.
4. **In-process vs. Quack parity** — **confirmed empirically.** Spun the Quack testcontainer (`TestingDucklakeQuackEngineServer`), shipped `SET TimeZone` + `SELECT year(TIMESTAMPTZ ...)` server-side via `quack_query_by_name`, results match in-process byte-for-byte across UTC / LA / Singapore (year-boundary smoking gun fires identically through the RPC). → `SET TimeZone` lands on both executors with one shared implementation pattern (same precedent as `TrinoFunctionAliases.applyDirect`).

`ProbeDuckDbTimeZoneHandling` should be deleted after the step-4 PR bakes these findings into shipped tests, per the deleted-probe precedent in [REPORT-hash-null-handling.md](REPORT-hash-null-handling.md).
