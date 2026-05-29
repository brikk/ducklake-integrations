# REPORT: DuckDB date/time + TZ handling

Empirical findings for the four open questions parked at the bottom of [PLAN-pushdown-datetime.md](PLAN-pushdown-datetime.md). Probed via `ProbeDuckDbTimeZoneHandling` (test class; delete after this report is committed, matching the `ProbeConcatNullHandling` / `ProbeHashNullHandling` precedent). All probes ran against the in-process `jdbc:duckdb:` driver bundled in this project's gradle classpath; DuckDB version per `libs.duckdb.jdbc` in the version catalog.

## Q1 — TIMESTAMPTZ storage shape (DuckDB and DuckLake)

**Question.** When we write a `TIMESTAMPTZ` value into a DuckDB database file (or a DuckLake-format table), does storage preserve the original zone, or does it become a pure instant that the reading session reinterprets through its own `TimeZone` setting?

**Answer — instant only; the writer's session zone is discarded.**

Wrote the same instant (`2024-06-15 12:00:00 UTC`) from three sessions with writer `TimeZone` = UTC / America/Los_Angeles / Asia/Singapore. Read all three rows from a single reader session, varying the reader's `TimeZone`. Results (probe output, condensed):

| Reader session `TimeZone` | All three rows render as | `year(ts)` | `hour(ts)` |
|---|---|---|---|
| `UTC` | `2024-06-15 12:00:00+00` | 2024 | 12 |
| `America/Los_Angeles` | `2024-06-15 05:00:00-07` | 2024 | 5 |
| `Asia/Singapore` | `2024-06-15 20:00:00+08` | 2024 | 20 |

All three writer-zone rows produce **identical** rendering and extraction in any given reader session. The writer's `TimeZone` does not travel with the value. Repeated for plain DuckDB storage AND for DuckLake-format storage (full `ATTACH 'ducklake:metadata.db' AS lake (DATA_PATH ...)`) — behaviour is identical.

**Companion data point.** `TIMESTAMP` (no TZ) is wall-clock: the same column reads `2024-06-15 12:00:00` regardless of reader `TimeZone`, and `year/hour/day` always return `2024/12/15`. `DATE` likewise TZ-invariant.

**Implication for pushdown.** Tier C correctness requires the connector to set DuckDB's `TimeZone` to match Trino's session `TimeZoneKey` on every attach, before any predicate executes. Without it, `year(timestamptz_col) = 2024` could push and silently return rows from `2025` (when the reader session is east of UTC and the boundary crosses).

## Q2 — `SET TimeZone` without ICU + `Etc/GMT` translation table

**Question.** Does `SET TimeZone = '...'` work without an explicit `INSTALL icu; LOAD icu`? Which zone shapes are accepted? Can fixed offsets be translated to `Etc/GMT±N`?

**Answer — named IANA zones work; fixed-offset shapes do NOT; `Etc/GMT±N` translation works with POSIX sign inversion.**

| Zone literal | Without ICU loaded | After `INSTALL icu; LOAD icu` |
|---|---|---|
| `UTC` | OK | OK |
| `GMT` | OK | OK |
| `EST` | OK | OK |
| `America/Los_Angeles` | OK | OK |
| `Europe/Berlin` | OK | OK |
| `Asia/Singapore` | OK | OK |
| `Pacific/Chatham` | OK | OK |
| `+00:00` | FAIL — `Unknown TimeZone '+00:00'!` | FAIL (same) |
| `+05:00` | FAIL | FAIL |
| `-08:00` | FAIL | FAIL |

Explicit `LOAD icu` does **not** change the accepted set in either direction — the duckdb-jdbc bundle already includes ICU functionality at startup (this is a known property of the Maven-published artifact; the explicit `LOAD icu` becomes a no-op). The error message suggests `GMT0` for `+00:00`, which hints DuckDB recognises a small POSIX-style zone set that does not include the `±HH:MM` shape Trino sometimes hands us.

### `Etc/GMT±N` translation table (probed empirically)

POSIX-style `Etc/GMT±N` zones translate fixed offsets — with the documented sign inversion (a positive UTC offset gets a negative `Etc/GMT-N`). The probe set `TimeZone = 'Etc/GMT-N'` and rendered the reference instant `2024-06-15 12:00:00+00`:

| Set `TimeZone` value | Rendered offset (= actual UTC offset) | Verdict |
|---|---|---|
| `Etc/GMT` | `+00:00` | UTC alias — OK |
| `Etc/GMT0` | `+00:00` | UTC alias — OK |
| `Etc/UTC` | `+00:00` | UTC alias — OK |
| `Etc/GMT-5` | `+05:00` | UTC+5 (POSIX inversion) — OK |
| `Etc/GMT+5` | `-05:00` | UTC-5 — OK |
| `Etc/GMT-8` | `+08:00` | UTC+8 (Singapore-like) — OK |
| `Etc/GMT+8` | `-08:00` | UTC-8 (LA-non-DST) — OK |
| `Etc/GMT-12` | `+12:00` | far-east — OK |
| `Etc/GMT+12` | `-12:00` | OK |
| `Etc/GMT-14` | `+14:00` | Kiribati range — OK |
| `Etc/GMT-5:30` | — | FAIL — `Unknown TimeZone 'Etc/GMT-5:30'!` |

Integer hours only. Fractional offsets (India UTC+05:30, Newfoundland UTC-03:30, Chatham UTC+12:45) cannot be expressed as `Etc/GMT±N` and must be served by their named IANA zones (`Asia/Kolkata`, `America/St_Johns`, `Pacific/Chatham`) instead.

**Implication for pushdown.** The connector must normalise Trino's `TimeZoneKey` to a string DuckDB accepts before emitting `SET TimeZone`. The candidate normaliser, validated end-to-end in Q3 below, has three rules:
1. `Z` → `UTC`.
2. `±HH:MM` with `MM == 00` → `Etc/GMT∓HH` (POSIX sign inversion).
3. Anything else (named IANA, `UTC`, `GMT`, fractional offset) → pass through unchanged. DuckDB accepts the named cases; the fractional cases get cleanly rejected, and the caller falls back to "don't push Tier C this query."

## Q3 — embedded `jdbc:duckdb:` default `TimeZone`

**Question.** What does a fresh `DriverManager.getConnection("jdbc:duckdb:")` default to for `TimeZone`?

**Answer — DuckDB inherits the JVM's system default zone, not UTC.**

Probe ran on a machine where `java.time.ZoneId.systemDefault() == "America/Costa_Rica"`. The fresh DuckDB session reported `current_setting('TimeZone') = America/Costa_Rica` and `current_setting('Calendar') = gregorian`, with `TIMESTAMPTZ '2024-06-15 12:00:00+00'` rendering as `2024-06-15T06:00-06:00`. The JVM and DuckDB agree on the system zone.

**Implication for pushdown — major.** This is a **silent portability bomb**:
- Dev machine in Costa Rica: DuckDB default = `America/Costa_Rica`.
- CI worker in Linux container: DuckDB default = `UTC` (assuming container TZ).
- Production worker in another region: default = whatever the host OS says.

A query whose result depends on session zone (which is most `TIMESTAMPTZ` queries) will quietly produce different results on different workers if we do not explicitly set the zone. **The fix is non-optional**: `SET TimeZone = '<trino_session_zone>'` must run on every attach, even for Tier A/B work that we believe to be TZ-invariant — because (a) we want a deterministic baseline for debugging and (b) any future Tier C addition would inherit the hazard.

The same applies to the in-process executor and (presumably) Quack; see Q4.

## Q3 (extended) — Trino `TimeZoneKey` → DuckDB `SET TimeZone` propagation

**Question.** Can the connector mechanically read Trino's session zone, normalise it, and `SET TimeZone` on DuckDB such that subsequent extracts match what Trino itself would compute? Probe via a candidate normaliser run against a curated set of `TimeZoneKey`-shaped inputs, with `java.time` as the ground truth (since Trino's runtime uses `java.time` for the same calculations).

**Answer — works end-to-end for every named IANA zone and every integer-hour offset; cleanly refuses fractional offsets.**

14 inputs covering the realistic `TimeZoneKey` shape space, extracting `year(TIMESTAMPTZ '2024-12-31 22:00:00+00')` and `hour(...)`. Verdict = result match between DuckDB extract and `Instant.atZone(ZoneId.of(trinoZone))`:

| Trino zone | Normalised → DuckDB | DuckDB (year/hr) | java.time (year/hr) | Verdict |
|---|---|---|---|---|
| `UTC` | `UTC` | 2024 / 22 | 2024 / 22 | MATCH |
| `GMT` | `GMT` | 2024 / 22 | 2024 / 22 | MATCH |
| `Z` | `UTC` | 2024 / 22 | 2024 / 22 | MATCH |
| `America/Los_Angeles` | passthrough | 2024 / 14 | 2024 / 14 | MATCH |
| `Europe/Berlin` | passthrough | 2024 / 23 | 2024 / 23 | MATCH |
| `Asia/Singapore` | passthrough | **2025 / 6** | **2025 / 6** | MATCH (year-boundary smoking gun) |
| `Asia/Kolkata` (UTC+05:30) | passthrough | 2025 / 3 | 2025 / 3 | MATCH (fractional offset survives via named IANA) |
| `Pacific/Chatham` (UTC+12:45) | passthrough | 2025 / 11 | 2025 / 11 | MATCH |
| `+00:00` | `Etc/GMT-0` | 2024 / 22 | 2024 / 22 | MATCH |
| `+05:00` | `Etc/GMT-5` | 2025 / 3 | 2025 / 3 | MATCH |
| `-08:00` | `Etc/GMT+8` | 2024 / 14 | 2024 / 14 | MATCH |
| `+14:00` | `Etc/GMT-14` | 2025 / 12 | 2025 / 12 | MATCH |
| `+05:30` | `+05:30` (no translation possible) | — | (n/a) | DUCK-FAIL — `Unknown TimeZone '+05:30'!` |
| `-03:30` | `-03:30` | — | (n/a) | DUCK-FAIL |

**Take.** The three-rule normaliser is correct as designed: every integer-hour offset and every named IANA zone produces extracts that exactly match `java.time`. Fractional bare-offset shapes (`+05:30`, `-03:30`) fail cleanly with a useful DuckDB error message — the right outcome, because there's no integer-hour `Etc/GMT-N` that means UTC+5:30. The two callers of this normaliser are:
- **The attach hook** — try `SET TimeZone = '<normalised>'`. On failure, log a one-shot WARN and disable Tier C pushdown for this attach.
- **Per-query gate** (optional second layer) — if the session zone is fractional-offset bare-form, refuse Tier C even on a successful attach (a session that bound a non-fractional zone but later switched to one would otherwise leak).

In practice fractional offsets in Trino almost always arrive as the corresponding named IANA zone (`Asia/Kolkata` rather than `+05:30`), so the failure mode is narrow.

## Q4 — `InProcessDuckDbExecutor` vs. `QuackDuckDbExecutor` parity

**Question.** Does `SET TimeZone` + extract behave identically on both executor paths?

**Answer — yes, byte-for-byte parity confirmed.**

In-process probe across three session zones:

| Session `TimeZone` | `year(TIMESTAMPTZ '2024-12-31 22:00:00+00')` | `hour(...)` |
|---|---|---|
| `UTC` | 2024 | 22 |
| `America/Los_Angeles` | 2024 | 14 |
| `Asia/Singapore` | **2025** | **6** |

Quack-container probe (`ProbeDuckDbTimeZoneHandling#probeQ4b_quackContainerParity`): spins `TestingDucklakeQuackEngineServer`, opens a local JDBC client wired to it via the quack extension, ships `SET TimeZone = '...'` and `SELECT year(TIMESTAMPTZ ...)` server-side via `quack_query_by_name`, reads back through Arrow:

| Session `TimeZone` (server-side) | Quack `year(TIMESTAMPTZ)` | Quack `hour(TIMESTAMPTZ)` | Quack `year(TIMESTAMP)` | Quack `hour(TIMESTAMP)` |
|---|---|---|---|---|
| `UTC` | 2024 | 22 | 2024 | 22 |
| `America/Los_Angeles` | 2024 | 14 | 2024 | 22 |
| `Asia/Singapore` | **2025** | **6** | 2024 | 22 |

**Every cell matches in-process.** The Singapore year-boundary smoking gun fires through Quack RPC just as it does locally — same behaviour, same DuckDB underneath. Wall-clock columns (`TIMESTAMP` no-TZ) are session-zone invariant on both paths, as expected.

This was the most consequential validation: it pins parity for both halves of the executor abstraction so the step-4 implementation PR can land `SET TimeZone` on attach knowing both backends honour it. The `TrinoFunctionAliases.applyDirect` pattern that already runs identically on both executors is the precedent — add `SET TimeZone` to the same hook and both paths inherit it.

## Implications summary (for the step-4 implementation PR)

1. **Set `TimeZone` on every attach, unconditionally.** Even Tier A/B work benefits from a deterministic baseline (the Costa Rica → CI default-divergence story from Q3). Lands alongside the existing `TrinoFunctionAliases.applyDirect(stmt)` call in both `InProcessDuckDbExecutor:70` and the equivalent server-side hook in `QuackDuckDbExecutor`. Q4b confirms both paths honour `SET TimeZone` byte-for-byte identically.
2. **Use the validated three-rule normaliser** (Q3 table): `Z` → `UTC`; `±HH:MM` with `MM == 00` → `Etc/GMT∓HH` (POSIX inversion); everything else passes through. Validated against `java.time` ground truth for 12 of 14 representative shapes; the remaining 2 (fractional bare offsets) fail cleanly with a useful DuckDB error.
3. **No ICU dependency to assert.** ICU is already in the JDBC bundle and behaves as if loaded at startup. The existing best-effort `INSTALL icu; LOAD icu` in `TrinoFunctionAliases` is harmless but not load-bearing for zone resolution. Don't gate Tier C pushdown on `LOAD icu` success — gate on `SET TimeZone` success.
4. **DuckLake storage drops the zone.** Tier C correctness lives entirely on the session-`TimeZone` knob. There is no per-column zone metadata to interrogate.
5. **`ProbeDuckDbTimeZoneHandling` deletion**: delete the probe class once the step-4 implementation PR lands and its findings are baked into shipped tests. Matches the deleted-probe precedent in [REPORT-hash-null-handling.md](REPORT-hash-null-handling.md).

## Open follow-up probes (NOT blockers for the step-4 PR)

- **DST history accuracy across engines.** Both DuckDB ICU and Trino `java.time` use IANA, but the tzdb version they were built against can differ. Probe a date in `Europe/Moscow` pre-2014 (Russia's permanent DST change) and `America/Mexico_City` pre-2022 (DST abolition) — if DuckDB and `java.time` disagree on the offset for any historical instant, we have to either pin tzdb versions in the Quack container build or refuse to push for the affected window. Low-risk but cheap to verify.
- **Mid-query session-zone change.** Trino allows `SET SESSION timezone = ...` within a query batch. The connector currently picks up the session at `applyFilter` time; if the session changes between the WHERE-clause planning and the page-source's executor attach, the zone the translator assumed might not match the zone the executor sets. Verify whether the in-flight session is stable across that boundary.
