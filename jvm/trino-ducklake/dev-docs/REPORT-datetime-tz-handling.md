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

## Q2 — `SET TimeZone` without ICU

**Question.** Does `SET TimeZone = '...'` work without an explicit `INSTALL icu; LOAD icu`? Which zone shapes are accepted?

**Answer — named IANA zones work; fixed-offset shapes do NOT.**

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

**Implication for pushdown.** The connector must normalise Trino's `TimeZoneKey` to a string DuckDB accepts before emitting `SET TimeZone`. Concretely:
- Trino's `TimeZoneKey` covers both **named** zones (`America/Los_Angeles`, `UTC`) and **fixed-offset** zones (`+05:00`, `-08:00`).
- For named zones: pass through verbatim.
- For fixed-offset zones: translate `+HH:MM` / `-HH:MM` to the GMT-form DuckDB accepts (e.g. `+05:00` → `Etc/GMT-5` — note POSIX sign inversion). Verify the translation table empirically before shipping (probe extension to Q2 in a follow-up).
- Or, the conservative shortcut: refuse Tier C pushdown when the Trino session zone isn't a named IANA zone DuckDB accepts. Log a one-shot WARN; Trino re-evaluates above the scan.

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

## Q4 — `InProcessDuckDbExecutor` vs. `QuackDuckDbExecutor` parity

**Question.** Does `SET TimeZone` + extract behave identically on both executor paths?

**In-process — confirmed.** Probe ran the extract sequence inside the in-process JDBC driver across three session zones; results matched the documented DuckDB semantics in every case:

| Session `TimeZone` | `year(TIMESTAMPTZ '2024-12-31 22:00:00+00')` | `hour(...)` |
|---|---|---|
| `UTC` | 2024 | 22 |
| `America/Los_Angeles` | 2024 | 14 |
| `Asia/Singapore` | **2025** | **6** |

The Singapore result is the smoking gun for the year-boundary test case in `PLAN-pushdown-datetime.md` — the same instant produces a different `year()` depending on session zone.

`TIMESTAMP` (no TZ) and `DATE` returned `2024 / 22` and `2024` in every session, confirming the wall-clock-invariant claim.

**Quack — not probed in this round.** The Quack executor runs DuckDB in a separate process via a Testcontainers-managed container; running the probe inside the Quack process requires spinning the container. The published Quack image bundles the same DuckDB build the project's JDBC driver uses, so parity is the expected outcome — but **verifying this is the step-4 implementation PR's job**, not this report's. Concrete check for that PR:

```
./gradlew -Dducklake.test.catalog-backend=DUCKDB_QUACK :trino-ducklake:test \
    --tests "*TestDucklakeDuckDbReadMode*"
```

After Tier-C `SET TimeZone` plumbing is wired, add a test that asserts the same year-boundary smoking gun returns the right row through the Quack backend. If results diverge from in-process, the divergence is most likely (a) a different DuckDB binary in the container image, or (b) the container's `LD_LIBRARY_PATH` not exposing ICU. Both are fixable inside the container's Dockerfile; neither is a plan-blocker.

## Implications summary (for the step-4 implementation PR)

1. **Set `TimeZone` on every attach, unconditionally.** Even Tier A/B work benefits from a deterministic baseline. Lands alongside the existing `TrinoFunctionAliases.applyDirect(stmt)` call in both `InProcessDuckDbExecutor:70` and `QuackDuckDbExecutor` (similar attach hook).
2. **Normalise Trino's `TimeZoneKey` before emitting SQL.** Named IANA zones pass through; fixed-offset zones either get translated to `Etc/GMT±N` or block Tier C pushdown (TBD).
3. **No ICU dependency to assert.** ICU is already in the JDBC bundle and behaves as if loaded at startup. The existing best-effort `INSTALL icu; LOAD icu` in `TrinoFunctionAliases` is harmless but not load-bearing for zone resolution. Note for the step-4 PR: don't gate Tier C pushdown on `LOAD icu` success — gate it on `SET TimeZone` success instead.
4. **DuckLake storage drops the zone.** Tier C correctness lives entirely on the session-`TimeZone` knob. There is no per-column zone metadata to interrogate.
5. **`ProbeDuckDbTimeZoneHandling` deletion**: delete the probe class once the step-4 implementation PR lands and its findings are baked into shipped tests. Matches the deleted-probe precedent in [REPORT-hash-null-handling.md](REPORT-hash-null-handling.md).

## Open follow-up probes (NOT blockers for the step-4 PR)

- Fixed-offset zone translation table: probe `Etc/GMT+5` / `Etc/GMT-5` etc. against expected Trino offsets to pin the sign convention before emitting normalised SQL.
- DST history accuracy: probe a date in `Europe/Moscow` pre-2014 (Russia's permanent DST change) — does DuckDB's ICU agree with Trino's `java.time` tz database version? Likely yes (both use IANA), but worth one explicit check.
- Quack-container in-process probe parity: re-run `ProbeDuckDbTimeZoneHandling`-equivalent SQL inside the Quack container during the step-4 PR's verification step.
