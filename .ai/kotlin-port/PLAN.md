# DuckLake JVM → Kotlin port — working plan

> Start-here doc for any session. Companion files in this dir:
> - `adversarial-review-result.json` — the 73 confirmed findings (full detail/suggestion/reasoning). Source of truth for bug locations.
> - `.claude/workflows/kotlin-port.js` — the port workflow.
> Memory: `ducklake-kotlin-port-workflow` (design + conventions).

## State (as of 2026-05-31)
- Adversarial code review **done**: 184 agents, 73 confirmed findings.
- Port workflow **authored, not yet run**.
- Triage of review → BEFORE/DURING/AFTER **agreed with the user** (below).
- Next action: do the **BEFORE** fixes in Java (red-test-first), then run the port slices.

## Port model (unchanged — see memory for full conventions)
Per slice, two independently-verified steps:
1. **Faithful port** (close 1:1, language-level Kotlin only, NO shared helpers) → verify.
2. **Build idiom kit FROM the ported Kotlin** → apply → re-verify.

Slice order: `catalog-test → catalog-main → trino-test → trino-main`, then `slice="combine"`.
Conventions: `.java`→`.java.old` (never delete); `generated/` never touched; no Gradle changes (Kotlin already wired in `build-logic`); never `gradle clean`; interop annotations mandatory; `.java.old` is the diff oracle.
Verify granularity: catalog = full suites freely; trino-test = only the class being worked on (aggregate from those); trino-main = small value/state types batched + one full suite, then big files one-at-a-time with `Test<ClassName>` then full suite.
Controls: `args.stopAfter="port"|"kit"`, `args.slice="combine"`, `args.smallLoc`.

**Kit policy (decided): kit-apply MAY make correctness wins** (leaks→`use{}`, locale→`Locale.ROOT`, null-safety), each logged + re-verified; the equivalence step reports them as *intentional divergences* rather than failing them.

---

## BEFORE — fix in Java first (so the baseline is correct and the faithful port doesn't enshrine a bug)
Red-test-first where deterministic; reasoned fix for the race.

### B1. Type vocabulary + stats comparison cluster (ONE root cause)
The catalog stores canonical types `int8/int16/int32/int64/int128`, `uint8…uint128`, `float32/float64`, `decimal(p,s)`, date/timestamp\*. Three paths drifted and compare lexicographically:
- `JdbcDucklakeCatalog.typedCompare` (584–601 / dup 587–595) — switches on non-existent names (`bigint`/`hugeint`/`decimal`) → all numeric/float min/max merge falls back to string compare.
- `JdbcDucklakeCatalog.isNumericType`/`findDataFileIdsInRange` (2445–2458) — misses `int128`/`uint128` → **hard-prunes matching files** (silent missing rows).
- `DucklakeStatsExtractor` (103–106, 192–200) — lexicographic min/max across row groups.
- `DucklakeStatsExtractor` (179–183) — decimal decoded big-endian, but short decimals (INT32/INT64) are little-endian.

**Fix:** one canonical type vocabulary + one type-aware comparator/parser that all three route through (kills future drift; becomes the centralized helper the port wants anyway).
**Test:** type-coverage matrix — every canonical type × {cross-file min/max merge, range-prune, stats round-trip}, red today → green after.

### B2. `$divide` pushdown (deterministic → red-test)
`DuckDbExpressionTranslator.arithmeticOperator` (717) maps `$divide` → DuckDB `/`. Diverges: DuckDB `/` is true division (`5/2→2.5`; integer div is `//`), and divide-by-zero returns NULL vs Trino throws.
**Fix (architecturally consistent):** add a `trino_divide` parity macro to `duckdb-trino-parity-extension`, register in the alias set (`trino_meta()` / `TestTrinoFunctionAliases` enforce parity), route integer `$divide` through `translateMacroCall`. Cheaper alt: stop pushing integer `$divide`/`$modulo` (return null). **Check `$modulo` for the same zero divergence.**
**Test:** integer column, `WHERE x/2 = 2`, assert pushed == Trino-native.

### B3. Delete-fragment correctness
- `applyDeleteFragments` never end-snapshots a prior active delete file for the same `data_file_id` (`JdbcDucklakeCatalog` 2223–2260).
- Delete-row filter mis-aligns file positions when the Parquet predicate prunes row groups/pages (`DucklakePageSourceProvider` 520–544, 907–946; DuckDB-path variant 603–624). → red-test with a predicate that prunes row groups + a delete.

### B4. Temporal partition pruning (silent data loss)
`DucklakeTemporalPartitionMatcher` (108–136) — unbounded-low exclusive-high at a transform boundary prunes ALL files under CALENDAR encoding; (128–134) non-monotonic wrapping guard omits YEAR but applies the decrement to YEAR and ignores unbounded low. → red-test per encoding/boundary.

### B5. Arrow writer join-before-close (reasoned fix, NOT a red-test — it's an ordering bug)
`DuckDbArrowStreamFileWriter.cleanupAfterFailure` (624–637) only `interrupt()`s the consumer then frees the allocator; `interrupt()` can't stop a native DuckDB INSERT, so buffers are freed under it. **Fix:** `consumerThread.join(bounded)` before closing the allocator (mirror the happy path at line 446). Verify by inspection/review; optional stress test with injected slow consumer (don't gate on it).

---

## DURING — mark, don't preload; fixed at kit-apply (correctness-permissive)
As the faithful-port agent reads each file, plant `// TODO(kit: …)` at the exact site. The kit-apply step sweeps them (markers travel with the code → drift-proof worklist; `grep TODO(kit`).
- Resource leaks → `use{}`: `DuckDbFileWriter` (380–393, 143–162), `DucklakeMergeSink` (176–202).
- Locale `toLowerCase()` → `lowercase(Locale.ROOT)`: `DucklakeTypeConverter` (77), `DucklakeMetadata` (1495).
- NPE on null `SQLException.message` → `?.message ?: …`: `QuackDuckDbExecutor` (299), `InProcessDuckDbExecutor` (194–200).
- `getRetainedSizeInBytes` double-count: `DucklakeSplit` (208–228), `DucklakeColumnHandle` (68–71).

Seed `kit-notes.json` with expected helpers (built from real code, not preloaded): `Locale.ROOT` lowercase; JDBC/Arrow `use{}` runner; null-safe SQL-message.

---

## AFTER — post-port (saved JSON is source of truth; plant `// TODO(review:after …)` markers during port)
Tracking is safe: efficiency findings are method-anchored, SPI gaps are override-anchored; the JSON has full detail; markers travel with the code. **Nothing moves to BEFORE for trackability.**
- All ~14 efficiency findings (N+1 probes, O(rows×files) scans, double-serialization, whole-collection materializations).
- SPI/contract & feature gaps: `listTables` omits views (336–360); `getMemoryUsage`/`getCompletedBytes` not overridden (`DucklakePageSink`).
- Long-tail robustness (regex greediness, `Boolean.parseBoolean('1')`, year<1000 date formatting, footer-size overflow, etc.).

**User's-call: reconsider for BEFORE on merit (not tracking)** — semi-silent correctness:
- `listTableColumns` ignores session/time-travel snapshot (`DucklakeMetadata` 769).
- `DucklakeParquetTypeUtils` (90–97) ClassCastException on 2-level Parquet lists (a crash).

---

## Resume / account-swap model
- **Across sessions/accounts (same machine + repo): resume at SLICE boundaries, not mid-workflow.** Everything completed is durable on disk: `.java.old`, `<slice>/baseline.json`, `<slice>/prekit/`, `_idioms/IdiomKit.kt` + `kit-notes.json`, planted TODO markers. A fresh session reads this PLAN + memory and runs the next slice.
- **Don't hand off a mid-flight background workflow between accounts** — a running Workflow task belongs to the session that launched it; the new session can't attach. Let a slice finish, then swap. (`resumeFromRunId` cache is same-session only.)
- The **BEFORE** fixes are plain git work — fully resumable across sessions via this checklist.
- Memory lives under the project path `-Users-jminard` (per-machine/project, NOT per-account) → it loads for the new account automatically.

### Run commands
```
# BEFORE fixes first (manual git work, red-test-first), then:
Workflow({scriptPath:".../.claude/workflows/kotlin-port.js", args:{slice:"catalog-test"}})
#   → catalog-main → trino-test → trino-main → {slice:"combine"}
# Inspect checkpoints: args.stopAfter:"port" (after translation) or "kit" (before applying kit)
```
