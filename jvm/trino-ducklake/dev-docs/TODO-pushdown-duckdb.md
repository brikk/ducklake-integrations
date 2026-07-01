# TODO: Trino → DuckDB pushdown

Bring our duckdb-format read path up to the BigQuery / Snowflake connector shape: a curated translator that turns Trino's `ConnectorExpression` predicates into DuckDB SQL, plus DuckDB-namespaced functions exposed through Trino's function SPI for things Trino doesn't have natively.

This file is the **program tracker** (roadmap, history, open items). For the
**current pushable surface** — every operator, transform, and function that
pushes today — see [README-duckdb-format-pushdown-reference.md](../README-duckdb-format-pushdown-reference.md).

Background and architecture: see [RESEARCH-lance-and-pushdown.md](RESEARCH-lance-and-pushdown.md).
Function mapping reference: see [RESEARCH-function-mapping.md](RESEARCH-function-mapping.md).

> **Status as of the trino_parity extension migration:** the `@placeholder` /
> warn-on-emit machinery, the in-tree `trino-function-aliases.sql` resource,
> and the `TrinoFunctionAliases.applyDirect` SQL-replay path are all GONE.
> `trino_lower` / `trino_upper` / `trino_reverse` / `trim` family /
> `normalize/{1,2}` are native C++ in
> [duckdb-trino-parity-extension](../../../duckdb-trino-parity-extension);
> the rest of the `trino_<name>` macros are `DefaultMacro[]` arrays in the
> same extension; `sha512`/`xxhash64`/`hmac_sha256` are native C++ over vendored
> primitives; `trino_meta()` has 95 entries; the connector bundles the extension
> binary into the plugin jar and `LOAD`s it on attach. Functions DuckDB can't
> match natively — full-case-folding `lower`/`upper`, code-point `reverse` —
> are native C++ in the extension too (the old `@placeholder` / warn-on-emit
> machinery and the SQL-resource replay path are gone).
> Current open work: item 5 (DuckDB-namespaced exclusives via
> `ConnectorFunctionProvider`), plus the extension repo's own
> [TODO.md](../../../duckdb-trino-parity-extension/TODO.md). (Tier C default-on
> flip landed 2026-06-06 — `pushdown_timestamp_with_timezone` now defaults on.)

## Discipline (non-negotiable)

- **Lossless pushdown only.** Anything we can't translate with confidence stays in Trino. Partial pushdown is correct, ambitious pushdown that quietly changes semantics is a bug factory.
- **Curated map, not "translate anything that looks similar."** Each translation is an explicit entry: Trino signature → DuckDB fragment → recorded NULL / Unicode / edge semantics.
- **Cross-engine semantic test per entry.** For each mapped function, a test that runs the same input through "Trino does the filter" vs "pushdown does the filter" and asserts identical results. Edge cases (NULL, empty string, multi-byte, leap-day, etc.) where the function spec calls them out.
- **Conservative defaults.** When in doubt, don't push. Adding translations later is cheap; un-pushing a wrong one is expensive.

## Priority order

1. ✅ **`LIKE` / `NOT LIKE`** — extremely common, perfectly defined in both engines, near-zero risk. Likely single biggest win for selective queries. Shipped: translator branch on `$like` reads `io.trino.type.LikePattern` reflectively (the class lives in `trino-main`, not `trino-spi`) and emits `(value LIKE 'pattern' [ESCAPE 'c'])`. `NOT LIKE` reuses the existing `$not` recursion. Dynamic pattern (non-`Constant`) and NULL pattern stay unpushed.
2. ✅ **Numeric / comparison expressions in predicates** — `col + 1 > 5`, `col % 10 = 0`, etc. Same shape, mostly trivial. Pin overflow + decimal semantics.
   - Comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL`, `NOT`, `AND`, `OR`) — translated by `DuckDbExpressionTranslator` directly.
   - **Arithmetic operators (`$add`, `$subtract`, `$multiply`, `$divide`, `$modulo`)** — translated directly to infix SQL (`+`, `-`, `*`, `/`, `%`). Shipped round 6f.
   - **`$coalesce` (variadic), `$nullif` (2-arg), `$identical` (IS NOT DISTINCT FROM)** — translated directly. Shipped round 6f.
   - Function-shape numerics shipped via macros: `abs/1`, `ceil/1`, `floor/1`, `mod/2`, `power/2`, `sqrt/1`, `exp/1`, `ln/1`, `log2/1`, `log10/1`, plus round 5 trig + round 6a sign/bit_length/pi/bitwise_xor.
3. ✅ **String basics** — `SUBSTRING`, `LENGTH`, `LOWER`, `UPPER`, `TRIM`, `CONCAT`, `POSITION`. Watch the Unicode caveat — pin behavior, test against non-ASCII.
   - Shipped: `length/1`, `reverse/1`, `trim/1`, `ltrim/1`, `rtrim/1`, `substring/{2,3}`, `replace/3`, `strpos/2`, `starts_with/2`, `lpad/3`, `rpad/3`, `concat_ws/{2..5}`, `translate/3`.
   - **`lower/1`, `upper/1`, `reverse/1` SHIPPED as native C++ in the extension (fully Trino-aligned).** DuckDB's built-in `lower`/`upper` do *simple* case folding (`lower('İ')` → `'i'`, `upper('ß')` → `'ẞ'`) while Trino's Java implementations do *full* case folding (`lower('İ')` → `'i' + U+0307`, `upper('ß')` → `'SS'`); DuckDB's `reverse` is grapheme-cluster-aware while Trino's is code-point-only. Collations / `nfc_normalize` do NOT fix this (verified by the since-deleted `ProbeDuckDbCaseFolding`). These were initially shipped as pushed *placeholders* (ASCII-safe, warn-on-emit), then replaced by native ICU-backed C++ functions in `trino_parity` (`src/string_functions.cpp`) that match Trino's full-folding / code-point semantics. They are now in `trino_meta()` + `PUSHABLE_FUNCTIONS`, pinned by `TestTrinoFunctionAliases#testUnicodeCaseFoldingMatchesTrinoSpec`.
   - **`concat` shipped as translator rewrite** (round 6e): `Call(concat, [a,b,c])` → `(a || b || c)` when return type is `VARCHAR`. DuckDB's `concat` skips NULL; the `||` operator NULL-propagates in both engines and matches Trino's `concat` semantics.
   - Not yet shipped: `position` (operator-form; needs translator special case).
   - Regex (RE2 on both): `regexp_like/2`, `regexp_extract/{2,3}`. `regexp_like` exercises the rename pattern (Trino `regexp_like` → DuckDB `regexp_matches` via macro body).
4. ✅ **Date / time** — complete. Plan in [archive/PLAN-pushdown-datetime.md](archive/PLAN-pushdown-datetime.md). Empirical TZ findings in [archive/REPORT-datetime-tz-handling.md](archive/REPORT-datetime-tz-handling.md). Chunk-by-chunk record in [archive/TODO-pushdown-datetime.md](archive/TODO-pushdown-datetime.md). Shipped: 1 = translator type-gate + Tier A (DATE) + Tier B (TIMESTAMP no-TZ); 2 = session-TZ plumbing + normaliser; 3 = Tier C session-property gate (narrow, `to_unixtime` only); 3.5 = Arrow converter fix unblocks full Tier C surface (year/month/day/hour/...); 4 = `from_unixtime`/`with_timezone` extras + `at_timezone` documented as not-pushable.
5. ⏳ **DuckDB-namespaced exclusives** — vector / JSON / list / struct / regex variants that exist in DuckDB but not in Trino. Register through `ConnectorFunctionProvider`, route through the same translator. Lower-risk than #4 because we own both ends of the semantic contract. Not started.
6. ✅ **Lance table functions** — shipped (`lance_vector_search` / `lance_fts` / `lance_hybrid_search` via `applyTableFunction`/`applyFilter`/`applyTopN`). Change-feed table functions (`table_insertions`/`table_deletions`/`table_changes`) also shipped on the same scan-rewrite pattern.

## Infrastructure to land before any specific mapping

- ✅ **Done.** `DuckDbExpressionTranslator` reads `Constraint.getExpression()` and emits DuckDB SQL fragments via `trino_<name>(...)` macros. `DucklakeMetadata.applyFilter` decomposes top-level AND-conjuncts, translates each, and stores the survivors on `DucklakeTableHandle.pushedExpressions`. `ConstraintApplicationResult` returns translated conjuncts in `remainingExpression` too (mixed-format safety — see "When pushdown is allowed to fire" below).
- ✅ **Done.** The handle's pushed state surfaces in the SQL via `DuckDbSelectSqlBuilder`, shared by both `InProcessDuckDbExecutor` and `QuackDuckDbExecutor`. Same WHERE-clause shape regardless of engine.
- ✅ **Done — alias layer + brain.** The `trino_*` macros and the `trino_meta()` table macro are `DefaultMacro[]` / `DefaultTableMacro[]` arrays in the `trino_parity` extension (`src/macro_definitions.cpp`), registered at LOAD via `DefaultFunctionGenerator::CreateInternalMacroInfo`; functions DuckDB can't match as a macro (`lower`/`upper`/`reverse`, the trim family, `normalize/1`, and the hash trio) are native C++. `TrinoFunctionAliases` just `LOAD`s the bundled extension binary on attach (no SQL-resource replay; it also best-effort `INSTALL/LOAD icu` for `with_timezone`). The Java-side `DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS` set is kept in lockstep with `trino_meta()` by `TestTrinoFunctionAliases#testJavaPushableSetMatchesDuckDbMeta`. Adding a new entry means updating both sides and adding a semantic fixture; the lockstep tests fail otherwise.
- ⏳ `DucklakeFunctionProvider` (for step 5 — DuckDB-namespaced exclusives via `ConnectorFunctionProvider`). Not started.
- ✅ **Done — native DuckDB extension for Trino-equivalent semantics.** The `trino_parity` extension carries native C++ implementations for functions where DuckDB's built-in diverges from Trino in ways collations / SQL wrapping can't fix: `lower`/`upper` (ICU full case folding), `reverse` (code-point), the trim family, `normalize/1` (NFC), and the hash trio `sha512`/`xxhash64`/`hmac_sha256` (vendored xxHash + WjCryptLib). The connector bundles the host-built binary and `LOAD`s it on attach. Any future divergence of this shape gets a native function in the same extension rather than a placeholder.

### Native-semantics functions (formerly placeholders)

Some functions can't be expressed as a macro over DuckDB's built-ins without diverging from Trino on real-world (non-ASCII) input. These were *initially* shipped as pushed "placeholders" (ASCII-safe macros, with a one-shot warn-on-emit), and have since been **replaced by native C++ implementations in the `trino_parity` extension** (`src/string_functions.cpp`, ICU-backed). They are fully Trino-aligned now, in `trino_meta()` + `PUSHABLE_FUNCTIONS`, with no placeholder tag and no warn-on-emit. The `@placeholder` machinery and `PLACEHOLDER_TRINO_NAMES` are gone.

The divergences the native versions fix (kept here as the rationale; the originating audit is [REPORT-string-unicode-audit.md](REPORT-string-unicode-audit.md)):
- `lower` / `upper` — DuckDB does *simple* case folding, Trino does *full*. `lower('İ')`: DuckDB → `'i'`, Trino → `'i' + U+0307`. `upper('ß')`: DuckDB → `'ẞ'` (U+1E9E), Trino → `'SS'`. Native impl uses ICU full case folding (root locale), matching Java.
- `reverse` — DuckDB is grapheme-cluster-aware, Trino is code-point-only. Diverges on combining marks (`reverse('cafe' + U+0301)`) and ZWJ emoji families. Native impl reverses code points.
- The trim family + `normalize/1` (NFC) are native for the same reason (vendored ICU; `normalize/2` stays unpushed because the bundled ICU snapshot only carries NFC data).

Pinned by `TestTrinoFunctionAliases#testUnicodeCaseFoldingMatchesTrinoSpec`, which asserts the formerly-divergent inputs now match Trino.

## When pushdown is allowed to fire (mixed-format tables)

DuckLake tables can carry mixed file formats — parquet and duckdb side by side, snapshot by snapshot. Pushdown gates per format.

**Regime 1 — common-SQL functions** (`LIKE`, `UPPER`, `SUBSTRING`, basic math/date — anything in both engines' built-in set).
- Push as a hint AND return as `remainingFilter`.
- `.db` splits: DuckDB pre-filters server-side, fast path.
- `.parquet` splits: Trino re-applies the same predicate above the scan, correct result.
- No homogeneity check needed. Double-evaluation on `.db` splits is trivial — DuckDB already removed most rows.

**Regime 2 — DuckDB-namespaced functions** (`cosine_distance`, `list_*`, `struct_extract`, …).
- Trino has no implementation, so a parquet split has no way to evaluate the function above the scan.
- `applyFilter` checks `ducklake_data_file` at plan time: every file for this scan must have `file_format = 'duckdb'`. If not, reject with a clear error pointing at the offending format mix.
- If homogeneous: push to the executor's SQL AND remove from `remainingExpression` — we own the evaluation end-to-end.

Skip the "route parquet through DuckDB too" alternative. Loses Trino's native parquet reader + Alluxio page-cache + soft-affinity work; undoes the layered design from `archive/CONCEPT-duckdb-as-parquet-file-cache.md`.

## What's out of scope

- Aggregate / window pushdown (`applyAggregation`). Different SPI, different design problem. Tracked separately.
- Join pushdown (`applyJoin`). JDBC connector territory; not relevant for us.
- Trino → backend pushdown for non-DuckDB executors (e.g., direct-to-S3 parquet reads). The parquet path stays as it is.

## FIXED (2026-06-11) — Quack-engine secret write-write race; NEW vortex-s3 read finding

The race: concurrent queries with s3 targets on `ducklake.execution-engine=quack` each ran
`CREATE OR REPLACE SECRET ducklake_s3 (...)` server-side
(`QuackDuckDbExecutor.serverInitStatementsFor`, both the `HttpfsS3` and s3-`FileScan` branches),
and DuckDB 1.5.3 aborts overlapping catalog writes — `Catalog write-write conflict on
create/alter with "ducklake_s3"` — despite identical content (observed live 2026-06-10; the old
"same credentials → safe race" comment was wrong). Reproduction also showed a second mode the
conflicts had masked: the `OR REPLACE` drop+recreate window left concurrently-binding scans
secretless.

**The fix (two halves, pinned by `TestDucklakeQuackS3InitRace` — 8 barrier-aligned threads × 3
rounds × both target shapes):**
- `DuckDbS3Config.renderCreateSecretSql()` now emits **`CREATE SECRET IF NOT EXISTS`** — probed
  on 1.5.3: an existing secret makes it a catalog no-op (400 concurrent creates, 0 conflicts),
  so steady state is write-free; in-process executors are unaffected (fresh instance per split).
  Tradeoff: first creator wins, so rotated s3 creds need a Quack-server restart.
- **`DuckDbCatalogWriteRetry`** (bounded, jittered, "write-write conflict" substring match)
  around the server-side init statements + ATTACH for the first-contact storm (probed: 7 of 8
  simultaneous first-creates conflict). Each attempt uses a fresh local Statement — duckdb_jdbc
  invalidates a Statement after a failed execution. Unit-pinned by `TestDuckDbCatalogWriteRetry`.

**New finding (separate pre-existing bug, discovered by the regression test):** `read_vortex`
over s3 **never consumed the DuckDB httpfs secret** — it binds through Rust object_store and
fails to the IMDS fallback (`PUT http://169.254.169.254/...`, seconds-long) even single-threaded
with the secret present (probed 2026-06-11). Only the vortex COPY *write* honors the secret,
which is what made the secret channel look sufficient. Vortex-s3 reads are lance-shaped
(HANDOFF-lance-route-a O1): they need the `AWS_*` env channel
(`DuckDbS3Config.toObjectStoreEnv()` on the Quack sidecar). **Resolved same day:** the streaming
vortex FileScan no longer carries `DuckDbS3Config` (mirrors lance — see
`resolveDuckDbReadTarget`), so `FileScan.s3Config` is production-empty for every current format;
the executor plumbing is kept for a future scan extension that reads through DuckDB's own
filesystem layer (which DOES honor secrets, like `.db` ATTACH). `.db` (`HttpfsS3`) targets
genuinely use the secret and are fully covered by the regression test.

## Status

- Step 0 (this doc, mapping table) — drafted.
- Pushdown infrastructure (translator + alias layer + applyFilter wiring + Java↔DuckDB parity test) — **shipped**.
- Step 1 (LIKE / NOT LIKE) — **shipped**; translator branch on `$like` with reflective `LikePattern` access (the class is in `trino-main`, not `trino-spi`). `NOT LIKE` reuses the existing `$not` recursion. Unit tests cover wildcard, ESCAPE, single-quote escaping in pattern and escape char, function-expression on the value side, dynamic-pattern rejection. End-to-end test in `TestDucklakeDuckDbReadMode#testLikePredicatePushesDown`.
- Step 2 (numeric / comparison) — **partial**; comparisons + 25 numeric macros shipped (abs/ceil/floor/mod/power/sqrt/exp/ln/log2/log10 + sin/cos/tan/asin/acos/atan/atan2/sinh/cosh/tanh/degrees/radians/cbrt/truncate); arithmetic operator translation (`$add`, `$subtract`, etc.) still deferred.
- Step 3 (string basics) — **shipped**; 15 string macros + 3 regex macros + 7 encoding/distance macros, including:
  - Trim family with full Java whitespace set (round 4 fix).
  - `lower/1`, `upper/1`, `reverse/1` shipped as **native C++ in the extension**, fully Trino-aligned on non-ASCII input (ICU full case folding + code-point reverse). Initially shipped as ASCII-safe placeholders (round 4), then made native — see "Native-semantics functions (formerly placeholders)" above and [REPORT-string-unicode-audit.md](REPORT-string-unicode-audit.md).
  - `chr`, `url_encode/decode`, `to_hex/from_hex`, `to_base64/from_base64`, `levenshtein_distance`, `hamming_distance` (round 4).
  - Still deferred: `concat` (NULL-propagation differs), `position` (operator-form).
- Step 4 (date / time) — **complete (chunks 1 + 2 + 3 + 3.5 + 4 shipped)**. Chunk 1 (round 6j) added the translator's argument-type-gate registry plus 12 new pushable entries (Tier A DATE-only, Tier B DATE or TIMESTAMP no-TZ). Chunk 2 added `TrinoTimeZoneNormaliser` and threaded Trino's session `TimeZoneKey` through the page-source / executor stack so `SET TimeZone` fires at attach in both `InProcessDuckDbExecutor` and `QuackDuckDbExecutor`. Chunk 3 added the `pushdown_timestamp_with_timezone` session property (default off), threaded `ConnectorSession` through the translator's gates. Chunk 3.5 fixed `DucklakeArrowToPageConverter` to use the Arrow schema TZ instead of hardcoding `UTC_KEY` and promoted the full Tier C surface (`year/month/day/quarter/hour/minute/second/millisecond/date_trunc/date_diff`). Chunk 4 added `from_unixtime/1` and `with_timezone/2`; `at_timezone(WTZ, varchar)` was probed and confirmed **not pushable** through this connector (DuckDB's TIMESTAMPTZ has no per-value zone metadata, so the "rezone display" operation is fundamentally not expressible — documented). Year-boundary smoking gun confirmed end-to-end: Singapore session + property on + `WHERE year(ts) = 2025` over a `'2024-12-31 22:00 UTC'` literal matches the row through the full Trino stack. User-visible behaviour change: `SELECT timestamptz_col FROM duckdb_table` now renders in session zone (matches Iceberg / Hive / Parquet `isAdjustedToUtc=true`) instead of UTC. Date-specific design docs archived to [archive/](archive/). Open follow-up: parquet-format read path uses Trino's standard parquet reader which hardcodes `UTC_KEY` (same fault mode as our Arrow path pre-3.5) — documented in the archived TODO; not blocking since function-shape pushdown doesn't fire on parquet splits today. **Default-on flip landed 2026-06-06: `pushdown_timestamp_with_timezone` now defaults to `true`** (set `false` to keep Tier C predicates above the scan).
- Step 5 (DuckDB-namespaced exclusives via `ConnectorFunctionProvider`) — not started. NB: a
  class named `DucklakeFunctionProvider` now exists, but it's the `FunctionProvider` for the Lance
  and change-feed TABLE functions — a different SPI from the scalar `ConnectorFunctionProvider` this
  step needs.
- Step 6 (Lance table functions) — shipped (`lance_vector_search` / `lance_fts` /
  `lance_hybrid_search`).

### Catalog totals (as of the native hash port — 2026-06-06)

- 95 `trino_meta()` rows / ~79 function names across 8 categories (string, numeric, regex, encoding, distance, hash, date, conditional). Round 6j (step 4 chunk 1) added 12 date/time entries with the translator's first argument-type gate; step 4 chunk 4 added `from_unixtime/1` and `with_timezone/2` as Tier C extras; `at_timezone` is documented as not-pushable. Date-specific design docs are in [archive/](archive/).
- Hash port shipped (2026-06-06): `sha512/1`, `xxhash64/1`, and `hmac_sha256/2` are **native C++ scalar functions** in the `trino_parity` extension (`src/hash_functions.cpp`), over vendored xxHash (BSD-2) + WjCryptLib SHA (public domain) in `third_party/hash/`. This replaced an interim macro layer that INSTALL/LOADed the `crypto`/`hashfuncs` *community* extensions best-effort — dropped to remove both the per-DuckDB-version availability lag and those extensions' load-time telemetry. `hmac_sha256` is pushable natively (raw-bytes HMAC) where the VARCHAR-only `crypto_hmac` macro route was not. `murmur3` deferred — see rounds 6b-ext/6c below.
- `lower/1`, `upper/1`, `reverse/1` (+ trim family, `normalize/1`) are native C++ in the extension — fully Trino-aligned, no longer placeholders and no warn-on-emit.
- Round 6a shipped: `sign/1`, `bit_length/1`, `pi/0`, `bitwise_xor/2`, `regexp_replace/{2,3}` (with `'g'` flag for Trino-aligned global default).
- Round 6b-core shipped: `md5/1`, `sha1/1`, `sha256/1` (via `unhex(...)` wrap for VARBINARY return type).
- Round 6f shipped (translator): arithmetic ops `$add/$subtract/$multiply/$divide/$modulo`, `$coalesce` (variadic), `$nullif`, `$identical` (IS NOT DISTINCT FROM).
- Round 6g shipped (macros): bitwise function-form `bitwise_and/or/not/left_shift/right_shift` + date convenience `year/month/day/quarter`.
- Round 6h shipped (translator): `$cast` / `$try_cast` for primitive types (BOOLEAN/TINYINT/SMALLINT/INTEGER/BIGINT/DOUBLE/VARCHAR/DATE), plus `$negate` (unary minus).
- Round 6i shipped (macros): `if/{2,3}`, `date_trunc/2`, `date_diff/3`. Plus end-to-end verification that `BETWEEN` already pushes (Trino's planner decomposes to `≥ AND ≤` before `applyFilter`, so the existing comparison + AND translators handle it without code change).
- Round 6j shipped (step 4 chunk 1, translator + macros): argument-type-gate registry in the translator (sparse `TYPE_GATES` map keyed by `NameArity`, accepts/rejects entries based on actual call argument types — closes the latent bug where date macros pushed for any arg type). Re-gated `year/month/day/quarter`/`date_trunc`/`date_diff` to `{DATE, TIMESTAMP no-TZ}`. Added Tier A (DATE-only): `day_of_week/1` (`isodow`, ISO 1=Mon..7=Sun, avoiding DuckDB's bare 0=Sun trap), `day_of_year/1`, `last_day_of_month/1`, `week/1`, `week_of_year/1`, `year_of_week/1`, `yow/1`. Added Tier B (DATE or TIMESTAMP no-TZ): `hour/1`, `minute/1`, `second/1`, `millisecond/1` (millis-of-second 0..999), `to_unixtime/1`. TIMESTAMP WITH TIME ZONE deferred to chunk 3 (needs session-TZ plumbing from chunk 2).
- Step 4 chunk 2 shipped (plumbing, no new macros): `TrinoTimeZoneNormaliser` in production (lift of the validated three-rule function from `ProbeDuckDbTimeZoneHandling`, 14 unit-test cases covering every shape from the REPORT Q3 table). `ExecutionRequest` extended with `Optional<String> duckDbTimeZone`; threaded through `DucklakePageSourceProvider.createDuckDbPageSource` → `DuckDbFilePageSource` constructor. Both executors now run `SET TimeZone = '<normalised>'` immediately after attach (`InProcessDuckDbExecutor.applySessionTimeZone`; `QuackDuckDbExecutor.applyServerSideTimeZone` via `quack_query_by_name`). One-shot WARN per (executor, normalised-zone) on `SET TimeZone` failure; execution proceeds without setting the zone — Tier A/B is wall-clock invariant so the failure mode is graceful. Empirically confirmed (`TestDucklakeDuckDbExecutorBackends.sessionTimeZonePropagatesToBothBackends`) that the SET TimeZone persists across `quack_query_by_name` calls in the same client session, that LA-zone request renders the smoking-gun TIMESTAMPTZ in LA, and that LA-zone request does NOT match a Singapore-rendered string (control case, proves the zone is being consulted not incidentally matched).
- Step 4 chunk 3 shipped (narrow Tier C): added `pushdown_timestamp_with_timezone` session property (default off). Translator's `ArgTypeGate` interface extended with `ConnectorSession session`; backward-compat overloads (`translateConjuncts(expr, assignments)`, `translate(expr, assignments)`) delegate with `null` session → property reads as off → existing test fixtures unchanged. `DucklakeMetadata.applyFilter` passes session through. ONLY `to_unixtime/1` was promoted to accept `TIMESTAMP WITH TIME ZONE` when property is on — it's the lone zone-invariant entry in the catalog (returns absolute UTC epoch in both engines regardless of any zone). Zone-dependent extracts (`year/month/day/quarter/hour/minute/second/millisecond/date_trunc/date_diff`) stayed blocked over WTZ pending chunk 3.5 converter fix.
- Step 4 chunk 3.5 shipped (converter + full Tier C): `DucklakeArrowToPageConverter.writeTimestampTzColumn` now resolves a `TimeZoneKey` from `vector.getField().getType()` as `ArrowType.Timestamp.getTimezone()` and constructs WTZ values with it. Defensive fallback to `UTC_KEY` on null TZ or unparseable string (`TimeZoneKey.getTimeZoneKey` throwing `TimeZoneNotSupportedException` or `IllegalArgumentException`) with one-shot WARN per zone. Translator gates for `year/month/day/quarter/hour/minute/second/millisecond/date_trunc/date_diff` unified to share `argTier(...)` with `to_unixtime`. Year-boundary smoking gun (`testTierCYearBoundarySmokingGunSingaporeSession`) fires end-to-end: Singapore session + property on + `WHERE year(ts) = 2025` over `'2024-12-31 22:00 UTC'` matches the row through Trino's full stack; property-off returns the same row (invariance under toggle is the load-bearing claim). User-visible behaviour change: `SELECT timestamptz_col FROM duckdb_table` renders in session zone now, matching other Trino connectors (Iceberg / Hive / Parquet `isAdjustedToUtc=true`); no existing test pinned the prior UTC rendering. Property defaulted off at ship; flipped to default **on** 2026-06-06 after burn-in.
- Step 4 chunk 4 shipped (Tier C extras): `from_unixtime/1` (Trino `double → TIMESTAMP(3) WITH TIME ZONE`) via DuckDB's `to_timestamp(numeric)`; both engines return zone-invariant absolute epochs and the WTZ output composes safely with chunk-3.5's converter. `with_timezone/2` (Trino `(timestamp, varchar) → WTZ`) via DuckDB's `timezone(zone, t)` — note the arg-order flip; gate strictly to TIMESTAMP no-TZ input. Probed `at_timezone(WTZ, varchar)` and confirmed **not pushable** through this connector — DuckDB's `WTZ AT TIME ZONE 'X'` and `timezone('X', WTZ)` return TIMESTAMP no-TZ instead of WTZ, because DuckDB's TIMESTAMPTZ has no per-value zone metadata. Documented in macro SQL header + RESEARCH-function-mapping.md. After this chunk the date-specific design docs (PLAN / REPORT / chunk-tracker) are archived to `dev-docs/archive/`; the date pushdown program is complete for this iteration.

---

## Round 6+ — Easy adds via known-safe extensions

Picked from the community-extensions audit (see [RESEARCH-function-community-extensions.md](RESEARCH-function-community-extensions.md)). Scope rule: each item is a macro-shape add (rename, optional unhex wrap), no new infrastructure required. Excluded: anything needing operator translation, table-function pushdown, or aggregate pushdown.

**Skipped explicitly (per workload triage):**
- `splink_udfs` — `soundex` is the only Trino-relevant entry, and it's niche. Defer indefinitely.
- `datasketches` — sketch state is not wire-compatible with Trino's, so cross-engine upstream aggregation would diverge. We're not on aggregate pushdown yet anyway. Defer until aggregate pushdown lands.
- `inet` operator pushdown — `contains(network, address)` → `network >>= addr` requires translator to learn DuckDB's `>>=` / `<<=` operators. Separate infrastructure round.
- `netquack` `url_extract_parameter(url, name)` — requires correlated-subquery emission for the `extract_query_parameters(url)` table function. Separate infrastructure round.

### Round 6a — Core DuckDB extras (no new extensions) — ✅ **SHIPPED**

Cheap wins. All confirmed aligned via DuckDB docs check; no probe required.

| Trino | DuckDB | Status |
|---|---|---|
| `sign(x)` | `sign(x)` | ✅ shipped |
| `bit_length(string)` | `bit_length(string)` | ✅ shipped |
| `pi()` / 0-arg | `pi()` | ✅ shipped — first 0-arg entry in catalog, exercises translator's empty-args path |
| `bitwise_xor(x, y)` | `xor(x, y)` | ✅ shipped — DuckDB's only scalar bitwise op (and/or/not/shift are operator-only, queued for translator rewrite in 6e) |
| `regexp_replace(s, p)` (Trino: removes matches) | `regexp_replace(s, p, '', 'g')` | ✅ shipped — macro passes `''` + `'g'` to match Trino's global-remove default |
| `regexp_replace(s, p, repl)` | `regexp_replace(s, p, repl, 'g')` | ✅ shipped — `'g'` flag makes DuckDB's first-match default into Trino's global default |
| `truncate(x, n)` (2-arg) | — | ⏳ Deferred — DuckDB's `trunc` is 1-arg only. Would need `cast(x * pow(10,n)) / pow(10,n)` shim; not as clean. |

### Round 6b-core — Core DuckDB hash macros (NO new extension, 3 entries)

**Promoted to front of queue** based on `ProbeHashNullHandling` findings (see [archive/REPORT-hash-null-handling.md](archive/REPORT-hash-null-handling.md)). Core DuckDB ships `md5`, `sha1`, `sha256` returning hex VARCHAR; wrap with `unhex(...)` to match Trino's VARBINARY return. NULL propagation empirically verified aligned (`md5(NULL) → NULL`, `md5('a' || NULL || 'c') → NULL`).

| Trino | DuckDB macro body | Notes |
|---|---|---|
| `md5(varbinary) -> varbinary` | `unhex(md5(b))` | ✅ NULL-aligned. No extension needed. |
| `sha1(varbinary) -> varbinary` | `unhex(sha1(b))` | ✅ |
| `sha256(varbinary) -> varbinary` | `unhex(sha256(b))` | ✅ |

> **PORTED TO NATIVE (2026-06-06).** `sha512`, `xxhash64`, and `hmac_sha256` are
> now native C++ scalar functions in the `trino_parity` extension
> (`src/hash_functions.cpp`), implemented over vendored primitives — **xxHash**
> (BSD-2, header-only) and **WjCryptLib SHA-256/512** (public domain) — in
> `third_party/hash/`. The macro layer and the best-effort `INSTALL … FROM
> community` of `crypto`/`hashfuncs` were **removed**. Rationale: those are
> *community* extensions (per-DuckDB-version availability lag — we hit a 2-week
> 1.5.3 gap), and both ship **load-time telemetry** (`query_farm_telemetry.cpp`
> POSTs to `duckdb-in.query-farm.services`, auto-loading httpfs) — unacceptable
> for an embedded data-plane connector. Going native also **revived
> `hmac_sha256`** (see below). The round notes below are kept as the research
> record of how we got here.

### Round 6b-ext — Crypto extension — **sha512 + hmac_sha256 PORTED NATIVE**

**`sha512` — ✅ SHIPPED (native).** Standard SHA-512 over the raw bytes, 64-byte
BLOB matching Trino's VARBINARY. NULL-propagates. Vectors: `''`→`cf83e135…da3e`,
`'abc'`→`ddaf35a1…ca49f`. (Interim macro route, now removed, used `crypto`'s
`crypto_hash('sha2-512', b)` — which in 1.5.3 returns BLOB directly, no `unhex`.)

**`hmac_sha256` — ✅ SHIPPED (native), reversing the earlier rejection.** The
*macro* route was not pushable: DuckDB's `crypto_hmac` is
**`[VARCHAR, VARCHAR, VARCHAR]`** — key/message VARCHAR-only — while Trino's
`hmac_sha256(data, key)` (`HmacFunctions.java`, 481: data first) operates on
arbitrary **`VARBINARY`**. A `BLOB→VARCHAR` bridge **escapes non-printable
bytes** (`from_hex('ff00')::VARCHAR` → the 8-char string `'\xFF\x00'`), silently
hashing the wrong bytes on binary input. The **native** function (HMAC-SHA256 over
WjCryptLib SHA-256, raw bytes) has no such problem, so it is pushable. Vectors:
HMAC(`key`,`"The quick brown fox…"`)→`f7bc83f4…2d1a3cd8`; HMAC(`''`,`''`)→`b613679a…92c5ad`;
NULL data or key → NULL. `hmac_md5`/`hmac_sha1`/`hmac_sha512` remain unported
(add the matching WjCryptLib primitives if a workload wants them).

### Round 6c — Hashfuncs extension — **xxhash64 PORTED NATIVE; murmur3 DEFERRED**

**`xxhash64` — ✅ SHIPPED (native).** Byte-order question resolved:
- Trino 481 (`VarbinaryFunctions.xxhash64`): `hash.setLong(0, Long.reverseBytes(XxHash64.hash(slice)))`. `Slice.setLong` writes little-endian, so `reverseBytes(H)` written LE = **big-endian serialization of `H`**.
- xxHash64 (seed 0) is a frozen spec: empty → `ef46db3751d8e999`, `abc` → `44bc2cf5ad770999`. The native function calls upstream `XXH64(bytes, len, 0)` and emits the 8 bytes big-endian, byte-for-byte equal to Trino. `xxhash64(NULL)=NULL`.

The native function (`trino_xxhash64`, no type gate — Trino emits only over VARBINARY, same as md5/sha1/sha256) replaces the interim macro `unhex(printf('%016x', xxh64(b)))` that depended on the `hashfuncs` extension.

**`murmur3` — ⚠️ DEFER (reconstructable, but not worth it yet).** DuckDB `murmurhash3_x64_128(ANY) -> UHUGEINT` is standard seed-0 (empty → 0; `abc` → `3ba2744126ca2d52b4963f3f3fad7867`, packing `UHUGEINT = (h1<<64)|h2`). Trino 481 (`murmur3`) returns `setLong(0,h1); setLong(8,h2)` = **`LE(h1) ++ LE(h2)`** (each half byte-reversed vs the big-endian packing). A faithful macro would be:
```
reverse(unhex(printf('%016x', (murmurhash3_x64_128(b) >> 64)::UBIGINT)))
  || reverse(unhex(printf('%016x', (murmurhash3_x64_128(b) & 18446744073709551615)::UBIGINT)))
```
i.e. expected Trino `murmur3('abc')` bytes = `522dca264174a23b6778ad3f3f3f96b4`. **Why defer:** (a) a macro can't bind a local, so it double-evaluates the 128-bit hash; (b) the `h1==hi64` half-assignment + airlift seed-0 equivalence is *inferred from source*, not yet confirmed against a live Trino-481 value; (c) `murmur3` is niche. Recommendation: leave unpushed unless a workload asks; if pursued, pin one live Trino value first and consider a small native helper instead of the double-hash macro.

### Round 6d — NetQuack extension — **❌ NOT PUSHABLE (research 2026-06-06)**

`netquack` is in the catalog, but the `extract_*` functions are **not lossless** against Trino's `url_extract_*`. Verified divergences:

- **NULL vs empty string (systematic).** Trino 481 `url_extract_*` are all `@SqlNullable` and return **NULL** when the component is absent or the URL is malformed (`return (uri == null) ? null : slice(uri.getScheme())`, etc.). DuckDB `extract_*` return **empty string `''`** for absent components (`extract_schema('ex.com/a')=''`, `extract_query_string('http://ex.com/a')=''`, `extract_fragment(...)=''`). Any predicate touching absent components (`IS NULL`, `= ''`, `IS NOT NULL`) would silently change results.
- **`url_extract_port` type mismatch.** Trino: `-> BIGINT`, NULL when absent. DuckDB `extract_port`: `-> VARCHAR`, `''` when absent.
- **Path divergence.** Trino `url_extract_path('http://ex.com')` → `''` (Java `URI.getPath()`); DuckDB `extract_path('http://ex.com')` → `'/'`.

Per the lossless discipline ("when in doubt, don't push"), **reject all six.** Revisiting would require per-function `NULLIF(x,'')` + cast wrappers and case-by-case malformed-URL alignment — that's a curated normalization project, not a rename round. Not worth it absent a concrete workload.

Note: NetQuack also ships `base64_encode`/`base64_decode` overlapping core DuckDB's `to_base64`/`from_base64` (already in round 4). No additional value.

### Round 6e (deferred — separate infra round) — Translator rewrites, not macros

These need translator-level work because the shape change can't be expressed as a single macro substitution:

- ✅ **`concat(a, b, c, ...)` → `(a || b || c || ...)` operator chain — SHIPPED.** Empirically verified ([archive/REPORT-hash-null-handling.md](archive/REPORT-hash-null-handling.md)): DuckDB's `concat` skips NULL while Trino's NULL-propagates, but the `||` operator NULL-propagates on both sides. `DuckDbExpressionTranslator` rewrites `Call(concat, [args])` into a parenthesized `||` chain when the return type is `VARCHAR` (gates out the `concat(array, array)` overload, which has different NULL semantics). Variadic, any arity ≥ 2. End-to-end coverage in `TestDucklakeDuckDbReadMode#testConcatPredicatePushesDownAsOperatorChain` and `#testConcatWithNullPropagatesTrinoSemantics` (the second pins the NULL-propagation guarantee on the duckdb-format path).
- `contains(network, address)` → emit `network >>= addr` operator form (inet extension).
- `url_extract_parameter(url, name)` → emit correlated subquery through `extract_query_parameters(url)` table function (netquack extension).
- IPADDRESS / IPPREFIX type → connector-side type plumbing.

Tracked here for visibility; not part of round 6 macro adds.

### Extension load mechanism

To avoid load-failure cascades:

1. Extend `TrinoFunctionAliases` to load extensions in best-effort fashion alongside the existing `INSTALL icu; LOAD icu;` pair. New `INSTALL`/`LOAD` lines tagged best-effort by `isBestEffort()`.
2. Each round 6 macro body references its extension's functions — `applyDirect()` will skip macro creation cleanly if the extension load failed, because the macro body will fail to bind. We want this: if an extension isn't available, the macros silently don't ship and the translator's parity check between Java `PUSHABLE_FUNCTIONS` and DuckDB `trino_meta()` flags the gap.
3. Or — alternative — split the alias SQL into per-extension chunks and only register the corresponding `PUSHABLE_FUNCTIONS` entries when the extension load succeeds. More work, more correct under partial-loads.

Pick the simpler form (1+2) for round 6; revisit if production deploys need partial-load guarantees.

### Test surface backing the pushdown layer

- `TestDuckDbExpressionTranslator` — synthetic `ConnectorExpression` → SQL string assertions.
- `TestDuckDbSelectSqlBuilder` — pushed-expression strings appear in the WHERE clause sent to DuckDB.
- `TestTrinoFunctionAliases` — every `trino_meta()` row has both a macro and a Trino-aligned semantic fixture; Java `PUSHABLE_FUNCTIONS` matches DuckDB-side `trino_meta()` exactly (drift = test failure).
- `TestDucklakeDuckDbReadMode#testFunctionPredicatePushesDownThroughTrinoMacro` — end-to-end Trino query against a `.db` table with `WHERE lower(name) = 'apple'`; the WHERE clause sent to DuckDB contains `trino_lower(...)`, so DuckDB resolving the query is the live signal that the macro layer is installed and the translator wired through.

### Adding a new alias — checklist

Macros and `trino_meta()` live in the `trino_parity` extension repo
([duckdb-trino-parity-extension](../../../duckdb-trino-parity-extension)), so
steps 1–2 are edits there + a rebuild of the bundled binary.

1. Add the entry to the extension's `src/macro_definitions.cpp`: a `DefaultMacro`
   row (`{DEFAULT_SCHEMA, "trino_<name>", {args…, nullptr}, {{nullptr,nullptr}}, "<duckdb_body>"}`),
   OR a native C++ scalar function (in `src/string_functions.cpp` /
   `src/hash_functions.cpp`, registered in `trino_parity_extension.cpp`) when the
   semantics can't be matched by a macro over DuckDB built-ins.
2. Add the matching `('name', arity, 'category')` row to `trino_meta()` in
   `src/macro_definitions.cpp`, then rebuild the host binary (`make`) — the
   trino-ducklake gradle build re-bundles it.
3. Add the matching `NameArity(name, arity)` to `DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS`
   (and a `TYPE_GATES` entry if the push must be restricted by argument type).
4. Add one or more `c("...", name, arity, "SELECT trino_<name>(...)", expected)` fixtures in `TestTrinoFunctionAliases#semanticCases()` (expected value chosen to match Trino's documented behaviour, not DuckDB's).
5. Run `:trino-ducklake:test` — the parity + coverage guards (`testJavaPushableSetMatchesDuckDbMeta`, `testMetaMatchesFixtures`) fail loudly if any of steps 1–4 are out of sync.
