# TODO: Trino → DuckDB pushdown

Bring our duckdb-format read path up to the BigQuery / Snowflake connector shape: a curated translator that turns Trino's `ConnectorExpression` predicates into DuckDB SQL, plus DuckDB-namespaced functions exposed through Trino's function SPI for things Trino doesn't have natively.

Background and architecture: see [RESEARCH-lance-and-pushdown.md](RESEARCH-lance-and-pushdown.md).
Function mapping reference: see [RESEARCH-function-mapping.md](RESEARCH-function-mapping.md).

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
   - **`lower/1` and `upper/1` ARE placeholders, NOT shipped.** Empirically, DuckDB's lower/upper do simple Unicode case folding (`lower('İ')` → `'i'`, `upper('ß')` → `'ẞ'`) while Trino's Java-based implementations do full case folding (`lower('İ')` → `'i' + U+0307`, `upper('ß')` → `'SS'`). Collations (NOCASE / NOACCENT / NFC / icu_*) and `nfc_normalize` do NOT change `lower()`/`upper()` output in DuckDB — verified by `ProbeDuckDbCaseFolding` before deletion. Pushing would silently drop rows Trino would have kept. The macros are installed (for ASCII-safe direct use) but excluded from `trino_meta()` and `PUSHABLE_FUNCTIONS`. See "Placeholder macros — extension required" above.
   - **`concat` shipped as translator rewrite** (round 6e): `Call(concat, [a,b,c])` → `(a || b || c)` when return type is `VARCHAR`. DuckDB's `concat` skips NULL; the `||` operator NULL-propagates in both engines and matches Trino's `concat` semantics.
   - Not yet shipped: `position` (operator-form; needs translator special case).
   - Regex (RE2 on both): `regexp_like/2`, `regexp_extract/{2,3}`. `regexp_like` exercises the rename pattern (Trino `regexp_like` → DuckDB `regexp_matches` via macro body).
4. ⏳ **Date / time** — risky because of timezone, locale, calendar. Do one function at a time, with cross-engine semantic tests. Skip anything that touches session timezone interpretation until we've verified the alignment. Plan + test corpus in [PLAN-pushdown-datetime.md](PLAN-pushdown-datetime.md). Tiered rollout (A: DATE only → B: TIMESTAMP no-TZ → C: TIMESTAMP WTZ behind session flag → D: format/parse never push). First commit fixes the latent type-gate bug in existing `year/month/day/quarter`/`date_trunc`/`date_diff` entries (they currently push for any arg type, would silently diverge on TIMESTAMP WTZ).
5. ⏳ **DuckDB-namespaced exclusives** — vector / JSON / list / struct / regex variants that exist in DuckDB but not in Trino. Register through `ConnectorFunctionProvider`, route through the same translator. Lower-risk than #4 because we own both ends of the semantic contract. Not started.
6. ⏳ **Lance table functions** when we get there. Sits on top of the infrastructure above.

## Infrastructure to land before any specific mapping

- ✅ **Done.** `DuckDbExpressionTranslator` reads `Constraint.getExpression()` and emits DuckDB SQL fragments via `trino_<name>(...)` macros. `DucklakeMetadata.applyFilter` decomposes top-level AND-conjuncts, translates each, and stores the survivors on `DucklakeTableHandle.pushedExpressions`. `ConstraintApplicationResult` returns translated conjuncts in `remainingExpression` too (mixed-format safety — see "When pushdown is allowed to fire" below).
- ✅ **Done.** The handle's pushed state surfaces in the SQL via `DuckDbSelectSqlBuilder`, shared by both `InProcessDuckDbExecutor` and `QuackDuckDbExecutor`. Same WHERE-clause shape regardless of engine.
- ✅ **Done — alias layer + brain.** `trino-function-aliases.sql` defines `trino_*` macros and a `trino_meta()` table macro. `TrinoFunctionAliases` loads the SQL on every attach (best-effort INSTALL/LOAD icu; required CREATE OR REPLACE MACRO). The Java-side `DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS` set is kept in lockstep with `trino_meta()` by `TestTrinoFunctionAliases#testJavaPushableSetMatchesDuckDbMeta`. Adding a new alias means updating both sides and adding a semantic fixture; the lockstep tests fail otherwise.
- ⏳ `DucklakeFunctionProvider` (for step 5 — DuckDB-namespaced exclusives via `ConnectorFunctionProvider`). Not started.
- ⏳ **Native DuckDB extension for Trino-equivalent semantics.** Required for functions where DuckDB's built-in diverges from Trino's documented behavior in ways collations and SQL-only wrapping can't fix. First known cases: `lower`/`upper` (Unicode case folding). Likely future cases: anything where Trino's Java-stdlib semantics produce multi-codepoint expansions or locale-specific behavior DuckDB doesn't expose. Rust template: <https://github.com/duckdb/extension-template-rs>. Until shipped, the affected macros stay as ASCII-only placeholders (see below).

### Placeholder macros — extension required

When a `trino_<name>` macro can be installed but produces results that diverge from Trino on real-world inputs, we tag it with `-- @placeholder <name>` in `trino-function-aliases.sql`. As of round 4 the policy is:

- Placeholders ARE in `trino_meta()` and `DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS` so the translator pushes them. This lets us measure pushdown performance and characterize the corner-case impact on real workloads.
- `DuckDbExpressionTranslator` logs a one-shot WARN per placeholder name when emitting a pushdown call. `TrinoFunctionAliases` also logs a one-shot WARN at first attach naming all installed placeholders. Two signals, complementary scopes.
- ASCII inputs are handled correctly by all placeholders today. Non-ASCII corner-case divergences are catalogued in [REPORT-string-unicode-audit.md](REPORT-string-unicode-audit.md).

Current placeholders:
- `trino_lower(s)` — DuckDB does simple case folding; Trino does full case folding. `lower('İ')`: DuckDB → `'i'`, Trino → `'i' + U+0307`. Collations (NOCASE / NOACCENT / NFC / icu_*) do not change `lower()` output — verified empirically. Fix path: native extension exposing ICU `u_strFoldCase`.
- `trino_upper(s)` — same root cause. `upper('ß')`: DuckDB → `'ẞ'` (U+1E9E), Trino → `'SS'`.
- `trino_reverse(s)` — DuckDB reverse is grapheme-cluster-aware; Trino is code-point-only. Diverges on combining marks (`reverse('cafe' + U+0301)`) and ZWJ sequences (emoji families). Fix path: extension that calls `u_strReverse` on raw code points.

To re-promote a placeholder to fully-aligned once the extension ships:
1. Change the macro body in `trino-function-aliases.sql` to call the extension function.
2. Remove the `-- @placeholder` tag (or keep it temporarily during the migration).
3. Confirm `DuckDbExpressionTranslator.PLACEHOLDER_TRINO_NAMES` no longer contains the name (it's auto-derived from `@placeholder` lines, so removing the tag is enough).
4. Add Trino-aligned semantic fixtures including the Unicode corpus that originally tripped the divergence — converts the audit-doc "documented divergences" into "regression tests".

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

Skip the "route parquet through DuckDB too" alternative. Loses Trino's native parquet reader + Alluxio page-cache + soft-affinity work; undoes the layered design from `CONCEPT-duckdb-as-parquet-file-cache.md`.

## What's out of scope

- Aggregate / window pushdown (`applyAggregation`). Different SPI, different design problem. Tracked separately.
- Join pushdown (`applyJoin`). JDBC connector territory; not relevant for us.
- Trino → backend pushdown for non-DuckDB executors (e.g., direct-to-S3 parquet reads). The parquet path stays as it is.

## Status

- Step 0 (this doc, mapping table) — drafted.
- Pushdown infrastructure (translator + alias layer + applyFilter wiring + Java↔DuckDB parity test) — **shipped**.
- Step 1 (LIKE / NOT LIKE) — **shipped**; translator branch on `$like` with reflective `LikePattern` access (the class is in `trino-main`, not `trino-spi`). `NOT LIKE` reuses the existing `$not` recursion. Unit tests cover wildcard, ESCAPE, single-quote escaping in pattern and escape char, function-expression on the value side, dynamic-pattern rejection. End-to-end test in `TestDucklakeDuckDbReadMode#testLikePredicatePushesDown`.
- Step 2 (numeric / comparison) — **partial**; comparisons + 25 numeric macros shipped (abs/ceil/floor/mod/power/sqrt/exp/ln/log2/log10 + sin/cos/tan/asin/acos/atan/atan2/sinh/cosh/tanh/degrees/radians/cbrt/truncate); arithmetic operator translation (`$add`, `$subtract`, etc.) still deferred.
- Step 3 (string basics) — **shipped**; 15 string macros + 3 regex macros + 7 encoding/distance macros, including:
  - Trim family with full Java whitespace set (round 4 fix).
  - `lower/1`, `upper/1`, `reverse/1` shipped as **pushable placeholders** with one-shot warn-on-emit (round 4 — see [REPORT-string-unicode-audit.md](REPORT-string-unicode-audit.md)). Native extension required for full Trino-equivalent semantics on non-ASCII input.
  - `chr`, `url_encode/decode`, `to_hex/from_hex`, `to_base64/from_base64`, `levenshtein_distance`, `hamming_distance` (round 4).
  - Still deferred: `concat` (NULL-propagation differs), `position` (operator-form).
- Step 4 (date / time) — not started; plan + cross-engine test corpus drafted in [PLAN-pushdown-datetime.md](PLAN-pushdown-datetime.md). First step-4 PR fixes the latent type-gate bug in the round-6g/6i date macros before adding new entries.
- Step 5 (DuckDB-namespaced exclusives via `ConnectorFunctionProvider`) — not started.
- Step 6 (Lance table functions) — not started.

### Catalog totals (as of round 6i)

- 78 `trino_meta()` rows / ~62 function names across 8 categories (string, numeric, regex, encoding, distance, hash, date, conditional).
- 3 placeholders: `lower/1`, `upper/1`, `reverse/1`. Pushed for perf; warn-on-emit fires once per name per JVM.
- Round 6a shipped: `sign/1`, `bit_length/1`, `pi/0`, `bitwise_xor/2`, `regexp_replace/{2,3}` (with `'g'` flag for Trino-aligned global default).
- Round 6b-core shipped: `md5/1`, `sha1/1`, `sha256/1` (via `unhex(...)` wrap for VARBINARY return type).
- Round 6f shipped (translator): arithmetic ops `$add/$subtract/$multiply/$divide/$modulo`, `$coalesce` (variadic), `$nullif`, `$identical` (IS NOT DISTINCT FROM).
- Round 6g shipped (macros): bitwise function-form `bitwise_and/or/not/left_shift/right_shift` + date convenience `year/month/day/quarter`.
- Round 6h shipped (translator): `$cast` / `$try_cast` for primitive types (BOOLEAN/TINYINT/SMALLINT/INTEGER/BIGINT/DOUBLE/VARCHAR/DATE), plus `$negate` (unary minus).
- Round 6i shipped (macros): `if/{2,3}`, `date_trunc/2`, `date_diff/3`. Plus end-to-end verification that `BETWEEN` already pushes (Trino's planner decomposes to `≥ AND ≤` before `applyFilter`, so the existing comparison + AND translators handle it without code change).

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

**Promoted to front of queue** based on `ProbeHashNullHandling` findings (see [REPORT-hash-null-handling.md](REPORT-hash-null-handling.md)). Core DuckDB ships `md5`, `sha1`, `sha256` returning hex VARCHAR; wrap with `unhex(...)` to match Trino's VARBINARY return. NULL propagation empirically verified aligned (`md5(NULL) → NULL`, `md5('a' || NULL || 'c') → NULL`).

| Trino | DuckDB macro body | Notes |
|---|---|---|
| `md5(varbinary) -> varbinary` | `unhex(md5(b))` | ✅ NULL-aligned. No extension needed. |
| `sha1(varbinary) -> varbinary` | `unhex(sha1(b))` | ✅ |
| `sha256(varbinary) -> varbinary` | `unhex(sha256(b))` | ✅ |

### Round 6b-ext — Crypto extension (1 new ext, 5 entries) — **WAITING on catalog**

Macro pattern: `CREATE OR REPLACE MACRO trino_<name>(...) AS unhex(crypto_hash('<algo>', ...));` — wraps hex VARCHAR back into BLOB matching Trino's VARBINARY return.

Pre-step: extend `TrinoFunctionAliases` (or the executor attach) to `INSTALL crypto; LOAD crypto;` as best-effort, mirroring the ICU pattern.

**⚠️ Catalog wait:** `crypto` was HTTP 404 on `extensions.duckdb.org` for DuckDB 1.5.3 (probed 2026-05-28). Likely cause is the community catalog hasn't yet cut a 1.5.3 build — not a permanent platform gap. Re-probe in a few days; the catalog catches up over time. See [REPORT-hash-null-handling.md](REPORT-hash-null-handling.md).

| Trino | DuckDB macro body | Notes |
|---|---|---|
| `sha512(varbinary) -> varbinary` | `unhex(crypto_hash('sha2-512', b))` | ✅ no core equivalent — extension required |
| `hmac_md5(k, m) -> varbinary` | `unhex(crypto_hmac('md5', k, m))` | ✅ |
| `hmac_sha1(k, m) -> varbinary` | `unhex(crypto_hmac('sha1', k, m))` | ✅ |
| `hmac_sha256(k, m) -> varbinary` | `unhex(crypto_hmac('sha2-256', k, m))` | ✅ |
| `hmac_sha512(k, m) -> varbinary` | `unhex(crypto_hmac('sha2-512', k, m))` | ✅ |

Verification step before promoting to `PUSHABLE_FUNCTIONS`: probe hash output exact-bytes against Trino-reference vectors. Once aligned, ship.

### Round 6c — Hashfuncs extension (1 new ext, ~2 new entries) — **WAITING on catalog**

**⚠️ Catalog wait:** `hashfuncs` HTTP 404 for DuckDB 1.5.3 — community catalog likely hasn't cut a 1.5.3 build yet. Re-probe in a few days.

| Trino | DuckDB macro body | Notes |
|---|---|---|
| `xxhash64(varbinary) -> varbinary` (8 bytes) | TBD — `xxh64(b)` returns UBIGINT; need to encode as 8-byte BLOB. Candidates: `unhex(printf('%016x', xxh64(b)))`, or a struct-cast trick. | ⚠️ Verify byte-order (Trino XXH64 → big-endian per impl? little-endian?). Probe before shipping. |
| `murmur3(varbinary) -> varbinary` (16 bytes, x64 128-bit variant) | TBD — `murmurhash3_x64_128(b)` returns UHUGEINT; encode as 16-byte BLOB. Same byte-order question. | ⚠️ Same — verify byte-order vs Trino. |

If byte-order verification fails, these become extension-required candidates instead.

Pre-step: `INSTALL hashfuncs; LOAD hashfuncs;` best-effort.

### Round 6d — NetQuack extension (1 new ext, 6 new entries) — **WAITING on catalog**

**⚠️ Catalog wait:** `netquack` HTTP 404 for DuckDB 1.5.3 — same likely cause as crypto/hashfuncs. Re-probe in a few days.

All straight macro renames. No type conversion.

| Trino | DuckDB macro body | Notes |
|---|---|---|
| `url_extract_protocol(url)` | `extract_schema(url)` | ✅ |
| `url_extract_host(url)` | `extract_host(url)` | ✅ |
| `url_extract_port(url)` | `extract_port(url)` | ✅ |
| `url_extract_path(url)` | `extract_path(url)` | ✅ |
| `url_extract_query(url)` | `extract_query_string(url)` | ✅ Note the name diff. |
| `url_extract_fragment(url)` | `extract_fragment(url)` | ✅ |

Pre-step: `INSTALL netquack; LOAD netquack;` best-effort.

Note: NetQuack also ships `base64_encode`/`base64_decode` that overlap core DuckDB's `to_base64`/`from_base64` (already in round 4). No additional value beyond URL parsing — we don't re-route through netquack for base64.

### Round 6e (deferred — separate infra round) — Translator rewrites, not macros

These need translator-level work because the shape change can't be expressed as a single macro substitution:

- ✅ **`concat(a, b, c, ...)` → `(a || b || c || ...)` operator chain — SHIPPED.** Empirically verified ([REPORT-hash-null-handling.md](REPORT-hash-null-handling.md)): DuckDB's `concat` skips NULL while Trino's NULL-propagates, but the `||` operator NULL-propagates on both sides. `DuckDbExpressionTranslator` rewrites `Call(concat, [args])` into a parenthesized `||` chain when the return type is `VARCHAR` (gates out the `concat(array, array)` overload, which has different NULL semantics). Variadic, any arity ≥ 2. End-to-end coverage in `TestDucklakeDuckDbReadMode#testConcatPredicatePushesDownAsOperatorChain` and `#testConcatWithNullPropagatesTrinoSemantics` (the second pins the NULL-propagation guarantee on the duckdb-format path).
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

1. Add `CREATE OR REPLACE MACRO trino_<name>(...) AS <duckdb_body>;` to `resources/dev/brikk/ducklake/trino/plugin/trino-function-aliases.sql`.
2. Add the matching `(name, arity, category)` row to `trino_meta()` in the same file.
3. Add the matching `new NameArity(name, arity)` to `DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS`.
4. Add one or more `c("...", name, arity, "SELECT trino_<name>(...)", expected)` fixtures in `TestTrinoFunctionAliases#semanticCases()` (expected value chosen to match Trino's documented behaviour, not DuckDB's).
5. Run `:trino-ducklake:test` — the parity + coverage guards fail loudly if any of steps 1–4 are out of sync.
