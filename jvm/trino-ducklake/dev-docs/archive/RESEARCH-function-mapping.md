# RESEARCH: Trino ↔ DuckDB function mapping

**Status:** Canonical reference, not exhaustive. Updated as new pushdown candidates are added.
**Sources:** Trino 481 docs (`vendor/docs.trino.io/481/functions/`); DuckDB LTS docs (`vendor/duckdb-web/docs/lts/sql/functions/`).
**Companion doc:** [RESEARCH-function-community-extensions.md](RESEARCH-function-community-extensions.md) — DuckDB community / core extensions (`crypto`, `hashfuncs`, `datasketches`, `netquack`, `splink_udfs`, core `inet`, …) that fill many "Trino-only" gaps below.
**Convention:** Each row is one logical operation. "Trino" and "DuckDB" columns show the function name and signature in that engine. If the engines disagree on a detail (NULL handling, Unicode, type signature), note it in the Notes column. `—` means the engine doesn't have it.

A Doris column will be added later when we extend to that connector; leave the schema as it is for now.

Pushdown rating in the Notes column where useful:
- ✅ safe to translate directly
- ⚠️ translatable with caveat (note the caveat)
- ❌ do not translate — semantics differ, or one engine doesn't have it

**Done column** (added Mar 2026, tables where we have shipped at least one entry):
- `yes rN` — registered in `trino_meta()` and `DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS`. The number is the round it shipped in. See [TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md) for round-by-round detail.
- `yes rN ⚠️ placeholder` — macro is installed and PUSHABLE (so we can performance-test pushdown), but DuckDB's built-in diverges from Trino on specific non-ASCII inputs. `DuckDbExpressionTranslator` logs a one-shot WARN per name when emitting one of these. Native extension (Rust template: https://github.com/duckdb/extension-template-rs) is the durable fix. See [REPORT-string-unicode-audit.md](REPORT-string-unicode-audit.md) for the divergence catalog.
- `ext TBD` — *deprecated marker*: previously used when placeholders were excluded from pushdown. As of round 4 placeholders are pushable with warn-on-emit instead; no rows currently carry this marker.
- `—` — not implemented; not on the immediate roadmap.
- blank — not applicable (operator-shape, lambda-shape, or one-engine-only).

---

## Scalar functions

### String functions

| Operation | Trino | DuckDB | Done | Notes |
|---|---|---|---|---|
| String concat operator (NULL propagates) | `a \|\| b` | `a \|\| b` | | ✅ Both: any NULL operand → NULL. |
| Multi-arg concat (NULL propagates) | `concat(s1, ..., sN) -> varchar` | — (DuckDB `concat` SKIPS nulls); `\|\|` operator chain has aligned NULL-propagation | yes step 1 (translator rewrite) | ✅ Shipped as translator rewrite. Verified empirically ([archive/REPORT-hash-null-handling.md](archive/REPORT-hash-null-handling.md)): DuckDB `concat('a', NULL, 'c') = 'acd'`, `concat(NULL, NULL) = ''`; Trino returns NULL in both cases. `DuckDbExpressionTranslator` rewrites Trino's `Call(concat, [a, b, c])` → `(a \|\| b \|\| c)` when the return type is `VARCHAR` (gates out the `concat(array, array)` overload). Both engines NULL-propagate `\|\|` identically, so the rewrite is lossless. |
| Multi-arg concat (NULL skipped) | — | `concat(value, ...)` | | ❌ DuckDB-only semantics; route through Trino `concat_ws` or chain `coalesce`. |
| Concat with separator | `concat_ws(separator, s1, ..., sN)`, `concat_ws(sep, array(varchar))` | `concat_ws(separator, string, ...)` | yes r2 (2..5 arg) | ⚠️ Trino: NULL separator → NULL result; DuckDB: NULL separator → NULL result. NULL elements: Trino skips, DuckDB skips. Mostly aligned, but verify separator-NULL on the actual engine before pushing. Shipped as fixed-arity overloads 2..5. Array-form Trino-only. |
| Lowercase | `lower(string) -> varchar` | `lower(string)` | yes r1 ⚠️ placeholder | ⚠️ DuckDB does simple case folding; Trino does full case folding. Diverges on `'İ'` → DuckDB `'i'` vs Trino `'i'` + U+0307. ASCII safe. Pushed for perf with warn-on-emit; native extension is the durable fix. |
| Uppercase | `upper(string) -> varchar` | `upper(string)` | yes r1 ⚠️ placeholder | ⚠️ DuckDB `upper('ß')` = `'ẞ'` (U+1E9E); Trino's Java = `'SS'`. ASCII safe. Pushed with warn-on-emit. |
| Character length (code points) | `length(string) -> bigint` | `length(string)` | yes r1 | ⚠️ Both return count of code points (NOT bytes) for varchar. NULL → NULL in both. Trino has no separate `octet_length`; DuckDB has `strlen(string)` for bytes. |
| Byte length | `length(varbinary) -> bigint` | `strlen(string)`, `octet_length(blob)` | — | ✅ Map Trino `length(varbinary)` → DuckDB `octet_length`. Translator needs type awareness to choose between `length` and `octet_length` based on Trino arg type; queued. |
| Bit length | `bit_length(varchar) -> bigint` | `bit_length(string)` | yes r6 | ✅ Both return bits in the UTF-8 byte sequence (8 × octet length). |
| Grapheme cluster length | — | `length_grapheme(string)` | | ❌ DuckDB-only. |
| Substring (start) | `substring(string, start) -> varchar`, `substr(string, start)` | `substring(string, start)`, `substr(...)` | yes r1 | ✅ Both 1-based; negative start counts from end in both. **Unit: Unicode code points** (verified empirically + Trino source). NOT UTF-8 bytes, NOT graphemes. Both engines split combining marks, ZWJ emoji sequences, and flag emoji at codepoint boundaries — surprising but aligned. Verify both treat `start=0` identically (Trino: undefined-ish, DuckDB: behaves like 1) before pushing zero. |
| Substring (start, length) | `substring(string, start, length)` | `substring(string, start, length)` | yes r1 | ✅ Aligned for positive args. Unit pins added in fixtures: 2-byte UTF-8 (Cyrillic `'пингвин'`), 4-byte UTF-8 (penguin/duck/snake emoji), combining mark (`'café'` as `e + U+0301`), ZWJ family emoji (`'👨‍👩‍👧'`) — all match Trino's codepoint count exactly. See `TestTrinoFunctionAliases#semanticCases()` "substring 3:" rows. |
| Left N chars | — | `left(string, count)` | | ❌ DuckDB-only (Trino uses `substring(s,1,n)`). |
| Right N chars | — | `right(string, count)` | | ❌ DuckDB-only. |
| Trim both | `trim(string)`, `trim([LEADING\|TRAILING\|BOTH] chars FROM string)` | `trim(string[, characters])` | yes r1 (Java whitespace set) | ✅ Macro passes the full Java `Character.isWhitespace` set via the `characters` arg (helper `trino__java_whitespace_chars()`). Strips tab/LF/CR/FF/EM SPACE/Unicode separators; correctly leaves NBSP/figure space/narrow NBSP. Custom-char trim grammar still not pushed. |
| Trim left | `ltrim(string)` | `ltrim(string[, characters])` | yes r1 (Java whitespace set) | ✅ Same — full Java whitespace set. |
| Trim right | `rtrim(string)` | `rtrim(string[, characters])` | yes r1 (Java whitespace set) | ✅ Same. |
| Left pad | `lpad(string, size, padstring) -> varchar` | `lpad(string, count, character)` | yes r2 | ⚠️ Trino: `size` is code-point count of result; DuckDB: same. Both truncate to fit. Behavior on empty pad differs — Trino raises; DuckDB returns NULL-ish. Verify. |
| Right pad | `rpad(string, size, padstring)` | `rpad(string, count, character)` | yes r2 | Same caveat as `lpad`. |
| Replace (no replacement, i.e. remove) | `replace(string, search) -> varchar` | — | — | ❌ Trino single-arg form; DuckDB requires 3 args. Map by passing `''` as replacement. |
| Replace | `replace(string, search, replace)` | `replace(string, source, target)` | yes r1 | ✅ Same semantics: replace all occurrences, NULL → NULL. |
| Reverse | `reverse(string)` | `reverse(string)` | yes r1 ⚠️ placeholder | ⚠️ DuckDB reverse is grapheme-cluster-aware; Trino reverse is code-point-only. Diverges on combining marks and ZWJ sequences. ASCII safe. Pushed for perf with warn-on-emit; native extension needed for codepoint-strict reverse. |
| Repeat | — (via `repeat` for arrays only) | `repeat(string, count)` | | ❌ Trino has `repeat` only for arrays; use `array_join(repeat(s,n),'')` if needed. |
| Position of substring (1-based) | `strpos(string, substring) -> bigint`, `position(substring IN string)` | `instr(string, search_string)`, `strpos`, `position(s IN t)` | yes r1 (`strpos`/2) | ✅ Both 1-based, 0 if not found. Trino `strpos(s, sub, instance)` 3-arg form has no DuckDB equivalent. `position(... IN ...)` operator-form deferred. |
| Position of N-th occurrence | `strpos(string, substring, instance)` | — | | ❌ Trino-only. |
| Starts with | `starts_with(string, substring) -> boolean` | `starts_with(string, search_string)`, `s ^@ t`, `prefix(s, t)` | yes r1 | ✅ Aligned. |
| Ends with | — | `ends_with(string, search_string)`, `suffix(s, t)` | | ❌ Trino has no native `ends_with`; can be expressed via `substring` or `like '%x'`. |
| Contains substring | — (use `LIKE '%x%'` or `strpos > 0`) | `contains(string, search_string)` | | ❌ DuckDB has explicit `contains`; for pushdown translate Trino's `strpos(x) > 0` → DuckDB `contains` (safe) or leave both as `LIKE '%x%'`. |
| Split by delimiter (returns array) | `split(string, delimiter) -> array(varchar)`, `split(s, d, limit)` | `string_split(s, sep)`, `split(s, sep)` | — | ⚠️ Limit form is Trino-only. Empty-string delimiter behavior differs — Trino splits to character array; DuckDB returns a single-element array. |
| Split, get part at index | `split_part(string, delimiter, index) -> varchar` | `split_part(string, separator, index)` | — | ⚠️ Both 1-based; out-of-bounds: Trino returns NULL, DuckDB returns empty string. ❌ for pushdown unless wrap in NULLIF. |
| Split to map | `split_to_map(string, entryDelim, kvDelim)` | — | | ❌ Trino-only. |
| Lev distance | `levenshtein_distance(s1, s2) -> bigint` | `levenshtein(s1, s2)` | yes r4 | ⚠️ Same algorithm, different name. Renamed via macro body. Verify behavior on unequal-length strings — both engines compute insertions+deletions+substitutions. |
| Hamming distance | `hamming_distance(s1, s2) -> bigint` | `hamming(s1, s2)` | yes r4 | ✅ Same algorithm, different name. Renamed via macro. Both raise on unequal-length input. |
| Damerau-Lev distance | — | `damerau_levenshtein(s1, s2)` | | ❌ DuckDB-only. |
| Jaccard / Jaro / Jaro-Winkler | — | `jaccard`, `jaro_similarity`, `jaro_winkler_similarity` | | ❌ DuckDB-only. |
| ASCII code of first char | `codepoint(string) -> integer` | `ascii(string)`, `unicode(string)`, `ord(string)` | | ⚠️ Trino `codepoint` requires single-char varchar(1); DuckDB `unicode` takes any varchar and uses first char. NOT 1:1. |
| Char from code | `chr(n) -> varchar` | `chr(code_point)` | yes r4 | ✅ Aligned for valid code points. |
| Translate chars | `translate(source, from, to) -> varchar` | `translate(string, from, to)` | yes r3 | ✅ Same algorithm: char-by-char replacement, extra `from` chars deleted. |
| Unicode normalize | `normalize(string[, form])` | `nfc_normalize(string)` | ⚠️ DuckDB only NFC; Trino has NFC/NFD/NFKC/NFKD. Push only when both sides agree on form=NFC. |
| Soundex | `soundex(char) -> string` | `soundex(s)` (splink_udfs); also `double_metaphone(s)` for a stronger encoder | — | ✅ Pushable when `splink_udfs` is loaded. See [community-extensions § Splink UDFs](archive/RESEARCH-function-community-extensions-detail.md#splink-udfs). |
| Word stem | `word_stem(word[, lang]) -> varchar` | — | | ❌ Trino-only. |
| Luhn check | `luhn_check(string) -> boolean` | — | | ❌ Trino-only. |
| Format with `printf`-style | `format(format, args...) -> varchar` | `printf(format, ...)`, `format(format, ...)` | | ❌ Different format specifications (Trino: Java `Formatter`; DuckDB: fmt/printf). Do not push. |
| To UTF-8 bytes | `to_utf8(string) -> varbinary` | `encode(string)` | — | ⚠️ Names differ; semantics match. |
| From UTF-8 bytes | `from_utf8(binary[, replace])` | `decode(blob)` | — | ⚠️ Names differ. Behavior on invalid UTF-8 differs — Trino has a replacement-char form; DuckDB errors. |
| URL decode | `url_decode(value) -> varchar` | `url_decode(string)` | yes r4 | ✅ Aligned (RFC 3986 percent-encoding). |
| URL encode | `url_encode(value) -> varchar` | `url_encode(string)` | yes r4 | ✅ Aligned. |
| Hex encode | `to_hex(binary) -> varchar` | `hex(blob)`, `to_hex(string)` | yes r4 | ✅ Aligned. Macro body calls DuckDB `hex`. |
| Hex decode | `from_hex(string) -> varbinary` | `unhex(value)`, `from_hex(value)` | yes r4 | ✅ Aligned. Macro body calls DuckDB `unhex`; returns BLOB. |
| Base64 encode | `to_base64(binary) -> varchar` | `to_base64(blob)`, `base64(blob)` | yes r4 | ✅ Aligned (standard alphabet). |
| Base64 decode | `from_base64(string) -> varbinary` | `from_base64(string)` | yes r4 | ✅ Aligned. |
| Base64URL encode | `to_base64url(binary)` | — | ❌ Trino-only (URL-safe alphabet). |
| Base32 encode | `to_base32(binary)` | — | ❌ Trino-only. |
| Strip accents | — | `strip_accents(string)` | ❌ DuckDB-only. |

### Numeric / math functions

| Operation | Trino | DuckDB | Done | Notes |
|---|---|---|---|---|
| Abs | `abs(x) -> [same]` | `abs(x)`, `@(x)` | yes r2 | ✅ Aligned. Watch INT overflow: `abs(MIN_INT)` — Trino throws, DuckDB throws. |
| Ceiling | `ceil(x)`, `ceiling(x)` | `ceil(x)`, `ceiling(x)` | yes r2 | ✅ Aligned. |
| Floor | `floor(x)` | `floor(x)` | yes r2 | ✅ Aligned. |
| Round half-up | `round(x)`, `round(x, d)` | `round(v, s)` | — | ⚠️ Trino: `round` is half-up. DuckDB: `round` is half-away-from-zero (since 0.10) — verify per version. Also `round_even` in DuckDB for banker's rounding. Do NOT push when `d > 0` until verified. |
| Round half-even | — (no direct) | `round_even(v, s)` | | ❌ DuckDB-only. |
| Truncate toward zero | `truncate(x)` | `trunc(x)` | yes r5 | ⚠️ Different names; same semantics. Renamed via macro body. |
| Sign | `sign(x) -> [same]` | `sign(x)` | yes r6 | ⚠️ Both return -1/0/1. NaN behaviour on floats verified to align (both NaN→NaN). |
| Mod | `mod(n, m) -> [same]`, `n % m` | `n % m`, `mod(n, m)` via `fmod` for floats | yes r2 (int only) | ⚠️ Integer `%`: both follow truncated division (sign follows dividend). Float `%`: Trino uses IEEE `remainder`-ish; DuckDB has `fmod`. ❌ Do not push float `%` until aligned. Macro is type-agnostic; translator must gate by arg type. |
| Power | `pow(x, p) -> double`, `power(x, p)` | `pow(x, y)`, `power(x, y)` | yes r2 | ✅ Aligned. |
| Sqrt | `sqrt(x) -> double` | `sqrt(x)` | yes r3 | ✅ Aligned. NaN on negative in both. |
| Cube root | `cbrt(x) -> double` | `cbrt(x)` | yes r5 | ✅ Aligned. |
| Exp | `exp(x) -> double` | `exp(x)` | yes r3 | ✅ Aligned. |
| Natural log | `ln(x) -> double` | `ln(x)` | yes r3 | ✅ Aligned. NaN on x≤0 in both. |
| Log base 2 | `log2(x) -> double` | `log2(x)` | yes r3 | ✅ Aligned. |
| Log base 10 | `log10(x) -> double` | `log10(x)`, `log(x)` (single-arg) | yes r3 | ⚠️ DuckDB `log(x)` = log10 (PostgreSQL convention). Trino `log(b, x)` is log-base-b. Shipped as explicit `trino_log10` → `log10` to avoid the `log` collision; do not push the single-arg `log(x)`. |
| Log base b | `log(b, x) -> double` | — (use `ln(x)/ln(b)`) | | ❌ DuckDB has no 2-arg `log`. |
| Pi / e | `pi() -> double`, `e() -> double` | `pi()` | yes r6 (`pi` only) | ✅ `pi()` shipped. DuckDB has no `e()`; for Trino's `e()` use `exp(1)` if needed. |
| Trig (sin/cos/tan/asin/acos/atan/atan2) | `sin(x)`, `cos(x)`, `tan(x)`, `asin(x)`, `acos(x)`, `atan(x)`, `atan2(y, x)` | same names; also `cot`, `asinh`, `acosh`, `atanh` | yes r5 | ✅ Aligned for the common set. DuckDB has extras Trino lacks. |
| Hyperbolic | `sinh`, `cosh`, `tanh` | `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` | yes r5 (forward only) | ⚠️ Forward hyperbolics shipped. Inverse hyperbolics DuckDB-only — not pushed. |
| Degrees / radians | `degrees(x)`, `radians(x)` | `degrees(x)`, `radians(x)` | yes r5 | ✅ Aligned. |
| NaN / infinity | `is_nan(x)`, `is_finite(x)`, `is_infinite(x)`, `nan()`, `infinity()` | `isnan(x)`, `isfinite(x)`, `isinf(x)`, `'nan'::double`, `'infinity'::double` | ⚠️ Function names differ; behavior aligned. Comparisons with NaN: BOTH treat `NaN = NaN` as `true` for grouping/distinct (Trino since recent versions; DuckDB consistent) — verify in pushdown tests. |
| Random | `rand()`, `random()`, `random(n)`, `random(m,n)` | `random()` (returns [0,1) double) | ❌ Non-deterministic. Do not push. |
| Width bucket | `width_bucket(x, b1, b2, n)`, `width_bucket(x, bins)` | `equi_width_bins(min, max, bincount)` | ⚠️ Different shape; not 1:1. Do not push. |
| From base (parse) | `from_base(string, radix) -> bigint` | — | ❌ Trino-only. |
| To base (format) | `to_base(x, radix) -> varchar` | `to_base(number, radix[, min_length])` | ⚠️ DuckDB supports padding; semantics for positive ints match. |
| Factorial | — | `factorial(x)` | ❌ DuckDB-only. |
| GCD / LCM | — | `gcd(x, y)`, `lcm(x, y)` | ❌ DuckDB-only. |
| Statistical CDFs (beta/normal/t) | `beta_cdf`, `inverse_beta_cdf`, `normal_cdf`, `inverse_normal_cdf`, `t_cdf`, `t_pdf` | — | ❌ Trino-only. |
| Cosine distance / similarity | `cosine_distance(array(double),array(double))`, `cosine_similarity(...)` | `array_cosine_distance`, `array_cosine_similarity`, `list_cosine_distance`, `list_cosine_similarity` | ⚠️ Trino uses sparse-vector form (map) and dense array form; DuckDB array vs list. Verify shape before pushing. |
| Euclidean / dot product | `euclidean_distance(array,array)`, `dot_product(array,array)` | `array_distance`, `array_dot_product`, `list_distance`, `list_dot_product`, `list_inner_product` | ⚠️ Pushable for ARRAY-typed columns; LIST is DuckDB-only shape. |

### Date / time / timestamp / interval functions

⚠️ **Whole category is high-risk for pushdown.** Trino has rich `TIMESTAMP WITH TIME ZONE` vs `TIMESTAMP` distinction; DuckDB has `TIMESTAMP` and `TIMESTAMPTZ`. Calendar = proleptic Gregorian in both. Week numbering (`week()`/`week_of_year`) follows ISO-8601 in BOTH (Mon-start, week 01 contains Jan 4). Default DOW base: Trino `day_of_week` = 1..7 with Monday=1; DuckDB `dayofweek` from `date_part('dow', ...)` = 0..6 with **Sunday=0** — ❌ do not map directly. Use `isodow` in DuckDB (1..7, Monday=1) to match Trino.

This table is 4-column (no separate Done column); shipped rows are flagged inline in the Notes with a leading `yes r6:` token.

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Current date | `current_date` | `current_date`, `today()` | ⚠️ Both session-local. Do not push if behavior must match a specific timezone definition. |
| Current timestamp | `current_timestamp`, `current_timestamp(p)`, `now() -> timestamp(3)` | `current_timestamp`, `now()`, `get_current_timestamp()`, `transaction_timestamp()` | ⚠️ Both bind once per query. Precision: Trino default = 3; DuckDB = µs (6). Cast carefully if comparing. |
| Local timestamp | `localtimestamp`, `localtimestamp(p)`, `localtime` | `localtimestamp`, `localtime`, `current_localtimestamp()` | ⚠️ Same idea, different precision defaults. |
| Build date | — | `make_date(y, m, d)` | ❌ DuckDB-only. Trino uses `date '2020-01-01'` literal or cast. |
| Build timestamp | — | `make_timestamp(y, m, d, h, mi, s)`, `make_timestamp(microseconds)` | ❌ DuckDB-only. |
| Build time | — | `make_time(h, m, s)` | ❌ DuckDB-only. |
| Date truncate | `date_trunc(unit, x) -> [same]` | `date_trunc(part, x)` | **yes r6**: ✅ Shipped as passthrough macro. Aligned for the intersection unit set (`second/minute/hour/day/week/month/quarter/year`). ⚠️ Return-type caveat: DuckDB always returns TIMESTAMP (even for DATE input); Trino preserves input type. Auto-cast in comparisons makes typical WHERE predicates align numerically. Revisit if a use case needs DATE-typed output. |
| Date add | `date_add(unit, value, x) -> [same]` | `date_add(date, interval)`, `x + INTERVAL n unit` | ❌ Signatures incompatible. Translate Trino `date_add('day', 5, x)` → DuckDB `x + INTERVAL 5 DAY`. |
| Date diff | `date_diff(unit, t1, t2) -> bigint` | `date_diff(part, t1, t2)` | **yes r6**: ✅ Shipped as passthrough macro. Both engines return integer count of unit-boundaries crossed (not whole units elapsed); verified empirically with `date_diff('month', '2024-01-31', '2024-02-01') = 1`. Unit-name intersection same as `date_trunc`. |
| Date subtract | — | `date_sub(part, t1, t2)` | ❌ DuckDB returns total complete units; differs from `date_diff`. Trino has no equivalent. |
| Date format | `date_format(timestamp, format) -> varchar`, `format_datetime(timestamp, format)` | `strftime(date, format)` | ❌ Format strings differ entirely: Trino uses JodaTime/MySQL format; DuckDB uses strftime. Do not push. |
| Parse date/time from string | `parse_datetime(string, format) -> timestamp`, `from_iso8601_*` | `strptime(text, format)`, `try_strptime`, ISO via cast | ❌ Format syntax differs. Push only ISO-8601 case (Trino `from_iso8601_timestamp(s)` ≈ DuckDB `s::timestamp`). |
| Extract field | `extract(field FROM x)` | `extract(part FROM x)`, `date_part(part, x)` | ⚠️ Field names mostly aligned (`year, month, day, hour, minute, second`). `dow`/`day_of_week` differ — see header. `quarter` aligned. |
| Year / month / day convenience | `year(x)`, `month(x)`, `day(x)` | `year(x)`, `month(x)`, `day(x)` | **yes r6**: ✅ Shipped via direct macro passthrough. Both engines align on DATE and TIMESTAMP input. |
| Day of week | `day_of_week(x) -> bigint`, `dow(x)` | `isodow(x)` (1..7, Mon=1) OR `dayofweek(x)` (0..6, Sun=0) | yes step 4 ch1 (DATE only) — ✅ Shipped: `trino_day_of_week(d) AS isodow(d)`. Type gate restricts to DATE pending Tier B follow-up for TIMESTAMP. The DuckDB-`dayofweek(x)` 0=Sun trap is avoided by using `isodow`. Pinned by fixture `'2024-01-07'` → 7 (Sunday). |
| Day of year | `day_of_year(x)`, `doy(x)` | `dayofyear(x)` | yes step 4 ch1 (DATE only) — ✅ Shipped: `trino_day_of_year(d) AS dayofyear(d)`. Pinned by leap-day fixture `'2024-02-29'` → 60. |
| Day of month | `day_of_month(x)` | `date_part('day', x)` | ✅ Aligned. |
| Week | `week(x)`, `week_of_year(x)` | `week(x)` | yes step 4 ch1 (DATE only) — ✅ Shipped: `trino_week(d) AS week(d)`, `trino_week_of_year(d) AS week(d)`. DuckDB's bare `week()` IS already ISO-aligned (probed: `week('2023-01-01') = 52`, `week('2024-12-30') = 1`). Boundary fixtures pin both. |
| Year of week | `year_of_week(x)`, `yow(x)` | `extract('isoyear' FROM x)` | yes step 4 ch1 (DATE only) — ✅ Shipped: `trino_year_of_week(d) AS extract('isoyear' FROM d)::BIGINT`, same body for `trino_yow`. DuckDB has no bare `isoyear()` — reach via `extract('isoyear' ...)`. Pinned by `'2024-12-30'` → 2025 (Monday but ISO week 1 of 2025). |
| Quarter | `quarter(x)` | `date_part('quarter', x)`, `quarter(x)` | **yes r6**: ✅ Shipped via direct macro passthrough. |
| Hour / minute / second / millisecond | `hour(x)`, `minute(x)`, `second(x)`, `millisecond(x)` | direct `hour(x)`/`minute(x)`/`second(x)`; `extract('millisecond' FROM x)` for millis-of-second | yes step 4 ch1 (DATE or TIMESTAMP no-TZ) — ✅ Shipped (Tier B). Trino's `millisecond()` returns the millis-OF-SECOND (0..999), NOT epoch millis — DuckDB's `extract('millisecond' FROM t)` matches. Cast to BIGINT in the macro to align return types. Pinned by `'2024-06-15 12:00:00.123'` → 123. |
| Timezone hour / minute | `timezone_hour(timestamp)`, `timezone_minute(timestamp)` | — | ❌ Trino-only; emulate via `date_part('timezone_hour', x)` in DuckDB. |
| At time zone | `at_timezone(timestamp(p) with tz, zone)`, `with_timezone(timestamp(p), zone)` | `timezone(text, timestamp)`, `x AT TIME ZONE z` | yes step 4 ch4 (`with_timezone` only) — ✅ `with_timezone(TIMESTAMP, varchar)` shipped via `timezone(zone, t)` macro (arg order flipped). Result is `TIMESTAMPTZ` in both engines. `at_timezone(WTZ, varchar)` **NOT pushable through this connector** because DuckDB's `WTZ AT TIME ZONE 'X'` and `timezone('X', WTZ)` return `TIMESTAMP` (no-TZ) — DuckDB's `TIMESTAMPTZ` has no per-value zone metadata, so "rezone display" is fundamentally not expressible. |
| To unix time (seconds) | `to_unixtime(timestamp) -> double` | `epoch(timestamp)::DOUBLE` | yes step 4 ch1 (DATE or TIMESTAMP no-TZ) — ✅ Shipped: `trino_to_unixtime(t) AS epoch(t)::DOUBLE`. Explicit cast lifts DuckDB's bigint-seconds to Trino's double-seconds shape. Pinned: epoch `'1970-01-01 00:00:00'` → 0.0; pre-epoch `'1969-12-31 23:59:59'` → -1.0. |
| From unix time | `from_unixtime(unixtime) -> timestamp(3) with time zone`, `from_unixtime(unixtime, zone)`, `from_unixtime_nanos` | `to_timestamp(double)` returns `TIMESTAMPTZ` | yes step 4 ch4 (1-arg only) — ✅ Shipped: `trino_from_unixtime(d) AS to_timestamp(d)`. Both engines return the same absolute instant for the same epoch; rendering depends on session zone. Chunk-3.5 converter constructs the WTZ output with session zone so downstream operations align. Negative / subsecond / large-epoch round-trips pinned. Zone-form (2-arg) and `from_unixtime_nanos` not shipped (no DuckDB equivalent / signature mismatch). |
| To ISO 8601 string | `to_iso8601(x) -> varchar` | — (`strftime(x, '%Y-%m-%dT%H:%M:%S.%fZ')` or implicit cast) | ❌ Push as cast or do not push. |
| ISO timestamp parse | `from_iso8601_timestamp(string) -> timestamp(3)`, `from_iso8601_date(string) -> date` | implicit cast from ISO string | ⚠️ Map Trino `from_iso8601_timestamp(s)` → DuckDB `CAST(s AS TIMESTAMP)`. Verify offset handling. |
| Last day of month | `last_day_of_month(x) -> date` | `last_day(x)` | yes step 4 ch1 (DATE only) — ✅ Shipped: `trino_last_day_of_month(d) AS last_day(d)`. Pinned by leap-Feb `'2024-02-15'` → `'2024-02-29'` and non-leap century `'1900-02-15'` → `'1900-02-28'`. |
| Day name / month name | — | `dayname(x)`, `monthname(x)` | ❌ DuckDB-only (English only; locale not pluggable). |
| Time bucket | — | `time_bucket(width, x[, offset/origin])` | ❌ DuckDB-only; analogous to Trino `date_bin` (not in 481 docs). |
| Age between two timestamps | — | `age(t1, t2)`, `age(t)`, `ago(interval)` | ❌ DuckDB-only. |
| Interval extract | — | `date_part(part, interval)`, `epoch(interval)`, `to_*(integer)` | ❌ Trino has `extract` on intervals via SQL grammar; pushdown risky. |
| Build interval from N units | — | `to_days(n)`, `to_hours(n)`, `to_months(n)`, etc. | ❌ DuckDB-only; Trino uses literal `INTERVAL n DAY`. |
| Parse duration string | `parse_duration(string) -> interval` | — | ❌ Trino-only. |
| Human readable seconds | `human_readable_seconds(double) -> varchar` | — | ❌ Trino-only. |
| To milliseconds (from interval) | `to_milliseconds(interval) -> bigint` | `epoch_ms(timestamp)`, `to_milliseconds(integer)` | ❌ Different operand types — Trino takes interval, DuckDB takes integer (constructs interval). Not 1:1. |
| Julian day | — | `julian(x)` | ❌ DuckDB-only. |
| Generate timestamp series | — | `generate_series(t1, t2, interval)`, `range(t1, t2, interval)` | ❌ DuckDB-only as scalar; Trino's `sequence` is similar (array form). |

### Pattern matching (LIKE) and regular expressions

| Operation | Trino | DuckDB | Done | Notes |
|---|---|---|---|---|
| LIKE | `string LIKE pattern [ESCAPE c]` | `string LIKE target` | yes step 1 | ✅ Translator branch on `$like`. Pattern arrives as a `Constant` of `LikePatternType` whose value is an `io.trino.type.LikePattern`; that class lives in `trino-main` (not `trino-spi`), so `DuckDbExpressionTranslator` reads `getPattern()` / `getEscape()` reflectively (`LikePatternAccessor`). Emits `(value LIKE 'pattern' [ESCAPE 'c'])`. Wildcards `%` / `_` aligned. ESCAPE aligned. NULL handling aligned. Dynamic pattern (non-`Constant`) stays unpushed. |
| NOT LIKE | `string NOT LIKE pattern` | `string NOT LIKE target` | yes step 1 | ✅ Arrives as `Call($not, [Call($like, ...)])`; the existing `$not` branch recurses into the LIKE handler — no extra code. |
| ILIKE (case-insensitive) | — | `string ILIKE target`, `ilike_escape(...)` | | ❌ Trino has no native ILIKE (use `lower(s) LIKE lower(p)`). DuckDB native ILIKE — map carefully. |
| SIMILAR TO (POSIX-ish) | `string SIMILAR TO pattern` | `string SIMILAR TO regex` | — | ⚠️ Both use a SQL-standard SIMILAR TO. Verify subtle differences in `*`/`+`/`?` quantifier scoping before pushing. |
| Regex match (contains) | `regexp_like(string, pattern) -> boolean` | `regexp_matches(string, pattern[, options])` | yes r3 | ⚠️ Both use **RE2** engine (Trino via Re2J; DuckDB via google/re2). Syntax aligned. Trino's pattern is case-sensitive by default; DuckDB likewise unless `'i'` option. ✅ Safe to push when no options used. Shipped via rename (macro body calls DuckDB `regexp_matches`). |
| Regex full match | — (use `^...$`) | `regexp_full_match(string, regex[, options])` | | ⚠️ DuckDB-specific. Translate Trino `regexp_like(s, '^p$')` → DuckDB `regexp_full_match(s, 'p')`. |
| Regex count | `regexp_count(string, pattern) -> bigint` | — (use `len(regexp_extract_all(...))`) | | ❌ Trino-only direct form. |
| Regex extract (first match) | `regexp_extract(string, pattern)`, `regexp_extract(s, p, group)` | `regexp_extract(string, regex[, group][, options])` | yes r3 (2- and 3-arg) | ⚠️ Default group: Trino = 0 (whole match), DuckDB = 0. ✅ Aligned for 2- and 3-arg form. Note: DuckDB reserves `group`; macro parameter named `group_index`. |
| Regex extract all | `regexp_extract_all(string, pattern[, group])` | `regexp_extract_all(string, regex[, group][, options])` | — | ⚠️ Empty-match handling differs; verify before pushing. |
| Regex replace | `regexp_replace(s, p)`, `regexp_replace(s, p, repl)`, `regexp_replace(s, p, fn)` | `regexp_replace(s, p, repl[, options])` | yes r6 (2- and 3-arg) | ⚠️ Macro passes `('g')` options flag so DuckDB's first-match default becomes Trino's global default. 2-arg form passes `''` as replacement (Trino's remove-matches semantics). Lambda form Trino-only — not pushed. Backreference syntax aligned (`\1`,`\2`). |
| Regex split | `regexp_split(string, pattern)` | `regexp_split_to_array(s, r[, options])`, `string_split_regex`, `regexp_split_to_table` | — | ⚠️ Name differs. Push as renamed call. |
| Regex position | `regexp_position(s, p[, start[, occurrence]])` | — | ❌ Trino-only. |
| Escape regex special chars | — | `regexp_escape(string)` | ❌ DuckDB-only. |

### JSON functions

⚠️ **Path syntax mismatch.** Trino uses a restricted JSONPath subset (`$.foo`, `$.bar[2]`) but documented behavior is "JSON Path Reference" — see Trino docs. DuckDB-core JSON functions live in the **`json` extension** (auto-loaded) and use JSONPath-ish with `$.foo[*]`. Wildcards and filters differ. Do not push complex paths without per-case verification.

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Is JSON scalar | `is_json_scalar(json) -> boolean` | — | ❌ Trino-only. |
| JSON array contains | `json_array_contains(json, value) -> boolean` | — | ❌ Trino-only. |
| JSON array length | `json_array_length(json) -> bigint` | `json_array_length(json[, path])` (extension) | ⚠️ Both exist; verify path arg behavior. |
| JSON array get by index | `json_array_get(json_array, index) -> json` | `json_extract(json, '$[i]')` | ❌ Trino-only direct form. |
| JSON extract (returns JSON) | `json_extract(json, json_path) -> json` | `json_extract(json, path)` | ⚠️ Path syntax differs (Trino restricted vs DuckDB JSONPath). Push only when path is `$.a.b` simple-property form. |
| JSON extract (returns varchar) | `json_extract_scalar(json, path)` | `json_extract_string(json, path)` | ⚠️ Different name; same intent. |
| JSON format / serialize | `json_format(json) -> varchar` | `cast(json AS VARCHAR)`, `to_json(value)` | ❌ Different API. |
| JSON parse | `json_parse(string) -> json` | `cast(s AS JSON)`, `from_json(s, type)` | ❌ Different API. |
| JSON size | `json_size(json, json_path) -> bigint` | — | ❌ Trino-only. |
| JSON keys | — | `json_keys(json[, path])` | ❌ DuckDB-only. |
| JSON type | — | `json_type(json[, path])` | ❌ DuckDB-only. |

### Array / list functions

⚠️ Both engines use **1-based indexing**. Trino uses `array(T)`; DuckDB has `LIST(T)` (variable length) and `ARRAY(T, n)` (fixed length). Function families overlap heavily but names diverge.

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Length / cardinality | `cardinality(array) -> bigint` | `length(list)`, `len(list)`, `array_length(list)`, `cardinality(map)` | ⚠️ Both support `cardinality` only for arrays in Trino; DuckDB `length` is the canonical form. NULL → NULL in both. |
| Element at index (1-based) | `element_at(array(E), index) -> E`, `array[index]` | `list_extract(list, index)`, `array_extract`, `list[index]`, `list_element` | ⚠️ Trino `[i]` raises on out-of-bounds; `element_at` returns NULL. DuckDB `list[i]` returns NULL on out-of-bounds. **Negative index:** both count from end. |
| Contains element | `contains(array, element) -> boolean` | `list_contains(list, element)`, `array_contains`, `list_has`, `array_has` | ✅ Aligned NULL handling: contains-with-NULL returns NULL in both. |
| Position of element | `array_position(array, element) -> bigint` | `list_position(list, element)`, `array_position`, `list_indexof` | ✅ Both 1-based, 0 if not found. |
| Remove element (all occurrences) | `array_remove(array, element) -> array` | — | ❌ Trino-only direct form (use `list_filter` in DuckDB). |
| Distinct elements | `array_distinct(array) -> array` | `list_distinct(list)`, `array_unique` (returns count!) | ❌ Pitfall: DuckDB `array_unique` returns COUNT, not the distinct list. Use `list_distinct`. |
| Intersect | `array_intersect(x, y) -> array` | `list_intersect(list1, list2)` | ✅ Aligned. |
| Union | `array_union(x, y) -> array` | — (use `list_distinct(list_concat(...))`) | ❌ Trino-only direct form. |
| Except / difference | `array_except(x, y) -> array` | — | ❌ Trino-only direct form. |
| Concat | `concat(array1, ..., arrayN) -> array` | `list_concat(list_1, ..., list_n)`, `list_cat`, `array_concat`, `\|\|` | ✅ Aligned. |
| Slice | `slice(array, start, length) -> array` | `list_slice(list, begin, end)`, `array_slice` | ❌ **DIFFERENT SHAPE.** Trino: `(start, length)`. DuckDB: `(begin, end)` (Python-like). Trino `slice(a,2,3)` = 3 elements starting at 2 ≠ DuckDB `list_slice(a,2,3)` = elements 2..3. Do not translate by name. |
| Reverse | `reverse(array) -> array` | `list_reverse(list)`, `array_reverse` | ✅ Aligned. |
| Sort ascending | `array_sort(array) -> array` | `list_sort(list)`, `array_sort` | ⚠️ NULL ordering: Trino NULLS LAST; DuckDB default NULLS LAST. Verify if your data has NULLs. |
| Sort with comparator | `array_sort(array, function(T,T,int))` | `list_sort(list, col1, col2)` | ❌ Different shape. |
| Min / max | `array_min(array)`, `array_max(array)` | `list_min(list)`, `list_max(list)` | ⚠️ NULL handling differs — Trino propagates NULL if any element is NULL; DuckDB skips NULLs. ❌ Do not push without explicit NULL-free filter. |
| Sum / avg / product | — | `list_sum`, `list_avg`, `list_product` | ❌ DuckDB-only. Trino uses `reduce` or `array_agg`. |
| All match (predicate) | `all_match(array, fn) -> boolean` | — (use lambda) | ❌ Trino lambda form; DuckDB uses `list_filter` then check length. |
| Any match | `any_match(array, fn) -> boolean` | — | ❌ Trino-only. |
| Filter | `filter(array, fn) -> array` | `list_filter(list, lambda)`, `array_filter` | ❌ Lambda translation = expression-tree, not function-by-function. |
| Map / transform | `transform(array, fn) -> array` | `list_transform(list, lambda)`, `array_transform`, `list_apply` | ❌ Lambda. |
| Reduce | `reduce(array, init, inputFn, outputFn) -> R` | `list_reduce(list, lambda[, init])` | ❌ Different shape & lambda. |
| Zip | `zip(arrays...)`, `zip_with(a1, a2, fn)` | `list_zip(list_1, ..., list_n[, truncate])` | ❌ Trino lambda variant; DuckDB returns list of structs. |
| Sequence | `sequence(start, stop[, step]) -> array` | `range(start[, stop][, step])`, `generate_series(...)` | ⚠️ Trino `sequence` is INCLUSIVE on stop; DuckDB `range` is EXCLUSIVE, `generate_series` is INCLUSIVE. Choose by inclusivity. |
| Flatten | `flatten(array(array(T))) -> array(T)` | `flatten(nested_list)` | ✅ Aligned (one-level flatten). |
| N-grams | `ngrams(array, n)` | — | ❌ Trino-only. |
| Combinations | `combinations(array, n)` | — | ❌ Trino-only. |
| Repeat | `repeat(element, count) -> array` | `repeat(list, count)`, `array_resize` | ⚠️ Different shape: Trino repeats a single element; DuckDB `repeat(list,n)` repeats the list. ❌ Do not push by name. |
| Shuffle | `shuffle(array) -> array` | — | ❌ Trino-only. Non-deterministic. |
| Join to string | `array_join(array, delimiter[, null_repl]) -> varchar` | `array_to_string(list, delimiter)`, `list_string_agg` | ⚠️ NULL replacement: Trino's third arg substitutes NULLs; DuckDB silently drops NULLs (or use COALESCE). |
| Histogram of array | `array_histogram(array) -> map` | `list_histogram(list)`, `histogram(list)` | ⚠️ Same idea; verify map key/value types. |
| First / last | `array_first(array)`, `array_last(array)` | `list_first(list)`, `list_last(list)` | ✅ Aligned (1st / last element). |
| Vector distances | `euclidean_distance`, `cosine_distance/similarity`, `dot_product` | `list_distance`, `list_cosine_distance/similarity`, `list_dot_product`, `list_inner_product`, `array_*` siblings | ⚠️ Names differ; semantics aligned for double arrays of equal length. ✅ Pushable as renamed call. |

### Map functions

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Cardinality | `cardinality(map) -> bigint` | `cardinality(map)` | ✅ Aligned. |
| Get value at key | `element_at(map(K,V), key) -> V` | `element_at(map, key)`, `map[key]`, `map_extract(map, key)` | ⚠️ Missing-key behavior: Trino returns NULL; DuckDB returns empty list (because `element_at` returns LIST). ❌ Do not push without unwrapping. |
| Map keys | `map_keys(map) -> array` | `map_keys(map) -> list` | ✅ Aligned. |
| Map values | `map_values(map) -> array` | `map_values(map) -> list` | ✅ Aligned. |
| Map entries | `map_entries(map) -> array(row)` | `map_entries(map)` | ⚠️ Element type differs: Trino `row(K,V)`, DuckDB `struct(key,value)`. Effectively same shape; row→struct mapping needed at the connector type layer. |
| Concat / merge maps | `map_concat(map1, ..., mapN) -> map` | `map_concat(maps...)` | ✅ Aligned. Last-wins on key conflicts in both. |
| Build map | `map(array_keys, array_values)`, `map()` empty | `map(['a','b'], [1,2])`, `map()` empty | ✅ Aligned literal form. |
| From entries | `map_from_entries(array(row(K,V))) -> map` | `map_from_entries(STRUCT(k,v)[])` | ✅ Aligned. |
| Filter / transform keys / transform values | `map_filter`, `transform_keys`, `transform_values`, `map_zip_with` | — | ❌ Trino lambda forms. DuckDB needs manual unrolling. |
| Contains key | — | `map_contains(map, key)` | ❌ DuckDB-only direct form. |

### Struct / row functions

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Field by name | `row.field` (SQL grammar) | `struct.entry`, `struct[entry]`, `struct_extract(struct, 'entry')` | ✅ Both expose dotted access at the SQL grammar level. Pushdown works as field reference; no function call to translate. |
| Build row / struct | `ROW(a, b, c)` (positional), `CAST(ROW(...) AS row(x int, y int))` | `row(any, ...)`, `struct_pack(name := value, ...)` | ❌ DuckDB uses named pack; Trino uses positional. Do not push as expression. |
| Insert / update field | — | `struct_insert(struct, name := any, ...)`, `struct_update(...)` | ❌ DuckDB-only. |
| Concat structs | — | `struct_concat(structs...)` | ❌ DuckDB-only. |
| Position of field | — | `struct_position(struct, entry)` | ❌ DuckDB-only. |

### Conversion / cast / try-cast

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Cast | `cast(value AS type)`, `cast(value) -> type` (functional) | `CAST(value AS type)`, `value::type` | **yes r6 (primitive types)**: ✅ Translator handles `$cast` standard function; emits `CAST(expr AS <ducktype>)` for BOOLEAN/TINYINT/SMALLINT/INTEGER/BIGINT/DOUBLE/VARCHAR/DATE. TIMESTAMP precision + DECIMAL scale + nested types fail cleanly (unpushed). |
| Try cast | `try_cast(value AS type)` | `TRY_CAST(value AS type)` | **yes r6 (primitive types)**: ✅ Same as `cast` — translator handles `$try_cast` standard function; emits `TRY_CAST(...)` for the same primitive set. |
| Format number to string | `format_number(number) -> varchar` | — | ❌ Trino-only. |
| Parse data size | `parse_data_size(string)` | — | ❌ Trino-only. |
| Typeof | `typeof(expr) -> varchar` | `typeof(expression)` | ⚠️ Returns engine-specific type names. Do not push when comparing strings. |
| `printf`-format | `format(format, args...) -> varchar` | `format(format, ...)`, `printf(format, ...)` | ❌ Different format specifications. |

### Comparison and conditional

| Operation | Trino | DuckDB | Done | Notes |
|---|---|---|---|---|
| `=`, `<>`, `<`, `<=`, `>`, `>=` | SQL grammar | SQL grammar | yes (translator) | ✅ Aligned for non-NULL operands. NULL comparison: both produce NULL (3VL). Translator emits operators directly. |
| IS NULL / IS NOT NULL | SQL grammar | SQL grammar | yes (translator) | ✅ Aligned. |
| IS [NOT] DISTINCT FROM | `a IS [NOT] DISTINCT FROM b` | `a IS [NOT] DISTINCT FROM b` | yes r6 (translator) | ✅ Trino encodes as `$identical` standard function. Translator emits `IS NOT DISTINCT FROM` operator form directly. NULL-safe equality. |
| BETWEEN | `x BETWEEN a AND b` | `x BETWEEN a AND b` | — | ✅ Aligned (inclusive). |
| IN | `x IN (a, b, c)` | `x IN (a, b, c)` | yes (via TupleDomain) | ✅ Aligned. Trino delivers IN-lists of constants through `TupleDomain`, which our existing `DuckDbWhereClauseTranslator` handles. NULL-in-list handling identical (NULL in list yields NULL, not false). |
| `greatest` / `least` | `greatest(v1, ..., vN) -> [same]`, `least(...)` | `greatest(x1, x2, ...)`, `least(...)` | — never | ⚠️ NULL handling differs! **Trino: NULL anywhere → NULL.** **DuckDB: skips NULLs.** ❌ Do not push when args may be NULL. |
| CASE / IF / COALESCE / NULLIF | SQL grammar; `if(cond, t)`, `if(cond, t, f)`, `coalesce(...)`, `nullif(a, b)`, `try(expr)` | SQL grammar; `if(a, b, c)`, `ifnull(expr, other)`, `coalesce(...)`, `nullif(a, b)` | yes r6 (coalesce, nullif) | ✅ `coalesce` (variadic) and `nullif/2` translated directly to DuckDB grammar. `CASE` and `IF` still TBD (special grammar handling). Trino's `try(expr)` is Trino-only (DuckDB has only `TRY_CAST`). |

### Logical / boolean

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| AND / OR / NOT | SQL grammar | SQL grammar | ✅ Aligned 3VL. |

### Bitwise

| Operation | Trino | DuckDB | Done | Notes |
|---|---|---|---|---|
| AND | `bitwise_and(x, y) -> bigint` | `x & y`, `bit_and(x)` (aggregate) | yes r6 | ✅ Shipped via macro body `x & y` — DuckDB macros embed the operator. |
| OR | `bitwise_or(x, y) -> bigint` | `x \| y` | yes r6 | ✅ Shipped via macro body `x \| y`. |
| XOR | `bitwise_xor(x, y) -> bigint` | `xor(x, y)`, `x # y` | yes r6 | ✅ Shipped via macro body `xor(x, y)`. (DuckDB also has `#` operator; `^` is exponentiation in DuckDB — don't confuse.) |
| NOT | `bitwise_not(x) -> bigint` | `~x` | yes r6 | ✅ Shipped via macro body `~x` (unary). |
| Left shift | `bitwise_left_shift(value, shift)` | `value << shift` | yes r6 | ✅ Shipped via macro body `v << s`. |
| Right shift (logical) | `bitwise_right_shift(value, shift)` | `value >> shift` | yes r6 ⚠️ | ⚠️ Shipped, but verify signed/unsigned semantics for negative integers — they CAN differ between engines. Safe for typical positive-integer use. |
| Right shift (arithmetic) | `bitwise_right_shift_arithmetic(value, shift)` | — | | ❌ Trino-only direct; DuckDB `>>` is arithmetic for signed. |
| Bit count (popcount) | `bit_count(x, bits) -> bigint` | `bit_count(x)`, `bit_count(bitstring)` | — | ⚠️ Trino requires explicit bit-width arg; DuckDB infers. Do not push without matching width. |

### Hash / digest

> Two community extensions close most of this gap: **crypto** (cryptographic hashes + HMAC) and **hashfuncs** (non-crypto: xxHash, MurmurHash3, RapidHash). See [RESEARCH-function-community-extensions.md § Crypto](archive/RESEARCH-function-community-extensions-detail.md#crypto) and [§ Hashfuncs](archive/RESEARCH-function-community-extensions-detail.md#hashfuncs).

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| MD5 | `md5(binary) -> varbinary` | `md5(string) -> VARCHAR` (hex) wrapped in `unhex(...)` | yes r6 | ✅ Shipped via macro body `unhex(md5(b))` to convert DuckDB's hex-VARCHAR to BLOB matching Trino's VARBINARY. NULL propagation verified ([archive/REPORT-hash-null-handling.md](archive/REPORT-hash-null-handling.md)). |
| SHA-1 | `sha1(binary) -> varbinary` | `sha1(value) -> VARCHAR` wrapped in `unhex(...)` | yes r6 | ✅ Same pattern. |
| SHA-256 | `sha256(binary) -> varbinary` | `sha256(value) -> VARCHAR` wrapped in `unhex(...)` | yes r6 | ✅ Same pattern. |
| SHA-512 | `sha512(binary) -> varbinary` | `crypto_hash('sha2-512', x) -> VARCHAR` (crypto) | ⚠️ Available with `crypto`; output is hex VARCHAR, so cast to VARBINARY (or compare against hex form) before equating to Trino's. |
| CRC32 | `crc32(binary) -> bigint` | — | ❌ Trino-only — no extension cover. |
| xxhash64 | `xxhash64(binary) -> varbinary` | `xxh64(x)` (hashfuncs); core `hash(value)` is not xxhash | ✅ Push as `xxh64` when `hashfuncs` is loaded. Core `hash` is non-crypto generic and not interchangeable. |
| spooky_hash_v2 | `spooky_hash_v2_32(binary)`, `spooky_hash_v2_64(binary)` | — | ❌ Trino-only — no extension cover. |
| murmur3 | `murmur3(binary) -> varbinary` (128-bit) | `murmurhash3_x64_128(x)` / `murmurhash3_128(x)` (hashfuncs); `murmurhash3_32(x)` for 32-bit | ✅ Push as `murmurhash3_x64_128` when `hashfuncs` is loaded. |
| HMAC | `hmac_md5`, `hmac_sha1`, `hmac_sha256`, `hmac_sha512` | `crypto_hmac('md5'\|'sha1'\|'sha2-256'\|'sha2-512', key, msg)` (crypto) | ✅ All four pushable when `crypto` is loaded; output is hex VARCHAR. |
| Non-crypto generic hash | — | `hash(value, ...)` | ❌ DuckDB-only; not pushdown-safe. |

### UUID

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Random UUID | `uuid() -> uuid` | `uuid()`, `uuidv4()`, `gen_random_uuid()` | ⚠️ Non-deterministic — do not push. |
| UUIDv7 | — | `uuidv7()` | ❌ DuckDB-only. |
| Extract timestamp from UUIDv7 | — | `uuid_extract_timestamp(uuidv7)` | ❌ DuckDB-only. |
| UUID version | — | `uuid_extract_version(uuid)` | ❌ DuckDB-only. |

### URL

> The community **NetQuack** extension supplies all the `url_extract_*` operations. See [RESEARCH-function-community-extensions.md § NetQuack](archive/RESEARCH-function-community-extensions-detail.md#netquack).

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Extract protocol | `url_extract_protocol(url)` | `extract_schema(url)` (netquack) | ✅ Pushable when `netquack` is loaded. |
| Extract host | `url_extract_host(url)` | `extract_host(url)` (netquack) | ✅ Pushable. |
| Extract port | `url_extract_port(url)` | `extract_port(url)` (netquack) | ✅ Pushable. |
| Extract path | `url_extract_path(url)` | `extract_path(url)` (netquack) | ✅ Pushable. |
| Extract query | `url_extract_query(url)` | `extract_query_string(url)` (netquack) | ✅ Pushable. |
| Extract fragment | `url_extract_fragment(url)` | `extract_fragment(url)` (netquack) | ✅ Pushable. |
| Extract parameter | `url_extract_parameter(url, name)` | join through `extract_query_parameters(url)` table function (netquack) | ⚠️ Different shape — Trino scalar vs DuckDB table function. Wrap in a correlated subquery to push. |
| URL encode / decode | `url_encode(value)`, `url_decode(value)` | `url_encode(string)`, `url_decode(string)` | ✅ Aligned (also listed in String table). |

### IP address

> DuckDB's **core `inet` extension** provides a unified `INET` type (IPv4 + IPv6 with optional CIDR), subnet operators, and host/netmask/network/broadcast helpers. See [RESEARCH-function-community-extensions.md § Inet](archive/RESEARCH-function-community-extensions-detail.md#inet-core-extension--not-community). The community **NetQuack** extension supplies textual IP validators / classifiers (`is_valid_ip`, `is_private_ip`, `ip_version`, `ipcalc`).

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| IPADDRESS / IPPREFIX type | `IPADDRESS`, `IPPREFIX` | `INET` (core `inet` extension) | ⚠️ DuckDB has **one** type that covers both IPv4 and IPv6 and embeds CIDR; Trino splits address vs prefix into two types. Pushable when the catalog loads `inet`. |
| Subnet contains address | `contains(network, address) -> boolean` | `network >>= address` (core `inet`) | ✅ Pushable as the `>>=` operator once `inet` is loaded. DuckDB also exposes the inverse `<<=` (contained-by). |
| Host / network / broadcast / netmask | — | `host(INET)`, `network(INET)`, `broadcast(INET)`, `netmask(INET)` | ❌ DuckDB-only direct names; Trino does not expose these. |
| IP version (4 vs 6) | — | `ip_version(varchar)` (netquack) | ❌ Neither has a built-in family discriminator on the typed value; route via VARCHAR + netquack if needed. |

### Binary / blob

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Concat binary | `concat(b1, ..., bN) -> varbinary` | `arg1 \|\| arg2` for BLOB | ✅ Aligned. |
| Length | `length(binary) -> bigint` | `octet_length(blob)` | ⚠️ Different name; same semantics. |
| Substring | `substr(binary, start[, length]) -> varbinary` | — direct | ❌ Use `cast` to bitstring or work at hex level. |
| Reverse | `reverse(binary) -> varbinary` | — | ❌ Trino-only for binary. |
| Hex encode/decode | `to_hex`, `from_hex` | `hex(blob)`, `unhex(value)` | ✅ Aligned (see Hash/encoding rows). |
| Big-endian int32/int64 ↔ bytes | `from_big_endian_32/64`, `to_big_endian_32/64` | — | ❌ Trino-only. |
| IEEE 754 ↔ bytes | `from_ieee754_32/64`, `to_ieee754_32/64` | — | ❌ Trino-only. |
| Read file as blob/text | — | `read_blob(source)`, `read_text(source)` | ❌ DuckDB-only; not pushdown territory (filesystem). |

### Type-introspection / system

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| Current user | `current_user` | — | ❌ Trino-only. |
| Current groups | `current_groups()` | — | ❌ Trino-only. |
| Current catalog | `current_catalog` | `current_catalog()`, `current_database()` | ⚠️ Aligned in concept; literal vs function. |
| Current schema | `current_schema` | `current_schema()`, `current_schemas(boolean)` | ⚠️ Aligned in concept. |
| Engine version | `version() -> varchar` | `version()` | ⚠️ Aligned; do NOT push (returns engine-specific string). |
| Typeof | `typeof(expr) -> varchar` | `typeof(expression)`, `pg_typeof(expression)` | ⚠️ Returns engine-specific type names. Do not push. |
| Read settings | — | `current_setting('name')`, `getenv(var)` | ❌ DuckDB-only. |
| Sequences | — | `nextval('seq')`, `currval('seq')` | ❌ DuckDB-only. |

---

## Aggregate functions

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| count(*) | `count(*) -> bigint` | `count()` (no args) | ✅ Aligned (DuckDB also accepts `count(*)`). |
| count(expr) | `count(x) -> bigint` | `count(arg)` | ✅ Both skip NULLs. |
| count distinct | `count(DISTINCT x)` | `count(DISTINCT arg)` | ✅ Aligned. |
| count if predicate | `count_if(x) -> bigint` | `countif(arg)`, `count_if(x)` (alias) | ✅ Aligned. |
| sum | `sum(x) -> [same]` | `sum(arg)` | ⚠️ Both skip NULLs. Sum of all-NULL: both return NULL. Overflow: Trino throws; DuckDB throws. ✅ |
| avg | `avg(x) -> double`, `avg(real/decimal/...)` | `avg(arg)` | ✅ Skips NULLs. |
| min / max | `min(x)`, `max(x)`, `min(x, n) -> array`, `max(x, n) -> array` | `min(arg)`, `max(arg)`, `min(arg, n)`, `max(arg, n)` | ✅ Aligned. The `n` variant returns top/bottom-n. |
| min_by / max_by | `min_by(x, y)`, `max_by(x, y)`, with-n variant | `arg_min(arg, val)`, `arg_max(arg, val)`, with-n variant; plus `arg_min_null`, `arg_max_null` | ⚠️ Different names. NULL semantics: Trino skips, DuckDB has separate `_null` variants. ❌ Do not push by name. |
| first / last (any value) | `arbitrary(x)`, `any_value(x)` | `any_value(arg)`, `first(arg)`, `last(arg)` | ⚠️ Order-dependence in both is undefined unless ORDER BY clause used. |
| bool_and / every | `bool_and(boolean)`, `every(boolean)` | `bool_and(arg)` | ✅ Aligned. |
| bool_or | `bool_or(boolean)` | `bool_or(arg)` | ✅ Aligned. |
| listagg / string_agg | `listagg(x, separator) WITHIN GROUP (...)` | `string_agg(arg[, sep])`, `list_string_agg(list)` | ❌ Different shapes: Trino has `WITHIN GROUP`. DuckDB lacks that grammar. |
| array_agg | `array_agg(x) -> array` | `list(arg)`, `array_agg(arg)` | ⚠️ DuckDB returns LIST; Trino returns ARRAY. |
| map_agg | `map_agg(key, value)`, `map_union(x)` | — (use `map_from_entries`) | ❌ Trino-only direct form. |
| multimap_agg | `multimap_agg(key, value)` | — | ❌ Trino-only. |
| histogram | `histogram(x) -> map<K,bigint>` | `histogram(arg[, boundaries])` | ⚠️ Trino: counts by value. DuckDB: same. ✅ Aligned. |
| checksum | `checksum(x) -> varbinary` | — | ❌ Trino-only. |
| geometric_mean | `geometric_mean(x) -> double` | `geometric_mean(arg)` | ✅ Aligned. |
| product | — | `product(arg)` | ❌ DuckDB-only. |
| weighted_avg | — | `weighted_avg(arg, weight)` | ❌ DuckDB-only. |
| favg / fsum | — | `favg(arg)`, `fsum(arg)` | ❌ DuckDB-only (Kahan-compensated). |
| Variance / stddev | `variance(x)`, `var_pop(x)`, `var_samp(x)`, `stddev(x)`, `stddev_pop(x)`, `stddev_samp(x)` | `var_pop(x)`, `var_samp(x)`, `stddev_pop(x)`, `stddev_samp(x)` | ✅ Aligned. Trino `variance`/`stddev` aliases for samp. |
| Correlation / covariance | `corr(y, x)`, `covar_pop(y, x)`, `covar_samp(y, x)` | `corr(y, x)`, `covar_pop(y, x)`, `covar_samp(y, x)` | ✅ Aligned. |
| Skewness / kurtosis | `skewness(x)`, `kurtosis(x)` | `skewness(x)`, `kurtosis(x)`, `kurtosis_pop(x)`, `list_kurtosis_pop(list)` | ⚠️ Trino `kurtosis` = sample excess; DuckDB has both sample and pop. Push only with explicit match. |
| Regression | `regr_intercept(y, x)`, `regr_slope(y, x)` | `regr_intercept`, `regr_slope`, `regr_count`, `regr_avgx`, `regr_avgy`, `regr_r2`, `regr_sxx`, `regr_sxy`, `regr_syy` | ⚠️ Trino has slope/intercept only; DuckDB full set. |
| Entropy | — | `entropy(x)` | ❌ DuckDB-only. |
| Median / mode / MAD | — | `median(x)`, `mode(x)`, `mad(x)` | ❌ DuckDB-only direct. Trino uses `approx_percentile` for median. |
| Quantile (continuous / discrete) | `approx_percentile(...)` | `quantile_cont(x, pos)`, `quantile_disc(x, pos)` | ❌ Different shape (DuckDB exact, Trino approximate by default). |
| Approx distinct (HLL) | `approx_distinct(x[, e]) -> bigint`, `approx_set`, `merge(HyperLogLog)` | `approx_count_distinct(x)`, `list_approx_count_distinct(list)`; also `datasketch_hll*` and `datasketch_cpc*` for a richer sketch surface (datasketches) | ⚠️ Names differ; HLL state types are not interchangeable across engines. Push only as cardinality call, not state. The `datasketches` extension adds confidence bounds and CPC if needed. |
| Approx most frequent | `approx_most_frequent(buckets, value, capacity)` | `datasketch_frequent_items(lg_max_k, column)` + `datasketch_frequent_items_get_frequent(sketch, error_type)` (datasketches) | ✅ Pushable as the canonical Frequent-Items sketch when `datasketches` is loaded. See [community-extensions § DataSketches](archive/RESEARCH-function-community-extensions-detail.md#datasketches). |
| Bitwise aggregates | `bitwise_and_agg(x)`, `bitwise_or_agg(x)`, `bitwise_xor_agg(x)` | `bit_and(arg)`, `bit_or(arg)`, `bit_xor(arg)`, `bitstring_agg(arg[, min, max])` | ⚠️ Names differ; semantics aligned. |
| Reduce (aggregate lambda) | `reduce_agg(input, init, inputFn, combineFn)` | — | ❌ Trino-only. |
| Sketch (theta) | `theta_sketch_union`, `theta_sketch_cardinality` | `datasketch_theta*` family (datasketches): `_union`, `_intersect`, `_a_not_b`, `_estimate` | ✅ Pushable when `datasketches` is loaded. ⚠️ Serialized sketch states are **not** wire-compatible with Trino's — only computed-cardinality / set-op paths are safe to push, not sketch values crossing engine boundaries. |
| t-digest | `tdigest_agg`, `value_at_quantile`, `quantile_at_value` | `datasketch_tdigest` + `datasketch_tdigest_quantile` / `_rank` / `_cdf` / `_pmf` (datasketches) | ✅ Pushable when `datasketches` is loaded. Same state-incompatibility caveat as theta. |
| q-digest | `qdigest_agg` and read-side scalars | — (no qdigest in datasketches; use KLL, classic Quantiles, or REQ as substitutes) | ⚠️ No exact qdigest port. `datasketch_kll*` is the modern Apache-recommended replacement; `datasketch_quantiles*` / `datasketch_req*` are alternatives. ❌ Cannot push as Trino qdigest semantics. |

> Many statistical aggregates are niche on both sides; see source docs for the long tail (60+ entries in Trino aggregate.html, 40+ in DuckDB aggregates.md).

---

## Window functions

⚠️ Window pushdown is out of scope for the first phase (see [TODO-pushdown-duckdb.md](TODO-pushdown-duckdb.md): "Aggregate / window pushdown … tracked separately"). Listed here for completeness.

| Operation | Trino | DuckDB | Notes |
|---|---|---|---|
| row_number | `row_number() -> bigint` | `row_number([ORDER BY ...])` | ✅ Aligned. |
| rank | `rank() -> bigint` | `rank([ORDER BY ...])` | ✅ Aligned. |
| dense_rank | `dense_rank() -> bigint` | `dense_rank()` | ✅ Aligned. |
| percent_rank | `percent_rank() -> double` | `percent_rank([ORDER BY ...])` | ✅ Aligned. |
| cume_dist | `cume_dist() -> bigint` | `cume_dist([ORDER BY ...])` | ⚠️ Trino docs say `-> bigint`; DuckDB returns double in [0,1]. **Trino actually returns double too** — doc bug in Trino 481 `cume_dist` page; trust the type system. |
| ntile | `ntile(n) -> bigint` | `ntile(n[ ORDER BY ...])` | ✅ Aligned. |
| first_value / last_value | `first_value(x)`, `last_value(x)` | `first_value(expr[ ORDER BY ...][ IGNORE NULLS])`, `last_value(...)` | ✅ Aligned. `IGNORE NULLS` available in DuckDB grammar. |
| nth_value | `nth_value(x, offset)` | `nth_value(expr, nth[ ORDER BY ...][ IGNORE NULLS])` | ✅ Aligned. |
| lead / lag | `lead(x[, offset[, default]])`, `lag(x[, offset[, default]])` | `lead(expr[, offset[, default]][ ORDER BY ...][ IGNORE NULLS])`, `lag(...)` | ✅ Aligned. |
| fill | — | `fill(expr[ ORDER BY ...])` | ❌ DuckDB-only (forward/backward fill nulls). |

Any aggregate function (`sum`, `avg`, `count`, etc.) can also be used as a window function in both engines via the `OVER` clause — semantics aligned subject to the per-function NULL caveats above.

---

## Notes on intentional omissions

- **Geospatial** — Trino has a large ST_* set (`ST_Point`, `ST_Contains`, `ST_Buffer`, …). DuckDB has these via the `spatial` extension. Deferred — not in the pushdown path for our first phase.
- **HyperLogLog / qdigest / tdigest / setdigest / Theta sketch** — Trino has its own state serialization. The DuckDB **`datasketches`** community extension supplies the Apache DataSketches port for HLL, CPC, KLL, classic Quantiles, REQ, T-Digest, Theta, and Frequent Items — see [RESEARCH-function-community-extensions.md § DataSketches](archive/RESEARCH-function-community-extensions-detail.md#datasketches). Cardinality / quantile / set-op results can be pushed when the extension is loaded; **sketch state is still not interchangeable across engines**, so cross-engine sketch transport requires re-aggregation.
- **AI / ML functions** — `ai_analyze_sentiment`, `ai_classify`, `ai_extract`, `ai_fix_grammar`, `ai_gen`, `ai_mask`, `ai_translate`, `learn_classifier`, `classify`, `learn_regressor`, `regress`, `features`, `learn_libsvm_*` are Trino-only. Do not push.
- **Color** — `color()`, `bar()`, `render()`, `rgb()` are Trino terminal-rendering functions. Do not push.
- **Datasketches** — `theta_sketch_*` — see HyperLogLog above.
- **Lambda forms** (`x -> x + 1`) — handled as a special case at the expression-tree level, not function-by-function. Lambda-taking functions (`transform`, `filter`, `reduce`, `all_match`, `any_match`, `none_match`, `map_filter`, `transform_keys`, `transform_values`, `zip_with`, `regexp_replace` with lambda) require the translator to recognize the lambda node and either reject pushdown or generate a DuckDB lambda body. Conservative default: reject.
- **Variant type** — Trino `variant_is_null` and the variant family are Trino-only.
- **Table functions** — `exclude_columns`, `sequence` (table form), `regexp_split_to_table` (DuckDB). Table-function pushdown is a separate problem from predicate pushdown.
- **Teradata-compat aliases** (`char2hexint`, `index`, `to_char`, `to_timestamp`, `to_date`) — Trino-only; consider mapping case-by-case as aliases.
- **DuckDB extension-only functions** — geospatial (`spatial`), full-text (`fts`), Postgres scanner, MySQL scanner, Iceberg, Delta — out of scope.
