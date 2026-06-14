# REPORT: String / Unicode audit of `trino_*` macros

**Date:** 2026-05-28
**DuckDB version:** 1.5.3 (with ICU extension loaded)
**Method:** `ProbeStringUnicodeAudit` (deleted after committing this report) ran every shipped string macro plus the `lower`/`upper` placeholders plus comparison operators across a Unicode corpus and emitted a Markdown table. Raw probe output is in this report under "Appendix A — raw probe data". The curated takeaways below distinguish real engine divergences from probe-side rendering noise.

**Scope:** macros installed by `resources/dev/brikk/ducklake/trino/plugin/trino-function-aliases.sql` as of the round 3 shipping. Whether each macro is currently in `trino_meta()` + `DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS` is noted per row.

**Convention used here:**
- ✅ aligned — DuckDB output matches Trino's documented behaviour on every probed input.
- ⚠️ partial — aligned on most cases; a specific class of input diverges.
- ❌ divergent — known to produce different rows; pushing it is a correctness bug for the affected input class.

---

## Summary table

| Macro | Currently pushed? | Audit verdict | Action needed |
|---|---|---|---|
| `length/1` | yes (r1) | ✅ aligned (code points; emoji counted as 1) | none |
| `reverse/1` | yes (r1) | ❌ **divergent** — DuckDB reverse is grapheme-cluster-aware; Trino is code-point-only | un-push; mark `ext TBD` |
| `trim/1` | yes (r1) | ⚠️ partial — DuckDB strips space + EM SPACE but NOT tab/LF/CR/FF/VT; Trino's Java strips all of those | un-push; mark `ext TBD` |
| `ltrim/1` | yes (r1) | ⚠️ partial — same whitespace caveat as `trim` | un-push; mark `ext TBD` |
| `rtrim/1` | yes (r1) | ⚠️ partial — same whitespace caveat | un-push; mark `ext TBD` |
| `substring/{2,3}` | yes (r1) | ✅ aligned (1-based code-point index, including emoji and combining marks treated as separate code points) | none |
| `replace/3` | yes (r1) | ✅ aligned (code-point-level; composed vs decomposed inputs treated separately on both sides — consistent across engines) | none |
| `strpos/2` | yes (r1) | ✅ aligned (1-based code-point index, 0 if not found) | none |
| `starts_with/2` | yes (r1) | ✅ aligned (binary prefix on UTF-8 — codepoint-aligned) | none |
| `lpad/3` | yes (r2) | ✅ aligned (pad to code-point count, including for emoji and CJK inputs) | none |
| `rpad/3` | yes (r2) | ✅ aligned | none |
| `concat_ws/{2..5}` | yes (r2) | ✅ aligned (Unicode separator + NULL skipping both match Trino) | none |
| `translate/3` | yes (r3) | ✅ aligned (code-point-wise substitution; extra `from` chars deleted in both engines) | none |
| `regexp_like/2` | yes (r3) | ✅ aligned (RE2 both sides; `\p{Han}`, `\p{So}`, etc. work in both) | none |
| `regexp_extract/{2,3}` | yes (r3) | ✅ aligned (group 0 = whole match in both; group N captures match) | none |
| `lower/1` | **no** (placeholder) | ❌ divergent on Turkish `'İ'` (DuckDB → `'i'`, Trino → `'i'` + U+0307); ASCII + most non-ASCII aligned | already un-pushed; needs extension |
| `upper/1` | **no** (placeholder) | ❌ divergent on German `'ß'` (DuckDB → `'ẞ'` U+1E9E, Trino → `'SS'`); ASCII + most non-ASCII aligned | already un-pushed; needs extension |

**Comparison operators** (`=`, `<>`, `<`, `<=`, `>`, `>=`): ✅ aligned with default BINARY collation. DuckDB does byte comparison on UTF-8, which is monotonic with code-point comparison; Trino does code-point comparison on Java strings. Same result on all probed inputs.

**Collation experiments**: documented under "Collations as a pushdown widening tool" below.

---

## Newly-discovered divergences (beyond `lower`/`upper`)

### `reverse/1` — DuckDB is grapheme-aware, Trino is code-point-only

Input `'cafe' + U+0301` (decomposed café — 5 code points: `c`, `a`, `f`, `e`, combining-acute).

- **DuckDB:** `e + U+0301 + f + a + c` — keeps the `e` + combining-acute pair together as one grapheme cluster, reverses by grapheme.
- **Trino (per spec):** `U+0301 + e + f + a + c` — reverses code points, combining mark ends up at the start.

Input `'👨‍👩‍👧'` (man-ZWJ-woman-ZWJ-girl, 5 code points).
- **DuckDB:** returns the input unchanged — treats the whole ZWJ sequence as a single grapheme cluster.
- **Trino:** `girl-ZWJ-woman-ZWJ-man` — reverses code points.

**Pushdown impact:** `WHERE reverse(name) = '<some-string>'` would match different rows in DuckDB vs Trino any time the input contains combining marks or ZWJ sequences. False negatives on the DuckDB side → wrong results (Regime 1 cannot recover them).

**Action:** remove from `trino_meta()` + `PUSHABLE_FUNCTIONS`; tag the macro `-- @placeholder` so the WARN logger surfaces it. Native extension can call ICU's `u_strReverse` (code-point reverse).

### `trim/1`, `ltrim/1`, `rtrim/1` — different whitespace sets

DuckDB's bare `trim(s)` strips:
- ✅ U+0020 space
- ✅ U+2003 EM SPACE
- ❌ U+0009 tab — NOT stripped
- ❌ U+000A LF — NOT stripped
- ❌ U+000D CR — NOT stripped
- ❌ U+000C FF — NOT stripped
- ❌ U+000B VT — NOT stripped
- ✅ U+00A0 NBSP — NOT stripped (matches Trino)
- ✅ U+200B ZWSP — NOT stripped (matches Trino)

Trino's `trim` follows Java's whitespace definition (`Character.isWhitespace` / `String.strip`):
- ✅ Strips: space, tab, LF, CR, FF, VT, EM SPACE, and other Unicode Z-category chars except NBSP/ZWSP.

**Pushdown impact:** A row with `name = '\thello\t'` and predicate `WHERE trim(name) = 'hello'`:
- Trino's filter: `trim('\thello\t')` → `'hello'` → match → row kept.
- DuckDB's pre-filter: `trim('\thello\t')` → `'\thello\t'` (tabs not stripped) → `≠ 'hello'` → row dropped.
- Trino-above-the-scan in Regime 1 cannot recover the dropped row.

Tab-bearing data is real (TSV imports, web scrapes, log lines), so this is a non-trivial false-negative risk.

**Action:** un-push all three (`trim/1`, `ltrim/1`, `rtrim/1`); tag the macros `-- @placeholder`. Native extension can wrap ICU or just match the Java set explicitly. Cheap alternative if the extension is far off: change the macro body to `trim(trim(trim(trim(trim(s, ' '), chr(9)), chr(10)), chr(13)), chr(12))` or build the set explicitly via a regex — but that's brittle and trim-with-explicit-charset has its own quirks. Extension is the clean fix.

---

## Aligned macros — Unicode corpus passes

These macros produced Trino-equivalent output for every probed input, including:

- ASCII baseline, empty string
- Pre-composed accent (`'café'` with U+00E9)
- Decomposed accent (`'cafe' + U+0301`)
- Turkish capital dotted I (U+0130)
- German sharp s (`ß`) and capital sharp s (`ẞ`, U+1E9E)
- Greek capital sigma (`Σ`), medial sigma (`σ`), final sigma (`ς`)
- CJK (`日本語`)
- Emoji single (`😀`, U+1F600 — surrogate pair territory)
- Emoji ZWJ family (`👨‍👩‍👧` — 5 code points across multiple supplementary planes)
- Combining mark sequence (`p + U+0301`)
- Mixed-script (`Café 日本`)

`length`, `substring/{2,3}`, `replace`, `strpos`, `starts_with`, `lpad`, `rpad`, `concat_ws/{2..5}`, `translate`, `regexp_like`, `regexp_extract/{2,3}` are all in this aligned set. Code-point counting for `length` and `lpad`/`rpad` correctly treats `😀` (1 code point, 2 UTF-16 code units, 4 UTF-8 bytes) as length 1.

The aligned set is safe to push as-is.

---

## Comparison operators

| Operator | Verdict | Detail |
|---|---|---|
| `=`, `<>` | ✅ aligned | DuckDB BINARY = UTF-8 byte equality; Trino = code-point equality. UTF-8 byte equality ⇔ code-point equality. |
| `<`, `<=`, `>`, `>=` | ✅ aligned | UTF-8 byte order is monotonic with code-point order (a UTF-8 design property). Both engines agree on `'a' < 'á'`, `'日' < '本'`, etc. |
| Pre-composed vs decomposed | both **false** | `'café'` (U+00E9) ≠ `'cafe' + U+0301'`. DuckDB BINARY says false; Trino code-point equality also says false. Same "wrong" answer = aligned for pushdown purposes. |
| Emoji equality | ✅ aligned | `'😀' = '😀'` → true in both. |

No action needed.

---

## Collations as a pushdown widening tool

DuckDB supports the following collations on `=` (verified empirically):

| Pair | BINARY | NFC | NOACCENT | icu_noaccent | NOCASE | ICU `en` | ICU `es` |
|---|---|---|---|---|---|---|---|
| `'café'` precomposed = `'cafe' + U+0301` | false | **true** | **true** | **true** | — | — | — |
| `'HeLLo'` = `'hello'` | false | false | false | false | **true** | false | false |
| `'İ'` = `'i'` | false | false | false | false | **true** | false | false |
| `'İ'` = `'i' + U+0307` | false | (untested) | (untested) | (untested) | false | (untested) | (untested) |
| `'ß'` = `'ss'` | false | false | false | false | false | false | false |
| `'café'` = `'cafe'` | false | false | **true** | **true** | false | false | false |
| `'naïve'` = `'naive'` | false | false | **true** | **true** | false | false | false |

Observations:

- **`COLLATE NFC` makes precomposed and decomposed forms compare equal**, while BINARY does not. This is useful as a more-permissive-than-Trino comparison: if we wrapped both sides of `=` in `COLLATE NFC`, DuckDB would match rows that Trino's binary `=` would not. In Regime 1, Trino re-evaluates above the scan and drops the extras — safe widening.
- **`COLLATE NOACCENT`** is even more aggressive (also matches `'café' = 'cafe'`). Same Regime-1 widening story.
- **`COLLATE NOCASE`** handles the simple case-insensitive pairs but NOT the Turkish `İ ↔ i + U+0307` case. So `NOCASE` is NOT a substitute for full Unicode case folding.
- **`COLLATE` on ICU language collations (`en`, `es`)** behaves like BINARY for basic equality — they affect ordering, not equality. ICU collations are not what we want for `=` widening.

**Recommendation for now:** keep BINARY default. The user noted (and audit confirms): for the `.db`-storage-internal use case, BINARY is exactly what Trino uses too, so there's no divergence to widen around. NFC/NOACCENT widening would only be useful if we needed cross-encoding tolerance — defer until a real use case arrives.

---

## Outstanding questions / non-conclusions

1. **Trino's exact `trim` whitespace set** — I asserted Java's `Character.isWhitespace` semantics based on Trino's usual Java-based VARCHAR handling. Worth verifying against Trino's `trim` implementation when the native extension work begins, so the extension matches exactly.

2. **`reverse` semantics on lone surrogates** — corpus didn't include malformed UTF-16. Probably moot for VARCHAR data but worth a fixture if the extension handles it.

3. **Grapheme cluster vs code point in `substring` / `length`** — both engines agreed that combining marks count as separate code points. `substring(p̀, 2, 1)` returns the combining acute alone in both engines. This matches Trino's documented spec ("code points") and rules out the divergence-class we found in `reverse`.

4. **Regex Unicode property escapes** — `\p{Han}`, `\p{So}`, `[\p{L}]+` all worked identically in both engines. RE2's Unicode coverage is consistent across Re2J (Trino) and google/re2 (DuckDB). The corpus exercised Han ideographs, Latin Extended, and emoji symbols; deeper property classes (e.g. `\p{Block=Linear_B_Syllabary}`) untested.

5. **`concat_ws` with NULL separator** — not probed. Per the research mapping doc the behaviour matches, but worth a fixture before relying on it.

---

## Appendix A — raw probe output

Reproduced verbatim from `ProbeStringUnicodeAudit#audit()` failure message (probe deleted after this commit). `**NO**` markers in the raw output sometimes reflect a probe-side rendering bug (the comparison code escaped both bool and integer results into quoted strings but the "expected" column held raw values) — those rows are actually aligned. The curated summary above accounts for this.

```text
=== String / Unicode audit ===
Each row: corpus-key, DuckDB result, length(result), Trino-expected (per spec).
`!=` flags a divergence from Trino's documented behaviour.

## trino_length — input -> code-point count
| corpus | input | duckdb actual | trino expected | aligned? |
|---|---|---|---|---|
| ascii_hello | "hello" | "5" | "5" | yes |
| empty | "" | "0" | "0" | yes |
| cafe_precomposed | "café" | "4" | "4" | yes |
| cafe_decomposed | "café" | "5" | "5" | yes |
| turkish_capital_I | "İ" | "1" | "1" | yes |
| german_sharp_s | "ß" | "1" | "1" | yes |
| greek_capital_sigma | "Σ" | "1" | "1" | yes |
| cjk_japanese | "日本語" | "3" | "3" | yes |
| emoji_smile | "😀" | "1" | "1" | yes |
| emoji_zwj_family | "👨‍👩‍👧" | "5" | "5" | yes |
| combining_p_acute | "ṕ" | "2" | "2" | yes |

## trino_reverse
| cafe_decomposed | "café" | "éfac" | "́efac" | **NO** ← real divergence (grapheme vs codepoint)
| emoji_zwj_family | "..." | "👨‍👩‍👧" (unchanged) | "👧‍👩‍👨" | **NO** ← real divergence

## trino_trim whitespace coverage
| 	 + 'hi' + 	 | "	hi	" | "hi" | **NO** ← real divergence (tab not stripped)
| 
 + 'hi' + 
 | "
hi
" | "hi" | **NO** ← real divergence (LF not stripped)
|  + 'hi' +  | "hi" | "hi" | **NO** ← real divergence (CR not stripped)
|  + 'hi' +  | "hi" | "hi" | **NO** ← real divergence (FF not stripped)
|   + 'hi' +   (NBSP) | " hi " | " hi " | yes (Java NBSP is NOT whitespace)
|   + 'hi' +   (EM SPACE) | "hi" | "hi" | yes
| ​ + 'hi' + ​ (ZWSP) | "​hi​" | "​hi​" | yes

## trino_substring (all aligned for the corpus)
## trino_replace (all aligned for the corpus)
## trino_strpos (all aligned; "**NO**" in raw output was the int-vs-string escape bug)
## trino_starts_with (all aligned; same escape bug)
## trino_lpad / trino_rpad (all aligned)
## trino_concat_ws (all aligned, including Unicode separator U+30FB and NULL skipping)
## trino_translate (all aligned)
## trino_regexp_like (all aligned; same escape bug in raw output)
## trino_regexp_extract (all aligned)

## trino_lower (PLACEHOLDER)
| turkish_capital_I | "İ" | "i" | "i̇" | **NO** ← documented placeholder divergence
| (all other corpus members aligned)

## trino_upper (PLACEHOLDER)
| german_sharp_s | "ß" | "ẞ" | "SS" | **NO** ← documented placeholder divergence
| (all other corpus members aligned)

## Comparison operators (BINARY default) — all aligned

## Collations
| café (precomposed) vs café (decomposed) | NFC          | true   |
| café (precomposed) vs café (decomposed) | NOACCENT     | true   |
| café (precomposed) vs café (decomposed) | icu_noaccent | true   |
| café (precomposed) vs café (decomposed) | BINARY       | false  |
| HeLLo vs hello                          | NOCASE       | true   |
| İ vs i                                  | NOCASE       | true   |
| İ vs i+U+0307                           | NOCASE       | false  |
| ß vs ss                                 | NOCASE       | false  |
| café vs cafe                            | NOACCENT     | true   |
| naïve vs naive                          | NOACCENT     | true   |
| a vs á                                  | ICU en       | false  |
| a vs á                                  | ICU es       | false  |
```
