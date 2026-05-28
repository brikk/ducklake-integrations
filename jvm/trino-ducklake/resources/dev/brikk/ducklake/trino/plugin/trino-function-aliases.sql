-- Trino -> DuckDB function aliases.
--
-- This is the interpretation layer between the Trino pushdown translator and
-- DuckDB. The translator emits trino_<name>(...) calls; each macro below maps
-- one Trino function (by name + arity) to the DuckDB construct that matches
-- Trino's semantics. Semantic fixes (NULL handling, collation, edge cases) go
-- here so they can be corrected without rebuilding the Trino plugin.
--
-- Applied to every DuckDB instance the plugin talks to: the per-split in-process
-- JDBC connection AND the Quack server's catalog (via the wrapper). All
-- statements are idempotent (CREATE OR REPLACE) so re-applying on every attach
-- is safe.
--
-- Round 1 scope: a handful of string functions whose Trino vs DuckDB semantics
-- are aligned per dev-docs/RESEARCH-function-mapping.md. See trino_meta() at the
-- bottom for the authoritative list the translator reads.

-- Extensions. INSTALL is cached on disk per DuckDB version; LOAD is required
-- per DuckDB instance. ICU is loaded so Trino's documented Unicode-aware
-- lower/upper has a chance of matching server-side; treated best-effort by the
-- caller so a sandboxed test environment without network access still works
-- (the round 1 macros do not yet reference ICU collations).
INSTALL icu;
LOAD icu;

-- ---- String functions ----

-- @placeholder trino_lower trino_upper
-- These macros are PUSHABLE (in trino_meta + PUSHABLE_FUNCTIONS) to allow
-- performance characterization of the Trino → DuckDB pushdown path, but their
-- semantics DIVERGE from Trino on specific non-ASCII inputs:
--
--   lower('İ' /* U+0130 */):   DuckDB → 'i'         |  Trino → 'i' + U+0307
--   upper('ß'):                 DuckDB → 'ẞ' U+1E9E  |  Trino → 'SS'
--
-- Collations (NOCASE / NOACCENT / NFC / icu_*) and nfc_normalize() do NOT
-- change lower()/upper() output — collations only affect comparison, and
-- DuckDB's bare lower/upper do simple Unicode case folding, not the full case
-- folding Trino's Java does. Resolution path: native DuckDB extension exposing
-- ICU's u_strFoldCase (Rust template: https://github.com/duckdb/extension-template-rs).
-- Once that ships and matches Trino across the test corpus, swap the macro
-- bodies and drop the @placeholder tag.
--
-- Wire-level signal: DuckDbExpressionTranslator logs a one-shot WARN per
-- placeholder name when it emits a pushdown call. Loud enough to spot in
-- production logs, quiet enough not to flood.
CREATE OR REPLACE MACRO trino_lower(s) AS lower(s);

CREATE OR REPLACE MACRO trino_upper(s) AS upper(s);

CREATE OR REPLACE MACRO trino_length(s) AS length(s);

-- @placeholder trino_reverse
-- DuckDB reverse is grapheme-cluster-aware; Trino reverse is code-point-only.
-- Diverges on inputs with combining marks or ZWJ sequences (e.g. emoji families).
-- Macro stays pushable for performance characterization; warn-on-emit fires when
-- the translator pushes it. Native extension is the durable fix
-- (e.g. ICU u_strReverse on raw code points). See REPORT-string-unicode-audit.md.
CREATE OR REPLACE MACRO trino_reverse(s) AS reverse(s);

-- Whitespace set used by Trino's trim — matches Java's Character.isWhitespace
-- (the semantics behind String.strip()). DuckDB's bare trim() strips only ASCII
-- space + EM SPACE by default, so we pass the explicit character set as the
-- second argument. Excludes U+00A0 NBSP, U+2007 FIGURE SPACE, U+202F NARROW
-- NBSP per the Java spec — these are intentionally NOT whitespace.
CREATE OR REPLACE MACRO trino__java_whitespace_chars() AS
    chr(9) || chr(10) || chr(11) || chr(12) || chr(13)         -- HT LF VT FF CR
    || chr(28) || chr(29) || chr(30) || chr(31) || chr(32)     -- FS GS RS US SPACE
    || chr(5760)                                                -- U+1680 OGHAM SPACE MARK
    || chr(8192) || chr(8193) || chr(8194) || chr(8195)        -- U+2000-3 quads/spaces
    || chr(8196) || chr(8197) || chr(8198)                     -- U+2004-6 m-spaces
    -- U+2007 FIGURE SPACE intentionally excluded
    || chr(8200) || chr(8201) || chr(8202)                     -- U+2008-A puncts/thin/hair
    || chr(8232) || chr(8233)                                  -- U+2028-9 line/para sep
    -- U+202F NARROW NBSP intentionally excluded
    || chr(8287)                                                -- U+205F MEDIUM MATH SPACE
    || chr(12288);                                              -- U+3000 IDEOGRAPHIC SPACE

CREATE OR REPLACE MACRO trino_trim(s)  AS trim(s,  trino__java_whitespace_chars());

CREATE OR REPLACE MACRO trino_ltrim(s) AS ltrim(s, trino__java_whitespace_chars());

CREATE OR REPLACE MACRO trino_rtrim(s) AS rtrim(s, trino__java_whitespace_chars());

CREATE OR REPLACE MACRO trino_substring
    (s, start) AS substring(s, start),
    (s, start, length) AS substring(s, start, length);

CREATE OR REPLACE MACRO trino_replace(s, search, replacement) AS replace(s, search, replacement);

CREATE OR REPLACE MACRO trino_strpos(s, sub) AS strpos(s, sub);

CREATE OR REPLACE MACRO trino_starts_with(s, prefix) AS starts_with(s, prefix);

CREATE OR REPLACE MACRO trino_lpad(s, size, padstring) AS lpad(s, size, padstring);

CREATE OR REPLACE MACRO trino_rpad(s, size, padstring) AS rpad(s, size, padstring);

-- DuckDB macros are fixed-arity; concat_ws is variadic in both Trino and DuckDB.
-- 2..5 arg overloads cover common pushdown shapes; extend the list (and
-- trino_meta below) if real workloads call for more.
CREATE OR REPLACE MACRO trino_concat_ws
    (sep, s1)                 AS concat_ws(sep, s1),
    (sep, s1, s2)             AS concat_ws(sep, s1, s2),
    (sep, s1, s2, s3)         AS concat_ws(sep, s1, s2, s3),
    (sep, s1, s2, s3, s4)     AS concat_ws(sep, s1, s2, s3, s4);

-- ---- Numeric functions ----

CREATE OR REPLACE MACRO trino_abs(x) AS abs(x);

CREATE OR REPLACE MACRO trino_ceil(x) AS ceil(x);

CREATE OR REPLACE MACRO trino_floor(x) AS floor(x);

-- Integer mod is semantically aligned (truncated division, sign follows
-- dividend). Float mod diverges (Trino IEEE-remainder, DuckDB fmod) and must be
-- gated at the translator before pushdown — the macro itself is not type-aware.
CREATE OR REPLACE MACRO trino_mod(n, m) AS mod(n, m);

CREATE OR REPLACE MACRO trino_power(x, y) AS power(x, y);

CREATE OR REPLACE MACRO trino_sqrt(x) AS sqrt(x);

CREATE OR REPLACE MACRO trino_exp(x) AS exp(x);

CREATE OR REPLACE MACRO trino_ln(x) AS ln(x);

CREATE OR REPLACE MACRO trino_log2(x) AS log2(x);

CREATE OR REPLACE MACRO trino_log10(x) AS log10(x);

-- ---- More string functions ----

CREATE OR REPLACE MACRO trino_translate(source, src_chars, dest_chars) AS translate(source, src_chars, dest_chars);

-- ---- Regex (RE2 on both sides) ----
-- trino_regexp_like is the canonical demonstration of the interpretation layer:
-- Trino calls regexp_like(s, p), the translator pushes trino_regexp_like(...),
-- and the macro routes to DuckDB's regexp_matches(...). Renaming is invisible to
-- the plugin code.
CREATE OR REPLACE MACRO trino_regexp_like(s, pattern) AS regexp_matches(s, pattern);

-- `group` is a DuckDB reserved word; use `group_index` for the macro parameter.
CREATE OR REPLACE MACRO trino_regexp_extract
    (s, pattern)              AS regexp_extract(s, pattern),
    (s, pattern, group_index) AS regexp_extract(s, pattern, group_index);

-- ---- Round 4: encoding / distance / character-from-code ----

-- Char-from-codepoint. Aligned for valid code points; behavior outside the
-- Unicode range diverges but Trino's signature is `chr(bigint)` matching.
CREATE OR REPLACE MACRO trino_chr(n) AS chr(n);

-- URL percent-encoding (RFC 3986). Both engines aligned.
CREATE OR REPLACE MACRO trino_url_encode(s) AS url_encode(s);

CREATE OR REPLACE MACRO trino_url_decode(s) AS url_decode(s);

-- Hex / base64 encode + decode. Trino returns VARBINARY for to_hex / to_base64
-- and VARCHAR for from_hex / from_base64; DuckDB's hex() returns VARCHAR and
-- unhex() returns BLOB. Type alignment relies on the connector's BLOB↔VARBINARY
-- mapping; output bytes/chars are identical to Trino's.
CREATE OR REPLACE MACRO trino_to_hex(b) AS hex(b);

CREATE OR REPLACE MACRO trino_from_hex(s) AS unhex(s);

CREATE OR REPLACE MACRO trino_to_base64(b) AS to_base64(b);

CREATE OR REPLACE MACRO trino_from_base64(s) AS from_base64(s);

-- Distance metrics. DuckDB names differ (no _distance suffix); macro renames.
-- Levenshtein: number of single-char edits. Hamming: positional differences
-- (requires equal-length strings — both engines raise on mismatch).
CREATE OR REPLACE MACRO trino_levenshtein_distance(s1, s2) AS levenshtein(s1, s2);

CREATE OR REPLACE MACRO trino_hamming_distance(s1, s2) AS hamming(s1, s2);

-- ---- Round 5: trig + math ----

-- All double -> double; both engines use IEEE 754 standard math, output is
-- bit-exact aligned for finite inputs. NaN / ±Inf behaviour matches.

CREATE OR REPLACE MACRO trino_sin(x)  AS sin(x);
CREATE OR REPLACE MACRO trino_cos(x)  AS cos(x);
CREATE OR REPLACE MACRO trino_tan(x)  AS tan(x);
CREATE OR REPLACE MACRO trino_asin(x) AS asin(x);
CREATE OR REPLACE MACRO trino_acos(x) AS acos(x);
CREATE OR REPLACE MACRO trino_atan(x) AS atan(x);

CREATE OR REPLACE MACRO trino_atan2(y, x) AS atan2(y, x);

CREATE OR REPLACE MACRO trino_sinh(x) AS sinh(x);
CREATE OR REPLACE MACRO trino_cosh(x) AS cosh(x);
CREATE OR REPLACE MACRO trino_tanh(x) AS tanh(x);

CREATE OR REPLACE MACRO trino_degrees(x) AS degrees(x);
CREATE OR REPLACE MACRO trino_radians(x) AS radians(x);

CREATE OR REPLACE MACRO trino_cbrt(x) AS cbrt(x);

-- Trino name is `truncate`; DuckDB name is `trunc`. Macro rename, same semantics
-- (truncate toward zero, integer result for double input — both return DOUBLE).
CREATE OR REPLACE MACRO trino_truncate(x) AS trunc(x);

-- ---- Round 6b-core: cryptographic hashes (md5 / sha1 / sha256) ----

-- DuckDB core md5/sha1/sha256 return hex VARCHAR; Trino returns VARBINARY.
-- Wrap with unhex(...) to produce a BLOB matching Trino's wire shape.
-- NULL handling verified aligned (md5(NULL) → NULL; md5('a' || NULL || 'c') → NULL).
-- See REPORT-hash-null-handling.md.
-- sha512 / HMAC family require the crypto extension and are queued in
-- TODO-pushdown-duckdb.md → "Round 6b-ext", currently blocked on extension
-- availability for our platform.

CREATE OR REPLACE MACRO trino_md5(b) AS unhex(md5(b));

CREATE OR REPLACE MACRO trino_sha1(b) AS unhex(sha1(b));

CREATE OR REPLACE MACRO trino_sha256(b) AS unhex(sha256(b));

-- ---- Catalog of aliased functions ----
--
-- One row per (trino_name, arg_count) the translator may push down. The
-- translator reads this once per session and treats it as the authoritative
-- pushable set: if a (name, arity) is not here, do not push, even if a
-- trino_<name> macro happens to exist.
CREATE OR REPLACE MACRO trino_meta() AS TABLE
SELECT * FROM (
    VALUES
        -- Round 1 — string
        ('lower',        1, 'string'),    -- @placeholder (Unicode divergence; warn-on-emit)
        ('upper',        1, 'string'),    -- @placeholder
        ('length',       1, 'string'),
        ('reverse',      1, 'string'),    -- @placeholder (grapheme vs codepoint)
        ('trim',         1, 'string'),
        ('ltrim',        1, 'string'),
        ('rtrim',        1, 'string'),
        ('substring',    2, 'string'),
        ('substring',    3, 'string'),
        ('replace',      3, 'string'),
        ('strpos',       2, 'string'),
        ('starts_with',  2, 'string'),
        -- Round 2 — string
        ('lpad',         3, 'string'),
        ('rpad',         3, 'string'),
        ('concat_ws',    2, 'string'),
        ('concat_ws',    3, 'string'),
        ('concat_ws',    4, 'string'),
        ('concat_ws',    5, 'string'),
        -- Round 2 — numeric
        ('abs',          1, 'numeric'),
        ('ceil',         1, 'numeric'),
        ('floor',        1, 'numeric'),
        ('mod',          2, 'numeric'),
        ('power',        2, 'numeric'),
        -- Round 3 — numeric (math)
        ('sqrt',         1, 'numeric'),
        ('exp',          1, 'numeric'),
        ('ln',           1, 'numeric'),
        ('log2',         1, 'numeric'),
        ('log10',        1, 'numeric'),
        -- Round 3 — string
        ('translate',    3, 'string'),
        -- Round 3 — regex
        ('regexp_like',  2, 'regex'),
        ('regexp_extract', 2, 'regex'),
        ('regexp_extract', 3, 'regex'),
        -- Round 4 — encoding / distance / char-from-code
        ('chr',          1, 'string'),
        ('url_encode',   1, 'encoding'),
        ('url_decode',   1, 'encoding'),
        ('to_hex',       1, 'encoding'),
        ('from_hex',     1, 'encoding'),
        ('to_base64',    1, 'encoding'),
        ('from_base64',  1, 'encoding'),
        ('levenshtein_distance', 2, 'distance'),
        ('hamming_distance',     2, 'distance'),
        -- Round 5 — trig / hyperbolic / angle / cube root / truncate
        ('sin',          1, 'numeric'),
        ('cos',          1, 'numeric'),
        ('tan',          1, 'numeric'),
        ('asin',         1, 'numeric'),
        ('acos',         1, 'numeric'),
        ('atan',         1, 'numeric'),
        ('atan2',        2, 'numeric'),
        ('sinh',         1, 'numeric'),
        ('cosh',         1, 'numeric'),
        ('tanh',         1, 'numeric'),
        ('degrees',      1, 'numeric'),
        ('radians',      1, 'numeric'),
        ('cbrt',         1, 'numeric'),
        ('truncate',     1, 'numeric'),
        -- Round 6b-core — crypto hashes (no extension)
        ('md5',          1, 'hash'),
        ('sha1',         1, 'hash'),
        ('sha256',       1, 'hash')
) AS t(trino_name, arg_count, category);
