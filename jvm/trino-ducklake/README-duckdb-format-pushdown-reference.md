# Pushdown Reference — DuckDB-Format Reads

The complete set of predicates the connector pushes **server-side into DuckDB**
when a query reads a **duckdb-format** data file (a `.db` split; see the
[DuckDB-Format Data Files](README.md#duckdb-format-data-files-experimental)
section of the README). This is a feature of the *duckdb file format read path*,
not of the connector in general — parquet-format reads use Trino's standard
Parquet reader and only get the TupleDomain + file-pruning layer (§1), never the
expression/function layer (§2–§3).

For the roadmap, history, and open items (steps 5–6, deferred functions) see
[TODO-pushdown-duckdb.md](dev-docs/TODO-pushdown-duckdb.md). This doc is the *current
surface*; that one is the *program tracker*.

## Discipline (non-negotiable)

- **Lossless only.** Anything we can't translate with confidence stays in Trino.
- **Curated, not "anything that looks similar."** Every entry is explicit, with recorded NULL / Unicode / edge semantics.
- **Cross-engine semantic test per entry.** The pushed result must match Trino's own evaluation byte-for-byte (`TestTrinoFunctionAliases`, `TestDuckDbExpressionTranslator`, `TestDucklakeDuckDbReadMode`).

## How it fires

`DucklakeMetadata.applyFilter` → `DuckDbExpressionTranslator.translateConjuncts`
emits a DuckDB SQL fragment per pushable conjunct; survivors are stored on the
table handle and rendered into the `WHERE` clause by `DuckDbSelectSqlBuilder`
(same shape for the in-process and Quack executors). Function-shape entries
resolve to `trino_<name>(...)` macros / native scalar functions provided by the
bundled [`trino_parity` DuckDB extension](../../duckdb-trino-parity-extension),
`LOAD`ed on attach.

**Mixed-format safety.** A DuckLake table can carry both parquet and duckdb
splits. Translated conjuncts are returned *both* as a pushdown hint (consumed by
`.db` splits) *and* in `remainingExpression` (so Trino re-evaluates above the
scan for parquet splits). Each path is independently correct.

---

## 1. Predicate / value pushdown (all formats)

These work on **both** parquet and duckdb splits — they don't need the expression
translator.

| Surface | Notes |
|---|---|
| **TupleDomain on `WHERE`** | Range/equality/`IN`/`IS NULL` constraints on all column types, intersected with file statistics. |
| **File-level pruning** | Whole files eliminated via `ducklake_file_column_stats` min/max/null counts (top-level columns; nested-leaf stats are written for DuckDB-side readers). |
| **Partition pruning — identity** | Files pruned by partition value for identity-partitioned columns. |
| **Partition pruning — temporal** | `year()` / `month()` / `day()` / `hour()` partition transforms pruned against the partition value (DuckLake 1.0 calendar contract). |
| **Partition pruning — bucket** | `bucket(N, col)`: equality predicates pruned by hashing the constant (Iceberg-compatible Murmur3); ranges are *not* pruned (bucketing scrambles ordering). |
| **Dynamic filters** | Join-build dynamic filters intersected with file-level stats. |

## 2. Operators & transforms (duckdb-format only)

Translator-level rewrites — emitted directly as SQL, not via the macro catalog.

| Group | Pushed |
|---|---|
| Comparison | `=`, `<>`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT DISTINCT FROM` |
| Logical | `AND`, `OR`, `NOT` |
| Arithmetic | `+`, `-`, `*`, `/`, `%` (infix), unary `-` (negate) |
| Null-handling | `COALESCE` (variadic), `NULLIF` |
| Cast | `CAST` / `TRY_CAST` for primitive types (BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DOUBLE, VARCHAR, DATE) |
| Pattern | `LIKE` / `NOT LIKE` with optional `ESCAPE` (constant patterns only; dynamic or NULL patterns stay unpushed) |
| `BETWEEN` | Pushed implicitly — Trino's planner decomposes it to `>= AND <=` before `applyFilter`, so the comparison + `AND` translators handle it. |
| `concat(a, b, …)` → `(a \|\| b \|\| …)` | Translator rewrite for VARCHAR returns, **not** a macro: DuckDB's `concat` skips NULL while Trino's NULL-propagates; the `\|\|` operator propagates in both, matching Trino. |

## 3. Functions (duckdb-format only)

**95 entries** in `trino_meta()` (the catalog the translator's
`PUSHABLE_FUNCTIONS` set is kept in lockstep with). Most are `trino_<name>`
macros in the extension's `macro_definitions.cpp`; a few are **native C++** where
DuckDB's built-in diverges from Trino and a macro can't fix it (noted below).
Counts: string 22, numeric 32, regex 5, encoding 6, distance 2, hash 6, date 20,
conditional 2.

| Category | Functions | Notes |
|---|---|---|
| **String — native (ICU)** | `lower`, `upper`, `reverse`, `trim`, `ltrim`, `rtrim`, `normalize/1` | Native C++ (`string_functions.cpp`) for full Trino parity: root-locale **full** case folding (`lower('İ')`→`'i'+U+0307`, `upper('ß')`→`'SS'`), **code-point** reverse, `Character.isWhitespace`-aligned trim, NFC via `icu::Normalizer2`. `normalize/2` (NFD/NFKC/NFKD selector) is **not** pushed — the vendored ICU snapshot ships only NFC data. |
| **String — macro** | `length`, `substring/{2,3}`, `replace`, `strpos`, `starts_with`, `lpad`, `rpad`, `concat_ws/{2..5}`, `translate`, `chr`, `bit_length` | Code-point (not byte / grapheme) semantics; pinned in fixtures against ZWJ-family emoji, combining marks, 4-byte code points. |
| **Numeric** | `abs`, `ceil`, `floor`, `mod`, `power`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `sinh`, `cosh`, `tanh`, `degrees`, `radians`, `cbrt`, `truncate`, `sign`, `pi/0`, `bitwise_and`, `bitwise_or`, `bitwise_not`, `bitwise_xor`, `bitwise_left_shift`, `bitwise_right_shift` | Float `mod` is gated out (Trino IEEE-remainder vs DuckDB fmod); `bitwise_right_shift` on negatives can differ (signed/unsigned) — safe for typical positive-integer use. |
| **Regex** | `regexp_like/2`, `regexp_extract/{2,3}`, `regexp_replace/{2,3}` | RE2 on both sides; `regexp_replace` forces the `'g'` flag to match Trino's global default. |
| **Encoding** | `url_encode`, `url_decode`, `to_hex`, `from_hex`, `to_base64`, `from_base64` | RFC-3986 / hex / base64; output bytes identical. |
| **Distance** | `levenshtein_distance`, `hamming_distance` | Code-point edit distance. |
| **Hash** | `md5`, `sha1`, `sha256`, `sha512`, `xxhash64`, `hmac_sha256/2` | `md5`/`sha1`/`sha256` wrap DuckDB's hex output in `unhex(...)`. `sha512`, `xxhash64`, `hmac_sha256` are **native C++** (`hash_functions.cpp`) over vendored xxHash (BSD-2) + WjCryptLib SHA (public domain) — no third-party community-extension dependency. `xxhash64` is emitted **big-endian** to match Trino; `hmac_sha256(data, key)` runs over raw VARBINARY bytes (which the VARCHAR-only `crypto_hmac` couldn't do). |
| **Date / time** | `year`, `month`, `day`, `quarter`, `hour`, `minute`, `second`, `millisecond`, `day_of_week` (ISO), `day_of_year`, `last_day_of_month`, `week` / `week_of_year` (ISO), `year_of_week` / `yow`, `date_trunc/2`, `date_diff/3`, `to_unixtime`, `from_unixtime`, `with_timezone/2` | Type-gated to safe argument types. Over `TIMESTAMP WITH TIME ZONE` they push only when the `pushdown_timestamp_with_timezone` session property is on (**default on**); set `false` to keep them above the scan. Requires `SET TimeZone` to succeed on attach (automatic for named IANA zones + integer-hour offsets). |
| **Conditional** | `if/{2,3}` | 2-arg form returns NULL on the false branch. |

## Not pushable (by design)

- `at_timezone(WTZ, varchar)` — DuckDB's TIMESTAMPTZ has no per-value zone metadata, so "rezone display" isn't expressible.
- `hmac_md5` / `hmac_sha1` / `hmac_sha512` — only `hmac_sha256` is ported natively; add the WjCryptLib primitives if a workload needs the others.
- `murmur3` — reconstructable from `murmurhash3_x64_128` but deferred (niche; needs a live Trino byte-layout confirmation). See TODO.
- `url_extract_*` (netquack) — rejected: DuckDB returns empty strings where Trino returns NULL, plus a `BIGINT`-vs-`VARCHAR` port mismatch.
- `normalize/2`, `position` (operator-form), `lower`/`upper` over collations.

## Adding an entry

See the "Adding a new alias — checklist" in
[TODO-pushdown-duckdb.md](dev-docs/TODO-pushdown-duckdb.md) — macros/`trino_meta()` live in
the `trino_parity` extension repo; the connector side is `PUSHABLE_FUNCTIONS` +
a `TestTrinoFunctionAliases` fixture, with lockstep guards that fail on drift.
