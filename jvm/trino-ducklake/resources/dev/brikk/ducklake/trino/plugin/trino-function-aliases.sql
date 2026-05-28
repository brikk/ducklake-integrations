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

CREATE OR REPLACE MACRO trino_lower(s) AS lower(s);

CREATE OR REPLACE MACRO trino_upper(s) AS upper(s);

CREATE OR REPLACE MACRO trino_length(s) AS length(s);

CREATE OR REPLACE MACRO trino_reverse(s) AS reverse(s);

CREATE OR REPLACE MACRO trino_trim(s) AS trim(s);

CREATE OR REPLACE MACRO trino_ltrim(s) AS ltrim(s);

CREATE OR REPLACE MACRO trino_rtrim(s) AS rtrim(s);

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

-- ---- Catalog of aliased functions ----
--
-- One row per (trino_name, arg_count) the translator may push down. The
-- translator reads this once per session and treats it as the authoritative
-- pushable set: if a (name, arity) is not here, do not push, even if a
-- trino_<name> macro happens to exist.
CREATE OR REPLACE MACRO trino_meta() AS TABLE
SELECT * FROM (
    VALUES
        ('lower',       1, 'string'),
        ('upper',       1, 'string'),
        ('length',      1, 'string'),
        ('reverse',     1, 'string'),
        ('trim',        1, 'string'),
        ('ltrim',       1, 'string'),
        ('rtrim',       1, 'string'),
        ('substring',   2, 'string'),
        ('substring',   3, 'string'),
        ('replace',     3, 'string'),
        ('strpos',      2, 'string'),
        ('starts_with', 2, 'string'),
        ('lpad',        3, 'string'),
        ('rpad',        3, 'string'),
        ('concat_ws',   2, 'string'),
        ('concat_ws',   3, 'string'),
        ('concat_ws',   4, 'string'),
        ('concat_ws',   5, 'string'),
        ('abs',         1, 'numeric'),
        ('ceil',        1, 'numeric'),
        ('floor',       1, 'numeric'),
        ('mod',         2, 'numeric'),
        ('power',       2, 'numeric')
) AS t(trino_name, arg_count, category);
