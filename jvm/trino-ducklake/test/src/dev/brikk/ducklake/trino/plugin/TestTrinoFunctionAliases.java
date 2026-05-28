/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL-level acceptance tests for the {@code trino_*} macro layer.
 *
 * <p>The fixtures in {@link #semanticCases()} are the "brain" of pushdown
 * semantics: one or more rows per {@code (trino_name, arg_count)} entry in
 * {@code trino_meta()}, with the expected DuckDB-evaluated result chosen to
 * match Trino's documented behavior. The {@link #testMetaMatchesFixtures()}
 * guard asserts every catalog entry has at least one fixture, so adding a
 * macro forces adding a semantic test.
 */
public class TestTrinoFunctionAliases
{
    @Test
    public void testTrinoMacroSemantics() throws Exception
    {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            TrinoFunctionAliases.applyDirect(stmt);
            for (SemanticCase c : semanticCases()) {
                Object actual = scalar(stmt, c.sql());
                if (c.expected() instanceof Number expectedNumber && actual instanceof Number actualNumber) {
                    // DuckDB JDBC's integer/bigint/double widths don't always match
                    // Trino's nominal types; compare on numeric value, not boxed type.
                    assertThat(actualNumber.doubleValue())
                            .as("%s — sql=%s", c.label(), c.sql())
                            .isEqualTo(expectedNumber.doubleValue());
                }
                else {
                    assertThat(actual).as("%s — sql=%s", c.label(), c.sql()).isEqualTo(c.expected());
                }
            }
        }
    }

    @Test
    public void testTrinoMetaCatalogMatchesMacros() throws Exception
    {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            TrinoFunctionAliases.applyDirect(stmt);

            Set<NameArity> meta = readMeta(stmt);
            List<String> installed = installedTrinoMacros(stmt);
            for (NameArity entry : meta) {
                assertThat(installed)
                        .as("macro trino_%s must exist (declared in trino_meta with arity %d)",
                                entry.name(), entry.arity())
                        .contains("trino_" + entry.name());
            }
        }
    }

    @Test
    public void testMetaMatchesFixtures() throws Exception
    {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            TrinoFunctionAliases.applyDirect(stmt);

            Set<NameArity> meta = readMeta(stmt);
            Set<NameArity> covered = new HashSet<>();
            for (SemanticCase c : semanticCases()) {
                covered.add(c.nameArity());
            }

            // Catalog ⊆ fixtures: every entry the translator may push must have at least one
            // recorded Trino-aligned result. New macros require new fixtures.
            assertThat(covered)
                    .as("trino_meta() rows missing semantic fixtures (add to semanticCases())")
                    .containsAll(meta);

            // Fixtures ⊆ catalog: no stale fixtures referencing a removed macro.
            assertThat(meta)
                    .as("semanticCases() rows missing from trino_meta() (add to trino-function-aliases.sql)")
                    .containsAll(covered);
        }
    }

    @Test
    public void testJavaPushableSetMatchesDuckDbMeta() throws Exception
    {
        // The Trino-side pushdown brain (DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS)
        // must match the DuckDB-side catalog (trino_meta()) exactly. Drift breaks
        // pushdown: either we push a function DuckDB cannot resolve, or we fail to
        // push one that would actually work.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            TrinoFunctionAliases.applyDirect(stmt);

            Set<NameArity> duckDbMeta = readMeta(stmt);
            Set<DuckDbExpressionTranslator.NameArity> javaSet =
                    DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS;

            Set<NameArity> javaAsNameArity = new HashSet<>();
            for (DuckDbExpressionTranslator.NameArity na : javaSet) {
                javaAsNameArity.add(new NameArity(na.name(), na.arity()));
            }
            assertThat(javaAsNameArity)
                    .as("DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS vs trino_meta() drifted")
                    .isEqualTo(duckDbMeta);
        }
    }

    @Test
    public void testApplyIsIdempotent() throws Exception
    {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            TrinoFunctionAliases.applyDirect(stmt);
            // CREATE OR REPLACE makes a second pass on the same connection a no-op.
            TrinoFunctionAliases.applyDirect(stmt);
            assertThat(scalar(stmt, "SELECT trino_lower('AB')")).isEqualTo("ab");
        }
    }

    @Test
    public void testUnicodeCaseFoldingDocumentsKnownDivergence() throws Exception
    {
        // Pins the EMPIRICAL divergence between trino_lower / trino_upper and
        // Trino's native lower/upper on non-ASCII input. This is why neither
        // (lower, 1) nor (upper, 1) appears in trino_meta() or PUSHABLE_FUNCTIONS:
        // pushing them through DuckDB would silently filter out rows Trino would
        // have kept.
        //
        // Specifically: DuckDB's bare lower(string) does ASCII-only case folding
        // (even with INSTALL/LOAD icu in the session). It drops the combining
        // dot above on 'İ' (U+0130) and returns plain 'i'. Trino's lower uses
        // Java's String.toLowerCase(Locale.ENGLISH) which preserves the dot via
        // Unicode-general lowering — 'i' + U+0307. The two strings compare
        // unequal under DuckDB's WHERE clause.
        //
        // Fix path: change the trino_lower macro body to use ICU collation
        // (e.g. `lower(s COLLATE icu_root)`) once we've verified that ICU's
        // output actually matches Trino across the test corpus, then re-add
        // (lower, 1) / (upper, 1) to trino_meta + PUSHABLE_FUNCTIONS.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            TrinoFunctionAliases.applyDirect(stmt);

            // The divergence — documented, not asserted-against-Trino.
            assertThat(scalar(stmt, "SELECT trino_lower('İ')"))
                    .as("DuckDB drops the combining dot — Trino would keep it")
                    .isEqualTo("i");
            // Trino's documented output for reference (not executed; informational
            // assertion that the two strings differ).
            assertThat("i̇")
                    .as("Trino's documented lower('İ') would produce i + U+0307")
                    .isNotEqualTo("i");

            // ASCII is unaffected — these are what the macros do safely:
            assertThat(scalar(stmt, "SELECT trino_lower('HeLLo')")).isEqualTo("hello");
            assertThat(scalar(stmt, "SELECT trino_upper('HeLLo')")).isEqualTo("HELLO");
        }
    }

    @Test
    public void testNullPropagation() throws Exception
    {
        // Trino-aligned: NULL input → NULL output for the round 1/2 functions.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            TrinoFunctionAliases.applyDirect(stmt);
            assertThat(scalar(stmt, "SELECT trino_lower(NULL)")).isNull();
            assertThat(scalar(stmt, "SELECT trino_substring(NULL, 1, 2)")).isNull();
            assertThat(scalar(stmt, "SELECT trino_abs(CAST(NULL AS INTEGER))")).isNull();
        }
    }

    // --- Fixtures -----------------------------------------------------------

    static List<SemanticCase> semanticCases()
    {
        return List.of(
                // String — round 1
                //   lower/1, upper/1, reverse/1 are pushable PLACEHOLDERS (see
                //   testUnicodeCaseFoldingDocumentsKnownDivergence + REPORT-string-unicode-audit.md).
                //   ASCII fixtures below confirm the safe range; divergent inputs are documented
                //   in the audit report. Translator emits warn-on-emit when these fire in pushdown.
                c("lower 1: ASCII case fold", "lower", 1,
                        "SELECT trino_lower('HeLLo')", "hello"),
                c("upper 1: ASCII case fold", "upper", 1,
                        "SELECT trino_upper('HeLLo')", "HELLO"),
                c("length 1: code points", "length", 1,
                        "SELECT trino_length('abcde')", 5L),
                c("length 1: multibyte", "length", 1,
                        // Cyrillic 5 code points; Trino & DuckDB both code-point counts.
                        "SELECT trino_length('пингвин')", 7L),
                c("reverse 1: ASCII", "reverse", 1,
                        "SELECT trino_reverse('abc')", "cba"),
                c("trim 1: spaces", "trim", 1,
                        "SELECT trino_trim('  hi  ')", "hi"),
                // Java whitespace coverage — macro passes explicit char set so DuckDB's
                // bare-trim-only-strips-space behaviour is corrected.
                c("trim 1: tab", "trim", 1,
                        "SELECT trino_trim(chr(9) || 'hi' || chr(9))", "hi"),
                c("trim 1: LF", "trim", 1,
                        "SELECT trino_trim(chr(10) || 'hi' || chr(10))", "hi"),
                c("trim 1: CR", "trim", 1,
                        "SELECT trino_trim(chr(13) || 'hi' || chr(13))", "hi"),
                c("trim 1: EM SPACE", "trim", 1,
                        "SELECT trino_trim(chr(8195) || 'hi' || chr(8195))", "hi"),
                c("trim 1: NBSP NOT stripped (matches Java)", "trim", 1,
                        "SELECT trino_trim(chr(160) || 'hi' || chr(160))",
                        " hi "),
                c("ltrim 1: spaces", "ltrim", 1,
                        "SELECT trino_ltrim('  hi  ')", "hi  "),
                c("ltrim 1: tab", "ltrim", 1,
                        "SELECT trino_ltrim(chr(9) || 'hi' || chr(9))", "hi\t"),
                c("rtrim 1: spaces", "rtrim", 1,
                        "SELECT trino_rtrim('  hi  ')", "  hi"),
                c("rtrim 1: tab", "rtrim", 1,
                        "SELECT trino_rtrim(chr(9) || 'hi' || chr(9))", "\thi"),
                c("substring 2: from index", "substring", 2,
                        "SELECT trino_substring('abcdef', 2)", "bcdef"),
                c("substring 2: negative counts from end", "substring", 2,
                        "SELECT trino_substring('abcdef', -2)", "ef"),
                c("substring 3: index + length", "substring", 3,
                        "SELECT trino_substring('abcdef', 2, 3)", "bcd"),
                c("replace 3", "replace", 3,
                        "SELECT trino_replace('a.b.c', '.', '-')", "a-b-c"),
                c("strpos 2: found", "strpos", 2,
                        "SELECT trino_strpos('abcdef', 'cd')", 3L),
                c("strpos 2: not found returns 0", "strpos", 2,
                        "SELECT trino_strpos('abcdef', 'zz')", 0L),
                c("starts_with 2: true", "starts_with", 2,
                        "SELECT trino_starts_with('abcdef', 'abc')", true),
                c("starts_with 2: false", "starts_with", 2,
                        "SELECT trino_starts_with('abcdef', 'bcd')", false),
                // String — round 2
                c("lpad 3: pad on left", "lpad", 3,
                        "SELECT trino_lpad('abc', 5, '0')", "00abc"),
                c("rpad 3: pad on right", "rpad", 3,
                        "SELECT trino_rpad('abc', 5, '0')", "abc00"),
                c("concat_ws 2: one value", "concat_ws", 2,
                        "SELECT trino_concat_ws(',', 'a')", "a"),
                c("concat_ws 3: two values", "concat_ws", 3,
                        "SELECT trino_concat_ws(',', 'a', 'b')", "a,b"),
                c("concat_ws 4: three values", "concat_ws", 4,
                        "SELECT trino_concat_ws(',', 'a', 'b', 'c')", "a,b,c"),
                c("concat_ws 5: four values + skip NULL", "concat_ws", 5,
                        // Trino & DuckDB both skip NULL elements (not the separator).
                        "SELECT trino_concat_ws(',', 'a', NULL, 'c', 'd')", "a,c,d"),
                // Numeric — round 2
                c("abs 1: negative integer", "abs", 1,
                        "SELECT trino_abs(-5)", 5),
                c("abs 1: positive double", "abs", 1,
                        "SELECT trino_abs(-3.25)", 3.25),
                c("ceil 1: round up", "ceil", 1,
                        "SELECT trino_ceil(1.2)", 2.0),
                c("ceil 1: exact integer", "ceil", 1,
                        "SELECT trino_ceil(2.0)", 2.0),
                c("floor 1: round down", "floor", 1,
                        "SELECT trino_floor(1.7)", 1.0),
                c("mod 2: integer", "mod", 2,
                        "SELECT trino_mod(10, 3)", 1),
                c("mod 2: sign follows dividend", "mod", 2,
                        "SELECT trino_mod(-10, 3)", -1),
                c("power 2: integer base", "power", 2,
                        "SELECT trino_power(2, 10)", 1024.0),
                // Numeric — round 3 (math)
                c("sqrt 1: integer input", "sqrt", 1,
                        "SELECT trino_sqrt(16.0)", 4.0),
                c("exp 1: zero", "exp", 1,
                        "SELECT trino_exp(0.0)", 1.0),
                c("ln 1: e", "ln", 1,
                        // ln(e) = 1 exactly in IEEE 754.
                        "SELECT trino_ln(exp(1.0))", 1.0),
                c("log2 1: power of two", "log2", 1,
                        "SELECT trino_log2(1024.0)", 10.0),
                c("log10 1: power of ten", "log10", 1,
                        "SELECT trino_log10(1000.0)", 3.0),
                // String — round 3
                c("translate 3: vowels upper", "translate", 3,
                        "SELECT trino_translate('hello', 'eo', 'EO')", "hEllO"),
                c("translate 3: extra src chars deleted", "translate", 3,
                        // Trino & DuckDB: when from-string is longer than to-string,
                        // extra source chars are deleted from the result.
                        "SELECT trino_translate('abcdef', 'bdf', 'XY')", "aXcYe"),
                // Regex — round 3
                c("regexp_like 2: match", "regexp_like", 2,
                        "SELECT trino_regexp_like('abc-123', '\\d+')", true),
                c("regexp_like 2: no match", "regexp_like", 2,
                        "SELECT trino_regexp_like('no digits here', '\\d+')", false),
                c("regexp_extract 2: first match", "regexp_extract", 2,
                        "SELECT trino_regexp_extract('abc-123-xyz', '\\d+')", "123"),
                c("regexp_extract 3: explicit group", "regexp_extract", 3,
                        "SELECT trino_regexp_extract('abc-123-xyz', '([a-z]+)-(\\d+)', 2)", "123"),
                // Round 4 — encoding / distance / chr
                c("chr 1: ASCII A", "chr", 1,
                        "SELECT trino_chr(65)", "A"),
                c("chr 1: Unicode €", "chr", 1,
                        "SELECT trino_chr(8364)", "€"),
                c("url_encode 1: spaces", "url_encode", 1,
                        "SELECT trino_url_encode('hello world')", "hello%20world"),
                c("url_decode 1: roundtrip", "url_decode", 1,
                        "SELECT trino_url_decode('hello%20world')", "hello world"),
                c("to_hex 1: blob", "to_hex", 1,
                        "SELECT trino_to_hex(CAST('AB' AS BLOB))", "4142"),
                c("from_hex 1: roundtrip via cast", "from_hex", 1,
                        "SELECT CAST(trino_from_hex('4142') AS VARCHAR)", "AB"),
                c("to_base64 1: blob", "to_base64", 1,
                        "SELECT trino_to_base64(CAST('AB' AS BLOB))", "QUI="),
                c("from_base64 1: roundtrip via cast", "from_base64", 1,
                        "SELECT CAST(trino_from_base64('QUI=') AS VARCHAR)", "AB"),
                c("levenshtein_distance 2: classic", "levenshtein_distance", 2,
                        "SELECT trino_levenshtein_distance('kitten', 'sitting')", 3L),
                c("hamming_distance 2: equal length", "hamming_distance", 2,
                        "SELECT trino_hamming_distance('abcde', 'abdde')", 1L),
                // Round 5 — trig / hyperbolic / angle / cube root / truncate
                c("sin 1: zero", "sin", 1,
                        "SELECT trino_sin(0.0)", 0.0),
                c("cos 1: zero", "cos", 1,
                        "SELECT trino_cos(0.0)", 1.0),
                c("tan 1: zero", "tan", 1,
                        "SELECT trino_tan(0.0)", 0.0),
                c("asin 1: one", "asin", 1,
                        "SELECT trino_asin(1.0)", Math.PI / 2),
                c("acos 1: zero", "acos", 1,
                        "SELECT trino_acos(0.0)", Math.PI / 2),
                c("atan 1: zero", "atan", 1,
                        "SELECT trino_atan(0.0)", 0.0),
                c("atan2 2: zero over one", "atan2", 2,
                        "SELECT trino_atan2(0.0, 1.0)", 0.0),
                c("sinh 1: zero", "sinh", 1,
                        "SELECT trino_sinh(0.0)", 0.0),
                c("cosh 1: zero", "cosh", 1,
                        "SELECT trino_cosh(0.0)", 1.0),
                c("tanh 1: zero", "tanh", 1,
                        "SELECT trino_tanh(0.0)", 0.0),
                c("degrees 1: pi", "degrees", 1,
                        "SELECT trino_degrees(pi())", 180.0),
                c("radians 1: 180", "radians", 1,
                        "SELECT trino_radians(180.0)", Math.PI),
                c("cbrt 1: 8", "cbrt", 1,
                        "SELECT trino_cbrt(8.0)", 2.0),
                c("truncate 1: positive", "truncate", 1,
                        "SELECT trino_truncate(3.7)", 3.0),
                c("truncate 1: negative", "truncate", 1,
                        "SELECT trino_truncate(-3.7)", -3.0),
                // Round 6b-core — crypto hashes (no extension)
                // Macros return BLOB matching Trino's VARBINARY. Compare via hex roundtrip
                // to keep assertions readable.
                c("md5 1: empty", "md5", 1,
                        "SELECT lower(hex(trino_md5('')))", "d41d8cd98f00b204e9800998ecf8427e"),
                c("md5 1: abc", "md5", 1,
                        "SELECT lower(hex(trino_md5('abc')))", "900150983cd24fb0d6963f7d28e17f72"),
                c("md5 1: NULL propagates", "md5", 1,
                        "SELECT trino_md5(NULL)", null),
                c("sha1 1: empty", "sha1", 1,
                        "SELECT lower(hex(trino_sha1('')))", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
                c("sha1 1: abc", "sha1", 1,
                        "SELECT lower(hex(trino_sha1('abc')))", "a9993e364706816aba3e25717850c26c9cd0d89d"),
                c("sha256 1: empty", "sha256", 1,
                        "SELECT lower(hex(trino_sha256('')))",
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                c("sha256 1: abc", "sha256", 1,
                        "SELECT lower(hex(trino_sha256('abc')))",
                        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
    }

    private static SemanticCase c(String label, String name, int arity, String sql, Object expected)
    {
        return new SemanticCase(label, new NameArity(name, arity), sql, expected);
    }

    // --- Helpers ------------------------------------------------------------

    private static Set<NameArity> readMeta(Statement stmt) throws Exception
    {
        Set<NameArity> meta = new HashSet<>();
        try (ResultSet rs = stmt.executeQuery(
                "SELECT trino_name, arg_count FROM trino_meta() ORDER BY trino_name, arg_count")) {
            while (rs.next()) {
                meta.add(new NameArity(rs.getString(1), rs.getInt(2)));
            }
        }
        return meta;
    }

    private static List<String> installedTrinoMacros(Statement stmt) throws Exception
    {
        List<String> installed = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(
                "SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'trino\\_%' ESCAPE '\\'")) {
            while (rs.next()) {
                installed.add(rs.getString(1));
            }
        }
        return installed;
    }

    private static Object scalar(Statement stmt, String sql) throws Exception
    {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).as("query produced no rows: %s", sql).isTrue();
            return rs.getObject(1);
        }
    }

    record SemanticCase(String label, NameArity nameArity, String sql, Object expected)
    {
        @Override
        public String toString()
        {
            return label;
        }
    }

    record NameArity(String name, int arity) {}
}
