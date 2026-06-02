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
import java.util.Properties;
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
    /**
     * Opens an in-memory DuckDB JDBC connection with the trino_parity extension
     * LOADed. Path comes from {@link TrinoParityExtensionResolver} — same code
     * path the production in-process executor uses. Fails fast if the bundled
     * extension is missing on the test classpath (build the extension first:
     * {@code (cd duckdb-trino-parity-extension && GEN=ninja make)}).
     */
    private static Connection openConnectionWithExtension() throws java.sql.SQLException
    {
        String path = TrinoParityExtensionResolver.resolveBundledExtensionPath()
                .orElseThrow(() -> new AssertionError(
                        "trino_parity extension not bundled in plugin jar on this platform — " +
                                "build it first: `(cd duckdb-trino-parity-extension && GEN=ninja make)`."));
        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);
        TrinoFunctionAliases.loadInProcess(conn, path);
        return conn;
    }

    @Test
    public void testTrinoMacroSemantics() throws Exception
    {
        try (Connection conn = openConnectionWithExtension();
                Statement stmt = conn.createStatement()) {
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
        try (Connection conn = openConnectionWithExtension();
                Statement stmt = conn.createStatement()) {
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
        try (Connection conn = openConnectionWithExtension();
                Statement stmt = conn.createStatement()) {
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
        try (Connection conn = openConnectionWithExtension();
                Statement stmt = conn.createStatement()) {
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
    public void testUnicodeCaseFoldingMatchesTrinoSpec() throws Exception
    {
        // Pins the FULL-CASE-FOLDING semantics provided by the native trino_lower /
        // trino_upper / trino_reverse in the trino_parity extension (ICU-backed,
        // root locale). Used to be a "documented divergence" test back when the
        // macro layer wrapped DuckDB's bare lower/upper — those did simple case
        // folding and disagreed with Trino on U+0130 / U+00DF. The native
        // extension closes those gaps.
        try (Connection conn = openConnectionWithExtension();
                Statement stmt = conn.createStatement()) {
            // Turkish capital dotted I → 'i' + U+0307 (NOT bare 'i').
            assertThat(scalar(stmt, "SELECT trino_lower(chr(304))"))
                    .as("trino_lower('İ') matches Trino's full case folding: 'i' + U+0307")
                    .isEqualTo("i̇");

            // German sharp s upper → 'SS' (NOT U+1E9E).
            assertThat(scalar(stmt, "SELECT trino_upper('ß')"))
                    .as("trino_upper('ß') matches Trino's full case folding: 'SS'")
                    .isEqualTo("SS");
            assertThat(scalar(stmt, "SELECT trino_upper('straße strauß')"))
                    .as("multiple ß expand independently")
                    .isEqualTo("STRASSE STRAUSS");

            // Code-point reverse on decomposed café — combining mark detaches from the e.
            assertThat(scalar(stmt, "SELECT trino_reverse('cafe' || chr(769))"))
                    .as("trino_reverse splits combining marks (code-point reverse)")
                    .isEqualTo("́efac");

            // ASCII baseline still works.
            assertThat(scalar(stmt, "SELECT trino_lower('HeLLo')")).isEqualTo("hello");
            assertThat(scalar(stmt, "SELECT trino_upper('HeLLo')")).isEqualTo("HELLO");
        }
    }

    @Test
    public void testNullPropagation() throws Exception
    {
        // Trino-aligned: NULL input → NULL output for the round 1/2 functions.
        try (Connection conn = openConnectionWithExtension();
                Statement stmt = conn.createStatement()) {
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
                //   lower/1, upper/1, reverse/1 are now NATIVE C++ scalar functions in
                //   the trino_parity extension (ICU full case folding, root locale, plus
                //   code-point reverse). The placeholder/warn-on-emit machinery is gone.
                //   testUnicodeCaseFoldingMatchesTrinoSpec pins the formerly-divergent inputs.
                c("lower 1: ASCII case fold", "lower", 1,
                        "SELECT trino_lower('HeLLo')", "hello"),
                c("lower 1: Turkish dotted I", "lower", 1,
                        "SELECT trino_lower(chr(304))", "i̇"),
                c("upper 1: ASCII case fold", "upper", 1,
                        "SELECT trino_upper('HeLLo')", "HELLO"),
                c("upper 1: German ß expands to SS", "upper", 1,
                        "SELECT trino_upper('ß')", "SS"),
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
                // Unit pins — Trino's StringFunctions.substring uses offsetOfCodePoint /
                // countCodePoints; DuckDB's substr probed empirically to do the same.
                // Both engines: Unicode code points, NOT bytes, NOT graphemes.
                c("substring 3: 2-byte UTF-8 codepoint (Cyrillic)", "substring", 3,
                        // Each Cyrillic letter is 2 bytes in UTF-8 / 1 codepoint / 1 grapheme.
                        // substr(2, 3) at byte-mode would slice mid-codepoint and return garbage;
                        // at codepoint-mode returns three letters; at grapheme-mode same as codepoint.
                        "SELECT trino_substring('пингвин', 2, 3)", "инг"),
                c("substring 3: 4-byte UTF-8 codepoint (emoji)", "substring", 3,
                        // Each emoji is 4 bytes UTF-8 / 1 codepoint / 1 grapheme. substr(2, 1)
                        // returning the second whole emoji confirms the engine is NOT counting bytes.
                        "SELECT trino_substring('🐧🦆🐍', 2, 1)", "🦆"),
                c("substring 3: splits grapheme cluster (combining mark)", "substring", 3,
                        // 'café' as 'cafe' + U+0301 has 5 codepoints / 4 graphemes. Position 4 is the
                        // bare 'e' (codepoint 4) — a grapheme-aware substr would return the composed
                        // 'é'. Both engines splitting the grapheme is the alignment-confirming case.
                        "SELECT trino_substring('caf' || chr(101) || chr(769), 4, 1)", "e"),
                c("substring 3: splits ZWJ emoji sequence", "substring", 3,
                        // '👨‍👩‍👧' is 5 codepoints (man, ZWJ, woman, ZWJ, girl) / 1 grapheme.
                        // Position 1 length 1 returns the bare man emoji in both engines — confirms
                        // codepoint-counting (a grapheme-aware substr would return the whole family).
                        "SELECT trino_substring('👨‍👩‍👧', 1, 1)", "👨"),
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
                // Normalize — native NFC via vendored ICU's Normalizer2.
                // 2-arg form (NFD/NFKC/NFKD) is not pushable; Trino handles those above-scan.
                c("normalize 1: NFC default composes decomposed café", "normalize", 1,
                        // 'cafe' + U+0301 (5 cp) → 'café' with composed U+00E9 (4 cp).
                        "SELECT trino_normalize('cafe' || chr(769)) = 'café'", true),
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
                        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
                // Round 6a — core DuckDB easy wins
                c("sign 1: negative", "sign", 1,
                        "SELECT trino_sign(-5)", -1),
                c("sign 1: zero", "sign", 1,
                        "SELECT trino_sign(0)", 0),
                c("sign 1: positive double", "sign", 1,
                        "SELECT trino_sign(7.3)", 1.0),
                c("bit_length 1: empty", "bit_length", 1,
                        "SELECT trino_bit_length('')", 0L),
                c("bit_length 1: 3 ASCII chars", "bit_length", 1,
                        "SELECT trino_bit_length('abc')", 24L),
                c("pi 0: constant", "pi", 0,
                        "SELECT trino_pi()", Math.PI),
                c("bitwise_xor 2: 5 ^ 3", "bitwise_xor", 2,
                        // 5 = 0b101, 3 = 0b011, xor = 0b110 = 6
                        "SELECT trino_bitwise_xor(5, 3)", 6),
                c("bitwise_xor 2: identity", "bitwise_xor", 2,
                        "SELECT trino_bitwise_xor(0, 0)", 0),
                c("regexp_replace 2: global delete (Trino default)", "regexp_replace", 2,
                        "SELECT trino_regexp_replace('abc-123-xyz', '\\d')", "abc--xyz"),
                c("regexp_replace 3: global replace (Trino default)", "regexp_replace", 3,
                        // DuckDB default is FIRST match; macro forces 'g' so 'aaa' → 'bbb' (not 'baa').
                        "SELECT trino_regexp_replace('aaa', 'a', 'b')", "bbb"),
                c("regexp_replace 3: backreferences", "regexp_replace", 3,
                        "SELECT trino_regexp_replace('abc', '(a)(b)', '\\2\\1')", "bac"),
                // Round 6g — bitwise function-form
                c("bitwise_and 2", "bitwise_and", 2,
                        // 12 = 0b1100, 10 = 0b1010, AND = 0b1000 = 8
                        "SELECT trino_bitwise_and(12, 10)", 8),
                c("bitwise_or 2", "bitwise_or", 2,
                        // 12 = 0b1100, 10 = 0b1010, OR = 0b1110 = 14
                        "SELECT trino_bitwise_or(12, 10)", 14),
                c("bitwise_not 1: ~0 (8-bit conceptual = -1 in 2's-complement)", "bitwise_not", 1,
                        // For DuckDB INTEGER (4 bytes), ~0 = -1.
                        "SELECT trino_bitwise_not(0)", -1),
                c("bitwise_left_shift 2: 1 << 4", "bitwise_left_shift", 2,
                        "SELECT trino_bitwise_left_shift(1, 4)", 16),
                c("bitwise_right_shift 2: 16 >> 2", "bitwise_right_shift", 2,
                        "SELECT trino_bitwise_right_shift(16, 2)", 4),
                // Round 6g — date convenience
                c("year 1: from DATE", "year", 1,
                        "SELECT trino_year(DATE '2024-01-15')", 2024L),
                c("month 1: from DATE", "month", 1,
                        "SELECT trino_month(DATE '2024-01-15')", 1L),
                c("day 1: from DATE", "day", 1,
                        "SELECT trino_day(DATE '2024-01-15')", 15L),
                c("quarter 1: Q1", "quarter", 1,
                        "SELECT trino_quarter(DATE '2024-02-15')", 1L),
                c("quarter 1: Q3", "quarter", 1,
                        "SELECT trino_quarter(DATE '2024-08-15')", 3L),
                // Round 6i — conditional + date arithmetic
                c("if 2: true branch", "if", 2,
                        "SELECT trino_if(true, 'yes')", "yes"),
                c("if 2: false returns NULL", "if", 2,
                        "SELECT trino_if(false, 'yes')", null),
                c("if 3: true branch", "if", 3,
                        "SELECT trino_if(true, 'a', 'b')", "a"),
                c("if 3: false branch", "if", 3,
                        "SELECT trino_if(false, 'a', 'b')", "b"),
                c("date_trunc 2: month on DATE returns TIMESTAMP-typed value", "date_trunc", 2,
                        // DuckDB's date_trunc returns TIMESTAMP even for DATE input; Trino's
                        // returns DATE in the same case. The values are numerically equivalent
                        // — for typical WHERE predicates DuckDB auto-casts DATE to TIMESTAMP
                        // at midnight so comparisons align. Pinning the actual format here.
                        "SELECT CAST(trino_date_trunc('month', DATE '2024-03-15') AS VARCHAR)",
                        "2024-03-01 00:00:00"),
                c("date_trunc 2: day on TIMESTAMP", "date_trunc", 2,
                        "SELECT CAST(trino_date_trunc('day', TIMESTAMP '2024-01-15 12:34:56') AS VARCHAR)",
                        "2024-01-15 00:00:00"),
                c("date_diff 3: days", "date_diff", 3,
                        "SELECT trino_date_diff('day', DATE '2024-01-01', DATE '2024-01-15')", 14L),
                c("date_diff 3: month boundary", "date_diff", 3,
                        // Both engines count boundary crossings, not whole units elapsed.
                        "SELECT trino_date_diff('month', DATE '2024-01-31', DATE '2024-02-01')", 1L),
                // === Unicode pressure for string macros — alignment lock-in ===
                //
                // Every non-placeholder string macro gets at least one fixture with
                // multi-byte UTF-8 input. The reference "nasty character" is the
                // ZWJ family emoji '👨‍👩‍👧' — 5 codepoints (man, ZWJ, woman, ZWJ, girl),
                // 1 grapheme, 18 bytes UTF-8. If an engine ever switched to
                // byte-counting or grapheme-counting, these tests would surface it
                // immediately. Both engines today count Unicode CODE POINTS for
                // all of these — verified empirically and (for Trino) in source.
                //
                // length: ZWJ family has 5 codepoints (man, ZWJ, woman, ZWJ, girl).
                c("length 1: ZWJ family emoji = 5 codepoints", "length", 1,
                        "SELECT trino_length('👨‍👩‍👧')", 5L),
                c("length 1: 4-byte emoji × 3 = 3 codepoints", "length", 1,
                        "SELECT trino_length('🐧🦆🐍')", 3L),
                // strpos: codepoint-indexed position (1-based).
                c("strpos 2: multi-byte haystack and needle", "strpos", 2,
                        // X(1) 你(2) 好(3) Y(4). Find '好' → 3.
                        "SELECT trino_strpos('X你好Y', '好')", 3L),
                c("strpos 2: bare emoji inside ZWJ sequence", "strpos", 2,
                        // 'a👨‍👩‍👧b' — codepoints: a(1) 👨(2) ZWJ(3) 👩(4) ZWJ(5) 👧(6) b(7).
                        // strpos of bare '👨' is 2; engines that index by grapheme
                        // would report 2 too but for the wrong reason. The needle
                        // '👨' alone is a codepoint inside a different grapheme;
                        // both engines find it at codepoint 2.
                        "SELECT trino_strpos('a👨‍👩‍👧b', '👨')", 2L),
                // starts_with: multi-byte prefix.
                c("starts_with 2: 4-byte emoji prefix", "starts_with", 2,
                        "SELECT trino_starts_with('🐧🦆🐍', '🐧')", true),
                c("starts_with 2: bare man emoji prefixes ZWJ family", "starts_with", 2,
                        // '👨' is a codepoint prefix of '👨‍👩‍👧' (which starts with man + ZWJ + ...).
                        "SELECT trino_starts_with('👨‍👩‍👧 the family', '👨')", true),
                // trim: ZWJ cluster as content is left intact (only whitespace stripped).
                c("trim 1: ZWJ family content not stripped", "trim", 1,
                        "SELECT trino_trim('  👨‍👩‍👧  ')", "👨‍👩‍👧"),
                // replace: multi-byte search + replacement.
                c("replace 3: CJK to CJK", "replace", 3,
                        "SELECT trino_replace('a你b', '你', '好')", "a好b"),
                c("replace 3: 4-byte emoji replacement", "replace", 3,
                        // Replace bare man emoji INSIDE a ZWJ sequence — both engines
                        // do codepoint substring match, so the man codepoint at
                        // position 2 of the family gets swapped while the ZWJ +
                        // woman + ZWJ + girl tail remains. Result is malformed as a
                        // grapheme but the codepoint sequence is well-defined.
                        "SELECT trino_replace('🐧🦆🐧', '🐧', '🐍')", "🐍🦆🐍"),
                // lpad / rpad: target length is in codepoints; pad with a multi-byte char.
                c("lpad 3: pad to 5 codepoints with CJK pad char", "lpad", 3,
                        // 'abc' (3 cp) → pad with '你' (1 cp) until total = 5 cp → '你你abc'.
                        "SELECT trino_lpad('abc', 5, '你')", "你你abc"),
                c("rpad 3: pad with 4-byte emoji", "rpad", 3,
                        "SELECT trino_rpad('abc', 5, '🐧')", "abc🐧🐧"),
                // concat_ws: multi-byte separator.
                c("concat_ws 3: multi-byte separator", "concat_ws", 3,
                        "SELECT trino_concat_ws('🔗', 'a', 'b')", "a🔗b"),
                // translate: codepoint-by-codepoint translate-from / translate-to.
                c("translate 3: CJK to ASCII pairwise", "translate", 3,
                        // 你 → A, 好 → B (codepoint-wise); 世界 unchanged.
                        "SELECT trino_translate('你好世界', '你好', 'AB')", "AB世界"),
                // bit_length: codepoint × byte-width × 8 bits.
                c("bit_length 1: 4-byte emoji × 3 = 96 bits", "bit_length", 1,
                        "SELECT trino_bit_length('🐧🦆🐍')", 96L),
                c("bit_length 1: Cyrillic 2-byte × 7 = 112 bits", "bit_length", 1,
                        "SELECT trino_bit_length('пингвин')", 112L),
                c("bit_length 1: ZWJ family 18 bytes = 144 bits", "bit_length", 1,
                        // 4 (man) + 3 (ZWJ) + 4 (woman) + 3 (ZWJ) + 4 (girl) = 18 bytes.
                        "SELECT trino_bit_length('👨‍👩‍👧')", 144L),
                // levenshtein_distance: edit distance by codepoint (not byte, not grapheme).
                c("levenshtein_distance 2: one-emoji substitution", "levenshtein_distance", 2,
                        "SELECT trino_levenshtein_distance('🐧🦆🐍', '🐧🦉🐍')", 1L),
                // hamming_distance: codepoint-by-codepoint diff count.
                c("hamming_distance 2: one-emoji substitution", "hamming_distance", 2,
                        "SELECT trino_hamming_distance('🐧🦆🐍', '🐧🦉🐍')", 1L),
                // chr: codepoint above U+FFFF (requires supplementary-plane handling).
                c("chr 1: 4-byte emoji codepoint (penguin U+1F427)", "chr", 1,
                        "SELECT trino_chr(128039)", "🐧"),
                // Round 6j (step 4 chunk 1) — Tier A: DATE-only date functions
                //
                // day_of_week: Trino is 1=Mon..7=Sun (ISO). Sunday is the divergence
                // smoking gun — DuckDB's bare dayofweek() would return 0 here.
                c("day_of_week 1: Sunday → 7 (ISO, NOT 0)", "day_of_week", 1,
                        "SELECT trino_day_of_week(DATE '2024-01-07')", 7L),
                c("day_of_week 1: Monday → 1", "day_of_week", 1,
                        "SELECT trino_day_of_week(DATE '2024-01-08')", 1L),
                c("day_of_year 1: leap-day", "day_of_year", 1,
                        "SELECT trino_day_of_year(DATE '2024-02-29')", 60L),
                c("day_of_year 1: end of leap year (366)", "day_of_year", 1,
                        "SELECT trino_day_of_year(DATE '2024-12-31')", 366L),
                c("last_day_of_month 1: leap February", "last_day_of_month", 1,
                        "SELECT CAST(trino_last_day_of_month(DATE '2024-02-15') AS VARCHAR)",
                        "2024-02-29"),
                c("last_day_of_month 1: non-leap century February", "last_day_of_month", 1,
                        // 1900 is divisible by 100 but NOT 400 → not a leap year.
                        "SELECT CAST(trino_last_day_of_month(DATE '1900-02-15') AS VARCHAR)",
                        "1900-02-28"),
                // ISO week / year — calendar year ≠ ISO year on '2024-12-30' (Monday
                // of ISO week 1 of 2025). This is the classic "ISO year ≠ calendar
                // year" trap; if it ever flips back we've broken the alignment.
                c("week 1: ISO week 1 starts Monday", "week", 1,
                        "SELECT trino_week(DATE '2024-01-01')", 1L),
                c("week 1: 2024-12-30 → ISO week 1 of 2025", "week", 1,
                        "SELECT trino_week(DATE '2024-12-30')", 1L),
                c("week_of_year 1: alias of week", "week_of_year", 1,
                        "SELECT trino_week_of_year(DATE '2023-01-01')", 52L),
                c("week 1: ISO week 53 (only exists in ISO numbering)", "week", 1,
                        // 2021-01-01 is a Friday in ISO week 53 of 2020. Week 53
                        // only exists in years whose Jan 1 lands Thu (regular) or
                        // Wed/Thu (leap) — a clean ISO-vs-Gregorian smoking gun
                        // that a non-ISO `week()` would never produce.
                        "SELECT trino_week(DATE '2021-01-01')", 53L),
                c("year_of_week 1: ISO year ≠ calendar year (forward)", "year_of_week", 1,
                        // Calendar year 2024 but ISO year 2025 because the week
                        // containing this Monday extends into 2025. Trino spells
                        // this `year_of_week`; DuckDB exposes it via extract('isoyear').
                        "SELECT trino_year_of_week(DATE '2024-12-30')", 2025L),
                c("year_of_week 1: ISO year ≠ calendar year (backward)", "year_of_week", 1,
                        // 2021-01-01 is a Friday → ISO week 53 of 2020. Calendar
                        // year says 2021 but ISO year says 2020. Pairs with the
                        // forward case above so any regression that swapped
                        // year_of_week for year() shows up immediately.
                        "SELECT trino_year_of_week(DATE '2021-01-01')", 2020L),
                c("year_of_week 1: leading days of Jan belong to prior ISO year", "year_of_week", 1,
                        // 2023-01-01 is a Sunday → ISO week 52 of 2022.
                        "SELECT trino_year_of_week(DATE '2023-01-01')", 2022L),
                c("yow 1: short alias of year_of_week", "yow", 1,
                        "SELECT trino_yow(DATE '2024-12-30')", 2025L),
                c("yow 1: short alias on backward boundary", "yow", 1,
                        "SELECT trino_yow(DATE '2021-01-01')", 2020L),
                // Round 6j — Tier B: DATE or TIMESTAMP (no TZ)
                c("hour 1: from TIMESTAMP", "hour", 1,
                        "SELECT trino_hour(TIMESTAMP '2024-12-31 22:30:45')", 22L),
                c("minute 1: from TIMESTAMP", "minute", 1,
                        "SELECT trino_minute(TIMESTAMP '2024-12-31 22:30:45')", 30L),
                c("second 1: from TIMESTAMP", "second", 1,
                        "SELECT trino_second(TIMESTAMP '2024-12-31 22:30:45')", 45L),
                c("millisecond 1: millis-of-second (0..999)", "millisecond", 1,
                        // Trino's millisecond() returns the milliseconds component of
                        // the second, NOT total millis since epoch.
                        "SELECT trino_millisecond(TIMESTAMP '2024-06-15 12:00:00.123')", 123L),
                c("to_unixtime 1: epoch", "to_unixtime", 1,
                        "SELECT trino_to_unixtime(TIMESTAMP '1970-01-01 00:00:00')", 0.0),
                c("to_unixtime 1: one second before epoch", "to_unixtime", 1,
                        "SELECT trino_to_unixtime(TIMESTAMP '1969-12-31 23:59:59')", -1.0),
                // Step 4 chunk 4 — Tier C extras
                //
                // from_unixtime: Trino's `double → WTZ` round-trips through DuckDB's
                // to_timestamp(numeric). Verify by extracting epoch back from the result;
                // both engines produce zone-invariant epochs (the epoch IS the instant).
                c("from_unixtime 1: epoch 0 → instant", "from_unixtime", 1,
                        "SELECT epoch(trino_from_unixtime(0.0))", 0.0),
                c("from_unixtime 1: subsecond preserved", "from_unixtime", 1,
                        "SELECT epoch(trino_from_unixtime(1.234567))", 1.234567),
                c("from_unixtime 1: negative epoch (pre-1970)", "from_unixtime", 1,
                        "SELECT epoch(trino_from_unixtime(-1.0))", -1.0),
                c("from_unixtime 1: year-boundary instant", "from_unixtime", 1,
                        // 1735682400 = 2024-12-31 22:00:00 UTC — the year-boundary
                        // smoking-gun instant. epoch round-trip confirms the macro
                        // doesn't mangle large positive doubles.
                        "SELECT epoch(trino_from_unixtime(1735682400.0))", 1735682400.0),
                // with_timezone: Trino's `(timestamp, varchar) → WTZ` attaches the zone
                // to the wall-clock. DuckDB's `timezone(zone, ts)` does the same with
                // arg order flipped (the macro handles the swap). Verify the instant
                // by extracting epoch.
                c("with_timezone 2: UTC wall-clock attach", "with_timezone", 2,
                        // 2024-06-15 12:00:00 in UTC → epoch 1718452800
                        "SELECT epoch(trino_with_timezone(TIMESTAMP '2024-06-15 12:00:00', 'UTC'))",
                        1.7184528E9),
                c("with_timezone 2: Singapore wall-clock attach", "with_timezone", 2,
                        // 2024-06-15 12:00:00 wall-clock in Singapore (+08) is
                        // 2024-06-15 04:00:00 UTC → epoch 1718424000.
                        "SELECT epoch(trino_with_timezone(TIMESTAMP '2024-06-15 12:00:00', 'Asia/Singapore'))",
                        1.7184240E9),
                c("with_timezone 2: epoch wall-clock attach", "with_timezone", 2,
                        "SELECT epoch(trino_with_timezone(TIMESTAMP '1970-01-01 00:00:00', 'UTC'))",
                        0.0));
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
