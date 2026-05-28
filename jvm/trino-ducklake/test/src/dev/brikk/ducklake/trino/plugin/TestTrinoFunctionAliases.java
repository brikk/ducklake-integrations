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
                c("lower 1: case fold", "lower", 1,
                        "SELECT trino_lower('HeLLo')", "hello"),
                c("upper 1: case fold", "upper", 1,
                        "SELECT trino_upper('HeLLo')", "HELLO"),
                c("length 1: code points", "length", 1,
                        "SELECT trino_length('abcde')", 5L),
                c("length 1: multibyte", "length", 1,
                        // Cyrillic 5 code points; Trino & DuckDB both code-point counts.
                        "SELECT trino_length('пингвин')", 7L),
                c("reverse 1", "reverse", 1,
                        "SELECT trino_reverse('abc')", "cba"),
                c("trim 1", "trim", 1,
                        "SELECT trino_trim('  hi  ')", "hi"),
                c("ltrim 1", "ltrim", 1,
                        "SELECT trino_ltrim('  hi  ')", "hi  "),
                c("rtrim 1", "rtrim", 1,
                        "SELECT trino_rtrim('  hi  ')", "  hi"),
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
                        "SELECT trino_power(2, 10)", 1024.0));
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
