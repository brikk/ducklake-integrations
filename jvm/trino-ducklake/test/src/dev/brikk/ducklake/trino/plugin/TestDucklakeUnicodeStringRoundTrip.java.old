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

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Canary: round-trip + pushdown over VARCHAR columns containing
 * &quot;nasty&quot; Unicode strings, asserting byte-perfect preservation across
 * the full Trino → connector → DuckDB → Arrow → connector → Trino loop.
 *
 * <p>Companion to {@code TestTrinoFunctionAliases}'s Unicode pressure
 * fixtures, which probe each string macro's semantics in isolation. This
 * suite catches regressions in the *transport* layer: Arrow vector
 * encoding, page conversion, the SQL builder's quoting, and pushdown
 * predicate translation when the predicate or column data contains
 * codepoints above the ASCII range.
 *
 * <p>Reference strings (the &quot;nasties&quot;):
 * <ul>
 *   <li>{@code Cyrillic} — 2-byte UTF-8 per codepoint.</li>
 *   <li>{@code CJK} — 3-byte UTF-8 per codepoint.</li>
 *   <li>{@code Emoji} — 4-byte UTF-8 per codepoint (above U+FFFF; supplementary plane).</li>
 *   <li>{@code Combining} — {@code 'café'} as {@code 'cafe' + U+0301}: 5 codepoints, 4 graphemes.</li>
 *   <li>{@code Flag} — {@code '🇺🇸'} (regional indicators U+1F1FA U+1F1F8): 2 codepoints, 1 grapheme.</li>
 *   <li>{@code ZwjFamily} — {@code '👨‍👩‍👧'}: 5 codepoints (man, ZWJ, woman, ZWJ, girl), 18 bytes, 1 grapheme.</li>
 * </ul>
 *
 * <p>If any of these mutate on round-trip, the regression is in the
 * connector's transport — NOT in the engines' string semantics, which the
 * macro fixtures already pin.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeUnicodeStringRoundTrip
        extends AbstractDucklakeIntegrationTest
{
    private static final String ASCII =      "Apple";
    private static final String CYRILLIC =   "пингвин";        // 7 cp, 14 bytes
    private static final String CJK =        "你好世界";        // 4 cp, 12 bytes
    private static final String EMOJI =      "🐧🦆🐍";          // 3 cp, 12 bytes
    private static final String COMBINING =  "café";     // 5 cp, 6 bytes (4 graphemes)
    private static final String FLAG =       "🇺🇸"; // 🇺🇸 — 2 cp, 8 bytes
    private static final String ZWJ_FAMILY = "👨‍👩‍👧"; // 👨‍👩‍👧 — 5 cp, 18 bytes

    @Override
    protected String isolatedCatalogName()
    {
        return "unicode-roundtrip";
    }

    /**
     * Pure transport sanity: WRITE → READ must preserve every codepoint of every
     * nasty string with no normalisation, no truncation, no surrogate-pair
     * mishandling. This is the load-bearing test — every subsequent assertion
     * relies on the column data being byte-faithful.
     */
    @Test
    public void testWriteReadRoundTripPreservesNastyStrings()
    {
        computeActual("CREATE TABLE test_schema.unicode_round_trip (id INTEGER, s VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.unicode_round_trip VALUES "
                    + "(1, '" + ASCII + "'), "
                    + "(2, '" + CYRILLIC + "'), "
                    + "(3, '" + CJK + "'), "
                    + "(4, '" + EMOJI + "'), "
                    + "(5, '" + COMBINING + "'), "
                    + "(6, '" + FLAG + "'), "
                    + "(7, '" + ZWJ_FAMILY + "')");

            MaterializedResult rows = computeActual(
                    "SELECT id, s FROM test_schema.unicode_round_trip ORDER BY id");
            assertThat(rows.getRowCount()).isEqualTo(7);
            assertThat(rows.getMaterializedRows().get(0).getField(1)).isEqualTo(ASCII);
            assertThat(rows.getMaterializedRows().get(1).getField(1)).isEqualTo(CYRILLIC);
            assertThat(rows.getMaterializedRows().get(2).getField(1)).isEqualTo(CJK);
            assertThat(rows.getMaterializedRows().get(3).getField(1)).isEqualTo(EMOJI);
            assertThat(rows.getMaterializedRows().get(4).getField(1)).isEqualTo(COMBINING);
            assertThat(rows.getMaterializedRows().get(5).getField(1)).isEqualTo(FLAG);
            assertThat(rows.getMaterializedRows().get(6).getField(1)).isEqualTo(ZWJ_FAMILY);
        }
        finally {
            tryDropTable("test_schema.unicode_round_trip");
        }
    }

    /**
     * Equality and IN-list pushdown over Unicode VARCHAR values: the connector
     * must encode the predicate constants byte-faithfully into the WHERE clause
     * (or TupleDomain) and DuckDB must match the same row that Trino's above-scan
     * eval would. ZWJ family is the strongest case — 18 bytes containing two
     * U+200D codepoints that the SQL builder's single-quote escaper must not
     * corrupt.
     */
    @Test
    public void testEqualityAndInPushdownPreserveNastyStringConstants()
    {
        computeActual("CREATE TABLE test_schema.unicode_pred (id INTEGER, s VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.unicode_pred VALUES "
                    + "(1, '" + ASCII + "'), "
                    + "(2, '" + ZWJ_FAMILY + "'), "
                    + "(3, '" + CJK + "'), "
                    + "(4, '" + FLAG + "')");

            // Equality on the ZWJ family — exact byte match required.
            MaterializedResult eqZwj = computeActual(
                    "SELECT id FROM test_schema.unicode_pred WHERE s = '" + ZWJ_FAMILY + "'");
            assertThat(eqZwj.getRowCount()).isEqualTo(1);
            assertThat(eqZwj.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);

            // IN-list over CJK + flag — both multi-codepoint, neither ASCII.
            MaterializedResult in = computeActual(
                    "SELECT id FROM test_schema.unicode_pred "
                            + "WHERE s IN ('" + CJK + "', '" + FLAG + "') ORDER BY id");
            assertThat(in.getRowCount()).isEqualTo(2);
            assertThat(in.getMaterializedRows().get(0).getField(0)).isEqualTo(3);
            assertThat(in.getMaterializedRows().get(1).getField(0)).isEqualTo(4);
        }
        finally {
            tryDropTable("test_schema.unicode_pred");
        }
    }

    /**
     * LIKE pushdown (chunk 1 of the pushdown program) with Unicode patterns.
     * Both engines treat LIKE wildcards ({@code %}, {@code _}) as codepoint-level,
     * so a pattern containing multi-byte characters matches against the column's
     * codepoint sequence — not bytes. This test catches a regression where the
     * connector escaped a non-ASCII pattern incorrectly, or where the LIKE branch
     * mis-encoded the pattern constant.
     */
    @Test
    public void testLikePushdownPreservesMultiByteWildcardPatterns()
    {
        computeActual("CREATE TABLE test_schema.unicode_like (id INTEGER, s VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.unicode_like VALUES "
                    + "(1, '" + CYRILLIC + " walks'), "
                    + "(2, '" + CJK + " greets'), "
                    + "(3, 'plain ASCII row')");

            // CJK prefix wildcard.
            MaterializedResult cjkPrefix = computeActual(
                    "SELECT id FROM test_schema.unicode_like WHERE s LIKE '" + CJK + "%'");
            assertThat(cjkPrefix.getRowCount()).isEqualTo(1);
            assertThat(cjkPrefix.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);

            // Cyrillic _ (single codepoint) wildcard — pattern matches strings where
            // a single codepoint follows the first 6 codepoints of CYRILLIC.
            MaterializedResult cyrillicUnderscore = computeActual(
                    "SELECT id FROM test_schema.unicode_like "
                            + "WHERE s LIKE '" + CYRILLIC.substring(0, CYRILLIC.length() - 1) + "_ walks'");
            assertThat(cyrillicUnderscore.getRowCount()).isEqualTo(1);
            assertThat(cyrillicUnderscore.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.unicode_like");
        }
    }

    /**
     * Function-shape pushdown over Unicode column data: {@code length()},
     * {@code starts_with()}, {@code strpos()}. Pins that DuckDB's server-side
     * eval and Trino's above-scan eval agree on codepoint counts for the column
     * values — both engines count codepoints (verified at the macro layer by
     * {@code TestTrinoFunctionAliases}; here we prove it through the full read
     * path against actual column data).
     */
    @Test
    public void testCodepointFunctionPushdownOverUnicodeColumn()
    {
        computeActual("CREATE TABLE test_schema.unicode_fn (id INTEGER, s VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.unicode_fn VALUES "
                    + "(1, '" + ASCII + "'), "       // 5 cp
                    + "(2, '" + CYRILLIC + "'), "    // 7 cp
                    + "(3, '" + ZWJ_FAMILY + "')");  // 5 cp (man + ZWJ + woman + ZWJ + girl)

            // length(): codepoint count, same in both engines.
            MaterializedResult len5 = computeActual(
                    "SELECT id FROM test_schema.unicode_fn WHERE length(s) = 5 ORDER BY id");
            // ASCII 'Apple' (5 cp) AND ZWJ family (5 cp) — BOTH have length 5. A
            // grapheme-counting engine would report 1 for the ZWJ family and miss
            // this row.
            assertThat(len5.getRowCount()).isEqualTo(2);
            assertThat(len5.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(len5.getMaterializedRows().get(1).getField(0)).isEqualTo(3);

            // starts_with on a multi-byte prefix.
            MaterializedResult startsCyrillic = computeActual(
                    "SELECT id FROM test_schema.unicode_fn "
                            + "WHERE starts_with(s, '" + CYRILLIC.substring(0, 3) + "')");
            assertThat(startsCyrillic.getRowCount()).isEqualTo(1);
            assertThat(startsCyrillic.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);

            // strpos: codepoint-indexed position of bare man emoji inside the ZWJ family.
            // ZWJ family codepoints: man(1) ZWJ(2) woman(3) ZWJ(4) girl(5).
            MaterializedResult strposMan = computeActual(
                    "SELECT id FROM test_schema.unicode_fn "
                            + "WHERE strpos(s, '👨') = 1");
            assertThat(strposMan.getRowCount()).isEqualTo(1);
            assertThat(strposMan.getMaterializedRows().getFirst().getField(0)).isEqualTo(3);
        }
        finally {
            tryDropTable("test_schema.unicode_fn");
        }
    }
}
