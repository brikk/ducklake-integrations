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
package dev.brikk.duckbridge.trino.plugin

import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Unicode round-trip + pushdown fidelity through the duckbridge catalog on plain DuckDB tables
 * (ported from the DuckLake connector's `TestDucklakeUnicodeStringRoundTrip`; rewritten against
 * base-jdbc tables, semantic case matrix preserved). The "nasty strings" exercise multibyte UTF-8,
 * combining marks, supplementary-plane emoji, regional-indicator flags, and ZWJ sequences — if any
 * mutate on WRITE→READ or through a pushed predicate, the transport/predicate encoding is broken.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDuckBridgeUnicodeStringRoundTrip : AbstractTestQueryFramework() {
    override fun createQueryRunner(): QueryRunner {
        val runner = DuckBridgeQueryRunner.create(DuckBridgeQueryRunner.freshDatabaseUrl())
        runner.execute("CREATE SCHEMA ${DuckBridgeQueryRunner.CATALOG}.${DuckBridgeQueryRunner.SCHEMA}")
        return runner
    }

    @Test
    fun writeReadRoundTripPreservesNastyStrings() {
        computeActual("CREATE TABLE unicode_round_trip (id integer, s varchar)")
        try {
            computeActual(
                "INSERT INTO unicode_round_trip VALUES " +
                    "(1, '$ASCII'), (2, '$CYRILLIC'), (3, '$CJK'), (4, '$EMOJI'), " +
                    "(5, '$COMBINING'), (6, '$FLAG'), (7, '$ZWJ_FAMILY')",
            )
            val rows = computeActual("SELECT id, s FROM unicode_round_trip ORDER BY id")
            assertThat(rows.rowCount).isEqualTo(7)
            assertThat(rows.materializedRows[0].getField(1)).isEqualTo(ASCII)
            assertThat(rows.materializedRows[1].getField(1)).isEqualTo(CYRILLIC)
            assertThat(rows.materializedRows[2].getField(1)).isEqualTo(CJK)
            assertThat(rows.materializedRows[3].getField(1)).isEqualTo(EMOJI)
            assertThat(rows.materializedRows[4].getField(1)).isEqualTo(COMBINING)
            assertThat(rows.materializedRows[5].getField(1)).isEqualTo(FLAG)
            assertThat(rows.materializedRows[6].getField(1)).isEqualTo(ZWJ_FAMILY)
        } finally {
            computeActual("DROP TABLE IF EXISTS unicode_round_trip")
        }
    }

    @Test
    fun equalityAndInPushdownPreserveNastyStringConstants() {
        computeActual("CREATE TABLE unicode_pred (id integer, s varchar)")
        try {
            computeActual(
                "INSERT INTO unicode_pred VALUES (1, '$ASCII'), (2, '$ZWJ_FAMILY'), (3, '$CJK'), (4, '$FLAG')",
            )
            val eqZwj = computeActual("SELECT id FROM unicode_pred WHERE s = '$ZWJ_FAMILY'")
            assertThat(eqZwj.rowCount).isEqualTo(1)
            assertThat(eqZwj.materializedRows.first().getField(0)).isEqualTo(2)

            val inList = computeActual("SELECT id FROM unicode_pred WHERE s IN ('$CJK', '$FLAG') ORDER BY id")
            assertThat(inList.rowCount).isEqualTo(2)
            assertThat(inList.materializedRows[0].getField(0)).isEqualTo(3)
            assertThat(inList.materializedRows[1].getField(0)).isEqualTo(4)
        } finally {
            computeActual("DROP TABLE IF EXISTS unicode_pred")
        }
    }

    @Test
    fun likePushdownPreservesMultiByteWildcardPatterns() {
        computeActual("CREATE TABLE unicode_like (id integer, s varchar)")
        try {
            computeActual(
                "INSERT INTO unicode_like VALUES (1, '$CYRILLIC walks'), (2, '$CJK greets'), (3, 'plain ASCII row')",
            )
            val cjkPrefix = computeActual("SELECT id FROM unicode_like WHERE s LIKE '$CJK%'")
            assertThat(cjkPrefix.rowCount).isEqualTo(1)
            assertThat(cjkPrefix.materializedRows.first().getField(0)).isEqualTo(2)
        } finally {
            computeActual("DROP TABLE IF EXISTS unicode_like")
        }
    }

    @Test
    fun functionShapePushdownOverUnicodeColumnData() {
        computeActual("CREATE TABLE unicode_fn (id integer, s varchar)")
        try {
            computeActual(
                "INSERT INTO unicode_fn VALUES (1, '$CYRILLIC'), (2, '$CJK'), (3, '$ZWJ_FAMILY'), (4, '$EMOJI')",
            )
            // length() counts code points in both engines: CYRILLIC=7, CJK=2, ZWJ_FAMILY=5, EMOJI=3.
            val len7 = computeActual("SELECT id FROM unicode_fn WHERE length(s) = 7")
            assertThat(len7.materializedRows.map { it.getField(0) }).containsExactly(1)
            val len5 = computeActual("SELECT id FROM unicode_fn WHERE length(s) = 5")
            assertThat(len5.materializedRows.map { it.getField(0) }).containsExactly(3)
            // starts_with over a multibyte prefix.
            val startsCjk = computeActual("SELECT id FROM unicode_fn WHERE starts_with(s, '世')")
            assertThat(startsCjk.materializedRows.map { it.getField(0) }).containsExactly(2)
        } finally {
            computeActual("DROP TABLE IF EXISTS unicode_fn")
        }
    }

    private companion object {
        const val ASCII = "hello world"
        const val CYRILLIC = "пингвин"
        const val CJK = "世界"
        const val EMOJI = "🐧🦆🐍"
        const val COMBINING = "cafe\u0301"
        const val FLAG = "\uD83C\uDDFA\uD83C\uDDF8"
        const val ZWJ_FAMILY = "\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67"
    }
}
