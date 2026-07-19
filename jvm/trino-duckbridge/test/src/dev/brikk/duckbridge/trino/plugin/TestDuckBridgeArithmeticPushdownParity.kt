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

import io.trino.sql.planner.plan.FilterNode
import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Trino ↔ DuckDB arithmetic-parity tests, ported from the DuckLake connector's
 * `TestDucklakeArithmeticPushdownParity`. The DuckLake version proved that pushing bare `/` and `%`
 * onto the `.db` read path silently diverges (integer truncation + divide-by-zero suppression).
 *
 * In this base-jdbc connector the translator refuses `$divide`/`$modulo` outright, so the divergence
 * cannot occur: the predicate stays above the scan (a FilterNode remains) and Trino evaluates it
 * natively — including throwing DIVISION_BY_ZERO. These tests pin BOTH the plan shape (not pushed)
 * AND the end-to-end semantics, so a future change that started pushing divide/modulo would go RED.
 *
 * Additions (`+`, `-`, `*`) DO push; a companion assertion pins that they push fully.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDuckBridgeArithmeticPushdownParity : AbstractTestQueryFramework() {
    override fun createQueryRunner(): QueryRunner {
        val runner = DuckBridgeQueryRunner.create(DuckBridgeQueryRunner.freshDatabaseUrl())
        runner.execute("CREATE SCHEMA ${DuckBridgeQueryRunner.CATALOG}.${DuckBridgeQueryRunner.SCHEMA}")
        return runner
    }

    @Test
    fun additionPredicatePushesFully() {
        withTable("add_push", "SELECT * FROM (VALUES (4), (5), (6)) AS t(id)") {
            assertThat(query("SELECT id FROM add_push WHERE id + 1 = 6")).isFullyPushedDown()
        }
    }

    @Test
    fun integerDividePredicateStaysAboveScanAndMatchesTrino() {
        withTable("div_trunc", "SELECT * FROM (VALUES (4), (5), (3), (6)) AS t(id)") {
            // Not pushed → FilterNode remains; Trino evaluates `id / 2 = 2` with truncating
            // integer division (4/2=2, 5/2=2), returning 4 and 5.
            assertThat(query("SELECT id FROM div_trunc WHERE id / 2 = 2"))
                .isNotFullyPushedDown(FilterNode::class.java)
            val ids =
                computeActual("SELECT id FROM div_trunc WHERE id / 2 = 2 ORDER BY id")
                    .materializedRows
                    .map { it.getField(0) as Int }
            assertThat(ids).containsExactly(4, 5)
        }
    }

    @Test
    fun integerDivideByZeroThrowsEndToEnd() {
        withTable("div_zero", "SELECT * FROM (VALUES (10, 2), (20, 0), (30, 5)) AS t(id, divisor)") {
            val thrown =
                catchThrowable {
                    drain("SELECT id FROM div_zero WHERE id / divisor = 5")
                }
            assertThat(thrown).`as`("Trino integer divide-by-zero must throw end to end").isNotNull()
            assertThat(thrown.message?.lowercase().orEmpty())
                .containsAnyOf("division by zero", "divide by zero", "/ by zero")
        }
    }

    @Test
    fun integerModuloMatchesTrinoSemantics() {
        withTable("mod_basic", "SELECT * FROM (VALUES (10), (11), (12), (13)) AS t(id)") {
            assertThat(query("SELECT id FROM mod_basic WHERE id % 3 = 1"))
                .isNotFullyPushedDown(FilterNode::class.java)
            val ids =
                computeActual("SELECT id FROM mod_basic WHERE id % 3 = 1 ORDER BY id")
                    .materializedRows
                    .map { it.getField(0) as Int }
            assertThat(ids).containsExactly(10, 13)
        }
    }

    @Test
    fun integerModuloByZeroThrowsEndToEnd() {
        withTable("mod_zero", "SELECT * FROM (VALUES (10, 2), (20, 0), (30, 5)) AS t(id, divisor)") {
            val thrown =
                catchThrowable {
                    drain("SELECT id FROM mod_zero WHERE id % divisor = 0")
                }
            assertThat(thrown).`as`("Trino integer modulo-by-zero must throw end to end").isNotNull()
            assertThat(thrown.message?.lowercase().orEmpty())
                .containsAnyOf("division by zero", "divide by zero", "/ by zero")
        }
    }

    private fun drain(sql: String) {
        val rows = computeActual(sql).materializedRows
        for (row in rows) {
            for (i in 0 until row.fieldCount) {
                row.getField(i)
            }
        }
    }

    private fun withTable(name: String, asSelect: String, body: () -> Unit) {
        computeActual("CREATE TABLE $name AS $asSelect")
        try {
            body()
        } finally {
            computeActual("DROP TABLE IF EXISTS $name")
        }
    }
}
