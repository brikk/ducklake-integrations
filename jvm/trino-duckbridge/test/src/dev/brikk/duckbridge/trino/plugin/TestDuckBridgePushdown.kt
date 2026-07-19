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
import io.trino.sql.planner.plan.LimitNode
import io.trino.sql.planner.plan.TopNNode
import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Provable pushdown tests: assert the PLAN SHAPE (via `isFullyPushedDown` / the plan matchers),
 * not just result correctness, so a regression that silently stops pushing predicates/limits is a
 * RED test. Runs the whole thing against a real embedded DuckDB with the built trino_parity
 * extension LOADed (parity enabled by default).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDuckBridgePushdown : AbstractTestQueryFramework() {
    override fun createQueryRunner(): QueryRunner {
        val runner = DuckBridgeQueryRunner.create(DuckBridgeQueryRunner.freshDatabaseUrl())
        runner.execute("CREATE SCHEMA ${DuckBridgeQueryRunner.CATALOG}.${DuckBridgeQueryRunner.SCHEMA}")
        return runner
    }

    @BeforeAll
    fun setUpData() {
        computeActual(
            """
            CREATE TABLE people (id bigint, name varchar, birth date, weight double)
            """.trimIndent(),
        )
        computeActual(
            """
            INSERT INTO people VALUES
                (1, 'Alice',   DATE '1990-05-01', 60.5),
                (2, 'bob',     DATE '1985-12-30', 80.0),
                (3, 'CAROL',   DATE '2000-01-15', 55.25),
                (4, 'straße',  DATE '1970-01-01', 72.0),
                (5, 'δοκιμή',  DATE '2024-02-29', 90.9)
            """.trimIndent(),
        )
    }

    @AfterAll
    fun tearDownData() {
        computeActual("DROP TABLE IF EXISTS people")
    }

    // ---- Function-shape (parity) pushdown ----------------------------------

    @Test
    fun lengthPredicateIsFullyPushedDown() {
        // trino_length(name) is a parity function → the whole filter pushes into DuckDB.
        assertThat(query("SELECT id FROM people WHERE length(name) = 5"))
            .isFullyPushedDown()
    }

    @Test
    fun upperPredicateIsFullyPushedDown() {
        assertThat(query("SELECT id FROM people WHERE upper(name) = 'BOB'"))
            .isFullyPushedDown()
    }

    @Test
    fun yearOnDatePredicateIsFullyPushedDown() {
        assertThat(query("SELECT id FROM people WHERE year(birth) = 2000"))
            .isFullyPushedDown()
    }

    @Test
    fun partialPushdownLeavesUnsupportedConjunctAboveScan() {
        // length(name)=5 pushes; the divide predicate is intentionally NOT pushed (Trino/DuckDB
        // integer-division divergence), so a FilterNode must remain above the scan. This proves the
        // per-conjunct partial-pushdown split works end to end.
        assertThat(query("SELECT id FROM people WHERE length(name) = 5 AND id / 2 = 1"))
            .isNotFullyPushedDown(FilterNode::class.java)
    }

    @Test
    fun unsupportedFunctionPredicateStaysAboveScan() {
        // No parity function for `to_iso8601` → filter remains in Trino.
        assertThat(query("SELECT id FROM people WHERE to_iso8601(birth) = '2000-01-15'"))
            .isNotFullyPushedDown(FilterNode::class.java)
    }

    // ---- LIMIT / TopN pushdown ---------------------------------------------
    //
    // LIMIT is pushed AND guaranteed (a remote `LIMIT n` returns at most n rows, which is Trino's
    // whole LIMIT contract — determinism is not required), so no LimitNode survives above the scan.
    // TopN is pushed but NOT guaranteed (DuckDB's ordering — e.g. varchar collation — is not proven
    // identical to Trino's), so Trino keeps its own TopN on top while still pushing the bound into
    // the remote scan; we prove that push by asserting `limit=`/`sortOrder=` on the TableScan.

    @Test
    fun limitIsPushedIntoTableScan() {
        val plan = explain("SELECT id FROM people LIMIT 2")
        assertThat(plan)
            .`as`("DuckDB LIMIT should be pushed onto the TableScan handle")
            .contains("limit=2")
        assertThat(plan)
            .`as`("guaranteed LIMIT pushdown should remove Trino's own Limit node")
            .doesNotContain("Limit[")
    }

    @Test
    fun topNIsPushedIntoTableScan() {
        val plan = explain("SELECT id FROM people ORDER BY weight DESC LIMIT 3")
        assertThat(plan)
            .`as`("ORDER BY ... LIMIT should push sortOrder + limit onto the TableScan handle")
            .contains("limit=3")
        assertThat(plan).contains("sortOrder=[weight")
    }

    private fun explain(sql: String): String =
        computeActual("EXPLAIN (TYPE DISTRIBUTED) $sql")
            .materializedRows
            .joinToString("\n") { it.getField(0).toString() }

    // ---- Correctness alongside the plan shape ------------------------------

    @Test
    fun pushedPredicateReturnsCorrectRows() {
        // Alice(5), CAROL(5) are length 5; straße(6), δοκιμή(6) are not.
        val ids =
            computeActual("SELECT id FROM people WHERE length(name) = 5 ORDER BY id")
                .materializedRows
                .map { it.getField(0) as Long }
        assertThat(ids).containsExactly(1L, 3L)
    }

    @Test
    fun pushedTopNReturnsCorrectOrder() {
        val names =
            computeActual("SELECT name FROM people ORDER BY weight DESC LIMIT 2")
                .materializedRows
                .map { it.getField(0) as String }
        assertThat(names).containsExactly("δοκιμή", "bob")
    }

    // Reference the imported plan-node classes so a stale import is a compile error, keeping the
    // matcher classes discoverable for future TopN/Limit-specific plan assertions.
    @Suppress("unused")
    private val planNodeClasses = listOf(LimitNode::class.java, TopNNode::class.java)
}
