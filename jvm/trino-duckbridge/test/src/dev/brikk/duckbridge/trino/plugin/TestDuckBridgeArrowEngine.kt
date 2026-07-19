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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * End-to-end test of the T2 DUCKDB_LOCAL Arrow data plane through a Trino QueryRunner
 * (`duckbridge.execution-engine=DUCKDB_LOCAL`): [DuckBridgePageSourceProvider] diverts scans to the
 * in-process executor + [DuckBridgeArrowToPageConverter] instead of the default JDBC record set.
 * Proves the channel with real queries — projection, predicate pushdown, and unicode fidelity all
 * come back through the Arrow path.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDuckBridgeArrowEngine : AbstractTestQueryFramework() {
    override fun createQueryRunner(): QueryRunner {
        val runner =
            DuckBridgeQueryRunner.create(
                DuckBridgeQueryRunner.freshDatabaseUrl(),
                mapOf("duckbridge.execution-engine" to "DUCKDB_LOCAL"),
            )
        runner.execute("CREATE SCHEMA ${DuckBridgeQueryRunner.CATALOG}.${DuckBridgeQueryRunner.SCHEMA}")
        return runner
    }

    @BeforeAll
    fun createData() {
        computeActual("CREATE TABLE nums (id bigint, name varchar, d double)")
        computeActual(
            "INSERT INTO nums VALUES " +
                "(1, 'alpha', 1.5), (2, 'béta', 2.5), (3, 'γάμμα', 3.5), (4, 'straße', 4.5)",
        )
    }

    @Test
    fun fullScanThroughArrowEngine() {
        val rows = computeActual("SELECT id, name, d FROM nums ORDER BY id").materializedRows
        assertThat(rows.map { it.getField(0) as Long }).containsExactly(1L, 2L, 3L, 4L)
        assertThat(rows.map { it.getField(1) as String }).containsExactly("alpha", "béta", "γάμμα", "straße")
        assertThat(rows.map { it.getField(2) as Double }).containsExactly(1.5, 2.5, 3.5, 4.5)
    }

    @Test
    fun projectionThroughArrowEngine() {
        val names = computeActual("SELECT name FROM nums ORDER BY id").materializedRows.map { it.getField(0) as String }
        assertThat(names).containsExactly("alpha", "béta", "γάμμα", "straße")
    }

    @Test
    fun domainPredicatePushdownThroughArrowEngine() {
        val ids =
            computeActual("SELECT id FROM nums WHERE id >= 3 ORDER BY id").materializedRows.map { it.getField(0) as Long }
        assertThat(ids).containsExactly(3L, 4L)
    }

    @Test
    fun parityPredicatePushdownThroughArrowEngine() {
        // upper(name) pushes as trino_upper server-side (the executor LOADs the extension); the Arrow
        // result comes back through the T2 page source.
        val ids =
            computeActual("SELECT id FROM nums WHERE upper(name) = 'STRASSE' ORDER BY id")
                .materializedRows
                .map { it.getField(0) as Long }
        assertThat(ids).containsExactly(4L)
    }

    @Test
    fun countThroughArrowEngine() {
        assertThat(computeActual("SELECT count(*) FROM nums").materializedRows.single().getField(0)).isEqualTo(4L)
    }
}
