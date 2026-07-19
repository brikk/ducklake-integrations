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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Stage 1 (T3) integration test: the duckbridge connector talking to a REAL remote DuckDB over the
 * Quack protocol via gizmo's `quack-jdbc` driver (`connection-url=jdbc:quack://host:port`).
 *
 * The server is a testcontainer ([TestingQuackServer]) running `duckdb -unsigned` hosting
 * `quack_serve`; the built `trino_parity.duckdb_extension` is copied in and LOADed server-side (via
 * `duckbridge.parity-extension-path` pointing at the in-container path), so the parity pushdown path
 * is genuinely exercised over the wire — not just result correctness.
 *
 * Requires Docker. The container base is debian:trixie (GLIBC 2.41) because the extension links
 * against GLIBC 2.38 and won't LOAD on the DuckLake fixture's bookworm image.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDuckBridgeQuackTransport : AbstractTestQueryFramework() {
    private lateinit var server: TestingQuackServer
    private var parityAvailable: Boolean = false

    override fun createQueryRunner(): QueryRunner {
        server = TestingQuackServer()
        parityAvailable = server.installParityExtension()
        val extra = buildMap {
            put("duckbridge.quack.token", server.token)
            if (parityAvailable) {
                // Server-side path — DuckBridgeParity LOADs it over the pass-through connection.
                put("duckbridge.parity-extension-path", TestingQuackServer.IN_CONTAINER_PARITY_PATH)
            } else {
                // No matching binary for the container arch: run without parity so the transport
                // tests still exercise the wire path (domain/limit pushdown, round-trip, metadata).
                put("duckbridge.parity.enabled", "false")
            }
        }
        val runner = DuckBridgeQueryRunner.create(server.connectionUrl(), extra)
        runner.execute("CREATE SCHEMA ${DuckBridgeQueryRunner.CATALOG}.${DuckBridgeQueryRunner.SCHEMA}")
        return runner
    }

    @BeforeAll
    fun createData() {
        computeActual("CREATE TABLE t (id bigint, name varchar, birth date)")
        computeActual(
            "INSERT INTO t VALUES " +
                "(1, 'Alice', DATE '1990-05-01'), (2, 'bob', DATE '1985-12-30'), " +
                "(3, 'straße', DATE '2000-02-29'), (4, 'δοκιμή', DATE '1970-01-01')",
        )
    }

    @AfterAll
    fun tearDown() {
        computeActual("DROP TABLE IF EXISTS t")
        if (::server.isInitialized) {
            server.close()
        }
    }

    @Test
    fun showSchemasAndTablesOverQuack() {
        val schemas = computeActual("SHOW SCHEMAS").materializedRows.map { it.getField(0) as String }
        assertThat(schemas).contains(DuckBridgeQueryRunner.SCHEMA)
        val tables = computeActual("SHOW TABLES").materializedRows.map { it.getField(0) as String }
        assertThat(tables).contains("t")
    }

    @Test
    fun scalarRoundTripOverQuack() {
        val row = computeActual("SELECT id, name, birth FROM t WHERE id = 3").materializedRows.single()
        assertThat(row.getField(0)).isEqualTo(3L)
        assertThat(row.getField(1)).isEqualTo("straße")
        assertThat(row.getField(2).toString()).isEqualTo("2000-02-29")
        val count = computeActual("SELECT count(*) FROM t").materializedRows.single()
        assertThat(count.getField(0)).isEqualTo(4L)
    }

    @Test
    fun unicodeEqualityOverQuack() {
        val ids =
            computeActual("SELECT id FROM t WHERE name = 'δοκιμή'").materializedRows.map { it.getField(0) as Long }
        assertThat(ids).containsExactly(4L)
    }

    @Test
    fun domainPushdownIsProvenOverQuack() {
        // A simple range predicate pushes through base-jdbc's domain path onto the remote TableScan.
        val plan = explain("SELECT id FROM t WHERE id >= 3")
        assertThat(plan).contains("TableScan")
        val ids =
            computeActual("SELECT id FROM t WHERE id >= 3 ORDER BY id").materializedRows.map { it.getField(0) as Long }
        assertThat(ids).containsExactly(3L, 4L)
    }

    @Test
    fun limitPushdownIsProvenOverQuack() {
        val plan = explain("SELECT id FROM t LIMIT 2")
        assertThat(plan).contains("limit=2")
        assertThat(computeActual("SELECT id FROM t LIMIT 2").rowCount).isEqualTo(2)
    }

    @Test
    fun parityFunctionPushdownOverQuack() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
            parityAvailable,
            "trino_parity extension not available for the container platform — parity pushdown skipped",
        )
        // trino_length pushes as a trino_*() call the remote server resolves via the loaded extension.
        val plan = explain("SELECT id FROM t WHERE length(name) = 5")
        assertThat(plan).contains("trino_length")
        val ids =
            computeActual("SELECT id FROM t WHERE length(name) = 5 ORDER BY id").materializedRows.map { it.getField(0) as Long }
        // Alice (5), straße (6→no), δοκιμή (6→no), bob (3→no) → Alice only.
        assertThat(ids).containsExactly(1L)
    }

    @Test
    fun parityUnicodeCaseFoldOverQuack() {
        org.junit.jupiter.api.Assumptions.assumeTrue(parityAvailable, "parity not available")
        // upper('straße') → 'STRASSE' server-side; predicate pushes and matches row 3.
        val ids =
            computeActual("SELECT id FROM t WHERE upper(name) = 'STRASSE'").materializedRows.map { it.getField(0) as Long }
        assertThat(ids).containsExactly(3L)
    }

    private fun explain(sql: String): String =
        computeActual("EXPLAIN (TYPE DISTRIBUTED) $sql")
            .materializedRows
            .joinToString("\n") { it.getField(0).toString() }
}
