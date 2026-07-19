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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.util.Properties

/**
 * SQL-level acceptance + drift tests for the `trino_*` parity layer, run against the REAL built
 * `trino_parity.duckdb_extension` binary (resolved and extracted by [TrinoParityExtensionResolver],
 * the same path the production client uses). Ported from the DuckLake connector's
 * `TestTrinoFunctionAliases`, trimmed to the drift-critical checks plus a representative semantic
 * sample; the full 200-fixture semantic matrix stays in the extension's own test suite.
 *
 * [testJavaPushableSetMatchesDuckDbMeta] is THE DRIFT TEST: the Java-side
 * [DuckBridgeExpressionTranslator.PUSHABLE_FUNCTIONS] must equal the DuckDB-side `trino_meta()`
 * catalog of the loaded binary, exactly.
 */
class TestTrinoFunctionAliases {
    @Test
    @Throws(Exception::class)
    fun testJavaPushableSetMatchesDuckDbMeta() {
        openConnectionWithExtension().use { conn ->
            conn.createStatement().use { stmt ->
                val duckDbMeta = readMeta(stmt)
                val javaAsNameArity =
                    DuckBridgeExpressionTranslator.PUSHABLE_FUNCTIONS
                        .map { NameArity(it.name, it.arity) }
                        .toHashSet()
                assertThat(javaAsNameArity)
                    .`as`("DuckBridgeExpressionTranslator.PUSHABLE_FUNCTIONS vs trino_meta() drifted")
                    .isEqualTo(duckDbMeta)
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTrinoMetaCatalogMatchesInstalledMacros() {
        openConnectionWithExtension().use { conn ->
            conn.createStatement().use { stmt ->
                val meta = readMeta(stmt)
                val installed = installedTrinoFunctions(stmt)
                for (entry in meta) {
                    assertThat(installed)
                        .`as`("trino_%s must be registered (declared in trino_meta with arity %d)", entry.name, entry.arity)
                        .contains("trino_" + entry.name)
                }
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testRepresentativeMacroSemantics() {
        openConnectionWithExtension().use { conn ->
            conn.createStatement().use { stmt ->
                // A representative slice of the semantic matrix — enough to prove the loaded binary
                // actually evaluates the aligned semantics (ISO week/day, ß expansion, code-point
                // length, NULL propagation), not just that the names exist.
                assertThat(scalar(stmt, "SELECT trino_lower('HeLLo')")).isEqualTo("hello")
                assertThat(scalar(stmt, "SELECT trino_upper('ß')")).isEqualTo("SS")
                assertThat(numeric(stmt, "SELECT trino_length('пингвин')")).isEqualTo(7.0)
                assertThat(numeric(stmt, "SELECT trino_day_of_week(DATE '2024-01-07')")).isEqualTo(7.0)
                assertThat(numeric(stmt, "SELECT trino_year_of_week(DATE '2024-12-30')")).isEqualTo(2025.0)
                assertThat(numeric(stmt, "SELECT trino_week(DATE '2021-01-01')")).isEqualTo(53.0)
                assertThat(scalar(stmt, "SELECT trino_lower(NULL)")).isNull()
                assertThat(scalar(stmt, "SELECT trino_abs(CAST(NULL AS INTEGER))")).isNull()
            }
        }
    }

    private data class NameArity(val name: String, val arity: Int)

    companion object {
        /**
         * Opens an in-memory DuckDB with the built trino_parity extension LOADed via the production
         * resolver + loader. Fails fast if the bundled extension is missing on the test classpath —
         * the extension is now built, so a miss is a real failure, not an environment quirk.
         */
        @Throws(SQLException::class)
        private fun openConnectionWithExtension(): Connection {
            val path =
                TrinoParityExtensionResolver.resolveBundledExtensionPath()
                    ?: throw AssertionError(
                        "trino_parity extension not bundled in plugin jar on this platform — build it first: " +
                            "`(cd duckdb-trino-parity-extension && make)`.",
                    )
            val props = Properties()
            props.setProperty("allow_unsigned_extensions", "true")
            val conn = DriverManager.getConnection("jdbc:duckdb:", props)
            TrinoFunctionAliases.loadInProcess(conn, path)
            return conn
        }

        @Throws(SQLException::class)
        private fun readMeta(stmt: Statement): Set<NameArity> {
            val meta = HashSet<NameArity>()
            stmt.executeQuery("SELECT trino_name, arg_count FROM trino_meta() ORDER BY trino_name, arg_count").use { rs ->
                while (rs.next()) {
                    meta.add(NameArity(rs.getString(1), rs.getInt(2)))
                }
            }
            return meta
        }

        @Throws(SQLException::class)
        private fun installedTrinoFunctions(stmt: Statement): List<String> {
            val installed = ArrayList<String>()
            stmt.executeQuery(
                "SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'trino\\_%' ESCAPE '\\'",
            ).use { rs ->
                while (rs.next()) {
                    installed.add(rs.getString(1))
                }
            }
            return installed
        }

        @Throws(SQLException::class)
        private fun scalar(stmt: Statement, sql: String): Any? {
            stmt.executeQuery(sql).use { rs ->
                assertThat(rs.next()).`as`("query produced no rows: %s", sql).isTrue()
                return rs.getObject(1)
            }
        }

        @Throws(SQLException::class)
        private fun numeric(stmt: Statement, sql: String): Double {
            stmt.executeQuery(sql).use { rs ->
                assertThat(rs.next()).`as`("query produced no rows: %s", sql).isTrue()
                return (rs.getObject(1) as Number).toDouble()
            }
        }
    }
}
