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
package dev.brikk.ducklake.catalog

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.net.ServerSocket
import java.sql.Connection
import java.sql.DriverManager

/**
 * In-process JDBC-layer probes for the Quack-as-DuckLake-catalog plan tracked in
 * `dev-docs/TODO-WRITE-MODE.md § Quack Catalog Backend`.
 *
 * Two questions this test answers:
 *  1. Can the embedded DuckDB JDBC driver install + load the Quack extension
 *     in-process? Quack ships as a core extension as of DuckDB 1.5.3, so plain
 *     `INSTALL quack` against the default repo is sufficient.
 *  2. Does a round-trip through two JDBC connections work — one running
 *     `quack_serve(...)`, the other doing `ATTACH 'quack:...'` and
 *     reading a server-side table?
 *
 * Coverage of the full DuckLake-on-Quack-against-an-out-of-process-sidecar
 * surface lives in [TestDuckDbQuackCatalogServerSmoke], which uses the
 * containerized [TestingDucklakeDuckDbQuackCatalogServer] fixture (CLI
 * version pinned by the in-tree Dockerfile, no host-CLI dependency).
 */
internal class QuackJdbcProbeTest {
    @Test
    fun installLoadQuackFromCore() {
        openEmbeddedDuckDb().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("INSTALL quack")
                statement.execute("LOAD quack")
            }
        }
    }

    @Test
    fun quackRoundTripBetweenTwoEmbeddedConnections() {
        val port = findFreePort()
        val quackUri = "quack:localhost:$port"
        val token = "probe-secret"

        // The server connection must outlive the client query — quack_serve binds the
        // listener to the embedded DuckDB instance backing this connection.
        openEmbeddedDuckDb().use { server ->
            server.createStatement().use { s ->
                s.execute("INSTALL quack")
                s.execute("LOAD quack")

                s.execute("CREATE TABLE hello AS SELECT 42 AS x, 'world' AS name")

                // quack_serve is non-blocking; the listener runs in a background thread
                // inside the DuckDB engine, lifetime tied to the embedded instance.
                s.execute("CALL quack_serve('$quackUri', token = '$token')")

                openEmbeddedDuckDb().use { client ->
                    client.createStatement().use { c ->
                        c.execute("INSTALL quack")
                        c.execute("LOAD quack")
                        c.execute("CREATE SECRET (TYPE quack, TOKEN '$token')")
                        c.execute("ATTACH '$quackUri' AS remote")

                        c.executeQuery("SELECT x, name FROM remote.hello").use { rs ->
                            assertThat(rs.next()).isTrue()
                            assertThat(rs.getInt("x")).isEqualTo(42)
                            assertThat(rs.getString("name")).isEqualTo("world")
                            assertThat(rs.next()).isFalse()
                        }
                    }
                }
            }
        }
    }

    private fun openEmbeddedDuckDb(): Connection {
        return DriverManager.getConnection("jdbc:duckdb:")
    }

    private fun findFreePort(): Int {
        ServerSocket(0).use { socket ->
            return socket.localPort
        }
    }
}
