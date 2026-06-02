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
package dev.brikk.ducklake.catalog;

import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In-process JDBC-layer probes for the Quack-as-DuckLake-catalog plan tracked in
 * {@code dev-docs/TODO-WRITE-MODE.md § Quack Catalog Backend}.
 *
 * <p>Two questions this test answers:
 * <ol>
 *   <li>Can the embedded DuckDB JDBC driver install + load the Quack extension
 *       in-process? Quack ships as a core extension as of DuckDB 1.5.3, so plain
 *       {@code INSTALL quack} against the default repo is sufficient.
 *   <li>Does a round-trip through two JDBC connections work — one running
 *       {@code quack_serve(...)}, the other doing {@code ATTACH 'quack:...'} and
 *       reading a server-side table?
 * </ol>
 *
 * <p>Coverage of the full DuckLake-on-Quack-against-an-out-of-process-sidecar
 * surface lives in {@link TestDuckDbQuackCatalogServerSmoke}, which uses the
 * containerized {@link TestingDucklakeDuckDbQuackCatalogServer} fixture (CLI
 * version pinned by the in-tree Dockerfile, no host-CLI dependency).
 */
final class QuackJdbcProbeTest
{
    @Test
    void installLoadQuackFromCore() throws Exception
    {
        try (Connection connection = openEmbeddedDuckDb();
                Statement statement = connection.createStatement()) {
            statement.execute("INSTALL quack");
            statement.execute("LOAD quack");
        }
    }

    @Test
    void quackRoundTripBetweenTwoEmbeddedConnections() throws Exception
    {
        int port = findFreePort();
        String quackUri = "quack:localhost:" + port;
        String token = "probe-secret";

        // The server connection must outlive the client query — quack_serve binds the
        // listener to the embedded DuckDB instance backing this connection.
        try (Connection server = openEmbeddedDuckDb();
                Statement s = server.createStatement()) {
            s.execute("INSTALL quack");
            s.execute("LOAD quack");

            s.execute("CREATE TABLE hello AS SELECT 42 AS x, 'world' AS name");

            // quack_serve is non-blocking; the listener runs in a background thread
            // inside the DuckDB engine, lifetime tied to the embedded instance.
            s.execute("CALL quack_serve('" + quackUri + "', token = '" + token + "')");

            try (Connection client = openEmbeddedDuckDb();
                    Statement c = client.createStatement()) {
                c.execute("INSTALL quack");
                c.execute("LOAD quack");
                c.execute("CREATE SECRET (TYPE quack, TOKEN '" + token + "')");
                c.execute("ATTACH '" + quackUri + "' AS remote");

                try (ResultSet rs = c.executeQuery("SELECT x, name FROM remote.hello")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt("x")).isEqualTo(42);
                    assertThat(rs.getString("name")).isEqualTo("world");
                    assertThat(rs.next()).isFalse();
                }
            }
        }
    }

    private static Connection openEmbeddedDuckDb()
            throws SQLException
    {
        return DriverManager.getConnection("jdbc:duckdb:");
    }

    private static int findFreePort()
            throws Exception
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
