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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Gate probe for the Quack-as-DuckLake-catalog plan tracked in
 * {@code dev-docs/TODO-WRITE-MODE.md § Quack Catalog Backend}.
 *
 * <p>Two questions this test answers, both required before we build out fixtures:
 * <ol>
 *   <li>Can the embedded DuckDB JDBC driver actually install + load the Quack
 *       extension from {@code core_nightly} in-process? Quack is experimental and
 *       ships only on the nightly extension repo today.
 *   <li>Does a round-trip through two JDBC connections work — one running
 *       {@code quack_serve(...)}, the other doing {@code ATTACH 'quack:...'} and
 *       reading a server-side table?
 * </ol>
 *
 * <p>If either fails, the user-facing plan (local DuckDB JDBC delegating to a
 * remote DuckDB-as-catalog over Quack) is blocked at the JDBC layer and we'd
 * need to pursue a subprocess / CLI-server fallback instead.
 */
final class QuackJdbcProbeTest
{
    @Test
    void installLoadQuackFromCoreNightly() throws Exception
    {
        try (Connection connection = openEmbeddedDuckDb();
                Statement statement = connection.createStatement()) {
            statement.execute("INSTALL quack FROM core_nightly");
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
            s.execute("INSTALL quack FROM core_nightly");
            s.execute("LOAD quack");

            s.execute("CREATE TABLE hello AS SELECT 42 AS x, 'world' AS name");

            // quack_serve is non-blocking; the listener runs in a background thread
            // inside the DuckDB engine, lifetime tied to the embedded instance.
            s.execute("CALL quack_serve('" + quackUri + "', token = '" + token + "')");

            try (Connection client = openEmbeddedDuckDb();
                    Statement c = client.createStatement()) {
                c.execute("INSTALL quack FROM core_nightly");
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

    @Test
    void duckLakeCatalogOverQuackThroughRealSidecar() throws Exception
    {
        // Upstream's run_quack_tests.py is explicit that DuckLake-on-Quack and quack_serve
        // cannot share a DatabaseInstance — they deadlock on a connection-id self-RPC. So
        // the server must live in a different process from the client. This probe spawns a
        // real duckdb CLI sidecar; we skip if the CLI isn't on PATH.
        assumeTrue(duckdbCliAvailable(), "duckdb CLI not on PATH; skipping sidecar probe");

        int port = findFreePort();
        String token = "probe-secret";
        // URI form mirrors upstream's run_quack_tests.py — quack:// scheme with trailing
        // slash for serve, and quack:host:port for the client ATTACH URL. We bind to
        // 127.0.0.1 (not "localhost") because Quack resolves "localhost" to IPv6 only
        // on macOS, and Java's default address resolution prefers IPv4 — the mismatch
        // produces Connection-refused from an otherwise running listener.
        String serveUri = "quack://127.0.0.1:" + port + "/";
        String attachQuackUri = "quack:127.0.0.1:" + port;

        Path tmpDir = Files.createTempDirectory("ducklake-quack-probe-");
        Path dataPath = tmpDir.resolve("data");
        Files.createDirectories(dataPath);

        ProcessBuilder pb = new ProcessBuilder("duckdb", "-unsigned");
        pb.redirectErrorStream(true);
        Process sidecar = pb.start();
        OutputStream sidecarStdin = sidecar.getOutputStream();

        // Drain sidecar stdout/stderr so the OS pipe buffer doesn't fill and stall the
        // CLI, and so we can surface diagnostics in failure messages.
        StringBuilder sidecarLog = new StringBuilder();
        AtomicReference<Thread> drainerRef = new AtomicReference<>();
        Thread drainer = new Thread(() -> {
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(sidecar.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (sidecarLog) {
                        sidecarLog.append(line).append('\n');
                    }
                }
            }
            catch (IOException ignored) {
            }
        }, "duckdb-quack-sidecar-drain");
        drainer.setDaemon(true);
        drainer.start();
        drainerRef.set(drainer);

        try {
            // The trailing range query is a keep-alive: DuckDB's CLI in batch mode reads
            // stdin and exits when its queue drains. quack_serve returns immediately
            // (it just spawns the listener thread), so without a long-running query the
            // CLI would terminate and tear down the engine that hosts the listener.
            // The WHERE range < 0 predicate makes the query yield zero rows after the
            // (very long) scan, but the scan keeps the engine alive until we SIGTERM.
            // Borrowed verbatim from upstream's scripts/run_quack_tests.py.
            String serverInit = ""
                    + "LOAD httpfs;\n"
                    + "INSTALL quack FROM core_nightly;\n"
                    + "LOAD quack;\n"
                    + "SELECT * FROM quack_serve('" + serveUri + "', token := '" + token + "');\n"
                    + "SELECT count(*) FROM range(1000000000000) WHERE range < 0;\n";
            sidecarStdin.write(serverInit.getBytes(StandardCharsets.UTF_8));
            sidecarStdin.flush();
            // Leave stdin OPEN — closing it sends EOF, which makes the CLI exit and tears
            // down the engine that hosts quack_serve. We close it in the finally block.

            try {
                waitForPortOpen("127.0.0.1", port, 20_000);
            }
            catch (AssertionError fail) {
                synchronized (sidecarLog) {
                    throw new AssertionError(fail.getMessage()
                            + "\n--- sidecar output ---\n" + sidecarLog + "--- end sidecar output ---",
                            fail.getCause());
                }
            }

            Properties props = new Properties();
            props.setProperty("allow_unsigned_extensions", "true");
            try (Connection client = DriverManager.getConnection("jdbc:duckdb:", props);
                    Statement c = client.createStatement()) {
                c.execute("INSTALL quack FROM core_nightly");
                c.execute("LOAD quack");
                // DuckLake-on-Quack requires the QuackMetadataManager merged into ducklake
                // 2026-05-12 (PR #1151). Released ducklake may not have it yet — FORCE
                // INSTALL replaces any previously-installed core build with the nightly.
                c.execute("FORCE INSTALL ducklake FROM core_nightly");
                c.execute("LOAD ducklake");
                c.execute("CREATE SECRET (TYPE quack, TOKEN '" + token + "')");
                c.execute("ATTACH 'ducklake:" + attachQuackUri + "' AS lake "
                        + "(DATA_PATH '" + dataPath.toAbsolutePath() + "', METADATA_CATALOG 'metadata')");
                c.execute("USE lake");
                c.execute("CREATE TABLE probe(x INTEGER, name VARCHAR)");
                c.execute("INSERT INTO probe VALUES (42, 'world'), (7, 'hi')");

                try (ResultSet rs = c.executeQuery("SELECT x, name FROM probe ORDER BY x")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt("x")).isEqualTo(7);
                    assertThat(rs.getString("name")).isEqualTo("hi");
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt("x")).isEqualTo(42);
                    assertThat(rs.getString("name")).isEqualTo("world");
                    assertThat(rs.next()).isFalse();
                }
            }
        }
        finally {
            try {
                sidecarStdin.close();
            }
            catch (IOException ignored) {
            }
            sidecar.destroy();
            if (!sidecar.waitFor(5, TimeUnit.SECONDS)) {
                sidecar.destroyForcibly();
            }
            Thread d = drainerRef.get();
            if (d != null) {
                d.join(2000);
            }
            deleteRecursively(tmpDir);
        }
    }

    private static boolean duckdbCliAvailable()
    {
        try {
            Process p = new ProcessBuilder("duckdb", "--version").redirectErrorStream(true).start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        }
        catch (IOException | InterruptedException ex) {
            return false;
        }
    }

    private static void waitForPortOpen(String host, int port, long timeoutMillis)
            throws Exception
    {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new java.net.InetSocketAddress(host, port), 250);
                return;
            }
            catch (IOException ex) {
                lastError = ex;
                Thread.sleep(100);
            }
        }
        throw new AssertionError("Quack sidecar did not start listening on "
                + host + ":" + port + " within " + timeoutMillis + " ms", lastError);
    }

    private static void deleteRecursively(Path dir)
    {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                }
                catch (IOException ignored) {
                }
            });
        }
        catch (IOException ignored) {
        }
    }

    private static Connection openEmbeddedDuckDb()
            throws SQLException
    {
        Properties props = new Properties();
        // Nightly extensions can have a different signing posture than core; relax
        // unsigned-extension policy so the probe doesn't get blocked by signature
        // checking on the core_nightly repo.
        props.setProperty("allow_unsigned_extensions", "true");
        return DriverManager.getConnection("jdbc:duckdb:", props);
    }

    private static int findFreePort()
            throws Exception
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
