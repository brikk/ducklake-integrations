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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end smoke for {@link TestingDucklakeDuckDbQuackCatalogServer}: stand the
 * container up, ATTACH a DuckLake catalog via Quack from an in-process JDBC
 * client, write rows, read them back. Proves the container fixture is the
 * usable shape for cross-engine tests — i.e. that the JDBC-client side
 * (INSTALL/LOAD/CREATE SECRET/ATTACH) round-trips correctly against the
 * containerized Quack server.
 */
final class TestDuckDbQuackCatalogServerSmoke
{
    private static TestingDucklakeDuckDbQuackCatalogServer server;
    private static Path dataDir;

    @BeforeAll
    static void setUp() throws Exception
    {
        server = new TestingDucklakeDuckDbQuackCatalogServer();
        dataDir = Files.createTempDirectory("ducklake-quack-smoke-");
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        if (server != null) {
            server.close();
        }
        if (dataDir != null) {
            deleteRecursively(dataDir);
        }
    }

    @Test
    void clientAttachesDucklakeViaQuackAndRoundTripsAQuery() throws Exception
    {
        Properties props = new Properties();
        props.setProperty("allow_unsigned_extensions", "true");

        try (Connection client = DriverManager.getConnection("jdbc:duckdb:", props);
                Statement s = client.createStatement()) {
            s.execute("INSTALL quack FROM core_nightly");
            s.execute("LOAD quack");
            // FORCE INSTALL replaces any previously-installed core-repo ducklake
            // with the core_nightly build that contains QuackMetadataManager
            // (merged upstream 2026-05-12, duckdb/ducklake#1151).
            s.execute("FORCE INSTALL ducklake FROM core_nightly");
            s.execute("LOAD ducklake");
            s.execute("CREATE SECRET (TYPE quack, TOKEN '" + server.getToken() + "')");
            s.execute("ATTACH '" + server.getDucklakeAttachUri() + "' AS lake "
                    + "(DATA_PATH '" + dataDir.toAbsolutePath() + "', METADATA_CATALOG 'smoke')");
            s.execute("USE lake");
            s.execute("CREATE TABLE t (x INTEGER, name VARCHAR)");
            s.execute("INSERT INTO t VALUES (42, 'world'), (7, 'hi')");

            try (ResultSet rs = s.executeQuery("SELECT x, name FROM t ORDER BY x")) {
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
}
