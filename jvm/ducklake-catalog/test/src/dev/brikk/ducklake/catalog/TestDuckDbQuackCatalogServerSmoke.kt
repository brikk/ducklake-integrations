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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager
import java.util.Comparator

/**
 * End-to-end smoke for [TestingDucklakeDuckDbQuackCatalogServer]: stand the
 * container up, ATTACH a DuckLake catalog via Quack from an in-process JDBC
 * client, write rows, read them back. Proves the container fixture is the
 * usable shape for cross-engine tests — i.e. that the JDBC-client side
 * (INSTALL/LOAD/CREATE SECRET/ATTACH) round-trips correctly against the
 * containerized Quack server.
 */
internal class TestDuckDbQuackCatalogServerSmoke {

    @Test
    fun clientAttachesDucklakeViaQuackAndRoundTripsAQuery() {
        DriverManager.getConnection("jdbc:duckdb:").use { client ->
            client.createStatement().use { s ->
                s.execute("INSTALL quack")
                s.execute("LOAD quack")
                s.execute("INSTALL ducklake")
                s.execute("LOAD ducklake")
                s.execute("CREATE SECRET (TYPE quack, TOKEN '" + server!!.getToken() + "')")
                s.execute(
                    "ATTACH '" + server!!.getDucklakeAttachUri() + "' AS lake " +
                        "(DATA_PATH '" + dataDir!!.toAbsolutePath() + "', METADATA_CATALOG 'smoke')"
                )
                s.execute("USE lake")
                s.execute("CREATE TABLE t (x INTEGER, name VARCHAR)")
                s.execute("INSERT INTO t VALUES (42, 'world'), (7, 'hi')")

                s.executeQuery("SELECT x, name FROM t ORDER BY x").use { rs ->
                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getInt("x")).isEqualTo(7)
                    assertThat(rs.getString("name")).isEqualTo("hi")
                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getInt("x")).isEqualTo(42)
                    assertThat(rs.getString("name")).isEqualTo("world")
                    assertThat(rs.next()).isFalse()
                }
            }
        }
    }

    companion object {
        private var server: TestingDucklakeDuckDbQuackCatalogServer? = null
        private var dataDir: Path? = null

        @BeforeAll
        @JvmStatic
        fun setUp() {
            server = TestingDucklakeDuckDbQuackCatalogServer()
            dataDir = Files.createTempDirectory("ducklake-quack-smoke-")
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            server?.close()
            dataDir?.let { deleteRecursively(it) }
        }

        private fun deleteRecursively(dir: Path) {
            if (!Files.exists(dir)) {
                return
            }
            try {
                Files.walk(dir).use { walk ->
                    walk.sorted(Comparator.reverseOrder()).forEach { p ->
                        try {
                            Files.deleteIfExists(p)
                        }
                        catch (ignored: IOException) {
                        }
                    }
                }
            }
            catch (ignored: IOException) {
            }
        }
    }
}
