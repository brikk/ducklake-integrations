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
package dev.brikk.ducklake.trino.plugin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager

/**
 * Round-trip + basic-interop slice of the cross-engine compatibility surface: Trino-side
 * INSERT/CTAS/multi-insert flows, scalar-type coverage, NULLs, and DuckDB-side metadata
 * queries (SHOW TABLES, DESCRIBE) over Trino-written tables. Plus a sanity check that
 * DuckDB still reads its own simple_table fixture (proves the isolated catalog has the
 * standard test seed data) and a diagnostic that dumps catalog state alongside a
 * Trino-write/DuckDB-read round trip.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
internal class TestDucklakeCrossEngineRoundTrip : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String {
        return "cross-engine-roundtrip"
    }

    @Test
    @Throws(Exception::class)
    fun testTrinoInsertDuckdbRead() {
        computeActual("CREATE TABLE test_schema.xengine_basic (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.xengine_basic VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

            // Verify Trino can read it
            val trinoResult = computeActual("SELECT count(*) FROM test_schema.xengine_basic")
            assertThat(trinoResult.materializedRows[0].getField(0)).isEqualTo(3L)

            // Verify DuckDB can read it via DuckLake catalog — no sync needed with PostgreSQL
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_basic ORDER BY id").use { rs ->
                        assertThat(rs.next()).`as`("DuckDB should find row 1").isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(1)
                        assertThat(rs.getString("name")).isEqualTo("alice")
                        assertThat(rs.next()).`as`("DuckDB should find row 2").isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(2)
                        assertThat(rs.getString("name")).isEqualTo("bob")
                        assertThat(rs.next()).`as`("DuckDB should find row 3").isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(3)
                        assertThat(rs.getString("name")).isEqualTo("charlie")
                        assertThat(rs.next()).isFalse()
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_basic")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTrinoCtasDuckdbRead() {
        computeActual("CREATE TABLE test_schema.xengine_ctas AS " +
                "SELECT CAST(id AS INTEGER) AS id, CAST(name AS VARCHAR) AS name " +
                "FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) AS t(id, name)")
        try {
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT id, name FROM ducklake_db.test_schema.xengine_ctas ORDER BY id").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(1)
                        assertThat(rs.getString("name")).isEqualTo("alpha")
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(2)
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(3)
                        assertThat(rs.getString("name")).isEqualTo("gamma")
                        assertThat(rs.next()).isFalse()
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_ctas")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTrinoMultipleInsertsDuckdbRead() {
        computeActual("CREATE TABLE test_schema.xengine_multi (id INTEGER, value DOUBLE)")
        try {
            computeActual("INSERT INTO test_schema.xengine_multi VALUES (1, 10.0), (2, 20.0)")
            computeActual("INSERT INTO test_schema.xengine_multi VALUES (3, 30.0), (4, 40.0)")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT count(*), sum(value) FROM ducklake_db.test_schema.xengine_multi").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getLong(1)).isEqualTo(4)
                        assertThat(rs.getDouble(2)).isEqualTo(100.0)
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_multi")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTrinoTypesDuckdbRead() {
        computeActual("CREATE TABLE test_schema.xengine_types (" +
                "col_int INTEGER, " +
                "col_bigint BIGINT, " +
                "col_double DOUBLE, " +
                "col_varchar VARCHAR, " +
                "col_boolean BOOLEAN, " +
                "col_date DATE" +
                ")")
        try {
            computeActual("INSERT INTO test_schema.xengine_types VALUES " +
                    "(42, 1000000000, 3.14, 'hello world', true, DATE '2024-06-15')")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_types").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("col_int")).isEqualTo(42)
                        assertThat(rs.getLong("col_bigint")).isEqualTo(1000000000L)
                        assertThat(rs.getDouble("col_double")).isEqualTo(3.14)
                        assertThat(rs.getString("col_varchar")).isEqualTo("hello world")
                        assertThat(rs.getBoolean("col_boolean")).isTrue()
                        assertThat(rs.getDate("col_date").toString()).isEqualTo("2024-06-15")
                        assertThat(rs.next()).isFalse()
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_types")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testTrinoNullsDuckdbRead() {
        computeActual("CREATE TABLE test_schema.xengine_nulls (id INTEGER, name VARCHAR, value DOUBLE)")
        try {
            computeActual("INSERT INTO test_schema.xengine_nulls VALUES (1, 'present', 10.0), (2, NULL, NULL)")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT * FROM ducklake_db.test_schema.xengine_nulls ORDER BY id").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(1)
                        assertThat(rs.getString("name")).isEqualTo("present")
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(2)
                        assertThat(rs.getString("name")).isNull()
                        assertThat(rs.getObject("value")).isNull()
                        assertThat(rs.next()).isFalse()
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_nulls")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbShowTablesIncludesTrinoTables() {
        computeActual("CREATE TABLE test_schema.xengine_visible (id INTEGER)")
        try {
            computeActual("INSERT INTO test_schema.xengine_visible VALUES (1)")

            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SHOW TABLES FROM ducklake_db.test_schema").use { rs ->
                        val tables = ArrayList<String>()
                        while (rs.next()) {
                            tables.add(rs.getString(1))
                        }
                        assertThat(tables).contains("xengine_visible")
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_visible")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbDescribeTrinoTable() {
        computeActual("CREATE TABLE test_schema.xengine_describe (id INTEGER, name VARCHAR, amount DOUBLE)")
        try {
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT column_name FROM information_schema.columns " +
                            "WHERE table_schema = 'test_schema' AND table_name = 'xengine_describe' " +
                            "AND table_catalog = 'ducklake_db' ORDER BY ordinal_position").use { rs ->
                        val columnNames = ArrayList<String>()
                        while (rs.next()) {
                            columnNames.add(rs.getString(1))
                        }
                        assertThat(columnNames).containsExactly("id", "name", "amount")
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.xengine_describe")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbReadsDuckdbCreatedData() {
        // Sanity check: can DuckDB read a table that DuckDB itself created? The isolated
        // catalog ships with simple_table seeded by DucklakeCatalogGenerator.
        createDuckdbConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT count(*) FROM ducklake_db.test_schema.simple_table").use { rs ->
                    assertThat(rs.next()).isTrue()
                    assertThat(rs.getLong(1)).`as`("DuckDB should read its own simple_table").isEqualTo(5)
                }
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDiagnosticTrinoWriteThenDuckdbRead() {
        computeActual("CREATE TABLE test_schema.diag_table (id INTEGER, name VARCHAR)")
        computeActual("INSERT INTO test_schema.diag_table VALUES (1, 'hello'), (2, 'world')")

        // Verify Trino reads it
        assertThat(computeActual("SELECT count(*) FROM test_schema.diag_table")
                .materializedRows[0].getField(0)).isEqualTo(2L)

        // Dump catalog state for debugging via PostgreSQL JDBC
        val catalog: DucklakeCatalogGenerator.IsolatedCatalog = getIsolatedCatalog()
        try {
            DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { pgConn ->
                pgConn.createStatement().use { pgStmt ->
                    val snapRs = pgStmt.executeQuery("SELECT max(snapshot_id) FROM ducklake_snapshot")
                    val currentSnapshot: Long = if (snapRs.next()) snapRs.getLong(1) else -1

                    val tableRs = pgStmt.executeQuery(
                            "SELECT table_id, begin_snapshot, end_snapshot FROM ducklake_table WHERE table_name = 'diag_table'")
                    var tableId: Long = -1
                    var tableBegin: Long = -1
                    var tableEnd: String? = "?"
                    if (tableRs.next()) {
                        tableId = tableRs.getLong(1)
                        tableBegin = tableRs.getLong(2)
                        tableEnd = tableRs.getString(3)
                    }

                    val fileRs = pgStmt.executeQuery(
                            "SELECT data_file_id, begin_snapshot, end_snapshot, path, record_count FROM ducklake_data_file WHERE table_id = $tableId")
                    var fileBegin: Long = -1
                    var fileEnd: String? = "?"
                    var filePath: String? = "?"
                    var fileRecords: Long = -1
                    if (fileRs.next()) {
                        fileBegin = fileRs.getLong(2)
                        fileEnd = fileRs.getString(3)
                        filePath = fileRs.getString(4)
                        fileRecords = fileRs.getLong(5)
                    }

                    // Now try DuckDB — verify count (column values have known interop issue)
                    createDuckdbConnection().use { duckConn ->
                        duckConn.createStatement().use { duckStmt ->
                            duckStmt.executeQuery("SELECT count(*) FROM ducklake_db.test_schema.diag_table").use { duckRs ->
                                val duckCount: Long = if (duckRs.next()) duckRs.getLong(1) else -1

                                assertThat(duckCount)
                                        .`as`("DuckDB count (snapshot=%d, table=%d begin=%d end=%s, file begin=%d end=%s path=%s records=%d)",
                                                currentSnapshot, tableId, tableBegin, tableEnd, fileBegin, fileEnd, filePath, fileRecords)
                                        .isEqualTo(2)
                            }
                        }
                    }
                }
            }
        }
        finally {
            tryDropTable("test_schema.diag_table")
        }
    }
}
