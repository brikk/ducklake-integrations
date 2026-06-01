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

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_COLUMN_STATS
import dev.brikk.ducklake.catalog.testing.CatalogPredicates.currentlyActive
import dev.brikk.ducklake.catalog.testing.CatalogQueries
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_WRITER_MODE
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.WRITER_MODE_APPENDER
import io.trino.Session
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.nio.file.Path
import java.sql.DriverManager

/**
 * Phase 1 Step 2 — DuckDB-format *writes*.
 *
 * Validates that CTAS / INSERT with `data_file_format = 'duckdb'` actually
 * produces a `.db` file on disk that DuckDB itself can open and read. Reads
 * back through Trino are not yet implemented (Step 3), so verification goes through
 * the DuckDB JDBC driver directly, opening the artifact the connector wrote.
 */
@Execution(ExecutionMode.SAME_THREAD)
open class TestDucklakeDuckDbFormatWrite : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "duckdb-format-write"
    }

    private fun duckDbSession(): Session {
        return Session.builder(session)
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build()
    }

    private fun dataDir(): Path {
        return Path.of("build", "test-data", "test-catalog-isolated-" + isolatedCatalogName(), "data")
                .toAbsolutePath()
    }

    @Throws(Exception::class)
    private fun findDuckDbFileForTable(relativePath: String): Path {
        // The path column on $files is relative to the table data path. We resolve it
        // against the catalog data root by walking — for these isolated tests there
        // are only a handful of files in the directory.
        Files.walk(dataDir()).use { stream ->
            val matches = stream
                    .filter { p -> p.toString().endsWith(relativePath) }
                    .toList()
            assertThat(matches)
                    .`as`("data file matching %s under %s", relativePath, dataDir())
                    .isNotEmpty()
            return matches.first()
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCtasIntoDuckDbFormatProducesValidDbFile() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_simple AS " +
                        "SELECT * FROM (VALUES (1, 'alice', true, 3.14), (2, 'bob', false, 2.71)) " +
                        "AS t(id, name, flag, score)")
        try {
            // Catalog metadata reports the new format
            val files = computeActual(
                    "SELECT path, file_format, record_count FROM \"duck_simple\$files\"")
            assertThat(files.rowCount).isEqualTo(1)
            val row = files.materializedRows.first()
            val relPath = row.getField(0) as String
            assertThat(relPath).endsWith(".db")
            assertThat(row.getField(1)).isEqualTo("duckdb")
            assertThat(row.getField(2)).isEqualTo(2L)

            // The file is openable by DuckDB and the contents match
            val absPath = findDuckDbFileForTable(relPath)
            assertThat(Files.size(absPath)).isPositive()

            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("ATTACH '" + absPath.toString().replace("'", "''") + "' AS s (READ_ONLY)")
                    stmt.executeQuery("SELECT id, name, flag, score FROM s.t ORDER BY id").use { rs ->
                        val rows = ArrayList<List<Any>>()
                        while (rs.next()) {
                            rows.add(listOf(
                                    rs.getInt(1),
                                    rs.getString(2),
                                    rs.getBoolean(3),
                                    rs.getDouble(4)))
                        }
                        assertThat(rows).containsExactly(
                                listOf(1, "alice", true, 3.14),
                                listOf(2, "bob", false, 2.71))
                    }
                    stmt.execute("DETACH s")
                }
            }
        }
        finally {
            tryDropTable("test_schema.duck_simple")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testCtasWithNullsRoundTripsThroughDuckDb() {
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_nulls AS " +
                        "SELECT * FROM (VALUES (1, CAST('alice' AS VARCHAR), CAST(NULL AS VARCHAR)), " +
                        "(2, CAST(NULL AS VARCHAR), CAST('present' AS VARCHAR))) " +
                        "AS t(id, opt_a, opt_b)")
        try {
            val files = computeActual("SELECT path FROM \"duck_nulls\$files\"")
            val relPath = files.materializedRows.first().getField(0) as String
            val absPath = findDuckDbFileForTable(relPath)

            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("ATTACH '" + absPath.toString().replace("'", "''") + "' AS s (READ_ONLY)")
                    stmt.executeQuery("SELECT id, opt_a, opt_b FROM s.t ORDER BY id").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt(1)).isEqualTo(1)
                        assertThat(rs.getString(2)).isEqualTo("alice")
                        assertThat(rs.getString(3)).isNull()

                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt(1)).isEqualTo(2)
                        assertThat(rs.getString(2)).isNull()
                        assertThat(rs.getString(3)).isEqualTo("present")

                        assertThat(rs.next()).isFalse()
                    }
                    stmt.execute("DETACH s")
                }
            }
        }
        finally {
            tryDropTable("test_schema.duck_nulls")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testInsertAfterParquetCreateWritesDuckDbFile() {
        // Table created without the session prop — initial schema only, no data files.
        computeActual("CREATE TABLE test_schema.duck_insert (id INTEGER, name VARCHAR)")
        try {
            // Insert with the duckdb session — should produce a .db file even though the
            // table was originally created on the default parquet path.
            computeActual(duckDbSession(),
                    "INSERT INTO test_schema.duck_insert VALUES (10, 'x'), (20, 'y'), (30, 'z')")

            val files = computeActual(
                    "SELECT path, file_format, record_count FROM \"duck_insert\$files\"")
            assertThat(files.rowCount).isEqualTo(1)
            val row = files.materializedRows.first()
            assertThat(row.getField(1)).isEqualTo("duckdb")
            assertThat(row.getField(2)).isEqualTo(3L)

            val absPath = findDuckDbFileForTable(row.getField(0) as String)
            DriverManager.getConnection("jdbc:duckdb:").use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("ATTACH '" + absPath.toString().replace("'", "''") + "' AS s (READ_ONLY)")
                    stmt.executeQuery("SELECT count(*) FROM s.t").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getLong(1)).isEqualTo(3L)
                    }
                    stmt.execute("DETACH s")
                }
            }
        }
        finally {
            tryDropTable("test_schema.duck_insert")
        }
    }

    @Test
    fun testReadbackThroughTrino() {
        // After Step 3 the duckdb-format reader is wired in. SELECT through Trino
        // should round-trip the data we just wrote.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_readback AS SELECT 7 AS id, CAST('hello' AS VARCHAR) AS name")
        try {
            val result = computeActual("SELECT id, name FROM test_schema.duck_readback")
            assertThat(result.rowCount).isEqualTo(1)
            val row = result.materializedRows.first()
            assertThat(row.getField(0)).isEqualTo(7)
            assertThat(row.getField(1)).isEqualTo("hello")
        }
        finally {
            tryDropTable("test_schema.duck_readback")
        }
    }

    @Test
    @Throws(Exception::class)
    fun testColumnStatsWrittenForDuckDbFormat() {
        // Per-column min/max/null_count rows must be persisted in
        // ducklake_column_stats. The DuckLake DuckDB extension's
        // duckdb_tables() / TransformGlobalStats path crashes if any data file
        // has missing stats — so this is a cross-engine compatibility guard,
        // not just a Trino-side optimization.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_stats AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('alpha' AS VARCHAR), 100), " +
                        "  (2, CAST('beta'  AS VARCHAR), 200), " +
                        "  (3, CAST(NULL    AS VARCHAR), 300), " +
                        "  (4, CAST('delta' AS VARCHAR), NULL)" +
                        ") AS t(id, label, amount)")
        try {
            openCatalogConnection().use { conn ->
                val dsl = CatalogTestSupport.dsl(conn)
                val tableId = CatalogQueries.activeTableId(dsl, "duck_stats")

                // Three-table join: file_column_stats → column (active rows only) → data_file,
                // restricted to the duckdb-format files of the currently-active "duck_stats" table.
                // No canned helper covers the shape; the predicates compose cleanly inline.
                val colstats = DUCKLAKE_FILE_COLUMN_STATS.`as`("colstats")
                val col = DUCKLAKE_COLUMN.`as`("col")
                val file = DUCKLAKE_DATA_FILE.`as`("file")
                val rows = HashMap<String, StatsRow>()
                dsl.select(
                                col.COLUMN_NAME,
                                colstats.VALUE_COUNT,
                                colstats.NULL_COUNT,
                                colstats.MIN_VALUE,
                                colstats.MAX_VALUE)
                        .from(colstats)
                        .join(col)
                                .on(col.COLUMN_ID.eq(colstats.COLUMN_ID)
                                        .and(currentlyActive(col.END_SNAPSHOT)))
                        .join(file)
                                .on(file.DATA_FILE_ID.eq(colstats.DATA_FILE_ID))
                        .where(file.FILE_FORMAT.eq("duckdb")
                                .and(col.TABLE_ID.eq(tableId)))
                        .forEach { r ->
                            rows[r.get(col.COLUMN_NAME)] = StatsRow(
                                    orZero(r.get(colstats.VALUE_COUNT)),
                                    orZero(r.get(colstats.NULL_COUNT)),
                                    r.get(colstats.MIN_VALUE),
                                    r.get(colstats.MAX_VALUE))
                        }
                assertThat(rows).containsKeys("id", "label", "amount")

                // id: 1..4, no nulls
                assertThat(rows["id"]!!.valueCount).isEqualTo(4L)
                assertThat(rows["id"]!!.nullCount).isEqualTo(0L)
                assertThat(rows["id"]!!.minValue).isEqualTo("1")
                assertThat(rows["id"]!!.maxValue).isEqualTo("4")

                // label: alpha/beta/delta + 1 null
                assertThat(rows["label"]!!.valueCount).isEqualTo(3L)
                assertThat(rows["label"]!!.nullCount).isEqualTo(1L)
                assertThat(rows["label"]!!.minValue).isEqualTo("alpha")
                assertThat(rows["label"]!!.maxValue).isEqualTo("delta")

                // amount: 100/200/300 + 1 null
                assertThat(rows["amount"]!!.valueCount).isEqualTo(3L)
                assertThat(rows["amount"]!!.nullCount).isEqualTo(1L)
                assertThat(rows["amount"]!!.minValue).isEqualTo("100")
                assertThat(rows["amount"]!!.maxValue).isEqualTo("300")
            }
        }
        finally {
            tryDropTable("test_schema.duck_stats")
        }
    }

    @JvmRecord
    private data class StatsRow(val valueCount: Long, val nullCount: Long, val minValue: String?, val maxValue: String?)

    @Test
    @Throws(Exception::class)
    fun testTablePropertyOverridesSessionToDuckDb() {
        // Default session is parquet. The CREATE TABLE WITH override should still
        // produce a duckdb-format file.
        computeActual("CREATE TABLE test_schema.tp_duckdb (id INTEGER, label VARCHAR) WITH (data_file_format = 'duckdb')")
        try {
            computeActual("INSERT INTO test_schema.tp_duckdb VALUES (1, 'a'), (2, 'b')")
            // INSERTs use the session default (parquet), so this row goes to a parquet file.
            // But CREATE TABLE AS would use the property — let's verify with a CTAS instead.
        }
        finally {
            tryDropTable("test_schema.tp_duckdb")
        }

        // CTAS form: property drives the writer for the materialized rows.
        computeActual("CREATE TABLE test_schema.tp_duckdb_ctas WITH (data_file_format = 'duckdb') AS " +
                "SELECT * FROM (VALUES (1, CAST('alpha' AS VARCHAR)), (2, CAST('beta' AS VARCHAR))) AS t(id, label)")
        try {
            val files = computeActual(
                    "SELECT file_format, record_count FROM \"tp_duckdb_ctas\$files\"")
            assertThat(files.rowCount).isEqualTo(1)
            assertThat(files.materializedRows.first().getField(0)).isEqualTo("duckdb")
            assertThat(files.materializedRows.first().getField(1)).isEqualTo(2L)
        }
        finally {
            tryDropTable("test_schema.tp_duckdb_ctas")
        }
    }

    @Test
    fun testTablePropertyOverridesSessionToParquet() {
        // Session set to duckdb, but the CREATE TABLE WITH override forces parquet.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.tp_parquet_override WITH (data_file_format = 'parquet') AS " +
                        "SELECT 1 AS id, CAST('hello' AS VARCHAR) AS label")
        try {
            val files = computeActual(
                    "SELECT file_format FROM \"tp_parquet_override\$files\"")
            assertThat(files.materializedRows.first().getField(0)).isEqualTo("parquet")
        }
        finally {
            tryDropTable("test_schema.tp_parquet_override")
        }
    }

    @Test
    fun testInvalidTablePropertyRejected() {
        assertThatThrownBy {
            computeActual(
                    "CREATE TABLE test_schema.tp_bad WITH (data_file_format = 'vortex') AS SELECT 1 AS id")
        }
                .hasMessageContaining("data_file_format must be one of")
        tryDropTable("test_schema.tp_bad")
    }

    @Test
    fun testPartitionedCtasInDuckDbFormat() {
        // CTAS into a duckdb-format table partitioned by an identity column.
        // The page sink routes positions per partition into separate writers — should
        // produce one .db file per partition and the data should round-trip correctly.
        computeActual(duckDbSession(),
                "CREATE TABLE test_schema.duck_part " +
                        "WITH (partitioned_by = ARRAY['region']) AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('us' AS VARCHAR), 100), " +
                        "  (2, CAST('eu' AS VARCHAR), 200), " +
                        "  (3, CAST('us' AS VARCHAR), 300), " +
                        "  (4, CAST('jp' AS VARCHAR), 400), " +
                        "  (5, CAST('eu' AS VARCHAR), 500)" +
                        ") AS t(id, region, amount)")
        try {
            val files = computeActual(
                    "SELECT path, file_format, record_count FROM \"duck_part\$files\" ORDER BY path")
            // Three distinct region values → three .db files.
            assertThat(files.rowCount).isEqualTo(3)
            for (row in files.materializedRows) {
                val path = row.getField(0) as String
                assertThat(path).endsWith(".db")
                assertThat(path).matches(".*region=(us|eu|jp)/ducklake-.+\\.db")
                assertThat(row.getField(1)).isEqualTo("duckdb")
            }

            val all = computeActual(
                    "SELECT id, region, amount FROM test_schema.duck_part ORDER BY id")
            assertThat(all.rowCount).isEqualTo(5)

            // Predicate on the partition column — should prune to a single .db file.
            val pruned = computeActual(
                    "SELECT id, amount FROM test_schema.duck_part WHERE region = 'us' ORDER BY id")
            assertThat(pruned.rowCount).isEqualTo(2)
            assertThat(pruned.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(pruned.materializedRows[0].getField(1)).isEqualTo(100)
            assertThat(pruned.materializedRows[1].getField(0)).isEqualTo(3)
            assertThat(pruned.materializedRows[1].getField(1)).isEqualTo(300)

            // GROUP BY across partitions — exercises both readers in one query.
            val agg = computeActual(
                    "SELECT region, sum(amount) FROM test_schema.duck_part GROUP BY region ORDER BY region")
            assertThat(agg.rowCount).isEqualTo(3)
            assertThat(agg.materializedRows[0].getField(0)).isEqualTo("eu")
            assertThat(agg.materializedRows[0].getField(1)).isEqualTo(700L)
            assertThat(agg.materializedRows[1].getField(0)).isEqualTo("jp")
            assertThat(agg.materializedRows[1].getField(1)).isEqualTo(400L)
            assertThat(agg.materializedRows[2].getField(0)).isEqualTo("us")
            assertThat(agg.materializedRows[2].getField(1)).isEqualTo(400L)
        }
        finally {
            tryDropTable("test_schema.duck_part")
        }
    }

    @Test
    fun testUuidRoundTripThroughAppender() {
        // The legacy `DuckDbFileWriter` (Appender API) supported UUID since
        // Phase 1 (DuckDB JDBC's appender takes a `java.util.UUID` natively).
        // The reader, however, had no UUID converter case until the same fix that
        // added it for the arrow_stream writer landed — so even appender-written
        // UUID files were unreadable through Trino. This test pins both halves.
        val appenderSession = Session.builder(session)
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .setCatalogSessionProperty("ducklake", DUCKDB_WRITER_MODE, WRITER_MODE_APPENDER)
                .build()

        computeActual(appenderSession,
                "CREATE TABLE test_schema.appender_uuids AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('00000000-0000-0000-0000-000000000001' AS UUID)), " +
                        "  (2, CAST('aabbccdd-eeff-0011-2233-445566778899' AS UUID)), " +
                        "  (3, CAST(NULL AS UUID))" +
                        ") AS t(id, u)")
        try {
            val files = computeActual("SELECT file_format FROM \"appender_uuids\$files\"")
            assertThat(files.materializedRows.first().getField(0)).isEqualTo("duckdb")

            val result = computeActual(
                    "SELECT id, u FROM test_schema.appender_uuids ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3)
            assertThat(result.materializedRows[0].getField(1).toString())
                    .isEqualTo("00000000-0000-0000-0000-000000000001")
            assertThat(result.materializedRows[1].getField(1).toString())
                    .isEqualTo("aabbccdd-eeff-0011-2233-445566778899")
            assertThat(result.materializedRows[2].getField(1)).isNull()
        }
        finally {
            tryDropTable("test_schema.appender_uuids")
        }
    }

    @Test
    fun testParquetPathUnchangedAtDefaultSession() {
        // No session override — parquet baseline.
        computeActual("CREATE TABLE test_schema.duck_parquet_default AS SELECT 1 AS id, 'hello' AS msg")
        try {
            val result = computeActual("SELECT * FROM test_schema.duck_parquet_default")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows.first().getField(0)).isEqualTo(1)
            assertThat(result.materializedRows.first().getField(1)).isEqualTo("hello")

            val files = computeActual(
                    "SELECT file_format FROM \"duck_parquet_default\$files\"")
            assertThat(files.rowCount).isEqualTo(1)
            assertThat(files.materializedRows.first().getField(0)).isEqualTo("parquet")
        }
        finally {
            tryDropTable("test_schema.duck_parquet_default")
        }
    }

    companion object {
        private fun orZero(value: Long?): Long {
            // ducklake_file_column_stats columns are nullable in the schema; the original
            // raw-SQL path used ResultSet#getLong which returns 0 on SQL NULL, and the
            // assertions baked that in. Preserve that behavior here.
            return value ?: 0L
        }
    }
}
