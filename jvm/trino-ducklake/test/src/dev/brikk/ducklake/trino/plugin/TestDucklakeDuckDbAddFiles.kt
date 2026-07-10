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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.sql.DriverManager

/**
 * `CALL ducklake.system.add_files(..., file_format => 'duckdb')` — the duckdb sibling of
 * [TestDucklakeVortexAddFiles] / [TestDucklakeLanceAddFiles]: registers an **externally-written**
 * single DuckDB `.db` file (holding its data in a `main.t` table, the shape the connector's own
 * duckdb writer emits and the read path ATTACHes) without rewriting, then SELECTs it back through
 * the ATTACH read path. The catalog must record `file_format='duckdb'`, the scanned `record_count`,
 * and the real file size.
 *
 * Also pins the v1 gates shared with lance/vortex: partitioned tables require `hive_partitioning`.
 *
 * `duckdb` is a DuckDB *core* extension (no INSTALL) so this is not network-gated. SAME_THREAD:
 * writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeDuckDbAddFiles : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "integration-duckdb-add-files"

    /** Write a single `.db` file holding `main.t` with the given rows, via raw DuckDB. */
    private fun writeDuckDbFile(path: java.nio.file.Path, createTableAsSelect: String) {
        val escaped = path.toString().replace("'", "''")
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("ATTACH '$escaped' AS ext (READ_WRITE)")
                s.execute("CREATE TABLE ext.main.t AS $createTableAsSelect")
                s.execute("DETACH ext")
            }
        }
    }

    @Test
    fun addFilesRegistersDuckDbFileThenSelects() {
        val table = "duckdb_added"

        // Write a .db file out-of-band via raw DuckDB. Column names (id, name) must match the
        // table — the duckdb read path ATTACHes main.t and projects by name.
        val dir = Files.createTempDirectory("duckdb-add-files")
        val file = dir.resolve("$table.db")
        writeDuckDbFile(file, "SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) v(id, name)")

        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            val filePath = file.toAbsolutePath().toString().replace("'", "''")
            computeActual(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => '$table', "
                            + "files => ARRAY['$filePath'], "
                            + "file_format => 'duckdb')")

            // Catalog must record the registered file as duckdb-format with the scanned row
            // count and the real on-disk size.
            val files = computeActual(
                    "SELECT file_format, record_count, file_size_bytes FROM \"$table\$files\"")
            assertThat(files.rowCount).isEqualTo(1)
            assertThat(files.materializedRows[0].getField(0)).isEqualTo("duckdb")
            assertThat((files.materializedRows[0].getField(1) as Number).toLong())
                    .`as`("add_files must source record_count by scanning the .db file's main.t")
                    .isEqualTo(3L)
            assertThat((files.materializedRows[0].getField(2) as Number).toLong())
                    .isEqualTo(Files.size(file))

            // Read back through the ATTACH dispatch.
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(3L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alpha", "beta", "gamma")

            // A predicate over the duckdb read must filter correctly.
            assertThat(computeActual("SELECT id FROM $table WHERE name = 'beta'").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(2L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun duckDbAddFilesIntoPartitionedTableRequiresHivePartitioning() {
        val table = "duckdb_added_part_gate"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR) " +
                    "WITH (partitioned_by = ARRAY['region'])")

            // Without hive_partitioning we can't know which partition the opaque file belongs to.
            assertThatThrownBy {
                computeActual("CALL ducklake.system.add_files("
                        + "schema_name => 'test_schema', table_name => '$table', "
                        + "files => ARRAY['/nonexistent.db'], file_format => 'duckdb')")
            }.hasMessageContaining("requires hive_partitioning => true")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun duckDbAddFilesIntoPartitionedTableWithHivePartitioning() {
        val table = "duckdb_added_part"
        // Write a .db file under a hive-style region=US/ directory. Opaque (duckdb) files are read
        // column-projected by the DuckDB engine, so the partition column must be PRESENT in the
        // file; hive_partitioning records the value from the path so the file is PRUNABLE.
        val dir = Files.createTempDirectory("duckdb-add-part").resolve("region=US")
        Files.createDirectories(dir)
        val file = dir.resolve("data.db")
        writeDuckDbFile(file, "SELECT * FROM (VALUES (1, 'US'), (2, 'US')) v(id, region)")
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR) " +
                    "WITH (partitioned_by = ARRAY['region'])")
            val filePath = file.toAbsolutePath().toString().replace("'", "''")
            computeActual("CALL ducklake.system.add_files("
                    + "schema_name => 'test_schema', table_name => '$table', "
                    + "files => ARRAY['$filePath'], hive_partitioning => true, file_format => 'duckdb')")

            assertThat(computeActual("SELECT id, region FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() to it.getField(1) as String })
                    .containsExactly(1L to "US", 2L to "US")

            // Partition pruning resolves the registered file's stored partition value.
            assertThat(computeActual("SELECT id FROM $table WHERE region = 'US' ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 2L)
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE region = 'EU'") as Long)
                    .`as`("pruning excludes the US file for a non-matching partition predicate").isEqualTo(0L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun addFilesRejectsUnknownFormat() {
        val table = "duckdb_bad_format"
        try {
            computeActual("CREATE TABLE $table (id INTEGER)")
            assertThatThrownBy {
                computeActual("CALL ducklake.system.add_files("
                        + "schema_name => 'test_schema', table_name => '$table', "
                        + "files => ARRAY['/nonexistent.db'], file_format => 'sqlite')")
            }.hasMessageContaining("file_format must be 'parquet', 'lance', 'vortex', or 'duckdb'")
        }
        finally {
            tryDropTable(table)
        }
    }
}
