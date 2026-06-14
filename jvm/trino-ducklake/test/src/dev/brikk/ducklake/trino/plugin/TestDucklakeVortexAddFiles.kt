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
 * `CALL ducklake.system.add_files(..., file_format => 'vortex')` — the vortex sibling of
 * [TestDucklakeLanceAddFiles]: registers an **externally-written** single `.vortex` file
 * (DuckDB `COPY ... (FORMAT vortex)`) without rewriting, then SELECTs it back through the
 * FileScan `read_vortex` path. The catalog must record `file_format='vortex'`, the scanned
 * `record_count`, and the real file size (vortex is a single file, so size is exact — not
 * the lance directory best-effort).
 *
 * Also pins the v1 gates shared with lance: partitioned tables and `hive_partitioning`
 * are rejected with clean errors.
 *
 * Network-gated: the fixture write and the registration scan both need `INSTALL vortex`;
 * skips when unavailable. SAME_THREAD: writes to the shared catalog; concurrent commits
 * would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeVortexAddFiles : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "integration-vortex-add-files"

    @Test
    fun addFilesRegistersVortexFileThenSelects() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("vortex")
        val table = "vortex_added"

        // Write a .vortex file out-of-band via raw DuckDB. Column names (id, name) must match
        // the table — vortex reads project by name through read_vortex.
        val dir = Files.createTempDirectory("vortex-add-files")
        val file = dir.resolve("$table.vortex")
        val escaped = file.toString().replace("'", "''")
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("INSTALL vortex")
                s.execute("LOAD vortex")
                s.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) v(id, name)")
                s.execute("COPY t TO '$escaped' (FORMAT vortex)")
            }
        }

        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            val filePath = file.toAbsolutePath().toString().replace("'", "''")
            computeActual(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => '$table', "
                            + "files => ARRAY['$filePath'], "
                            + "file_format => 'vortex')")

            // Catalog must record the registered file as vortex-format with the scanned row
            // count and the real on-disk size.
            val files = computeActual(
                    "SELECT file_format, record_count, file_size_bytes FROM \"$table\$files\"")
            assertThat(files.rowCount).isEqualTo(1)
            assertThat(files.materializedRows[0].getField(0)).isEqualTo("vortex")
            assertThat((files.materializedRows[0].getField(1) as Number).toLong())
                    .`as`("add_files must source record_count by scanning the vortex file")
                    .isEqualTo(3L)
            assertThat((files.materializedRows[0].getField(2) as Number).toLong())
                    .isEqualTo(Files.size(file))

            // Read back through the FileScan(read_vortex) dispatch.
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(3L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alpha", "beta", "gamma")

            // A predicate over the vortex read must filter correctly.
            assertThat(computeActual("SELECT id FROM $table WHERE name = 'beta'").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(2L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun vortexAddFilesRejectsPartitionedTablesAndHivePartitioning() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("vortex")
        val table = "vortex_added_part"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR) " +
                    "WITH (partitioned_by = ARRAY['region'])")

            assertThatThrownBy {
                computeActual("CALL ducklake.system.add_files("
                        + "schema_name => 'test_schema', table_name => '$table', "
                        + "files => ARRAY['/nonexistent.vortex'], file_format => 'vortex')")
            }.hasMessageContaining("does not support partitioned tables")

            assertThatThrownBy {
                computeActual("CALL ducklake.system.add_files("
                        + "schema_name => 'test_schema', table_name => '$table', "
                        + "files => ARRAY['/nonexistent.vortex'], "
                        + "hive_partitioning => true, file_format => 'vortex')")
            }.hasMessageContaining("does not support hive_partitioning")
        }
        finally {
            tryDropTable(table)
        }
    }
}
