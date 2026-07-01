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
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.nio.file.Files
import java.sql.DriverManager

/**
 * End-to-end SQL-level Lance read through the connector's `createPageSource` dispatch — the layer
 * the executor-level probe ([TestDucklakeLanceFileScanRead]) can't reach. Since the Lance writer is
 * still Phase A4, this registers an **externally-written** `.lance` dataset directory via
 * `CALL ducklake.system.add_files(..., file_format => 'lance')` (the cheapest enabler, HANDOFF
 * Step 5), then `SELECT`s it back: catalog records `file_format='lance'` + the scanned
 * `record_count`, and reads/predicates flow through the FileScan `__lance_scan` path.
 *
 * Network/platform-gated: writing the fixture needs `INSTALL lance` (404 on osx_amd64); skips if
 * unavailable. The catalog is a PostgreSQL testcontainer (multi-arch) and reads run in-process, so
 * this runs on Apple Silicon — it does NOT touch the amd64-only Quack parity container.
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeLanceAddFiles : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "integration-lance-add-files"

    @Test
    fun addFilesRegistersLanceDatasetThenSelects() {
        assumeLanceExtensionAvailable()
        val table = "lance_added"

        // Write a Lance dataset *directory* out-of-band via raw DuckDB. Column names (id, name)
        // must match the table — Lance reads project by name through __lance_scan.
        val dir = Files.createTempDirectory("lance-add-files")
        val dataset = dir.resolve("$table.lance")
        val escaped = dataset.toString().replace("'", "''")
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("INSTALL lance")
                s.execute("LOAD lance")
                s.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')) v(id, name)")
                s.execute("COPY t TO '$escaped' (FORMAT lance)")
            }
        }

        try {
            computeActual("CREATE TABLE $table (id INTEGER, name VARCHAR)")
            val datasetPath = dataset.toAbsolutePath().toString().replace("'", "''")
            computeActual(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => '$table', "
                            + "files => ARRAY['$datasetPath'], "
                            + "file_format => 'lance')")

            // Catalog must record the registered file as lance-format with the scanned row count.
            assertThat(computeScalar("SELECT DISTINCT file_format FROM \"$table\$files\"") as String)
                    .isEqualTo("lance")
            assertThat((computeScalar("SELECT sum(record_count) FROM \"$table\$files\"") as Number).toLong())
                    .`as`("add_files must source record_count by scanning the lance dataset")
                    .isEqualTo(3L)

            // Read back through the FileScan(__lance_scan) dispatch.
            assertThat(computeScalar("SELECT count(*) FROM $table") as Long).isEqualTo(3L)
            assertThat(computeActual("SELECT name FROM $table ORDER BY id").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly("alpha", "beta", "gamma")

            // A predicate over the lance read must push down and filter correctly.
            assertThat(computeActual("SELECT id FROM $table WHERE name = 'beta'").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(2L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun addFilesRegistersPartitionedLanceDatasetWithHivePartitioning() {
        assumeLanceExtensionAvailable()
        val table = "lance_added_part"
        // Lance dataset directory under a hive-style region=US/ path. The partition column is
        // present in the dataset (opaque files are column-projected by the DuckDB engine);
        // hive_partitioning records the value from the path so the file is prunable.
        val base = Files.createTempDirectory("lance-add-part").resolve("region=US")
        Files.createDirectories(base)
        val dataset = base.resolve("data.lance")
        val escaped = dataset.toString().replace("'", "''")
        DriverManager.getConnection("jdbc:duckdb:").use { c ->
            c.createStatement().use { s ->
                s.execute("INSTALL lance")
                s.execute("LOAD lance")
                s.execute("COPY (SELECT * FROM (VALUES (1, 'US'), (2, 'US')) v(id, region)) TO '$escaped' (FORMAT lance)")
            }
        }
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR) WITH (partitioned_by = ARRAY['region'])")
            val datasetPath = dataset.toAbsolutePath().toString().replace("'", "''")
            computeActual("CALL ducklake.system.add_files("
                    + "schema_name => 'test_schema', table_name => '$table', "
                    + "files => ARRAY['$datasetPath'], hive_partitioning => true, file_format => 'lance')")

            assertThat(computeActual("SELECT id, region FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() to it.getField(1) as String })
                    .containsExactly(1L to "US", 2L to "US")
            assertThat(computeActual("SELECT id FROM $table WHERE region = 'US' ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 2L)
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE region = 'EU'") as Long)
                    .`as`("pruning excludes the US dataset for a non-matching partition predicate").isEqualTo(0L)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun assumeLanceExtensionAvailable() {
        try {
            DriverManager.getConnection("jdbc:duckdb:").use { c ->
                c.createStatement().use { s ->
                    s.execute("INSTALL lance")
                    s.execute("LOAD lance")
                }
            }
        }
        catch (e: Exception) {
            assumeTrue(false, "lance DuckDB extension unavailable (offline / unsupported platform — 404 on osx_amd64): ${e.message}")
        }
    }
}
