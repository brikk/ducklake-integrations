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
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Partitioned CTAS/INSERT with non-parquet data file formats — the second T1 zero-test
 * unknown. [TestDucklakePartitionedWrite] proves partitioned writes for parquet; the
 * writers all accept partition values for every format, but nothing exercised the
 * combination. Per format (`duckdb`, `vortex`, `lance`):
 *
 *  - identity-partitioned CREATE + INSERT: rows land in per-partition data files of the
 *    requested format under hive-style `region=<value>/` paths, partition-pruned SELECTs
 *    find them, later INSERTs append to existing and new partitions, NULL partition
 *    values round-trip via `IS NULL`,
 *  - partitioned CTAS: same, with partitioning + format declared together at CTAS time.
 *
 * A year() temporal-transform case runs for the duckdb format (transform encoding is
 * format-independent — the partition value computer runs before the writer — so one
 * format suffices for the transform x format interaction).
 *
 * SAME_THREAD: writes to the shared catalog; concurrent commits would race the snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakePartitionedWriteFormats : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "write-partitioned-formats"

    @Test
    fun partitionedInsertDuckDbFormat() {
        partitionedInsertRoundTrip("pf_insert_duckdb", DucklakeSessionProperties.FORMAT_DUCKDB)
    }

    @Test
    fun partitionedCtasDuckDbFormat() {
        partitionedCtasRoundTrip("pf_ctas_duckdb", DucklakeSessionProperties.FORMAT_DUCKDB)
    }

    @Test
    fun partitionedInsertVortexFormat() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("vortex")
        partitionedInsertRoundTrip("pf_insert_vortex", DucklakeSessionProperties.FORMAT_VORTEX)
    }

    @Test
    fun partitionedCtasVortexFormat() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("vortex")
        partitionedCtasRoundTrip("pf_ctas_vortex", DucklakeSessionProperties.FORMAT_VORTEX)
    }

    @Test
    fun partitionedInsertLanceFormat() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("lance")
        partitionedInsertRoundTrip("pf_insert_lance", DucklakeSessionProperties.FORMAT_LANCE)
    }

    @Test
    fun partitionedCtasLanceFormat() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("lance")
        partitionedCtasRoundTrip("pf_ctas_lance", DucklakeSessionProperties.FORMAT_LANCE)
    }

    @Test
    fun partitionedTemporalYearDuckDbFormat() {
        val table = "pf_year_duckdb"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, event_date DATE) " +
                    "WITH (partitioned_by = ARRAY['year(event_date)'], data_file_format = 'duckdb')")
            computeActual("INSERT INTO $table VALUES " +
                    "(1, DATE '2023-01-15'), (2, DATE '2023-06-20'), (3, DATE '2024-03-05')")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)
            assertThat(computeScalar("SELECT count(*) FROM $table " +
                    "WHERE event_date >= DATE '2023-01-01' AND event_date < DATE '2024-01-01'"))
                    .isEqualTo(2L)

            val files = computeActual("SELECT path, file_format FROM \"$table\$files\"").materializedRows
            assertThat(files.map { it.getField(1) as String }).containsOnly("duckdb")
            assertThat(files.size).isGreaterThanOrEqualTo(2)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun partitionedInsertRoundTrip(table: String, format: String) {
        try {
            computeActual("CREATE TABLE $table (id INTEGER, region VARCHAR, amount DOUBLE) " +
                    "WITH (partitioned_by = ARRAY['region'], data_file_format = '$format')")
            computeActual("INSERT INTO $table VALUES " +
                    "(1, 'US', 10.0e0), (2, 'EU', 20.0e0), (3, 'US', 30.0e0), (4, 'APAC', 40.0e0)")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(4L)
            // Partition-pruned point lookups find the right rows.
            assertThat(computeActual("SELECT id FROM $table WHERE region = 'US' ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L)
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE region = 'EU'")).isEqualTo(1L)

            // Every partition gets its own data file(s) — exact counts depend on writer
            // parallelism — all in the requested format, under hive-style paths.
            val files = computeActual("SELECT path, file_format FROM \"$table\$files\"").materializedRows
            assertThat(files.map { it.getField(1) as String }).containsOnly(format)
            val regions = files
                    .map { (it.getField(0) as String) }
                    .map { path -> path.substringAfter("region=").substringBefore("/") }
                    .toSet()
            assertThat(regions).containsExactlyInAnyOrder("US", "EU", "APAC")

            // Later INSERTs append to existing partitions, new partitions, and NULL.
            computeActual("INSERT INTO $table VALUES (5, 'US', 50.0e0), (6, NULL, 60.0e0)")
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE region = 'US'")).isEqualTo(3L)
            assertThat(computeScalar("SELECT count(*) FROM $table WHERE region IS NULL")).isEqualTo(1L)
            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(6L)
            assertThat(computeActual("SELECT DISTINCT file_format FROM \"$table\$files\"").materializedRows
                    .map { it.getField(0) as String })
                    .containsExactly(format)
        }
        finally {
            tryDropTable(table)
        }
    }

    private fun partitionedCtasRoundTrip(table: String, format: String) {
        try {
            computeActual("CREATE TABLE $table " +
                    "WITH (partitioned_by = ARRAY['region'], data_file_format = '$format') AS " +
                    "SELECT * FROM (VALUES (1, 'US'), (2, 'EU'), (3, 'US')) AS t(id, region)")

            assertThat(computeScalar("SELECT count(*) FROM $table")).isEqualTo(3L)
            assertThat(computeActual("SELECT id FROM $table WHERE region = 'US' ORDER BY id").materializedRows
                    .map { (it.getField(0) as Number).toLong() })
                    .containsExactly(1L, 3L)

            val files = computeActual("SELECT path, file_format FROM \"$table\$files\"").materializedRows
            assertThat(files.map { it.getField(1) as String }).containsOnly(format)
            val regions = files
                    .map { (it.getField(0) as String) }
                    .map { path -> path.substringAfter("region=").substringBefore("/") }
                    .toSet()
            assertThat(regions).containsExactlyInAnyOrder("US", "EU")
        }
        finally {
            tryDropTable(table)
        }
    }
}
