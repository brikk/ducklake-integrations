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

import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.READ_SNAPSHOT_ID
import io.trino.Session
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * DDL surface tests for "create something / drop something / verify schema": CREATE/DROP
 * SCHEMA, CREATE/DROP TABLE on simple/nested/partitioned/wide-types tables. Asserts the
 * connector returns the expected DESCRIBE/SHOW shape and that DROP TABLE leaves the table
 * visible at older snapshots via `READ_SNAPSHOT_ID` (proving DDL participates in the
 * snapshot lineage rather than mutating in place).
 *
 *
 * ALTER TABLE flows live in [TestDucklakeAlterTable]; snapshot/schema_versions
 * tracking lives in `TestDucklakeSnapshotAndSchemaVersion`.
 */
@Execution(ExecutionMode.SAME_THREAD)
open class TestDucklakeSchemaAndTableDdl : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String {
        return "ddl-schema-and-table"
    }

    // ==================== Schema DDL ====================

    @Test
    fun testCreateAndDropSchema() {
        computeActual("CREATE SCHEMA new_schema")

        val schemas = computeActual("SHOW SCHEMAS").materializedRows.map { it.getField(0).toString() }
        assertThat(schemas).contains("new_schema")

        computeActual("DROP SCHEMA new_schema")

        val schemasAfterDrop = computeActual("SHOW SCHEMAS").materializedRows.map { it.getField(0).toString() }
        assertThat(schemasAfterDrop).doesNotContain("new_schema")
    }

    @Test
    fun testDropSchemaWithTablesFails() {
        computeActual("CREATE SCHEMA schema_with_table")
        try {
            computeActual("CREATE TABLE schema_with_table.t1 (id INTEGER, name VARCHAR)")
            try {
                assertThatThrownBy { computeActual("DROP SCHEMA schema_with_table") }
                    .hasMessageContaining("non-empty")
            }
            finally {
                computeActual("DROP TABLE schema_with_table.t1")
            }
        }
        finally {
            tryDropSchema("schema_with_table")
        }
    }

    // ==================== Table DDL (simple) ====================

    @Test
    fun testCreateAndDropSimpleTable() {
        computeActual("CREATE TABLE test_schema.simple_write_test (id INTEGER, name VARCHAR, price DOUBLE, active BOOLEAN)")
        try {
            // Verify table appears in SHOW TABLES
            val tables = computeActual("SHOW TABLES FROM test_schema").materializedRows.map { it.getField(0).toString() }
            assertThat(tables).contains("simple_write_test")

            // Verify column metadata
            val columns = computeActual("DESCRIBE test_schema.simple_write_test")
            val columnNames = columns.materializedRows.map { it.getField(0).toString() }
            assertThat(columnNames).containsExactly("id", "name", "price", "active")

            // Verify it's an empty table
            val data = computeActual("SELECT count(*) FROM test_schema.simple_write_test")
            assertThat(data.materializedRows[0].getField(0)).isEqualTo(0L)
        }
        finally {
            tryDropTable("test_schema.simple_write_test")
        }
    }

    @Test
    fun testDropTableMakesItInvisible() {
        computeActual("CREATE TABLE test_schema.drop_test (id INTEGER)")

        // Get snapshot before drop
        val snapshot = computeActual("SELECT max(snapshot_id) FROM \"simple_table\$snapshots\"")
        val snapshotBeforeDrop = snapshot.materializedRows[0].getField(0) as Long

        computeActual("DROP TABLE test_schema.drop_test")

        // Table gone at current snapshot
        val tables = computeActual("SHOW TABLES FROM test_schema").materializedRows.map { it.getField(0).toString() }
        assertThat(tables).doesNotContain("drop_test")

        // Table still visible at old snapshot via time travel
        val oldSnapshot = Session.builder(session)
            .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_ID, snapshotBeforeDrop.toString())
            .build()
        val oldTables = computeActual(oldSnapshot, "SHOW TABLES FROM test_schema").materializedRows.map { it.getField(0).toString() }
        assertThat(oldTables).contains("drop_test")
    }

    // ==================== Table DDL (nested types) ====================

    @Test
    fun testCreateTableWithArrayType() {
        computeActual("CREATE TABLE test_schema.array_write_test (id INTEGER, tags ARRAY(VARCHAR))")
        try {
            val columns = computeActual("DESCRIBE test_schema.array_write_test")
            val columnNames = columns.materializedRows.map { it.getField(0).toString() }
            assertThat(columnNames).containsExactly("id", "tags")

            // Check the type is correct
            val tagsType = columns.materializedRows[1].getField(1).toString()
            assertThat(tagsType).isEqualTo("array(varchar)")
        }
        finally {
            tryDropTable("test_schema.array_write_test")
        }
    }

    @Test
    fun testCreateTableWithStructType() {
        computeActual("CREATE TABLE test_schema.struct_write_test (id INTEGER, metadata ROW(key VARCHAR, value VARCHAR))")
        try {
            val columns = computeActual("DESCRIBE test_schema.struct_write_test")
            val columnNames = columns.materializedRows.map { it.getField(0).toString() }
            assertThat(columnNames).containsExactly("id", "metadata")

            val metadataType = columns.materializedRows[1].getField(1).toString()
            assertThat(metadataType).isEqualTo("row(\"key\" varchar, \"value\" varchar)")
        }
        finally {
            tryDropTable("test_schema.struct_write_test")
        }
    }

    @Test
    fun testCreateTableWithMapType() {
        computeActual("CREATE TABLE test_schema.map_write_test (id INTEGER, attributes MAP(VARCHAR, VARCHAR))")
        try {
            val columns = computeActual("DESCRIBE test_schema.map_write_test")
            val columnNames = columns.materializedRows.map { it.getField(0).toString() }
            assertThat(columnNames).containsExactly("id", "attributes")

            val mapType = columns.materializedRows[1].getField(1).toString()
            assertThat(mapType).isEqualTo("map(varchar, varchar)")
        }
        finally {
            tryDropTable("test_schema.map_write_test")
        }
    }

    // ==================== Table DDL (partitioned) ====================

    @Test
    fun testCreateTableWithIdentityPartition() {
        computeActual("CREATE TABLE test_schema.partitioned_write_test (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])")
        try {
            // Verify table exists
            val tables = computeActual("SHOW TABLES FROM test_schema").materializedRows.map { it.getField(0).toString() }
            assertThat(tables).contains("partitioned_write_test")

            // Verify columns
            val columns = computeActual("DESCRIBE test_schema.partitioned_write_test")
            val columnNames = columns.materializedRows.map { it.getField(0).toString() }
            assertThat(columnNames).containsExactly("id", "region", "amount")
        }
        finally {
            tryDropTable("test_schema.partitioned_write_test")
        }
    }

    @Test
    fun testCreateTableWithTemporalPartitions() {
        computeActual("CREATE TABLE test_schema.temporal_part_test (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)', 'month(event_date)'])")
        try {
            val tables = computeActual("SHOW TABLES FROM test_schema").materializedRows.map { it.getField(0).toString() }
            assertThat(tables).contains("temporal_part_test")

            val columns = computeActual("DESCRIBE test_schema.temporal_part_test")
            val columnNames = columns.materializedRows.map { it.getField(0).toString() }
            assertThat(columnNames).containsExactly("id", "event_date", "amount")
        }
        finally {
            tryDropTable("test_schema.temporal_part_test")
        }
    }

    // ==================== Wide types ====================

    @Test
    fun testCreateTableWithWideTypes() {
        computeActual("CREATE TABLE test_schema.wide_type_test (" +
                "col_tinyint TINYINT, " +
                "col_smallint SMALLINT, " +
                "col_int INTEGER, " +
                "col_bigint BIGINT, " +
                "col_real REAL, " +
                "col_double DOUBLE, " +
                "col_decimal DECIMAL(10,2), " +
                "col_varchar VARCHAR, " +
                "col_boolean BOOLEAN, " +
                "col_date DATE, " +
                "col_timestamp TIMESTAMP(6), " +
                "col_timestamptz TIMESTAMP(6) WITH TIME ZONE" +
                ")")
        try {
            val columns = computeActual("DESCRIBE test_schema.wide_type_test")
            assertThat(columns.rowCount).isEqualTo(12)
        }
        finally {
            tryDropTable("test_schema.wide_type_test")
        }
    }
}
