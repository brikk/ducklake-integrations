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
package dev.brikk.ducklake.trino.plugin;

import io.trino.Session;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.List;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * DDL surface tests for "create something / drop something / verify schema": CREATE/DROP
 * SCHEMA, CREATE/DROP TABLE on simple/nested/partitioned/wide-types tables. Asserts the
 * connector returns the expected DESCRIBE/SHOW shape and that DROP TABLE leaves the table
 * visible at older snapshots via {@code READ_SNAPSHOT_ID} (proving DDL participates in the
 * snapshot lineage rather than mutating in place).
 *
 * <p>ALTER TABLE flows live in {@link TestDucklakeAlterTable}; snapshot/schema_versions
 * tracking lives in {@link TestDucklakeSnapshotAndSchemaVersion}.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeSchemaAndTableDdl
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "ddl-schema-and-table";
    }

    // ==================== Schema DDL ====================

    @Test
    public void testCreateAndDropSchema()
    {
        computeActual("CREATE SCHEMA new_schema");

        List<String> schemas = computeActual("SHOW SCHEMAS").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(schemas).contains("new_schema");

        computeActual("DROP SCHEMA new_schema");

        List<String> schemasAfterDrop = computeActual("SHOW SCHEMAS").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(schemasAfterDrop).doesNotContain("new_schema");
    }

    @Test
    public void testDropSchemaWithTablesFails()
    {
        computeActual("CREATE SCHEMA schema_with_table");
        try {
            computeActual("CREATE TABLE schema_with_table.t1 (id INTEGER, name VARCHAR)");
            try {
                assertThatThrownBy(() -> computeActual("DROP SCHEMA schema_with_table"))
                        .hasMessageContaining("non-empty");
            }
            finally {
                computeActual("DROP TABLE schema_with_table.t1");
            }
        }
        finally {
            tryDropSchema("schema_with_table");
        }
    }

    // ==================== Table DDL (simple) ====================

    @Test
    public void testCreateAndDropSimpleTable()
    {
        computeActual("CREATE TABLE test_schema.simple_write_test (id INTEGER, name VARCHAR, price DOUBLE, active BOOLEAN)");
        try {
            // Verify table appears in SHOW TABLES
            List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(tables).contains("simple_write_test");

            // Verify column metadata
            MaterializedResult columns = computeActual("DESCRIBE test_schema.simple_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "name", "price", "active");

            // Verify it's an empty table
            MaterializedResult data = computeActual("SELECT count(*) FROM test_schema.simple_write_test");
            assertThat(data.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);
        }
        finally {
            tryDropTable("test_schema.simple_write_test");
        }
    }

    @Test
    public void testDropTableMakesItInvisible()
    {
        computeActual("CREATE TABLE test_schema.drop_test (id INTEGER)");

        // Get snapshot before drop
        MaterializedResult snapshot = computeActual("SELECT max(snapshot_id) FROM \"simple_table$snapshots\"");
        long snapshotBeforeDrop = (long) snapshot.getMaterializedRows().get(0).getField(0);

        computeActual("DROP TABLE test_schema.drop_test");

        // Table gone at current snapshot
        List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(tables).doesNotContain("drop_test");

        // Table still visible at old snapshot via time travel
        Session oldSnapshot = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_ID, String.valueOf(snapshotBeforeDrop))
                .build();
        List<String> oldTables = computeActual(oldSnapshot, "SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(oldTables).contains("drop_test");
    }

    // ==================== Table DDL (nested types) ====================

    @Test
    public void testCreateTableWithArrayType()
    {
        computeActual("CREATE TABLE test_schema.array_write_test (id INTEGER, tags ARRAY(VARCHAR))");
        try {
            MaterializedResult columns = computeActual("DESCRIBE test_schema.array_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "tags");

            // Check the type is correct
            String tagsType = columns.getMaterializedRows().get(1).getField(1).toString();
            assertThat(tagsType).isEqualTo("array(varchar)");
        }
        finally {
            tryDropTable("test_schema.array_write_test");
        }
    }

    @Test
    public void testCreateTableWithStructType()
    {
        computeActual("CREATE TABLE test_schema.struct_write_test (id INTEGER, metadata ROW(key VARCHAR, value VARCHAR))");
        try {
            MaterializedResult columns = computeActual("DESCRIBE test_schema.struct_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "metadata");

            String metadataType = columns.getMaterializedRows().get(1).getField(1).toString();
            assertThat(metadataType).isEqualTo("row(\"key\" varchar, \"value\" varchar)");
        }
        finally {
            tryDropTable("test_schema.struct_write_test");
        }
    }

    @Test
    public void testCreateTableWithMapType()
    {
        computeActual("CREATE TABLE test_schema.map_write_test (id INTEGER, attributes MAP(VARCHAR, VARCHAR))");
        try {
            MaterializedResult columns = computeActual("DESCRIBE test_schema.map_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "attributes");

            String mapType = columns.getMaterializedRows().get(1).getField(1).toString();
            assertThat(mapType).isEqualTo("map(varchar, varchar)");
        }
        finally {
            tryDropTable("test_schema.map_write_test");
        }
    }

    // ==================== Table DDL (partitioned) ====================

    @Test
    public void testCreateTableWithIdentityPartition()
    {
        computeActual("CREATE TABLE test_schema.partitioned_write_test (id INTEGER, region VARCHAR, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            // Verify table exists
            List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(tables).contains("partitioned_write_test");

            // Verify columns
            MaterializedResult columns = computeActual("DESCRIBE test_schema.partitioned_write_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "region", "amount");
        }
        finally {
            tryDropTable("test_schema.partitioned_write_test");
        }
    }

    @Test
    public void testCreateTableWithTemporalPartitions()
    {
        computeActual("CREATE TABLE test_schema.temporal_part_test (id INTEGER, event_date DATE, amount DOUBLE) " +
                "WITH (partitioned_by = ARRAY['year(event_date)', 'month(event_date)'])");
        try {
            List<String> tables = computeActual("SHOW TABLES FROM test_schema").getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(tables).contains("temporal_part_test");

            MaterializedResult columns = computeActual("DESCRIBE test_schema.temporal_part_test");
            List<String> columnNames = columns.getMaterializedRows().stream()
                    .map(row -> row.getField(0).toString())
                    .toList();
            assertThat(columnNames).containsExactly("id", "event_date", "amount");
        }
        finally {
            tryDropTable("test_schema.temporal_part_test");
        }
    }

    // ==================== Wide types ====================

    @Test
    public void testCreateTableWithWideTypes()
    {
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
                ")");
        try {
            MaterializedResult columns = computeActual("DESCRIBE test_schema.wide_type_test");
            assertThat(columns.getRowCount()).isEqualTo(12);
        }
        finally {
            tryDropTable("test_schema.wide_type_test");
        }
    }
}
