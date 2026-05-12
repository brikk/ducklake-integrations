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

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the {@code CALL ducklake.system.add_files(...)} procedure end-to-end.
 * Mirrors upstream's {@code test/sql/add_files/} corpus:
 * <ul>
 *   <li>Round-trip register → read</li>
 *   <li>Column reorder between parquet schema and table schema</li>
 *   <li>Missing / extra column handling (with and without flags)</li>
 *   <li>Mapping_id dedup across same-schema files</li>
 *   <li>Hive partitioning (IDENTITY transform)</li>
 * </ul>
 *
 * <p>Each test follows the same pattern: use Trino's INSERT to materialize a parquet
 * file on disk (the simplest portable way to produce a valid DuckLake-compatible
 * parquet under test), then register that file as a data file of a separately-created
 * target table via {@code add_files}.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeAddFiles
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "write-add-files";
    }

    // ==================== Helpers ====================

    /**
     * Absolute path on disk for the (only) parquet file backing the given (unqualified)
     * table in the default {@code test_schema}. The {@code path} stored in
     * {@code ducklake_data_file} is relative to the table's data sub-directory (which
     * itself sits under schema-then-catalog data paths); rather than reconstruct the
     * full layout, we locate the file on disk by its (UUID-based, unique) basename.
     */
    private String singleFileAbsolutePath(String unqualifiedTable)
    {
        MaterializedResult files = computeActual(
                "SELECT path FROM \"" + unqualifiedTable + "$files\"");
        assertThat(files.getRowCount())
                .as("expected exactly one file backing %s", unqualifiedTable)
                .isEqualTo(1);
        String storedPath = (String) files.getMaterializedRows().get(0).getField(0);
        Path absolute = Paths.get(storedPath).isAbsolute()
                ? Paths.get(storedPath)
                : findByBasename(dataPathDir(), Paths.get(storedPath).getFileName().toString());
        assertThat(Files.exists(absolute))
                .as("expected parquet file to exist at %s", absolute)
                .isTrue();
        return absolute.toAbsolutePath().toString();
    }

    private static Path findByBasename(String rootDir, String basename)
    {
        try (java.util.stream.Stream<Path> walk = Files.walk(Paths.get(rootDir))) {
            return walk
                    .filter(p -> p.getFileName().toString().equals(basename))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "parquet file '" + basename + "' not found under " + rootDir));
        }
        catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Resolves the on-disk data path the connector was configured with. The isolated
     * test harness wires {@code ducklake.data-path} to a per-suite temp directory and
     * stores it as the {@code data_path} key in {@code ducklake_metadata}.
     */
    private String dataPathDir()
    {
        try (java.sql.Connection conn = openCatalogConnection();
                java.sql.Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery(
                        "SELECT value FROM ducklake_metadata WHERE key = 'data_path'")) {
            if (!rs.next()) {
                throw new IllegalStateException("ducklake_metadata.data_path missing");
            }
            return rs.getString(1);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to read data_path from ducklake_metadata", e);
        }
    }

    // ==================== Round-trip ====================

    @Test
    public void testAddFilesBasicRoundTrip()
    {
        computeActual("CREATE TABLE test_schema.add_files_src (id INTEGER, name VARCHAR)");
        computeActual("CREATE TABLE test_schema.add_files_dst (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.add_files_src VALUES (1, 'alice'), (2, 'bob')");
            String fileAbs = singleFileAbsolutePath("add_files_src");

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'add_files_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs));

            MaterializedResult result = computeActual(
                    "SELECT id, name FROM test_schema.add_files_dst ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("bob");

            // Confirm the registered file appears in $files with the absolute path
            MaterializedResult files = computeActual(
                    "SELECT path FROM \"add_files_dst$files\"");
            assertThat(files.getRowCount()).isEqualTo(1);
            assertThat((String) files.getMaterializedRows().get(0).getField(0))
                    .isEqualTo(fileAbs);
        }
        finally {
            tryDropTable("test_schema.add_files_src");
            tryDropTable("test_schema.add_files_dst");
        }
    }

    @Test
    public void testAddFilesEmptyFilesArrayRejected()
    {
        computeActual("CREATE TABLE test_schema.add_files_dst_empty (id INTEGER)");
        try {
            assertThatThrownBy(() -> computeActual(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'add_files_dst_empty', "
                            + "files => ARRAY[])"))
                    .hasMessageContaining("non-empty array");
        }
        finally {
            tryDropTable("test_schema.add_files_dst_empty");
        }
    }

    @Test
    public void testAddFilesNonExistentFileRejected()
    {
        computeActual("CREATE TABLE test_schema.add_files_dst_missing (id INTEGER)");
        try {
            assertThatThrownBy(() -> computeActual(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'add_files_dst_missing', "
                            + "files => ARRAY['/nonexistent/path/foo.parquet'])"))
                    .hasMessageContaining("does not exist");
        }
        finally {
            tryDropTable("test_schema.add_files_dst_missing");
        }
    }

    @Test
    public void testAddFilesNonExistentTableRejected()
    {
        assertThatThrownBy(() -> computeActual(
                "CALL ducklake.system.add_files("
                        + "schema_name => 'test_schema', "
                        + "table_name => 'definitely_does_not_exist', "
                        + "files => ARRAY['/tmp/x.parquet'])"))
                .hasMessageContaining("Table not found");
    }

    // ==================== Column reorder ====================

    @Test
    public void testAddFilesColumnReorderByName()
    {
        // Source: write the same logical columns in (name, id) order
        computeActual("CREATE TABLE test_schema.reorder_src (name VARCHAR, id INTEGER)");
        computeActual("CREATE TABLE test_schema.reorder_dst (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.reorder_src VALUES ('alice', 1), ('bob', 2)");
            String fileAbs = singleFileAbsolutePath("reorder_src");

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'reorder_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs));

            MaterializedResult result = computeActual(
                    "SELECT id, name FROM test_schema.reorder_dst ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("alice");
        }
        finally {
            tryDropTable("test_schema.reorder_src");
            tryDropTable("test_schema.reorder_dst");
        }
    }

    // ==================== Missing columns ====================

    @Test
    public void testAddFilesMissingColumnRejectedWithoutFlag()
    {
        computeActual("CREATE TABLE test_schema.missing_src (j INTEGER)");
        computeActual("CREATE TABLE test_schema.missing_dst (i INTEGER, j INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.missing_src VALUES (42)");
            String fileAbs = singleFileAbsolutePath("missing_src");

            assertThatThrownBy(() -> computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'missing_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs)))
                    .hasMessageContaining("not found in file");
        }
        finally {
            tryDropTable("test_schema.missing_src");
            tryDropTable("test_schema.missing_dst");
        }
    }

    @Test
    public void testAddFilesMissingColumnAcceptedWithFlag()
    {
        computeActual("CREATE TABLE test_schema.missing_src_ok (j INTEGER)");
        computeActual("CREATE TABLE test_schema.missing_dst_ok (i INTEGER, j INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.missing_src_ok VALUES (42)");
            String fileAbs = singleFileAbsolutePath("missing_src_ok");

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'missing_dst_ok', "
                            + "files => ARRAY['%s'], "
                            + "allow_missing => true)",
                    fileAbs));

            MaterializedResult result = computeActual(
                    "SELECT i, j FROM test_schema.missing_dst_ok");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isNull();
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(42);
        }
        finally {
            tryDropTable("test_schema.missing_src_ok");
            tryDropTable("test_schema.missing_dst_ok");
        }
    }

    // ==================== Extra columns ====================

    @Test
    public void testAddFilesExtraColumnRejectedWithoutFlag()
    {
        computeActual("CREATE TABLE test_schema.extra_src (i INTEGER, j INTEGER, x INTEGER)");
        computeActual("CREATE TABLE test_schema.extra_dst (i INTEGER, j INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.extra_src VALUES (1, 2, 100)");
            String fileAbs = singleFileAbsolutePath("extra_src");

            assertThatThrownBy(() -> computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'extra_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs)))
                    .hasMessageContaining("not found in table");
        }
        finally {
            tryDropTable("test_schema.extra_src");
            tryDropTable("test_schema.extra_dst");
        }
    }

    @Test
    public void testAddFilesExtraColumnAcceptedWithFlag()
    {
        computeActual("CREATE TABLE test_schema.extra_src_ok (i INTEGER, j INTEGER, x INTEGER)");
        computeActual("CREATE TABLE test_schema.extra_dst_ok (i INTEGER, j INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.extra_src_ok VALUES (1, 2, 100)");
            String fileAbs = singleFileAbsolutePath("extra_src_ok");

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'extra_dst_ok', "
                            + "files => ARRAY['%s'], "
                            + "ignore_extra_columns => true)",
                    fileAbs));

            MaterializedResult result = computeActual(
                    "SELECT i, j FROM test_schema.extra_dst_ok");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.extra_src_ok");
            tryDropTable("test_schema.extra_dst_ok");
        }
    }

    // ==================== Mapping dedup ====================

    @Test
    public void testAddFilesMappingDedupForSameSchema()
    {
        // Two source files with identical parquet schema → registered together must
        // share one ducklake_column_mapping row. Mirrors upstream's
        // "COUNT(*) FROM ducklake_column_mapping == 1" assertion in add_files.test.
        computeActual("CREATE TABLE test_schema.dedup_src1 (id INTEGER, name VARCHAR)");
        computeActual("CREATE TABLE test_schema.dedup_src2 (id INTEGER, name VARCHAR)");
        computeActual("CREATE TABLE test_schema.dedup_dst (id INTEGER, name VARCHAR)");
        try {
            computeActual("INSERT INTO test_schema.dedup_src1 VALUES (1, 'alice')");
            computeActual("INSERT INTO test_schema.dedup_src2 VALUES (2, 'bob')");
            String f1 = singleFileAbsolutePath("dedup_src1");
            String f2 = singleFileAbsolutePath("dedup_src2");

            int mappingsBefore = countColumnMappings();

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'dedup_dst', "
                            + "files => ARRAY['%s', '%s'])",
                    f1, f2));

            int mappingsAfter = countColumnMappings();
            assertThat(mappingsAfter - mappingsBefore)
                    .as("only one column_mapping row should be inserted for two identical-schema files")
                    .isEqualTo(1);

            MaterializedResult result = computeActual(
                    "SELECT id, name FROM test_schema.dedup_dst ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.dedup_src1");
            tryDropTable("test_schema.dedup_src2");
            tryDropTable("test_schema.dedup_dst");
        }
    }

    private int countColumnMappings()
    {
        try (java.sql.Connection conn = openCatalogConnection();
                java.sql.Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM ducklake_column_mapping")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to count ducklake_column_mapping rows", e);
        }
    }

    // ==================== Duplicate paths within one call ====================

    @Test
    public void testAddFilesDuplicatePathDedup()
    {
        computeActual("CREATE TABLE test_schema.duppath_src (id INTEGER)");
        computeActual("CREATE TABLE test_schema.duppath_dst (id INTEGER)");
        try {
            computeActual("INSERT INTO test_schema.duppath_src VALUES (1), (2), (3)");
            String fileAbs = singleFileAbsolutePath("duppath_src");

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'duppath_dst', "
                            + "files => ARRAY['%s', '%s'])",
                    fileAbs, fileAbs));

            // Both entries dedupe to one file → 3 rows total, not 6
            MaterializedResult result = computeActual(
                    "SELECT COUNT(*) FROM test_schema.duppath_dst");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(3L);
        }
        finally {
            tryDropTable("test_schema.duppath_src");
            tryDropTable("test_schema.duppath_dst");
        }
    }

    // ==================== Hive partitioning (IDENTITY) ====================

    @Test
    public void testAddFilesHivePartitioningIdentity()
            throws Exception
    {
        // Source: parquet file with just `val` (no partition column inside), placed under a
        // hive-style {@code region=us/} subdir. With {@code hive_partitioning => true},
        // add_files writes a {@code ducklake_file_partition_value} row and the page source
        // provider projects the catalog's value as a constant for the partition column
        // (which isn't present in the parquet body).
        computeActual("CREATE TABLE test_schema.hive_src (val VARCHAR)");
        computeActual("CREATE TABLE test_schema.hive_dst (region VARCHAR, val VARCHAR) " +
                "WITH (partitioned_by = ARRAY['region'])");
        try {
            computeActual("INSERT INTO test_schema.hive_src VALUES ('hello'), ('world')");
            String srcFile = singleFileAbsolutePath("hive_src");

            Path srcPath = Paths.get(srcFile);
            Path destDir = srcPath.getParent().resolve("region=us");
            Files.createDirectories(destDir);
            Path destPath = destDir.resolve("data.parquet");
            Files.copy(srcPath, destPath);

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'hive_dst', "
                            + "files => ARRAY['%s'], "
                            + "hive_partitioning => true)",
                    destPath.toAbsolutePath()));

            // Read-side: partition column projects as the catalog-recorded value, not NULL.
            MaterializedResult result = computeActual(
                    "SELECT region, val FROM test_schema.hive_dst ORDER BY val");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("us");
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("hello");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo("us");
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("world");

            // Partition pushdown still prunes — a predicate matching the value keeps the
            // file, a non-matching predicate drops it.
            MaterializedResult matchedCount = computeActual(
                    "SELECT COUNT(*) FROM test_schema.hive_dst WHERE region = 'us'");
            assertThat(matchedCount.getMaterializedRows().get(0).getField(0)).isEqualTo(2L);

            MaterializedResult prunedCount = computeActual(
                    "SELECT COUNT(*) FROM test_schema.hive_dst WHERE region = 'eu'");
            assertThat(prunedCount.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);
        }
        finally {
            tryDropTable("test_schema.hive_src");
            tryDropTable("test_schema.hive_dst");
        }
    }

    // ==================== Name-map consulted at read time ====================

    @Test
    public void testAddFilesReadConsultsNameMapForCaseDifferingColumns()
            throws Exception
    {
        // Source file written by DuckDB with an UPPERCASE column name. Trino's reader
        // does case-sensitive lookup against the parquet schema, so without the name_map
        // consultation it would return NULL for the lowercase table column. The
        // name_map records source_name="ID" -> target_field_id=<id column's field_id>,
        // and the page source uses that to find the column at read time.
        java.nio.file.Path duckdbOutputDir = Paths.get("build", "test-data",
                "test-catalog-isolated-write-add-files", "add_files_case");
        Files.createDirectories(duckdbOutputDir);
        java.nio.file.Path parquetPath = duckdbOutputDir.resolve("upper.parquet").toAbsolutePath();

        // Write the parquet file via DuckDB's COPY (in-memory DuckDB — no catalog
        // attach needed since we're just emitting raw parquet).
        try (java.sql.Connection duckdb = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                java.sql.Statement stmt = duckdb.createStatement()) {
            stmt.execute(String.format(
                    "COPY (SELECT 1 AS \"ID\", 'alice' AS \"NAME\" UNION ALL "
                            + "SELECT 2 AS \"ID\", 'bob' AS \"NAME\") "
                            + "TO '%s' (FORMAT PARQUET)",
                    parquetPath));
        }

        computeActual("CREATE TABLE test_schema.case_dst (id INTEGER, name VARCHAR)");
        try {
            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'case_dst', "
                            + "files => ARRAY['%s'])",
                    parquetPath));

            MaterializedResult result = computeActual(
                    "SELECT id, name FROM test_schema.case_dst ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo("alice");
            assertThat(result.getMaterializedRows().get(1).getField(0)).isEqualTo(2);
            assertThat(result.getMaterializedRows().get(1).getField(1)).isEqualTo("bob");
        }
        finally {
            tryDropTable("test_schema.case_dst");
        }
    }

    // ==================== List/Struct nested types ====================

    @Test
    public void testAddFilesNestedStruct()
    {
        // Reorder struct fields in source vs. table: source writes {col1: ROW(j, i)}, table
        // declares ROW(i, j). Mapping should still align by field name.
        computeActual("CREATE TABLE test_schema.nested_src (data ROW(j INTEGER, i INTEGER))");
        computeActual("CREATE TABLE test_schema.nested_dst (data ROW(i INTEGER, j INTEGER))");
        try {
            computeActual("INSERT INTO test_schema.nested_src SELECT CAST(ROW(20, 10) AS ROW(j INTEGER, i INTEGER))");
            String fileAbs = singleFileAbsolutePath("nested_src");

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'nested_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs));

            MaterializedResult result = computeActual(
                    "SELECT data.i, data.j FROM test_schema.nested_dst");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(10);
            assertThat(result.getMaterializedRows().get(0).getField(1)).isEqualTo(20);
        }
        finally {
            tryDropTable("test_schema.nested_src");
            tryDropTable("test_schema.nested_dst");
        }
    }
}
