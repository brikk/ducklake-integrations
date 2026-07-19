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
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Tests the `CALL ducklake.system.add_files(...)` procedure end-to-end.
 * Mirrors upstream's `test/sql/add_files/` corpus:
 *
 *  * Round-trip register → read
 *  * Column reorder between parquet schema and table schema
 *  * Missing / extra column handling (with and without flags)
 *  * Mapping_id dedup across same-schema files
 *  * Hive partitioning (IDENTITY transform)
 *
 * Each test follows the same pattern: use Trino's INSERT to materialize a parquet
 * file on disk (the simplest portable way to produce a valid DuckLake-compatible
 * parquet under test), then register that file as a data file of a separately-created
 * target table via `add_files`.
 */
@Execution(ExecutionMode.SAME_THREAD)
internal open class TestDucklakeAddFiles
    : AbstractDucklakeIntegrationTest()
{
    override fun isolatedCatalogName(): String
    {
        return "write-add-files"
    }

    // ==================== Helpers ====================

    /**
     * Absolute path on disk for the (only) parquet file backing the given (unqualified)
     * table in the default `test_schema`. The `path` stored in
     * `ducklake_data_file` is relative to the table's data sub-directory (which
     * itself sits under schema-then-catalog data paths); rather than reconstruct the
     * full layout, we locate the file on disk by its (UUID-based, unique) basename.
     */
    private fun singleFileAbsolutePath(unqualifiedTable: String): String
    {
        val files = computeActual(
                "SELECT path FROM \"" + unqualifiedTable + "\$files\"")
        assertThat(files.rowCount)
                .`as`("expected exactly one file backing %s", unqualifiedTable)
                .isEqualTo(1)
        val storedPath = files.materializedRows[0].getField(0) as String
        val absolute = if (Paths.get(storedPath).isAbsolute)
            Paths.get(storedPath)
        else
            findByBasename(dataPathDir(), Paths.get(storedPath).fileName.toString())
        assertThat(Files.exists(absolute))
                .`as`("expected parquet file to exist at %s", absolute)
                .isTrue()
        return absolute.toAbsolutePath().toString()
    }

    /**
     * Resolves the on-disk data path the connector was configured with. The isolated
     * test harness wires `ducklake.data-path` to a per-suite temp directory and
     * stores it as the `data_path` key in `ducklake_metadata`.
     */
    private fun dataPathDir(): String
    {
        try {
            openCatalogConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT value FROM ducklake_metadata WHERE key = 'data_path'").use { rs ->
                        if (!rs.next()) {
                            throw IllegalStateException("ducklake_metadata.data_path missing")
                        }
                        return rs.getString(1)
                    }
                }
            }
        }
        catch (e: Exception) {
            throw RuntimeException("Failed to read data_path from ducklake_metadata", e)
        }
    }

    // ==================== Round-trip ====================

    @Test
    fun testAddFilesBasicRoundTrip()
    {
        computeActual("CREATE TABLE test_schema.add_files_src (id INTEGER, name VARCHAR)")
        computeActual("CREATE TABLE test_schema.add_files_dst (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.add_files_src VALUES (1, 'alice'), (2, 'bob')")
            val fileAbs = singleFileAbsolutePath("add_files_src")

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'add_files_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs))

            val result = computeActual(
                    "SELECT id, name FROM test_schema.add_files_dst ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("alice")
            assertThat(result.materializedRows[1].getField(0)).isEqualTo(2)
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("bob")

            // Confirm the registered file appears in $files with the absolute path
            val files = computeActual(
                    "SELECT path FROM \"add_files_dst\$files\"")
            assertThat(files.rowCount).isEqualTo(1)
            assertThat(files.materializedRows[0].getField(0) as String)
                    .isEqualTo(fileAbs)
        }
        finally {
            tryDropTable("test_schema.add_files_src")
            tryDropTable("test_schema.add_files_dst")
        }
    }

    @Test
    fun testAddFilesEmptyFilesArrayRejected()
    {
        computeActual("CREATE TABLE test_schema.add_files_dst_empty (id INTEGER)")
        try {
            assertThatThrownBy {
                computeActual(
                        "CALL ducklake.system.add_files("
                                + "schema_name => 'test_schema', "
                                + "table_name => 'add_files_dst_empty', "
                                + "files => ARRAY[])")
            }
                    .hasMessageContaining("non-empty array")
        }
        finally {
            tryDropTable("test_schema.add_files_dst_empty")
        }
    }

    @Test
    fun testAddFilesNonExistentFileRejected()
    {
        computeActual("CREATE TABLE test_schema.add_files_dst_missing (id INTEGER)")
        try {
            assertThatThrownBy {
                computeActual(
                        "CALL ducklake.system.add_files("
                                + "schema_name => 'test_schema', "
                                + "table_name => 'add_files_dst_missing', "
                                + "files => ARRAY['/nonexistent/path/foo.parquet'])")
            }
                    .hasMessageContaining("does not exist")
        }
        finally {
            tryDropTable("test_schema.add_files_dst_missing")
        }
    }

    @Test
    fun testAddFilesNonExistentTableRejected()
    {
        assertThatThrownBy {
            computeActual(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'definitely_does_not_exist', "
                            + "files => ARRAY['/tmp/x.parquet'])")
        }
                .hasMessageContaining("Table not found")
    }

    // ==================== Column reorder ====================

    @Test
    fun testAddFilesColumnReorderByName()
    {
        // Source: write the same logical columns in (name, id) order
        computeActual("CREATE TABLE test_schema.reorder_src (name VARCHAR, id INTEGER)")
        computeActual("CREATE TABLE test_schema.reorder_dst (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.reorder_src VALUES ('alice', 1), ('bob', 2)")
            val fileAbs = singleFileAbsolutePath("reorder_src")

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'reorder_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs))

            val result = computeActual(
                    "SELECT id, name FROM test_schema.reorder_dst ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("alice")
        }
        finally {
            tryDropTable("test_schema.reorder_src")
            tryDropTable("test_schema.reorder_dst")
        }
    }

    // ==================== Missing columns ====================

    @Test
    fun testAddFilesMissingColumnRejectedWithoutFlag()
    {
        computeActual("CREATE TABLE test_schema.missing_src (j INTEGER)")
        computeActual("CREATE TABLE test_schema.missing_dst (i INTEGER, j INTEGER)")
        try {
            computeActual("INSERT INTO test_schema.missing_src VALUES (42)")
            val fileAbs = singleFileAbsolutePath("missing_src")

            assertThatThrownBy {
                computeActual(String.format(
                        "CALL ducklake.system.add_files("
                                + "schema_name => 'test_schema', "
                                + "table_name => 'missing_dst', "
                                + "files => ARRAY['%s'])",
                        fileAbs))
            }
                    .hasMessageContaining("not found in file")
        }
        finally {
            tryDropTable("test_schema.missing_src")
            tryDropTable("test_schema.missing_dst")
        }
    }

    @Test
    fun testAddFilesMissingColumnAcceptedWithFlag()
    {
        computeActual("CREATE TABLE test_schema.missing_src_ok (j INTEGER)")
        computeActual("CREATE TABLE test_schema.missing_dst_ok (i INTEGER, j INTEGER)")
        try {
            computeActual("INSERT INTO test_schema.missing_src_ok VALUES (42)")
            val fileAbs = singleFileAbsolutePath("missing_src_ok")

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'missing_dst_ok', "
                            + "files => ARRAY['%s'], "
                            + "allow_missing => true)",
                    fileAbs))

            val result = computeActual(
                    "SELECT i, j FROM test_schema.missing_dst_ok")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(0)).isNull()
            assertThat(result.materializedRows[0].getField(1)).isEqualTo(42)
        }
        finally {
            tryDropTable("test_schema.missing_src_ok")
            tryDropTable("test_schema.missing_dst_ok")
        }
    }

    // ==================== Extra columns ====================

    @Test
    fun testAddFilesExtraColumnRejectedWithoutFlag()
    {
        computeActual("CREATE TABLE test_schema.extra_src (i INTEGER, j INTEGER, x INTEGER)")
        computeActual("CREATE TABLE test_schema.extra_dst (i INTEGER, j INTEGER)")
        try {
            computeActual("INSERT INTO test_schema.extra_src VALUES (1, 2, 100)")
            val fileAbs = singleFileAbsolutePath("extra_src")

            assertThatThrownBy {
                computeActual(String.format(
                        "CALL ducklake.system.add_files("
                                + "schema_name => 'test_schema', "
                                + "table_name => 'extra_dst', "
                                + "files => ARRAY['%s'])",
                        fileAbs))
            }
                    .hasMessageContaining("not found in table")
        }
        finally {
            tryDropTable("test_schema.extra_src")
            tryDropTable("test_schema.extra_dst")
        }
    }

    @Test
    fun testAddFilesExtraColumnAcceptedWithFlag()
    {
        computeActual("CREATE TABLE test_schema.extra_src_ok (i INTEGER, j INTEGER, x INTEGER)")
        computeActual("CREATE TABLE test_schema.extra_dst_ok (i INTEGER, j INTEGER)")
        try {
            computeActual("INSERT INTO test_schema.extra_src_ok VALUES (1, 2, 100)")
            val fileAbs = singleFileAbsolutePath("extra_src_ok")

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'extra_dst_ok', "
                            + "files => ARRAY['%s'], "
                            + "ignore_extra_columns => true)",
                    fileAbs))

            val result = computeActual(
                    "SELECT i, j FROM test_schema.extra_dst_ok")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo(2)
        }
        finally {
            tryDropTable("test_schema.extra_src_ok")
            tryDropTable("test_schema.extra_dst_ok")
        }
    }

    // ==================== Mapping dedup ====================

    @Test
    fun testAddFilesMappingDedupForSameSchema()
    {
        // Two source files with identical parquet schema → registered together must
        // share one ducklake_column_mapping row. Mirrors upstream's
        // "COUNT(*) FROM ducklake_column_mapping == 1" assertion in add_files.test.
        computeActual("CREATE TABLE test_schema.dedup_src1 (id INTEGER, name VARCHAR)")
        computeActual("CREATE TABLE test_schema.dedup_src2 (id INTEGER, name VARCHAR)")
        computeActual("CREATE TABLE test_schema.dedup_dst (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.dedup_src1 VALUES (1, 'alice')")
            computeActual("INSERT INTO test_schema.dedup_src2 VALUES (2, 'bob')")
            val f1 = singleFileAbsolutePath("dedup_src1")
            val f2 = singleFileAbsolutePath("dedup_src2")

            val mappingsBefore = countColumnMappings()

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'dedup_dst', "
                            + "files => ARRAY['%s', '%s'])",
                    f1, f2))

            val mappingsAfter = countColumnMappings()
            assertThat(mappingsAfter - mappingsBefore)
                    .`as`("only one column_mapping row should be inserted for two identical-schema files")
                    .isEqualTo(1)

            val result = computeActual(
                    "SELECT id, name FROM test_schema.dedup_dst ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
        }
        finally {
            tryDropTable("test_schema.dedup_src1")
            tryDropTable("test_schema.dedup_src2")
            tryDropTable("test_schema.dedup_dst")
        }
    }

    private fun countColumnMappings(): Int
    {
        try {
            openCatalogConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT COUNT(*) FROM ducklake_column_mapping").use { rs ->
                        return if (rs.next()) rs.getInt(1) else 0
                    }
                }
            }
        }
        catch (e: Exception) {
            throw RuntimeException("Failed to count ducklake_column_mapping rows", e)
        }
    }

    private fun footerSizeForLatestDataFile(): Long
    {
        try {
            openCatalogConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT footer_size FROM ducklake_data_file ORDER BY data_file_id DESC LIMIT 1").use { rs ->
                        if (!rs.next()) {
                            throw IllegalStateException("no ducklake_data_file rows")
                        }
                        val value = rs.getLong(1)
                        return if (rs.wasNull()) -1 else value
                    }
                }
            }
        }
        catch (e: Exception) {
            throw RuntimeException("Failed to read footer_size from ducklake_data_file", e)
        }
    }

    @Test
    fun testAddFilesPopulatesFooterSize()
    {
        // The footer_size hint stored in ducklake_data_file lets the read path skip the
        // blind 48 KB tail read on subsequent queries (FooterPrefetchingParquetDataSource).
        // Pre-fix this was hardcoded to 0; this test pins the new behavior.
        computeActual("CREATE TABLE test_schema.footer_src (id INTEGER, name VARCHAR)")
        computeActual("CREATE TABLE test_schema.footer_dst (id INTEGER, name VARCHAR)")
        try {
            computeActual("INSERT INTO test_schema.footer_src VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
            val fileAbs = singleFileAbsolutePath("footer_src")

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'footer_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs))

            val footerSize = footerSizeForLatestDataFile()
            assertThat(footerSize)
                    .`as`("add_files must populate a non-zero footer_size (parsed from the parquet post-script)")
                    .isGreaterThan(0L)
            // Sanity ceiling: full file size on disk. Footer is a subset of the file.
            val fileSize = try {
                Files.size(Paths.get(fileAbs))
            }
            catch (e: java.io.IOException) {
                throw RuntimeException(e)
            }
            assertThat(footerSize).isLessThan(fileSize)

            // Read path must still work with the new hint.
            val result = computeActual(
                    "SELECT id, name FROM test_schema.footer_dst ORDER BY id")
            assertThat(result.rowCount).isEqualTo(3)
        }
        finally {
            tryDropTable("test_schema.footer_src")
            tryDropTable("test_schema.footer_dst")
        }
    }

    // ==================== Duplicate paths within one call ====================

    @Test
    fun testAddFilesDuplicatePathDedup()
    {
        computeActual("CREATE TABLE test_schema.duppath_src (id INTEGER)")
        computeActual("CREATE TABLE test_schema.duppath_dst (id INTEGER)")
        try {
            computeActual("INSERT INTO test_schema.duppath_src VALUES (1), (2), (3)")
            val fileAbs = singleFileAbsolutePath("duppath_src")

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'duppath_dst', "
                            + "files => ARRAY['%s', '%s'])",
                    fileAbs, fileAbs))

            // Both entries dedupe to one file → 3 rows total, not 6
            val result = computeActual(
                    "SELECT COUNT(*) FROM test_schema.duppath_dst")
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(3L)
        }
        finally {
            tryDropTable("test_schema.duppath_src")
            tryDropTable("test_schema.duppath_dst")
        }
    }

    // ==================== Hive partitioning (IDENTITY) ====================

    @Test
    @Throws(Exception::class)
    fun testAddFilesHivePartitioningIdentity()
    {
        // Source: parquet file with just `val` (no partition column inside), placed under a
        // hive-style `region=us/` subdir. With `hive_partitioning => true`,
        // add_files writes a `ducklake_file_partition_value` row and the page source
        // provider projects the catalog's value as a constant for the partition column
        // (which isn't present in the parquet body).
        computeActual("CREATE TABLE test_schema.hive_src (val VARCHAR)")
        computeActual("CREATE TABLE test_schema.hive_dst (region VARCHAR, val VARCHAR) " +
                "WITH (partitioned_by = ARRAY['region'])")
        try {
            computeActual("INSERT INTO test_schema.hive_src VALUES ('hello'), ('world')")
            val srcFile = singleFileAbsolutePath("hive_src")

            val srcPath = Paths.get(srcFile)
            val destDir = srcPath.parent.resolve("region=us")
            Files.createDirectories(destDir)
            val destPath = destDir.resolve("data.parquet")
            Files.copy(srcPath, destPath)

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'hive_dst', "
                            + "files => ARRAY['%s'], "
                            + "hive_partitioning => true)",
                    destPath.toAbsolutePath()))

            // Read-side: partition column projects as the catalog-recorded value, not NULL.
            val result = computeActual(
                    "SELECT region, val FROM test_schema.hive_dst ORDER BY val")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo("us")
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("hello")
            assertThat(result.materializedRows[1].getField(0)).isEqualTo("us")
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("world")

            // Partition pushdown still prunes — a predicate matching the value keeps the
            // file, a non-matching predicate drops it.
            val matchedCount = computeActual(
                    "SELECT COUNT(*) FROM test_schema.hive_dst WHERE region = 'us'")
            assertThat(matchedCount.materializedRows[0].getField(0)).isEqualTo(2L)

            val prunedCount = computeActual(
                    "SELECT COUNT(*) FROM test_schema.hive_dst WHERE region = 'eu'")
            assertThat(prunedCount.materializedRows[0].getField(0)).isEqualTo(0L)
        }
        finally {
            tryDropTable("test_schema.hive_src")
            tryDropTable("test_schema.hive_dst")
        }
    }

    // ==================== Name-map consulted at read time ====================

    @Test
    @Throws(Exception::class)
    fun testAddFilesReadConsultsNameMapForCaseDifferingColumns()
    {
        // Source file written by DuckDB with an UPPERCASE column name. Trino's reader
        // does case-sensitive lookup against the parquet schema, so without the name_map
        // consultation it would return NULL for the lowercase table column. The
        // name_map records source_name="ID" -> target_field_id=<id column's field_id>,
        // and the page source uses that to find the column at read time.
        val duckdbOutputDir = Paths.get("build", "test-data",
                "test-catalog-isolated-write-add-files", "add_files_case")
        Files.createDirectories(duckdbOutputDir)
        val parquetPath = duckdbOutputDir.resolve("upper.parquet").toAbsolutePath()

        // Write the parquet file via DuckDB's COPY (in-memory DuckDB — no catalog
        // attach needed since we're just emitting raw parquet).
        java.sql.DriverManager.getConnection("jdbc:duckdb:").use { duckdb ->
            duckdb.createStatement().use { stmt ->
                stmt.execute(String.format(
                        "COPY (SELECT 1 AS \"ID\", 'alice' AS \"NAME\" UNION ALL "
                                + "SELECT 2 AS \"ID\", 'bob' AS \"NAME\") "
                                + "TO '%s' (FORMAT PARQUET)",
                        parquetPath))
            }
        }

        computeActual("CREATE TABLE test_schema.case_dst (id INTEGER, name VARCHAR)")
        try {
            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'case_dst', "
                            + "files => ARRAY['%s'])",
                    parquetPath))

            val result = computeActual(
                    "SELECT id, name FROM test_schema.case_dst ORDER BY id")
            assertThat(result.rowCount).isEqualTo(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo("alice")
            assertThat(result.materializedRows[1].getField(0)).isEqualTo(2)
            assertThat(result.materializedRows[1].getField(1)).isEqualTo("bob")
        }
        finally {
            tryDropTable("test_schema.case_dst")
        }
    }

    // ==================== List/Struct nested types ====================

    @Test
    fun testAddFilesNestedStruct()
    {
        // Reorder struct fields in source vs. table: source writes {col1: ROW(j, i)}, table
        // declares ROW(i, j). Mapping should still align by field name.
        computeActual("CREATE TABLE test_schema.nested_src (data ROW(j INTEGER, i INTEGER))")
        computeActual("CREATE TABLE test_schema.nested_dst (data ROW(i INTEGER, j INTEGER))")
        try {
            computeActual("INSERT INTO test_schema.nested_src SELECT CAST(ROW(20, 10) AS ROW(j INTEGER, i INTEGER))")
            val fileAbs = singleFileAbsolutePath("nested_src")

            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'nested_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs))

            val result = computeActual(
                    "SELECT data.i, data.j FROM test_schema.nested_dst")
            assertThat(result.rowCount).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(10)
            assertThat(result.materializedRows[0].getField(1)).isEqualTo(20)
        }
        finally {
            tryDropTable("test_schema.nested_src")
            tryDropTable("test_schema.nested_dst")
        }
    }

    companion object
    {
        private fun findByBasename(rootDir: String, basename: String): Path
        {
            try {
                Files.walk(Paths.get(rootDir)).use { walk ->
                    return walk
                            .filter { p -> p.fileName.toString() == basename }
                            .findFirst()
                            .orElseThrow {
                                IllegalStateException(
                                        "parquet file '" + basename + "' not found under " + rootDir)
                            }
                }
            }
            catch (e: java.io.IOException) {
                throw RuntimeException(e)
            }
        }
    }
}
