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

import dev.brikk.ducklake.catalog.testing.CatalogQueries;
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport;
import io.trino.testing.MaterializedResult;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_COLUMN_STATS;
import static dev.brikk.ducklake.catalog.testing.CatalogPredicates.currentlyActive;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins nested-leaf {@code ducklake_file_column_stats} emission for the connector's
 * Parquet write path (INSERT) and for {@code add_files}. Each test creates a table
 * with a nested column, writes rows, then reads back the per-leaf stats rows via
 * JDBC against the catalog and asserts that:
 *
 * <ul>
 *   <li>one stats row landed per primitive leaf (not per top-level column),</li>
 *   <li>the stats rows are keyed by the <em>leaf's</em> {@code column_id} from
 *       {@code ducklake_column} (not the parent ROW/ARRAY/MAP column's id),</li>
 *   <li>min/max values decode to the expected string form per the type's stats
 *       contract — i.e. integers as decimal strings, varchars verbatim.</li>
 * </ul>
 *
 * <p>Upstream emits nested-leaf stats the same way (see
 * {@code third_party/ducklake/src/functions/ducklake_add_data_files.cpp::MapColumnStats}),
 * so this also pins cross-engine compatibility — DuckDB's reader queries
 * {@code ducklake_file_column_stats} by leaf field_id when planning pruning, and would
 * see incomplete coverage if we elided the nested-leaf rows.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeNestedLeafStats
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "write-nested-leaf-stats";
    }

    @Test
    public void rowOfPrimitivesEmitsOneStatsRowPerLeaf()
            throws Exception
    {
        // CREATE → catalog records ROW field plus one ducklake_column per child leaf,
        // each with its own column_id. INSERT should emit one ducklake_file_column_stats
        // row keyed by *each* leaf's column_id.
        computeActual("CREATE TABLE test_schema.row_stats (id INTEGER, data ROW(a INTEGER, b VARCHAR))");
        try {
            computeActual("INSERT INTO test_schema.row_stats VALUES "
                    + "(1, CAST(ROW(10, 'apple') AS ROW(a INTEGER, b VARCHAR))), "
                    + "(2, CAST(ROW(20, 'cherry') AS ROW(a INTEGER, b VARCHAR))), "
                    + "(3, CAST(ROW(5, 'banana') AS ROW(a INTEGER, b VARCHAR)))");

            Map<String, FileStatsRow> rowsByPath = readFileStatsByColumnPath("row_stats");

            // Three primitive leaves → three rows. The ROW container itself has its own
            // column_id but no file-level stats row (no parquet column chunk).
            assertThat(rowsByPath).containsOnlyKeys("id", "data.a", "data.b");

            assertThat(rowsByPath.get("id").minValue()).isEqualTo("1");
            assertThat(rowsByPath.get("id").maxValue()).isEqualTo("3");

            assertThat(rowsByPath.get("data.a").valueCount()).isEqualTo(3L);
            assertThat(rowsByPath.get("data.a").nullCount()).isEqualTo(0L);
            assertThat(rowsByPath.get("data.a").minValue()).isEqualTo("5");
            assertThat(rowsByPath.get("data.a").maxValue()).isEqualTo("20");

            assertThat(rowsByPath.get("data.b").valueCount()).isEqualTo(3L);
            assertThat(rowsByPath.get("data.b").nullCount()).isEqualTo(0L);
            assertThat(rowsByPath.get("data.b").minValue()).isEqualTo("apple");
            assertThat(rowsByPath.get("data.b").maxValue()).isEqualTo("cherry");
        }
        finally {
            tryDropTable("test_schema.row_stats");
        }
    }

    @Test
    public void arrayElementLeafEmitsStatsRow()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.array_stats (id INTEGER, tags ARRAY(VARCHAR))");
        try {
            computeActual("INSERT INTO test_schema.array_stats VALUES "
                    + "(1, ARRAY['apple', 'banana']), "
                    + "(2, ARRAY['cherry'])");

            Map<String, FileStatsRow> rowsByPath = readFileStatsByColumnPath("array_stats");

            // id + the array's element leaf.
            assertThat(rowsByPath).containsOnlyKeys("id", "tags.element");

            // value_count for a list element is the count of non-null elements
            // (3 elements across the two rows), not the count of rows.
            assertThat(rowsByPath.get("tags.element").valueCount()).isEqualTo(3L);
            assertThat(rowsByPath.get("tags.element").minValue()).isEqualTo("apple");
            assertThat(rowsByPath.get("tags.element").maxValue()).isEqualTo("cherry");
        }
        finally {
            tryDropTable("test_schema.array_stats");
        }
    }

    @Test
    public void mapKeyAndValueLeavesEmitStatsRows()
            throws Exception
    {
        computeActual("CREATE TABLE test_schema.map_stats (id INTEGER, attrs MAP(VARCHAR, BIGINT))");
        try {
            computeActual("INSERT INTO test_schema.map_stats VALUES "
                    + "(1, MAP(ARRAY['k1', 'k2'], ARRAY[100, 200])), "
                    + "(2, MAP(ARRAY['k3'], ARRAY[300]))");

            Map<String, FileStatsRow> rowsByPath = readFileStatsByColumnPath("map_stats");

            // id + map key + map value = 3 leaves.
            assertThat(rowsByPath).containsOnlyKeys("id", "attrs.key", "attrs.value");

            assertThat(rowsByPath.get("attrs.key").minValue()).isEqualTo("k1");
            assertThat(rowsByPath.get("attrs.key").maxValue()).isEqualTo("k3");
            assertThat(rowsByPath.get("attrs.value").minValue()).isEqualTo("100");
            assertThat(rowsByPath.get("attrs.value").maxValue()).isEqualTo("300");
        }
        finally {
            tryDropTable("test_schema.map_stats");
        }
    }

    @Test
    public void arrayOfRowEmitsOneStatsRowPerStructLeaf()
            throws Exception
    {
        // ARRAY<ROW(num INTEGER, label VARCHAR)>
        //   events                     → ARRAY container, no leaf
        //     events.element           → ROW container, no leaf
        //       events.element.num     → INTEGER leaf
        //       events.element.label   → VARCHAR leaf
        // Expected: two file_column_stats rows for events.element.{num,label}, plus one for id.
        computeActual("CREATE TABLE test_schema.array_of_row_stats ("
                + "id INTEGER, "
                + "events ARRAY(ROW(num INTEGER, label VARCHAR)))");
        try {
            computeActual("INSERT INTO test_schema.array_of_row_stats VALUES "
                    + "(1, ARRAY[CAST(ROW(1, 'x') AS ROW(num INTEGER, label VARCHAR)), "
                    + "         CAST(ROW(2, 'y') AS ROW(num INTEGER, label VARCHAR))]), "
                    + "(2, ARRAY[CAST(ROW(7, 'z') AS ROW(num INTEGER, label VARCHAR))])");

            Map<String, FileStatsRow> rowsByPath = readFileStatsByColumnPath("array_of_row_stats");

            assertThat(rowsByPath).containsOnlyKeys(
                    "id",
                    "events.element.num",
                    "events.element.label");

            assertThat(rowsByPath.get("events.element.num").minValue()).isEqualTo("1");
            assertThat(rowsByPath.get("events.element.num").maxValue()).isEqualTo("7");
            assertThat(rowsByPath.get("events.element.label").minValue()).isEqualTo("x");
            assertThat(rowsByPath.get("events.element.label").maxValue()).isEqualTo("z");
        }
        finally {
            tryDropTable("test_schema.array_of_row_stats");
        }
    }

    @Test
    public void rowOfRowEmitsDeeplyNestedLeafStats()
            throws Exception
    {
        // ROW(a INTEGER, nested ROW(b BIGINT, c VARCHAR)) → leaves a, nested.b, nested.c.
        computeActual("CREATE TABLE test_schema.nested_row_stats ("
                + "id INTEGER, "
                + "data ROW(a INTEGER, nested ROW(b BIGINT, c VARCHAR)))");
        try {
            computeActual("INSERT INTO test_schema.nested_row_stats VALUES "
                    + "(1, CAST(ROW(100, ROW(BIGINT '1000000000000', 'alpha')) AS ROW(a INTEGER, nested ROW(b BIGINT, c VARCHAR)))), "
                    + "(2, CAST(ROW(200, ROW(BIGINT '500000000000',  'omega')) AS ROW(a INTEGER, nested ROW(b BIGINT, c VARCHAR))))");

            Map<String, FileStatsRow> rowsByPath = readFileStatsByColumnPath("nested_row_stats");

            assertThat(rowsByPath).containsOnlyKeys(
                    "id",
                    "data.a",
                    "data.nested.b",
                    "data.nested.c");
            assertThat(rowsByPath.get("data.a").minValue()).isEqualTo("100");
            assertThat(rowsByPath.get("data.a").maxValue()).isEqualTo("200");
            assertThat(rowsByPath.get("data.nested.b").minValue()).isEqualTo("500000000000");
            assertThat(rowsByPath.get("data.nested.b").maxValue()).isEqualTo("1000000000000");
            assertThat(rowsByPath.get("data.nested.c").minValue()).isEqualTo("alpha");
            assertThat(rowsByPath.get("data.nested.c").maxValue()).isEqualTo("omega");
        }
        finally {
            tryDropTable("test_schema.nested_row_stats");
        }
    }

    @Test
    public void addFilesEmitsNestedLeafStats()
            throws Exception
    {
        // Materialize a parquet file via the connector itself (the simplest portable way
        // to get a DuckLake-compatible file on disk), then register it via add_files
        // against a separately-created target table with the same schema. The companion
        // id column gives VALUES a 2-tuple shape so the ROW literal doesn't unpack into
        // top-level columns at the INSERT site.
        computeActual("CREATE TABLE test_schema.add_files_src (id INTEGER, data ROW(a INTEGER, b VARCHAR))");
        computeActual("CREATE TABLE test_schema.add_files_dst (id INTEGER, data ROW(a INTEGER, b VARCHAR))");
        try {
            computeActual("INSERT INTO test_schema.add_files_src VALUES "
                    + "(1, CAST(ROW(42, 'hello') AS ROW(a INTEGER, b VARCHAR))), "
                    + "(2, CAST(ROW(7,  'world') AS ROW(a INTEGER, b VARCHAR)))");

            String fileAbs = singleFileAbsolutePath("add_files_src");
            computeActual(String.format(
                    "CALL ducklake.system.add_files("
                            + "schema_name => 'test_schema', "
                            + "table_name => 'add_files_dst', "
                            + "files => ARRAY['%s'])",
                    fileAbs));

            Map<String, FileStatsRow> rowsByPath = readFileStatsByColumnPath("add_files_dst");

            // Both struct leaves should have stats emitted via the add_files name-mapper
            // leaf walk — pinning that nested children flow through the same path the
            // INSERT writer uses.
            assertThat(rowsByPath).containsOnlyKeys("id", "data.a", "data.b");
            assertThat(rowsByPath.get("data.a").minValue()).isEqualTo("7");
            assertThat(rowsByPath.get("data.a").maxValue()).isEqualTo("42");
            assertThat(rowsByPath.get("data.b").minValue()).isEqualTo("hello");
            assertThat(rowsByPath.get("data.b").maxValue()).isEqualTo("world");
        }
        finally {
            tryDropTable("test_schema.add_files_src");
            tryDropTable("test_schema.add_files_dst");
        }
    }

    // ==================== Catalog readers ====================

    /**
     * Reads {@code ducklake_file_column_stats} rows for an active table, joining
     * {@code ducklake_column} to build the dotted column path (id, data.a, etc.) for
     * each emitted leaf. Returns a map keyed by that path. Asserts there's exactly
     * one parquet data file backing the table — the INSERTs are small enough not to
     * roll over file boundaries, so a multi-file outcome would indicate a
     * test-environment surprise.
     */
    private Map<String, FileStatsRow> readFileStatsByColumnPath(String unqualifiedTable)
            throws Exception
    {
        try (Connection conn = openCatalogConnection()) {
            DSLContext dsl = CatalogTestSupport.dsl(conn);
            long tableId = CatalogQueries.activeTableId(dsl, unqualifiedTable);

            var file = DUCKLAKE_DATA_FILE.as("file");
            Long fileCount = dsl.selectCount()
                    .from(file)
                    .where(file.TABLE_ID.eq(tableId).and(currentlyActive(file.END_SNAPSHOT)))
                    .fetchOne(0, Long.class);
            assertThat(orZero(fileCount))
                    .as("expected exactly one active data file for %s", unqualifiedTable)
                    .isEqualTo(1L);

            Map<Long, String> pathById = buildColumnPaths(dsl, tableId);

            var colstats = DUCKLAKE_FILE_COLUMN_STATS.as("colstats");
            Map<String, FileStatsRow> rowsByPath = new LinkedHashMap<>();
            dsl.select(
                            colstats.COLUMN_ID,
                            colstats.VALUE_COUNT,
                            colstats.NULL_COUNT,
                            colstats.MIN_VALUE,
                            colstats.MAX_VALUE)
                    .from(colstats)
                    .innerJoin(file)
                            .on(file.DATA_FILE_ID.eq(colstats.DATA_FILE_ID))
                    .where(file.TABLE_ID.eq(tableId)
                            .and(currentlyActive(file.END_SNAPSHOT)))
                    .forEach(r -> {
                        long columnId = orZero(r.get(colstats.COLUMN_ID));
                        String path = pathById.get(columnId);
                        assertThat(path)
                                .as("ducklake_file_column_stats row references column_id %d "
                                        + "but no active ducklake_column row with that id exists "
                                        + "for table %s", columnId, unqualifiedTable)
                                .isNotNull();
                        rowsByPath.put(path, new FileStatsRow(
                                orZero(r.get(colstats.VALUE_COUNT)),
                                orZero(r.get(colstats.NULL_COUNT)),
                                r.get(colstats.MIN_VALUE),
                                r.get(colstats.MAX_VALUE)));
                    });
            return rowsByPath;
        }
    }

    /**
     * Walks {@code ducklake_column}'s parent/child tree for {@code tableId} (currently
     * active rows only) and returns a {@code column_id → dotted path} map. The dotted
     * path mirrors the catalog naming convention used at write time: ROW fields by
     * name, ARRAY uses {@code .element}, MAP uses {@code .key} / {@code .value} —
     * the same convention {@link DucklakeStatsLeafProjector} keys leaves by.
     */
    private static Map<Long, String> buildColumnPaths(DSLContext dsl, long tableId)
    {
        var col = DUCKLAKE_COLUMN.as("col");
        Map<Long, ColumnRow> byId = new LinkedHashMap<>();
        dsl.select(col.COLUMN_ID, col.COLUMN_NAME, col.PARENT_COLUMN)
                .from(col)
                .where(col.TABLE_ID.eq(tableId).and(currentlyActive(col.END_SNAPSHOT)))
                .forEach(r -> {
                    long id = orZero(r.get(col.COLUMN_ID));
                    byId.put(id, new ColumnRow(id, r.get(col.COLUMN_NAME), r.get(col.PARENT_COLUMN)));
                });

        Map<Long, String> pathById = new LinkedHashMap<>();
        for (ColumnRow row : byId.values()) {
            pathById.put(row.columnId(), resolvePath(row, byId));
        }
        return pathById;
    }

    private static String resolvePath(ColumnRow row, Map<Long, ColumnRow> byId)
    {
        if (row.parentId() == null) {
            return row.name();
        }
        ColumnRow parent = byId.get(row.parentId());
        if (parent == null) {
            return row.name();
        }
        return resolvePath(parent, byId) + "." + row.name();
    }

    private record ColumnRow(long columnId, String name, Long parentId) {}

    private record FileStatsRow(long valueCount, long nullCount, String minValue, String maxValue) {}

    private static long orZero(Long v)
    {
        return v == null ? 0L : v;
    }

    // ==================== add_files helpers (mirrors TestDucklakeAddFiles) ====================

    private String singleFileAbsolutePath(String unqualifiedTable)
    {
        MaterializedResult files = computeActual("SELECT path FROM \"" + unqualifiedTable + "$files\"");
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
}
