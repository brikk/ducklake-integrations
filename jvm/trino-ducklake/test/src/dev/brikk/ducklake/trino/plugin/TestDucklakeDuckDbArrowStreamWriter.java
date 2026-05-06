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
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_WRITER_MODE;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.WRITER_MODE_ARROW_STREAM;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Coverage for the alternative {@link DuckDbArrowStreamFileWriter} path.
 * Selected via {@code duckdb_writer_mode = 'arrow_stream'}; should produce
 * byte-identical-shaped catalog output to the default Appender writer
 * (same {@code file_format='duckdb'}, same row counts, same column stats).
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDuckDbArrowStreamWriter
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "duckdb-arrow-stream-write";
    }

    private Session arrowStreamSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .setCatalogSessionProperty("ducklake", DUCKDB_WRITER_MODE, WRITER_MODE_ARROW_STREAM)
                .build();
    }

    @Test
    public void testScalarRoundTripThroughArrowStream()
    {
        computeActual(arrowStreamSession(),
                "CREATE TABLE test_schema.arrow_scalars AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, true, CAST(127 AS TINYINT), CAST(32000 AS SMALLINT), 9223372036854775000, CAST(1.5 AS REAL), CAST(2.71828 AS DOUBLE), CAST('alpha' AS VARCHAR)), " +
                        "  (2, false, CAST(-128 AS TINYINT), CAST(-32768 AS SMALLINT), -9223372036854775000, CAST(-3.5 AS REAL), CAST(-1.0 AS DOUBLE), CAST('beta' AS VARCHAR))" +
                        ") AS t(id, flag, ti, si, bi, r, d, s)");
        try {
            MaterializedResult files = computeActual(
                    "SELECT file_format, record_count FROM \"arrow_scalars$files\"");
            assertThat(files.getMaterializedRows().getFirst().getField(0)).isEqualTo("duckdb");
            assertThat(files.getMaterializedRows().getFirst().getField(1)).isEqualTo(2L);

            MaterializedResult result = computeActual(
                    "SELECT id, flag, ti, si, bi, r, d, s FROM test_schema.arrow_scalars ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);
            MaterializedRow r1 = result.getMaterializedRows().get(0);
            assertThat(r1.getField(0)).isEqualTo(1);
            assertThat(r1.getField(1)).isEqualTo(true);
            assertThat(r1.getField(2)).isEqualTo((byte) 127);
            assertThat(r1.getField(3)).isEqualTo((short) 32000);
            assertThat(r1.getField(4)).isEqualTo(9223372036854775000L);
            assertThat(((Number) r1.getField(5)).floatValue()).isEqualTo(1.5f);
            assertThat(((Number) r1.getField(6)).doubleValue()).isEqualTo(2.71828);
            assertThat(r1.getField(7)).isEqualTo("alpha");
        }
        finally {
            tryDropTable("test_schema.arrow_scalars");
        }
    }

    @Test
    public void testNullsThroughArrowStream()
    {
        computeActual(arrowStreamSession(),
                "CREATE TABLE test_schema.arrow_nulls AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('a' AS VARCHAR), 100), " +
                        "  (2, CAST(NULL AS VARCHAR), 200), " +
                        "  (3, CAST('c' AS VARCHAR), CAST(NULL AS INTEGER)), " +
                        "  (4, CAST(NULL AS VARCHAR), CAST(NULL AS INTEGER))" +
                        ") AS t(id, label, amount)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT id, label, amount FROM test_schema.arrow_nulls ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(4);

            MaterializedRow r2 = result.getMaterializedRows().get(1);
            assertThat(r2.getField(0)).isEqualTo(2);
            assertThat(r2.getField(1)).isNull();
            assertThat(r2.getField(2)).isEqualTo(200);

            MaterializedRow r4 = result.getMaterializedRows().get(3);
            assertThat(r4.getField(0)).isEqualTo(4);
            assertThat(r4.getField(1)).isNull();
            assertThat(r4.getField(2)).isNull();
        }
        finally {
            tryDropTable("test_schema.arrow_nulls");
        }
    }

    @Test
    public void testTemporalAndDecimalThroughArrowStream()
    {
        computeActual(arrowStreamSession(),
                "CREATE TABLE test_schema.arrow_temporal AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, DATE '2026-05-05', CAST('123.45' AS DECIMAL(10,2))), " +
                        "  (2, DATE '1999-12-31', CAST('-9999.99' AS DECIMAL(10,2)))" +
                        ") AS t(id, d, dec)");
        try {
            MaterializedResult result = computeActual(
                    "SELECT id, d, dec FROM test_schema.arrow_temporal ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(2);

            MaterializedRow r1 = result.getMaterializedRows().get(0);
            assertThat(r1.getField(1)).isEqualTo(LocalDate.of(2026, 5, 5));
            assertThat(r1.getField(2)).isEqualTo(new BigDecimal("123.45"));

            MaterializedRow r2 = result.getMaterializedRows().get(1);
            assertThat(r2.getField(1)).isEqualTo(LocalDate.of(1999, 12, 31));
            assertThat(r2.getField(2)).isEqualTo(new BigDecimal("-9999.99"));
        }
        finally {
            tryDropTable("test_schema.arrow_temporal");
        }
    }

    @Test
    public void testColumnStatsPopulatedFromArrowStream()
            throws Exception
    {
        computeActual(arrowStreamSession(),
                "CREATE TABLE test_schema.arrow_stats AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('alpha' AS VARCHAR), 100), " +
                        "  (2, CAST('beta'  AS VARCHAR), 200), " +
                        "  (3, CAST(NULL    AS VARCHAR), 300), " +
                        "  (4, CAST('delta' AS VARCHAR), CAST(NULL AS INTEGER))" +
                        ") AS t(id, label, amount)");
        try (Connection conn = openCatalogConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT c.column_name, s.value_count, s.null_count, s.min_value, s.max_value " +
                                "FROM ducklake_file_column_stats s " +
                                "JOIN ducklake_column c " +
                                "  ON c.column_id = s.column_id AND c.end_snapshot IS NULL " +
                                "JOIN ducklake_data_file f ON f.data_file_id = s.data_file_id " +
                                "WHERE f.file_format = 'duckdb' AND c.table_id = ( " +
                                "  SELECT table_id FROM ducklake_table " +
                                "  WHERE table_name = 'arrow_stats' AND end_snapshot IS NULL )");
                ResultSet rs = stmt.executeQuery()) {
            Map<String, StatsRow> rows = new HashMap<>();
            while (rs.next()) {
                rows.put(rs.getString(1), new StatsRow(
                        rs.getLong(2), rs.getLong(3), rs.getString(4), rs.getString(5)));
            }
            assertThat(rows).containsKeys("id", "label", "amount");
            assertThat(rows.get("id").valueCount).isEqualTo(4L);
            assertThat(rows.get("id").nullCount).isEqualTo(0L);
            assertThat(rows.get("label").valueCount).isEqualTo(3L);
            assertThat(rows.get("label").nullCount).isEqualTo(1L);
            assertThat(rows.get("label").minValue).isEqualTo("alpha");
            assertThat(rows.get("label").maxValue).isEqualTo("delta");
            assertThat(rows.get("amount").valueCount).isEqualTo(3L);
            assertThat(rows.get("amount").nullCount).isEqualTo(1L);
            assertThat(rows.get("amount").minValue).isEqualTo("100");
            assertThat(rows.get("amount").maxValue).isEqualTo("300");
        }
        finally {
            tryDropTable("test_schema.arrow_stats");
        }
    }

    @Test
    public void testBulkInsertThroughArrowStream()
    {
        // Generate ~10K rows so the Page → Arrow → INSERT pipeline runs through
        // multiple batches and exercises the producer-consumer queue.
        computeActual(arrowStreamSession(),
                "CREATE TABLE test_schema.arrow_bulk AS " +
                        "SELECT n AS id, CAST(n * 2 AS BIGINT) AS doubled, CAST(n % 3 AS VARCHAR) AS mod3 " +
                        "FROM UNNEST(sequence(1, 10000)) AS t(n)");
        try {
            MaterializedResult cnt = computeActual("SELECT count(*) FROM test_schema.arrow_bulk");
            assertThat(cnt.getMaterializedRows().getFirst().getField(0)).isEqualTo(10000L);

            MaterializedResult agg = computeActual(
                    "SELECT mod3, count(*), sum(doubled) FROM test_schema.arrow_bulk GROUP BY mod3 ORDER BY mod3");
            // 1..10000 mod 3 → groups: '0' (3334 rows), '1' (3333 rows), '2' (3333 rows)
            assertThat(agg.getRowCount()).isEqualTo(3);
        }
        finally {
            tryDropTable("test_schema.arrow_bulk");
        }
    }

    private record StatsRow(long valueCount, long nullCount, String minValue, String maxValue) {}
}
