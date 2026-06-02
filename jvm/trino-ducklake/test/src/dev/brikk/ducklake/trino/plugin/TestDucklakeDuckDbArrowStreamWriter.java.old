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
import io.trino.Session;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_COLUMN_STATS;
import static dev.brikk.ducklake.catalog.testing.CatalogPredicates.currentlyActive;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_WRITER_MODE;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.WRITER_MODE_ARROW_STREAM;
import static java.lang.String.format;
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
        try (Connection conn = openCatalogConnection()) {
            DSLContext dsl = CatalogTestSupport.dsl(conn);
            long tableId = CatalogQueries.activeTableId(dsl, "arrow_stats");

            // Same 3-table join shape as TestDucklakeDuckDbFormatWrite.testColumnStatsWrittenForDuckDbFormat:
            // file_column_stats → column (active rows only) → data_file, restricted to duckdb-format
            // files of the currently-active table. The arrow-stream writer must produce stats with
            // the same shape as the appender writer for cross-engine compatibility.
            var colstats = DUCKLAKE_FILE_COLUMN_STATS.as("colstats");
            var col = DUCKLAKE_COLUMN.as("col");
            var file = DUCKLAKE_DATA_FILE.as("file");
            Map<String, StatsRow> rows = new HashMap<>();
            dsl.select(
                            col.COLUMN_NAME,
                            colstats.VALUE_COUNT,
                            colstats.NULL_COUNT,
                            colstats.MIN_VALUE,
                            colstats.MAX_VALUE)
                    .from(colstats)
                    .join(col)
                            .on(col.COLUMN_ID.eq(colstats.COLUMN_ID)
                                    .and(currentlyActive(col.END_SNAPSHOT)))
                    .join(file)
                            .on(file.DATA_FILE_ID.eq(colstats.DATA_FILE_ID))
                    .where(file.FILE_FORMAT.eq("duckdb")
                            .and(col.TABLE_ID.eq(tableId)))
                    .forEach(r -> rows.put(
                            r.get(col.COLUMN_NAME),
                            new StatsRow(
                                    orZero(r.get(colstats.VALUE_COUNT)),
                                    orZero(r.get(colstats.NULL_COUNT)),
                                    r.get(colstats.MIN_VALUE),
                                    r.get(colstats.MAX_VALUE))));
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

    private static long orZero(Long value)
    {
        // ducklake_file_column_stats columns are nullable in the schema; the original
        // raw-SQL path used ResultSet#getLong which returns 0 on SQL NULL, and the
        // assertions baked that in. Preserve that behavior here.
        return value == null ? 0L : value;
    }

    /**
     * UUID round-trip through the arrow-stream writer + duckdb-format reader.
     * Pre-fix: arrow-stream writer threw NOT_SUPPORTED for UUID (missing branch
     * in {@code toArrowType}), and the reader had no UUID converter case at all.
     * The pair was a silent regression after arrow_stream became the default
     * writer mode — appender supported UUID, arrow_stream silently rejected it,
     * and even if you wrote via appender you couldn't read back. Both ends now
     * exchange UUIDs as Arrow {@code FixedSizeBinary(16)}; bytes flow byte-for-byte
     * (Trino's UUID Slice and DuckDB's UUID storage are both big-endian 16 bytes).
     */
    @Test
    public void testUuidRoundTripThroughArrowStream()
    {
        computeActual(arrowStreamSession(),
                "CREATE TABLE test_schema.arrow_uuids AS " +
                        "SELECT * FROM (VALUES " +
                        "  (1, CAST('00000000-0000-0000-0000-000000000001' AS UUID)), " +
                        "  (2, CAST('aabbccdd-eeff-0011-2233-445566778899' AS UUID)), " +
                        "  (3, CAST('ffffffff-ffff-ffff-ffff-ffffffffffff' AS UUID)), " +
                        "  (4, CAST(NULL AS UUID))" +
                        ") AS t(id, u)");
        try {
            // File format check — confirms we exercised the arrow_stream path,
            // not the appender (which would have worked even before the fix).
            MaterializedResult files = computeActual("SELECT file_format FROM \"arrow_uuids$files\"");
            assertThat(files.getMaterializedRows().getFirst().getField(0)).isEqualTo("duckdb");

            MaterializedResult result = computeActual(
                    "SELECT id, u FROM test_schema.arrow_uuids ORDER BY id");
            assertThat(result.getRowCount()).isEqualTo(4);

            assertThat(result.getMaterializedRows().get(0).getField(1).toString())
                    .as("UUID at id=1 round-trips bit-exactly")
                    .isEqualTo("00000000-0000-0000-0000-000000000001");
            assertThat(result.getMaterializedRows().get(1).getField(1).toString())
                    .as("UUID at id=2 round-trips bit-exactly (mixed bytes catch endian flips)")
                    .isEqualTo("aabbccdd-eeff-0011-2233-445566778899");
            assertThat(result.getMaterializedRows().get(2).getField(1).toString())
                    .as("UUID at id=3 round-trips (all-bits-set catches sign-extension bugs)")
                    .isEqualTo("ffffffff-ffff-ffff-ffff-ffffffffffff");
            assertThat(result.getMaterializedRows().get(3).getField(1))
                    .as("NULL UUID stays NULL")
                    .isNull();

            // Predicate round-trip: equality on UUID should also work end-to-end
            // (Trino-side filter; DuckDB-side pushdown for UUID isn't wired yet).
            MaterializedResult filtered = computeActual(
                    "SELECT id FROM test_schema.arrow_uuids " +
                            "WHERE u = CAST('aabbccdd-eeff-0011-2233-445566778899' AS UUID)");
            assertThat(filtered.getRowCount()).isEqualTo(1);
            assertThat(filtered.getMaterializedRows().getFirst().getField(0)).isEqualTo(2);
        }
        finally {
            tryDropTable("test_schema.arrow_uuids");
        }
    }

    /**
     * Regression test for a writer-side bug in {@link DuckDbArrowStreamFileWriter}.
     *
     * <p>Repro shape: a table read from a real connector page source (here: parquet
     * via this very catalog) feeds a CTAS into duckdb format with the arrow-stream
     * writer. The arrow-stream writer queues {@link io.trino.spi.Page} instances
     * and reads block values asynchronously on a consumer thread. Trino's read
     * path can hand pages whose block contents are not stable past the
     * {@code write()} call (e.g. {@code LazyBlock} backed by reader-internal
     * buffers that get reused once the producer thread moves on). When that
     * happens the consumer reads stale or zero data, and the catalog ends up
     * with rows whose values are wrong.
     *
     * <p>Symptoms in production were exactly this:
     * <ul>
     *   <li>{@code count(*)} correct (queued atomically per page)</li>
     *   <li>{@code count(DISTINCT)} of a high-cardinality BIGINT column drops
     *       below the source value (~5-12% on the user's repro)</li>
     *   <li>{@code min(custkey)} becomes 0 even when the source has no zero
     *       values — Arrow buffer freshly allocated by {@code allocateNew} stays
     *       at zero for positions whose source data was clobbered</li>
     *   <li>Non-deterministic: distinct count varies between writes</li>
     * </ul>
     *
     * <p>This test reads a parquet source table into a duckdb target via the
     * arrow-stream writer and asserts that all distinct values survive. The
     * existing {@link #testBulkInsertThroughArrowStream} doesn't catch the bug
     * because its source is {@code UNNEST(sequence(...))} — inline values, no
     * read-side block lifecycle. The bug only surfaces when the source pages
     * come from a real {@code ConnectorPageSource}.
     */
    @Test
    public void testArrowStreamPreservesAllDistinctValuesFromConnectorSource()
    {
        // Source: 100K distinct BIGINT values written as parquet (the default
        // format on this catalog). Reading this back through Trino's parquet
        // page source is what produces the LazyBlock-shaped pages the writer
        // bug feeds on. Cross-join two sequences because Trino caps a single
        // sequence() call at 10K entries — and we need >10K so the source
        // produces multiple pages, which is the precondition for the
        // multi-batch race in the arrow-stream writer.
        int outer = 1000;
        int inner = 100;
        int numRows = outer * inner;
        computeActual(format(
                "CREATE TABLE test_schema.arrow_repro_src AS " +
                        "SELECT CAST(a * %d + b AS BIGINT) AS k " +
                        "FROM UNNEST(sequence(1, %d)) AS t1(a) " +
                        "CROSS JOIN UNNEST(sequence(0, %d)) AS t2(b)",
                inner, outer, inner - 1));
        // k = a*inner + b where a ∈ [1..outer], b ∈ [0..inner-1] → k ∈ [inner .. outer*inner + (inner-1)]
        long expectedMin = inner;
        long expectedMax = (long) outer * inner + (inner - 1);
        try {
            // Sanity: source itself is right (so any failure below is the writer,
            // not a flaky source).
            MaterializedResult srcStats = computeActual(
                    "SELECT count(*), count(DISTINCT k), min(k), max(k) FROM test_schema.arrow_repro_src");
            MaterializedRow s = srcStats.getMaterializedRows().getFirst();
            assertThat(s.getField(0)).as("source count(*)").isEqualTo((long) numRows);
            assertThat(s.getField(1)).as("source count(DISTINCT)").isEqualTo((long) numRows);
            assertThat(s.getField(2)).as("source min").isEqualTo(expectedMin);
            assertThat(s.getField(3)).as("source max").isEqualTo(expectedMax);

            // Target: same data, but written through the arrow-stream writer.
            computeActual(arrowStreamSession(),
                    "CREATE TABLE test_schema.arrow_repro_tgt AS SELECT * FROM test_schema.arrow_repro_src");
            try {
                // Sanity: confirm the target was actually written in duckdb format
                // (not parquet) so we know we exercised the arrow-stream path.
                MaterializedResult files = computeActual(
                        "SELECT file_format FROM \"arrow_repro_tgt$files\"");
                assertThat(files.getMaterializedRows()).isNotEmpty();
                assertThat(files.getMaterializedRows().getFirst().getField(0))
                        .as("target file_format must be 'duckdb' for this test to mean anything")
                        .isEqualTo("duckdb");

                // The actual assertions: every value preserved.
                MaterializedResult tgtStats = computeActual(
                        "SELECT count(*), count(DISTINCT k), min(k), max(k) FROM test_schema.arrow_repro_tgt");
                MaterializedRow t = tgtStats.getMaterializedRows().getFirst();
                assertThat(t.getField(0)).as("target count(*)").isEqualTo((long) numRows);
                assertThat(t.getField(1))
                        .as("target count(DISTINCT k) — if < %d, arrow-stream writer dropped values", numRows)
                        .isEqualTo((long) numRows);
                assertThat(t.getField(2))
                        .as("target min(k) — if < %d (source has no smaller values), arrow-stream writer wrote stale zeros", expectedMin)
                        .isEqualTo(expectedMin);
                assertThat(t.getField(3)).as("target max(k)").isEqualTo(expectedMax);
            }
            finally {
                tryDropTable("test_schema.arrow_repro_tgt");
            }
        }
        finally {
            tryDropTable("test_schema.arrow_repro_src");
        }
    }
}
