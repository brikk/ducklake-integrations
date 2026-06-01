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
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_WRITER_MODE;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.WRITER_MODE_APPENDER;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.WRITER_MODE_ARROW_STREAM;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins write-time rollover for the DuckDB-format writer: when accumulated input
 * bytes exceed {@code ducklake.duckdb.target-write-bytes}, the page sink rolls
 * to a new {@code .db} file. Catalog ends up with N {@code ducklake_data_file}
 * rows and read-back returns all rows.
 *
 * <p>Threshold is forced to 16 kB for the test — small enough that a 10k-row
 * CTAS rolls many times.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDuckDbWriteRollover
        extends AbstractDucklakeIntegrationTest
{
    private static final String CATALOG_NAME = "duckdb-write-rollover";

    @Override
    protected String isolatedCatalogName()
    {
        return CATALOG_NAME;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog(CATALOG_NAME)
                .addConnectorProperty("ducklake.duckdb.target-write-bytes", "16kB")
                .build();
    }

    private static Session duckDbSession(QueryRunner runner, String writerMode)
    {
        return Session.builder(runner.getDefaultSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .setCatalogSessionProperty("ducklake", DUCKDB_WRITER_MODE, writerMode)
                .build();
    }

    @Test
    public void testArrowStreamWriterRollsToMultipleFiles()
    {
        runRolloverScenario(WRITER_MODE_ARROW_STREAM, "duck_rollover_arrow");
    }

    @Test
    public void testAppenderWriterRollsToMultipleFiles()
    {
        runRolloverScenario(WRITER_MODE_APPENDER, "duck_rollover_appender");
    }

    private void runRolloverScenario(String writerMode, String tableName)
    {
        // ~10k rows × (8 BIGINT + 32-char VARCHAR) = ~400 KB of logical input bytes.
        // Threshold 16 kB → many files. (repeat() in Trino returns an array; lpad
        // is the canonical way to get a fixed-width VARCHAR.)
        computeActual(duckDbSession(getQueryRunner(), writerMode),
                "CREATE TABLE test_schema." + tableName + " AS " +
                        "SELECT id, lpad(cast(id AS varchar), 32, '0') AS payload " +
                        "FROM UNNEST(sequence(1, 10000)) AS t(id)");
        try {
            MaterializedResult files = computeActual(
                    "SELECT count(*) FROM \"" + tableName + "$files\"");
            long fileCount = (long) files.getMaterializedRows().getFirst().getField(0);
            assertThat(fileCount)
                    .as("16 kB target on a ~440 KB write must roll to multiple files")
                    .isGreaterThan(1L);

            MaterializedResult count = computeActual(
                    "SELECT count(*) FROM test_schema." + tableName);
            assertThat(count.getMaterializedRows().getFirst().getField(0))
                    .as("all rows round-trip across the rolled files")
                    .isEqualTo(10000L);

            MaterializedResult agg = computeActual(
                    "SELECT min(id), max(id), sum(id) FROM test_schema." + tableName);
            var row = agg.getMaterializedRows().getFirst();
            assertThat(row.getField(0)).isEqualTo(1L);
            assertThat(row.getField(1)).isEqualTo(10000L);
            assertThat(row.getField(2)).isEqualTo(50005000L);
        }
        finally {
            tryDropTable("test_schema." + tableName);
        }
    }
}
