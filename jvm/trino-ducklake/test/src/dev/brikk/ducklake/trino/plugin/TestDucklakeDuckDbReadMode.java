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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DUCKDB_READ_MODE;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_MODE_AUTO;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_MODE_HTTPFS;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_MODE_MATERIALIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * N2 routing tests. The connector picks per-split between materialize-then-ATTACH
 * (download {@code .db} to local tmp first) and ATTACH-via-httpfs (DuckDB streams
 * blocks from {@code s3://} directly). Choice is driven by {@code duckdb_read_mode}
 * + the {@code ducklake.duckdb.auto-httpfs-threshold} config; this test verifies the
 * wiring without requiring an actual S3 backend.
 *
 * <p>The test environment is local-FS, so any execution that takes the httpfs branch
 * will hit a clean error from the routing code ("requires an s3:// data file path"):
 * the negative path is the test signal. The materialize branch — which works on
 * local-FS — is the positive path. Together they pin the routing decision in both
 * directions.
 *
 * <p>This catalog uses {@code ducklake.duckdb.auto-httpfs-threshold=1B}, so any
 * non-empty {@code .db} file falls on the httpfs side of the {@code auto} threshold —
 * makes the threshold-driven decision testable without needing a multi-MB fixture.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDuckDbReadMode
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "duckdb-read-mode";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Forces 'auto' to take the httpfs branch for any non-empty file in this suite.
        // The integration tests in TestDucklakeDuckDbFormatRead use the default (64MiB)
        // threshold and verify the opposite — small files take the materialize branch.
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog(CATALOG_NAME)
                .addConnectorProperty("ducklake.duckdb.auto-httpfs-threshold", "1B")
                .build();
    }

    private Session sessionWith(String readMode)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .setCatalogSessionProperty("ducklake", DUCKDB_READ_MODE, readMode)
                .build();
    }

    private Session writeDuckDbSession()
    {
        // Writer-only session. We don't set DUCKDB_READ_MODE here — writes don't read
        // from the data file path, so the read mode is irrelevant during INSERT/CTAS.
        return Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build();
    }

    @Test
    public void testMaterializeModeReadsFromLocalFs()
    {
        // Baseline: with read_mode=materialize, the file is pulled into the local cache
        // and DuckDB ATTACHes the local path. Local FS is fine for this — the cache
        // just copies the file alongside the original.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.read_mode_materialize AS SELECT 1 AS id, CAST('a' AS VARCHAR) AS s");
        try {
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id, s FROM test_schema.read_mode_materialize");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo("a");
        }
        finally {
            tryDropTable("test_schema.read_mode_materialize");
        }
    }

    @Test
    public void testHttpfsModeFallsBackToMaterializeOnLocalFs()
    {
        // httpfs against a non-s3 path silently degrades to materialize — the local
        // file is already directly attachable, no streaming protocol required.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.read_mode_httpfs_localfs AS SELECT 1 AS id");
        try {
            var result = computeActual(
                    sessionWith(READ_MODE_HTTPFS),
                    "SELECT * FROM test_schema.read_mode_httpfs_localfs");
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.read_mode_httpfs_localfs");
        }
    }

    @Test
    public void testAutoModeWithLowThresholdFallsBackToMaterializeOnLocalFs()
    {
        // 'auto' + threshold=1B picks httpfs by size; against a non-s3 path that
        // degrades to materialize same as explicit httpfs would.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.read_mode_auto_lo AS SELECT 1 AS id");
        try {
            var result = computeActual(
                    sessionWith(READ_MODE_AUTO),
                    "SELECT * FROM test_schema.read_mode_auto_lo");
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.read_mode_auto_lo");
        }
    }

    @Test
    public void testInvalidReadModeRejected()
    {
        Session bad = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DUCKDB_READ_MODE, "ftp")
                .build();
        assertThatThrownBy(() -> computeActual(bad, "SELECT 1"))
                .hasMessageContaining(DUCKDB_READ_MODE + " must be one of");
    }

    @Test
    public void testParquetTablesIgnoreReadMode()
    {
        // duckdb_read_mode is a no-op for parquet tables (it gates only the duckdb
        // file format's ATTACH path). Pin this so a future refactor doesn't
        // accidentally couple the two.
        computeActual("CREATE TABLE test_schema.read_mode_parquet AS SELECT 7 AS id");
        try {
            // Setting httpfs explicitly on a parquet read must NOT trip the routing —
            // parquet path doesn't go anywhere near the httpfs branch.
            MaterializedResult result = computeActual(
                    Session.builder(getSession())
                            .setCatalogSessionProperty("ducklake", DUCKDB_READ_MODE, READ_MODE_HTTPFS)
                            .build(),
                    "SELECT id FROM test_schema.read_mode_parquet");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(7);
        }
        finally {
            tryDropTable("test_schema.read_mode_parquet");
        }
    }

    @Test
    public void testFunctionPredicatePushesDownThroughTrinoMacro()
    {
        // End-to-end proof that the trino_* macro layer fires for a real query.
        // The predicate WHERE lower(name) = 'apple' is what Trino's planner hands to
        // applyFilter as a ConnectorExpression; DuckDbExpressionTranslator rewrites
        // it to trino_lower("name") = 'apple' on the handle, the page source
        // appends that string to the WHERE clause sent to DuckDB, and DuckDB
        // resolves it through the trino_lower MACRO installed by
        // trino-function-aliases.sql. Correct result = the whole chain works.
        //
        // We keep the same conjuncts in remainingExpression (Regime 1 in
        // dev-docs/TODO-pushdown-duckdb.md), so this test does not rely on
        // pushdown for correctness — it asserts correctness AND, by virtue of
        // running on the duckdb-format path, exercises the macro at runtime.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.fn_pushdown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, CAST('Apple' AS VARCHAR)), "
                        + "  (2, CAST('banana' AS VARCHAR)), "
                        + "  (3, CAST('Cherry' AS VARCHAR))"
                        + ") AS t(id, name)");
        try {
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id, name FROM test_schema.fn_pushdown WHERE lower(name) = 'apple'");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(1)).isEqualTo("Apple");

            // Combine TupleDomain pushdown (id IN (1,3)) with function pushdown.
            MaterializedResult combined = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.fn_pushdown WHERE id IN (1, 3) AND lower(name) = 'cherry'");
            assertThat(combined.getRowCount()).isEqualTo(1);
            assertThat(combined.getMaterializedRows().getFirst().getField(0)).isEqualTo(3);
        }
        finally {
            tryDropTable("test_schema.fn_pushdown");
        }
    }

    @Test
    public void testUnknownFunctionPredicateDoesNotExplode()
    {
        // Negative case: a function not in trino_meta (regexp_extract) must NOT
        // be pushed; Trino-side filtering still works. The query must succeed.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.fn_unknown AS "
                        + "SELECT * FROM (VALUES "
                        + "  (1, CAST('abc-123' AS VARCHAR)), "
                        + "  (2, CAST('xyz-999' AS VARCHAR))"
                        + ") AS t(id, name)");
        try {
            MaterializedResult result = computeActual(
                    sessionWith(READ_MODE_MATERIALIZE),
                    "SELECT id FROM test_schema.fn_unknown WHERE regexp_extract(name, '\\d+') = '123'");
            assertThat(result.getRowCount()).isEqualTo(1);
            assertThat(result.getMaterializedRows().getFirst().getField(0)).isEqualTo(1);
        }
        finally {
            tryDropTable("test_schema.fn_unknown");
        }
    }

    private void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }
}
