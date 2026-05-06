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
    private static final String EXPECTED_HTTPFS_LOCAL_FS_ERROR = "requires an s3:// data file path";

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
    public void testHttpfsModeRejectsLocalFsPath()
    {
        // The httpfs branch requires the data file to be on S3 (DuckDB's httpfs only
        // speaks http/https/s3). On local FS it must fail clearly rather than silently
        // fall back to materialize — that's the routing contract being pinned here.
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.read_mode_httpfs_localfs AS SELECT 1 AS id");
        try {
            assertThatThrownBy(() -> computeActual(
                    sessionWith(READ_MODE_HTTPFS),
                    "SELECT * FROM test_schema.read_mode_httpfs_localfs"))
                    .hasMessageContaining(EXPECTED_HTTPFS_LOCAL_FS_ERROR);
        }
        finally {
            tryDropTable("test_schema.read_mode_httpfs_localfs");
        }
    }

    @Test
    public void testAutoModeWithLowThresholdRoutesToHttpfs()
    {
        // 'auto' + threshold=1B + non-empty file = httpfs branch chosen. Confirms the
        // threshold-comparison side of the auto heuristic. (The opposite case —
        // 'auto' + small file under default 64MiB threshold = materialize — is already
        // covered by every test in TestDucklakeDuckDbFormatRead.)
        computeActual(writeDuckDbSession(),
                "CREATE TABLE test_schema.read_mode_auto_lo AS SELECT 1 AS id");
        try {
            assertThatThrownBy(() -> computeActual(
                    sessionWith(READ_MODE_AUTO),
                    "SELECT * FROM test_schema.read_mode_auto_lo"))
                    .hasMessageContaining(EXPECTED_HTTPFS_LOCAL_FS_ERROR);
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

    private void tryDropTable(String tableName)
    {
        try {
            computeActual("DROP TABLE " + tableName);
        }
        catch (Exception ignored) {
        }
    }
}
