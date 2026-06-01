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

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.DATA_FILE_FORMAT;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.FORMAT_DUCKDB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cross-cutting plumbing checks for the {@code data_file_format} session property.
 * Covers cases that don't fit either the parquet baseline tests or the DuckDB write
 * tests: empty {@code CREATE TABLE} (no writer fires) and invalid format string.
 */
@Execution(ExecutionMode.SAME_THREAD)
public class TestDucklakeDuckDbFormatSkeleton
        extends AbstractDucklakeIntegrationTest
{
    @Override
    protected String isolatedCatalogName()
    {
        return "duckdb-format-skeleton";
    }

    @Test
    public void testCreateEmptyDuckDbTableSucceeds()
    {
        // CREATE TABLE without rows does not invoke the writer, so no .db file is
        // produced. This proves the property and table-handle plumbing accepts
        // 'duckdb' as a valid format value without depending on the writer.
        Session duckDbSession = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, FORMAT_DUCKDB)
                .build();
        computeActual(duckDbSession, "CREATE TABLE test_schema.skeleton_empty (id INTEGER, name VARCHAR)");
        try {
            MaterializedResult result = computeActual("SELECT count(*) FROM test_schema.skeleton_empty");
            assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);
        }
        finally {
            tryDropTable("test_schema.skeleton_empty");
        }
    }

    @Test
    public void testInvalidFormatRejected()
    {
        Session badSession = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, "vortex")
                .build();
        assertThatThrownBy(() -> computeActual(
                badSession,
                "CREATE TABLE test_schema.skeleton_bad AS SELECT 1 AS id"))
                .hasMessageContaining("data_file_format must be one of");
        tryDropTable("test_schema.skeleton_bad");
    }
}
