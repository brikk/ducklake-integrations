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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that DuckDB-created views are hidden (non-Trino dialect views require transpiler).
 * Uses an isolated catalog to avoid cross-test interference.
 */
public class TestDucklakeViewDialectFiltering
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("view-dialect-filtering")
                .build();
    }

    @Test
    public void testDuckdbViewsHidden()
    {
        // All views in test catalog are DuckDB dialect — should be hidden
        MaterializedResult result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema'");
        List<String> viewNames = result.getMaterializedRows().stream()
                .map(row -> row.getField(0).toString())
                .toList();
        assertThat(viewNames).doesNotContain("simple_view", "aliased_view", "duckdb_specific_view");
    }

    @Test
    public void testTrinoCreatedViewVisible()
    {
        try {
            computeActual("CREATE VIEW test_schema.trino_only_view AS SELECT id FROM simple_table");

            MaterializedResult result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'trino_only_view'");
            assertThat(result.getRowCount()).isEqualTo(1);
        }
        finally {
            try {
                computeActual("DROP VIEW IF EXISTS test_schema.trino_only_view");
            }
            catch (Exception _) {
            }
        }
    }
}
