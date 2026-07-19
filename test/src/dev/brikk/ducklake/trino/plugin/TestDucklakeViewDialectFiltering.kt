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

import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Tests that DuckDB-created views are hidden (non-Trino dialect views require transpiler).
 * Uses an isolated catalog to avoid cross-test interference.
 */
class TestDucklakeViewDialectFiltering : AbstractTestQueryFramework() {
    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("view-dialect-filtering")
                .build()
    }

    @Test
    fun testDuckdbViewsHidden() {
        // All views in test catalog are DuckDB dialect — should be hidden
        val result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema'")
        val viewNames: List<String> = result.materializedRows.stream()
                .map { row -> row.getField(0).toString() }
                .toList()
        assertThat(viewNames).doesNotContain("simple_view", "aliased_view", "duckdb_specific_view")
    }

    @Test
    fun testTrinoCreatedViewVisible() {
        try {
            computeActual("CREATE VIEW test_schema.trino_only_view AS SELECT id FROM simple_table")

            val result = computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = 'test_schema' AND table_name = 'trino_only_view'")
            assertThat(result.rowCount).isEqualTo(1)
        }
        finally {
            try {
                computeActual("DROP VIEW IF EXISTS test_schema.trino_only_view")
            }
            catch (_: Exception) {
            }
        }
    }
}
