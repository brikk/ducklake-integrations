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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke test that exercises the Trino connector end-to-end against whichever
 * catalog backend is selected by {@link DucklakeTestCatalogBackend#fromSystemProperty()}.
 * Runs under both POSTGRES (default) and DUCKDB_QUACK when the system property
 * is set, proving the backend selector actually swaps the catalog plumbing.
 *
 * <p>Stays deliberately minimal: CREATE SCHEMA / CREATE TABLE / INSERT / SELECT.
 * Wider Quack coverage is gated on the catalog-bootstrap parity work tracked in
 * {@code TODO-WRITE-MODE.md § Quack Catalog Backend}.
 */
final class TestDucklakeBackendDispatchSmoke
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DucklakeQueryRunner.builder()
                .useIsolatedCatalog("backend-dispatch-smoke")
                .build();
    }

    @BeforeEach
    void skipUnderQuackUntilUpstreamSupportsMultiTableQueries()
    {
        // POSTGRES (default) and DUCKDB_LOCAL both run this smoke unmodified;
        // DUCKDB_QUACK skips until upstream Quack supports multi-table queries
        // (JOINs hit the planner's partition-info join) and UPDATE/DELETE on
        // remote tables (drop paths end-snapshot via UPDATE). Tracked in
        // dev-docs/TODO-WRITE-MODE.md § Quack Catalog Backend.
        Assumptions.assumeTrue(
                DucklakeTestCatalogEnvironment.selectedBackend() != DucklakeTestCatalogBackend.DUCKDB_QUACK,
                "Quack backend doesn't yet support multi-table queries or UPDATE/DELETE on remote metadata — "
                        + "see dev-docs/TODO-WRITE-MODE.md § Quack Catalog Backend");
    }

    @Test
    void createInsertSelectRoundTrip()
    {
        assertUpdate("CREATE SCHEMA IF NOT EXISTS test_schema");
        assertUpdate("CREATE TABLE test_schema.dispatch_smoke (id INTEGER, name VARCHAR)");
        try {
            assertUpdate("INSERT INTO test_schema.dispatch_smoke VALUES (1, 'alpha'), (2, 'beta')", 2);

            MaterializedResult rows = computeActual(
                    "SELECT id, name FROM test_schema.dispatch_smoke ORDER BY id");
            MaterializedResult expected = resultBuilder(getSession(), INTEGER, VARCHAR)
                    .row(1, "alpha")
                    .row(2, "beta")
                    .build();

            assertThat(rows.getMaterializedRows())
                    .containsExactlyElementsOf(expected.getMaterializedRows());
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_schema.dispatch_smoke");
        }
    }
}
