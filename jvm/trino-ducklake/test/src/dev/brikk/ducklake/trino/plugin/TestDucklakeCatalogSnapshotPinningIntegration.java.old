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
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_ID;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeCatalogSnapshotPinningIntegration
        extends AbstractTestQueryFramework
{
    private SnapshotBounds snapshotBounds;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        snapshotBounds = getSchemaEvolutionSnapshotBounds();
        return DucklakeQueryRunner.builder()
                .addConnectorProperty("ducklake.default-snapshot-id", String.valueOf(snapshotBounds.historicalSnapshotId()))
                .build();
    }

    @Test
    public void testCatalogDefaultSnapshotIdPinsReads()
    {
        assertQuery("SELECT count(*) FROM schema_evolution_table", "VALUES 2");
    }

    @Test
    public void testQuerySnapshotOverrideBeatsCatalogDefault()
    {
        assertQuery(
                "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF " + snapshotBounds.currentSnapshotId(),
                "VALUES 4");
    }

    @Test
    public void testSessionSnapshotOverrideBeatsCatalogDefault()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_ID, String.valueOf(snapshotBounds.currentSnapshotId()))
                .build();

        assertQuery(session, "SELECT count(*) FROM schema_evolution_table", "VALUES 4");
    }

    private static SnapshotBounds getSchemaEvolutionSnapshotBounds()
            throws Exception
    {
        JdbcDucklakeCatalog catalog = new JdbcDucklakeCatalog(DucklakeTestCatalogEnvironment.createDucklakeConfig().toCatalogConfig());
        try {
            long currentSnapshotId = catalog.getCurrentSnapshotId();
            DucklakeTable table = catalog.getTable("test_schema", "schema_evolution_table", currentSnapshotId)
                    .orElseThrow(() -> new AssertionError("Missing schema_evolution_table"));

            long historicalSnapshotId = -1;
            for (long snapshotId = currentSnapshotId; snapshotId >= 1; snapshotId--) {
                if (catalog.getSnapshot(snapshotId).isEmpty()) {
                    continue;
                }
                if (catalog.getTable("test_schema", "schema_evolution_table", snapshotId).isEmpty()) {
                    continue;
                }
                long recordCount = catalog.getDataFiles(table.tableId(), snapshotId).stream()
                        .mapToLong(DucklakeDataFile::recordCount)
                        .sum();
                if (recordCount == 2) {
                    historicalSnapshotId = snapshotId;
                    break;
                }
            }

            assertThat(historicalSnapshotId).isGreaterThan(0);
            return new SnapshotBounds(currentSnapshotId, historicalSnapshotId);
        }
        finally {
            catalog.close();
        }
    }

    private record SnapshotBounds(long currentSnapshotId, long historicalSnapshotId) {}
}
