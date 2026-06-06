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

import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.READ_SNAPSHOT_ID
import io.trino.Session
import io.trino.testing.AbstractTestQueryFramework
import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TestDucklakeCatalogSnapshotPinningIntegration : AbstractTestQueryFramework() {
    private lateinit var snapshotBounds: SnapshotBounds

    @Throws(Exception::class)
    override fun createQueryRunner(): QueryRunner {
        snapshotBounds = getSchemaEvolutionSnapshotBounds()
        return DucklakeQueryRunner.builder()
                .addConnectorProperty("ducklake.default-snapshot-id", snapshotBounds.historicalSnapshotId.toString())
                .build()
    }

    @Test
    fun testCatalogDefaultSnapshotIdPinsReads() {
        assertQuery("SELECT count(*) FROM schema_evolution_table", "VALUES 2")
    }

    @Test
    fun testQuerySnapshotOverrideBeatsCatalogDefault() {
        assertQuery(
                "SELECT count(*) FROM schema_evolution_table FOR VERSION AS OF " + snapshotBounds.currentSnapshotId,
                "VALUES 4")
    }

    @Test
    fun testSessionSnapshotOverrideBeatsCatalogDefault() {
        val session = Session.builder(getSession())
                .setCatalogSessionProperty("ducklake", READ_SNAPSHOT_ID, snapshotBounds.currentSnapshotId.toString())
                .build()

        assertQuery(session, "SELECT count(*) FROM schema_evolution_table", "VALUES 4")
    }

    @JvmRecord
    private data class SnapshotBounds(val currentSnapshotId: Long, val historicalSnapshotId: Long)

    companion object {
        @Throws(Exception::class)
        private fun getSchemaEvolutionSnapshotBounds(): SnapshotBounds {
            val catalog = JdbcDucklakeCatalog(DucklakeTestCatalogEnvironment.createDucklakeConfig().toCatalogConfig())
            try {
                val currentSnapshotId = catalog.currentSnapshotId
                val table = catalog.getTable("test_schema", "schema_evolution_table", currentSnapshotId)
                        ?: throw AssertionError("Missing schema_evolution_table")

                var historicalSnapshotId: Long = -1
                var snapshotId = currentSnapshotId
                while (snapshotId >= 1) {
                    if (catalog.getSnapshot(snapshotId) == null) {
                        snapshotId--
                        continue
                    }
                    if (catalog.getTable("test_schema", "schema_evolution_table", snapshotId) == null) {
                        snapshotId--
                        continue
                    }
                    val recordCount = catalog.getDataFiles(table.tableId, snapshotId).stream()
                            .mapToLong { obj: DucklakeDataFile -> obj.recordCount }
                            .sum()
                    if (recordCount == 2L) {
                        historicalSnapshotId = snapshotId
                        break
                    }
                    snapshotId--
                }

                assertThat(historicalSnapshotId).isGreaterThan(0)
                return SnapshotBounds(currentSnapshotId, historicalSnapshotId)
            }
            finally {
                catalog.close()
            }
        }
    }
}
