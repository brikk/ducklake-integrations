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
package dev.brikk.ducklake.catalog

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * Integration test proving that the catalog's optimistic-retry loop actually
 * re-runs the transaction against a real Postgres-backed catalog when a
 * competing commit advances the snapshot lineage between this transaction's
 * read and its INSERT.
 *
 * Determinism is provided by [ConcurrentWriterHarness], which uses
 * the `beforeWriteTransactionAction` test seam in
 * [JdbcDucklakeCatalog] to park the loser before its first mutation.
 */
class TestJdbcDucklakeCatalogConcurrentCommit {
    companion object {
        private var server: TestingDucklakePostgreSqlCatalogServer? = null
        private var catalog: JdbcDucklakeCatalog? = null

        @JvmStatic
        @BeforeAll
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server!!, "concurrent-commit")

            val config = DucklakeCatalogConfig().apply {
                catalogDatabaseUrl = isolated.jdbcUrl
                catalogDatabaseUser = isolated.user
                catalogDatabasePassword = isolated.password
                dataPath = isolated.dataDir.toAbsolutePath().toString()
                maxCatalogConnections = 5
            }
            catalog = JdbcDucklakeCatalog(config)
        }

        @JvmStatic
        @AfterAll
        fun tearDownClass() {
            catalog?.close()
            server?.close()
        }
    }

    @Test
    @Throws(Exception::class)
    fun concurrentCommitTriggersRetryAndBothSchemasLand() {
        val catalog = catalog!!
        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.createSchema("winner_schema") },
            Runnable { catalog.createSchema("loser_schema") },
        )

        assertThat(result.loserException)
            .`as`("loser must commit cleanly on retry; both schemas are non-conflicting")
            .isNull()
        assertThat(result.loserAttemptCount)
            .`as`("loser must hit the hook twice: first attempt (paused) + retry (no-op)")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getSchema("winner_schema", latestSnapshot))
            .`as`("winner_schema must be present at latest snapshot")
            .isNotNull()
        assertThat(catalog.getSchema("loser_schema", latestSnapshot))
            .`as`("loser_schema must be present at latest snapshot (proves retry committed)")
            .isNotNull()
    }
}
