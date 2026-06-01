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
 * Acceptance test for the `altered_views × altered_views` matrix
 * entry at `ducklake_transaction.cpp:1311–1313`. Two writers
 * concurrently calling `replaceViewMetadata` on the same view —
 * each end-snapshots the active row and inserts a new one with the
 * same `view_id` — must conflict; otherwise the second commit
 * silently overwrites the first writer's SQL.
 *
 * The loser's retry's action successfully resolves the active view
 * row (it sees the winner's freshly-inserted post-alter row), so the
 * action runs to completion and recordChange logs
 * `AlteredView(view_id)`. The matrix then sees that `view_id`
 * in `other.alteredViews` and throws.
 */
class TestConcurrentAlterViewVsAlterView {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                server, "concurrent-alter-view-vs-alter-view")

            val config = DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl)
                .setCatalogDatabaseUser(isolated.user)
                .setCatalogDatabasePassword(isolated.password)
                .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                .setMaxCatalogConnections(5)
            catalog = JdbcDucklakeCatalog(config)

            // The view exists before the race; both writers want to rewrite its SQL.
            catalog.createView("test_schema", "alter_dueling_view",
                "SELECT 1 AS x", "trino", null)
        }

        @AfterAll
        @JvmStatic
        fun tearDownClass() {
            if (::catalog.isInitialized) {
                catalog.close()
            }
            if (::server.isInitialized) {
                server.close()
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun duelingReplaceViewMetadataConflicts() {
        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable {
                catalog.replaceViewMetadata("test_schema", "alter_dueling_view",
                    "SELECT 2 AS winner_sql", "trino", null)
            },
            Runnable {
                catalog.replaceViewMetadata("test_schema", "alter_dueling_view",
                    "SELECT 3 AS loser_sql", "trino", null)
            })

        assertThat(result.loserException)
            .`as`("two concurrent replaceViewMetadata on the same view must conflict")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
            .`as`("error message must reference alter view")
            .contains("alter view")
            .contains("altered it")

        assertThat((result.loserException as TransactionConflictException).retryable())
            .`as`("logical conflicts are non-retryable")
            .isFalse()
        assertThat(result.loserAttemptCount)
            .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        val landedView = catalog.getView("test_schema", "alter_dueling_view", latestSnapshot)
            .orElseThrow()
        assertThat(landedView.sql)
            .`as`("winner's SQL is the one that landed; loser's was rolled back")
            .isEqualTo("SELECT 2 AS winner_sql")
    }
}
