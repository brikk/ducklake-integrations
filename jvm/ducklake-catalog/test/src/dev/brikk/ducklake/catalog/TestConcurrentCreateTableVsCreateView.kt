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
import java.util.Optional

/**
 * Acceptance test for the cross-kind name-collision branch of
 * [ConflictMatrix] — port of upstream's
 * `created_tables` loop at
 * `ducklake_transaction.cpp:1218–1242`, where tables and views share
 * a namespace within a schema. T1 creates table `S.foo`, T2 races
 * with creating view `S.foo`; the loser must fail non-retryably
 * with a message naming the existing entry's kind ("table"), not silently
 * land both.
 */
class TestConcurrentCreateTableVsCreateView {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                server, "concurrent-create-table-vs-create-view")

            val config = DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl)
                .setCatalogDatabaseUser(isolated.user)
                .setCatalogDatabasePassword(isolated.password)
                .setDataPath(isolated.dataDir.toAbsolutePath().toString())
                .setMaxCatalogConnections(5)
            catalog = JdbcDucklakeCatalog(config)
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
    fun createTableWinnerCreateViewLoserConflicts() {
        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable {
                catalog.createTable("test_schema", "shared_name",
                    listOf(TableColumnSpec.leaf("id", "integer", false)),
                    Optional.empty(),
                    Optional.empty())
            },
            Runnable {
                catalog.createView("test_schema", "shared_name",
                    "SELECT 1 AS x", "trino", null)
            })

        assertThat(result.loserException)
            .`as`("creating a view with a name a concurrent transaction just created as a table must conflict")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
            .`as`("error message must name the entry kind already created (table) and the contended name")
            .contains("create view")
            .contains("shared_name")
            .contains("test_schema")
            .contains("table") // the existing entry's kind, per upstream message shape

        assertThat((result.loserException as TransactionConflictException).retryable())
            .`as`("logical conflicts are non-retryable")
            .isFalse()
        assertThat(result.loserAttemptCount)
            .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getTable("test_schema", "shared_name", latestSnapshot))
            .`as`("winner's table lands at the latest snapshot")
            .isPresent
        assertThat(catalog.getView("test_schema", "shared_name", latestSnapshot))
            .`as`("loser's view did NOT land — its action was rolled back")
            .isEmpty
    }
}
