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
 * Acceptance test for [ConflictMatrix]'s
 * `created_table × created_table` entry
 * (port of `ducklake_transaction.cpp:1232–1242`).
 *
 * Two writers concurrently creating a table with the same
 * `(schema, name)` must not both land. Upstream's DDL has no UNIQUE
 * on `(schema_id, table_name)` for active rows (see
 * `ducklake_metadata_manager.cpp:199`); the matrix is the only safety
 * net.
 */
class TestConcurrentCreateTableSameName {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                server, "concurrent-create-table-same-name")

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
    fun duelingCreateTableConflicts() {
        val columns = listOf(
            TableColumnSpec.leaf("id", "integer", false))

        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.createTable("test_schema", "dueling_table", columns, Optional.empty(), Optional.empty()) },
            Runnable { catalog.createTable("test_schema", "dueling_table", columns, Optional.empty(), Optional.empty()) })

        assertThat(result.loserException)
            .`as`("two concurrent createTable with same (schema, name) must conflict")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
            .`as`("error message must name the conflicting table and schema")
            .contains("create table")
            .contains("dueling_table")
            .contains("test_schema")
            .contains("created by another transaction already")

        assertThat((result.loserException as TransactionConflictException).retryable())
            .`as`("logical conflicts are non-retryable")
            .isFalse()
        assertThat(result.loserAttemptCount)
            .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getTable("test_schema", "dueling_table", latestSnapshot))
            .`as`("winner's createTable lands; loser's aborted before commit")
            .isPresent
    }
}
