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
 * Acceptance test for [ConflictMatrix]'s
 * `created_schema × created_schema` entry
 * (port of `ducklake_transaction.cpp:1212–1215`).
 *
 * Two writers concurrently calling `createSchema("dueling_schema")`
 * must not both land. Without the matrix this corrupts the catalog —
 * upstream's metadata DDL has no UNIQUE on `ducklake_schema.schema_name`
 * (only single-column PK on `schema_id`, see
 * `ducklake_metadata_manager.cpp:198`), so two active rows with the
 * same name would otherwise both INSERT successfully on retry.
 */
class TestConcurrentCreateSchemaSameName {
    companion object {
        private lateinit var server: TestingDucklakePostgreSqlCatalogServer
        private lateinit var catalog: JdbcDucklakeCatalog

        @BeforeAll
        @JvmStatic
        @Throws(Exception::class)
        fun setUpClass() {
            server = TestingDucklakePostgreSqlCatalogServer()
            val isolated = JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(
                server, "concurrent-create-schema-same-name")

            val config = DucklakeCatalogConfig().apply {
                catalogDatabaseUrl = isolated.jdbcUrl
                catalogDatabaseUser = isolated.user
                catalogDatabasePassword = isolated.password
                dataPath = isolated.dataDir.toAbsolutePath().toString()
                maxCatalogConnections = 5
            }
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
    fun duelingCreateSchemaConflicts() {
        val result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
            catalog,
            Runnable { catalog.createSchema("dueling_schema") },
            Runnable { catalog.createSchema("dueling_schema") })

        assertThat(result.loserException)
            .`as`("two concurrent createSchema with same name must conflict — upstream matrix entry")
            .isInstanceOf(LogicalConflictException::class.java)
        assertThat(result.loserException!!.message)
            .`as`("error message must name the conflicting schema and the cause")
            .contains("create schema")
            .contains("dueling_schema")
            .contains("created a schema with this name already")

        assertThat((result.loserException as TransactionConflictException).retryable())
            .`as`("logical conflicts are non-retryable")
            .isFalse()
        assertThat(result.loserAttemptCount)
            .`as`("loser must NOT burn the retry budget — exactly two attempts: parked + matrix-failed")
            .isEqualTo(2)

        val latestSnapshot = catalog.currentSnapshotId
        assertThat(catalog.getSchema("dueling_schema", latestSnapshot))
            .`as`("winner's createSchema lands; loser's aborted before commit")
            .isNotNull()
    }
}
