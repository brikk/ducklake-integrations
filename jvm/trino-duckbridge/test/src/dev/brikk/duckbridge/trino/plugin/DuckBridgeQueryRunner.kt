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
package dev.brikk.duckbridge.trino.plugin

import io.trino.testing.DistributedQueryRunner
import io.trino.testing.QueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import java.nio.file.Files

/**
 * Test harness for the DuckBridge connector. Cribbed from upstream Trino 476's
 * `DuckDbQueryRunner` / `TestingDuckDb`: a [DistributedQueryRunner] whose catalog points at a
 * freshly-created temp `.db` file via the standard `connection-url` property.
 */
object DuckBridgeQueryRunner {
    const val CATALOG = "duckbridge"
    const val SCHEMA = "test_schema"

    /**
     * Creates a temp DuckDB database file path and returns its `jdbc:duckdb:` URL. The file
     * itself is created by DuckDB on first connect; we delete the empty placeholder so DuckDB
     * initialises a fresh database.
     */
    fun freshDatabaseUrl(): String {
        val path = Files.createTempFile("duckbridge-", ".db")
        Files.delete(path)
        return "jdbc:duckdb:$path"
    }

    fun create(connectionUrl: String): QueryRunner = create(connectionUrl, emptyMap())

    /**
     * @param connectionUrl the base-jdbc `connection-url` (`jdbc:duckdb:...` embedded or
     *        `jdbc:quack://host:port` remote).
     * @param extraProperties additional catalog properties (e.g. `duckbridge.quack.token`,
     *        `duckbridge.parity-extension-path`).
     */
    fun create(connectionUrl: String, extraProperties: Map<String, String>): QueryRunner {
        val session =
            testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build()
        val queryRunner = DistributedQueryRunner.builder(session).build()
        try {
            queryRunner.installPlugin(DuckBridgePlugin())
            queryRunner.createCatalog(
                CATALOG,
                "duckbridge",
                buildMap {
                    put("connection-url", connectionUrl)
                    putAll(extraProperties)
                },
            )
            return queryRunner
        } catch (e: Throwable) {
            queryRunner.close()
            throw e
        }
    }
}
