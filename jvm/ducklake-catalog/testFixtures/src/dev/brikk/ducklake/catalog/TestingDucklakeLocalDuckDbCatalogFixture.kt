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

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * Test-fixture wrapper for the local-DuckDB DuckLake catalog backend. Manages a
 * single root temp directory that hosts one `<testName>/lake.db` file per
 * isolated catalog plus its parquet data subdir. Clients connect with plain
 * `jdbc:duckdb:/path/to/lake.db` — no Quack, no synthetic URL, no
 * Testcontainer.
 *
 * Exists so [JdbcDucklakeCatalog]'s SQL + type handling can be
 * validated end-to-end against DuckDB-as-catalog while upstream Quack RPC
 * matures. The same fixture lets the Trino plugin tests run under
 * `DUCKDB_LOCAL` as a sibling to the existing `POSTGRES` backend.
 */
open class TestingDucklakeLocalDuckDbCatalogFixture : AutoCloseable {
    private val rootDir: Path

    init {
        try {
            this.rootDir = Files.createTempDirectory("ducklake-local-catalog-")
        }
        catch (ex: IOException) {
            throw RuntimeException("Failed to create local-DuckDB catalog root dir", ex)
        }
    }

    /**
     * Returns the directory that should host the `lake.db` file and the
     * `data/` subdir for `testName`. Caller (the catalog generator)
     * is responsible for creating / recreating it — by convention generators
     * wipe and recreate the directory so each test starts clean.
     */
    fun catalogDirectory(testName: String): Path {
        return rootDir.resolve("test-catalog-" + sanitize(testName))
    }

    override fun close() {
        deleteRecursively(rootDir)
    }

    companion object {
        private fun sanitize(name: String): String {
            return name.replace(Regex("[^A-Za-z0-9_-]"), "_")
        }

        private fun deleteRecursively(dir: Path) {
            if (!Files.exists(dir)) {
                return
            }
            try {
                Files.walk(dir).use { walk ->
                    walk.sorted(Comparator.reverseOrder()).forEach { p ->
                        try {
                            Files.deleteIfExists(p)
                        }
                        catch (ignored: IOException) {
                        }
                    }
                }
            }
            catch (ignored: IOException) {
            }
        }
    }
}
