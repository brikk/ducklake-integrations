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
package dev.brikk.ducklake.catalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Test-fixture wrapper for the local-DuckDB DuckLake catalog backend. Manages a
 * single root temp directory that hosts one {@code <testName>/lake.db} file per
 * isolated catalog plus its parquet data subdir. Clients connect with plain
 * {@code jdbc:duckdb:/path/to/lake.db} — no Quack, no synthetic URL, no
 * Testcontainer.
 *
 * <p>Exists so {@link JdbcDucklakeCatalog}'s SQL + type handling can be
 * validated end-to-end against DuckDB-as-catalog while upstream Quack RPC
 * matures. The same fixture lets the Trino plugin tests run under
 * {@code DUCKDB_LOCAL} as a sibling to the existing {@code POSTGRES} backend.
 */
public class TestingDucklakeLocalDuckDbCatalogFixture
        implements AutoCloseable
{
    private final Path rootDir;

    public TestingDucklakeLocalDuckDbCatalogFixture()
    {
        try {
            this.rootDir = Files.createTempDirectory("ducklake-local-catalog-");
        }
        catch (IOException ex) {
            throw new RuntimeException("Failed to create local-DuckDB catalog root dir", ex);
        }
    }

    /**
     * Returns the directory that should host the {@code lake.db} file and the
     * {@code data/} subdir for {@code testName}. Caller (the catalog generator)
     * is responsible for creating / recreating it — by convention generators
     * wipe and recreate the directory so each test starts clean.
     */
    public Path catalogDirectory(String testName)
    {
        return rootDir.resolve("test-catalog-" + sanitize(testName));
    }

    @Override
    public void close()
    {
        deleteRecursively(rootDir);
    }

    private static String sanitize(String name)
    {
        return name.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private static void deleteRecursively(Path dir)
    {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                }
                catch (IOException ignored) {
                }
            });
        }
        catch (IOException ignored) {
        }
    }
}
