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

import java.util.Locale;

/**
 * Selects which catalog-backend fixture the Trino plugin tests should run against.
 *
 * <p>Drives both {@link DucklakeTestCatalogEnvironment} (which server to stand up)
 * and {@link DucklakeQueryRunner.Builder} (which JDBC URL shape to feed the
 * connector). PostgreSQL remains the default — Quack-backed DuckDB-as-catalog
 * is opt-in until upstream Quack matures past the multi-scan and other FIXME
 * limitations tracked in {@code vendor/ducklake/test/configs/quack.json}.
 *
 * <p>Selected via the JVM system property {@code ducklake.test.catalog-backend}
 * (case-insensitive). Examples:
 *
 * <pre>{@code
 * # default — PostgreSQL via Testcontainers
 * ./gradlew :trino-ducklake:test
 *
 * # DuckDB-as-catalog over Quack RPC, via a containerized DuckDB sidecar
 * ./gradlew :trino-ducklake:test -Dducklake.test.catalog-backend=DUCKDB_QUACK
 * }</pre>
 */
public enum DucklakeTestCatalogBackend
{
    POSTGRES,
    /**
     * Local DuckDB {@code .db} file as the DuckLake catalog backend, accessed
     * via plain {@code jdbc:duckdb:/path/to/lake.db}. Exists to validate that
     * {@code JdbcDucklakeCatalog}'s SQL and type handling are DuckDB-compatible
     * — the dialect, the {@code renderSchema(false)} setting, and the type
     * round-trips — independent of the Quack RPC layer's current limitations.
     * When upstream Quack matures, the Quack backend reuses the same evidence.
     */
    DUCKDB_LOCAL,
    DUCKDB_QUACK;

    public static final String SYSTEM_PROPERTY = "ducklake.test.catalog-backend";

    public static DucklakeTestCatalogBackend fromSystemProperty()
    {
        String value = System.getProperty(SYSTEM_PROPERTY);
        if (value == null || value.isBlank()) {
            return POSTGRES;
        }
        try {
            return DucklakeTestCatalogBackend.valueOf(value.trim().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(
                    "Unknown " + SYSTEM_PROPERTY + " value: " + value
                            + " (expected one of POSTGRES, DUCKDB_LOCAL, DUCKDB_QUACK)", ex);
        }
    }
}
