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

import java.util.Locale

/**
 * Selects which catalog-backend fixture the Trino plugin tests should run against.
 *
 *
 * Drives both [DucklakeTestCatalogEnvironment] (which server to stand up)
 * and [DucklakeQueryRunner.Builder] (which JDBC URL shape to feed the
 * connector). PostgreSQL remains the default — Quack-backed DuckDB-as-catalog
 * is opt-in until upstream Quack matures past the multi-scan and other FIXME
 * limitations tracked in `vendor/ducklake/test/configs/quack.json`.
 *
 *
 * Selected via the JVM system property `ducklake.test.catalog-backend`
 * (case-insensitive). Examples:
 *
 * ```
 * # default — PostgreSQL via Testcontainers
 * ./gradlew :trino-ducklake:test
 *
 * # DuckDB-as-catalog over Quack RPC, via a containerized DuckDB sidecar
 * ./gradlew :trino-ducklake:test -Dducklake.test.catalog-backend=DUCKDB_QUACK
 * ```
 */
enum class DucklakeTestCatalogBackend {
    POSTGRES,

    /**
     * Local DuckDB `.db` file as the DuckLake catalog backend, accessed
     * via plain `jdbc:duckdb:/path/to/lake.db`. Exists to validate that
     * `JdbcDucklakeCatalog`'s SQL and type handling are DuckDB-compatible
     * — the dialect, the `renderSchema(false)` setting, and the type
     * round-trips — independent of the Quack RPC layer's current limitations.
     * When upstream Quack matures, the Quack backend reuses the same evidence.
     */
    DUCKDB_LOCAL,
    DUCKDB_QUACK;

    companion object {
        const val SYSTEM_PROPERTY: String = "ducklake.test.catalog-backend"

        @JvmStatic
        fun fromSystemProperty(): DucklakeTestCatalogBackend {
            val value = System.getProperty(SYSTEM_PROPERTY)
            if (value == null || value.isBlank()) {
                return POSTGRES
            }
            try {
                return valueOf(value.trim().uppercase(Locale.ROOT))
            }
            catch (ex: IllegalArgumentException) {
                throw IllegalArgumentException(
                        "Unknown " + SYSTEM_PROPERTY + " value: " + value
                                + " (expected one of POSTGRES, DUCKDB_LOCAL, DUCKDB_QUACK)", ex)
            }
        }
    }
}
