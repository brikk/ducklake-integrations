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

import com.google.inject.Inject
import io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import java.util.Objects.requireNonNull

/**
 * Constructs the configured {@link DucklakeDuckDbExecutor} per split. A new
 * executor is returned for each call — executors are per-split state and own a
 * JDBC connection through their lifecycle.
 *
 * <p>The selection happens at catalog-config level via
 * {@link DucklakeConfig#getExecutionEngine()}. Per-split or per-session
 * selection is not exposed (yet) because the choice of engine has operational
 * implications — different deployment topology, different failure modes — that
 * are best made once at catalog configuration time, not per query.
 *
 * <p>The trino_parity DuckDB extension is REQUIRED — there is no SQL-replay
 * fallback. The factory resolves the path with this precedence:
 * <ol>
 *   <li>{@code ducklake.duckdb.parity-extension-path} config property (any engine).</li>
 *   <li>For DUCKDB_LOCAL only: the bundled binary extracted by
 *       {@link TrinoParityExtensionResolver} from the plugin jar.</li>
 * </ol>
 * If both are absent, the catalog refuses to construct an executor — operators
 * see a clear configuration error at first use rather than a query-time
 * failure for missing macros.
 */
class DucklakeDuckDbExecutorFactory @Inject constructor(config: DucklakeConfig) {
    private val config: DucklakeConfig = requireNonNull(config, "config is null")

    fun create(): DucklakeDuckDbExecutor {
        val tuning = config.toDuckDbTuning()
        return when (config.getExecutionEngine()) {
            DucklakeExecutionEngine.DUCKDB_LOCAL -> InProcessDuckDbExecutor(tuning, resolveInProcessParityPath())
            DucklakeExecutionEngine.QUACK -> {
                val host = config.getQuackHost()
                val token = config.getQuackToken()
                if (host == null || host.isBlank()) {
                    throw TrinoException(CONFIGURATION_INVALID,
                            "ducklake.execution-engine=quack requires ducklake.quack.host")
                }
                if (token == null || token.isBlank()) {
                    throw TrinoException(CONFIGURATION_INVALID,
                            "ducklake.execution-engine=quack requires ducklake.quack.token")
                }
                QuackDuckDbExecutor(host, config.getQuackPort(), token, tuning,
                        resolveQuackParityPath())
            }
            DucklakeExecutionEngine.SWANLAKE -> throw TrinoException(NOT_SUPPORTED,
                    "ducklake.execution-engine=swanlake is reserved but not yet implemented. "
                            + "Use 'duckdb_local' or 'quack' for now.")
        }
    }

    /**
     * Path the in-process executor will LOAD into its JDBC DuckDB. Explicit
     * config wins; absent config falls back to the bundled-and-extracted path.
     */
    private fun resolveInProcessParityPath(): String {
        val configured = config.getDuckdbParityExtensionPath()
        if (configured.isPresent) {
            return configured.get()
        }
        val bundled = TrinoParityExtensionResolver.resolveBundledExtensionPath()
        if (bundled.isPresent) {
            return bundled.get()
        }
        throw TrinoException(CONFIGURATION_INVALID,
                "trino_parity extension required: set ducklake.duckdb.parity-extension-path " +
                        "to a built trino_parity.duckdb_extension binary, or rebuild the plugin jar with " +
                        "the extension binary present so the bundled-resource resolver can extract it. " +
                        "Extension repo: https://github.com/brikk/duckdb-trino-parity-extension")
    }

    /**
     * Path the Quack executor will forward through the wrapper. The Quack
     * server runs in a separate process / container; the resolver's bundled
     * binary lives only on the Trino worker's classpath and can't be read
     * server-side. So Quack REQUIRES an explicit config path pointing at where
     * the extension lives inside the Quack server's filesystem.
     */
    private fun resolveQuackParityPath(): String {
        val configured = config.getDuckdbParityExtensionPath()
        if (configured.isPresent) {
            return configured.get()
        }
        throw TrinoException(CONFIGURATION_INVALID,
                "trino_parity extension required for Quack engine: set ducklake.duckdb.parity-extension-path " +
                        "to the SERVER-SIDE path of trino_parity.duckdb_extension inside the Quack DuckDB process. " +
                        "The bundled-resource path used for the in-process engine is not usable here — the " +
                        "Quack server can't read the Trino worker's classpath.")
    }
}
