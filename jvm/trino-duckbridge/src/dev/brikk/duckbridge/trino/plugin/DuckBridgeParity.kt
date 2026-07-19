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

import com.google.inject.Inject
import io.airlift.log.Logger
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import java.sql.Connection
import java.sql.SQLException

/**
 * Owns parity-extension lifecycle for the connector: resolves the extension binary path once, and
 * on each session connection LOADs + probes the extension, failing loud if parity is enabled but the
 * extension can't be made available.
 *
 * "Fail loud over silently wrong": if the operator enabled parity (default) but the extension is
 * missing or won't load/probe, every query on that catalog throws a clear [TrinoException] with
 * install instructions rather than silently degrading to no function pushdown (which would look like
 * a mysterious perf cliff, not a misconfiguration).
 *
 * P3 note: this LOADs into the in-process (T1 embedded) DuckDB the JDBC driver opened. A remote
 * DuckDB (Quack) needs the LOAD forwarded server-side against a server-resident binary; this class's
 * "resolve a local path, LOAD it over the JDBC connection" assumption must generalize to "the server
 * already has the extension, just LOAD by name/server-path" for P3.
 */
class DuckBridgeParity
    @Inject
    constructor(private val config: DuckBridgeConfig) {
        val isEnabled: Boolean get() = config.isParityEnabled

        /**
         * Resolved extension path (explicit override or bundled extraction). Computed lazily and
         * memoised; null only when parity is disabled OR no binary is available (the latter throws at
         * [ensureInitialised] time so the failure surfaces per-query, not at wiring time).
         */
        private val resolvedPath: String? by lazy { resolvePath() }

        private fun resolvePath(): String? {
            if (!config.isParityEnabled) {
                return null
            }
            config.parityExtensionPath?.let { return it }
            return TrinoParityExtensionResolver.resolveBundledExtensionPath()
        }

        /**
         * LOAD + probe the parity extension on [connection] the first time it is handed out. Cheap and
         * idempotent to call per connection: DuckDB LOAD of an already-loaded extension is a no-op, and
         * the `trino_meta()` probe is a tiny table scan. No-op when parity is disabled.
         *
         * @throws TrinoException if parity is enabled but the extension can't be loaded or probed.
         */
        fun ensureInitialised(connection: Connection) {
            if (!config.isParityEnabled) {
                return
            }
            val path =
                resolvedPath
                    ?: throw parityUnavailable(
                        "the trino_parity DuckDB extension binary was not found for this platform " +
                            "(${TrinoParityExtensionResolver.detectPlatform() ?: "unknown platform"})",
                    )
            loadAndProbe(connection, path)?.let { throw it }
        }

        /** LOAD + probe; returns a ready-to-throw [TrinoException] on any failure, or null on success. */
        private fun loadAndProbe(connection: Connection, path: String): TrinoException? =
            try {
                TrinoFunctionAliases.loadInProcess(connection, path)
                val rows = TrinoFunctionAliases.probeMetaRowCount(connection)
                if (rows > 0) {
                    null
                } else {
                    parityUnavailable("trino_meta() returned no rows after LOAD '$path' — extension did not register")
                }
            } catch (e: SQLException) {
                parityUnavailable("failed to LOAD/probe trino_parity from '$path': ${e.message}", e)
            }

        private fun parityUnavailable(reason: String, cause: Throwable? = null): TrinoException {
            log.error(cause, "duckbridge: parity extension unavailable — %s", reason)
            return TrinoException(
                NOT_SUPPORTED,
                "DuckBridge parity pushdown is enabled but unavailable: $reason. " +
                    "Build the extension (`(cd duckdb-trino-parity-extension && make)`) so it is bundled in the " +
                    "plugin jar, set duckbridge.parity-extension-path to a valid binary, or disable pushdown with " +
                    "duckbridge.parity.enabled=false.",
                cause,
            )
        }

        private companion object {
            private val log: Logger = Logger.get(DuckBridgeParity::class.java)
        }
    }
