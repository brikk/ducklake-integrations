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
 * Transport-aware (P3):
 *  - EMBEDDED (T1): resolve a worker-local binary (bundled extraction or the configured path) and
 *    `LOAD '<local-path>'` over the in-process DuckDB connection.
 *  - QUACK (T3): the worker cannot extract a binary for the *remote* server. If
 *    `duckbridge.parity-extension-path` is set it is treated as a SERVER-SIDE path and LOADed over
 *    the pass-through connection; otherwise we assume the server pre-loaded the extension and just
 *    probe `trino_meta()`. Either way, failure throws with server-side install instructions.
 */
class DuckBridgeParity
    @Inject
    constructor(
        private val config: DuckBridgeConfig,
        private val transport: DuckBridgeTransport,
    ) {
        val isEnabled: Boolean get() = config.isParityEnabled

        /**
         * Resolved LOCAL extension path for the embedded transport (explicit override or bundled
         * extraction). Computed lazily and memoised. Never consulted for the Quack transport — a
         * worker-local binary can't be LOADed into a remote server.
         */
        private val resolvedLocalPath: String? by lazy {
            if (!config.isParityEnabled) {
                null
            } else {
                config.parityExtensionPath ?: TrinoParityExtensionResolver.resolveBundledExtensionPath()
            }
        }

        /**
         * LOAD (where applicable) + probe the parity extension on [connection] each time it is handed
         * out. Idempotent: DuckDB LOAD of an already-loaded extension is a no-op and the `trino_meta()`
         * probe is a tiny table scan. No-op when parity is disabled.
         *
         * @throws TrinoException if parity is enabled but the extension can't be loaded or probed.
         */
        fun ensureInitialised(connection: Connection) {
            if (!config.isParityEnabled) {
                return
            }
            when (transport) {
                DuckBridgeTransport.EMBEDDED -> initialiseEmbedded(connection)
                DuckBridgeTransport.QUACK -> initialiseQuack(connection)
            }
        }

        private fun initialiseEmbedded(connection: Connection) {
            val path =
                resolvedLocalPath
                    ?: throw parityUnavailable(
                        "the trino_parity DuckDB extension binary was not found for this platform " +
                            "(${TrinoParityExtensionResolver.detectPlatform() ?: "unknown platform"})",
                        remote = false,
                    )
            loadAndProbe(connection, path, remote = false)?.let { throw it }
        }

        private fun initialiseQuack(connection: Connection) {
            // On a remote server we can only LOAD a path the SERVER can read. When configured, treat
            // duckbridge.parity-extension-path as a server-side path; otherwise assume the server
            // pre-loaded the extension and just probe.
            val serverPath = config.parityExtensionPath
            if (serverPath != null) {
                loadAndProbe(connection, serverPath, remote = true)?.let { throw it }
            } else {
                probeOnly(connection)?.let { throw it }
            }
        }

        /** LOAD + probe; returns a ready-to-throw [TrinoException] on any failure, or null on success. */
        private fun loadAndProbe(connection: Connection, path: String, remote: Boolean): TrinoException? =
            try {
                TrinoFunctionAliases.loadInProcess(connection, path)
                probeAfterLoad(connection, path, remote)
            } catch (e: SQLException) {
                parityUnavailable("failed to LOAD/probe trino_parity from '$path': ${e.message}", remote, e)
            }

        /** Probe only — used for Quack when the server is expected to have pre-loaded the extension. */
        private fun probeOnly(connection: Connection): TrinoException? =
            try {
                probeAfterLoad(connection, path = null, remote = true)
            } catch (e: SQLException) {
                parityUnavailable("trino_meta() is not resolvable on the Quack server: ${e.message}", remote = true, e)
            }

        @Throws(SQLException::class)
        private fun probeAfterLoad(connection: Connection, path: String?, remote: Boolean): TrinoException? {
            val rows = TrinoFunctionAliases.probeMetaRowCount(connection)
            return if (rows > 0) {
                null
            } else {
                val where = path?.let { "after LOAD '$it'" } ?: "on the Quack server"
                parityUnavailable("trino_meta() returned no rows $where — extension did not register", remote)
            }
        }

        private fun parityUnavailable(reason: String, remote: Boolean, cause: Throwable? = null): TrinoException {
            log.error(cause, "duckbridge: parity extension unavailable — %s", reason)
            val fix =
                if (remote) {
                    "Install the trino_parity extension on the Quack/DuckDB server (start it with " +
                        "`duckdb -unsigned` and `LOAD` the binary, or set duckbridge.parity-extension-path to a " +
                        "server-side path)"
                } else {
                    "Build the extension (`(cd duckdb-trino-parity-extension && make)`) so it is bundled in the " +
                        "plugin jar, or set duckbridge.parity-extension-path to a valid local binary"
                }
            return TrinoException(
                NOT_SUPPORTED,
                "DuckBridge parity pushdown is enabled but unavailable: $reason. " +
                    "$fix, or disable pushdown with duckbridge.parity.enabled=false.",
                cause,
            )
        }

        private companion object {
            private val log: Logger = Logger.get(DuckBridgeParity::class.java)
        }
    }
