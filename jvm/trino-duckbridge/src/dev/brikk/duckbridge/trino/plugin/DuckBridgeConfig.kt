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

import io.airlift.configuration.Config
import io.airlift.configuration.ConfigDescription
import io.airlift.units.DataSize
import jakarta.validation.constraints.AssertTrue

/**
 * Connector-level configuration for the DuckBridge connector.
 *
 * The DuckDB connection URL comes from the standard base-jdbc `connection-url` catalog
 * property (handled by [io.trino.plugin.jdbc.BaseJdbcConfig]); this class holds the
 * DuckBridge-specific knobs.
 */
class DuckBridgeConfig {
    /**
     * When true, the in-process DuckDB connection is opened with
     * `allow_unsigned_extensions=true`, which is required later to `LOAD` the
     * locally-built (unsigned) `trino_parity.duckdb_extension`. Defaults to true so the
     * parity extension path works out of the box; operators can flip it off to harden a
     * deployment that never loads the extension.
     */
    var isAllowUnsignedExtensions: Boolean = true
        private set

    @Config("duckbridge.allow-unsigned-extensions")
    @ConfigDescription("Allow the in-process DuckDB to LOAD unsigned extensions (needed for the trino_parity extension)")
    fun setAllowUnsignedExtensions(allowUnsignedExtensions: Boolean): DuckBridgeConfig {
        this.isAllowUnsignedExtensions = allowUnsignedExtensions
        return this
    }

    /**
     * Master switch for parity-backed function-shape expression pushdown. When true (default), the
     * connector LOADs and probes the trino_parity extension on first connection and pushes
     * `trino_*(...)` predicates to DuckDB. When false, function-shape pushdown is OFF entirely
     * (the expression rewriter has no parity rules); domain and LIMIT/TopN pushdown still apply.
     */
    var isParityEnabled: Boolean = true
        private set

    @Config("duckbridge.parity.enabled")
    @ConfigDescription("Enable parity-backed function-shape expression pushdown via the trino_parity extension")
    fun setParityEnabled(parityEnabled: Boolean): DuckBridgeConfig {
        this.isParityEnabled = parityEnabled
        return this
    }

    // ---- lance / vortex scan+search PTFs (P5) ----------------------------------------------
    // These PTFs run through DuckDB's `lance` / `vortex` extensions. When enabled, the connector
    // INSTALLs (floating latest) + LOADs the extension on each connection so the PTF's synthetic
    // scan query resolves. Off by default so a plain DuckDB/parity deployment never pays the
    // per-connection extension load. On a remote (Quack) server the extension is a server-side
    // concern — the connector probes it, never manages it.

    /** Enable the `lance_scan` / `lance_vector_search` / `lance_fts` / `lance_hybrid_search` PTFs. */
    var isLanceEnabled: Boolean = false
        private set

    @Config("duckbridge.lance.enabled")
    @ConfigDescription("Enable the lance scan + search table functions (requires the DuckDB lance extension)")
    fun setLanceEnabled(lanceEnabled: Boolean): DuckBridgeConfig {
        this.isLanceEnabled = lanceEnabled
        return this
    }

    /** Enable the `vortex_scan` PTF. */
    var isVortexEnabled: Boolean = false
        private set

    @Config("duckbridge.vortex.enabled")
    @ConfigDescription("Enable the vortex scan table function (requires the DuckDB vortex extension)")
    fun setVortexEnabled(vortexEnabled: Boolean): DuckBridgeConfig {
        this.isVortexEnabled = vortexEnabled
        return this
    }

    /**
     * Optional explicit filesystem path to the `trino_parity.duckdb_extension` binary. When set,
     * it overrides the bundled-per-platform extraction path resolved by
     * [TrinoParityExtensionResolver]. Needed when the bundled binary doesn't match the runtime
     * platform, or (P3) when a server-side DuckDB must LOAD a path only it can resolve.
     */
    var parityExtensionPath: String? = null
        private set

    @Config("duckbridge.parity-extension-path")
    @ConfigDescription("Explicit path to trino_parity.duckdb_extension, overriding the bundled binary")
    fun setParityExtensionPath(parityExtensionPath: String?): DuckBridgeConfig {
        this.parityExtensionPath = parityExtensionPath?.takeIf { it.isNotBlank() }
        return this
    }

    // ---- Quack (T3 remote) auth -------------------------------------------------------------
    // Host/port live in the connection-url (jdbc:quack://host:port/...). Credentials live here so
    // they are not embedded in a copy-pasteable URL. Mapped straight onto quack-jdbc's connection
    // properties (token / tokenEnv / tokenFile / tls).

    /** Quack auth token, passed to quack-jdbc's `token` property. Prefer [quackTokenEnv]/[quackTokenFile] for secrets. */
    var quackToken: String? = null
        private set

    @Config("duckbridge.quack.token")
    @ConfigDescription("Quack authentication token (quack-jdbc 'token' property); prefer token-env/token-file for secrets")
    fun setQuackToken(quackToken: String?): DuckBridgeConfig {
        this.quackToken = quackToken?.takeIf { it.isNotBlank() }
        return this
    }

    /** Environment variable name holding the Quack token (quack-jdbc's `tokenEnv`). */
    var quackTokenEnv: String? = null
        private set

    @Config("duckbridge.quack.token-env")
    @ConfigDescription("Name of an environment variable holding the Quack token (quack-jdbc 'tokenEnv')")
    fun setQuackTokenEnv(quackTokenEnv: String?): DuckBridgeConfig {
        this.quackTokenEnv = quackTokenEnv?.takeIf { it.isNotBlank() }
        return this
    }

    /** File path holding the Quack token (quack-jdbc's `tokenFile`). */
    var quackTokenFile: String? = null
        private set

    @Config("duckbridge.quack.token-file")
    @ConfigDescription("Path to a file holding the Quack token (quack-jdbc 'tokenFile')")
    fun setQuackTokenFile(quackTokenFile: String?): DuckBridgeConfig {
        this.quackTokenFile = quackTokenFile?.takeIf { it.isNotBlank() }
        return this
    }

    /** Whether the Quack HTTP transport uses TLS (`https://`). Maps to quack-jdbc's `tls`. */
    var isQuackTls: Boolean = false
        private set

    @Config("duckbridge.quack.tls")
    @ConfigDescription("Use TLS (https) for the Quack HTTP transport (quack-jdbc 'tls')")
    fun setQuackTls(quackTls: Boolean): DuckBridgeConfig {
        this.isQuackTls = quackTls
        return this
    }

    // ---- T2 execution engine (Arrow data plane) --------------------------------------------
    // The default JDBC record-set path is production. The T2 Arrow page source is a benchmark
    // channel; the DUCKDB_LOCAL/QUACK engines opt into it.

    /**
     * Data-plane read strategy. Default [DuckBridgeExecutionEngine.JDBC] (the plain base-jdbc
     * row-by-row path). [DuckBridgeExecutionEngine.DUCKDB_LOCAL] / [DuckBridgeExecutionEngine.QUACK]
     * select the T2 Arrow page source — a BENCHMARK CHANNEL, not the default: Quack 1.5.4's fixed
     * server-side connection pool exhausts under per-split churn, so QUACK is gated until the pool
     * rework lands.
     */
    var executionEngine: DuckBridgeExecutionEngine = DuckBridgeExecutionEngine.JDBC
        private set

    @Config("duckbridge.execution-engine")
    @ConfigDescription("Data-plane engine: JDBC (default, production), DUCKDB_LOCAL or QUACK (T2 Arrow, benchmark)")
    fun setExecutionEngine(executionEngine: DuckBridgeExecutionEngine): DuckBridgeConfig {
        this.executionEngine = executionEngine
        return this
    }

    /**
     * Fail loud, never silently degrade: the QUACK T2 Arrow engine is gated on the upstream Quack
     * server pool rework (1.5.4's fixed server-side connection pool exhausts under per-split
     * churn) and `buildSql` cannot yet emit the `quack_query_by_name`-wrapped SQL it needs.
     * Accepting the setting and quietly serving the JDBC path instead would be a config that lies,
     * so it is rejected at startup until the gate lifts. Remote Quack is served by the T3
     * quack-jdbc transport (`connection-url=jdbc:quack://...`) in the interim.
     */
    @get:AssertTrue(
        message = "duckbridge.execution-engine=QUACK is gated on the Quack server pool rework (1.5.4 pool " +
            "exhaustion under per-split churn) and is not yet operational. Use the T3 quack-jdbc transport " +
            "(connection-url=jdbc:quack://host:port) for remote Quack, or DUCKDB_LOCAL/JDBC engines.",
    )
    val isExecutionEngineOperational: Boolean
        get() = executionEngine != DuckBridgeExecutionEngine.QUACK

    /** Quack server host for the T2 QUACK engine (the T3 quack-jdbc transport reads host from the URL). */
    var quackHost: String? = null
        private set

    @Config("duckbridge.quack.host")
    @ConfigDescription("Quack server host for the T2 QUACK execution engine")
    fun setQuackHost(quackHost: String?): DuckBridgeConfig {
        this.quackHost = quackHost?.takeIf { it.isNotBlank() }
        return this
    }

    /** Quack server port for the T2 QUACK engine. */
    var quackPort: Int = DEFAULT_QUACK_PORT
        private set

    @Config("duckbridge.quack.port")
    @ConfigDescription("Quack server port for the T2 QUACK execution engine")
    fun setQuackPort(quackPort: Int): DuckBridgeConfig {
        this.quackPort = quackPort
        return this
    }

    // ---- DuckDB tuning (applied by the T2 executors) ---------------------------------------

    var memoryLimit: DataSize? = null
        private set

    @Config("duckbridge.duckdb.memory-limit")
    @ConfigDescription("DuckDB memory_limit for the T2 execution engine (e.g. '4GB')")
    fun setMemoryLimit(memoryLimit: DataSize?): DuckBridgeConfig {
        this.memoryLimit = memoryLimit
        return this
    }

    var threads: Int? = null
        private set

    @Config("duckbridge.duckdb.threads")
    @ConfigDescription("DuckDB threads for the T2 execution engine")
    fun setThreads(threads: Int?): DuckBridgeConfig {
        this.threads = threads
        return this
    }

    fun toDuckDbTuning(): DuckDbTuning = DuckDbTuning(memoryLimit, threads, null, null, true)

    private companion object {
        private const val DEFAULT_QUACK_PORT = 9494
    }
}
